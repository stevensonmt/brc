defmodule Brc do
  @pool_size :erlang.system_info(:logical_processors)

  def process_file(file, worker_queue) do
    worker = Enum.take(worker_queue, 1) |> hd()
    w_q = Stream.drop(worker_queue, 1)

    case :prim_file.read(file, 80_000_000) do
      :eof ->
        :ok

      {:ok, buffer} ->
        buffer =
          case :prim_file.read_line(file) do
            :eof ->
              buffer

            {:ok, line} ->
              <<buffer::binary, line::binary>>
          end

        t = Task.async(fn -> process_lines(buffer, worker) end)

        process_file(file, w_q)
        Task.await(t, :infinity)
    end
  end

  def process_lines("", _worker), do: :ok

  def process_lines(lines, worker) do
    case parse_line(lines) do
      {station, {temp, rest}} ->
        BrcRegistry.register(worker, station, temp)
        process_lines(rest, worker)

      :ok ->
        :ok
    end
  end

  def parse_line(""), do: :ok
  def parse_line(<<"\n", rest::binary>>), do: parse_line(rest)

  def parse_line(chunk) do
    Parse.parse_line(chunk)
  end

  def run_file(filename) do
    workers = 1..@pool_size |> Enum.map(fn w -> String.to_atom("BrcRegistry#{w}") end)

    workers |> Enum.each(fn w -> BrcRegistry.start(w) end)

    {:ok, file} = :prim_file.open(to_string(filename), [:binary, :read, :read_ahead])

    process_file(file, Stream.cycle(workers))

    print_tables(workers)
  end

  def print_tables(workers) do
    out =
      workers
      |> Task.async_stream(fn w -> :ets.tab2list(w) |> Map.new() end,
        max_concurrency: @pool_size,
        timeout: :infinity
      )
      |> Enum.map(&elem(&1, 1))
      |> Enum.reduce(fn m, acc -> Map.merge(m, acc, fn _k, v1, v2 -> merge_temps(v1, v2) end) end)
      |> Task.async_stream(fn {station, temps} -> format_station(station, temps) end)
      |> Enum.map(&elem(&1, 1))
      |> Enum.sort()
      |> Enum.reduce(fn str, acc -> acc <> "," <> str end)

    IO.puts("{#{out}}")
  end

  def merge_temps({l1, min1, mean1, max1}, {l2, min2, mean2, max2}) do
    {l1 + l2, min(min1, min2), (mean1 * l1 + mean2 * l2) / (l1 + l2), max(max1, max2)}
  end

  def merge_station(station, workers) do
    default = {0, :infinity, 0, -1_000}

    temps =
      workers
      |> Enum.reduce(default, fn w, {len, min, mean, max} ->
        {ln, mn, av, mx} = :ets.lookup_element(w, station, 2, default)
        {len + ln, min(min, mn), (mean * len + av * ln) / (len + ln), max(max, mx)}
      end)

    {station, temps}
  end

  def format_station(station, {_len, min, mean, max}) do
    station <>
      "=" <>
      :erlang.float_to_binary(min, decimals: 1) <>
      "/" <>
      :erlang.float_to_binary(mean, decimals: 1) <>
      "/" <> :erlang.float_to_binary(max, decimals: 1)
  end

  def cleanup_tables() do
    :ets.all()
    |> Enum.filter(fn e -> is_atom(e) and Atom.to_string(e) |> String.contains?("BrcRegistry") end)
    |> Enum.each(fn e -> :ets.delete(e) end)
  end

  def main(file) do
    {uSec, :ok} =
      :timer.tc(fn ->
        run_file(file)
        :ok
      end)

    IO.puts("It took #{uSec / 1000} milliseconds")
    cleanup_tables()
  end
end

defmodule Parse do
  def parse_line(line), do: parse_line(line, line, 0)

  def parse_line(line, <<";", rest::binary>>, count) do
    <<station::binary-size(count), _::binary>> = line
    {<<station::binary>>, parse_temp(<<rest::binary>>)}
  end

  def parse_line(line, <<_::8, rest::binary>>, count), do: parse_line(line, rest, count + 1)
  # Generates a function that takes binary strings and parses them in 2 parts:
  # - the first part can have max length of 100 bytes and is delimited by a semicolon.
  # - the second part is a float number that may be followed by a newline character.
  @compile {:inline, parse_temp: 1}
  for i <- 0..99 do
    temp_int = Integer.to_string(i)

    for d <- 0..9 do
      temp_dec = <<d + ?0>>
      temp_str = "#{temp_int}.#{temp_dec}"

      def parse_temp(<<unquote(temp_str), rest::binary>>),
        do: {unquote(i) + unquote(d) / 10, rest}

      def parse_temp(<<"-", unquote(temp_str), rest::binary>>),
        do: {-1 * (unquote(i) + unquote(d) / 10), rest}
    end
  end
end

defmodule BrcRegistry do
  def start(worker) do
    :ets.new(worker, [
      :set,
      :public,
      :named_table,
      write_concurrency: true
    ])
  end

  def register(registry, name, temp) do
    if :ets.insert_new(registry, {name, {1, temp, temp, temp}}) do
      :ok
    else
      {len, min, mean, max} = :ets.lookup_element(registry, name, 2)
      updated_val = {len + 1, min(min, temp), (mean * len + temp) / (len + 1), max(max, temp)}

      :ets.insert(registry, {name, updated_val})
    end
  end

  def stations(registry) do
    BrcRegistry.lookup(registry, :stations)
  end

  def lookup(registry, key) do
    :ets.lookup_element(registry, key, 2)
  end
end
