defmodule Brc do
  @pool_size :erlang.system_info(:logical_processors) * 5
  @blob_size 1024 * 1024
  @max_rows 50_000_000
  @chunk_size :math.sqrt(@max_rows) |> ceil()

  def process_chunk(chunk) do
    chunk
    |> Stream.each(&process_line/1)
    |> Stream.run()
  end

  def process_line(line) do
    {station, temp} = parse_line(line)

    table = String.to_atom(station)

    case Worker.add_station(:stations, table) do
      :station_table_added ->
        :ets.insert(table, {:temps, {1, temp, temp, temp}})

      :update_table_instead ->
        update_table(table, temp)
    end
  end

  def parse_line(line) do
    Parse.parse_line(line)
  end

  def update_table(station, temp) do
    temps =
      case :ets.lookup(station, :temps) do
        [] ->
          {1, temp, temp, temp}

        [{:temps, {len, min, max, mean}}] ->
          {len + 1, min(min, temp), max(max, temp), (mean * len + temp) / (len + 1)}
      end

    :ets.insert(station, {:temps, temps})
  end

  def run_file(filename, chunk_size \\ @chunk_size) do
    filename
    |> File.stream!(read_ahead: @blob_size)
    |> Stream.chunk_every(chunk_size)
    |> Task.async_stream(fn chunk -> process_chunk(chunk) end,
      max_concurrency: @pool_size,
      timeout: :infinity
    )
    |> Stream.run()
  end

  def print_tables(tables) do
    out =
      tables
      |> Enum.map(fn table -> format_table(table) end)
      |> Enum.join(",")

    IO.puts("{#{out}}")
  end

  def format_table(table) do
    [{:temps, {_len, min, max, mean}}] = :ets.lookup(table, :temps)

    Atom.to_string(table) <>
      "=" <>
      :erlang.float_to_binary(min, decimals: 1) <>
      "/" <>
      :erlang.float_to_binary(mean, decimals: 1) <>
      "/" <> :erlang.float_to_binary(max, decimals: 1)
  end

  def cleanup_tables(tables) do
    Worker.get_stations(tables)
    |> Enum.each(fn table -> :ets.delete(table) end)
  end

  def main(file, size \\ 1_000_000_000) do
    Worker.start_link(:stations)

    {uSec, :ok} =
      :timer.tc(fn ->
        run_file(file, :math.sqrt(size) |> ceil())
        print_tables(Worker.get_stations(:stations))
        :ok
      end)

    IO.puts("It took #{uSec / 1000} milliseconds")
    cleanup_tables(:stations)
  end
end

defmodule Parse do
  # Generates a function that takes binary strings and parses them in 2 parts:
  # - the first part can have max length of 100 bytes and is delimited by a semicolon.
  # - the second part is a float number that may be followed by a newline character.
  @compile {:inline, parse_line: 1}
  for x <- 1..100 do
    station_len = x * 8

    for i <- 0..99 do
      temp_int = Integer.to_string(i)

      for d <- 0..9 do
        temp_dec = <<d + ?0>>
        temp_str = "#{temp_int}.#{temp_dec}"

        def parse_line(
              <<station::bitstring-size(unquote(station_len)), ";", unquote(temp_str),
                _rest::binary>>
            ),
            do: {station, unquote(i) + unquote(d) / 10}

        def parse_line(
              <<station::bitstring-size(unquote(station_len)), ";-", unquote(temp_str),
                _rest::binary>>
            ),
            do: {station, -1 * (unquote(i) + unquote(d) / 10)}
      end
    end
  end
end

defmodule Worker do
  use GenServer

  def start_link(name) when is_atom(name) do
    GenServer.start_link(__MODULE__, :ordsets.new(), name: name)
  end

  def add_station(name, station) do
    GenServer.call(name, {:add_station, station})
  end

  def get_stations(name) do
    GenServer.call(name, :get_stations)
  end

  @impl true
  def init(state), do: {:ok, state}

  @impl true
  def handle_call({:add_station, station}, _from, state) do
    if :ordsets.is_element(station, state) do
      {:reply, :update_table_instead, state}
    else
      :ets.new(station, [:public, :named_table, write_concurrency: true])
      {:reply, :station_table_added, :ordsets.add_element(station, state)}
    end
  end

  @impl true
  def handle_call(:get_stations, _from, state), do: {:reply, state, state}
end
