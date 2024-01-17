defmodule Brc do
  @pool_size :erlang.system_info(:logical_processors)
  @blob_size 1024 * 1024
  @max_rows 50_000_000
  @chunk_size :math.sqrt(@max_rows) |> ceil()
  @table :weather_stations

  def process_stream(stream) do
    for line <- stream do
      process_line(line)
    end
  end

  def process_line(""), do: ""

  def process_line(<<city::208, ";", temp::binary>>),
    do: update_table(<<city::208>>, Float.parse(temp))

  def process_line(<<city::200, ";", temp::binary>>),
    do: update_table(<<city::200>>, Float.parse(temp))

  def process_line(<<city::192, ";", temp::binary>>),
    do: update_table(<<city::192>>, Float.parse(temp))

  def process_line(<<city::184, ";", temp::binary>>),
    do: update_table(<<city::184>>, Float.parse(temp))

  def process_line(<<city::176, ";", temp::binary>>),
    do: update_table(<<city::176>>, Float.parse(temp))

  def process_line(<<city::168, ";", temp::binary>>),
    do: update_table(<<city::168>>, Float.parse(temp))

  def process_line(<<city::160, ";", temp::binary>>),
    do: update_table(<<city::160>>, Float.parse(temp))

  def process_line(<<city::152, ";", temp::binary>>),
    do: update_table(<<city::152>>, Float.parse(temp))

  def process_line(<<city::144, ";", temp::binary>>),
    do: update_table(<<city::144>>, Float.parse(temp))

  def process_line(<<city::136, ";", temp::binary>>),
    do: update_table(<<city::136>>, Float.parse(temp))

  def process_line(<<city::128, ";", temp::binary>>),
    do: update_table(<<city::128>>, Float.parse(temp))

  def process_line(<<city::120, ";", temp::binary>>),
    do: update_table(<<city::120>>, Float.parse(temp))

  def process_line(<<city::112, ";", temp::binary>>),
    do: update_table(<<city::112>>, Float.parse(temp))

  def process_line(<<city::104, ";", temp::binary>>),
    do: update_table(<<city::104>>, Float.parse(temp))

  def process_line(<<city::96, ";", temp::binary>>),
    do: update_table(<<city::96>>, Float.parse(temp))

  def process_line(<<city::88, ";", temp::binary>>),
    do: update_table(<<city::88>>, Float.parse(temp))

  def process_line(<<city::80, ";", temp::binary>>),
    do: update_table(<<city::80>>, Float.parse(temp))

  def process_line(<<city::72, ";", temp::binary>>),
    do: update_table(<<city::72>>, Float.parse(temp))

  def process_line(<<city::64, ";", temp::binary>>),
    do: update_table(<<city::64>>, Float.parse(temp))

  def process_line(<<city::56, ";", temp::binary>>),
    do: update_table(<<city::56>>, Float.parse(temp))

  def process_line(<<city::48, ";", temp::binary>>),
    do: update_table(<<city::48>>, Float.parse(temp))

  def process_line(<<city::40, ";", temp::binary>>),
    do: update_table(<<city::40>>, Float.parse(temp))

  def process_line(<<city::32, ";", temp::binary>>),
    do: update_table(<<city::32>>, Float.parse(temp))

  def process_line(<<city::24, ";", temp::binary>>),
    do: update_table(<<city::24>>, Float.parse(temp))

  def update_table(city, {temp, _}) do
    temps =
      case :ets.lookup(@table, city) do
        [] ->
          {1, temp, temp, temp}

        [{^city, {len, min, max, mean}}] ->
          {len + 1, min(min, temp), max(max, temp), (mean * len + temp) / (len + 1)}
      end

    :ets.insert(@table, {city, temps})
  end

  def run_file(filename) do
    filename
    |> File.stream!(read_ahead: @blob_size)
    |> Stream.chunk_every(@chunk_size)
    |> Task.async_stream(fn stream -> process_stream(stream) end,
      max_concurrency: @pool_size,
      timeout: :infinity
    )
    |> Stream.run()
  end

  def print_table(table) do
    out =
      :ets.tab2list(table)
      |> Task.async_stream(fn {city, {_len, min, max, mean}} ->
        city <>
          "=" <>
          :erlang.float_to_binary(min, decimals: 1) <>
          "/" <>
          :erlang.float_to_binary(mean, decimals: 1) <>
          "/" <> :erlang.float_to_binary(max, decimals: 1)
      end)
      |> Enum.map(&elem(&1, 1))
      |> Enum.sort()
      |> Enum.join(",")

    IO.puts("{#{out}}")
  end

  def main(args) do
    :ets.new(@table, [:set, :public, :named_table, write_concurrency: true])

    {uSec, :ok} =
      :timer.tc(fn ->
        run_file(Enum.at(args, 0))
        print_table(@table)
        :ok
      end)

    IO.puts("It took #{uSec / 1000} milliseconds")
    :ets.delete(@table)
  end
end
