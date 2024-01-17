defmodule Brc do
  @pool_size :erlang.system_info(:logical_processors)
  @blob_size 100_000
  @table :weather_stations
  @splitters [";", "\n"]
  @split_opts [:global, :trim_all]

  def process_stream(stream) do
    for line <- stream do
      process_line(line)
    end
  end

  def process_line(line) do
    [city, temp] = :binary.split(line, @splitters, @split_opts)

    {temp, ""} = temp |> Float.parse()

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
    :ets.new(@table, [:ordered_set, :public, :named_table])

    streams = File.stream!(filename, read_ahead: @blob_size) |> Stream.chunk_every(@blob_size)

    streams
    |> Task.async_stream(fn stream -> process_stream(stream) end,
      max_concurrency: @pool_size,
      timeout: :infinity
    )
    |> Stream.run()

    processed =
      :ets.tab2list(@table)
      |> Enum.map(fn {city, {_len, min, max, mean}} ->
        city <>
          "=" <>
          :erlang.float_to_binary(min, decimals: 1) <>
          "/" <>
          :erlang.float_to_binary(mean, decimals: 1) <>
          "/" <> :erlang.float_to_binary(max, decimals: 1)
      end)
      |> Enum.join(",")

    IO.puts("{#{processed}}")
  end

  def main(args) do
    {uSec, :ok} =
      :timer.tc(fn ->
        run_file(Enum.at(args, 0))
        :ok
      end)

    IO.puts("It took #{uSec / 1000} milliseconds")
  end
end
