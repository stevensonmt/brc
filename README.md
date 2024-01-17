# Brc

My attempt at the Billion Row Challenge in Elixir. Cloned from @rrcook but the actual code duplicated from their implementation is only the output printing in the `main` function. Consistently under 50 seconds for 50M rows on my 13yo i5 4-core machine at home.

Run with `Brc.main(path_to_file, number_of_rows)` where the `number_of_rows` parameter is optional and defaults to 1_000_000_000. If doing smaller files (like 50M) performance will be worse with the default setting.
