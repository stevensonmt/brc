# Brc

My attempt at the Billion Row Challenge in Elixir. Cloned from @rrcook but the actual code duplicated from their implementation is only the output printing in the `main` function. Consistently under 50 seconds for 50M rows on my 13yo i5 4-core machine at home.

To run, from the root of the project run `MIX_ENV=prod mix escript.build` to generate an executable in the root directory. Then `./brc /path/to/measurements.txt`.
