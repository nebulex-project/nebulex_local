# Nebulex.Adapters.Local
> A generational local cache adapter for [Nebulex][Nebulex].

[Nebulex]: https://github.com/cabol/nebulex

![CI](https://github.com/elixir-nebulex/nebulex_adapters_local/workflows/CI/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/elixir-nebulex/nebulex_adapters_local/badge.svg?branch=main)](https://coveralls.io/github/elixir-nebulex/nebulex_adapters_local?branch=main)
[![Hex Version](https://img.shields.io/hexpm/v/nebulex_adapters_local.svg)](https://hex.pm/packages/nebulex_adapters_local)
[![Docs](https://img.shields.io/badge/docs-hexpm-blue.svg)](https://hexdocs.pm/nebulex_adapters_local)

See the [online documentation][online_docs] for more information.

[online_docs]: https://hexdocs.pm/nebulex_adapters_local/

**NOTE:** This adapter only supports Nebulex v3.0.0 onwards.

## Installation

Add `:nebulex_adapters_local` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:nebulex_adapters_local, "~> 3.0"}
  ]
end
```

## Usage

You can define a cache as follows:

```elixir
defmodule MyApp.LocalCache do
  use Nebulex.Cache,
    otp_app: :my_app,
    adapter: Nebulex.Adapters.Local
end
```

Where the configuration for the cache must be in your application
environment, usually defined in your `config/config.exs`:

```elixir
config :my_app, MyApp.LocalCache,
  gc_interval: :timer.hours(12),
  max_size: 1_000_000,
  allocated_memory: 2_000_000_000,
  gc_cleanup_min_timeout: :timer.seconds(10),
  gc_cleanup_max_timeout: :timer.minutes(10)
```

If your application was generated with a supervisor (by passing `--sup`
to `mix new`) you will have a `lib/my_app/application.ex` file containing
the application start callback that defines and starts your supervisor.
You just need to edit the `start/2` function to start the cache as a
supervisor on your application's supervisor:

```elixir
def start(_type, _args) do
  children = [
    {MyApp.LocalCache, []},
  ]

  ...
end
```

See the [online documentation][online_docs] for more information.

## Testing

Since `Nebulex.Adapters.Local` uses the support modules and shared tests
from `Nebulex` and by default its test folder is not included in the Hex
dependency, the following steps are required for running the tests.

First of all, make sure you set the environment variable `NEBULEX_PATH`
to `nebulex`:

```
export NEBULEX_PATH=nebulex
```

Second, make sure you fetch `:nebulex` dependency directly from GtiHub
by running:

```
mix nbx.setup
```

Third, fetch deps:

```
mix deps.get
```

Finally, you can run the tests:

```
mix test
```

Running tests with coverage:

```
mix coveralls.html
```

You will find the coverage report within `cover/excoveralls.html`.

## Benchmarks

The adapter provides a set of basic benchmark tests using the library
[benchee](https://github.com/PragTob/benchee), and they are located within
the directory [benchmarks](./benchmarks).

To run a benchmark test you have to run:

```
$ MIX_ENV=test mix run benchmarks/{BENCH_TEST_FILE}
```

Where `BENCH_TEST_FILE` can be any of:

  * `local_with_ets_bench.exs`: benchmark for the local adapter using
    `:ets` backend.
  * `local_with_shards_bench.exs`: benchmark for the local adapter using
    `:shards` backend.

For example, for running the benchmark for the local adapter using `:shards`
backend:

```
$ MIX_ENV=test mix run benchmarks/local_with_shards_bench.exs
```

Additionally, you can also run performance tests using `:basho_bench`.
See [nebulex_bench example](https://github.com/cabol/nebulex_examples/tree/master/nebulex_bench)
for more information.

## Contributing

Contributions to Nebulex are very welcome and appreciated!

Use the [issue tracker](https://github.com/elixir-nebulex/nebulex_adapters_local/issues)
for bug reports or feature requests. Open a
[pull request](https://github.com/elixir-nebulex/nebulex_adapters_local/pulls)
when you are ready to contribute.

When submitting a pull request you should not update the
[CHANGELOG.md](CHANGELOG.md), and also make sure you test your changes
thoroughly, include unit tests alongside new or changed code.

Before to submit a PR it is highly recommended to run `mix check` and ensure
all checks run successfully.

## Copyright and License

Copyright (c) 2023 Carlos Andres Bolaños R.A.

`Nebulex.Adapters.Local` source code is licensed under the [MIT License](LICENSE).
