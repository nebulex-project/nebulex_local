defmodule Nebulex.Adapters.Local.Options do
  @moduledoc """
  Option definitions for the local adapter.
  """

  # Adapter option definitions
  adapter_opts = [
    backend: [
      type: {:in, [:ets, :shards]},
      required: false,
      default: :ets,
      doc: """
      The backend or storage to be used for the adapter.
      """
    ],
    read_concurrency: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      Since the adapter uses ETS tables internally, this option is when creating
      a new table or generation.

      See `:ets.new/2`.
      """
    ],
    write_concurrency: [
      type: :boolean,
      required: false,
      default: true,
      doc: """
      Since the adapter uses ETS tables internally, this option is when creating
      a new table or generation.

      See `:ets.new/2`.
      """
    ],
    compressed: [
      type: :boolean,
      required: false,
      default: false,
      doc: """
      Since the adapter uses ETS tables internally, this option is when creating
      a new table or generation.

      See `:ets.new/2`.
      """
    ],
    backend_type: [
      type: {:in, [:set, :ordered_set, :bag, :duplicate_bag]},
      required: false,
      default: :set,
      doc: """
      Since the adapter uses ETS tables internally, this option is when creating
      a new table or generation.

      See `:ets.new/2`.
      """
    ],
    partitions: [
      type: :pos_integer,
      required: false,
      doc: """
      Defines the number of partitions to use in case of using the `:shards`
      backend. See `:shards.new/2`.

      The default value is `System.schedulers_online()`.
      """
    ],
    purge_chunk_size: [
      type: :pos_integer,
      required: false,
      default: 100,
      doc: """
      This option is for limiting the max nested match specs based on number
      of keys when purging the older cache generation.
      """
    ],
    gc_interval: [
      type: :pos_integer,
      required: false,
      doc: """
      The interval time in milliseconds for garbage collection to run, delete
      the oldest generation, and create a new one. If not provided, the garbage
      collection is never executed, so new generations must be created
      explicitly, e.g., `MyCache.new_generation(opts)` (the default).
      """
    ],
    max_size: [
      type: :pos_integer,
      required: false,
      doc: """
      The max number of cached entries (cache limit). If not provided, the check
      to release memory is not performed (the default).
      """
    ],
    allocated_memory: [
      type: :pos_integer,
      required: false,
      doc: """
      The max size in bytes for the cache storage. If not provided, the check
      to release memory is not performed (the default).
      """
    ],
    gc_cleanup_min_timeout: [
      type: :pos_integer,
      required: false,
      default: :timer.seconds(10),
      doc: """
      The min timeout in milliseconds for triggering the next cleanup
      and memory check. It is the timeout to use when the max size or
      allocated memory reaches the limit.
      """
    ],
    gc_cleanup_max_timeout: [
      type: :pos_integer,
      required: false,
      default: :timer.minutes(10),
      doc: """
      The max timeout in milliseconds for triggering the next cleanup
      and memory check. It is the timeout to use when the cache starts,
      there are a few entries, or the consumed memory is near 0.
      """
    ],
    gc_flush_delay: [
      type: :pos_integer,
      required: false,
      default: :timer.seconds(10),
      doc: """
      The delay in milliseconds before objects from the oldest generation
      are flushed.
      """
    ]
  ]

  runtime_opts =
    [
      reset_timer: [
        type: :boolean,
        required: false,
        default: true,
        doc: """
        Whether the GC timer should be reset or not.
        """
      ]
    ] ++ adapter_opts

  # Adapter options schema
  @adapter_opts_schema NimbleOptions.new!(adapter_opts)

  # Runtime options schema
  @runtime_opts_schema NimbleOptions.new!(runtime_opts)

  ## Docs API

  # coveralls-ignore-start

  @spec adapter_options_docs() :: binary()
  def adapter_options_docs do
    NimbleOptions.docs(@adapter_opts_schema)
  end

  # coveralls-ignore-stop

  ## Validation API

  @spec validate_adapter_opts!(keyword()) :: keyword()
  def validate_adapter_opts!(opts) do
    adapter_opts =
      opts
      |> Keyword.take(Keyword.keys(@adapter_opts_schema.schema))
      |> NimbleOptions.validate!(@adapter_opts_schema)

    Keyword.merge(opts, adapter_opts)
  end

  @spec validate_runtime_opts!(keyword()) :: keyword()
  def validate_runtime_opts!(opts) do
    adapter_opts =
      opts
      |> Keyword.take(Keyword.keys(@runtime_opts_schema.schema))
      |> NimbleOptions.validate!(@runtime_opts_schema)

    Keyword.merge(opts, adapter_opts)
  end
end
