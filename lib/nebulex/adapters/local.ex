defmodule Nebulex.Adapters.Local do
  @moduledoc """
  A Local Generation Cache adapter for Nebulex; inspired by
  [epocxy cache](https://github.com/duomark/epocxy/blob/master/src/cxy_cache.erl).

  Generational caching using an ETS table (or multiple ones when used with
  `:shards`) for each generation of cached data. Accesses hit the newer
  generation first, and migrate from the older generation to the newer
  generation when retrieved from the stale table. When a new generation
  is started, the oldest one is deleted. This is a form of mass garbage
  collection which avoids using timers and expiration of individual
  cached elements.

  This implementation of generation cache uses only two generations, referred
  to as the `newer` and the `older` generation.

  See `Nebulex.Adapters.Local.Generation` to learn more about generation
  management and garbage collection.

  ## Overall features

    * Configurable backend (`ets` or `:shards`).
    * Expiration - A status based on TTL (Time To Live) option. To maintain
      cache performance, expired entries may not be immediately removed or
      evicted, they are expired or evicted on-demand, when the key is read.
    * Eviction - [Generational Garbage Collection][gc].
    * Sharding - For intensive workloads, the Cache may also be partitioned
      (by using `:shards` backend and specifying the `:partitions` option).
    * Support for transactions via Erlang global name registration facility.
    * Support for stats.

  [gc]: http://hexdocs.pm/nebulex/Nebulex.Adapters.Local.Generation.html

  ## Configuration options

  The following options can be used to configure the adapter:

  #{Nebulex.Adapters.Local.Options.adapter_options_docs()}

  > #### Memory check for `:max_size` and `:allocated_memory` options {: .info}
  > The memory check validates whether the cache has reached the memory
  > limit (either the `:max_size` or `:allocated_memory`). If so, the GC
  > creates a new generation, triggering the deletion of the older one
  > and releasing memory space.

  ## Usage

  `Nebulex.Cache` is the wrapper around the cache. We can define a
  local cache as follows:

      defmodule MyApp.LocalCache do
        use Nebulex.Cache,
          otp_app: :my_app,
          adapter: Nebulex.Adapters.Local
      end

  Where the configuration for the cache must be in your application
  environment, usually defined in your `config/config.exs`:

      config :my_app, MyApp.LocalCache,
        gc_interval: :timer.hours(12),
        max_size: 1_000_000,
        allocated_memory: 2_000_000_000,
        gc_cleanup_min_timeout: :timer.seconds(10),
        gc_cleanup_max_timeout: :timer.minutes(10)

  For intensive workloads, the Cache may also be partitioned using `:shards`
  as cache backend (`backend: :shards`) and configuring the desired number of
  partitions via the `:partitions` option. Defaults to
  `System.schedulers_online()`.

      config :my_app, MyApp.LocalCache,
        gc_interval: :timer.hours(12),
        max_size: 1_000_000,
        allocated_memory: 2_000_000_000,
        gc_cleanup_min_timeout: :timer.seconds(10),
        gc_cleanup_max_timeout: :timer.minutes(10),
        backend: :shards,
        partitions: System.schedulers_online() * 2

  If your application was generated with a supervisor (by passing `--sup`
  to `mix new`) you will have a `lib/my_app/application.ex` file containing
  the application start callback that defines and starts your supervisor.
  You just need to edit the `start/2` function to start the cache as a
  supervisor on your application's supervisor:

      def start(_type, _args) do
        children = [
          {MyApp.LocalCache, []},
          ...
        ]

  See `Nebulex.Cache` for more information.

  ## Eviction configuration

  This section helps you understand how the different configuration options work
  and gives you an idea of what values to set, especially if this is your first
  time using Nebulex.

  ### `:ttl` option

  The `:ttl` is a runtime option meant to set a key's expiration time. It is
  evaluated on-demand when a key is retrieved, and if it has expired, it is
  removed from the cache. Hence, it can not be used as an eviction method;
  it is more for maintaining the cache's integrity and consistency. For this
  reason, you should always configure the eviction or GC options. See the
  ["Garbage collection options"](#module-garbage-collection options) for more
  information.

  ### Caveats when using `:ttl` option:

    * When using the `:ttl` option, ensure it is less than `:gc_interval`.
      Otherwise, the key may be evicted, and the `:ttl` hasn't happened yet
      because the garbage collector may run before a fetch operation has
      evaluated the `:ttl` and expired the key.
    * Consider the following scenario based on the previous caveat. You have
      `:gc_interval` set to 1 hrs. Then you put a new key with `:ttl` set to
      2 hrs. One minute later, the GC runs, creating a new generation, and the
      key ends up in the older generation. Therefore, if the next GC cycle
      occurs (1 hr later) before the key is fetched (moving it to the newer
      generation), it is evicted from the cache when the GC removes the older
      generation so it won't be retrievable anymore.

  ### Garbage collection options

  This adapter implements a generational cache, which means its main eviction
  mechanism is pushing a new cache generation and remove the oldest one. In
  this way, we ensure only the most frequently used keys are always available
  in the newer generation and the the least frequently used are evicted when
  the garbage collector runs, and the garbage collector is triggered upon
  these conditions:

    * When the time interval defined by `:gc_interval` is completed.
      This makes the garbage-collector process to run creating a new
      generation and forcing to delete the oldest one.
    * When the "cleanup" timeout expires, and then the limits `:max_size`
      and `:allocated_memory` are checked, if one of those is reached,
      then the garbage collector runs (a new generation is created and
      the oldest one is deleted). The cleanup timeout is controlled by
      `:gc_cleanup_min_timeout` and `:gc_cleanup_max_timeout`, it works
      with an inverse linear backoff, which means the timeout is inverse
      proportional to the memory growth; the bigger the cache size is,
      the shorter the cleanup timeout will be.

  ### First-time configuration

  For configuring the cache with accurate and/or good values it is important
  to know several things in advance, like for example the size of an entry
  in average so we can calculate a good value for max size and/or allocated
  memory, how intensive will be the load in terms of reads and writes, etc.
  The problem is most of these aspects are unknown when it is a new app or
  we are using the cache for the first time. Therefore, the following
  recommendations will help you to configure the cache for the first time:

    * When configuring the `:gc_interval`, think about how that often the
      least frequently used entries should be evicted, or what is the desired
      retention period for the cached entries. For example, if `:gc_interval`
      is set to 1 hr, it means you will keep in cache only those entries that
      are retrieved periodically within a 2 hr period; `gc_interval * 2`,
      being 2 the number of generations. Longer than that, the GC will
      ensure is always evicted (the oldest generation is always deleted).
      If it is the first time using Nebulex, perhaps you can start with
      `gc_interval: :timer.hours(12)` (12 hrs), so the max retention
      period for the keys will be 1 day; but ensure you also set either the
      `:max_size` or `:allocated_memory`.
    * It is highly recommended to set either `:max_size` or `:allocated_memory`
      to ensure the oldest generation is deleted (least frequently used keys
      are evicted) when one of these limits is reached and also to avoid
      running out of memory. For example, for the `:allocated_memory` we can
      set 25% of the total memory, and for the `:max_size` something between
      `100_000` and `1_000_000`.
    * For `:gc_cleanup_min_timeout` we can set `10_000`, which means when the
      cache is reaching the size or memory limit, the polling period for the
      cleanup process will be 10 seconds. And for `:gc_cleanup_max_timeout`
      we can set `600_000`, which means when the cache is almost empty the
      polling period will be close to 10 minutes.

  ## Stats

  This adapter does support stats by using the default implementation
  provided by `Nebulex.Adapter.Stats`. The adapter also uses the
  `Nebulex.Telemetry.StatsHandler` to aggregate the stats and keep
  them updated. Therefore, it requires the Telemetry events are emitted
  by the adapter (the `:telemetry` option should not be set to `false`
  so the Telemetry events can be dispatched), otherwise, stats won't
  work properly.

  ## Queryable API

  Since this adapter is implemented on top of ETS tables, the query must be a
  valid [**"ETS Match Spec"**](https://www.erlang.org/doc/man/ets#match_spec).
  However, there are some predefined or shorthand queries you can use. See the
  section "Predefined queries" below for information.

  Internally, an entry is represented by the tuple
  `{:entry, key, value, touched, ttl}`, which means the match pattern within
  the `:ets.match_spec()` must be something like:
  `{:entry, :"$1", :"$2", :"$3", :"$4"}`.
  In order to make query building easier, you can use `Ex2ms` library.

  For match-spec queries, it is required to understand the adapter's entry
  structure, which is `{:entry, key, value, touched, ttl}`. Hence, one may
  write the following query:

      iex> match_spec = [
      ...>   {
      ...>     {:entry, :"$1", :"$2", :_, :_},
      ...>     [{:>, :"$2", 1}],
      ...>     [{{:"$1", :"$2"}}]
      ...>   }
      ...> ]
      iex> MyCache.get_all(query: match_spec)
      {:ok, [b: 1, c: 3]}

  > You can use the `Ex2ms` or `MatchSpec` library to build queries easier.

  ### Predefined queries

    * `:expired` - Matches all expired entries.

  Querying expired entries:

      iex> {:ok, expired} = MyCache.get_all(query: :expired)

  ## Extended API (convenience functions)

  This adapter provides some additional convenience functions to the
  `Nebulex.Cache` API.

  Creating new generations:

      MyCache.new_generation()
      MyCache.new_generation(reset_timer: false)

  Retrieving the current generations:

      MyCache.generations()

  Retrieving the newer generation:

      MyCache.newer_generation()

  """

  # Provide Cache Implementation
  @behaviour Nebulex.Adapter
  @behaviour Nebulex.Adapter.KV
  @behaviour Nebulex.Adapter.Queryable
  @behaviour Nebulex.Adapter.Persistence

  # Inherit default transaction implementation
  use Nebulex.Adapter.Transaction

  # Inherit default info implementation
  use Nebulex.Adapters.Common.Info

  import Nebulex.Utils
  import Record

  alias Nebulex.Adapters.Common.Info.Stats
  alias Nebulex.Adapters.Local.{Backend, Generation, Metadata}
  alias Nebulex.Time

  # Cache Entry
  defrecord(:entry,
    key: nil,
    value: nil,
    touched: nil,
    exp: nil
  )

  ## Nebulex.Adapter

  @impl true
  defmacro __before_compile__(_env) do
    quote do
      @doc """
      A convenience function for creating new generations.
      """
      def new_generation(opts \\ []) do
        Generation.new(get_dynamic_cache(), opts)
      end

      @doc """
      A convenience function for reset the GC timer.
      """
      def reset_generation_timer do
        Generation.reset_timer(get_dynamic_cache())
      end

      @doc """
      A convenience function for retrieving the current generations.
      """
      def generations do
        Generation.list(get_dynamic_cache())
      end

      @doc """
      A convenience function for retrieving the newer generation.
      """
      def newer_generation do
        Generation.newer(get_dynamic_cache())
      end
    end
  end

  @impl true
  def init(opts) do
    # Validate options
    opts = __MODULE__.Options.validate_adapter_opts!(opts)

    # Required options
    cache = Keyword.fetch!(opts, :cache)
    telemetry = Keyword.fetch!(opts, :telemetry)
    telemetry_prefix = Keyword.fetch!(opts, :telemetry_prefix)

    # Init internal metadata table
    meta_tab = opts[:meta_tab] || Metadata.init()

    # Init stats_counter
    stats_counter =
      if Keyword.get(opts, :stats, true) == true do
        Stats.init(telemetry_prefix)
      end

    # Resolve the backend to be used
    backend = Keyword.fetch!(opts, :backend)

    # Internal option for max nested match specs based on number of keys
    purge_chunk_size = Keyword.fetch!(opts, :purge_chunk_size)

    # Build adapter metadata
    adapter_meta = %{
      cache: cache,
      name: opts[:name] || cache,
      telemetry: telemetry,
      telemetry_prefix: telemetry_prefix,
      meta_tab: meta_tab,
      stats_counter: stats_counter,
      backend: backend,
      purge_chunk_size: purge_chunk_size,
      started_at: DateTime.utc_now()
    }

    # Build adapter child_spec
    child_spec = Backend.child_spec(backend, [adapter_meta: adapter_meta] ++ opts)

    {:ok, child_spec, adapter_meta}
  end

  ## Nebulex.Adapter.KV

  @impl true
  def fetch(adapter_meta, key, _opts) do
    adapter_meta.meta_tab
    |> list_gen()
    |> do_fetch(key, adapter_meta)
    |> return(:value)
  end

  defp do_fetch([newer], key, adapter_meta) do
    fetch_entry(newer, key, adapter_meta)
  end

  defp do_fetch([newer, older], key, adapter_meta) do
    with {:error, _} <- fetch_entry(newer, key, adapter_meta),
         {:ok, cached} <- pop_entry(older, key, adapter_meta) do
      true = adapter_meta.backend.insert(newer, cached)

      {:ok, cached}
    end
  end

  @impl true
  def put(adapter_meta, key, value, ttl, on_write, _opts) do
    now = Time.now()
    entry = entry(key: key, value: value, touched: now, exp: exp(now, ttl))

    wrap_ok do_put(on_write, adapter_meta.meta_tab, adapter_meta.backend, entry)
  end

  defp do_put(:put, meta_tab, backend, entry) do
    put_entries(meta_tab, backend, entry)
  end

  defp do_put(:put_new, meta_tab, backend, entry) do
    put_new_entries(meta_tab, backend, entry)
  end

  defp do_put(:replace, meta_tab, backend, entry(key: key, value: value)) do
    update_entry(meta_tab, backend, key, [{3, value}])
  end

  @impl true
  def put_all(adapter_meta, entries, ttl, on_write, _opts) do
    now = Time.now()
    exp = exp(now, ttl)

    entries =
      for {key, value} <- entries do
        entry(key: key, value: value, touched: now, exp: exp)
      end

    do_put_all(
      on_write,
      adapter_meta.meta_tab,
      adapter_meta.backend,
      adapter_meta.purge_chunk_size,
      entries
    )
    |> wrap_ok()
  end

  defp do_put_all(:put, meta_tab, backend, chunk_size, entries) do
    put_entries(meta_tab, backend, entries, chunk_size)
  end

  defp do_put_all(:put_new, meta_tab, backend, chunk_size, entries) do
    put_new_entries(meta_tab, backend, entries, chunk_size)
  end

  @impl true
  def delete(adapter_meta, key, _opts) do
    adapter_meta.meta_tab
    |> list_gen()
    |> Enum.each(&adapter_meta.backend.delete(&1, key))
  end

  @impl true
  def take(adapter_meta, key, _opts) do
    adapter_meta.meta_tab
    |> list_gen()
    |> Enum.reduce_while(nil, fn gen, _acc ->
      case pop_entry(gen, key, adapter_meta) do
        {:ok, res} -> {:halt, return({:ok, res}, :value)}
        error -> {:cont, error}
      end
    end)
  end

  @impl true
  def update_counter(adapter_meta, key, amount, ttl, default, _opts) do
    # Current time
    now = Time.now()

    # Get needed metadata
    meta_tab = adapter_meta.meta_tab
    backend = adapter_meta.backend

    # Verify if the key has expired
    _ =
      meta_tab
      |> list_gen()
      |> do_fetch(key, adapter_meta)

    # Run the counter operation
    meta_tab
    |> newer_gen()
    |> backend.update_counter(
      key,
      {3, amount},
      entry(key: key, value: default, touched: now, exp: exp(now, ttl))
    )
    |> wrap_ok()
  end

  @impl true
  def has_key?(adapter_meta, key, _opts) do
    case fetch(adapter_meta, key, []) do
      {:ok, _} -> {:ok, true}
      {:error, _} -> {:ok, false}
    end
  end

  @impl true
  def ttl(adapter_meta, key, _opts) do
    with {:ok, res} <- adapter_meta.meta_tab |> list_gen() |> do_fetch(key, adapter_meta) do
      {:ok, entry_ttl(res)}
    end
  end

  defp entry_ttl(entry(exp: :infinity)), do: :infinity

  defp entry_ttl(entry(exp: exp)) do
    exp - Time.now()
  end

  defp entry_ttl(entries) when is_list(entries) do
    Enum.map(entries, &entry_ttl/1)
  end

  @impl true
  def expire(adapter_meta, key, ttl, _opts) do
    now = Time.now()

    adapter_meta.meta_tab
    |> update_entry(adapter_meta.backend, key, [{4, now}, {5, exp(now, ttl)}])
    |> wrap_ok()
  end

  @impl true
  def touch(adapter_meta, key, _opts) do
    adapter_meta.meta_tab
    |> update_entry(adapter_meta.backend, key, [{4, Time.now()}])
    |> wrap_ok()
  end

  ## Nebulex.Adapter.Queryable

  @impl true
  def execute(adapter_meta, query_meta, opts)

  def execute(_adapter_meta, %{op: :get_all, query: {:in, []}}, _opts) do
    {:ok, []}
  end

  def execute(_adapter_meta, %{op: op, query: {:in, []}}, _opts)
      when op in [:count_all, :delete_all] do
    {:ok, 0}
  end

  def execute(
        %{meta_tab: meta_tab, backend: backend},
        %{op: :count_all, query: {:q, nil}},
        _opts
      ) do
    meta_tab
    |> list_gen()
    |> Enum.reduce(0, fn gen, acc ->
      gen
      |> backend.info(:size)
      |> Kernel.+(acc)
    end)
    |> wrap_ok()
  end

  def execute(
        %{meta_tab: meta_tab} = adapter_meta,
        %{op: :delete_all, query: {:q, nil}} = query_spec,
        _opts
      ) do
    with {:ok, count_all} <- execute(adapter_meta, %{query_spec | op: :count_all}, []) do
      :ok = Generation.delete_all(meta_tab)

      {:ok, count_all}
    end
  end

  def execute(
        %{meta_tab: meta_tab, backend: backend},
        %{op: :count_all, query: {:in, keys}},
        opts
      )
      when is_list(keys) do
    chunk_size = Keyword.get(opts, :chunk_size, 10)

    meta_tab
    |> list_gen()
    |> Enum.reduce(0, fn gen, acc ->
      do_count_all(backend, gen, keys, chunk_size) + acc
    end)
    |> wrap_ok()
  end

  def execute(
        %{meta_tab: meta_tab, backend: backend},
        %{op: :delete_all, query: {:in, keys}},
        opts
      )
      when is_list(keys) do
    chunk_size = Keyword.get(opts, :chunk_size, 10)

    meta_tab
    |> list_gen()
    |> Enum.reduce(0, fn gen, acc ->
      do_delete_all(backend, gen, keys, chunk_size) + acc
    end)
    |> wrap_ok()
  end

  def execute(
        %{meta_tab: meta_tab, backend: backend},
        %{op: :get_all, query: {:in, keys}, select: select},
        opts
      )
      when is_list(keys) do
    chunk_size = Keyword.get(opts, :chunk_size, 10)

    meta_tab
    |> list_gen()
    |> Enum.reduce([], fn gen, acc ->
      do_get_all(backend, gen, keys, match_return(select), chunk_size) ++ acc
    end)
    |> wrap_ok()
  end

  def execute(
        %{meta_tab: meta_tab, backend: backend},
        %{op: op, query: {:q, ms}, select: select},
        _opts
      ) do
    ms =
      ms
      |> assert_match_spec(select)
      |> maybe_match_spec_return_true(op)

    {reducer, acc_in} =
      case op do
        :get_all -> {&(backend.select(&1, ms) ++ &2), []}
        :count_all -> {&(backend.select_count(&1, ms) + &2), 0}
        :delete_all -> {&(backend.select_delete(&1, ms) + &2), 0}
      end

    meta_tab
    |> list_gen()
    |> Enum.reduce(acc_in, reducer)
    |> wrap_ok()
  end

  @impl true
  def stream(adapter_meta, query_meta, opts)

  def stream(_adapter_meta, %{query: {:in, []}}, _opts) do
    {:ok, Stream.map([], & &1)}
  end

  def stream(%{meta_tab: meta_tab, backend: backend}, %{query: {:in, keys}, select: select}, opts) do
    keys
    |> Stream.chunk_every(Keyword.fetch!(opts, :max_entries))
    |> Stream.map(fn chunk ->
      meta_tab
      |> list_gen()
      |> Enum.reduce([], &(backend.select(&1, in_match_spec(chunk, select)) ++ &2))
    end)
    |> wrap_ok()
  end

  def stream(adapter_meta, %{query: {:q, ms}, select: select}, opts) do
    adapter_meta
    |> do_stream(
      assert_match_spec(ms, select),
      Keyword.get(opts, :max_entries, 20)
    )
    |> wrap_ok()
  end

  defp do_stream(%{meta_tab: meta_tab, backend: backend}, match_spec, page_size) do
    Stream.resource(
      fn ->
        [newer | _] = generations = list_gen(meta_tab)

        {backend.select(newer, match_spec, page_size), generations}
      end,
      fn
        {:"$end_of_table", [_gen]} ->
          {:halt, []}

        {:"$end_of_table", [_gen | generations]} ->
          result =
            generations
            |> hd()
            |> backend.select(match_spec, page_size)

          {[], {result, generations}}

        {{elements, cont}, [_ | _] = generations} ->
          {[elements], {backend.select(cont), generations}}
      end,
      & &1
    )
  end

  ## Nebulex.Adapter.Persistence

  @impl true
  def dump(%{cache: cache}, path, opts) do
    with_file(cache, path, [:write], fn io_dev ->
      with {:ok, stream} <- cache.stream() do
        stream
        |> Stream.chunk_every(Keyword.get(opts, :entries_per_line, 10))
        |> Enum.each(fn chunk ->
          bin =
            chunk
            |> :erlang.term_to_binary(get_compression(opts))
            |> Base.encode64()

          :ok = IO.puts(io_dev, bin)
        end)
      end
    end)
  end

  # sobelow_skip ["Misc.BinToTerm"]
  @impl true
  def load(%{cache: cache}, path, opts) do
    with_file(cache, path, [:read], fn io_dev ->
      io_dev
      |> IO.stream(:line)
      |> Stream.map(&String.trim/1)
      |> Enum.each(fn line ->
        entries =
          line
          |> Base.decode64!()
          |> :erlang.binary_to_term([:safe])

        cache.put_all(entries, opts)
      end)
    end)
  end

  # sobelow_skip ["Traversal.FileModule"]
  defp with_file(cache, path, modes, function) do
    case File.open(path, modes) do
      {:ok, io_device} ->
        try do
          function.(io_device)
        after
          :ok = File.close(io_device)
        end

      {:error, reason} ->
        reason = %File.Error{reason: reason, action: "open", path: path}

        wrap_error Nebulex.Error, reason: reason, cache: cache
    end
  end

  defp get_compression(opts) do
    case Keyword.get(opts, :compression) do
      value when is_integer(value) and value >= 0 and value < 10 ->
        [compressed: value]

      _ ->
        [:compressed]
    end
  end

  ## Nebulex.Adapter.Info

  @impl true
  def info(adapter_meta, spec, opts)

  def info(%{meta_tab: meta_tab} = adapter_meta, :all, opts) do
    with {:ok, base_info} <- super(adapter_meta, :all, opts) do
      {:ok, Map.merge(base_info, %{memory: memory_info(meta_tab)})}
    end
  end

  def info(%{meta_tab: meta_tab}, :memory, _opts) do
    {:ok, memory_info(meta_tab)}
  end

  def info(adapter_meta, spec, opts) when is_list(spec) do
    Enum.reduce(spec, {:ok, %{}}, fn s, {:ok, acc} ->
      {:ok, info} = info(adapter_meta, s, opts)

      {:ok, Map.put(acc, s, info)}
    end)
  end

  def info(adapter_meta, spec, opts) do
    super(adapter_meta, spec, opts)
  end

  defp memory_info(meta_tab) do
    {mem_size, max_size} = Generation.memory_info(meta_tab)

    %{total: max_size, used: mem_size}
  end

  ## Helpers

  # Inline common instructions
  @compile {:inline, list_gen: 1, newer_gen: 1, fetch_entry: 3, pop_entry: 3, match_key: 2}

  defmacrop backend_call(adapter_meta, fun, tab, key) do
    quote do
      case unquote(adapter_meta).backend.unquote(fun)(unquote(tab), unquote(key)) do
        [] ->
          wrap_error Nebulex.KeyError, key: unquote(key), cache: unquote(adapter_meta).name

        [entry(exp: :infinity) = entry] ->
          {:ok, entry}

        [entry() = entry] ->
          validate_exp(entry, unquote(tab), unquote(adapter_meta))

        entries when is_list(entries) ->
          now = Time.now()

          entries =
            for entry(touched: touched, exp: exp) = e <- entries, now < exp, do: e

          {:ok, entries}
      end
    end
  end

  defp exp(_now, :infinity), do: :infinity
  defp exp(now, ttl), do: now + ttl

  defp list_gen(meta_tab) do
    Metadata.fetch!(meta_tab, :generations)
  end

  defp newer_gen(meta_tab) do
    meta_tab
    |> Metadata.fetch!(:generations)
    |> hd()
  end

  defp validate_exp(entry(key: key, exp: exp) = entry, tab, adapter_meta) do
    if Time.now() >= exp do
      true = adapter_meta.backend.delete(tab, key)

      wrap_error Nebulex.KeyError, key: key, cache: adapter_meta.name, reason: :expired
    else
      {:ok, entry}
    end
  end

  defp fetch_entry(tab, key, adapter_meta) do
    backend_call(adapter_meta, :lookup, tab, key)
  end

  defp pop_entry(tab, key, adapter_meta) do
    backend_call(adapter_meta, :take, tab, key)
  end

  defp put_entries(meta_tab, backend, entries, chunk_size \\ 0)

  defp put_entries(
         meta_tab,
         backend,
         entry(key: key, value: val, touched: touched, exp: exp) = entry,
         _chunk_size
       ) do
    case list_gen(meta_tab) do
      [newer_gen] ->
        backend.insert(newer_gen, entry)

      [newer_gen, older_gen] ->
        changes = [{3, val}, {4, touched}, {5, exp}]

        with false <- backend.update_element(newer_gen, key, changes) do
          true = backend.delete(older_gen, key)

          backend.insert(newer_gen, entry)
        end
    end
  end

  defp put_entries(meta_tab, backend, entries, chunk_size) when is_list(entries) do
    do_put_entries(meta_tab, backend, entries, fn older_gen ->
      keys = Enum.map(entries, fn entry(key: key) -> key end)

      do_delete_all(backend, older_gen, keys, chunk_size)
    end)
  end

  defp do_put_entries(meta_tab, backend, entry_or_entries, purge_fun) do
    case list_gen(meta_tab) do
      [newer_gen] ->
        backend.insert(newer_gen, entry_or_entries)

      [newer_gen, older_gen] ->
        _ = purge_fun.(older_gen)

        backend.insert(newer_gen, entry_or_entries)
    end
  end

  defp put_new_entries(meta_tab, backend, entries, chunk_size \\ 0)

  defp put_new_entries(meta_tab, backend, entry(key: key) = entry, _chunk_size) do
    do_put_new_entries(meta_tab, backend, entry, fn newer_gen, older_gen ->
      with true <- backend.insert_new(older_gen, entry) do
        true = backend.delete(older_gen, key)

        backend.insert_new(newer_gen, entry)
      end
    end)
  end

  defp put_new_entries(meta_tab, backend, entries, chunk_size) when is_list(entries) do
    do_put_new_entries(meta_tab, backend, entries, fn newer_gen, older_gen ->
      with true <- backend.insert_new(older_gen, entries) do
        keys = Enum.map(entries, fn entry(key: key) -> key end)

        _ = do_delete_all(backend, older_gen, keys, chunk_size)

        backend.insert_new(newer_gen, entries)
      end
    end)
  end

  defp do_put_new_entries(meta_tab, backend, entry_or_entries, purge_fun) do
    case list_gen(meta_tab) do
      [newer_gen] ->
        backend.insert_new(newer_gen, entry_or_entries)

      [newer_gen, older_gen] ->
        purge_fun.(newer_gen, older_gen)
    end
  end

  defp update_entry(meta_tab, backend, key, updates) do
    case list_gen(meta_tab) do
      [newer_gen] ->
        backend.update_element(newer_gen, key, updates)

      [newer_gen, older_gen] ->
        with false <- backend.update_element(newer_gen, key, updates),
             [entry() = entry] <- backend.take(older_gen, key) do
          entry =
            Enum.reduce(updates, entry, fn
              {3, value}, acc -> entry(acc, value: value)
              {4, value}, acc -> entry(acc, touched: value)
              {5, value}, acc -> entry(acc, exp: value)
            end)

          backend.insert(newer_gen, entry)
        else
          [] -> false
          other -> other
        end
    end
  end

  defp do_count_all(backend, tab, keys, chunk_size) do
    ets_select_keys(
      keys,
      chunk_size,
      0,
      &new_match_spec/1,
      &backend.select_count(tab, &1),
      &(&1 + &2)
    )
  end

  defp do_delete_all(backend, tab, keys, chunk_size) do
    ets_select_keys(
      keys,
      chunk_size,
      0,
      &new_match_spec/1,
      &backend.select_delete(tab, &1),
      &(&1 + &2)
    )
  end

  defp do_get_all(backend, tab, keys, select, chunk_size) do
    ets_select_keys(
      keys,
      chunk_size,
      [],
      &new_match_spec(&1, select),
      &backend.select(tab, &1),
      &Kernel.++/2
    )
  end

  defp ets_select_keys([k], chunk_size, acc, ms_fun, chunk_fun, after_fun) do
    k = if is_tuple(k), do: tuple_to_match_spec(k), else: k

    ets_select_keys(
      [],
      2,
      chunk_size,
      match_key(k, Time.now()),
      acc,
      ms_fun,
      chunk_fun,
      after_fun
    )
  end

  defp ets_select_keys([k1, k2 | keys], chunk_size, acc, ms_fun, chunk_fun, after_fun) do
    k1 = if is_tuple(k1), do: tuple_to_match_spec(k1), else: k1
    k2 = if is_tuple(k2), do: tuple_to_match_spec(k2), else: k2
    now = Time.now()

    ets_select_keys(
      keys,
      2,
      chunk_size,
      {:orelse, match_key(k1, now), match_key(k2, now)},
      acc,
      ms_fun,
      chunk_fun,
      after_fun
    )
  end

  defp ets_select_keys([], _count, _chunk_size, chunk_acc, acc, ms_fun, chunk_fun, after_fun) do
    chunk_acc
    |> ms_fun.()
    |> chunk_fun.()
    |> after_fun.(acc)
  end

  defp ets_select_keys(keys, count, chunk_size, chunk_acc, acc, ms_fun, chunk_fun, after_fun)
       when count >= chunk_size do
    acc =
      chunk_acc
      |> ms_fun.()
      |> chunk_fun.()
      |> after_fun.(acc)

    ets_select_keys(keys, chunk_size, acc, ms_fun, chunk_fun, after_fun)
  end

  defp ets_select_keys([k | keys], count, chunk_size, chunk_acc, acc, ms_fun, chunk_fun, after_fun) do
    k = if is_tuple(k), do: tuple_to_match_spec(k), else: k

    ets_select_keys(
      keys,
      count + 1,
      chunk_size,
      {:orelse, chunk_acc, match_key(k, Time.now())},
      acc,
      ms_fun,
      chunk_fun,
      after_fun
    )
  end

  defp tuple_to_match_spec(data) do
    data
    |> :erlang.tuple_to_list()
    |> tuple_to_match_spec([])
  end

  defp tuple_to_match_spec([], acc) do
    {acc |> Enum.reverse() |> :erlang.list_to_tuple()}
  end

  defp tuple_to_match_spec([e | tail], acc) do
    e = if is_tuple(e), do: tuple_to_match_spec(e), else: e

    tuple_to_match_spec(tail, [e | acc])
  end

  defp return({:ok, entry(value: value)}, :value) do
    {:ok, value}
  end

  defp return({:ok, entries}, :value) when is_list(entries) do
    {:ok, for(entry(value: value) <- entries, do: value)}
  end

  defp return(other, _field) do
    other
  end

  defp assert_match_spec(spec, select) when spec in [nil, :expired] do
    [
      {
        entry(key: :"$1", value: :"$2", touched: :"$3", exp: :"$4"),
        [comp_match_spec(spec)],
        [match_return(select)]
      }
    ]
  end

  defp assert_match_spec(spec, _select) do
    case :ets.test_ms(test_ms(), spec) do
      {:ok, _result} ->
        spec

      {:error, _result} ->
        msg = """
        invalid query, expected one of:

        - `nil` - match all entries
        - `:expired` - match only expired entries (available for delete_all)
        - `:ets.match_spec()` - ETS match spec

        but got:

        #{inspect(spec, pretty: true)}
        """

        raise Nebulex.QueryError, message: msg, query: spec
    end
  end

  defp comp_match_spec(nil) do
    {:orelse, {:"=:=", :"$4", :infinity}, {:<, Time.now(), :"$4"}}
  end

  defp comp_match_spec(:expired) do
    {:not, comp_match_spec(nil)}
  end

  defp maybe_match_spec_return_true([{pattern, conds, _ret}], op)
       when op in [:delete_all, :count_all] do
    [{pattern, conds, [true]}]
  end

  defp maybe_match_spec_return_true(match_spec, _op) do
    match_spec
  end

  defp in_match_spec([k1, k2 | keys], select) do
    now = Time.now()

    keys
    |> Enum.reduce(
      {:orelse, match_key(k1, now), match_key(k2, now)},
      &{:orelse, &2, match_key(&1, now)}
    )
    |> new_match_spec(match_return(select))
  end

  defp new_match_spec(conds, return \\ true) do
    [
      {
        entry(key: :"$1", value: :"$2", touched: :"$3", exp: :"$4"),
        [conds],
        [return]
      }
    ]
  end

  defp match_return(select) do
    case select do
      :key -> :"$1"
      :value -> :"$2"
      {:key, :value} -> {{:"$1", :"$2"}}
    end
  end

  defp match_key(k, now) do
    {:andalso, {:"=:=", :"$1", k}, {:orelse, {:"=:=", :"$4", :infinity}, {:<, now, :"$4"}}}
  end

  defp test_ms, do: entry(key: 1, value: 1, touched: Time.now(), exp: 1000)
end
