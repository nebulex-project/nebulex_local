defmodule Nebulex.Adapters.Local.Generation do
  @moduledoc """
  This module implements the generation manager and garbage collector.

  The generational garbage collector manages the heap as several sub-heaps,
  known as generations, based on the age of the objects. An object is allocated
  in the youngest generation, sometimes called the nursery, and is promoted to
  an older generation if its lifetime exceeds the threshold of its current
  generation (defined by option `:gc_interval`). Every time the GC runs, a new
  cache generation is created, and the oldest one is deleted.

  The GC is triggered upon the following events:

    * When the `:gc_interval` times outs.
    * When the scheduled memory check times out. The memory check is executed
      when the `:max_size` or `:allocated_memory` options (or both) are
      configured. Furthermore, the memory check timeout is determined based on
      the options `gc_cleanup_min_timeout` and `gc_cleanup_max_timeout`.

  The oldest generation is deleted in two steps. First, the underlying ETS table
  is flushed to release space and only marked for deletion as there may still be
  processes referencing it. The actual deletion of the ETS table happens at the
  next GC run. However, flushing is a blocking operation. Once started,
  processes wanting to access the table must wait until it finishes.
  To circumvent this, the flush can be delayed by configuring `:gc_flush_delay`
  to allow time for these processes to complete their work without being
  blocked.
  """

  # State
  defstruct [
    :cache,
    :name,
    :telemetry,
    :telemetry_prefix,
    :meta_tab,
    :backend,
    :backend_opts,
    :stats_counter,
    :gc_interval,
    :gc_heartbeat_ref,
    :max_size,
    :allocated_memory,
    :gc_cleanup_min_timeout,
    :gc_cleanup_max_timeout,
    :gc_cleanup_ref,
    :gc_flush_delay
  ]

  use GenServer

  import Nebulex.Utils

  alias Nebulex.Adapter
  alias Nebulex.Adapters.Common.Info.Stats
  alias Nebulex.Adapters.Local.{Backend, Metadata, Options}
  alias Nebulex.Telemetry

  @type t() :: %__MODULE__{}
  @type server_ref() :: pid() | atom() | :ets.tid()
  @type opts() :: Nebulex.Cache.opts()

  ## API

  @doc """
  Starts the garbage collector for the built-in local cache adapter.
  """
  @spec start_link(opts) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  @doc """
  Creates a new cache generation. Once the max number of generations
  is reached, when a new generation is created, the oldest one is
  deleted.

  ## Options

    * `:reset_timer` - Indicates if the poll frequency time-out should
      be reset or not (default: true).

  ## Example

      Nebulex.Adapters.Local.Generation.new(MyCache)

      Nebulex.Adapters.Local.Generation.new(MyCache, reset_timer: false)

  """
  @spec new(server_ref, opts) :: [atom]
  def new(server_ref, opts \\ []) do
    # Validate options
    opts = Options.validate_runtime_opts!(opts)

    do_call(server_ref, {:new_generation, Keyword.fetch!(opts, :reset_timer)})
  end

  @doc """
  Removes or flushes all entries from the cache (including all its generations).

  ## Example

      Nebulex.Adapters.Local.Generation.delete_all(MyCache)

  """
  @spec delete_all(server_ref) :: :ok
  def delete_all(server_ref) do
    do_call(server_ref, :delete_all)
  end

  @doc """
  Reallocates the block of memory that was previously allocated for the given
  `server_ref` with the new `size`. In other words, reallocates the max memory
  size for a cache generation.

  ## Example

      Nebulex.Adapters.Local.Generation.realloc(MyCache, 1_000_000)

  """
  @spec realloc(server_ref, pos_integer) :: :ok
  def realloc(server_ref, size) do
    do_call(server_ref, {:realloc, size})
  end

  @doc """
  Returns the memory info in a tuple form `{used_mem, total_mem}`.

  ## Example

      Nebulex.Adapters.Local.Generation.memory_info(MyCache)

  """
  @spec memory_info(server_ref) :: {used_mem :: non_neg_integer, total_mem :: non_neg_integer}
  def memory_info(server_ref) do
    do_call(server_ref, :memory_info)
  end

  @doc """
  Resets the timer for pushing new cache generations.

  ## Example

      Nebulex.Adapters.Local.Generation.reset_timer(MyCache)

  """
  @spec reset_timer(server_ref()) :: :ok
  def reset_timer(server_ref) do
    server_ref
    |> server()
    |> GenServer.cast(:reset_timer)
  end

  @doc """
  Returns the list of the generations in the form `[newer, older]`.

  ## Example

      Nebulex.Adapters.Local.Generation.list(MyCache)

  """
  @spec list(server_ref) :: [:ets.tid()]
  def list(server_ref) do
    server_ref
    |> get_meta_tab()
    |> Metadata.get(:generations, [])
  end

  @doc """
  Returns the newer generation.

  ## Example

      Nebulex.Adapters.Local.Generation.newer(MyCache)

  """
  @spec newer(server_ref) :: :ets.tid()
  def newer(server_ref) do
    server_ref
    |> get_meta_tab()
    |> Metadata.get(:generations, [])
    |> hd()
  end

  @doc """
  Returns the PID of the GC server for the given `server_ref`.

  ## Example

      Nebulex.Adapters.Local.Generation.server(MyCache)

  """
  @spec server(server_ref) :: pid
  def server(server_ref) do
    server_ref
    |> get_meta_tab()
    |> Metadata.fetch!(:gc_pid)
  end

  @doc """
  A convenience function for retrieving the state.
  """
  @spec get_state(server_ref) :: t()
  def get_state(server_ref) do
    server_ref
    |> server()
    |> GenServer.call(:get_state)
  end

  defp do_call(tab, message) do
    tab
    |> server()
    |> GenServer.call(message)
  end

  defp get_meta_tab(server_ref) when is_atom(server_ref) or is_pid(server_ref) do
    unwrap_or_raise Adapter.with_meta(server_ref, & &1.meta_tab)
  end

  defp get_meta_tab(server_ref), do: server_ref

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    # Trap exit signals to run cleanup process
    _ = Process.flag(:trap_exit, true)

    # Get adapter metadata
    adapter_meta = Keyword.fetch!(opts, :adapter_meta)

    # Add the GC PID to the meta table
    meta_tab = Map.fetch!(adapter_meta, :meta_tab)
    :ok = Metadata.put(meta_tab, :gc_pid, self())

    # Initial state
    state =
      struct(
        __MODULE__,
        opts
        |> Map.new()
        |> Map.merge(adapter_meta)
      )

    # Init cleanup timer
    cleanup_ref =
      if state.max_size || state.allocated_memory,
        do: start_timer(state.gc_cleanup_max_timeout, nil, :cleanup)

    # Timer ref
    {:ok, ref} =
      if state.gc_interval,
        do: {new_gen(state), start_timer(state.gc_interval)},
        else: {new_gen(state), nil}

    # Update state
    state = %{state | gc_cleanup_ref: cleanup_ref, gc_heartbeat_ref: ref}

    {:ok, state}
  end

  @impl true
  def terminate(_reason, state) do
    if ref = state.stats_counter, do: Telemetry.detach(ref)
  end

  @impl true
  def handle_call(:delete_all, _from, %__MODULE__{} = state) do
    :ok = new_gen(state)

    :ok =
      state.meta_tab
      |> list()
      |> Enum.each(&state.backend.delete_all_objects(&1))

    {:reply, :ok, %{state | gc_heartbeat_ref: maybe_reset_timer(true, state)}}
  end

  def handle_call({:new_generation, reset_timer?}, _from, state) do
    # Create new generation
    :ok = new_gen(state)

    # Maybe reset heartbeat timer
    heartbeat_ref = maybe_reset_timer(reset_timer?, state)

    {:reply, :ok, %{state | gc_heartbeat_ref: heartbeat_ref}}
  end

  def handle_call(
        :memory_info,
        _from,
        %__MODULE__{backend: backend, meta_tab: meta_tab, allocated_memory: allocated} = state
      ) do
    {:reply, {memory_info(backend, meta_tab), allocated}, state}
  end

  def handle_call({:realloc, mem_size}, _from, state) do
    {:reply, :ok, %{state | allocated_memory: mem_size}}
  end

  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_cast(:reset_timer, state) do
    {:noreply, %{state | gc_heartbeat_ref: maybe_reset_timer(true, state)}}
  end

  @impl true
  def handle_info(
        :heartbeat,
        %__MODULE__{
          gc_interval: gc_interval,
          gc_heartbeat_ref: heartbeat_ref
        } = state
      ) do
    # Create new generation
    :ok = new_gen(state)

    # Reset heartbeat timer
    heartbeat_ref = start_timer(gc_interval, heartbeat_ref)

    {:noreply, %{state | gc_heartbeat_ref: heartbeat_ref}}
  end

  def handle_info(:cleanup, state) do
    # Check size first, if the cleanup is done, skip checking the memory,
    # otherwise, check the memory too.
    {_, state} =
      with {false, state} <- check_size(state) do
        check_memory(state)
      end

    {:noreply, state}
  end

  def handle_info(
        :flush_older_gen,
        %__MODULE__{
          meta_tab: meta_tab,
          backend: backend
        } = state
      ) do
    if deprecated = Metadata.get(meta_tab, :deprecated) do
      true = backend.delete_all_objects(deprecated)
    end

    {:noreply, state}
  end

  ## Private Functions

  defp start_timer(time, ref \\ nil, event \\ :heartbeat)

  defp start_timer(nil, _, _) do
    nil
  end

  defp start_timer(time, ref, event) do
    _ = if ref, do: Process.cancel_timer(ref)

    Process.send_after(self(), event, time)
  end

  defp maybe_reset_timer(_, %__MODULE__{gc_interval: nil} = state) do
    state.gc_heartbeat_ref
  end

  defp maybe_reset_timer(false, state) do
    state.gc_heartbeat_ref
  end

  defp maybe_reset_timer(true, %__MODULE__{} = state) do
    start_timer(state.gc_interval, state.gc_heartbeat_ref)
  end

  defp new_gen(%__MODULE__{
         meta_tab: meta_tab,
         backend: backend,
         backend_opts: backend_opts,
         stats_counter: stats_counter,
         gc_flush_delay: gc_flush_delay
       }) do
    # Create new generation
    gen_tab = Backend.new(backend, meta_tab, backend_opts)

    # Update generation list
    case list(meta_tab) do
      [newer, older] ->
        # Since the older generation is deleted, update evictions count
        :ok = Stats.incr(stats_counter, :evictions, backend.info(older, :size))

        # Update generations
        :ok = Metadata.put(meta_tab, :generations, [gen_tab, newer])

        # Process the older generation:
        # - Delete previously stored deprecated generation
        # - Flush the older generation
        # - Deprecate it (mark it for deletion)
        :ok = process_older_gen(meta_tab, backend, older, gc_flush_delay)

      [newer] ->
        # Update generations
        :ok = Metadata.put(meta_tab, :generations, [gen_tab, newer])

      [] ->
        # update generations
        :ok = Metadata.put(meta_tab, :generations, [gen_tab])
    end
  end

  # The older generation cannot be removed immediately because there may be
  # ongoing operations using it, then it may cause race-condition errors.
  # Hence, the idea is to keep it alive till a new generation is created, but
  # flushing its data before so that we release memory space. By the time a new
  # generation is created, then it is safe to delete it completely.
  defp process_older_gen(meta_tab, backend, older, gc_flush_delay) do
    if deprecated = Metadata.get(meta_tab, :deprecated) do
      # Delete deprecated generation if it does exist
      _ = Backend.delete(backend, meta_tab, deprecated)
    end

    # Flush older generation to release space so it can be marked for deletion
    _ref = Process.send_after(self(), :flush_older_gen, gc_flush_delay)

    # Keep alive older generation reference into the metadata
    Metadata.put(meta_tab, :deprecated, older)
  end

  defp check_size(%__MODULE__{max_size: max_size} = state) when not is_nil(max_size) do
    maybe_cleanup(:size, state)
  end

  defp check_size(state) do
    {false, state}
  end

  defp check_memory(%__MODULE__{allocated_memory: allocated} = state) when not is_nil(allocated) do
    maybe_cleanup(:memory, state)
  end

  defp check_memory(state) do
    {false, state}
  end

  defp maybe_cleanup(
         info,
         %__MODULE__{
           cache: cache,
           name: name,
           gc_cleanup_ref: cleanup_ref,
           gc_cleanup_min_timeout: min_timeout,
           gc_cleanup_max_timeout: max_timeout,
           gc_interval: gc_interval,
           gc_heartbeat_ref: heartbeat_ref
         } = state
       ) do
    case cleanup_info(info, state) do
      {size, max_size} when size >= max_size ->
        # Create a new generation
        :ok = new_gen(state)

        # Delete expired entries
        _ = cache.delete_all(name, [query: :expired], [])

        # Reset the heartbeat timer
        heartbeat_ref = start_timer(gc_interval, heartbeat_ref)

        # Reset the cleanup timer
        cleanup_ref =
          info
          |> cleanup_info(state)
          |> elem(0)
          |> reset_cleanup_timer(max_size, min_timeout, max_timeout, cleanup_ref)

        {true, %{state | gc_heartbeat_ref: heartbeat_ref, gc_cleanup_ref: cleanup_ref}}

      {size, max_size} ->
        # Reset the cleanup timer
        cleanup_ref = reset_cleanup_timer(size, max_size, min_timeout, max_timeout, cleanup_ref)

        {false, %{state | gc_cleanup_ref: cleanup_ref}}
    end
  end

  defp cleanup_info(:size, %__MODULE__{backend: mod, meta_tab: tab, max_size: max}) do
    {size_info(mod, tab), max}
  end

  defp cleanup_info(:memory, %__MODULE__{backend: mod, meta_tab: tab, allocated_memory: max}) do
    {memory_info(mod, tab), max}
  end

  defp reset_cleanup_timer(size, max_size, min_timeout, max_timeout, cleanup_ref) do
    size
    |> linear_inverse_backoff(max_size, min_timeout, max_timeout)
    |> start_timer(cleanup_ref, :cleanup)
  end

  defp size_info(backend, meta_tab) do
    meta_tab
    |> list()
    |> Enum.reduce(0, &(backend.info(&1, :size) + &2))
  end

  defp memory_info(backend, meta_tab) do
    meta_tab
    |> list()
    |> Enum.reduce(0, fn gen, acc ->
      gen
      |> backend.info(:memory)
      |> Kernel.*(:erlang.system_info(:wordsize))
      |> Kernel.+(acc)
    end)
  end

  defp linear_inverse_backoff(size, _max_size, _min_timeout, max_timeout) when size <= 0 do
    max_timeout
  end

  defp linear_inverse_backoff(size, max_size, min_timeout, _max_timeout) when size >= max_size do
    min_timeout
  end

  defp linear_inverse_backoff(size, max_size, min_timeout, max_timeout) do
    round((min_timeout - max_timeout) / max_size * size + max_timeout)
  end
end
