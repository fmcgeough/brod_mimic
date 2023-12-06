defmodule BrodMimic.KafkaApis do
  @moduledoc """
    -export([ default_version/1
            , pick_version/2
            , start_link/0
            , stop/0
            ]).

    -export([ code_change/3
            , handle_call/3
            , handle_cast/2
            , handle_info/2
            , init/1
            , terminate/2
            ])
  """

  require Logger
  require Record

  @supported_versions %{
    produce: {0, 5},
    fetch: {0, 7},
    list_offsets: {0, 2},
    metadata: {0, 2},
    offset_commit: {2, 2},
    offset_fetch: {1, 2},
    find_coordinator: {0, 0},
    join_group: {0, 0},
    heartbeat: {0, 0},
    leave_group: {0, 0},
    sync_group: {0, 0},
    describe_groups: {0, 0},
    list_groups: {0, 0},
    create_topics: {0, 0},
    delete_topics: {0, 0}
  }

  Record.defrecord(:r_kafka_message_set, :kafka_message_set,
    topic: :undefined,
    partition: :undefined,
    high_wm_offset: :undefined,
    messages: :undefined
  )

  Record.defrecord(:r_kafka_fetch_error, :kafka_fetch_error,
    topic: :undefined,
    partition: :undefined,
    error_code: :undefined,
    error_desc: ''
  )

  Record.defrecord(:r_brod_call_ref, :brod_call_ref,
    caller: :undefined,
    callee: :undefined,
    ref: :undefined
  )

  Record.defrecord(:r_brod_produce_reply, :brod_produce_reply,
    call_ref: :undefined,
    base_offset: :undefined,
    result: :undefined
  )

  Record.defrecord(:r_kafka_group_member_metadata, :kafka_group_member_metadata,
    version: :undefined,
    topics: :undefined,
    user_data: :undefined
  )

  Record.defrecord(:r_brod_received_assignment, :brod_received_assignment,
    topic: :undefined,
    partition: :undefined,
    begin_offset: :undefined
  )

  Record.defrecord(:r_brod_cg, :brod_cg,
    id: :undefined,
    protocol_type: :undefined
  )

  Record.defrecord(:r_socket, :socket,
    pid: :undefined,
    host: :undefined,
    port: :undefined,
    node_id: :undefined
  )

  Record.defrecord(:r_cbm_init_data, :cbm_init_data,
    committed_offsets: :undefined,
    cb_fun: :undefined,
    cb_data: :undefined
  )

  Record.defrecord(:r_state, :state, [])

  @type vsn() :: :kpro.vsn()
  @type range() :: {vsn(), vsn()}
  @type api() :: :kpro.api()
  @type conn() :: :kpro.connection()

  @doc """
  Start process.
  """
  @spec start_link() :: {:ok, pid()}
  def start_link do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @spec stop() :: :ok
  def stop do
    GenServer.call(__MODULE__, :stop, :infinity)
  end

  @doc """
  Get default supported version for the given API.
  """
  @spec default_version(api()) :: vsn()
  def default_version(api) do
    {min, _max} = supported_versions(api)
    min
  end

  @doc """
  Pick API version for the given API.
  """
  @spec pick_version(conn(), api()) :: vsn()
  def pick_version(conn, api) do
    do_pick_version(conn, api, supported_versions(api))
  end

  ### _* gen_server callbacks =====================================================

  def init([]) do
    :brod_kafka_apis = :ets.new(:brod_kafka_apis, [:named_table, :public])

    {:ok, r_state()}
  end

  def handle_info({:DOWN, _mref, :process, conn, _reason}, state) do
    _ = :ets.delete(:brod_kafka_apis, conn)
    {:noreply, state}
  end

  def handle_info(info, state) do
    Logger.error("unknown info #{inspect(info)}")
    {:noreply, state}
  end

  def handle_cast({:monitor_connection, conn}, state) do
    :erlang.monitor(:process, conn)
    {:noreply, state}
  end

  def handle_cast(cast, state) do
    Logger.error("unknown cast #{inspect(cast)}")
    {:noreply, state}
  end

  def handle_call(:stop, from, state) do
    GenServer.reply(from, :ok)
    {:stop, :normal, state}
  end

  def handle_call(call, _from, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  def code_change(_oldvsn, state, _extra) do
    {:ok, state}
  end

  def terminate(_reason, _state) do
    :ok
  end

  #### Internals ================================================================

  @spec do_pick_version(conn(), api(), range()) :: vsn()
  def do_pick_version(_conn, _api, {v, v}), do: v

  def do_pick_version(conn, api, {min, max} = my_range) do
    case lookup_vsn_range(conn, api) do
      :none ->
        # no version received from kafka, use min
        min

      {kpro_min, kpro_max} = range when kpro_min > max or kpro_max < min ->
        :erlang.error({:unsupported_vsn_range, api, my_range, range})

      {_, kpro_max} ->
        ## try to use highest version
        min(kpro_max, max)
    end
  end

  @doc """
  Lookup API from cache, return ':none' if not found
  """
  @spec lookup_vsn_range(conn(), api()) :: {vsn(), vsn()} | :none
  def lookup_vsn_range(conn, api) do
    case :ets.lookup(:brod_kafka_apis, conn) do
      [] ->
        case :kpro.get_api_versions(conn) do
          {:ok, versions} when is_map(versions) ->
            :ets.insert(:brod_kafka_apis, {conn, versions})
            :ok = monitor_connection(conn)
            Map.get(api, versions, :none)

          {:error, _reason} ->
            # connection died, ignore
            :none
        end

      [{_conn, vsns}] ->
        Map.get(api, vsns, :none)
    end
  end

  # Do not change range without verification.
  def supported_versions(api) do
    Map.get(@supported_versions, api, :erlang.error({:unsupported_api, api}))
  end

  defp monitor_connection(conn) do
    GenServer.cast(__MODULE__, {:monitor_connection, conn})
  end
end
