defmodule BrodMimic.Client do
  @moduledoc """
  GenServer used to manage TCP connection to Kafka broker (mimics
  [brod_client](https://github.com/kafka4beam/brod/blob/master/src/brod_client.erl)).
  The Erlang library makes the following functions public (using the
  [export](https://erlang.org/doc/reference_manual/modules.html) keyword) in
  it's module.

  * `get_consumer/3`
  * `get_connection/3`
  * `get_group_coordinator/2`
  * `get_leader_connection/3`
  * `get_metadata/2`
  * `get_partitions_count/2`
  * `get_producer/3`
  * `register_consumer/3`
  * `register_producer/3`
  * `deregister_consumer/3`
  * `deregister_producer/3`
  * `start_link/3`
  * `start_producer/3`
  * `start_consumer/3`
  * `stop/1`
  * `stop_producer/2`
  * `stop_consumer/2`
  ```
  """
  use GenServer
  require BrodMimic.Macros
  require Logger
  require Record

  alias BrodMimic.{BrodConsumersSup, BrodProducersSup, Macros}

  @default_reconnect_cool_down_seconds 1
  # @default_get_metadata_timeout_seconds 5

  # @unknown_topic_cache_expire_seconds 120

  @type endpoint() :: Brod.endpoint()
  @type client() :: Brod.client()
  @type client_id() :: Brod.client_id()
  @type topic() :: Brod.topic()
  @type partition() :: Brod.partition()
  @type config() :: :proplists.proplist()
  @type group_id() :: Brod.group_id()

  @type partition_worker_key() ::
          {:producer, topic(), partition()} | {:consumer, topic(), partition()}

  @typedoc """
  Consumer errors

  * `:client_down` - returned if the client that manages TCP connection is not
    available. This is discovered via [ETS](https://erlang.org/doc/man/ets.html).
  * `{:consumer_down, :noproc}` - brod catches exceptions when a `GenServer`
    calls to consumer is done but the GenServer is no longer active.
  * `{:consumer_not_found, topic :: binary()}` - returned if caller specifies
    topic and there is no consumer associated with that topic.
  * `{:consumer_not_found, topic :: binary(), partition :: integer()}` -
    returned if caller specifies topic & partition and no consumer is found that
    matches.
  """
  @type get_consumer_error() ::
          :client_down
          | {:consumer_down, :noproc}
          | {:consumer_not_found, topic()}
          | {:consumer_not_found, topic(), partition()}

  @type get_worker_error :: get_producer_error() | get_consumer_error()

  @type connection() :: :kpro.connection()
  @type timestamp() ::
          {mega_secs :: non_neg_integer(), secs :: non_neg_integer(),
           micro_secs :: non_neg_integer()}
  @type dead_conn() :: {:dead_since, timestamp(), any()}
  Record.defrecord(:conn, endpoint: nil, pid: nil)
  @type conn :: record(:conn, endpoint: endpoint(), pid: connection() | dead_conn())

  @type conn_state() :: conn()
  Record.defrecord(:state,
    client_id: nil,
    bootstrap_endpoints: nil,
    meta_conn: nil,
    payload_conns: nil,
    producers_sup: nil,
    consumers_sup: nil,
    config: nil,
    workers_tab: nil
  )

  @type state ::
          record(:state,
            client_id: client_id(),
            bootstrap_endpoints: [endpoint()],
            meta_conn: :undef | connection(),
            payload_conns: [conn_state()],
            producers_sup: :undef | pid(),
            consumers_sup: :undef | pid(),
            config: :undef | config(),
            workers_tab: :undef | :ets.tab()
          )

  @typedoc """
  FM - Question on both `get_producer_error` and `get_consumer_error`: is
  `:client_down` ever returned standalone? It looks like it comes back as
  `{:error, :client_down}`
  """
  @type get_producer_error ::
          :client_down
          | {:producer_down, :noproc}
          | {:producer_not_found, topic()}
          | {:producer_not_found, topic(), partition()}

  @spec start_link([endpoint()], client_id(), config()) :: {:ok, pid()} | {:error, any()}
  def start_link(bootstrap_endpoints, client_id, config) when is_atom(client_id) do
    args = {bootstrap_endpoints, client_id, config}
    GenServer.start_link(__MODULE__, args, name: client_id)
  end

  @spec stop(client()) :: :ok
  def stop(client) do
    mref = Process.monitor(client)
    _ = safe_gen_call(client, :stop, :infinity)

    receive do
      {:DOWN, ^mref, :process, _pid, _reason} -> :ok
    end
  end

  @doc """
  Get producer of the given topic-partition

  The producer is started if `:auto_start_producers` is enabled in client config
  """
  @spec get_producer(client(), topic(), partition()) ::
          {:ok, pid()} | {:error, get_producer_error()}
  def get_producer(client, topic, partition) do
    case get_partition_worker(client, producer_key(topic, partition)) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:producer_not_found, topic}} = error ->
        ## try to start a producer for the given topic if
        ## auto_start_producers option is enabled for the client
        maybe_start_producer(client, topic, partition, error)

      error ->
        error
    end
  end

  @doc """
  Get consumer of the given topic-partition
  """
  @spec get_consumer(client(), topic(), partition()) ::
          {:ok, pid()} | {:error, get_consumer_error()}
  def get_consumer(client, topic, partition) do
    get_partition_worker(client, consumer_key(topic, partition))
  end

  @doc """
  Dynamically start a per-topic producer.
  Return ok if the producer is already started.
  """
  @spec start_producer(client(), topic(), Brod.producer_config()) :: :ok | {:error, any()}
  def start_producer(client, topic_name, producer_config) do
    case get_producer(client, topic_name, _partition = 0) do
      {:ok, _pid} ->
        # already started
        :ok

      {:error, {:producer_not_found, topic_name}} ->
        call = {:start_producer, topic_name, producer_config}
        safe_gen_call(client, call, :infinity)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Stop all partition producers of the given topic.
  """
  @spec stop_producer(client(), topic()) :: :ok | {:error, any()}
  def stop_producer(client, topic_name) do
    safe_gen_call(client, {:stop_producer, topic_name}, :infinity)
  end

  @doc """
  Dynamically start a topic consumer

  Returns :ok if the consumer is already started.
  """
  @spec start_consumer(client(), topic(), Brod.consumer_config()) :: :ok | {:error, any()}
  def start_consumer(client, topic_name, consumer_config) do
    case get_consumer(client, topic_name, _partition = 0) do
      {:ok, _pid} ->
        # already started
        :ok

      {:error, {:consumer_not_found, topic_name}} ->
        call = {:start_consumer, topic_name, consumer_config}
        safe_gen_call(client, call, :infinity)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Stop all partition consumers of the given topic.
  """
  @spec stop_consumer(client(), topic()) :: :ok | {:error, any()}
  def stop_consumer(client, topic_name) do
    safe_gen_call(client, {:stop_consumer, topic_name}, :infinity)
  end

  @doc """
  Get the connection to kafka broker which is a leader for given topic-partition

  Return already established connection towards the leader broker,
  Otherwise a new one is established and cached in client state.
  If the old connection was dead less than a configurable N seconds ago,
  `{:error, last_reason}` is returned.
  """
  @spec get_leader_connection(client(), topic(), partition()) :: {:ok, pid()} | {:error, any()}
  def get_leader_connection(client, topic, partition) do
    safe_gen_call(client, {:get_leader_connection, topic, partition}, :infinity)
  end

  @doc """
  Get connection to a kafka broker

  Return already established connection towards the broker,
  otherwise a new one is established and cached in client state.
  If the old connection was dead less than a configurable N seconds ago,
  `{error, last_reason}` is returned.
  """
  @spec get_connection(client(), Brod.hostname(), Brod.portnum()) ::
          {:ok, pid()} | {:error, any()}
  def get_connection(client, host, port) do
    safe_gen_call(client, {:get_connection, host, port}, :infinity)
  end

  @doc """
  Get topic metadata, if topic is undefined (`:undef`) it will fetch ALL metadata
  """
  @spec get_metadata(client(), :all | :undef | topic()) :: {:ok, :kpro.struct()} | {:error, any()}
  def get_metadata(client, :undef) do
    get_metadata(client, :all)
  end

  def get_metadata(client, topic) do
    safe_gen_call(client, {:get_metadata, topic}, :infinity)
  end

  @doc """
  Get number of partitions for a given topic.
  """
  @spec get_partitions_count(client(), topic()) :: {:ok, pos_integer()} | {:error, any()}
  def get_partitions_count(client, topic) when is_atom(client) do
    # ets is the client id
    get_partitions_count(client, client, topic)
  end

  def get_partitions_count(client, topic) when is_pid(client) do
    case safe_gen_call(client, :get_workers_table, :infinity) do
      {:ok, ets} -> get_partitions_count(client, ets, topic)
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Get broker endpoint and connection config for connecting a group coordinator.
  """
  @spec get_group_coordinator(client(), group_id()) ::
          {:ok, {endpoint(), Brod.conn_config()}} | {:error, any()}
  def get_group_coordinator(client, group_id) do
    safe_gen_call(client, {:get_group_coordinator, group_id}, :infinity)
  end

  @doc """
  Register [self()](https://hexdocs.pm/elixir/1.12/Kernel.html#self/0) as a
  partition producer. The pid is registered in an
  [ETS](https://erlang.org/doc/man/ets.html) table, then the callers may lookup
  a producer pid from the table and make produce requests to the producer
  process directly.
  """
  @spec register_producer(client(), topic(), partition()) :: :ok
  def register_producer(client, topic, partition) do
    producer = self()
    key = Macros.producer_key(topic, partition)
    GenServer.cast(client, {:register, key, producer})
  end

  @doc """
  De-register the producer for a partition. The partition producer
  entry is deleted from the ETS table to allow cleanup of purposefully
  stopped producers and allow later restart.
  """
  @spec deregister_producer(client(), topic(), partition()) :: :ok
  def deregister_producer(client, topic, partition) do
    key = Macros.producer_key(topic, partition)
    GenServer.cast(client, {:deregister, key})
  end

  @doc """
  Register [self()](https://hexdocs.pm/elixir/1.12/Kernel.html#self/0) as a
  partition consumer. The pid is registered in an
  [ETS](https://erlang.org/doc/man/ets.html) table, then the callers may lookup
  a consumer pid from the table ane make subscribe calls to the process
  directly.
  """
  @spec register_consumer(client(), topic(), partition()) :: :ok
  def register_consumer(client, topic, partition) do
    consumer = self()
    key = Macros.consumer_key(topic, partition)
    GenServer.cast(client, {:register, key, consumer})
  end

  @doc """
  De-register the consumer for a partition. The partition consumer
  entry is deleted from the ETS table to allow cleanup of purposefully
  stopped consumers and allow later restart.
  """
  @spec deregister_consumer(client(), topic(), partition()) :: :ok
  def deregister_consumer(client, topic, partition) do
    key = Macros.consumer_key(topic, partition)
    GenServer.cast(client, {:deregister, key})
  end

  # gen_server callbacks =====================================================

  def init({bootstrap_endpoints, client_id, config}) do
    Process.flag(:trap_exit, true)
    ets_options = [:named_table, :protected, {:read_concurrency, true}]
    tab = :ets.new(client_id, ets_options)

    {:ok,
     %{
       client_id: client_id,
       bootstrap_endpoints: bootstrap_endpoints,
       config: config,
       workers_tab: tab
     }}
  end

  def handle_info(:init, state0) do
    endpoints = state0.bootstrap_endpoints
    state1 = ensure_metadata_connection(state0)
    {:ok, producers_sup_pid} = BrodProducersSup.start_link()
    {:ok, consumers_sup_pid} = BrodConsumersSup.start_link()

    state =
      state(state1,
        bootstrap_endpoints: endpoints,
        producers_sup: producers_sup_pid,
        consumers_sup: consumers_sup_pid
      )

    {:noreply, state}
  end

  ### Internal use

  @doc """
  Ensure there is at least one metadata connection
  """
  def ensure_metadata_connection(state(bootstrap_endpoints: endpoints, meta_conn: :undef) = state) do
    conn_config = conn_config(state)

    pid =
      case :kpro.connect_any(endpoints, conn_config) do
        {:ok, pid_x} -> pid_x
        {:error, reason} -> :erlang.exit(reason)
      end

    state(state, meta_conn: pid)
  end

  def ensure_metadata_connection(state) do
    state
  end

  @spec get_partition_worker(client(), partition_worker_key()) ::
          {:ok, pid()} | {:error, get_worker_error()}
  def get_partition_worker(client_pid, key) when is_pid(client_pid) do
    case Process.info(client_pid, :registered_name) do
      {:registered_name, client_id} when is_atom(client_id) ->
        get_partition_worker(client_id, key)

      _ ->
        ## This is a client process started without registered name
        ## have to call the process to get the producer/consumer worker
        ## process registration table.
        case safe_gen_call(client_pid, :get_workers_table, :infinity) do
          {:ok, ets} -> lookup_partition_worker(client_pid, ets, key)
          {:error, reason} -> {:error, reason}
        end
    end
  end

  def get_partition_worker(client_id, key) when is_atom(client_id) do
    lookup_partition_worker(client_id, ets(client_id), key)
  end

  @doc """
  """
  @spec lookup_partition_worker(client(), :ets.tab(), partition_worker_key()) ::
          {:ok, pid()}
          | {:error, get_worker_error()}
  def lookup_partition_worker(client, ets, key) do
    case :ets.lookup(ets, key) do
      [] ->
        ## not yet registered, 2 possible reasons:
        ## 1. caller is too fast, producers/consumers are starting up
        ##    make a synced call all the way down the supervision tree
        ##    to the partition producer sup should resolve the race
        # 2. bad argument, no such worker started, supervisors should know
        find_partition_worker(client, key)

      [{{:producer, _topic, _partition}, pid}] ->
        {:ok, pid}

      [{{:consumer, _topic, _partition}, pid}] ->
        {:ok, pid}
    end
  catch
    :error, :badarg ->
      {:error, :client_down}
  end

  @spec find_partition_worker(client(), partition_worker_key()) ::
          {:ok, pid()} | {:error, get_worker_error()}
  def find_partition_worker(client, {:producer, topic, partition}) do
    find_producer(client, topic, partition)
  end

  def find_partition_worker(client, {:consumer, topic, partition}) do
    find_consumer(client, topic, partition)
  end

  @spec find_producer(client(), topic(), partition()) ::
          {:ok, pid()} | {:error, get_producer_error()}
  def find_producer(client, topic, partition) do
    case safe_gen_call(client, :get_producers_sup_pid, :infinity) do
      {:ok, sup_pid} ->
        # TODO brod_producers_sup:find_producer(sup_id, topic, partition)
        {sup_pid, topic, partition}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Find the Supervisor pid for the consumers and then ask the
  Supervisor to find the consumer for this particular topic
  and partition
  """
  @spec find_consumer(client(), topic(), partition()) ::
          {:ok, pid()} | {:error, get_consumer_error()}
  def find_consumer(client, topic, partition) do
    case safe_gen_call(client, :get_consumers_sup_pid, :infinity) do
      {:ok, sup_pid} ->
        # TODO brod_consumers_sup:find_consumer(sup_id, topic, partition)
        {sup_pid, topic, partition}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Catch `:noproc` exit exception when making GenServer.call
  """
  @spec safe_gen_call(pid() | atom(), call :: term(), timeout :: :infinity | integer()) ::
          :ok | {:ok, term()} | {:error, :client_down | term()}
  def safe_gen_call(server, call, timeout) do
    GenServer.call(server, call, timeout)
  catch
    :exit, {:noproc, _} ->
      {:error, :client_down}
  end

  @doc """
  `Process.exit/2` for client GenServer

  Note: Stop producers and consumers first because they are monitoring
  connections
  """
  def shutdown_pid(pid) when is_pid(pid) do
    Process.exit(pid, :shutdown)
    :ok
  end

  def shutdown_pid(_) do
    :ok
  end

  @doc """
  Get partition counter from cache.

  If cache is not hit, send meta data request to retrieve.
  """
  @spec get_partitions_count(client(), :ets.tab(), topic()) ::
          {:ok, pos_integer()} | {:error, any()}
  def get_partitions_count(client, ets, topic) do
    case lookup_partitions_count_cache(ets, topic) do
      {:ok, result} ->
        {:ok, result}

      {:error, reason} ->
        {:error, reason}

      false ->
        # This call should populate the cache
        case get_metadata(client, topic) do
          {:ok, meta} ->
            [topic_metadata] = kf(:topic_metadata, meta)
            do_get_partitions_count(topic_metadata)

          {:error, reason} ->
            {:error, reason}
        end
    end
  end

  @spec do_get_partitions_count(:kpro.struct()) :: {:ok, pos_integer()} | {:error, any()}
  def do_get_partitions_count(topic_metadata) do
    error_code = kf(:error_code, topic_metadata)
    partitions = kf(:partition_metadata, topic_metadata)

    case Macros.is_error(error_code) do
      true -> {:error, error_code}
      false -> {:ok, Enum.count(partitions)}
    end
  end

  @doc """
  Looks up the partition count in [ETS](https://erlang.org/doc/man/ets.html)
  """
  @spec lookup_partitions_count_cache(:ets.tab(), :undef | topic()) ::
          {:ok, pos_integer()}
          | {:error, any()}
          | false
  def lookup_partitions_count_cache(ets, topic) do
    case :ets.lookup(ets, Macros.topic_metadata_key(topic)) do
      [{_, count, _ts}] when is_integer(count) ->
        {:ok, count}

      [{_, {:error, reason}, ts}] ->
        ts_diff = :timer.now_diff(:os.timestamp(), ts)

        case ts_diff <= Macros.unknown_topic_cache_expire_seconds() * 1_000_000 do
          true -> {:error, reason}
          false -> false
        end

      [] ->
        false
    end
  catch
    :error, :badarg ->
      {:error, :client_down}
  end

  @spec kf(:kpro.field_name(), :kpro.struct()) :: :kpro.field_value()
  def kf(field_name, struct) do
    :kpro.find(field_name, struct)
  end

  @doc """
  Try to start a producer for the given topic if `:auto_start_producers option`
  is enabled for the client
  """
  @spec maybe_start_producer(client(), topic(), partition(), {:error, any()}) ::
          :ok | {:error, any()}
  def maybe_start_producer(client, topic, partition, error) do
    case safe_gen_call(:client, {:auto_start_producer, topic}, :infinity) do
      :ok ->
        get_partition_worker(client, Macros.producer_key(topic, partition))

      {:error, :disabled} ->
        error

      {:error, reason} ->
        {:error, reason}
    end
  end

  def conn_config(state(client_id: client_id, config: config)) do
    cfg = conn_config(config, :kpro_connection.all_cfg_keys(), [])
    :maps.from_list([{:client_id, ensure_binary(client_id)} | cfg])
  end

  def conn_config([], _conn_cfg_keys, acc), do: acc

  def conn_config([{k, v} | rest], conn_cfg_keys, acc) do
    new_acc =
      case :lists.member(k, conn_cfg_keys) do
        true -> [{k, v} | acc]
        false -> acc
      end

    conn_config(rest, conn_cfg_keys, new_acc)
  end

  def conn_config([k | rest], conn_cfg_keys, acc) when is_atom(k) do
    # translate proplist boolean mark to tuple
    conn_config([{k, true} | rest], conn_cfg_keys, acc)
  end

  @spec maybe_connect(state(), endpoint()) :: {{:ok, pid()} | {:error, any()}, state()}
  def maybe_connect(state, endpoint) do
    case find_conn(endpoint, state(state, :payload_conns)) do
      {:ok, pid} -> {{:ok, pid}, state}
      {:error, reason} -> maybe_connect(state, endpoint, reason)
    end
  end

  @spec maybe_connect(state(), endpoint(), :not_found | dead_conn()) ::
          {{:ok, pid()} | {:error, any()}, state()}
  def maybe_connect(state, endpoint, :not_found) do
    # connect for the first time
    connect(state, endpoint)
  end

  # state{client_id = ClientId} = State,
  def maybe_connect(
        state(client_id: client_id) = state,
        {host, port} = endpoint,
        {:dead_since, ts, reason}
      ) do
    case is_cooled_down(ts, state) do
      true ->
        connect(state, endpoint)

      false ->
        connect_to = "(re)connect to #{host}:#{port} aborted"
        msg = "#{client_id} #{connect_to}.\nlast failure: #{inspect(reason)}"
        Logger.error(msg)
        {{:error, reason}, state}
    end
  end

  @spec connect(state(), endpoint()) :: {{:ok, pid()} | {:error, any()}, state()}
  def connect(state(client_id: client_id, payload_conns: conns) = state, {host, port} = endpoint) do
    conn =
      case do_connect(endpoint, state) do
        {:ok, pid} ->
          Logger.info("client #{client_id} connected to #{host}:#{port}")
          conn(endpoint: endpoint, pid: pid)

        {:error, reason} ->
          Logger.info(
            "client #{client_id} failed to connect to #{host}:#{port}\nreason: #{inspect(reason)}"
          )

          conn(endpoint: endpoint, pid: mark_dead(reason))
      end

    new_conns = :lists.keystore(endpoint, conn(:endpoint), conns, conn)

    result =
      case conn(conn, :pid) do
        p when is_pid(p) -> {:ok, p}
        {:dead_since, _, r} -> {:error, r}
      end

    {result, state(state, payload_conns: new_conns)}
  end

  def do_connect(endpoint, state) do
    conn_config = conn_config(state)
    :kpro.connect(endpoint, conn_config)
  end

  @doc """
  Handle connection pid EXIT event, for payload sockets keep the timestamp,
  but do not restart yet. Payload connection will be re-established when a
  per-partition worker restarts and requests for a connection after
  it is cooled down.
  """
  @spec handle_connection_down(state(), pid(), any()) :: state()
  def handle_connection_down(state, pid, reason) do
    if state(state, :meta_conn) == pid do
      state(state, meta_conn: :undef)
    else
      conns = state(state, :payload_conns)
      client_id = state(state, :client_id)

      case :lists.keytake(pid, conn(:pid), conns) do
        {:value, conn, rest} ->
          {host, port} = conn(conn, :endpoint)

          msg =
            "client #{client_id}: payload connection down #{host}:#{port}\nreason:#{
              inspect(reason)
            }"

          Logger.info(msg)
          new_conn = conn(conn, pid: mark_dead(reason))
          state(state, payload_conns: [new_conn | rest])

        false ->
          # stale EXIT message
          state
      end
    end
  end

  def mark_dead(reason) do
    {:dead_since, :os.timestamp(), reason}
  end

  @spec find_conn(endpoint(), [conn_state()]) ::
          {:ok, pid()} | {:error, :not_found} | {:error, dead_conn()}
  def find_conn(endpoint, conns) do
    case :lists.keyfind(endpoint, conn(:endpoint), conns) do
      false ->
        {:error, :not_found}

      val ->
        pid = conn(val, :pid)

        if is_pid(pid) do
          {:ok, pid}
        else
          not_alive = {:dead_since, :os.timestamp(), pid}
          {:error, not_alive}
        end
    end
  end

  # Check if the connection is down for long enough to retry.
  def is_cooled_down(ts, state) do
    config = state(state, :config)
    threshold = config(:reconnect_cool_down_seconds, config, @default_reconnect_cool_down_seconds)
    now = :os.timestamp()
    diff = div(:timer.now_diff(now, ts), 1_000_000)
    diff >= threshold
  end

  def config(key, config, default) do
    :proplists.get_value(key, config, default)
  end

  def ensure_binary(client_id) when is_atom(client_id) do
    ensure_binary(:erlang.atom_to_binary(client_id, :utf8))
  end

  def ensure_binary(client_id) when is_binary(client_id) do
    client_id
  end

  defp ets(client_id), do: client_id

  defp producer_key(topic, partition) do
    {:producer, topic, partition}
  end

  defp consumer_key(topic, partition) do
    {:consumer, topic, partition}
  end
end
