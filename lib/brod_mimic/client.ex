defmodule BrodMimic.Client do
  @moduledoc """
  GenServer responsible for establishing and maintaining tcp sockets connecting to Kafka brokers.
  It also manages per-topic-partition producer and consumer processes under two-level supervision trees.

  You can start clients automatically at application startup or on demand.

  (mimics [brod_client](https://github.com/kafka4beam/brod/blob/master/src/brod_client.erl)).
  """
  use BrodMimic.Macros
  use GenServer

  import Record, only: [defrecord: 2, defrecord: 3, extract: 2]

  alias BrodMimic.ConsumersSup, as: BrodConsumersSup
  alias BrodMimic.KafkaRequest
  alias BrodMimic.ProducersSup, as: BrodProducersSup

  require Logger

  @producer_supervisor_down "client ~p producers supervisor down~nreason: ~p"
  @consumer_supervisor_down "client ~p consumers supervisor down~nreason: ~p"
  @unexpected_info "~p [~p] ~p got unexpected info: ~p"

  defrecord(:kpro_rsp, extract(:kpro_rsp, from_lib: "kafka_protocol/include/kpro.hrl"))

  defrecord(:r_state, :state,
    client_id: :undefined,
    bootstrap_endpoints: :undefined,
    meta_conn: :undefined,
    payload_conns: [],
    producers_sup: :undefined,
    consumers_sup: :undefined,
    config: :undefined,
    workers_tab: :undefined
  )

  @default_reconnect_cool_down_seconds 1
  @default_get_metadata_timeout_seconds 5

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
  defrecord(:conn, endpoint: nil, pid: nil)
  @type conn :: record(:conn, endpoint: endpoint(), pid: connection() | dead_conn())

  @type conn_state() :: conn()
  @type r_state ::
          record(:r_state,
            client_id: client_id(),
            bootstrap_endpoints: [endpoint()],
            meta_conn: :undefined | connection(),
            payload_conns: [conn_state()],
            producers_sup: :undefined | pid(),
            consumers_sup: :undefined | pid(),
            config: :undefined | config(),
            workers_tab: :undefined | :ets.table()
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

  @doc """
  Stop the GenServer specified (the client) and monitor it
  to wait for it to exit
  """
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
    get_partition_worker(client, {:consumer, topic, partition})
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

      {:error, {:producer_not_found, ^topic_name}} ->
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
  @spec get_metadata(client(), :all | :undefined | topic()) ::
          {:ok, :kpro.struct()} | {:error, any()}
  def get_metadata(client, :undefined) do
    get_metadata(client, :all)
  end

  def get_metadata(client, topic) do
    safe_gen_call(client, {:get_metadata, topic}, :infinity)
  end

  @doc """
  Ensure not topic auto creation even if Kafka has it enabled.
  """
  @spec get_metadata_safe(client(), topic()) :: {:ok, :kpro.struct()} | {:error, any()}
  def get_metadata_safe(client, topic) do
    safe_gen_call(client, {:get_metadata, {_fetch_metdata_for_topic = :all, topic}}, :infinity)
  end

  @doc """
  Get number of partitions for a given topic.
  """
  @spec get_partitions_count(client(), topic()) :: {:ok, pos_integer()} | {:error, any()}
  def get_partitions_count(client, topic) do
    # the name of the ets table that stores this data
    # is the same as the atom client id
    get_partitions_count(client, topic, %{allow_topic_auto_creation: true})
  end

  def get_partitions_count(client, topic, opts) when is_atom(client) do
    do_get_partitions_count(client, client, topic, opts)
  end

  def get_partitions_count(client, topic, opts) when is_pid(client) do
    case safe_gen_call(client, :get_workers_table, :infinity) do
      {:ok, ets} ->
        do_get_partitions_count(client, ets, topic, opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Get number of partitions for an existing topic.

  Ensured not to auto create a topic even when Kafka is configured
  with topic auto creation enabled.
  """
  @spec get_partitions_count_safe(client(), topic()) :: {:ok, pos_integer()} | {:error, any()}
  def get_partitions_count_safe(client, topic) do
    get_partitions_count(client, topic, %{allow_topic_auto_creation: false})
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
  Register this process as a partition producer. The pid is registered in an
  [ETS](https://erlang.org/doc/man/ets.html) table. This allows callers to
  lookup a producer pid from the table and make produce requests to the producer
  process directly.
  """
  @spec register_producer(client(), topic(), partition()) :: :ok
  def register_producer(client, topic, partition) do
    producer = self()
    key = producer_key(topic, partition)
    GenServer.cast(client, {:register, key, producer})
  end

  @doc """
  De-register the producer for a partition. The partition producer
  entry is deleted from the ETS table to allow cleanup of purposefully
  stopped producers and allow later restart.
  """
  @spec deregister_producer(client(), topic(), partition()) :: :ok
  def deregister_producer(client, topic, partition) do
    key = producer_key(topic, partition)
    GenServer.cast(client, {:deregister, key})
  end

  @doc """
  Register this process as a partition consumer. The pid is registered in an
  [ETS](https://erlang.org/doc/man/ets.html) table. This allows callers to
  lookup a consumer pid from the table ane make subscribe calls to the process
  directly.
  """
  @spec register_consumer(client(), topic(), partition()) :: :ok
  def register_consumer(client, topic, partition) do
    consumer = self()
    key = consumer_key(topic, partition)
    GenServer.cast(client, {:register, key, consumer})
  end

  @doc """
  De-register the consumer for a partition. The partition consumer
  entry is deleted from the ETS table to allow cleanup of purposefully
  stopped consumers and allow later restart.
  """
  @spec deregister_consumer(client(), topic(), partition()) :: :ok
  def deregister_consumer(client, topic, partition) do
    key = consumer_key(topic, partition)
    GenServer.cast(client, {:deregister, key})
  end

  @impl true
  def init({bootstrap_endpoints, client_id, config}) do
    Process.flag(:trap_exit, true)
    ets_options = [:named_table, :protected, {:read_concurrency, true}]
    tab = :ets.new(client_id, ets_options)

    {:ok,
     r_state(
       client_id: client_id,
       bootstrap_endpoints: bootstrap_endpoints,
       config: config,
       workers_tab: tab
     )}
  end

  @impl true
  def handle_info(:init, state0) do
    endpoints = state0.bootstrap_endpoints
    state1 = ensure_metadata_connection(state0)
    {:ok, producers_sup_pid} = BrodProducersSup.start_link()
    {:ok, consumers_sup_pid} = BrodConsumersSup.start_link()

    state =
      r_state(state1,
        bootstrap_endpoints: endpoints,
        producers_sup: producers_sup_pid,
        consumers_sup: consumers_sup_pid
      )

    {:noreply, state}
  end

  def handle_info(
        {:EXIT, pid, reason},
        r_state(client_id: client_id, producers_sup: pid) = state
      ) do
    error_string = :io_lib.format(@producer_supervisor_down, [client_id, pid, reason])
    Logger.error(error_string, %{domain: [:brod]})
    {:stop, {:producers_sup_down, reason}, state}
  end

  def handle_info(
        {:EXIT, pid, reason},
        r_state(client_id: client_id, consumers_sup: pid) = state
      ) do
    error_string = :io_lib.format(@consumer_supervisor_down, [client_id, pid, reason])
    Logger.error(error_string, %{domain: [:brod]})
    {:stop, {:consumers_sup_down, reason}, state}
  end

  def handle_info({:EXIT, pid, reason}, state) do
    new_state = handle_connection_down(state, pid, reason)
    {:noreply, new_state}
  end

  def handle_info(info, state) do
    error_string =
      :io_lib.format(@unexpected_info, [:brod_client, self(), r_state(state, :client_id), info])

    Logger.warning(error_string, %{domain: [:brod]})
    {:noreply, state}
  end

  @impl true
  def handle_call({:stop_producer, topic}, _from, state) do
    :ok = BrodProducersSup.stop_producer(r_state(state, :producers_sup), topic)
    {:reply, :ok, state}
  end

  def handle_call({:stop_consumer, topic}, _from, state) do
    :ok = BrodConsumersSup.stop_consumer(r_state(state, :consumers_sup), topic)
    {:reply, :ok, state}
  end

  def handle_call({:get_leader_connection, topic, partition}, _from, state) do
    {result, new_state} = do_get_leader_connection(state, topic, partition)
    {:reply, result, new_state}
  end

  def handle_call({:get_connection, host, port}, _from, state) do
    {result, new_state} = maybe_connect(state, {host, port})
    {:reply, result, new_state}
  end

  def handle_call({:get_group_coordinator, group_id}, _from, state) do
    {result, new_state} = do_get_group_coordinator(state, group_id)
    {:reply, result, new_state}
  end

  def handle_call({:start_producer, topic_name, producer_config}, _from, state) do
    {reply, new_state} = do_start_producer(topic_name, producer_config, state)
    {:reply, reply, new_state}
  end

  def handle_call({:start_consumer, topic_name, consumer_config}, _from, state) do
    {reply, new_state} = do_start_consumer(topic_name, consumer_config, state)
    {:reply, reply, new_state}
  end

  def handle_call({:auto_start_producer, topic}, _from, state) do
    config = r_state(state, :config)

    case config(:auto_start_producers, config, false) do
      true ->
        producer_config = config(:default_producer_config, config, [])
        {reply, new_state} = do_start_producer(topic, producer_config, state)
        {:reply, reply, new_state}

      false ->
        {:reply, {:error, :disabled}, state}
    end
  end

  def handle_call(:get_workers_table, _from, state) do
    {:reply, {:ok, r_state(state, :workers_tab)}, state}
  end

  def handle_call(:get_producers_sup_pid, _from, state) do
    {:reply, {:ok, r_state(state, :producers_sup)}, state}
  end

  def handle_call(:get_consumers_sup_pid, _from, state) do
    {:reply, {:ok, r_state(state, :consumers_sup)}, state}
  end

  def handle_call({:get_metadata, topic}, _from, state) do
    {result, new_state} = do_get_metadata(topic, state)
    {:reply, result, new_state}
  end

  def handle_call(:stop, _from, state) do
    {:stop, :normal, :ok, state}
  end

  def handle_call(call, _from, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  @impl true
  def handle_cast({:register, key, pid}, r_state(workers_tab: tab) = state) do
    :ets.insert(tab, {key, pid})
    {:noreply, state}
  end

  def handle_cast({:deregister, key}, r_state(workers_tab: tab) = state) do
    :ets.delete(tab, key)
    {:noreply, state}
  end

  def handle_cast(cast, state) do
    client_id = r_state(state, :client_id)
    msg = "#{__MODULE__}, #{inspect(self())}, #{client_id} got unexpected cast: #{inspect(cast)})"
    Logger.warn(msg)
    {:noreply, state}
  end

  ### Internal use

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
    case lookup_partition_worker(client_id, client_id, key) do
      {:ok, pid} ->
        case Process.alive?(pid) do
          true ->
            {:ok, pid}

          false ->
            get_partition_worker_with_ets(client_id, key)
        end

      other ->
        other
    end
  end

  defp get_partition_worker_with_ets(client, key) do
    case safe_gen_call(client, :get_workers_table, :infinity) do
      {:ok, ets} ->
        lookup_partition_worker(client, ets, key)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec lookup_partition_worker(client(), :ets.tab(), partition_worker_key()) ::
          {:ok, pid()}
          | {:error, get_worker_error()}
  defp lookup_partition_worker(client, ets, key) do
    case :ets.lookup(ets, key) do
      [] ->
        # not yet registered, 2 possible reasons:
        # 1. caller is too fast, producers/consumers are starting up
        #    make a synced call all the way down the supervision tree
        #    to the partition producer sup should resolve the race
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
  defp find_partition_worker(client, {:producer, topic, partition}) do
    find_producer(client, topic, partition)
  end

  defp find_partition_worker(client, {:consumer, topic, partition}) do
    find_consumer(client, topic, partition)
  end

  @spec find_producer(client(), topic(), partition()) ::
          {:ok, pid()} | {:error, get_producer_error()}
  def find_producer(client, topic, partition) do
    case safe_gen_call(client, :get_producers_sup_pid, :infinity) do
      {:ok, sup_pid} ->
        BrodProducersSup.find_producer(sup_pid, topic, partition)
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
        # MODIFY brod_consumers_sup:find_consumer(sup_id, topic, partition)
        {sup_pid, topic, partition}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def validate_topic_existence(topic, r_state(workers_tab: ets) = state, is_retry) do
    case lookup_partitions_count_cache(ets, topic) do
      {:ok, _count} ->
        {:ok, state}

      {:error, reason} ->
        {{:error, reason}, state}

      false when is_retry ->
        {{:error, :unknown_topic_or_partition}, state}

      false ->
        # Try fetch metadata (and populate partition count cache)
        # Then validate topic existence again.
        get_metadata_result = do_get_metadata_safe(topic, state)
        with_ok_func = fn _, s -> validate_topic_existence(topic, s, true) end
        with_ok(get_metadata_result, with_ok_func)
    end
  end

  # Continue with {{ok, Result}, NewState}
  # return whatever error immediately.
  defp with_ok({:ok, state}, continue) do
    continue.(:ok, state)
  end

  defp with_ok({{:ok, ok}, state}, continue) do
    continue.(ok, state)
  end

  defp with_ok({{:error, _}, r_state()} = return, _continue) do
    return
  end

  # If allow_topic_auto_creation is set 'false',
  # do not try to fetch metadata per topic name, fetch all topics instead.
  # As sending metadata request with topic name will cause an auto creation
  # of the topic if auto.create.topics.enable is enabled in broker config.
  defp do_get_metadata_safe(topic0, r_state(config: config) = state) do
    topic =
      case config(:allow_topic_auto_creation, config, true) do
        true -> topic0
        false -> {:all, topic0}
      end

    do_get_metadata(topic, state)
  end

  defp do_get_metadata({:all, topic}, state) do
    do_get_metadata(:all, topic, state)
  end

  defp do_get_metadata(topic, state) when not is_tuple(topic) do
    do_get_metadata(topic, topic, state)
  end

  defp do_get_metadata(
         fetch_metadata_for,
         topic,
         r_state(client_id: client_id, workers_tab: ets) = state0
       ) do
    topics =
      case fetch_metadata_for do
        :all -> :all
        _ -> [topic]
      end

    state = ensure_metadata_connection(state0)
    conn = get_metadata_connection(state)
    request = KafkaRequest.metadata(conn, topics)

    case request_sync(state, request) do
      {:ok, kpro_rsp(api: :metadata, msg: metadata)} ->
        topic_metadata_array = kf(:topics, metadata)
        :ok = update_partitions_count_cache(ets, topic_metadata_array)
        {{:ok, metadata}, state}

      {:error, reason} ->
        Logger.error(
          "#{inspect(client_id)} failed to fetch metadata for topics: #{inspect(topics)}\nreason=#{inspect(reason)}"
        )

        {{:error, reason}, state}
    end
  end

  @doc """
  Ensure there is at least one metadata connection
  """
  def ensure_metadata_connection(
        r_state(bootstrap_endpoints: endpoints, meta_conn: :undefined) = state
      ) do
    conn_config = conn_config(state)

    pid =
      case :kpro.connect_any(endpoints, conn_config) do
        {:ok, pid_x} -> pid_x
        {:error, reason} -> Process.exit(self(), reason)
      end

    r_state(state, meta_conn: pid)
  end

  def ensure_metadata_connection(state) do
    state
  end

  # must be called after ensure_metadata_connection
  defp get_metadata_connection(r_state(meta_conn: conn)), do: conn

  defp do_get_leader_connection(state0, topic, partition) do
    state = ensure_metadata_connection(state0)
    meta_conn = get_metadata_connection(state)
    timeout = timeout(state)

    case :kpro.discover_partition_leader(meta_conn, topic, partition, timeout) do
      {:ok, endpoint} -> maybe_connect(state, endpoint)
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  def do_get_group_coordinator(state0, group_id) do
    state = ensure_metadata_connection(state0)
    meta_conn = get_metadata_connection(state)
    timeout = timeout(state)

    case :kpro.discover_coordinator(meta_conn, :group, group_id, timeout) do
      {:ok, endpoint} ->
        {{:ok, {endpoint, conn_config(state)}}, state}

      {:error, reason} ->
        {{:error, reason}, state}
    end
  end

  defp timeout(r_state(config: config)) do
    timeout(config)
  end

  defp timeout(config) do
    t = config(:get_metadata_timeout_seconds, config, @default_get_metadata_timeout_seconds)
    :timer.seconds(t)
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

    :exit, {reason, _} ->
      {:error, {:client_down, reason}}
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

  # Get partition counter from cache.
  #
  # If cache is not hit, send meta data request to retrieve.
  @spec do_get_partitions_count(client(), :ets.tab(), topic(), %{
          required(:allow_topic_auto_creation) => boolean()
        }) :: {:ok, pos_integer()} | {:error, any()}
  defp do_get_partitions_count(client, ets, topic, %{
         allow_topic_auto_creation: allow_auto_creation
       }) do
    case lookup_partitions_count_cache(ets, topic) do
      {:ok, result} ->
        {:ok, result}

      {:error, reason} ->
        {:error, reason}

      false ->
        metadata_response =
          case allow_auto_creation do
            true ->
              get_metadata(client, topic)

            false ->
              get_metadata_safe(client, topic)
          end

        find_partition_count_in_metadata(metadata_response, topic)
    end
  end

  defp find_partition_count_in_metadata({:ok, meta}, topic) do
    topic_metadata_arrary = kf(:topics, meta)

    find_partition_count_in_topic_metadata_array(topic_metadata_arrary, topic)
  end

  defp find_partition_count_in_metadata({:error, reason}, _) do
    {:error, reason}
  end

  defp find_partition_count_in_topic_metadata_array(topic_metadata_arrary, topic) do
    filter_f = fn
      %{name: n} when n === topic ->
        true

      _ ->
        false
    end

    case :lists.filter(filter_f, topic_metadata_arrary) do
      [topic_metadata] ->
        get_partitions_count_in_metadata(topic_metadata)

      [] ->
        {:error, :unknown_topic_or_partition}
    end
  end

  @doc """
  Looks up the partition count in [ETS](https://erlang.org/doc/man/ets.html)
  """
  @spec lookup_partitions_count_cache(:ets.table(), :undefined | topic()) ::
          {:ok, pos_integer()}
          | {:error, any()}
          | false
  def lookup_partitions_count_cache(_ets, :undefined) do
    false
  end

  def lookup_partitions_count_cache(ets, topic) do
    case :ets.lookup(ets, topic_metadata_key(topic)) do
      [{_, count, _ts}] when is_integer(count) ->
        {:ok, count}

      [{_, {:error, reason}, ts}] ->
        ts_diff = :timer.now_diff(:os.timestamp(), ts)

        case ts_diff <= unknown_topic_cache_expire_seconds() * 1_000_000 do
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
        producer_key = producer_key(topic, partition)
        get_partition_worker(client, producer_key)

      {:error, :disabled} ->
        error

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec get_partitions_count_in_metadata(:kpro.struct()) :: {:ok, pos_integer()} | {:error, any()}
  def get_partitions_count_in_metadata(topic_metadata) do
    error_code = kf(:error_code, topic_metadata)
    partitions = kf(:partitions, topic_metadata)

    case is_error(error_code) do
      true -> {:error, error_code}
      false -> {:ok, length(partitions)}
    end
  end

  def request_sync(state, request) do
    pid = get_metadata_connection(state)
    timeout = timeout(state)
    :kpro.request_sync(pid, request, timeout)
  end

  def do_start_producer(topic_name, producer_config, state) do
    sup_pid = r_state(state, :producers_sup)
    f = fn -> BrodProducersSup.start_producer(sup_pid, self(), topic_name, producer_config) end
    ensure_partition_workers(topic_name, state, f)
  end

  def do_start_consumer(topic_name, consumer_config, state) do
    sup_pid = r_state(state, :consumers_sup)
    f = fn -> BrodConsumersSup.start_consumer(sup_pid, self(), topic_name, consumer_config) end
    ensure_partition_workers(topic_name, state, f)
  end

  defp ensure_partition_workers(topic_name, state, f) do
    validate_topic_result = validate_topic_existence(topic_name, state, _is_retry = false)

    with_ok_func = fn :ok, new_state ->
      case f.() do
        {:ok, _pid} ->
          {:ok, new_state}

        {:error, {:already_started, _pid}} ->
          {:ok, new_state}

        {:error, reason} ->
          {{:error, reason}, new_state}
      end
    end

    with_ok(validate_topic_result, with_ok_func)
  end

  def conn_config(r_state(client_id: client_id, config: config)) do
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

  @spec maybe_connect(r_state(), endpoint()) :: {{:ok, pid()} | {:error, any()}, r_state()}
  def maybe_connect(state, endpoint) do
    case find_conn(endpoint, r_state(state, :payload_conns)) do
      {:ok, pid} -> {{:ok, pid}, state}
      {:error, reason} -> maybe_connect(state, endpoint, reason)
    end
  end

  @spec maybe_connect(r_state(), endpoint(), :not_found | dead_conn()) ::
          {{:ok, pid()} | {:error, any()}, r_state()}
  def maybe_connect(state, endpoint, :not_found) do
    # connect for the first time
    connect(state, endpoint)
  end

  # state{client_id = ClientId} = State,
  def maybe_connect(
        r_state(client_id: client_id) = state,
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

  @spec connect(r_state(), endpoint()) :: {{:ok, pid()} | {:error, any()}, r_state()}
  def connect(
        r_state(client_id: client_id, payload_conns: conns) = state,
        {host, port} = endpoint
      ) do
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

    {result, r_state(state, payload_conns: new_conns)}
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
  def handle_connection_down(state, pid, reason) do
    if r_state(state, :meta_conn) == pid do
      r_state(state, meta_conn: :undefined)
    else
      conns = r_state(state, :payload_conns)
      client_id = r_state(state, :client_id)

      case :lists.keytake(pid, conn(:pid), conns) do
        {:value, conn, rest} ->
          {host, port} = conn(conn, :endpoint)

          msg =
            "client #{client_id}: payload connection down #{host}:#{port}\nreason:#{inspect(reason)}"

          Logger.info(msg)
          new_conn = conn(conn, pid: mark_dead(reason))
          r_state(state, payload_conns: [new_conn | rest])

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
    config = r_state(state, :config)
    threshold = config(:reconnect_cool_down_seconds, config, @default_reconnect_cool_down_seconds)
    now = :os.timestamp()
    diff = div(:timer.now_diff(now, ts), 1_000_000)
    diff >= threshold
  end

  @spec update_partitions_count_cache(:ets.tab(), [:kpro.struct()]) :: :ok
  defp update_partitions_count_cache(_ets, []), do: :ok

  defp update_partitions_count_cache(ets, [topic_metadata | rest]) do
    topic = kf(:name, topic_metadata)

    case get_partitions_count_in_metadata(topic_metadata) do
      {:ok, cnt} ->
        :ets.insert(ets, {{:topic_metadata, topic}, cnt, :os.timestamp()})

      {:error, :unknown_topic_or_partition} = err ->
        :ets.insert(ets, {{:topic_metadata, topic}, err, :os.timestamp()})

      {:error, _reason} ->
        :ok
    end

    update_partitions_count_cache(ets, rest)
  end

  def config(key, config, default) do
    :proplists.get_value(key, config, default)
  end

  def ensure_binary(client_id) when is_atom(client_id) do
    Atom.to_string(client_id)
  end

  def ensure_binary(client_id) when is_binary(client_id) do
    client_id
  end
end
