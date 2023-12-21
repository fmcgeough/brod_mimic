defmodule BrodMimic.Brod do
  @moduledoc """
  Brod helpers and types
  """
  @behaviour Application

  use BrodMimic.Macros

  import Bitwise
  import Record, only: [defrecordp: 2]

  alias BrodMimic.Client, as: BrodClient
  alias BrodMimic.Consumer, as: BrodConsumer
  alias BrodMimic.GroupSubscriber, as: BrodGroupSubscriber
  alias BrodMimic.GroupSubscriberv2, as: BrodGroupSubscriberv2
  alias BrodMimic.Producer, as: BrodProducer
  alias BrodMimic.Sup, as: BrodSup
  alias BrodMimic.TopicSubscriber, as: BrodTopicSubscriber
  alias BrodMimic.Utils, as: BrodUtils

  defrecordp(:brod_call_ref, caller: :undefined, callee: :undefined, ref: :undefined)

  ### Types created for Elixir port ============================================
  @type ets_table_id() :: atom() | term()
  @type ets_table() :: atom() | ets_table_id()
  @type req() :: :kpro.req()

  ### Types ====================================================================

  ## basics

  @type hostname() :: :kpro.hostname()
  @type portnum() :: pos_integer()
  @type endpoint() :: {hostname(), portnum()}
  @type topic_config() :: :kpro.struct()
  @type topic_partition() :: {topic(), partition()}
  @type key() :: :undefined | binary()
  #    no value, transformed to <<>>
  @type value() ::
          :undefined
          # single value
          | iodata()
          # one message with timestamp
          | {msg_ts(), binary()}
          # backward compatible
          | [{key(), value()}]
          # backward compatible
          | [{msg_ts(), key(), value()}]
          # one magic v2 message
          | :kpro.msg_input()
          # maybe nested batch
          | :kpro.batch_input()

  @type msg_input() :: :kpro.msg_input()
  @type batch_input() :: [msg_input()]

  @type msg_ts() :: :kpro.msg_ts()
  @type client_config() :: BrodMimic.Client.config()
  # default client config
  @type bootstrap() :: [endpoint()] | {[endpoint()], client_config()}

  ## producers
  @type producer_config() :: BrodMimic.Producer.config()
  @type partition_fun() :: (topic(), pos_integer(), key(), value() -> {:ok, partition()})
  @type partitioner() :: partition_fun() | :random | :hash
  @type produce_ack_cb() :: (partition(), offset() -> any())
  @type compression() :: :no_compression | :gzip | :snappy

  @typedoc """
  A call reference Record
  """
  @type call_ref() ::
          record(:brod_call_ref,
            caller: :undefined | pid(),
            callee: :undefined | pid(),
            ref: :undefined | reference()
          )
  @type produce_result() :: :brod_produce_req_buffered | :brod_produce_req_acked

  ## consumers

  @typedoc """
  The types of options allowed for a consumer, see `t:consumer_config/0`
  """
  @type consumer_option() ::
          :begin_offset
          | :min_bytes
          | :max_bytes
          | :max_wait_time
          | :sleep_timeout
          | :prefetch_count
          | :prefetch_bytes
          | :offset_reset_policy
          | :size_stat_window
  @type consumer_options() :: [{consumer_option(), integer()}]
  @type connection() :: :kpro.connection()

  @typedoc """
  Connection configuration that will be passed to kpro calls.

  This is defined in the `kafka_protocol` library.

  It is a tuple list or map with following keys (all of them are optional):

  - `:connection_timeout`: timeout (in ms) for the initial connection, defaults to 5 seconds
  - `:client_id`: string representing the client in Kafka, defaults to "kpro-client"
  - `:extra_sock_opts`: extra options passed down to `gen_tpc`, defaults to []
  - `:debug`: debugging mode, defaults to false
  - `:nolink`: whether not to link the `kpro_connection` process to the caller, defaults to false
  - `:query_api_version`: whether to query Kafka for supported API versions at the beginning,
    so that `kpro` can use newer APIs; the `ApiVersionRequest` was introduced in Kafka 0.10,
    so set this to false when using an older version of Kafka; defaults to true
  - `:request_timeout`: timeout (in ms) for the actual request, defaults to 4 minutes
  - `:sasl`: configuration of SASL authentication, can be either `{mechanism, username, password}`
     or `{mechanism, file}` or `:undefined`, where `mechanism` is `:plain | :scram_sha_256 | :scram_sha_512`,
     and `file` is the path to a text file which contains two lines, first line for username
     and second line for password; defaults to `:undefined`
  - `:ssl`: whether to use SSL, defaults to `false`, more information can be found in
     [brod documentation](https://hexdocs.pm/brod/authentication.html).
  """
  @type conn_config() :: [{atom(), term()}] | :kpro.conn_config()

  ## consumer groups
  @type group_member_id() :: binary()
  @type group_member() :: {group_member_id(), kafka_group_member_metadata()}
  @type group_generation_id() :: non_neg_integer()
  @type group_config() :: keyword()
  @type partition_assignment() :: {topic(), [partition()]}
  @type received_assignments() :: [brod_received_assignment()]
  @type fetch_opts() :: :kpro.fetch_opts()
  @type fold_acc() :: term()
  @type fold_stop_reason() ::
          :reached_end_of_partition
          | :reached_message_count_limit
          | :reached_target_offset
          | {:error, any()}

  @type get_consumer_error() ::
          :client_down
          | {:client_down, any()}
          | {:consumer_down, any()}
          | {:consumer_not_found, topic()}
          | {:consumer_not_found, topic(), partition()}

  @type get_producer_error() ::
          :client_down
          | {:client_down, any()}
          | {:producer_down, any()}
          | {:producer_not_found, topic()}
          | {:producer_not_found, topic(), partition()}

  @typedoc """
  Consumer configuration

  ## Keys

  - `:min_bytes` (optional, default = 0). Minimal bytes to fetch in a batch of
    messages
  - `:max_bytes` (optional, default = 1MB). Maximum bytes to fetch in a batch
      of messages. _This value might be expanded to retry when it is not
      enough to fetch even a single message, then slowly shrunk back to the
      given value_.
  - `:max_wait_time` (optional, default = 10_000 ms). Max number of seconds
    allowed for the broker to collect `min_bytes` of messages in fetch
    response
  - `:sleep_timeout` (optional, default = 1000 ms). Allow consumer process to
    sleep this amount of ms if Kafka replied `empty` message set.
  - `:prefetch_count` (optional, default = 10). The window size (number of
    messages) allowed to fetch-ahead.
  - `:prefetch_bytes` (optional, default = 100KB). The total number of bytes
    allowed to fetch-ahead. The `BrodMimic.Consumer` is greedy, it only stops
    fetching more messages when number of unacked messages has exceeded
    the `:prefetch_count` value and the unacked total volume has exceeded
    the `:prefetch_bytes` value.
  - `:begin_offset` (optional, default = `:latest`). The offset from which to
    begin fetch requests. A subscriber may consume and process messages,
    then persist the associated offset to a persistent storage, then start
    (or restart) from `last_processed_offset + 1` as the `:begin_offset` to
    proceed. The offset has to already exist at the time of calling.
  - `:offset_reset_policy` (optional, default = `:reset_by_subscriber`). How to
    reset `begin_offset` if `:offset_out_of_range` exception is received:
    - `:reset_by_subscriber`: consumer is suspended (`is_suspended: true` in
      state) and wait for subscriber to re-subscribe with a new
      `:begin_offset` option.
    - `:reset_to_earliest`: consume from the earliest offset.
    - `:reset_to_latest`: consume from the last available offset.
  - `:size_stat_window`: (optional, default = 5). The moving-average window
    size to calculate average message size.  Average message size is used to
    shrink `max_bytes` in fetch requests after it has been expanded to fetch a
    large message. Use 0 to immediately shrink back to original `:max_bytes`
    from config.  A size estimation allows users to set a relatively small
    `:max_bytes`, then let it dynamically adjust to a number around
    `prefetch_count * average_size`
  - `:isolation_level`: (optional, default = `:read_commited`). Level to
    control what transaction records are exposed to the consumer. Two values
    are allowed, `:read_uncommitted` to retrieve all records, independently on
    the transaction outcome (if any), and `:read_committed` to get only the
    records from committed transactions
  """
  @type consumer_config() :: [
          {:begin_offset, offset_time()}
          | {:min_bytes, non_neg_integer()}
          | {:max_bytes, non_neg_integer()}
          | {:max_wait_time, integer()}
          | {:sleep_timeout, integer()}
          | {:prefetch_count, integer()}
          | {:prefetch_bytes, non_neg_integer()}
          | {:offset_reset_policy, BrodConsumer.offset_reset_policy()}
          | {:size_stat_window, non_neg_integer()}
          | {:isolation_level, BrodConsumer.isolation_level()}
        ]

  @doc """
  Start the BrodMimic application
  """
  def start do
    {:ok, _apps} = Application.ensure_all_started(:brod_mimic)
    :ok
  end

  @doc """
  Stop the BrodMimic application
  """
  def stop do
    Application.stop(:brod_mimic)
  end

  @doc """
  Application behaviour callback
  """
  @impl Application
  def start(_start_type, _start_args) do
    BrodSup.start_link()
  end

  @doc """
  Application behaviour callback
  """
  @impl Application
  def stop(_state) do
    :ok
  end

  def start_client(bootstrap_endpoints) do
    start_client(bootstrap_endpoints, :brod_default_client)
  end

  def start_client(bootstrap_endpoints, client_id) do
    start_client(bootstrap_endpoints, client_id, [])
  end

  @doc """
  Start a client

   - `bootstrap_endpoints`: Kafka cluster endpoints, can be any of the brokers
      in the cluster, which does not necessarily have to be the leader of any
      partition, e.g. a load-balanced entrypoint to the remote Kafka cluster.
   - `client_id`: Atom to identify the client process
   - `config` is a proplist. See `t:client_config/0`
  """
  def start_client(bootstrap_endpoints, client_id, config) do
    case BrodSup.start_client(bootstrap_endpoints, client_id, config) do
      :ok ->
        :ok

      {:error, {:already_started, _pid}} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  The equivalent of `start_link_client(bootstrap_endpoints, :brod_default_client)`
  """
  @spec start_link_client([endpoint()]) :: {:ok, pid()} | {:error, any()}
  def start_link_client(bootstrap_endpoints) do
    start_link_client(bootstrap_endpoints, :brod_default_client)
  end

  @doc """
  The equivalent of `start_link_client(bootstrap_endpoints, client_id, [])`
  """
  @spec start_link_client([endpoint()], client_id()) :: {:ok, pid()} | {:error, any()}
  def start_link_client(bootstrap_endpoints, client_id) do
    start_link_client(bootstrap_endpoints, client_id, [])
  end

  @spec start_link_client([endpoint()], client_id(), client_config()) ::
          {:ok, pid()} | {:error, any()}
  def start_link_client(bootstrap_endpoints, client_id, config) do
    BrodClient.start_link(bootstrap_endpoints, client_id, config)
  end

  @doc """
  Stop a client
  """
  def stop_client(client) when is_atom(client) do
    case BrodSup.find_client(client) do
      [_pid] ->
        BrodSup.stop_client(client)

      [] ->
        BrodClient.stop(client)
    end
  end

  def stop_client(client) when is_pid(client) do
    BrodClient.stop(client)
  end

  @doc """
   Dynamically start a per-topic producer and register it in the client.

   You have to start a producer for each topic you want to produce messages
   into, unless you have specified `auto_start_producers: true` when starting
   the client (in that case you don't have to call this function at all).

   After starting the producer, you can call `produce/5` and friends
   for producing messages.

   A client has to be already started before making this call (e.g. by calling
   `BrodMimic.Brod.start_client/3`.

   See `BrodMimic.Producer.start_link/4` for a list of available configuration
   options.

   Example:

   ```
   iex> BrodMimic.Brod.start_producer(:my_client, "my_topic", [{:max_retries, 5}])
   :ok
   ```
  """
  def start_producer(client, topic_name, producer_config) do
    BrodClient.start_producer(client, topic_name, producer_config)
  end

  @doc """
  Dynamically start topic consumer(s) and register it in the client.

  A `BrodMimic.Consumer` is started for each partition of the given topic.
  Note that you can have only one consumer per client-topic.

  See `t:consumer_config/0` for details about consumer config.

  You can read more about consumers in the [brod consumers overview](https://hexdocs.pm/brod/readme.html#consumers).
  """
  @spec start_consumer(client(), topic(), consumer_config()) :: :ok | {:error, any()}
  def start_consumer(client, topic_name, consumer_config) do
    BrodClient.start_consumer(client, topic_name, consumer_config)
  end

  @doc """
  Get number of partitions for a given topic.

  The higher level producers may need the partition numbers to
  find the partition producer pid – if the number of partitions
  is not statically configured for them.

  It is up to the callers how they want to distribute their data
  (e.g. random, roundrobin or consistent-hashing) to the partitions.

  _The partitions count is cached for 120 seconds_.
  """
  @spec get_partitions_count(client(), topic()) :: {:ok, pos_integer()} | {:error, any()}
  def get_partitions_count(client, topic) do
    BrodClient.get_partitions_count(client, topic)
  end

  @doc """
  The same as `get_partitions_count/2` but ensured not to auto-create topics in Kafka even
  when Kafka has topic auto-creation configured.
  """
  @spec get_partitions_count_safe(client(), topic()) :: {:ok, pos_integer()} | {:error, any()}
  def get_partitions_count_safe(client, topic) do
    BrodClient.get_partitions_count_safe(client, topic)
  end

  @spec get_consumer(client(), topic(), partition()) ::
          {:ok, pid()} | {:error, get_consumer_error()}
  def get_consumer(client, topic, partition) do
    BrodClient.get_consumer(client, topic, partition)
  end

  @doc """
  Equivalent to `BrodMimic.Client.get_producer/3`
  """
  @spec get_producer(client(), topic(), partition()) ::
          {:ok, pid()} | {:error, get_producer_error()}
  def get_producer(client, topic, partition) do
    BrodClient.get_producer(client, topic, partition)
  end

  @doc """
  Equivalent of `produce(pid, <<>>, value)`
  """
  @spec produce(pid(), value()) :: {:ok, call_ref()} | {:error, any()}
  def produce(pid, value) do
    produce(pid, _key = <<>>, value)
  end

  @doc """
  Produce one or more messages

  The pid should be a partition producer pid, NOT client pid.

  The return value is a call reference of type `call_ref()`, so the caller can
  use it to expect (match) using a `brod_produce_reply` Record after the produce
  request has been acked by Kafka (`result: :brod_produce_req_acked` - See
  `BrodMimic.Producer`).
  """
  @spec produce(pid(), key(), value()) :: {:ok, call_ref()} | {:error, any()}
  def produce(producer_pid, key, value) do
    BrodProducer.produce(producer_pid, key, value)
  end

  def produce(client, topic, partition, key, value) when is_integer(partition) do
    case get_producer(client, topic, partition) do
      {:ok, pid} ->
        produce(pid, key, value)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Produce one or more messages.

  ## Parameters

  - value - can have many different forms:
    - `binary()`: Single message with key from the `key` argument
    - `{Brod.msg_ts(), binary()}`: Single message with its create-time timestamp and key from `key`
  - `{ts: Brod.msg_ts(), value: binary(), headers: [{_, _}]}`: Single message. If this map does not have a `key`
    field, `key` is used instead.
  - `[{k, v} | {t, k, v}]`: A batch, where `v` could be a nested list of such representation
  - `[{key: k, value: v, ts: t, headers: [{_, _}]}]`: A batch

   When `value` is a batch, the `key` argument is only used as partitioner input
   and all messages are written on the same partition.

   `ts` field is dropped for Kafka prior to version `0.10` (produce API version
   0, magic version 0). `headers` field is dropped for Kafka prior to version
   `0.11` (produce API version 0-2, magic version 0-1).

   `partition` may be either a concrete partition (an integer) or a partitioner
   (see `partitioner/0` for more info).

   A producer for the particular topic has to be already started (by calling
   `start_producer/3`, unless you have specified `auto_start_producers: true`
   when starting the client.

   This function first looks up the producer pid, then calls `produce/3` to do
   the real work.

   The return value is a call reference of type `t:call_ref/0`, so the caller
   can used it to expect (match) a `#brod_produce_reply{result =
   brod_produce_req_acked}` (see `t:produce_reply/0`) message after the produce
   request has been acked by Kafka.

   Example:
   ```
   > BrodMimic.Brod.produce(my_client, "my_topic", 0, "key", "Hello from BrodMimic!")
   {:ok, {brod_call_ref,<0.83.0>,<0.133.0>,#Ref<0.3024768151.2556690436.92841>}}
   ```
  """
  def produce(client, topic, partitioner, key, value) do
    part_fun = BrodUtils.make_part_fun(partitioner)

    case BrodClient.get_partitions_count(
           client,
           topic
         ) do
      {:ok, partitions_count} ->
        {:ok, partition} = part_fun.(topic, partitions_count, key, value)
        produce(client, topic, partition, key, value)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def produce_cb(producer_pid, key, value, ack_cb) do
    BrodProducer.produce_cb(producer_pid, key, value, ack_cb)
  end

  def produce_cb(client, topic, part, key, value, ack_cb) when is_integer(part) do
    case get_producer(client, topic, part) do
      {:ok, pid} ->
        produce_cb(pid, key, value, ack_cb)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def produce_cb(client, topic, partitioner, key, value, ack_cb) do
    part_fun = BrodUtils.make_part_fun(partitioner)

    case BrodClient.get_partitions_count(client, topic) do
      {:ok, partitions_count} ->
        {:ok, partition} = part_fun.(topic, partitions_count, key, value)

        case produce_cb(client, topic, partition, key, value, ack_cb) do
          :ok ->
            {:ok, partition}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp produce_no_ack(producer_pid, key, value) do
    BrodProducer.produce_no_ack(producer_pid, key, value)
  end

  def produce_no_ack(client, topic, part, key, value) when is_integer(part) do
    case get_producer(client, topic, part) do
      {:ok, pid} ->
        produce_no_ack(pid, key, value)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def produce_no_ack(client, topic, partitioner, key, value) do
    part_fun = BrodUtils.make_part_fun(partitioner)

    case BrodClient.get_partitions_count(client, topic) do
      {:ok, partitions_count} ->
        {:ok, partition} = part_fun.(topic, partitions_count, key, value)
        produce_no_ack(client, topic, partition, key, value)

      {:error, _reason} ->
        :ok
    end
  end

  def produce_sync(pid, value) do
    produce_sync(pid, _key = <<>>, value)
  end

  def produce_sync(pid, key, value) do
    case produce(pid, key, value) do
      {:ok, call_ref} ->
        sync_produce_request(call_ref)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def produce_sync(client, topic, partition, key, value) do
    case produce_sync_offset(client, topic, partition, key, value) do
      {:ok, _} ->
        :ok

      some_value ->
        some_value
    end
  end

  def produce_sync_offset(client, topic, partition, key, value) do
    case produce(client, topic, partition, key, value) do
      {:ok, call_ref} ->
        sync_produce_request_offset(call_ref)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def sync_produce_request(call_ref) do
    sync_produce_request(call_ref, :infinity)
  end

  def sync_produce_request(call_ref, timeout) do
    case sync_produce_request_offset(call_ref, timeout) do
      {:ok, _} ->
        :ok

      else__ ->
        else__
    end
  end

  def sync_produce_request_offset(call_ref) do
    sync_produce_request_offset(call_ref, :infinity)
  end

  def sync_produce_request_offset(call_ref, timeout) do
    BrodProducer.sync_produce_request(call_ref, timeout)
  end

  def subscribe(client, subscriber_pid, topic, partition, options) do
    case BrodClient.get_consumer(client, topic, partition) do
      {:ok, consumer_pid} ->
        case subscribe(consumer_pid, subscriber_pid, options) do
          :ok ->
            {:ok, consumer_pid}

          error ->
            error
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def subscribe(consumer_pid, subscriber_pid, options) do
    BrodConsumer.subscribe(consumer_pid, subscriber_pid, options)
  end

  def unsubscribe(client, topic, partition) do
    unsubscribe(client, topic, partition, self())
  end

  def unsubscribe(client, topic, partition, subscriber_pid) do
    case BrodClient.get_consumer(client, topic, partition) do
      {:ok, consumer_pid} ->
        unsubscribe(consumer_pid, subscriber_pid)

      error ->
        error
    end
  end

  def unsubscribe(consumer_pid) do
    unsubscribe(consumer_pid, self())
  end

  def unsubscribe(consumer_pid, subscriber_pid) do
    BrodConsumer.unsubscribe(consumer_pid, subscriber_pid)
  end

  @doc """
  Acknowledge that one or more messages have been processed.

  `BrodMimic.Consumer` sends message-sets to the subscriber process, and keep
   the messages in a `pending` queue. The subscriber may choose to ack any
   received offset. Acknowledging a greater offset will automatically
   acknowledge the messages before this offset. For example, if message `[1, 2,
   3, 4]` have been sent to (as one or more message-sets) to the subscriber, the
   subscriber may acknowledge with offset `3` to indicate that the first three
   messages are successfully processed, leaving behind only message `4` pending.


   The `pending` queue has a size limit (see `prefetch_count` consumer config)
   which is to provide a mechanism to handle back-pressure. If there are too
   many messages pending on ack, the consumer will stop fetching new ones so the
   subscriber won't get overwhelmed.

   Note, there is no range check done for the acknowledging offset, meaning if
   offset `[m, n]` are pending to be acknowledged, acknowledging with `offset >
   n` will cause all offsets to be removed from the pending queue, and
   acknowledging with `offset < m` has no effect.

   Use this function only with plain partition subscribers (i.e., when you
   manually call `subscribe/5`). Behaviours like `BrodMimic.TopicSubscriber`
   have their own way how to ack messages.
  """
  @spec consume_ack(client(), topic(), partition(), offset()) :: :ok | {:error, any()}
  def consume_ack(client, topic, partition, offset) do
    case BrodClient.get_consumer(client, topic, partition) do
      {:ok, consumer_pid} ->
        consume_ack(consumer_pid, offset)

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Equivalent to `BrodMimic.Consumer.ack/2`

  See `consume_ack/4` for more information.
  """
  @spec consume_ack(pid(), offset()) :: :ok | {:error, any()}
  def consume_ack(consumer_pid, offset) do
    BrodConsumer.ack(consumer_pid, offset)
  end

  @doc """
  See `BrodMimic.GroupSubscriber.start_link/7`
  """
  @spec start_link_group_subscriber(
          client(),
          group_id(),
          [topic()],
          group_config(),
          consumer_config(),
          module(),
          term()
        ) :: {:ok, pid()} | {:error, any()}
  def start_link_group_subscriber(
        client,
        group_id,
        topics,
        group_config,
        consumer_config,
        cbModule,
        cb_init_arg
      ) do
    BrodGroupSubscriber.start_link(
      client,
      group_id,
      topics,
      group_config,
      consumer_config,
      cbModule,
      cb_init_arg
    )
  end

  @doc """
  Start `BrodMimic.GroupSubscriberv2`
  """
  @spec start_link_group_subscriber_v2(BrodGroupSubscriberv2.subscriber_config()) ::
          {:ok, pid()} | {:error, any()}
  def start_link_group_subscriber_v2(config) do
    BrodGroupSubscriberv2.start_link(config)
  end

  @doc """
  Start `BrodMimic.GroupSubscriber`
  """
  def start_link_group_subscriber(
        client,
        group_id,
        topics,
        group_config,
        consumer_config,
        message_type,
        cb_module,
        cb_init_arg
      ) do
    BrodGroupSubscriber.start_link(
      client,
      group_id,
      topics,
      group_config,
      consumer_config,
      message_type,
      cb_module,
      cb_init_arg
    )
  end

  @deprecated "Please use `start_link_topic_subscriber/1` instead"
  def start_link_topic_subscriber(client, topic, consumer_config, cb_module, cb_init_arg) do
    start_link_topic_subscriber(client, topic, :all, consumer_config, cb_module, cb_init_arg)
  end

  @deprecated "Please use `start_link_topic_subscriber/1` instead"
  def start_link_topic_subscriber(
        client,
        topic,
        partitions,
        consumer_config,
        cb_module,
        cb_init_arg
      ) do
    start_link_topic_subscriber(
      client,
      topic,
      partitions,
      consumer_config,
      :message,
      cb_module,
      cb_init_arg
    )
  end

  @deprecated "Please use `start_link_topic_subscriber/1` instead"
  def start_link_topic_subscriber(
        client,
        topic,
        partitions,
        consumer_config,
        message_type,
        cb_module,
        cb_init_arg
      ) do
    args = %{
      client: client,
      topic: topic,
      partitions: partitions,
      consumer_config: consumer_config,
      message_type: message_type,
      cb_module: cb_module,
      init_data: cb_init_arg
    }

    BrodTopicSubscriber.start_link(args)
  end

  def start_link_topic_subscriber(config) do
    BrodTopicSubscriber.start_link(config)
  end

  @doc """
  Create topics equivalent to `create_topics(host, topic_configs, request_configs, [])`
  """
  def create_topics(hosts, topic_configs, request_configs) do
    BrodUtils.create_topics(hosts, topic_configs, request_configs)
  end

  @doc """
  Create topic(s) in Kafka

  - `topic_configs` - List of topic configurations. A topic configuration is a Map
    (or tuple list for backward compatibility) with the following keys (all of
    them are required):
    - `:name` The topic name
    - `:num_partitions` The number of partitions to create in the topic, or -1
      if we are either specifying a manual partition assignment or using the
      default partitions.
    - `:replication_factor` The number of replicas to create for each partition
      in the topic, or -1 if we are either specifying a manual partition
      assignment or using the default replication factor.
    - `:assignments` The manual partition assignment, or the empty list if we
      let Kafka automatically assign them. It is a list of maps (or tuple lists)
      with the following keys: `partition_index` and `broker_ids` (a list of of
      brokers to place the partition on).
    - `:configs` The custom topic configurations to set. It is a list of of maps
      (or tuple lists) with keys `name` and `value`. You can find possible
      options in the Kafka documentation.

  ## Example:

  ```
    iex> topic_configs = [
         %{
           name: "my_topic",
           num_partitions: 1,
           replication_factor: 1,
           assignments: [],
           configs: [%{name: "cleanup.policy", value: "compact"}]
         }
       ]
     iex> BrodMimic.Brod.create_topics([{"localhost", 9092}], topic_configs, %{timeout: 1000}, [])
     :ok
  ```
  """
  @spec create_topics(
          [endpoint()],
          [topic_config()],
          request_configs(),
          conn_config()
        ) :: :ok | {:error, any()}
  def create_topics(hosts, topic_configs, request_configs, options) do
    BrodUtils.create_topics(hosts, topic_configs, request_configs, options)
  end

  @doc """
  Equivalent to calling `delete_topics/4` with final options parameter set to an empty
  List.
  """
  @spec delete_topics([endpoint()], [topic()], pos_integer()) :: :ok | {:error, any()}
  def delete_topics(hosts, topics, timeout) do
    BrodUtils.delete_topics(hosts, topics, timeout)
  end

  @doc """
  Delete topic(s) from Kafka.

  ## Example:

  ```
  iex> BrodMimic.Brod.delete_topics([{"localhost", 9092}], ["my_topic"], 5000, [])
  :ok
  ```
  """
  @spec delete_topics([endpoint()], [topic()], pos_integer(), conn_config()) :: :ok | {:error, any()}
  def delete_topics(hosts, topics, timeout, options) do
    BrodUtils.delete_topics(hosts, topics, timeout, options)
  end

  @doc """
  Fetch broker metadata for all topics.

  See `get_metadata/3` for more information.
  """
  @spec get_metadata([endpoint()]) :: {:ok, :kpro.struct()} | {:error, any()}
  def get_metadata(hosts) do
    BrodUtils.get_metadata(hosts)
  end

  @doc """
  Fetch broker metadata for the given topics (or for every topic is `:all` is
  passed in)

  See `get_metadata/3` for more information.
  """
  @spec get_metadata([endpoint()], :all | [topic()]) :: {:ok, :kpro.struct()} | {:error, any()}
  def get_metadata(hosts, topics) do
    BrodUtils.get_metadata(hosts, topics)
  end

  @doc """
  Fetch broker metadata for the given topics using the given connection options.

  The response differs in each version of the `metadata` API call.
  The last supported `metadata` API version is 2, so this will be
  probably used (if your Kafka supports it too). See
  [kafka.bnf](https://github.com/kafka4beam/kafka_protocol/blob/master/priv/kafka.bnf).
  (search for `MetadataResponseV2`) for response schema with comments.

  Beware that when `auto.create.topics.enable` is set to true in
  the broker configuration, fetching metadata with a concrete
  topic specified (in the `topics` parameter) may cause creation of
  the topic when it does not exist. If you want a safe `get_metadata`
  call, always pass `:all` as `topics` and then filter them.

  ## Example

  ```
  iex> BrodMimic.Brod.get_metadata([{"localhost", 9092}], ["my_topic"], [])
  {:ok,
  %{
    brokers: [%{host: "127.0.0.1", node_id: 0, port: 9092, rack: ""}],
    cluster_id: "serqLrBPTBuq4Dp6Pb7Kdw",
    controller_id: 0,
    topics: [
      %{
        error_code: :no_error,
        is_internal: false,
        name: "my_topic",
        partitions: [
          %{
            error_code: :no_error,
            isr_nodes: [0],
            leader_id: 0,
            partition_index: 0,
            replica_nodes: [0]
          }
        ]
      }
    ]
  }}
  ```
  """
  @spec get_metadata([endpoint()], :all | [topic()], conn_config()) :: {:ok, :kpro.struct()} | {:error, any()}
  def get_metadata(hosts, topics, options) do
    BrodUtils.get_metadata(hosts, topics, options)
  end

  @doc """
  The equivalent of `resolve_offset(hosts, topic, partition, :latest, [])`
  """
  @spec resolve_offset([endpoint()], topic(), partition()) :: {:ok, offset()} | {:error, any()}
  def resolve_offset(hosts, topic, partition) do
    resolve_offset(hosts, topic, partition, :latest)
  end

  @doc """
  The equivalent of `resolve_offset(hosts, topic, partition, time, [])`
  """
  @spec resolve_offset([endpoint()], topic(), partition(), offset_time()) ::
          {:ok, offset()} | {:error, any()}
  def resolve_offset(hosts, topic, partition, time) do
    resolve_offset(hosts, topic, partition, time, [])
  end

  @doc """
  Resolve semantic offset or timestamp to real offset.

  The same as `resolve_offset/6` but the timeout is extracted from connection config
  """
  @spec resolve_offset([endpoint()], topic(), partition(), offset_time(), conn_config()) ::
          {:ok, offset()} | {:error, any()}
  def resolve_offset(hosts, topic, partition, time, conn_cfg) do
    BrodUtils.resolve_offset(hosts, topic, partition, time, conn_cfg)
  end

  @doc """
  Resolve semantic offset or timestamp to real offset.

  The function returns the offset of the first message with the given timestamp,
  or of the first message after the given timestamp (in case no message matches
  the timestamp exactly), or -1 if the timestamp is newer than (>) all messages
  in the topic.

  You can also use two semantic offsets instead of a timestamp: `:earliest`
  gives you the offset of the first message in the topic and `:latest` gives you
  the offset of the last message incremented by 1.

  If the topic is empty, both `:earliest` and `:latest` return the same value
  (which is 0 unless some messages were deleted from the topic), and any
  timestamp returns -1.

   An example for illustration:
   ```
   Messages:
   offset       0   1   2   3
   timestamp    10  20  20  30

   Calls:
   resolve_offset(endpoints, topic, partition, 5) → 0
   resolve_offset(endpoints, topic, partition, 10) → 0
   resolve_offset(endpoints, topic, partition, 13) → 1
   resolve_offset(endpoints, topic, partition, 20) → 1
   resolve_offset(endpoints, topic, partition, 31) → -1
   resolve_offset(endpoints, topic, partition, :earliest) → 0
   resolve_offset(endpoints, topic, partition, :latest) → 4
   ```
  """
  @spec resolve_offset([endpoint()], topic(), partition(), offset_time(), conn_config(), any()) ::
          {:ok, offset()} | {:error, any()}
  def resolve_offset(hosts, topic, partition, time, conn_cfg, opts) do
    BrodUtils.resolve_offset(hosts, topic, partition, time, conn_cfg, opts)
  end

  def fetch(conn_or_bootstrap, topic, partition, offset) do
    opts = %{max_wait_time: 1000, min_bytes: 1, max_bytes: bsl(1, 20)}
    fetch(conn_or_bootstrap, topic, partition, offset, opts)
  end

  @doc """
  Fetch a single message set from the given topic-partition.

   The first arg can either be an already established connection to leader,
   or `{endpoints, conn_config}` (or just `endpoints`) so to establish a new
   connection before fetch.

   The fourth argument is the start offset of the query. Messages with offset
   greater or equal will be fetched.

   You can also pass options for the fetch query. See `t:fetch_opts/0` for
   documentation. Only `:max_wait_time`, `:min_bytes`, `:max_bytes`, and `:isolation_level`
   options are currently supported. The defaults are the same as documented
   in the linked type, except for `:min_bytes` which defaults to 1.
   Note that `:max_bytes` will be rounded up so that full messages are
   retrieved. For example, if you specify `max_bytes: 42` and there
   are three messages of size 40 bytes, two of them will be fetched.

   On success, the function returns the messages along with the _last stable
   offset_ (when using `:read_committed` mode, the last committed offset) or the
   _high watermark offset_ (offset of the last message that was successfully
   copied to all replicas, incremented by 1), whichever is lower. In essence, this
   is the offset up to which it was possible to read the messages at the time of
   fetching. This is similar to what `resolve_offset/6` with `:latest`
   returns. You can use this information to determine how far from the end of the
   topic you currently are. Note that when you use this offset as the start offset
   for a subseuqent call, an empty list of messages will be returned (assuming the
   topic hasn't changed, e.g. no new message arrived). Only when you use an offset
   greater than this one, `{:error, :offset_out_of_range}` will be returned.

   Note also that Kafka batches messages in a message set only up to the end of
   a topic segment in which the first retrieved message is, so there may actually
   be more messages behind the last fetched offset even if the fetched size is
   significantly less than `:max_bytes` provided in `t:fetch_opts/0`.

   Example (the topic has only two messages):

   ```
   iex> BrodMimic.Brod.fetch([{"localhost", 9092}], "my_topic", 0, 0, %{max_bytes: 1024})
   {
      :ok,
      {
        2, [
        {kafka_message,0,<<"some_key">>,<<"Hello world!">>, create,1663940976473,[]},
        {kafka_message,1,<<"another_key">>,<<"This is a message with offset 1.">>, create,1663940996335,[]}
        ]
      }
    }

   iex> BrodMimic.Brod.fetch([{"localhost", 9092}], "my_topic", 0, 2, %{max_bytes: 1024})
   {:ok,{2,[]}}

   iex> BrodMimic.Brod.fetch([{"localhost", 9092}], "my_topic", 0, 3, %{max_bytes: 1024})
   {:error, :offset_out_of_range}
   ```
  """
  @spec fetch(
          connection() | client_id() | bootstrap(),
          topic(),
          partition(),
          offset(),
          fetch_opts()
        ) :: {:ok, {offset(), [message()]}} | {:error, any()}
  def fetch(conn_or_bootstrap, topic, partition, offset, opts) do
    BrodUtils.fetch(conn_or_bootstrap, topic, partition, offset, opts)
  end

  @doc """
  Equivalent to fetch(Hosts, Topic, Partition, Offset, Wait, MinBytes, MaxBytes, [])
  """
  @deprecated "Please use `fetch/5` instead"
  @spec fetch(
          [endpoint()],
          topic(),
          partition(),
          offset(),
          non_neg_integer(),
          non_neg_integer(),
          pos_integer()
        ) :: {:ok, [message()]} | {:error, any()}
  def fetch(hosts, topic, partition, offset, max_wait_time, min_bytes, max_bytes) do
    fetch(hosts, topic, partition, offset, max_wait_time, min_bytes, max_bytes, [])
  end

  @doc """
  Fetch a single message set from the given topic-partition
  """
  @deprecated "Please use `fetch/5` instead"
  @spec fetch(
          [endpoint()],
          topic(),
          partition(),
          offset(),
          non_neg_integer(),
          non_neg_integer(),
          pos_integer(),
          conn_config()
        ) :: {:ok, [message()]} | {:error, any()}
  def fetch(hosts, topic, partition, offset, max_wait_time, min_bytes, max_bytes, conn_config) do
    fetch_opts = %{max_wait_time: max_wait_time, min_bytes: min_bytes, max_bytes: max_bytes}

    case fetch({hosts, conn_config}, topic, partition, offset, fetch_opts) do
      {:ok, {_hw_offset, batch}} ->
        {:ok, batch}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Fold through messages in a partition

  Works like `:lists.foldl/3` but with below stop conditions:

  - Always return after reach high watermark offset
  - Return after the given message count limit is reached
  - Return after the given kafka offset is reached
  - Return if the fold function returns an `{:error, reason}` tuple

  _Exceptions from evaluating fold function are not caught_.
  """
  def fold(bootstrap, topic, partition, offset, opts, acc, fun, limits) do
    BrodUtils.fold(bootstrap, topic, partition, offset, opts, acc, fun, limits)
  end

  @doc """
  Connect partition leader
  """
  @spec connect_leader([endpoint()], topic(), partition(), conn_config()) :: {:ok, pid()}
  def connect_leader(hosts, topic, partition, conn_config) do
    kpro_options = BrodUtils.kpro_connection_options(conn_config)
    :kpro.connect_partition_leader(hosts, conn_config, topic, partition, kpro_options)
  end

  @doc """
    List ALL consumer groups in the given Kafka cluster.

    _Exception if failed to connect any of the coordinator brokers_.
  """
  @spec list_all_groups([endpoint()], conn_config()) :: [{endpoint(), [cg()] | {:error, any()}}]
  def list_all_groups(endpoints, conn_cfg) do
    BrodUtils.list_all_groups(endpoints, conn_cfg)
  end

  @doc """
  List consumer groups in the given group coordinator broker
  """
  @spec list_groups(endpoint(), conn_config()) :: {:ok, [cg()]} | {:error, any()}
  def list_groups(coordinator_endpoint, conn_cfg) do
    BrodUtils.list_groups(coordinator_endpoint, conn_cfg)
  end

  @doc """
    Describe consumer groups

    The given consumer group IDs should be all managed by the coordinator-broker
    running at the given endpoint. Otherwise error codes will be returned in the
    result structs. Return `describe_groups` response body field named `groups`.
    See `kpro_schema.erl` for struct details.
  """
  @spec describe_groups(endpoint(), conn_config(), [group_id()]) ::
          {:ok, [:kpro.struct()]} | {:error, any()}
  def describe_groups(coordinator_endpoint, conn_cfg, iDs) do
    BrodUtils.describe_groups(coordinator_endpoint, conn_cfg, iDs)
  end

  @doc """
    Connect to consumer group coordinator broker

    Done in steps:

    1. Connect to any of the given bootstrap endpoints
    2. Send group_coordinator_request to resolve group coordinator endpoint
    3. Connect to the resolved endpoint and return the connection pid
  """
  @spec connect_group_coordinator([endpoint()], conn_config(), group_id()) ::
          {:ok, pid()} | {:error, any()}
  def connect_group_coordinator(bootstrap_endpoints, conn_cfg, group_id) do
    kpro_options = BrodUtils.kpro_connection_options(conn_cfg)
    args = Map.merge(kpro_options, %{type: :group, id: group_id})

    :kpro.connect_coordinator(bootstrap_endpoints, conn_cfg, args)
  end

  @doc """
  Fetch committed offsets for ALL topics in the given consumer group

  Return the `responses` field of the `offset_fetch` response. See
  `kpro_schema.erl` for struct details.
  """
  @spec fetch_committed_offsets([endpoint()], conn_config(), group_id()) ::
          {:ok, [:kpro.struct()]} | {:error, any()}
  def fetch_committed_offsets(bootstrap_endpoints, conn_cfg, group_id) do
    BrodUtils.fetch_committed_offsets(bootstrap_endpoints, conn_cfg, group_id, [])
  end

  @doc """
  Same as `fetch_committed_offsets/3` but works with a started `brod_client`
  """
  @spec fetch_committed_offsets(client(), group_id()) :: {:ok, [:kpro.struct()]} | {:error, any()}
  def fetch_committed_offsets(client, group_id) do
    BrodUtils.fetch_committed_offsets(client, group_id, [])
  end
end
