defmodule BrodMimic.Brod do
  use BrodMimic.Macros

  import Bitwise

  alias BrodMimic.Client, as: BrodClient
  alias BrodMimic.Consumer, as: BrodConsumer
  alias BrodMimic.Producer, as: BrodProducer
  alias BrodMimic.Sup, as: BrodSup
  alias BrodMimic.Utils, as: BrodUtils

  ### Types created for Elixir port ============================================
  @type ets_table_id() :: atom() | term()
  @type ets_table() :: atom() | ets_table_id()
  @type req() :: :kpro.req()

  ### Types ====================================================================

  ## basics

  @type hostname() :: :kpro.hostname()
  @type portnum() :: pos_integer()
  @type endpoint() :: {hostname(), portnum()}
  @type topic() :: :kpro.topic()
  @type topic_config() :: :kpro.struct()
  @type partition() :: :kpro.partition()
  @type topic_partition() :: {topic(), partition()}
  @type offset() :: :kpro.offset()
  @type key() :: :undefined | binary()
  # %% no value, transformed to <<>>
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
  @type client_id() :: atom()
  @typedoc """
  A client is started with an atom to give it a unique GenServer name

  Thereafter its possible to pass either the pid returned by starting the
  GenServer or the atom (this module does a lookup for pid if atom is
  given).
  """
  @type client() :: client_id() | pid()
  @type client_config() :: BrodMimic.Client.config()
  # default client config
  @type bootstrap() ::
          [endpoint()]
          | {[endpoint()], client_config()}
  @type offset_time() :: integer() | :earliest | :latest
  @type message() :: :kpro.message()
  # kafka_message_set{}
  # @type message_set() ::
  #         @type(error_code() :: :kpro.error_code())

  ## producers
  @type produce_reply() :: BrodMimic.Records.ProduceReply.t()
  @type producer_config() :: BrodMimic.Producer.config()
  # @type partition_fun() :: fun((topic(), pos_integer(), key(), value()) ::
  #                                 {:ok, partition()})
  # @type partitioner() :: partition_fun() | random | hash
  # @type produce_ack_cb() :: fun((partition(), offset()) -> _)
  @type compression() :: :no_compression | :gzip | :snappy
  @type call_ref() :: map()
  @type produce_result() :: :brod_produce_req_buffered | :brod_produce_req_acked

  ## consumers
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
  @type consumer_config() :: BrodMimic.Consumer.config()
  @type connection() :: :kpro.connection()
  @type conn_config() :: [{atom(), term()}] | :kpro.conn_config()

  ## consumer groups
  @type group_id() :: :kpro.group_id()
  @type group_member_id() :: binary()
  @type group_member() :: {group_member_id(), BrodMimic.Records.GroupMemberMetada.t()}
  @type group_generation_id() :: non_neg_integer()
  @type group_config() :: keyword()
  @type partition_assignment() :: {topic(), [partition()]}
  @type received_assignments() :: [BrodMimic.Records.ReceivedAssignment.t()]
  # brod_cg{}
  @type cg() :: BrodMimic.Records.ConsumerGroup.t()
  @type cg_protocol_type() :: binary()
  @type fetch_opts() :: :kpro.fetch_opts()
  @type fold_acc() :: term()
  # @type fold_fun(acc) :: fun((message(), acc) -> {:ok, acc} | {:error, any()})
  ## `fold' always returns when reaches the high watermark offset `fold'
  ## also returns when any of the limits is hit
  @type fold_limits() :: BrodMimic.Records.FoldLimits.t()
  @type fold_stop_reason() ::
          :reached_end_of_partition
          | :reached_message_count_limit
          | :reached_target_offset
          | {:error, any()}
  ## OffsetToContinue: begin offset for the next fold call
  # @type fold_result() :: brod_fold_ret(fold_acc(), offset_to_continue :: offset(), fold_stop_reason())

  def start do
    {:ok, _apps} = :application.ensure_all_started(:brod)
    :ok
  end

  def stop do
    :application.stop(:brod)
  end

  def start(_start_type, _start_args) do
    BrodSup.start_link()
  end

  def stop(_state) do
    :ok
  end

  def start_client(bootstrap_endpoints) do
    start_client(bootstrap_endpoints, :brod_default_client)
  end

  def start_client(bootstrap_endpoints, client_id) do
    start_client(bootstrap_endpoints, client_id, [])
  end

  def start_client(bootstrap_endpoints, client_id, config) do
    case BrodSup.start_client(bootstrap_endpoints, client_id, config) do
      :ok ->
        :ok

      {:error, {:already_started, _Pid}} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  def start_link_client(bootstrap_endpoints) do
    start_link_client(bootstrap_endpoints, :brod_default_client)
  end

  def start_link_client(bootstrap_endpoints, client_id) do
    start_link_client(bootstrap_endpoints, client_id, [])
  end

  def start_link_client(bootstrap_endpoints, client_id, config) do
    BrodClient.start_link(bootstrap_endpoints, client_id, config)
  end

  def stop_client(client) when is_atom(client) do
    case BrodSup.find_client(client) do
      [_Pid] ->
        BrodSup.stop_client(client)

      [] ->
        BrodClient.stop(client)
    end
  end

  def stop_client(client) when is_pid(client) do
    BrodClient.stop(client)
  end

  def start_producer(client, topic_name, producer_config) do
    BrodClient.start_producer(client, topic_name, producer_config)
  end

  def start_consumer(client, topic_name, consumer_config) do
    BrodClient.start_consumer(client, topic_name, consumer_config)
  end

  def get_partitions_count(client, topic) do
    BrodClient.get_partitions_count(client, topic)
  end

  def get_partitions_count_safe(client, topic) do
    BrodClient.get_partitions_count_safe(client, topic)
  end

  def get_consumer(client, topic, partition) do
    BrodClient.get_consumer(client, topic, partition)
  end

  def get_producer(client, topic, partition) do
    BrodClient.get_producer(client, topic, partition)
  end

  def produce(pid, value) do
    produce(pid, _key = <<>>, value)
  end

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

  def produce(client, topic, partitioner, key, value) do
    partFun = BrodUtils.make_part_fun(partitioner)

    case BrodClient.get_partitions_count(
           client,
           topic
         ) do
      {:ok, partitions_count} ->
        {:ok, partition} = partFun.(topic, partitions_count, key, value)
        produce(client, topic, partition, key, value)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def produce_cb(producer_pid, key, value, ack_cb) do
    :brod_producer.produce_cb(producer_pid, key, value, ack_cb)
  end

  def produce_cb(client, topic, part, key, value, ack_cb)
      when is_integer(part) do
    case get_producer(client, topic, part) do
      {:ok, pid} ->
        produce_cb(pid, key, value, ack_cb)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def produce_cb(client, topic, partitioner, key, value, ackCb) do
    partFun = BrodUtils.make_part_fun(partitioner)

    case BrodClient.get_partitions_count(client, topic) do
      {:ok, partitions_count} ->
        {:ok, partition} = partFun.(topic, partitions_count, key, value)

        case produce_cb(client, topic, partition, key, value, ackCb) do
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

  def produce_no_ack(client, topic, part, key, value)
      when is_integer(part) do
    case get_producer(client, topic, part) do
      {:ok, pid} ->
        produce_no_ack(pid, key, value)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def produce_no_ack(client, topic, partitioner, key, value) do
    partFun = BrodUtils.make_part_fun(partitioner)

    case BrodClient.get_partitions_count(client, topic) do
      {:ok, partitions_count} ->
        {:ok, partition} = partFun.(topic, partitions_count, key, value)
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
    :brod_producer.sync_produce_request(call_ref, timeout)
  end

  def subscribe(client, subscriberPid, topic, partition, options) do
    case BrodClient.get_consumer(client, topic, partition) do
      {:ok, consumerPid} ->
        case subscribe(consumerPid, subscriberPid, options) do
          :ok ->
            {:ok, consumerPid}

          error ->
            error
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def subscribe(consumerPid, subscriberPid, options) do
    BrodConsumer.subscribe(consumerPid, subscriberPid, options)
  end

  def unsubscribe(client, topic, partition) do
    unsubscribe(client, topic, partition, self())
  end

  def unsubscribe(client, topic, partition, subscriberPid) do
    case BrodClient.get_consumer(client, topic, partition) do
      {:ok, consumerPid} ->
        unsubscribe(consumerPid, subscriberPid)

      error ->
        error
    end
  end

  def unsubscribe(consumerPid) do
    unsubscribe(consumerPid, self())
  end

  def unsubscribe(consumerPid, subscriberPid) do
    BrodConsumer.unsubscribe(consumerPid, subscriberPid)
  end

  def consume_ack(client, topic, partition, offset) do
    case BrodClient.get_consumer(client, topic, partition) do
      {:ok, consumerPid} ->
        consume_ack(consumerPid, offset)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def consume_ack(consumerPid, offset) do
    BrodConsumer.ack(consumerPid, offset)
  end

  def start_link_group_subscriber(
        client,
        groupId,
        topics,
        groupConfig,
        consumer_config,
        cbModule,
        cbInitArg
      ) do
    :brod_group_subscriber.start_link(
      client,
      groupId,
      topics,
      groupConfig,
      consumer_config,
      cbModule,
      cbInitArg
    )
  end

  def start_link_group_subscriber_v2(config) do
    :brod_group_subscriber_v2.start_link(config)
  end

  def start_link_group_subscriber(
        client,
        groupId,
        topics,
        groupConfig,
        consumer_config,
        messageType,
        cbModule,
        cbInitArg
      ) do
    :brod_group_subscriber.start_link(
      client,
      groupId,
      topics,
      groupConfig,
      consumer_config,
      messageType,
      cbModule,
      cbInitArg
    )
  end

  def start_link_topic_subscriber(client, topic, consumer_config, cbModule, cbInitArg) do
    start_link_topic_subscriber(client, topic, :all, consumer_config, cbModule, cbInitArg)
  end

  def start_link_topic_subscriber(client, topic, partitions, consumer_config, cbModule, cbInitArg) do
    start_link_topic_subscriber(
      client,
      topic,
      partitions,
      consumer_config,
      :message,
      cbModule,
      cbInitArg
    )
  end

  def start_link_topic_subscriber(
        client,
        topic,
        partitions,
        consumer_config,
        messageType,
        cbModule,
        cbInitArg
      ) do
    BrodTopicSubscriber.start_link(
      client,
      topic,
      partitions,
      consumer_config,
      messageType,
      cbModule,
      cbInitArg
    )
  end

  def start_link_topic_subscriber(config) do
    BrodTopicSubscriber.start_link(config)
  end

  def create_topics(hosts, topicConfigs, requestConfigs) do
    BrodUtils.create_topics(hosts, topicConfigs, requestConfigs)
  end

  def create_topics(hosts, topicConfigs, requestConfigs, options) do
    BrodUtils.create_topics(hosts, topicConfigs, requestConfigs, options)
  end

  def delete_topics(hosts, topics, timeout) do
    BrodUtils.delete_topics(hosts, topics, timeout)
  end

  def delete_topics(hosts, topics, timeout, options) do
    BrodUtils.delete_topics(hosts, topics, timeout, options)
  end

  def get_metadata(hosts) do
    BrodUtils.get_metadata(hosts)
  end

  def get_metadata(hosts, topics) do
    BrodUtils.get_metadata(hosts, topics)
  end

  def get_metadata(hosts, topics, options) do
    BrodUtils.get_metadata(hosts, topics, options)
  end

  def resolve_offset(hosts, topic, partition) do
    resolve_offset(hosts, topic, partition, :latest)
  end

  def resolve_offset(hosts, topic, partition, time) do
    resolve_offset(hosts, topic, partition, time, [])
  end

  def resolve_offset(hosts, topic, partition, time, connCfg) do
    BrodUtils.resolve_offset(hosts, topic, partition, time, connCfg)
  end

  def resolve_offset(hosts, topic, partition, time, connCfg, opts) do
    BrodUtils.resolve_offset(hosts, topic, partition, time, connCfg, opts)
  end

  def fetch(conn_or_bootstrap, topic, partition, offset) do
    opts = %{max_wait_time: 1000, min_bytes: 1, max_bytes: bsl(1, 20)}
    fetch(conn_or_bootstrap, topic, partition, offset, opts)
  end

  def fetch(conn_or_bootstrap, topic, partition, offset, opts) do
    BrodUtils.fetch(conn_or_bootstrap, topic, partition, offset, opts)
  end

  def fold(bootstrap, topic, partition, offset, opts, acc, fun, limits) do
    BrodUtils.fold(bootstrap, topic, partition, offset, opts, acc, fun, limits)
  end

  def fetch(hosts, topic, partition, offset, maxWaitTime, minBytes, maxBytes) do
    fetch(hosts, topic, partition, offset, maxWaitTime, minBytes, maxBytes, [])
  end

  def fetch(hosts, topic, partition, offset, maxWaitTime, minBytes, maxBytes, connConfig) do
    fetchOpts = %{max_wait_time: maxWaitTime, min_bytes: minBytes, max_bytes: maxBytes}

    case fetch({hosts, connConfig}, topic, partition, offset, fetchOpts) do
      {:ok, {_HwOffset, batch}} ->
        {:ok, batch}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def connect_leader(hosts, topic, partition, connConfig) do
    kproOptions = BrodUtils.kpro_connection_options(connConfig)
    :kpro.connect_partition_leader(hosts, connConfig, topic, partition, kproOptions)
  end

  def list_all_groups(endpoints, connCfg) do
    BrodUtils.list_all_groups(endpoints, connCfg)
  end

  def list_groups(coordinatorEndpoint, connCfg) do
    BrodUtils.list_groups(coordinatorEndpoint, connCfg)
  end

  def describe_groups(coordinatorEndpoint, connCfg, iDs) do
    BrodUtils.describe_groups(coordinatorEndpoint, connCfg, iDs)
  end

  def connect_group_coordinator(bootstrap_endpoints, connCfg, groupId) do
    kproOptions = BrodUtils.kpro_connection_options(connCfg)

    args =
      :maps.merge(
        kproOptions,
        %{type: :group, id: groupId}
      )

    :kpro.connect_coordinator(bootstrap_endpoints, connCfg, args)
  end

  def fetch_committed_offsets(bootstrap_endpoints, connCfg, groupId) do
    BrodUtils.fetch_committed_offsets(bootstrap_endpoints, connCfg, groupId, [])
  end

  def fetch_committed_offsets(client, groupId) do
    BrodUtils.fetch_committed_offsets(client, groupId, [])
  end
end
