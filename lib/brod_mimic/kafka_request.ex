defmodule BrodMimic.KafkaRequest do
  @moduledoc """
    The brod Erlang module exports the following functions

    -export([ create_topics/3
            , delete_topics/3
            , fetch/7
            , list_groups/1
            , list_offsets/4
            , join_group/2
            , metadata/2
            , offset_commit/2
            , offset_fetch/3
            , produce/7
            , sync_group/2
            ]).
  """
  require Record

  import Bitwise

  @type api() :: KafkaApis.api()
  @type vsn() :: KafkaApis.vsn()
  @type topic() :: Brod.topic()
  @type topic_config() :: :kpro.struct()
  @type partition() :: Brod.partition()
  @type offset() :: Brod.offset()
  @type conn() :: :kpro.connection()

  alias BrodMimic.KafkaApis

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

  def produce(maybePid, topic, partition, batchInput, requiredAcks, ackTimeout, compression) do
    vsn = pick_version(:produce, maybePid)

    :kpro_req_lib.produce(vsn, topic, partition, batchInput, %{
      required_acks: requiredAcks,
      ack_timeout: ackTimeout,
      compression: compression
    })
  end

  def create_topics(connection, topicConfigs, requestConfigs)
      when is_pid(connection) do
    vsn = BrodKafkaApis.pick_version(connection, :create_topics)
    create_topics(vsn, topicConfigs, requestConfigs)
  end

  def create_topics(vsn, topicConfigs, requestConfigs) do
    :kpro_req_lib.create_topics(vsn, topicConfigs, requestConfigs)
  end

  def delete_topics(connection, topics, timeout) when is_pid(connection) do
    vsn = BrodKafkaApis.pick_version(connection, :delete_topics)

    delete_topics(vsn, topics, timeout)
  end

  def delete_topics(vsn, topics, timeout) do
    :kpro_req_lib.delete_topics(vsn, topics, %{timeout: timeout})
  end

  def fetch(pid, topic, partition, offset, waitTime, minBytes, maxBytes, isolationLevel) do
    vsn = pick_version(:fetch, pid)

    :kpro_req_lib.fetch(vsn, topic, partition, offset, %{
      max_wait_time: waitTime,
      min_bytes: minBytes,
      max_bytes: maxBytes,
      isolation_level: isolationLevel
    })
  end

  def list_offsets(connection, topic, partition, timeOrSemanticOffset) do
    time = ensure_integer_offset_time(timeOrSemanticOffset)
    vsn = pick_version(:list_offsets, connection)
    :kpro_req_lib.list_offsets(vsn, topic, partition, time)
  end

  def metadata(connection, topics) when is_pid(connection) do
    vsn = BrodKafkaApis.pick_version(connection, :metadata)

    metadata(vsn, topics)
  end

  def metadata(vsn, topics) do
    :kpro_req_lib.metadata(vsn, topics)
  end

  def list_groups(connection) do
    vsn = pick_version(:list_groups, connection)
    :kpro.make_request(:list_groups, vsn, [])
  end

  def join_group(conn, fields) do
    make_req(:join_group, conn, fields)
  end

  def sync_group(conn, fields) do
    make_req(:sync_group, conn, fields)
  end

  def offset_commit(conn, fields) do
    make_req(:offset_commit, conn, fields)
  end

  defp make_req(aPI, conn, fields) when is_pid(conn) do
    vsn = pick_version(aPI, conn)
    make_req(aPI, vsn, fields)
  end

  defp make_req(aPI, vsn, fields) do
    :kpro.make_request(aPI, vsn, fields)
  end

  defp pick_version(_API, vsn) when is_integer(vsn) do
    vsn
  end

  defp pick_version(aPI, connection) when is_pid(connection) do
    BrodKafkaApis.pick_version(connection, aPI)
  end

  defp pick_version(aPI, _) do
    BrodKafkaApis.default_version(aPI)
  end

  defp ensure_integer_offset_time(:earliest) do
    -2
  end

  defp ensure_integer_offset_time(:latest) do
    -1
  end

  defp ensure_integer_offset_time(t) when is_integer(t) do
    t
  end
end
