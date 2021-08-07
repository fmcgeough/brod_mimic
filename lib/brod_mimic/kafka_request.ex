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

  @type api() :: KafkaApis.api()
  @type vsn() :: KafkaApis.vsn()
  @type topic() :: Brod.topic()
  @type topic_config() :: :kpro.struct()
  @type partition() :: Brod.partition()
  @type offset() :: Brod.offset()
  @type conn() :: :kpro.connection()

  alias BrodMimic.KafkaApis

  @doc """
  Make a produce request, If the first arg is a connection pid, call
  `brod_kafka_apis:pick_version/2' to resolve version.
  """
  @spec produce(
          conn() | vsn(),
          topic(),
          partition(),
          :kpro.batch_input(),
          integer(),
          integer(),
          Brod.compression()
        ) :: :kpro.req()
  def produce(maybe_pid, topic, partition, batch_input, required_acks, ack_timeout, compression) do
    vsn = pick_version(:produce, maybe_pid)

    :kpro_req_lib.produce(vsn, topic, partition, batch_input, %{
      required_acks: required_acks,
      ack_timeout: ack_timeout,
      compression: compression
    })
  end

  @doc """
  Make a create_topics request.
  """
  @spec create_topics(vsn() | conn(), [topic_config()], %{
          timeout: :kpro.int32(),
          validate_only: boolean()
        }) :: :kpro.req()
  def create_topics(connection, topic_configs, request_configs) when is_pid(connection) do
    vsn = KafkaApis.pick_version(connection, :create_topics)
    create_topics(vsn, topic_configs, request_configs)
  end

  def create_topics(vsn, topic_configs, request_configs) do
    :kpro_req_lib.create_topics(vsn, topic_configs, request_configs)
  end

  @doc """
  Make a delete_topics request.
  """
  @spec delete_topics(vsn() | conn(), [topic()], pos_integer()) :: :kpro.req()
  def delete_topics(connection, topics, timeout) when is_pid(connection) do
    vsn = KafkaApis.pick_version(connection, :delete_topics)
    delete_topics(vsn, topics, timeout)
  end

  def delete_topics(vsn, topics, timeout) do
    :kpro_req_lib.delete_topics(vsn, topics, %{timeout: timeout})
  end

  @doc """
  Make a fetch request, If the first arg is a connection pid, call

  `brod_kafka_apis:pick_version/2' to resolve version.
  """
  @spec fetch(conn(), topic(), partition(), offset(), :kpro.wait(), :kpro.count(), :kpro.count()) ::
          :kpro.req()
  def fetch(pid, topic, partition, offset, wait_time, min_bytes, max_bytes) do
    vsn = pick_version(:fetch, pid)

    :kpro_req_lib.fetch(vsn, topic, partition, offset, %{
      max_wait_time: wait_time,
      min_bytes: min_bytes,
      max_bytes: max_bytes
    })
  end

  @doc """
   Make a `list_offsets' request message for offset resolution.
  In kafka protocol, -2 and -1 are semantic 'time' to request for
  'earliest' and 'latest' offsets.

  In brod implementation, -2, -1, 'earliest' and 'latest'
  are semantic 'offset', this is why often a variable named
  Offset is used as the Time argument.
  """
  @spec list_offsets(conn(), topic(), partition(), Brod.offset_time()) :: :kpro.req()
  def list_offsets(connection, topic, partition, time_or_semantic_offset) do
    time = ensure_integer_offset_time(time_or_semantic_offset)
    vsn = pick_version(:list_offsets, connection)
    :kpro_req_lib.list_offsets(vsn, topic, partition, time)
  end

  @doc """
  Make a metadata request.
  """
  @spec metadata(vsn() | conn(), :all | [topic()]) :: :kpro.req()
  def metadata(connection, topics) when is_pid(connection) do
    vsn = KafkaApis.pick_version(connection, :metadata)
    metadata(vsn, topics)
  end

  def metadata(vsn, topics) do
    :kpro_req_lib.metadata(vsn, topics)
  end

  @doc """
  Make a offset fetch request.

  NOTE: empty topics list only works for kafka 0.10.2.0 or later
  """
  @spec offset_fetch(conn(), Brod.group_id(), [{topic(), [partition()]}]) :: :kpro.req()
  def offset_fetch(connection, group_id, topics0) do
    # FIXME
    # topics = :lists.map(fn -> {topic, partitions}) ->
    #       [ {:topic, topic}, {:partitions, [[{:partition, p}] || p <- partitions]}]
    #   end, topics0)
    # body = [ {:group_id, group_id}, {:topics, case topics do
    #                     [] -> ?kpro_null
    #                     _  -> topics
    #                 end}
    #      ]
    body = [{:group_id, group_id}, {:topics, :undefined}]
    vsn = pick_version(:offset_fetch, connection)
    :kpro.make_request(:offset_fetch, vsn, body)
  end

  @doc """
  Make a `list_groups' request.
  """
  @spec list_groups(conn()) :: :kpro.req()
  def list_groups(connection) do
    vsn = pick_version(:list_groups, connection)
    :kpro.make_request(:list_groups, vsn, [])
  end

  @doc """
  Make a `join_group' request.
  """
  @spec join_group(conn(), :kpro.struct()) :: :kpro.req()
  def join_group(conn, fields) do
    make_req(:join_group, conn, fields)
  end

  @doc """
  Make a `sync_group' request.
  """
  @spec sync_group(conn(), :kpro.struct()) :: :kpro.req()
  def sync_group(conn, fields) do
    make_req(:sync_group, conn, fields)
  end

  @doc """
  Make a `offset_commit' request.
  """
  @spec offset_commit(conn(), :kpro.struct()) :: :kpro.req()
  def offset_commit(conn, fields) do
    make_req(:offset_commit, conn, fields)
  end

  ####### Internal Functions =======================================================

  defp make_req(api, conn, fields) when is_pid(conn) do
    vsn = pick_version(api, conn)
    make_req(api, vsn, fields)
  end

  defp make_req(api, vsn, fields) do
    :kpro.make_request(api, vsn, fields)
  end

  @spec pick_version(api(), pid()) :: vsn()
  defp pick_version(_api, vsn) when is_integer(vsn), do: vsn

  defp pick_version(api, connection) when is_pid(connection) do
    KafkaApis.pick_version(connection, api)
  end

  defp pick_version(api, _) do
    KafkaApis.default_version(api)
  end

  @spec ensure_integer_offset_time(Brod.offset_time()) :: integer()
  defp ensure_integer_offset_time(@offset_earliest), do: -2
  defp ensure_integer_offset_time(@offset_latest), do: -1
  defp ensure_integer_offset_time(t) when is_integer(t), do: t
end
