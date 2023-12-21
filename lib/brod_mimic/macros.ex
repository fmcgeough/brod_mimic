defmodule BrodMimic.Macros do
  @moduledoc """
  Macros in Elixir as substitute for Erlang macros used in brod
  """

  defmacro __using__(_) do
    quote do
      import Record, only: [defrecordp: 2, extract: 2]

      defrecordp(:kpro_req, extract(:kpro_req, from_lib: "kafka_protocol/include/kpro.hrl"))
      defrecordp(:kpro_rsp, extract(:kpro_rsp, from_lib: "kafka_protocol/include/kpro.hrl"))

      defrecordp(
        :kafka_message,
        extract(:kafka_message, from_lib: "kafka_protocol/include/kpro.hrl")
      )

      defrecordp(:kafka_message_set,
        topic: :undefined,
        partition: :undefined,
        high_wm_offset: :undefined,
        messages: :undefined
      )

      defrecordp(:kafka_group_member_metadata,
        version: :undefined,
        topics: :undefined,
        user_data: :undefined
      )

      defrecordp(:brod_received_assignment,
        topic: :undefined,
        partition: :undefined,
        begin_offset: :undefined
      )

      defrecordp(:kafka_fetch_error,
        topic: :undefined,
        partition: :undefined,
        error_code: :undefined,
        error_desc: ""
      )

      defrecordp(:brod_cg, id: :undefined, protocol_type: :undefined)

      @typedoc """
      Kafka topics are the categories used to organize messages. Each topic has
      a name that is unique across the entire Kafka cluster.
      """
      @type topic() :: :kpro.topic()

      @typedoc """
      The messages stored for a topic are divided into partitions. A partition is
      the smallest storage unit that holds a subset of the total messages for a
      topic.
      """
      @type partition() :: :kpro.partition()

      @typedoc """
      Offsets are used to track the progress of a consumer group as it consumes
      messages from Kafka topics. Each partition of a Kafka topic has its own
      set of offsets, which indicate the last message that was successfully
      processed by the consumer group for that partition
      """
      @type offset() :: :kpro.offset()

      @typedoc """
      A record with offset, key, value, ts_type, ts, and headers.
      """
      @type message() :: :kpro.message()

      @typedoc """
      A client is identified using a unique atom
      """
      @type client_id() :: atom()

      @typedoc """
      A client is started with an atom to give it a unique GenServer name

      Thereafter its possible to pass either the pid returned by starting the
      GenServer or the atom (this module does a lookup for pid if atom is
      given).
      """
      @type client() :: client_id() | pid()

      @typedoc """
      Record defining a Kafka request for protocol library
      """
      @type kpro_req() ::
              record(:kpro_req,
                ref: reference(),
                api: :kpro.api(),
                vsn: :kpro.vsn(),
                no_ack: :boolean,
                msg: iodata() | :kpro.struct()
              )

      @typedoc """
      Record defining a Kafka response for protocol library
      """
      @type kpro_rsp() ::
              record(:kpro_rsp,
                ref: false | reference(),
                api: :kpro.api(),
                vsn: :kpro.vsn(),
                msg: binary() | :kpro.struct()
              )

      @typedoc """
      Consumer group protocol type
      """
      @type cg_protocol_type() :: binary()

      @typedoc """
      Record defining consumer group data (id and protocol type)
      """
      @type cg() ::
              record(:brod_cg,
                id: BrodMimic.Brod.group_id(),
                protocol_type: BrodMimic.Brod.cg_protocol_type()
              )

      @type kafka_group_member_metadata ::
              record(:kafka_group_member_metadata,
                version: non_neg_integer(),
                topics: [topic()],
                user_data: binary()
              )

      @typedoc """
      Received consumer assignment for a topic/partition
      """
      @type brod_received_assignment ::
              record(:brod_received_assignment,
                topic: topic(),
                partition: BrodMimic.Brod.partition(),
                begin_offset:
                  :undefined
                  | offset()
                  | {:begin_offset, BrodMimic.Brod.offset_time()}
              )

      @type kafka_fetch_error() ::
              record(:kafka_fetch_error,
                topic: topic(),
                partition: partition(),
                error_code: BrodMimic.Brod.error_code(),
                error_desc: String.t()
              )

      @typedoc """
      A record with topic, partition, high_wm_offset (max offset of the partition), and messages.
      """
      @type message_set ::
              record(:kafka_message_set,
                topic: topic(),
                partition: partition(),
                high_wm_offset: integer(),
                # the list of `t:message/0` is exposed to users of library
                # the `incomplete_batch` is internal only
                messages: [message()] | :kpro.incomplete_batch()
              )

      @type request_configs() :: %{
              optional(:timeout) => :kpro.int32(),
              optional(:validate_only) => boolean()
            }

      defp offset_earliest, do: :earliest
      defp offset_latest, do: :latest
      defp unknown_topic_cache_expire_seconds, do: 120
      defp topic_metadata_key(topic), do: {:topic_metadata, topic}
      defp is_error(ec), do: ec != :no_error

      defp escalate_ec(ec) do
        if is_error(ec) do
          throw(ec)
        end
      end

      defp kv(key, value), do: {key, value}
      defp tkv(ts, key, value), do: {ts, key, value}
      defp brod_fold_ret(acc, next_offset, reason), do: {acc, next_offset, reason}
      defp brod_default_timeout, do: :timer.seconds(5)
      defp consumer_key(topic, partition), do: {:consumer, topic, partition}
      defp producer_key(topic, partition), do: {:producer, topic, partition}
      defp producer(topic, partition, pid), do: {producer_key(topic, partition), pid}
      defp consumer(topic, partition, pid), do: {consumer_key(topic, partition), pid}
      defp incomplete_batch(expected_size), do: {:incomplete_batch, expected_size}
    end
  end
end
