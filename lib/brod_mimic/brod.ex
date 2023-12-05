defmodule BrodMimic.Brod do
  use BrodMimic.Macros

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
end
