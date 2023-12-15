defmodule BrodMimic.GroupSubscriberWorker do
  @moduledoc """
  Implements the `BrodMimic.TopicSubscriber` behaviour
  """

  @behaviour BrodMimic.TopicSubscriber

  import Record, only: [defrecord: 2, defrecord: 3, extract: 2]

  alias BrodMimic.Utils, as: BrodUtils

  require Logger

  @starting_group_subscriber "Starting group_subscriber_worker: ~p~nOffset: ~p~nPid: ~p~n"
  @discard_invalid_offset "Discarded invalid committed offset ~p for: ~s:~p~n"

  defrecord(
    :kafka_message,
    extract(:kafka_message, from_lib: "kafka_protocol/include/kpro.hrl")
  )

  defrecord(:r_kafka_message_set, :kafka_message_set,
    topic: :undefined,
    partition: :undefined,
    high_wm_offset: :undefined,
    messages: :undefined
  )

  defrecord(:r_state, :state,
    start_options: :undefined,
    cb_module: :undefined,
    cb_state: :undefined,
    commit_fun: :undefined
  )

  def init(topic, start_opts) do
    %{
      cb_module: cb_module,
      cb_config: cb_config,
      partition: partition,
      begin_offset: begin_offset,
      commit_fun: commit_fun
    } = start_opts

    init_info = Map.take(start_opts, [:topic, :partition, :group_id, :commit_fun])

    Logger.info(:io_lib.format(@starting_group_subscriber, [init_info, begin_offset, self()]), %{
      domain: [:brod]
    })

    {:ok, cb_state} = cb_module.init(init_info, cb_config)

    state =
      r_state(
        start_options: start_opts,
        cb_module: cb_module,
        cb_state: cb_state,
        commit_fun: commit_fun
      )

    committed_offsets = resolve_committed_offsets(topic, partition, begin_offset)
    {:ok, committed_offsets, state}
  end

  def handle_message(_partition, msg, state) do
    r_state(cb_module: cb_module, cb_state: cb_state, commit_fun: commit) = state

    case cb_module.handle_message(msg, cb_state) do
      {:ok, :commit, new_cb_state} ->
        new_state = r_state(state, cb_state: new_cb_state)
        commit.(get_last_offset(msg))
        {:ok, :ack, new_state}

      {:ok, :ack, new_cb_state} ->
        new_state = r_state(state, cb_state: new_cb_state)
        {:ok, :ack, new_state}

      {:ok, new_cb_state} ->
        new_state = r_state(state, cb_state: new_cb_state)
        {:ok, new_state}
    end
  end

  def terminate(
        reason,
        r_state(cb_module: cb_module, cb_state: state)
      ) do
    BrodUtils.optional_callback(cb_module, :terminate, [reason, state], :ok)
  end

  defp get_last_offset(kafka_message(offset: offset)) do
    offset
  end

  defp get_last_offset(r_kafka_message_set(messages: messages)) do
    messages |> :lists.last() |> kafka_message(:offset)
  end

  defp resolve_committed_offsets(_t, _p, :undefined) do
    []
  end

  defp resolve_committed_offsets(_t, partition, offset)
       when offset === :earliest or offset === :latest or offset === -2 or offset === -1 do
    [{partition, offset}]
  end

  defp resolve_committed_offsets(_t, partition, offset)
       when is_integer(offset) and offset >= 0 do
    [{partition, offset - 1}]
  end

  defp resolve_committed_offsets(topic, partition, offset) do
    Logger.warning(:io_lib.format(@discard_invalid_offset, [topic, partition, offset]), %{
      domain: [:brod]
    })

    []
  end
end
