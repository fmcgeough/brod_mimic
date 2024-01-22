defmodule BrodMimic.TopicSubscriberCbFun do
  @moduledoc """
  A wrapper module that enables backward compatible use of
  `BrodMimic.TopicSubscriber` with a fun instead of a callback module.
  """
  @behaviour BrodMimic.TopicSubscriber

  import Record, only: [defrecordp: 2]

  defrecordp(:cbm_init_data, committed_offsets: :undefined, cb_fun: :undefined, cb_data: :undefined)

  @doc """
  This is needed to implement backward-consistent `cb_fun` interface.
  """
  @impl BrodMimic.TopicSubscriber
  def init(_topic, cbm_init_data(committed_offsets: committed_offsets, cb_fun: cb_fun, cb_data: state)) do
    {:ok, committed_offsets, {cb_fun, state}}
  end

  @impl BrodMimic.TopicSubscriber
  def handle_message(partition, msg, {cb_fun, state}) do
    case cb_fun.(partition, msg, state) do
      {:ok, :ack, state} ->
        {:ok, :ack, {cb_fun, state}}

      {:ok, state} ->
        {:ok, {cb_fun, state}}

      err ->
        err
    end
  end
end
