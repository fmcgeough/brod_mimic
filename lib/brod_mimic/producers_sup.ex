defmodule BrodMimic.ProducersSup do
  @behaviour BrodSupervisor3

  import Bitwise

  require Record

  alias BrodMimic.Supervisor3, as: BrodSupervisor3
  alias BrodMimic.Client, as: BrodClient

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

  def start_link() do
    BrodSupervisor3.start_link(__MODULE__, __MODULE__)
  end

  def start_producer(sup_pid, client_pid, topic_name, config) do
    spec = producers_sup_spec(client_pid, topic_name, config)
    BrodSupervisor3.start_child(sup_pid, spec)
  end

  def stop_producer(sup_pid, topic_name) do
    BrodSupervisor3.terminate_child(sup_pid, topic_name)
    BrodSupervisor3.delete_child(sup_pid, topic_name)
  end

  def find_producer(sup_pid, topic, partition) do
    case BrodSupervisor3.find_child(sup_pid, topic) do
      [] ->
        {:error, {:producer_not_found, topic}}

      [partitions_sup_pid] ->
        try do
          case BrodSupervisor3.find_child(
                 partitions_sup_pid,
                 partition
               ) do
            [] ->
              {:error, {:producer_not_found, topic, partition}}

            [pid] ->
              {:ok, pid}
          end
        catch
          :exit, {reason, _} ->
            {:error, {:producer_down, reason}}
        end
    end
  end

  def init(__MODULE__) do
    {:ok, {{:one_for_one, 0, 1}, []}}
  end

  def init({:brod_producers_sup2, _client_pid, _topic, _config}) do
    :post_init
  end

  def post_init({:brod_producers_sup2, client_pid, topic, config}) do
    case BrodClient.get_partitions_count(client_pid, topic) do
      {:ok, partitions_cnt} ->
        children =
          for partition <-
                :lists.seq(
                  0,
                  partitions_cnt - 1
                ) do
            producer_spec(client_pid, topic, partition, config)
          end

        {:ok, {{:one_for_one, 0, 1}, children}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp producers_sup_spec(client_pid, topic_name, config0) do
    {config, delay_secs} = take_delay_secs(config0, :topic_restart_delay_seconds, 10)
    args = [:brod_producers_sup, {:brod_producers_sup2, client_pid, topic_name, config}]

    {_id = topic_name, _start = {BrodSupervisor3, :start_link, args},
     _restart = {:permanent, delay_secs}, _shutdown = :infinity, _type = :supervisor,
     _module = [:brod_producers_sup]}
  end

  defp producer_spec(client_pid, topic, partition, config0) do
    {config, delay_secs} = take_delay_secs(config0, :partition_restart_delay_seconds, 5)
    args = [client_pid, topic, partition, config]

    {_id = partition, _start = {:brod_producer, :start_link, args},
     _restart = {:permanent, delay_secs}, _shutdown = 5000, _type = :worker,
     _module = [:brod_producer]}
  end

  defp take_delay_secs(config, name, default_value) do
    secs =
      case :proplists.get_value(name, config) do
        n when is_integer(n) and n >= 1 ->
          n

        _ ->
          default_value
      end

    {:proplists.delete(name, config), secs}
  end
end
