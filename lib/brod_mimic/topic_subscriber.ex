defmodule BrodMimic.TopicSubscriber do
  use GenServer

  import Bitwise
  import Record, only: [defrecord: 2, extract: 2]

  Record.defrecord(
    :kafka_message,
    extract(:kafka_message, from_lib: "kafka_protocol/include/kpro.hrl")
  )

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

  Record.defrecord(:r_consumer, :consumer,
    partition: :undefined,
    consumer_pid: :undefined,
    consumer_mref: :undefined,
    acked_offset: :undefined,
    last_offset: :undefined
  )

  Record.defrecord(:r_state, :state,
    client: :undefined,
    client_mref: :undefined,
    topic: :undefined,
    consumers: [],
    cb_module: :undefined,
    cb_state: :undefined,
    message_type: :undefined
  )

  def start_link(client, topic, partitions, consumer_config, cb_module, cb_init_arg) do
    args = %{
      client: client,
      topic: topic,
      partitions: partitions,
      consumer_config: consumer_config,
      message_type: :message,
      cb_module: cb_module,
      init_data: cb_init_arg
    }

    start_link(args)
  end

  def start_link(client, topic, partitions, consumer_config, message_type, cb_module, cb_init_arg) do
    args = %{
      client: client,
      topic: topic,
      partitions: partitions,
      consumer_config: consumer_config,
      message_type: message_type,
      cb_module: cb_module,
      init_data: cb_init_arg
    }

    start_link(args)
  end

  def start_link(
        client,
        topic,
        partitions,
        consumer_config,
        committed_offsets,
        message_type,
        cb_fun,
        cb_initial_state
      ) do
    init_data =
      r_cbm_init_data(
        committed_offsets: committed_offsets,
        cb_fun: cb_fun,
        cb_data: cb_initial_state
      )

    args = %{
      client: client,
      topic: topic,
      partitions: partitions,
      consumer_config: consumer_config,
      message_type: message_type,
      cb_module: :brod_topic_subscriber_cb_fun,
      init_data: init_data
    }

    start_link(args)
  end

  def start_link(config) do
    GenServer.start_link(:brod_topic_subscriber, config, [])
  end

  def stop(pid) do
    mref = :erlang.monitor(:process, pid)
    :ok = GenServer.cast(pid, :stop)

    receive do
      {:DOWN, ^mref, :process, ^pid, _reason} ->
        :ok
    end
  end

  def ack(pid, partition, offset) do
    GenServer.cast(pid, {:ack, partition, offset})
  end

  def init(config) do
    defaults = %{
      message_type: :message_set,
      init_data: :undefined,
      consumer_config: [],
      partitions: :all
    }

    %{
      client: client,
      topic: topic,
      cb_module: cb_module,
      init_data: init_data,
      message_type: message_type,
      consumer_config: consumer_config,
      partitions: partitions
    } = :maps.merge(defaults, config)

    {:ok, committed_offsets, cb_state} = cb_module.init(topic, init_data)

    :ok = BrodUtils.assert_client(client)
    :ok = BrodUtils.assert_topic(topic)
    send(self(), {:"$start_consumer", consumer_config, committed_offsets, partitions})

    state =
      r_state(
        client: client,
        client_mref: :erlang.monitor(:process, client),
        topic: topic,
        cb_module: cb_module,
        cb_state: cb_state,
        message_type: message_type
      )

    {:ok, state}
  end

  def handle_info({_consumer_pid, r_kafka_message_set() = msg_set}, state0) do
    state = handle_consumer_delivery(msg_set, state0)
    {:noreply, state}
  end

  def handle_info(
        {:"$start_consumer", consumer_config, committed_offsets, partitions0},
        r_state(client: client, topic: topic) = state
      ) do
    :ok = Brod.start_consumer(client, topic, consumer_config)

    {:ok, partitions_count} = Brod.get_partitions_count(client, topic)

    all_partitions = :lists.seq(0, partitions_count - 1)

    partitions =
      case partitions0 do
        :all ->
          all_partitions

        l when is_list(l) ->
          pS = :lists.usort(l)

          case :lists.min(pS) >= 0 and :lists.max(pS) < partitions_count do
            true ->
              pS

            false ->
              :erlang.error({:bad_partitions, partitions0, partitions_count})
          end
      end

    consumers =
      :lists.map(
        fn partition ->
          ackedOffset =
            case :lists.keyfind(
                   partition,
                   1,
                   committed_offsets
                 ) do
              {^partition, offset} ->
                offset

              false ->
                :undefined
            end

          r_consumer(
            partition: partition,
            acked_offset: ackedOffset
          )
        end,
        partitions
      )

    new_state = r_state(state, consumers: consumers)
    _ = send_lo_cmd(:"$subscribe_partitions")
    {:noreply, new_state}
  end

  def handle_info(:"$subscribe_partitions", state) do
    {:ok, r_state() = new_state} = subscribe_partitions(state)
    _ = send_lo_cmd(:"$subscribe_partitions", 2000)
    {:noreply, new_state}
  end

  def handle_info(
        {:DOWN, mref, :process, _Pid, _reason},
        r_state(client_mref: mref) = state
      ) do
    {:stop, :client_down, state}
  end

  def handle_info(
        {:DOWN, _mref, :process, pid, reason},
        r_state(consumers: consumers) = state
      ) do
    case get_consumer(pid, consumers) do
      r_consumer() = c ->
        consumer =
          r_consumer(c,
            consumer_pid: {:down, BrodUtils.os_time_utc_str(), reason},
            consumer_mref: :undefined
          )

        newConsumers = put_consumer(consumer, consumers)
        new_state = r_state(state, consumers: newConsumers)
        {:noreply, new_state}

      false ->
        {:noreply, state}
    end
  end

  def handle_info(_info, state) do
    {:noreply, state}
  end

  def handle_call(call, _from, state) do
    {:reply, {:error, {:unknown_call, call}}, state}
  end

  def handle_cast({:ack, partition, offset}, state) do
    ackRef = {partition, offset}
    new_state = handle_ack(ackRef, state)
    {:noreply, new_state}
  end

  def handle_cast(:stop, state) do
    {:stop, :normal, state}
  end

  def handle_cast(_Cast, state) do
    {:noreply, state}
  end

  def code_change(_OldVsn, state, _Extra) do
    {:ok, state}
  end

  def terminate(
        reason,
        r_state(cb_module: cb_module, cb_state: cb_state)
      ) do
    BrodUtils.optional_callback(cb_module, :terminate, [reason, cb_state], :ok)
    :ok
  end

  defp handle_consumer_delivery(
         r_kafka_message_set(topic: topic, partition: partition, messages: messages) = msgSet,
         r_state(topic: topic, message_type: msgType) = state0
       ) do
    state = update_last_offset(partition, messages, state0)

    case msgType do
      :message ->
        handle_messages(partition, messages, state)

      :message_set ->
        handle_message_set(msgSet, state)
    end
  end

  defp update_last_offset(partition, messages, r_state(consumers: consumers) = state) do
    kafka_message(offset: lastOffset) = :lists.last(messages)
    c = get_consumer(partition, consumers)
    consumer = r_consumer(c, last_offset: lastOffset)
    r_state(state, consumers: put_consumer(consumer, consumers))
  end

  defp subscribe_partitions(r_state(client: client, topic: topic, consumers: consumers0) = state) do
    consumers =
      :lists.map(
        fn c ->
          subscribe_partition(client, topic, c)
        end,
        consumers0
      )

    {:ok, r_state(state, consumers: consumers)}
  end

  defp subscribe_partition(client, topic, consumer) do
    r_consumer(
      partition: partition,
      consumer_pid: pid,
      acked_offset: ackedOffset,
      last_offset: lastOffset
    ) = consumer

    case BrodUtils.is_pid_alive(pid) do
      true ->
        consumer

      false
      when ackedOffset !== lastOffset and lastOffset !== :undefined ->
        consumer

      false ->
        options = resolve_begin_offset(ackedOffset)

        case Brod.subscribe(client, self(), topic, partition, options) do
          {:ok, consumerPid} ->
            mref = :erlang.monitor(:process, consumerPid)

            r_consumer(consumer,
              consumer_pid: consumerPid,
              consumer_mref: mref
            )

          {:error, reason} ->
            r_consumer(consumer,
              consumer_pid: {:down, BrodUtils.os_time_utc_str(), reason},
              consumer_mref: :undefined
            )
        end
    end
  end

  defp resolve_begin_offset(:undefined) do
    []
  end

  defp resolve_begin_offset(offset)
       when offset === :earliest or offset === :latest or offset === -2 or offset === -1 do
    [{:begin_offset, offset}]
  end

  defp resolve_begin_offset(offset) do
    beginOffset = offset + 1
    beginOffset >= 0 or :erlang.error({:invalid_offset, offset})
    [{:begin_offset, beginOffset}]
  end

  defp handle_message_set(messageSet, state) do
    r_kafka_message_set(partition: partition, messages: messages) = messageSet
    r_state(cb_module: cb_module, cb_state: cb_state) = state

    {ackNow, newCbState} =
      case cb_module.handle_message(partition, messageSet, cb_state) do
        {:ok, newCbState_} ->
          {false, newCbState_}

        {:ok, :ack, newCbState_} ->
          {true, newCbState_}
      end

    state1 = r_state(state, cb_state: newCbState)

    case ackNow do
      true ->
        lastMessage = :lists.last(messages)
        lastOffset = r_kafka_message(lastMessage, :offset)
        ackRef = {partition, lastOffset}
        handle_ack(ackRef, state1)

      false ->
        state1
    end
  end

  defp handle_messages(_Partition, [], state) do
    state
  end

  defp handle_messages(partition, [msg | rest], state) do
    r_kafka_message(offset: offset) = msg
    r_state(cb_module: cb_module, cb_state: cb_state) = state
    ackRef = {partition, offset}

    {ackNow, newCbState} =
      case cb_module.handle_message(partition, msg, cb_state) do
        {:ok, newCbState_} ->
          {false, newCbState_}

        {:ok, :ack, newCbState_} ->
          {true, newCbState_}
      end

    state1 = r_state(state, cb_state: newCbState)

    new_state =
      case ackNow do
        true ->
          handle_ack(ackRef, state1)

        false ->
          state1
      end

    handle_messages(partition, rest, new_state)
  end

  defp handle_ack(ackRef, r_state(consumers: consumers) = state) do
    {partition, offset} = ackRef

    r_consumer(consumer_pid: pid) =
      consumer =
      get_consumer(
        partition,
        consumers
      )

    :ok = consume_ack(pid, offset)
    newConsumer = r_consumer(consumer, acked_offset: offset)
    newConsumers = put_consumer(newConsumer, consumers)
    r_state(state, consumers: newConsumers)
  end

  defp get_consumer(partition, consumers)
       when is_integer(partition) do
    :lists.keyfind(partition, r_consumer(:partition), consumers)
  end

  defp get_consumer(pid, consumers) when is_pid(pid) do
    :lists.keyfind(pid, r_consumer(:consumer_pid), consumers)
  end

  defp put_consumer(r_consumer(partition: p) = consumer, consumers) do
    :lists.keyreplace(p, r_consumer(:partition), consumers, consumer)
  end

  defp consume_ack(pid, offset) do
    is_pid(pid) and Brod.consume_ack(pid, offset)
    :ok
  end

  defp send_lo_cmd(cMD) do
    send_lo_cmd(cMD, 0)
  end

  defp send_lo_cmd(cMD, 0) do
    send(self(), cMD)
  end

  defp send_lo_cmd(cMD, delayMS) do
    :erlang.send_after(delayMS, self(), cMD)
  end
end
