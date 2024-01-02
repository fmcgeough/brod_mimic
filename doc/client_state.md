## Overview

After starting the client process, information about the client and the processes it starts can
be obtained using `state_info/1`/

## Usage

```
$ iex -S mix
iex> default_brokers = [{"localhost", 9092}]
iex> client_id = :brod_mimic
iex> settings = [reconnect_cool_down_seconds: 5.0, auto_start_producers: true, default_producer_config: [retry_backoff_ms: 5000]]
iex> BrodMimic.Brod.start_client(default_brokers, client_id, settings)
iex> client_state = BrodMimic.Client.state_info(client_id)
[
  client_id: :brod_mimic,
  bootstrap_endpoints: [{"localhost", 9092}],
  meta_conn: #PID<0.312.0>,
  payload_conns: [],
  producers_sup: #PID<0.316.0>,
  consumers_sup: #PID<0.317.0>,
  config: [
    reconnect_cool_down_seconds: 5.0,
    auto_start_producers: true,
    default_producer_config: [retry_backoff_ms: 5000]
  ],
  workers_tab: :brod_mimic
]
```

The `meta_conn` is a Process related to the Kafka protocol library. The `producers_sup` and
`consumers_sup` are processes started with `BrodMimic.Supervisor3`. The function
`supervisor_state_info/1` in that module can be used to get information on these supervisor
processes (there is also a `child_state_info/1` in that same module).

```
iex> producers_sup = Keyword.get(client_state, :producers_sup)
iex> BrodMimic.Supervisor3.supervisor_state_info(producers_sup)
[
  name: {#PID<0.316.0>, BrodMimic.ProducersSup},
  strategy: :one_for_one,
  children: [],
  dynamics: :undefined,
  intensity: 0,
  period: 1,
  restarts: [],
  module: BrodMimic.ProducersSup,
  args: BrodMimic.ProducersSup
]
iex> consumers_sup = Keyword.get(client_state, :consumers_sup)
iex> BrodMimic.Supervisor3.state_info(consumers_sup)
[
  name: {#PID<0.317.0>, BrodMimic.ConsumersSup},
  strategy: :one_for_one,
  children: [],
  dynamics: :undefined,
  intensity: 0,
  period: 1,
  restarts: [],
  module: BrodMimic.ConsumersSup,
  args: :brod_consumers_sup
]
```

## How to when using `brod`

This library has the same general framework as `brod`. This means you can use some of
this knowledge with the `brod` library. There are two things that you'll need to
work around.

- there is no `state_info` call for `:brod_supervisor3` or `:brod_client`
- the record information in `brod` cannot be easily extracted using Elixir

You can use `:sys.get_state/1` to extract the process state information. However, this
dumps the data without the Record keys. It's not possible to extract the record from
the Erlang source file. The Elixir Record module expects the definition to be in an
Erlang hrl file.

```
iex> default_brokers = [{"localhost", 9092}]
iex> client_id = :brod_mimic
iex> settings = [reconnect_cool_down_seconds: 5.0, auto_start_producers: true, default_producer_config: [retry_backoff_ms: 5000]]
iex> :brod.start_client(default_brokers, client_id, settings)
iex> pid = Process.whereis(client_id)
iex> client_state = :sys.get_state(pid)
{
  :brod_mimic,
  [{"localhost", 9092}],
  #PID<0.312.0>,
  [],
  #PID<0.316.0>,
  #PID<0.317.0>,
  [
    reconnect_cool_down_seconds: 5.0,
    auto_start_producers: true,
    default_producer_config: [retry_backoff_ms: 5000]
  ],
  :brod_mimic
}
```

That's obviously not easy to read. If we want to add the keys we'll have to do so manually. We'll turn
the returned tuple into a List and drop the first element that identifies the Record name (`:state` in
this case). Then we use `Enum.zip/2` to combine the keys with the values and `Map.new` to create a Map
of the result.

```
iex> client_state_data = Tuple.to_list(client_state) |> Enum.drop(1)
iex> client_state_keys = [:client_id, :bootstrap_endpoints, :meta_conn, :payload_conns, :producers_sup, :consumers_sup, :config, :workers_tab]
iex> client_state_map = client_state_keys |> Enum.zip(client_state_data) |> Map.new()
%{
  client_id: :brod_mimic,
  bootstrap_endpoints: [{"localhost", 9092}],
  meta_conn: #PID<0.312.0>,
  payload_conns: [],
  producers_sup: #PID<0.316.0>,
  consumers_sup: #PID<0.317.0>,
  config: [
    reconnect_cool_down_seconds: 5.0,
    auto_start_producers: true,
    default_producer_config: [retry_backoff_ms: 5000]
  ],
  workers_tab: :brod_mimic
}
```

Now we have a Map with keys and it's much easier to see what's going on.
Unfortunately, we're hard-coding the keys in the Erlang record. However, this is
worth doing if you want to examine `brod` internals. We could just put it
altogether in a pipeline now that you see what's going on:

```
iex> client_id |> Process.whereis() |> :sys.get_state() |> Tuple.to_list() |> Enum.drop(1) |> then(&Enum.zip(client_state_keys, &1))
```

The `consumers_sup` and `producers_sup` are Supervisor type processes started with `BrodMimic.Supervisor3`. So
we can do something similar with those to print out useful information. Once we have a consumer pid we can do the
same with it. Note that with Supervisor3 there are two types of states you have to deal with. The top-level process
contains a supervisor state. This state has a children key. The value stored is a list of child records.

```
# Define the keys we'll use for different states
iex> supervisor_keys =  [:name, :strategy, :children, :dynamics, :intensity, :period, :restarts, :module, :args]
iex> supervisor_child_keys = [:pid, :name, :mfargs, :restart_type, :shutdown, :child_type, :modules]
iex> consumer_keys = [:bootstrap, :connection, :topic, :partition, :begin_offset, :max_wait_time, :min_bytes,
    :max_bytes_orig, :sleep_timeout, :prefetch_count, :last_req_ref, :subscriber, :subscriber_mref, :pending_acks,
    :is_suspended,  :offset_reset_policy, :avg_bytes, :max_bytes, :size_stat_window, :prefetch_bytes,
    :connection_mref, :isolation_level]
# Get the state for the consumers supervisor
iex> consumer_sup_state = client_state_map.consumers_sup |> :sys.get_state() |> Tuple.to_list() |> Enum.drop(1) |> then(&Enum.zip(supervisor_keys, &1)) |> Map.new()
iex> children_states = Enum.map(supervisor_state.children, fn child ->
  data = child |> Tuple.to_list() |> Enum.drop(1) |> then(&Enum.zip(supervisor_child_keys, &1)) |> Map.new()
end)
```

It's a lot of work to follow the chain of these processes and get insights into what is going on. However,
it is possible. And it's most likely worth creating a library that makes this easier.
