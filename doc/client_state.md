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
`consumers_sup` are processes started with `BrodMimic.Supervisor3`. The existing `state_info/1`
in that module can be used to get information.

```
iex> producers_sup = Keyword.get(client_state, :producers_sup)
iex> BrodMimic.Supervisor3.state_info(producers_sup)
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
