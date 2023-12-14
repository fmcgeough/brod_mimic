# BrodMimic

An Elixir project to explore the Erlang library brod by porting it to Elixir.

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc).

## Notes on brod library in Elixir

- No plans (as of now) to convert the files: brod_cli.erl, brod_cli_pipe.erl. These are utility
  command line tools. Personally, I've never used them so I didn't think it was worth including
  that work.

## Current State

- no credo issues
- code compiles
- simple `iex` session can start a client
- bringing up observer after starting simple `iex` session shows same processes started as
  the brod library

## Sample Session

```
$ iex -S mix
iex> default_brokers = [{"localhost", 9092}]
iex> client_id = :brod_mimic
iex> settings = [reconnect_cool_down_seconds: 5.0, auto_start_producers: true, default_producer_config: [retry_backoff_ms: 5000]]
iex> BrodMimic.Brod.start_client(default_brokers, client_id, settings)
:ok

13:24:08.734 [notice]     :supervisor: {:local, :brod_sup}
    :started: [
  pid: #PID<0.265.0>,
  id: :brod_mimic,
  mfargs: {:brod_client, :start_link,
   [
     [{"localhost", 9092}],
     :brod_mimic,
     [
       reconnect_cool_down_seconds: 5.0,
       auto_start_producers: true,
       default_producer_config: [retry_backoff_ms: 5000]
     ]
   ]},
  restart_type: {:permanent, 10},
  shutdown: 5000,
  child_type: :worker
]
iex> BrodMimic.Brod.get_partitions_count(client_id, "my_test_topic")
{:ok, 2}
```

## Tasks

- Lots more types need to be defined
- Records were originally defined with `r_` as prefix. Want to remove that.
  Types needed for all records.
- Unit tests must be written
- Need a docker-compose.yml file
