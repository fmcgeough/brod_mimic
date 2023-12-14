# BrodMimic

An Elixir project to explore the Erlang library brod by porting it to Elixir.

This project was created for a couple of reasons.

1. the widespread popularity of using the `brod` library to connect to Kafka
2. Elixir developer's lack of knowledge on how to read the Erlang `brod` code.

I figured having an Elixir implementation would allow Elixir developers to examine
this code and then have a clearer understanding of how brod works. To make this a
reality I needed to convert the `brod` code file by file (so that a developer looking
at the code could align what is in this library with the `brod` code).

There are cases where new functions were introduced. Generally, this is to deal with
Erlang syntax that is extremely awkward and hard to read in Elixir. It was also done,
at times, to resolve credo issues (function too complex, etc).

Erlang functions like `:application.get_env` were converted to their Elixir equivalents.
In most cases the Elixir function has the same name so it's not too hard to compare
the `brod` code with this converted code.

The intention is to pull available doc from the `brod` code base and incorporate
it into the library.

The `brod` code base makes heavy use of `Record`. This is not something that is common
in Elixir code bases. In Erlang declaring a record allows defining the record and
specifying type information in a single statement. The Elixir approach requires a
definition of the record itself (using `defrecord`) and then a separate definition of
the record type information (see https://hexdocs.pm/elixir/1.12.3/Record.html#module-types).

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc).

## Notes on brod library in Elixir

- No plans (as of now) to convert the files: brod_cli.erl, brod_cli_pipe.erl. These are utility
  command line tools. Personally, I've never used them so I didn't think it was worth including
  that work.

## Current State

- no credo issues (`mix credo --strict` reports no problems)
- no dialyzer issues (`mix dialyzer` reports no problems)
- code compiles with no warnings/errors
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
