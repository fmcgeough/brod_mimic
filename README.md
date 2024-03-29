# BrodMimic

An Elixir project to explore the Erlang library
[brod](https://github.com/kafka4beam/brod/tree/master) by porting it to Elixir.

This project was created for a couple of reasons.

1. The `brod` library is the most popular means in Elixir to interface with Kafka.
2. Elixir developers generally lack knowledge on how to read the Erlang code.
   The `brod` code is complex and the end result is that Elixir developers may try
   and use the library as a black-box and that falls apart if problems occur
   that require some knowledge of how the library is talking to Kafka.
3. I figured having an Elixir implementation would allow Elixir developers to
   examine this code and then have a clearer understanding of how `brod` works.
   To make this a reality I needed to:
   - convert the `brod` code file by file (so that a developer looking at the code could
     align what is in this library with the`brod` code). So, for example, in the `brod`
     library there is a `src/brod_consumers_sup.erl` file. In this library there is a
     `lib/brod_mimic/consumers_sup.ex` file.
   - modify doc so that the information aligns with usage from Elixir (the `brod` documentation
     is written from an Erlang perspective)
   - add documentation that clarifies some of the more complicated aspects of the code

Developers should not use this library in lieu of `brod`. It's not nearly production
ready and the `brod` library has been used for years by all sorts of companies. This
project is meant to be educational.

## License/Notice

The `brod` library has an Apache 2.0 license. The same license is set for this
repo. The `brod` library includes the following notice:

```
Kafka client library in Erlang
Copyright 2014-2021 Klarna Bank AB (publ)

This product includes software developed by
Klarna Bank AB (publ) (https://www.klarna.com)
```

## Current State

- Fixed issue in ConsumerSup that resulted in deadlock and consumer not starting.
- Added a lot of logging that can provide insight into how a consumer is started up

- no credo issues (`mix credo --strict` reports no problems)
- no dialyzer issues (`mix dialyzer` reports no problems)
- code compiles with no warnings/errors
- simple `iex` session can start a client
- bringing up observer after starting simple `iex` session shows same processes started as
  the brod library. When the library is loaded `BrodMimic.Brod.start/2` is called (this is what
  is defined in the mix.exs file). This, in turn, calls `BrodMimic.Sup.start_link/0`. That module
  starts the `BrodMimic.KafkaApis` in it's init callback as it's supervised child process.
  ![Startup](./doc/images/brod_mimic_startup.png)
- Starting client (as `:brod_mimic`) starts under `BrodMimic.Sup`. The client
  process starts a consumer supervisor, producer supervisor and a metadata
  connection. ![After Client
Start](./doc/images/brod_mimic_after_client_start.png)
- simple publishing appears to work okay
- No plans (as of now) to convert the files: `brod_cli.erl`, `brod_cli_pipe.erl`. These are utility
  command line tools. Personally, I've never used them so I didn't think it was worth including
  that work.

## Porting Notes

### General

The `.formatter.exs` file is setup to have a line length of 120. This allows the long lines
in the `brod` Erlang code to be done on one line - which, hopefully, improves readability.

### Erlang functions converted to Elixir where possible

Erlang functions like `:application.get_env` were converted to their Elixir
equivalents. In most cases the Elixir function has the same name so it's not too
hard to compare the `brod` code with this converted code.

### Doc

The intention is to pull available doc from the `brod` code base and incorporate
it into this library. When the doc is moved over its edited to align with Elixir
naming and general standards. The goal is to make the doc as accessible as
possible for Elixir developers.

### New functions

There are cases where new functions are introduced in the Elixir version.
Generally, this is to deal with Erlang syntax that is extremely awkward and hard
to read in Elixir. It was also done, at times, to resolve credo issues (function
too complex, etc).

There are also some instances where debugging functions are introduced. For example,
`BrodMimic.Supervisor3` has a `state_info/1` function. This returns the internal
state (a Record) as a Keyword list. For example:

```
iex> pid = Process.whereis(:brod_sup)
iex> BrodMimic.Supervisor3.state_info(pid)
[
  name: {:local, :brod_sup},
  strategy: :one_for_one,
  children: [],
  dynamics: :undefined,
  intensity: 0,
  period: 1,
  restarts: [],
  module: BrodMimic.Sup,
  args: :clients_sup
]
```

This is much clearer than trying to use `:sys.get_state/1` which leaves out
the keys:

```
iex> :sys.get_state(pid)
{:state, {:local, :brod_sup}, :one_for_one, [], :undefined, 0, 1, [],
 BrodMimic.Sup, :clients_sup}
```

### Erlang catch

There are situations where the Erlang code converted directly to equivalent
Elixir is extremely difficult to read. In this case, there may be helper
functions created.

One example relates to the Erlang `catch` keyword. In Erlang you can do the following (as
demonstrated in [Learn You Some Erlang](https://learnyousomeerlang.com/errors-and-exceptions)):

```
catcher(X,Y) ->
  case catch X/Y of
    {'EXIT', {badarith,_}} -> "uh oh";
   N -> N
end.
```

In Elixir you cannot use `catch` in this way. So the direct implementation to Elixir
becomes something like:

```
def catcher(x, y) do
  case (try do
    x/y
  catch
    :error, e -> {:EXIT, {e, __STACKTRACE__}}
    :exit, e -> {:EXIT, e}
    e -> e
  end) do
    {:EXIT, {:badarith, _}} -> "uh oh"
    n -> n
  end
```

That's pretty hideous looking. So instead of doing this in Supervisor3 in
the `code_change` function

```
case (State#state.module):init(State#state.args) of
    {ok, {SupFlags, StartSpec}} ->
        case catch check_flags(SupFlags) of
            ok ->
                {Strategy, MaxIntensity, Period} = SupFlags,
                update_childspec(State#state{strategy = Strategy,
                                              intensity = MaxIntensity,
                                              period = Period},
                                  StartSpec);
            Error ->
                {error, Error}
        end;
```

In `BrodMimic.Supervisor3` this is:

```
case state(state, :module).init(state(state, :args)) do
  {:ok, {sup_flags, start_spec}} ->
    case check_flags_with_catch(sup_flags) do
      :ok ->
        {strategy, max_intensity, period} = sup_flags
        update_childspec(state(state, strategy: strategy, intensity: max_intensity, period: period), start_spec)

      error ->
        {:error, error}
    end
```

and the helper function `check_flags_with_catch` takes care of the exception handling.

```
  defp check_flags_with_catch(sup_flags) do
    check_flags(sup_flags)
  catch
    :error, e -> {:EXIT, {e, __STACKTRACE__}}
    :exit, e -> {:EXIT, e}
    e -> e
  end
```

### Records

The `brod` code base makes heavy use of `Record`. This is not something that is
common in Elixir code bases. In Erlang declaring a record allows defining the
record and specifying type information in a single statement. The Elixir
approach requires a definition of the record itself (using `defrecord`) and then
a separate definition of the record type information (see
https://hexdocs.pm/elixir/1.12.3/Record.html#module-types).

This library defines Records using `defrecordp`. This keeps the somewhat useless
information about the Record from showing up in the generated doc. If a type is
associated with a Record its documentation is generated. That shows the fields in
the Record and the field's type.

### Internal Supervisor

The `brod` code base uses its own `Supervisor` implementation. This has been
ported over to Elixir.

### Erlang Macros

The `brod` code makes significant use of Erlang macros. The module `BrodMimic.Macros`
was created to emulate the Erlang macros. In some cases the macro was discarded and
the code the macro expands to was just put in place in the Elixir version.

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

- Check that all records have a corresponding type
- Unit tests must be written
- Need a docker-compose.yml file
