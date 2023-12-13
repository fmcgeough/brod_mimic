# BrodMimic

An Elixir project to explore the Erlang library brod by porting it to Elixir.

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc).

## Notes

- (as of now) Will not convert the files: brod_cli.erl, brod_cli_pipe.erl
- no credo issues
- code compiles

## Tasks

- BrodMimic.Client starts up but an exception is raised on:
  `BrodMimic.Brod.get_partitions_count(client_id, some_topic)`
  with `{:unknown_vsn, [api: :metadata, vsn: :undefined, known_vsn_range: {0, 9}]}`
- Dialyzer is reporting issues. Total errors: 22
- Lots more types need to be defined
- Records were originally defined with `r_` as prefix. Want to remove that.
  Types needed for all records.
- No unit tests yet
