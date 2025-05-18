# cherry-pipelines

This is a collection of pipelines that are built using [cherry](https://github.com/steelcake/cherry) and ClickHouse materialized views.

All data is stored in ClickHouse.

## Python version

This project is meant to be run with Python 3.12

If you are using `uv` for development it should pick this up automatically because of the `.python-version` in the project root.

The docker image is configured to use this version of Python as well.

## Running a pipeline 

Use the `main` script to run a pipeline:

```bash
uv run scripts/main.py
```

It takes these parameters as environment variables:

- `CHERRY_PIPELINE_KIND`, "evm" or "svm".
- `CHERRY_PIPELINE_NAME`, name of the pipeline to run e.g. "erc20_transfers".
- `CHERRY_FROM_BLOCK`, specify the block that the indexing should start from. defaults to 0.
- `CHERRY_TO_BLOCK`, specify the block that the indexing should stop at. has no default. Indexing waits for new blocks when it reaches the tip of the chain if this argument is left empty.
- `CHERRY_EVM_PROVIDER_KIND`, specify which provider to use when indexing evm chains. Can be `hypersync` or `sqd`. Has no default and is required when indexing evm.
- `CHERRY_EVM_CHAIN_ID`, specify the chain_id when indexing an evm chain. has no default and is required when indexing evm.
- `CHERRY_PROVIDER_BUFFER_SIZE`, specify buffering between ingestion - processing - writer. Increasing this parameter might improve performance but can also cause higher memory usage. Defaults to 2.
- `CHERRY_INIT_DB`, It runs db setup script instead of the pipeline script if this is set to "true". 
- `CLICKHOUSE_HOST`, defaults to `127.0.0.1`.
- `CLICKHOUSE_PORT`, defaults to `8123`.
- `CLICKHOUSE_USER`, defaults to `default`.
- `CLICKHOUSE_PASSWORD`, defaults to empty string,
- `RUST_LOG` as explained in [env-logger docs](https://docs.rs/env_logger/latest/env_logger/#enabling-logging)
- `PY_LOG` as explained in [python logging docs](https://docs.python.org/3/howto/logging.html). Defaults to "INFO"

An `.env` file placed in the project root can be used to define these for development.

## Running with docker

We publish a docker image that runs the `main` script.

## Dev Setup

Run the docker-compose file to start a clickhouse instance for development.

```bash
docker-compose up -d
```

Run this to delete the data on disk:
```bash
docker-compose down -v
```

And this to stop the container without deleting the data:
```bash
docker-compose down
```

## Development

This repo uses `uv` for development.

- Format the code with `uv run ruff format`
- Lint the code with `uv run ruff check`
- Run type checks with `uv run pyright`
- Run the tests with `uv run pytest`

## Data Provider

All svm pipelines use `SQD`.

All evm pipelines are configurable using the `CHERRY_EVM_PROVIDER_KIND` env variable.

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

## Sponsors

[<img src="https://steelcake.com/envio-logo.png" width="150px" />](https://envio.dev)
[<img src="https://steelcake.com/sqd-logo.png" width="165px" />](https://sqd.ai)
[<img src="https://steelcake.com/space-operator-logo.webp" height="75px" />](https://linktr.ee/spaceoperator)
