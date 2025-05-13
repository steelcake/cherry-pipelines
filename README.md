# cherry-pipelines

This is a collection of pipelines that are built using cherry and ClickHouse materialized views.

All data is stored in ClickHouse.

## Configuration

- `CHERRY_FROM_BLOCK`, specify the block that the indexing should start from. defaults to 0.
- `CHERRY_TO_BLOCK`, specify the block that the indexing should stop at. has no default. Indexing waits for new blocks
- `CHERRY_EVM_PROVIDER_KIND`, specify which provider to use when indexing evm chains. Can be `hypersync` or `sqd`. Has no default an is required when indexing evm.
- `CHERRY_EVM_CHAIN_ID`, specify the chain_id when indexing an evm chain. has no default and is required when indexing evm.
when it reaches the tip of the chain if this argument is left empty.
- `CLICKHOUSE_HOST`, defaults to `127.0.0.1`.
- `CLICKHOUSE_PORT`, defaults to `8123`.
- `CLICKHOUSE_USER`, defaults to `default`.
- `CLICKHOUSE_PASSWORD`, defaults to empty string,
- `RUST_LOG` as explained in [env-logger docs](https://docs.rs/env_logger/latest/env_logger/#enabling-logging)
- `PY_LOG` as explained in [python logging docs](https://docs.python.org/3/howto/logging.html). Defaults to "INFO"

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

### Run a pipeline

```bash
uv run pipelines/evm/erc20_transfers.py
```

## Data Provider

All svm pipelines use `SQD`.

All evm pipelines are configurable using the `CHERRY_EVM_PROVIDER_KIND` env variable.

## Table definitions

Automatic table creation features of cherry aren't used and table definitions are managed separately. 

## Materialized Views

Materialized views are defined in SQL files with an accompanying script that deploys them.

## EVM multi-chain structure

The evm pipelines are multi-chain and index multiple blockchains in parallel.

All chains are written to their own tables. For example the table for erc20 transfers would have a table named
`erc20_chain1` for ethereum and `erc20_chain10` for optimism.

Specify the `CHERRY_EVM_CHAIN_ID` env variable to set the chain you want to index when indexing evm.

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
