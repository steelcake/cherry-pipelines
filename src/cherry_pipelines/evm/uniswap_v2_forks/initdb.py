from cherry_pipelines.config import make_evm_table_name
from clickhouse_connect.driver.asyncclient import AsyncClient


async def init_db(client: AsyncClient, pipeline_name: str, slug: str, network: str):
    protocol_table_name = "evm.protocols"
    token_table_name = f"evm.{network}_tokens"
    swap_table_name = make_evm_table_name(slug, network, "swap")
    deposit_table_name = make_evm_table_name(slug, network, "deposit")
    withdraw_table_name = make_evm_table_name(slug, network, "withdraw")
    liquidity_pool_table_name = make_evm_table_name(slug, network, "liquidity_pool")
    interaction_table_name = make_evm_table_name(slug, network, "interaction")

    await client.command(f"""
        CREATE TABLE IF NOT EXISTS {protocol_table_name} (
            id String,
            name String,
            slug String,
            schema_version String,
            pipeline_version String,
            network String,
            type String,
            INDEX id_idx id TYPE minmax GRANULARITY 4,
        ) ENGINE = MergeTree
        ORDER BY id; 
    """)

    await client.command(f"""
        CREATE TABLE IF NOT EXISTS {token_table_name} (
            id String,
            address String,
            name String,
            symbol String,
            decimals UInt8,
            exe_timestamp_utc Int32,
            INDEX id_idx id TYPE minmax GRANULARITY 4
        ) ENGINE = MergeTree
        ORDER BY id; 
    """)

    await client.command(f"""
        CREATE OR REPLACE TABLE {liquidity_pool_table_name} (
            id String,
            address String,
            protocol String,
            name String,
            symbol String,
            input_tokens Array(String),
            output_token String,
            created_tx_hash String,
            created_timestamp Int64,
            created_block_number Int64,
            exe_timestamp_utc Int64,
            INDEX id_idx id TYPE minmax GRANULARITY 4,
            INDEX created_timestamp_idx created_timestamp TYPE minmax GRANULARITY 4
        ) ENGINE = MergeTree
        ORDER BY created_timestamp; 
    """)

    await client.command(f"""
        CREATE OR REPLACE TABLE {swap_table_name} (
            id String,
            protocol String,
            block_timestamp Int64,
            liquidity_pool String,
            token_sold String,
            token_sold_symbol String,
            amount_sold_raw Decimal256(0),
            amount_sold Decimal256(0),
            token_bought String,
            token_bought_symbol String,
            amount_bought_raw Decimal256(0),
            amount_bought Decimal256(0),
            reserve_amounts Array(Decimal256(0)),
            `from` String,
            `to` String,
            tx_from String,
            tx_to String,
            tx_hash String,
            block_number UInt64,
            tx_index UInt64,
            log_index UInt64,
            exe_timestamp_utc Int64,
            INDEX tx_hash_idx tx_hash TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX from_idx `from` TYPE bloom_filter(0.01) GRANULARITY 4, 
            INDEX to_idx `to` TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX liquidity_pool_idx liquidity_pool TYPE bloom_filter(0.01) GRANULARITY 4
        ) ENGINE = MergeTree
        ORDER BY (block_number, log_index); 
    """)

    await client.command(f"""
        CREATE OR REPLACE TABLE {deposit_table_name} (
            id String,
            protocol String,
            block_timestamp Int64,
            liquidity_pool String,
            input_tokens Array(String),
            input_token_symbols Array(String),
            output_token String,
            input_token_amounts Array(Decimal256(0)),
            reserve_amounts Array(Decimal256(0)),
            `from` String,
            `to` String,
            tx_from String,
            tx_to String,
            tx_hash String,
            block_number UInt64,
            tx_index UInt64,
            log_index UInt64,
            exe_timestamp_utc Int64,
            INDEX tx_hash_idx tx_hash TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX from_idx `from` TYPE bloom_filter(0.01) GRANULARITY 4, 
            INDEX to_idx `to` TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX liquidity_pool_idx liquidity_pool TYPE bloom_filter(0.01) GRANULARITY 4
        ) ENGINE = MergeTree
        ORDER BY (block_number, log_index); 
    """)

    await client.command(f"""
        CREATE OR REPLACE TABLE {withdraw_table_name} (
            id String,
            protocol String,
            block_timestamp Int64,
            liquidity_pool String,
            input_tokens Array(String),
            input_token_symbols Array(String),
            output_token String,
            input_token_amounts Array(Decimal256(0)),
            reserve_amounts Array(Decimal256(0)),
            `from` String,
            `to` String,
            tx_from String,
            tx_to String,
            tx_hash String,
            block_number UInt64,
            tx_index UInt64,
            log_index UInt64,
            exe_timestamp_utc Int64,
            INDEX tx_hash_idx tx_hash TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX from_idx `from` TYPE bloom_filter(0.01) GRANULARITY 4, 
            INDEX to_idx `to` TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX liquidity_pool_idx liquidity_pool TYPE bloom_filter(0.01) GRANULARITY 4
        ) ENGINE = MergeTree
        ORDER BY (block_number, log_index); 
    """)

    await client.command(f"""
        CREATE OR REPLACE TABLE {interaction_table_name} (
            id String,
            protocol String,
            block_timestamp Int64,
            liquidity_pool String,
            `from` String,
            `to` String,
            tx_from String,
            tx_to String,
            tx_hash String,
            block_number UInt64,
            tx_index UInt64,
            log_index UInt64,
            exe_timestamp_utc Int64,
            INDEX tx_hash_idx tx_hash TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX from_idx `from` TYPE bloom_filter(0.01) GRANULARITY 4, 
            INDEX to_idx `to` TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX liquidity_pool_idx liquidity_pool TYPE bloom_filter(0.01) GRANULARITY 4
        ) ENGINE = MergeTree
        ORDER BY (block_number, log_index); 
    """)
