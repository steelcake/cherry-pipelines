import pyarrow as pa
import polars as pl
import asyncio
from typing import Dict, Any, List
from datetime import datetime, timezone
from clickhouse_connect.driver.exceptions import DatabaseError

from cherry_etl import config as cc
from cherry_etl.pipeline import run_pipeline
from cherry_etl.writers.writer import create_writer
from cherry_core import evm_signature_to_topic0, get_token_metadata_as_table
from cherry_core.ingest import (
    Query as IngestQuery,
    QueryKind,
)
from cherry_core.ingest.evm import (
    Query,
    Fields,
    BlockFields,
    TransactionFields,
    LogFields,
    LogRequest,
)

from cherry_pipelines import db
from cherry_pipelines.config import (
    EvmConfig,
    make_evm_table_name,
)

from ..pipeline import EvmPipeline
from . import transformations

from clickhouse_connect.driver.asyncclient import AsyncClient

class Pipeline(EvmPipeline):
    async def run(self, cfg: EvmConfig, pipeline_name: str):
        await run(cfg, pipeline_name)

    async def init_db(self, client: AsyncClient, pipeline_name: str):
        await init_db(client, pipeline_name)

PAIR_CREATED_EVENT_SIGNATURE = "PairCreated(address indexed token0, address indexed token1, address pair, uint256)"
MINT_EVENT_SIGNATURE = "Mint(address indexed sender, uint amount0, uint amount1)"
BURN_EVENT_SIGNATURE = "Burn(address indexed sender, uint amount0, uint amount1, address indexed to)"
SWAP_EVENT_SIGNATURE = "Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)"
SYNC_EVENT_SIGNATURE = "Sync(uint112 reserve0, uint112 reserve1)"

SCHEMA_VERSION = "1.0.0"
PIPELINE_VERSION = "1.0.0"

UNISWAP_V2_FORK_LIST = {
    "uniswap_v2_ethereum": [
        "Uniswap V2",
        "ethereum",
        "uniswap_v2",
        "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
        10000835
    ],
    "sushiswap_ethereum": [
        "SushiSwap",
        "ethereum",
        "sushiswap",
        "0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac",
        10794229
    ],
}


async def init_db(client: AsyncClient, pipeline_name: str):
    slug = UNISWAP_V2_FORK_LIST[pipeline_name][2]
    network = UNISWAP_V2_FORK_LIST[pipeline_name][1]
    protocol_table_name = f"evm.protocols"
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
        CREATE TABLE IF NOT EXISTS {liquidity_pool_table_name} (
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


async def create_row_for_protocol_table(
    address: str, name: str, slug: str, network: str
) -> pl.DataFrame:
    protocol_df = pl.DataFrame(
        {
            "id": [address],
            "name": [name],
            "slug": [slug],
            "schema_version": [SCHEMA_VERSION],
            "pipeline_version": [PIPELINE_VERSION],
            "network": [network],
            "type": ["stablecoin"],
        }
    )
    return protocol_df


def create_row_for_token_table(
    cfg: EvmConfig, token_address: List[str]
) -> pl.DataFrame:
    current_time = int(datetime.now(timezone.utc).timestamp())
    token_metadata = get_token_metadata_as_table(
        cfg.rpc_provider_url,
        token_address,
    )

    token_df = pl.from_arrow(token_metadata)
    token_df = token_df.select(
        pl.concat_str(pl.lit("0x"),pl.col("address").bin.encode("hex").str.to_lowercase()).alias("id"),
        pl.col("address").alias("address"),
        pl.col("name").alias("name"),
        pl.col("symbol").alias("symbol"),
        pl.col("decimals").alias("decimals"),
        pl.lit(current_time).alias("exe_timestamp_utc")
    )

    return token_df


def split_logs(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    logs = data["logs"]

    data["pair_created_logs"] = logs.filter(pl.col("topic0").bin.encode("hex").str.to_lowercase() == evm_signature_to_topic0(PAIR_CREATED_EVENT_SIGNATURE).removeprefix("0x"))
    data["mint_logs"] = logs.filter(pl.col("topic0").bin.encode("hex").str.to_lowercase() == evm_signature_to_topic0(MINT_EVENT_SIGNATURE).removeprefix("0x"))
    data["burn_logs"] = logs.filter(pl.col("topic0").bin.encode("hex").str.to_lowercase() == evm_signature_to_topic0(BURN_EVENT_SIGNATURE).removeprefix("0x"))
    data["swap_logs"] = logs.filter(pl.col("topic0").bin.encode("hex").str.to_lowercase() == evm_signature_to_topic0(SWAP_EVENT_SIGNATURE).removeprefix("0x"))
    data["sync_logs"] = logs.filter(pl.col("topic0").bin.encode("hex").str.to_lowercase() == evm_signature_to_topic0(SYNC_EVENT_SIGNATURE).removeprefix("0x"))

    return data
    

def save_to_parquet(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    for key, df in data.items():
        df.write_parquet(f"data/{key}.parquet")

    output_dict = {
        "ethereum_tokens": data["ethereum_tokens"],
        "uniswap_v2_ethereum_liquidity_pool": data["uniswap_v2_ethereum_liquidity_pool"],
        "uniswap_v2_ethereum_swap": data["uniswap_v2_ethereum_swap"],
        "uniswap_v2_ethereum_deposit": data["uniswap_v2_ethereum_deposit"],
        "uniswap_v2_ethereum_withdraw": data["uniswap_v2_ethereum_withdraw"],
        "uniswap_v2_ethereum_interaction": data["uniswap_v2_ethereum_interaction"],
    }
    return output_dict

def transformations(data: Dict[str, pl.DataFrame], context: Any) -> Dict[str, pl.DataFrame]:
    # context variables
    tables = context["tables"]
    cfg = context["cfg"]
    persistent_token_df = context["persistent_token_df"]
    factory_address = context["factory_address"]

    # input data tables
    pair_created_logs_df = data["pair_created_logs"]
    swap_logs_df = data["swap_logs"]
    mint_logs_df = data["mint_logs"]
    burn_logs_df = data["burn_logs"]
    sync_logs_df = data["sync_logs"]

    output_dict = {}
    
    new_tokens_list = transformations.get_new_tokens(pair_created_logs_df, persistent_token_df)
    new_tokens_df = create_row_for_token_table(
        cfg=cfg,
        token_address=new_tokens_list,
    )
    persistent_token_df.vstack(new_tokens_df, in_place=True)
    output_dict[tables["token"]] = new_tokens_df
    
    liquidity_pool_df = transformations.get_liquidity_pool_df(pair_created_logs_df, persistent_token_df, factory_address)
    output_dict[tables["liquidity_pool"]] = liquidity_pool_df

    pair_created_logs_df = pair_created_logs_df.select(
        pl.col("pair"),
        pl.col("token0"),
        pl.col("token1"),
    )
    sync_logs_df = sync_logs_df.select(
        pl.col("transaction_hash"),
        pl.col("address"),
        pl.col("log_index"),
        pl.col("reserve0"),
        pl.col("reserve1"),
    )

    swap_df = transformations.get_swap_df(swap_logs_df, pair_created_logs_df, persistent_token_df, sync_logs_df, factory_address)
    output_dict[tables["swap"]] = swap_df

    deposit_df = transformations.get_deposit_df(mint_logs_df, pair_created_logs_df, persistent_token_df, sync_logs_df, factory_address)
    output_dict[tables["deposit"]] = deposit_df

    withdraw_df = transformations.get_withdraw_df(burn_logs_df, pair_created_logs_df, persistent_token_df, sync_logs_df, factory_address)
    output_dict[tables["withdraw"]] = withdraw_df

    interaction_df = transformations.get_interaction_df(swap_df, deposit_df, withdraw_df)
    output_dict[tables["interaction"]] = interaction_df

    return output_dict


async def run(cfg: EvmConfig, pipeline_name: str) -> cc.Pipeline:
    try:
        name = UNISWAP_V2_FORK_LIST[pipeline_name][0]
        network = UNISWAP_V2_FORK_LIST[pipeline_name][1]
        slug = UNISWAP_V2_FORK_LIST[pipeline_name][2]
        address = UNISWAP_V2_FORK_LIST[pipeline_name][3]
        deployment_block = UNISWAP_V2_FORK_LIST[pipeline_name][4]
    except KeyError:
        raise ValueError(f"Pipeline name {pipeline_name} not found in stablecoin list")

    return await pipeline_factory(cfg, name, network, slug, address, deployment_block)

async def pipeline_factory(
    cfg: EvmConfig, name: str, network: str, slug: str, address: str, deployment_block: int
):
    tables = {
        "swap": make_evm_table_name(slug, network, "swap"),
        "deposit": make_evm_table_name(slug, network, "deposit"),
        "withdraw": make_evm_table_name(slug, network, "withdraw"),
        "liquidity_pool": make_evm_table_name(slug, network, "liquidity_pool"),
        "interaction": make_evm_table_name(slug, network, "interaction"),
        "protocol": "protocols",
        "token": "ethereum_tokens",
    }
    
    writer = cc.Writer(
        kind=cc.WriterKind.CLICKHOUSE,
        config=cc.ClickHouseWriterConfig(
            client=cfg.client,
            create_tables=False,
        ),
    )

    protocol_df = await create_row_for_protocol_table(
        address=address, name=name, slug=slug, network=network
    )

    try:
        protocol_exist = await cfg.client.query(
            f"SELECT CASE WHEN EXISTS (SELECT 1 FROM evm.protocols WHERE id = '{address}' AND network = '{network}') THEN 1 ELSE 0 END AS exists_flag;"
        )
        protocol_exist = bool(protocol_exist.result_rows[0][0])
    except DatabaseError:
        protocol_exist = False

    if not protocol_exist:
        data_writer = create_writer(writer)
        await data_writer.push_data({"protocols": protocol_df.to_arrow()})

    pair_created_topic0 = evm_signature_to_topic0(PAIR_CREATED_EVENT_SIGNATURE)
    mint_topic0 = evm_signature_to_topic0(MINT_EVENT_SIGNATURE)
    burn_topic0 = evm_signature_to_topic0(BURN_EVENT_SIGNATURE)
    swap_topic0 = evm_signature_to_topic0(SWAP_EVENT_SIGNATURE)
    sync_topic0 = evm_signature_to_topic0(SYNC_EVENT_SIGNATURE)

    # Need to improve this, because the last block might not have a swap event
    # last_from_block = await db.get_max_block(
    #     cfg.client, tables["swap"], "block_number"
    # )
    from_block = max(cfg.from_block, deployment_block) # 
    to_block = cfg.to_block

    # Querying
    query = IngestQuery(
        kind=QueryKind.EVM,
        params=Query(
            from_block=from_block,
            to_block=to_block,
            include_all_blocks=False,
            logs=[
                LogRequest(
                    address=[address],
                    topic0=[pair_created_topic0],
                    include_transactions=True,
                    include_blocks=True,
                ),
                LogRequest(
                    topic0=[mint_topic0, burn_topic0, swap_topic0, sync_topic0],
                    include_transactions=True,
                    include_blocks=True,
                ),
            ],
            fields=Fields(
                block=BlockFields(number=True, timestamp=True),
                transaction=TransactionFields(
                    block_number=True,
                    transaction_index=True,
                    from_=True,
                    to=True,
                ),
                log=LogFields(
                    block_number=True,
                    block_hash=True,
                    transaction_index=True,
                    log_index=True,
                    transaction_hash=True,
                    address=True,
                    topic0=True,
                    topic1=True,
                    topic2=True,
                    topic3=True,
                    data=True,
                ),
            ),
        ),
    )
    persistent_token = await cfg.client.query_arrow(
        f"SELECT * FROM {tables['token']}"
    )
    schema = pa.schema([
        pa.field('id', pa.string(), nullable=False),
        pa.field('address', pa.binary(), nullable=False),
        pa.field('name', pa.string(), nullable=False),
        pa.field('symbol', pa.string(), nullable=False),
        pa.field('decimals', pa.uint8(), nullable=False),
        pa.field('exe_timestamp_utc', pa.int32(), nullable=False)
    ])
    persistent_token = persistent_token.cast(schema)
    persistent_token_df = pl.from_arrow(persistent_token)
    print(persistent_token_df)

    # Transformation Steps
    steps = [
        # Handle decimal256 values
        cc.Step(
            kind=cc.StepKind.CAST_BY_TYPE,
            config=cc.CastByTypeConfig(
                from_type=pa.decimal256(76, 0),
                to_type=pa.float64(),
            ),
        ),
        cc.Step(
            kind=cc.StepKind.CUSTOM,
            config=cc.CustomStepConfig(
                runner=split_logs
            )
        ),
        cc.Step(
            kind=cc.StepKind.EVM_DECODE_EVENTS,
            config=cc.EvmDecodeEventsConfig(
                event_signature=PAIR_CREATED_EVENT_SIGNATURE,
                input_table="pair_created_logs",
                output_table="pair_created_logs",
                hstack=True,
                allow_decode_fail=True,
            ),
        ),
        cc.Step(
            kind=cc.StepKind.EVM_DECODE_EVENTS,
            config=cc.EvmDecodeEventsConfig(
                event_signature=MINT_EVENT_SIGNATURE,
                input_table="mint_logs",
                output_table="mint_logs",
                hstack=True,
                allow_decode_fail=True,
            ),
        ),
        cc.Step(
            kind=cc.StepKind.EVM_DECODE_EVENTS,
            config=cc.EvmDecodeEventsConfig(
                event_signature=BURN_EVENT_SIGNATURE,
                input_table="burn_logs",
                output_table="burn_logs",
                hstack=True,
                allow_decode_fail=True,
            ),
        ),
        cc.Step(
            kind=cc.StepKind.EVM_DECODE_EVENTS,
            config=cc.EvmDecodeEventsConfig(
                event_signature=SWAP_EVENT_SIGNATURE,
                input_table="swap_logs",
                output_table="swap_logs",
                hstack=True,
                allow_decode_fail=True,
            ),
        ),
        cc.Step(
            kind=cc.StepKind.EVM_DECODE_EVENTS,
            config=cc.EvmDecodeEventsConfig(
                event_signature=SYNC_EVENT_SIGNATURE,
                input_table="sync_logs",
                output_table="sync_logs",
                hstack=True,
                allow_decode_fail=True,
            ),
        ),
        # Handle decimal256 values
        cc.Step(
            kind=cc.StepKind.CAST_BY_TYPE,
            config=cc.CastByTypeConfig(
                from_type=pa.decimal256(76, 0),
                to_type=pa.float64(),
            ),
        ),
        # Join the transaction data
        cc.Step(
            kind=cc.StepKind.JOIN_EVM_TRANSACTION_DATA,
            config=cc.JoinEvmTransactionDataConfig(),
        ),
        # Join the block data
        cc.Step(
            kind=cc.StepKind.JOIN_BLOCK_DATA,
            config=cc.JoinBlockDataConfig(),
        ),
        cc.Step(
            kind=cc.StepKind.CUSTOM,
            config=cc.CustomStepConfig(
                runner=transformations,
                context={
                    "tables": tables,
                    "cfg": cfg,
                    "persistent_token_df": persistent_token_df,
                    "factory_address": address,
                }
            )
        ),
        # cc.Step(
        #     kind=cc.StepKind.HEX_ENCODE,
        #     config=cc.HexEncodeConfig(),
        # ),
        cc.Step(
            kind=cc.StepKind.CUSTOM,
            config=cc.CustomStepConfig(
                runner=save_to_parquet
            )
        )
    ]

    # Running a Pipeline
    pipeline = cc.Pipeline(
        provider=cfg.provider,
        query=query,
        writer=writer,
        steps=steps,
    )
    await run_pipeline(pipeline_name="uniswap_v2_forks", pipeline=pipeline)

    return pipeline