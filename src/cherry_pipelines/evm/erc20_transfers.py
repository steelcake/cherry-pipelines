from clickhouse_connect.driver.asyncclient import AsyncClient
import pyarrow as pa
from cherry_etl import config as cc
from cherry_core import ingest, evm_signature_to_topic0
import logging
from typing import Dict, Any, cast
import polars

from .. import db
from ..config import (
    EvmConfig,
    make_evm_table_name,
)

from .pipeline import EvmPipeline

logger = logging.getLogger(__name__)


class Pipeline(EvmPipeline):
    async def make_pipeline(self, cfg: EvmConfig) -> cc.Pipeline:
        return await make_pipeline(cfg)

    async def init_db(self, client: AsyncClient, chain_id: int):
        await init_db(client, chain_id)


_BASE_TABLE_NAME = "erc20_transfers"


async def init_db(client: AsyncClient, chain_id: int):
    table_name = make_evm_table_name(_BASE_TABLE_NAME, chain_id)
    await client.command(f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    block_number UInt64,
    transaction_index UInt64,
    log_index UInt64,
    transaction_hash String,
    address String,
    `from` String,
    `to` String,
    amount Decimal128(0),
    timestamp Int64,
    INDEX ts_idx timestamp TYPE minmax GRANULARITY 4,
    INDEX from_idx `from` TYPE bloom_filter(0.01) GRANULARITY 4, 
    INDEX to_idx `to` TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = MergeTree
ORDER BY block_number; 
""")


def join_data(
    data: Dict[str, polars.DataFrame], context: Any
) -> Dict[str, polars.DataFrame]:
    context = cast(Dict[str, bool], context)
    table_name = context["table_name"]

    blocks = data["blocks"]
    transfers = data[table_name]

    blocks = blocks.select(
        polars.col("number").alias("block_number"),
        polars.col("timestamp"),
    )
    out = transfers.join(blocks, on="block_number")
    out = out.drop(
        [
            "data",
            "topic0",
            "topic1",
            "topic2",
            "topic3",
        ]
    )

    out_d = {}
    out_d[table_name] = out
    return out_d


async def make_pipeline(cfg: EvmConfig) -> cc.Pipeline:
    table_name = make_evm_table_name(_BASE_TABLE_NAME, cfg.chain_id)
    next_block = await db.get_next_block(cfg.client, table_name, "block_number")
    from_block = max(cfg.from_block, next_block)
    logger.info(f"starting to ingest from block {from_block}")

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            logs=[
                ingest.evm.LogRequest(
                    # address=[
                    #     "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
                    #     "0xB8c77482e45F1F44dE1745F52C74426C631bDD52",  # BNB
                    #     "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
                    #     "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",  # stETH
                    #     "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",  # Wrapped BTC
                    #     "0x582d872A1B094FC48F5DE31D3B73F2D9bE47def1",  # Wrapped TON coin
                    # ],
                    topic0=[
                        evm_signature_to_topic0("Transfer(address,address,uint256)")
                    ],
                    include_blocks=True,
                )
            ],
            # select the fields we want
            fields=ingest.evm.Fields(
                block=ingest.evm.BlockFields(number=True, timestamp=True),
                log=ingest.evm.LogFields(
                    block_number=True,
                    transaction_index=True,
                    transaction_hash=True,
                    log_index=True,
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

    writer = cc.Writer(
        kind=cc.WriterKind.CLICKHOUSE,
        config=cc.ClickHouseWriterConfig(
            client=cfg.client,
            create_tables=False,
        ),
    )

    pipeline = cc.Pipeline(
        provider=cfg.provider,
        writer=writer,
        query=query,
        steps=[
            cc.Step(
                kind=cc.StepKind.EVM_DECODE_EVENTS,
                config=cc.EvmDecodeEventsConfig(
                    event_signature="Transfer(address indexed from, address indexed to, uint256 amount)",
                    output_table=table_name,
                    # Write null if decoding fails instead of erroring out.
                    #
                    # This is needed if we are trying to decode all logs that match our topic0 without
                    # filtering for contract address, because other events like NFT transfers also match our topic0
                    allow_decode_fail=True,
                ),
            ),
            # Cast all Decimal256 columns to Decimal128, we have to do this because polars doesn't support decimal256
            cc.Step(
                kind=cc.StepKind.CAST_BY_TYPE,
                config=cc.CastByTypeConfig(
                    from_type=pa.decimal256(76, 0),
                    to_type=pa.decimal128(38, 0),
                    # Write null if the value doesn't fit in decimal128,
                    allow_cast_fail=True,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=join_data,
                    context={"table_name": table_name},
                ),
            ),
            cc.Step(
                kind=cc.StepKind.CAST,
                config=cc.CastConfig(
                    table_name=table_name,
                    mappings={"timestamp": pa.int64()},
                ),
            ),
        ],
    )

    return pipeline
