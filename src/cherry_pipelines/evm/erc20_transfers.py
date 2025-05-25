from clickhouse_connect.driver.asyncclient import AsyncClient
import pyarrow as pa
from cherry_etl import config as cc, run_pipeline
from cherry_core import ingest, evm_signature_to_topic0
import logging
from typing import Dict, Any
import datafusion as df

from .. import db
from ..config import (
    EvmConfig,
)

from .pipeline import EvmPipeline

logger = logging.getLogger(__name__)


class Pipeline(EvmPipeline):
    async def run(self, cfg: EvmConfig):
        await run(cfg)

    async def init_db(self, client: AsyncClient):
        await init_db(client)


_TABLE_NAME = "erc20_transfers"


async def init_db(client: AsyncClient):
    await client.command(f"""
CREATE TABLE IF NOT EXISTS {_TABLE_NAME} (
    block_number UInt64,
    block_hash String,
    transaction_index UInt64,
    log_index UInt64,
    transaction_hash String,
    address String,
    `from` String,
    `to` String,
    amount Decimal256(0),
    timestamp Int64,
    chain_id UInt64,
    INDEX ts_idx timestamp TYPE minmax GRANULARITY 4,
    INDEX from_idx `from` TYPE bloom_filter(0.01) GRANULARITY 4, 
    INDEX to_idx `to` TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = MergeTree 
PARTITION BY chain_id
ORDER BY block_number; 
""")


def join_data(
    session_ctx: df.SessionContext, data: Dict[str, df.DataFrame], _: Any
) -> Dict[str, df.DataFrame]:
    _ = data

    out = session_ctx.sql("""
        SELECT
            transfers.block_number,
            transfers.block_hash,
            transfers.transaction_index,
            transfers.log_index,
            transfers.transaction_hash,
            transfers.address,
            transfers.`from`,
            transfers.`to`,
            transfers.amount,
            blocks.timestamp
        FROM erc20_transfers transfers
        LEFT JOIN blocks ON blocks.number = transfers.block_number
    """)

    out_d = {}
    out_d[_TABLE_NAME] = out
    return out_d


async def run(cfg: EvmConfig):
    next_block = await db.get_next_block(
        cfg.client, _TABLE_NAME, "block_number", cfg.chain_id
    )
    from_block = max(cfg.from_block, next_block)
    logger.info(f"starting to ingest from block {from_block}")

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            to_block=cfg.to_block,
            logs=[
                ingest.evm.LogRequest(
                    topic0=[
                        evm_signature_to_topic0("Transfer(address,address,uint256)")
                    ],
                    include_blocks=True,
                )
            ],
            fields=ingest.evm.Fields(
                block=ingest.evm.BlockFields(number=True, timestamp=True),
                log=ingest.evm.LogFields(
                    block_number=True,
                    block_hash=True,
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
                    output_table=_TABLE_NAME,
                    # Write null if decoding fails instead of erroring out.
                    #
                    # This is needed if we are trying to decode all logs that match our topic0 without
                    # filtering for contract address, because other events like NFT transfers also match our topic0
                    allow_decode_fail=True,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.DATAFUSION,
                config=cc.DataFusionStepConfig(
                    runner=join_data,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.CAST,
                config=cc.CastConfig(
                    table_name=_TABLE_NAME,
                    mappings={"timestamp": pa.int64()},
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SET_CHAIN_ID,
                config=cc.SetChainIdConfig(chain_id=cfg.chain_id),
            ),
        ],
    )

    await run_pipeline(pipeline=pipeline, pipeline_name=_TABLE_NAME)
