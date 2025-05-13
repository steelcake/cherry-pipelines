import pyarrow as pa
from cherry_etl import config as cc
from cherry_etl import run_pipeline
from cherry_core import ingest, evm_signature_to_topic0
import logging
import asyncio
from typing import Dict, Any
import polars

from ... import db
from ...setup_env import setup_env
from ...config import load_evm_config, make_evm_provider

setup_env()
logger = logging.getLogger(__name__)

_TABLE_NAME = "transfers"


# Custom processing step
def join_data(data: Dict[str, polars.DataFrame], _: Any) -> Dict[str, polars.DataFrame]:
    blocks = data["blocks"]
    transfers = data["transfers"]

    bn = blocks.get_column("number")
    logger.info(f"processing data from: {bn.min()} to: {bn.max()}")

    blocks = blocks.select(
        polars.col("number").alias("block_number"),
        polars.col("timestamp"),
    )
    out = transfers.join(blocks, on="block_number")

    return {"transfers": out}


async def main():
    cfg = load_evm_config()
    client = await db.connect_evm()
    from_block = await db.get_start_block(client, _TABLE_NAME, "block_number")
    from_block = max(cfg.from_block, from_block)
    logger.info(f"starting to ingest from block {from_block}")
    provider = make_evm_provider(cfg)

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=from_block,
            # Select the logs we are interested in
            logs=[
                ingest.evm.LogRequest(
                    # Don't pass address filter to get all erc20 transfers
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
                    # include the blocks related to our logs
                    include_blocks=True,
                )
            ],
            # select the fields we want
            fields=ingest.evm.Fields(
                block=ingest.evm.BlockFields(number=True, timestamp=True),
                log=ingest.evm.LogFields(
                    block_number=True,
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
            client=client,
        ),
    )

    pipeline = cc.Pipeline(
        provider=provider,
        writer=writer,
        query=query,
        steps=[
            cc.Step(
                kind=cc.StepKind.EVM_DECODE_EVENTS,
                config=cc.EvmDecodeEventsConfig(
                    event_signature="Transfer(address indexed from, address indexed to, uint256 amount)",
                    output_table="transfers",
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
                ),
            ),
            cc.Step(
                kind=cc.StepKind.CAST,
                config=cc.CastConfig(
                    table_name="transfers",
                    mappings={"timestamp": pa.int64()},
                ),
            ),
        ],
    )

    await run_pipeline(pipeline=pipeline)


if __name__ == "__main__":
    asyncio.run(main())
