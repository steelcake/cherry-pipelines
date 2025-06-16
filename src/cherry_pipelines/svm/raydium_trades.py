from clickhouse_connect.driver.asyncclient import AsyncClient
import logging
import polars as pl
import asyncio
import time

from .. import db
from ..config import (
    SvmConfig,
)

from .pipeline import SvmPipeline

logger = logging.getLogger(__name__)


class Pipeline(SvmPipeline):
    async def run(self, cfg: SvmConfig):
        await run(cfg)

    async def init_db(self, client: AsyncClient):
        await init_db(client)


_TABLE_NAME = "raydium_trades"


async def init_db(client: AsyncClient):
    await client.command(f"""
       CREATE TABLE IF NOT EXISTS {_TABLE_NAME} (
            block_slot UInt64,
            block_hash String,
            transaction_index UInt64,
            transaction_signature String,
            instruction_address Array(UInt32),
            program_id String,
            payer String,
            pool String,
            input_token_account String,
            output_token_account String,

            input_mint String,
            input_vault String,
            input_amount UInt64,
            input_price_usd Decimal128(0),

            output_mint String,
            output_vault String,
            output_amount UInt64,
            output_price_usd Decimal128(0),

            swap_kind Enum(
                'amm_base_in' = 1,
                'amm_base_out' = 2,
                'clmm_v1' = 3,
                'clmm_v2' = 4,
                'cp_swap_base_input' = 5,
                'cp_swap_base_output' = 6
            ),

            max_amount_in UInt64,
            amount_out UInt64,

            amount_in UInt64,
            minimum_amount_out UInt64,

            amount UInt64,
            other_amount_threshold UInt64,
            sqrt_price_limit_x64 Decimal128(0),
            is_base_input Boolean,

            timestamp Int64,
            block_height UInt64,
            found_input Boolean,
            found_output Boolean,

            INDEX ts_idx timestamp TYPE minmax GRANULARITY 4,
            INDEX height_idx block_height TYPE minmax GRANULARITY 4,
            INDEX input_token_account_idx `input_token_account` TYPE bloom_filter(0.01) GRANULARITY 4, 
            INDEX output_token_account_idx `output_token_account` TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX pool_idx `pool` TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX input_mint_idx `input_mint` TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX output_mint_idx `output_mint` TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX input_vault_idx `input_vault` TYPE bloom_filter(0.01) GRANULARITY 4,
            INDEX output_vault_idx `output_vault` TYPE bloom_filter(0.01) GRANULARITY 4
        ) ENGINE = MergeTree 
        ORDER BY (block_slot, transaction_index, instruction_address); 
""")


_BATCH_RANGE = 1000
_LOOKBACK_RANGE = 1000


async def query(
    client: AsyncClient, from_block: int, to_block: int
) -> tuple[pl.DataFrame, pl.DataFrame]:
    swaps = await client.query_arrow(
        f"""
        SELECT *
        FROM svm.raydium_swaps
        WHERE
            block_slot >= {from_block} AND block_slot <= {to_block} AND
            input_amount != 0 AND
            output_amount != 0
    """,
        use_strings=False,
    )
    prices = await client.query_arrow(
        f"""
        SELECT 
            block_slot,
            mint,
            price
        FROM svm.swap_prices
        WHERE
            block_slot >= {from_block - _LOOKBACK_RANGE} AND block_slot <= {to_block}
    """,
        use_strings=False,
    )
    swaps = pl.from_arrow(swaps)
    prices = pl.from_arrow(prices)
    assert isinstance(swaps, pl.DataFrame)
    assert isinstance(prices, pl.DataFrame)
    return swaps, prices


async def run(cfg: SvmConfig):
    # For development
    # TODO: move this to env vars
    # import clickhouse_connect
    # read_client = await clickhouse_connect.get_async_client(
    #     host="v2202505202021338886.bestsrv.de",
    #     port=8123,
    #     username="ilove",
    #     password="cherry",
    #     database="svm",
    # )
    read_client = cfg.client

    swaps_min_block = await db.get_min_block(
        read_client, "svm.raydium_swaps", "block_slot", None
    )
    prices_min_block = await db.get_min_block(
        read_client, "svm.swap_prices", "block_slot", None
    )
    if swaps_min_block is None or prices_min_block is None:
        raise ValueError("Both swaps_min_block and prices_min_block must be non-None")
    source_tables_min_block = max(swaps_min_block, prices_min_block)
    logger.info(f"min_block of source tables: {source_tables_min_block}")

    swaps_max_block = await db.get_next_block(
        read_client, "svm.raydium_swaps", "block_slot", None
    )
    prices_max_block = await db.get_next_block(
        read_client, "svm.swap_prices", "block_slot", None
    )
    source_tables_max_block = min(swaps_max_block, prices_max_block)
    logger.info(f"end of source table: {source_tables_max_block}")

    trades_max_block = await db.get_next_block(
        read_client, "svm.raydium_trades", "block_slot", None
    )
    logger.info(f"trades_max_block: {trades_max_block}")

    local_next_block = await db.get_next_block(
        cfg.client, _TABLE_NAME, "block_slot", None
    )
    logger.info(f"local_next_block: {local_next_block}")

    from_block = max(cfg.from_block, local_next_block)
    if source_tables_min_block is not None:
        from_block = max(from_block, source_tables_min_block)

    if cfg.to_block is not None:
        end_block = min(source_tables_max_block, cfg.to_block)
    else:
        end_block = source_tables_max_block

    logger.info(f"starting to ingest from block {from_block} until {end_block}")

    insert = None

    if from_block >= end_block:
        return

    to_block = min(end_block, from_block + _BATCH_RANGE)
    logger.debug(f"fetching from block {from_block} to {to_block}")
    fetch_task = asyncio.create_task(query(read_client, max(0, from_block), to_block))

    while from_block < end_block:
        to_block = min(end_block - 1, from_block + _BATCH_RANGE)

        start_time = time.time()
        swaps, prices = await fetch_task
        logger.debug(f"Fetch took {(time.time() - start_time) * 1000: .2f}ms")

        # Start the next fetch so we get next batch of data while processing the current batch
        next_from_block = from_block + _BATCH_RANGE + 1
        next_to_block = min(end_block - 1, next_from_block + _BATCH_RANGE)
        logger.debug(f"fetching from block {next_from_block} to {next_to_block}")
        fetch_task = asyncio.create_task(
            query(
                read_client,
                max(0, next_from_block),
                next_to_block,
            )
        )

        start_time = time.time()
        swaps = swaps.sort(["block_slot", "input_mint"]).lazy()
        prices = prices.sort(["block_slot", "mint"]).lazy()
        trades = (
            swaps.join_asof(
                prices,
                by_left="input_mint",
                by_right="mint",
                on="block_slot",
                tolerance=50,
                strategy="forward",
            )
            .rename({"price": "input_price_usd"})
            .join_asof(
                prices,
                by_left="output_mint",
                by_right="mint",
                on="block_slot",
                tolerance=50,
                strategy="forward",
            )
            .rename({"price": "output_price_usd"})
            .with_columns(
                [
                    (pl.col("input_amount") * pl.col("input_price_usd"))
                    .cast(pl.Decimal(38, 9))
                    .alias("input_volume_usd"),
                    (pl.col("output_amount") * pl.col("output_price_usd"))
                    .cast(pl.Decimal(38, 9))
                    .alias("output_volume_usd"),
                ]
            )
            .with_columns(
                pl.when(pl.col("input_volume_usd").is_null())
                .then(pl.col("output_volume_usd"))
                .when(pl.col("output_volume_usd").is_null())
                .then(pl.col("input_volume_usd"))
                .otherwise(
                    (pl.col("input_volume_usd") + pl.col("output_volume_usd")) / 2
                )
                .alias("volume_usd")
            )
            .collect()
        )

        logger.debug(f"Processing took {(time.time() - start_time) * 1000: .2f}ms")

        if insert is not None:
            start_time = time.time()
            await insert
            logger.debug(f"Insert took {(time.time() - start_time) * 1000: .2f}ms")
            insert = None

        insert = asyncio.create_task(
            cfg.client.insert_arrow(_TABLE_NAME, trades.to_arrow())
        )

        from_block += _BATCH_RANGE + 1

    if insert is not None:
        await insert
