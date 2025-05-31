from clickhouse_connect.driver.asyncclient import AsyncClient
from cherry_core import base58_decode_string
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


_TABLE_NAME = "swap_prices"


async def init_db(client: AsyncClient):
    await client.command(f"""
CREATE TABLE IF NOT EXISTS {_TABLE_NAME} (
    block_slot UInt64,
    mint String,
    price Decimal128(9),
    timestamp Int64,
    window_total_amount Decimal128(9),

    INDEX ts_idx timestamp TYPE minmax GRANULARITY 4
) ENGINE = MergeTree 
ORDER BY (mint, block_slot); 
""")


_WINDOW_RANGE = 20
_BATCH_RANGE = 200
_DECIMALS = 9
# Decimals of both USD coins are 6
_USD_DECIMALS = 6
_USD_PRICE = int(pow(10, _DECIMALS - _USD_DECIMALS))
_TOTAL_AMOUNT_THRESHOLD = _USD_PRICE * 1_000_000

_USD_COINS = [
    base58_decode_string("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),  # USDC
    base58_decode_string("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"),  # USDT
]
_WSOL = base58_decode_string("So11111111111111111111111111111111111111112")


async def query(client: AsyncClient, from_block: int, to_block: int) -> pl.DataFrame:
    res = await client.query_arrow(
        f"""
        SELECT input_amount, output_amount, input_mint, output_mint, block_slot, timestamp FROM svm.raydium_swaps
        WHERE
            block_slot >= {from_block} AND block_slot <= {to_block} AND
            input_amount != 0 AND
            output_amount != 0
    """,
        use_strings=False,
    )
    res = pl.from_arrow(res)
    assert isinstance(res, pl.DataFrame)
    return res


def select_swaps(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        "input_amount",
        "output_amount",
        "input_mint",
        "output_mint",
        "block_slot",
        "timestamp",
    )


async def run(cfg: SvmConfig):
    # For development
    # TODO: move this to env vars
    # read_client = await clickhouse_connect.get_async_client(
    #     host="v2202505202021338886.bestsrv.de",
    #     port=8123,
    #     username="ilove",
    #     password="cherry",
    #     database="svm",
    # )
    read_client = cfg.client

    min_block = await db.get_min_block(
        read_client, "svm.raydium_swaps", "block_slot", None
    )

    logger.info(f"min block from db: {min_block}")

    next_block = await db.get_next_block(cfg.client, _TABLE_NAME, "block_slot", None)

    end_block = await db.get_next_block(
        read_client, "svm.raydium_swaps", "block_slot", None
    )

    logger.info(f"end of source table: {end_block}")

    logger.info(f"next block from db: {next_block}")

    from_block = max(cfg.from_block, next_block)
    if min_block is not None:
        from_block = max(from_block, min_block)

    logger.info(f"starting to ingest from block {from_block}")

    insert = None

    if from_block >= end_block:
        return

    from_block = from_block + _WINDOW_RANGE
    to_block = min(end_block, from_block + _BATCH_RANGE)
    fetch_task = asyncio.create_task(
        query(read_client, max(0, from_block - _WINDOW_RANGE), to_block)
    )

    while from_block < end_block:
        to_block = min(end_block - 1, from_block + _BATCH_RANGE)

        start_time = time.time()
        data = await fetch_task
        logger.debug(f"Fetch took {(time.time() - start_time) * 1000: .2f}ms")

        # Start the next fetch so we get next batch of data while processing the current batch
        next_from_block = from_block + _BATCH_RANGE + 1
        next_to_block = min(end_block - 1, next_from_block + _BATCH_RANGE)
        fetch_task = asyncio.create_task(
            query(
                read_client,
                max(0, next_from_block - _WINDOW_RANGE),
                next_to_block,
            )
        )

        start_time = time.time()

        data = data.with_columns(
            pl.col("input_amount")
            .cast(pl.Decimal(precision=38, scale=9))
            .alias("input_amount"),
            pl.col("output_amount")
            .cast(pl.Decimal(precision=38, scale=9))
            .alias("output_amount"),
        )
        data = (
            select_swaps(data)
            .vstack(
                select_swaps(
                    data.rename(
                        {
                            "input_amount": "output_amount",
                            "output_amount": "input_amount",
                            "input_mint": "output_mint",
                            "output_mint": "input_mint",
                        }
                    )
                )
            )
            .filter(
                pl.col("input_mint")
                .is_in(_USD_COINS)
                .not_()
                .and_(
                    pl.col("output_mint")
                    .eq(_WSOL)
                    .or_(pl.col("output_mint").is_in(_USD_COINS))
                )
            )
        )

        sol_to_usd_swaps = data.filter(
            pl.col("input_mint").eq(_WSOL).and_(pl.col("output_mint").is_in(_USD_COINS))
        ).lazy()

        sol_prices = (
            sol_to_usd_swaps.select("block_slot", "timestamp")
            .join_where(
                sol_to_usd_swaps,
                pl.col("block_slot").ge(pl.lit(from_block).cast(pl.UInt64))
                & (pl.col("block_slot_right") >= pl.col("block_slot") - _WINDOW_RANGE)
                & (pl.col("block_slot_right") <= pl.col("block_slot")),
            )
            .group_by("block_slot", "timestamp")
            .agg(
                pl.col("input_amount").sum().alias("total_input"),
                pl.col("output_amount").sum().alias("total_output"),
            )
            .filter(pl.col("total_output").gt(_TOTAL_AMOUNT_THRESHOLD))
            .select(
                pl.col("total_output")
                .truediv(pl.col("total_input"))
                .cast(pl.Decimal(38, 9))
                .mul(pl.lit(_USD_PRICE).cast(pl.Decimal(38, 9)))
                .cast(pl.Decimal(38, 9))
                .alias("price"),
                pl.col("block_slot"),
                pl.lit(_WSOL).cast(pl.Binary).alias("mint"),
                pl.col("timestamp"),
                pl.col("total_output")
                .truediv(1000)
                .cast(pl.Decimal(38, 9))
                .alias("window_total_amount"),
            )
            .collect()
        )

        num_block_slots = to_block - from_block + 1
        usd_prices = pl.repeat(
            _USD_PRICE, num_block_slots, dtype=pl.Decimal(38, 9), eager=True
        )
        usd_block_slots = pl.int_range(
            start=from_block,
            end=to_block + 1,
            step=1,
            dtype=pl.UInt64,
            eager=True,
        )
        usd_prices = pl.concat(
            [
                pl.DataFrame(
                    {
                        "price": usd_prices,
                        "block_slot": usd_block_slots,
                        "mint": pl.Series(
                            pl.repeat(
                                addr, num_block_slots, dtype=pl.Binary, eager=True
                            ),
                        ),
                    }
                )
                for addr in _USD_COINS
            ]
        )
        prices = pl.concat(
            [sol_prices.select("price", "block_slot", "mint"), usd_prices]
        )

        token_swaps = (
            data.filter(pl.col("input_mint").ne(_WSOL))
            .join(
                prices,
                left_on=["output_mint", "block_slot"],
                right_on=["mint", "block_slot"],
            )
            .with_columns(
                [
                    pl.col("output_amount")
                    .mul("price")
                    .cast(pl.Decimal(38, 9))
                    .alias("output_price")
                ]
            )
            .lazy()
        )

        token_prices = (
            token_swaps.select("block_slot", "timestamp", "input_mint")
            .join_where(
                token_swaps,
                pl.col("block_slot").ge(pl.lit(from_block).cast(pl.UInt64))
                & pl.col("input_mint").eq(pl.col("input_mint_right"))
                & (pl.col("block_slot_right") >= pl.col("block_slot") - _WINDOW_RANGE)
                & (pl.col("block_slot_right") <= pl.col("block_slot")),
            )
            .group_by(["block_slot", "input_mint", "timestamp"])
            .agg(
                pl.col("input_amount").sum().alias("total_input"),
                pl.col("output_price").sum().alias("total_output"),
            )
            .select(
                pl.col("total_output")
                .truediv(pl.col("total_input"))
                .cast(pl.Decimal(38, 9))
                .alias("price"),
                pl.col("block_slot"),
                pl.col("input_mint").alias("mint"),
                pl.col("timestamp"),
                pl.col("total_output")
                .truediv(1000)
                .cast(pl.Decimal(38, 9))
                .alias("window_total_amount"),
            )
            .collect()
        )

        out_prices = pl.concat([sol_prices, token_prices])

        logger.debug(f"Processing took {(time.time() - start_time) * 1000: .2f}ms")

        if insert is not None:
            start_time = time.time()
            await insert
            logger.debug(f"Insert took {(time.time() - start_time) * 1000: .2f}ms")
            insert = None

        insert = asyncio.create_task(
            cfg.client.insert_arrow(_TABLE_NAME, out_prices.to_arrow())
        )

        from_block += _BATCH_RANGE + 1

    if insert is not None:
        await insert
