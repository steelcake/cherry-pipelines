from clickhouse_connect.driver.asyncclient import AsyncClient
from cherry_core import base58_decode_string
import logging
from typing import cast, Optional
import polars as pl
import asyncio
import clickhouse_connect

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
    price Decimal128(9)
) ENGINE = MergeTree 
ORDER BY (mint, block_slot); 
""")


_WINDOW_RANGE = 50
_BATCH_RANGE = 1000
_NUM_PRICE_ROUNDS = 5
_TOTAL_USD_THRESHOLD = 2000

_USD_COINS = [
    base58_decode_string("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"),  # USDC
    base58_decode_string("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"),  # USDT
]
# _SOL = base58_decode_string("So11111111111111111111111111111111111111112")


async def query(client: AsyncClient, from_block: int, to_block: int) -> pl.DataFrame:
    res = await client.query_arrow(
        f"""
        SELECT input_amount, output_amount, input_mint, output_mint, block_slot FROM svm.raydium_swaps
        WHERE
            block_slot >= {from_block} AND block_slot <= {to_block}
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
    )


async def run(cfg: SvmConfig):
    read_client = await clickhouse_connect.get_async_client(
        host="v2202505202021338886.bestsrv.de",
        port=8123,
        username="ilove",
        password="cherry",
        database="svm",
    )

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

    end_block = max(0, end_block - _WINDOW_RANGE)

    if from_block >= end_block:
        return

    to_block = min(end_block, from_block + _BATCH_RANGE)
    fetch_task = asyncio.create_task(
        query(read_client, max(0, from_block - _WINDOW_RANGE), to_block + _WINDOW_RANGE)
    )

    while from_block < end_block:
        to_block = min(end_block, from_block + _BATCH_RANGE)

        data = await fetch_task

        # Start the next fetch so we get next batch of data while processing the current batch
        next_from_block = from_block + _BATCH_RANGE
        next_to_block = min(end_block, next_from_block + _BATCH_RANGE)
        fetch_task = asyncio.create_task(
            query(
                read_client,
                max(0, next_from_block - _WINDOW_RANGE),
                next_to_block + _WINDOW_RANGE,
            )
        )

        data = data.with_columns(
            pl.col("input_amount")
            .cast(pl.Decimal(precision=38, scale=9))
            .alias("input_amount"),
            pl.col("output_amount")
            .cast(pl.Decimal(precision=38, scale=9))
            .alias("output_amount"),
        )

        prices_list = []
        for block_slot in range(from_block, to_block + 1):
            window = data.filter(
                pl.col("block_slot").is_between(
                    block_slot - _WINDOW_RANGE, block_slot + _WINDOW_RANGE
                )
            )
            window = (
                select_swaps(window)
                .vstack(
                    select_swaps(
                        window.rename(
                            {
                                "input_amount": "output_amount",
                                "output_amount": "input_amount",
                                "input_mint": "output_mint",
                                "output_mint": "input_mint",
                            }
                        )
                    )
                )
                .filter(pl.col("input_mint").is_in(_USD_COINS).not_())
            )

            prices = pl.concat(
                [
                    pl.DataFrame(
                        {
                            # 1000 because USD tokens have 6 decimals but we want to use 9 decimals
                            "price": pl.Series(
                                "price",
                                values=[1000],
                                dtype=pl.Decimal(precision=38, scale=9),
                            ),  # pl.repeat(1000, 1, dtype=pl.Decimal(precision=38, scale=9)),
                            "mint": pl.Series(
                                "mint", values=[addr], dtype=pl.Binary
                            ),  # pl.repeat(addr, 1, dtype=pl.Binary),
                        }
                    )
                    for addr in _USD_COINS
                ]
            )

            # window, prices = calculate_sol_prices(window, prices)

            for _ in range(0, _NUM_PRICE_ROUNDS):
                if window.height == 0:
                    break

                window, prices = calculate_prices(window, prices)

            window, prices = calculate_prices(window, prices, top=None)

            prices_list.append(
                prices.with_columns(
                    [
                        pl.lit(block_slot).alias("block_slot"),
                    ]
                )
            )

        if insert is not None:
            await insert
            insert = None
        insert = asyncio.create_task(
            cfg.client.insert_arrow(
                _TABLE_NAME, cast(pl.DataFrame, pl.concat(prices_list)).to_arrow()
            )
        )

        from_block += _BATCH_RANGE

    if insert is not None:
        await insert


def calculate_prices(
    data: pl.DataFrame, prices: pl.DataFrame, top: Optional[int] = 5
) -> tuple[pl.DataFrame, pl.DataFrame]:
    new_prices = (
        data.join(prices, left_on=["output_mint"], right_on=["mint"])
        .group_by(pl.col("input_mint"))
        .agg(
            pl.col("input_amount").sum().alias("total_input"),
            pl.col("output_amount")
            .mul(pl.col("price"))
            .cast(pl.Decimal(38, 9))
            .sum()
            .alias("total_output"),
        )
    )

    new_prices = new_prices.with_columns(
        pl.col("total_output")
        .truediv(pl.col("total_input"))
        .cast(pl.Decimal(38, 9))
        .alias("out_price")
    )

    new_prices = new_prices.filter(
        pl.col("total_output").gt(
            pl.lit(_TOTAL_USD_THRESHOLD).cast(pl.Decimal(precision=38, scale=9))
        )
    )

    if top is not None:
        new_prices = new_prices.top_k(top, by=pl.col("total_output"))

    data = data.filter(
        pl.col("input_mint").is_in(new_prices.get_column("input_mint")).not_()
    )

    return data, pl.concat(
        [
            prices,
            new_prices.select(
                pl.col("out_price").alias("price"), pl.col("input_mint").alias("mint")
            ),
        ]
    )
