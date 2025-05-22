import polars as pl
import logging
from typing import List
from datetime import datetime, timezone
from cherry_pipelines.config import EvmConfig
from cherry_core import get_token_metadata_as_table

logger = logging.getLogger(__name__)


def get_protocol_df(
    address: str,
    name: str,
    slug: str,
    network: str,
    schema_version: str,
    pipeline_version: str,
) -> pl.DataFrame:
    protocol_df = pl.DataFrame(
        {
            "id": [address],
            "name": [name],
            "slug": [slug],
            "schema_version": [schema_version],
            "pipeline_version": [pipeline_version],
            "network": [network],
            "type": ["stablecoin"],
        }
    )
    return protocol_df


def get_liquidity_pool_df(
    pair_created_logs_df: pl.DataFrame,
    factory_address: str,
) -> pl.DataFrame:
    current_time = int(datetime.now(timezone.utc).timestamp())
    liquidity_pool_df = (
        pair_created_logs_df.select(
            pl.concat_str(
                pl.lit("0x"), pl.col("pair").bin.encode("hex").str.to_lowercase()
            ).alias("id"),
            pl.col("pair").alias("address"),
            pl.lit(factory_address).alias("protocol"),
            pl.concat_list([pl.col("token0"), pl.col("token1")]).alias("input_tokens"),
            pl.col("pair").alias("output_token"),
            pl.col("transaction_hash").alias("created_tx_hash"),
            pl.col("timestamp").alias("created_timestamp"),
            pl.col("block_number").alias("created_block_number"),
            pl.lit(current_time).alias("exe_timestamp_utc"),
        )
    )
    return liquidity_pool_df


def get_swap_df(
    swap_logs_df: pl.DataFrame,
    pair_created_logs_df: pl.DataFrame,
    sync_logs_df: pl.DataFrame,
    factory_address: str,
) -> pl.DataFrame:
    current_time = int(datetime.now(timezone.utc).timestamp())
    token0_swap_df = (
        swap_logs_df.filter(
            pl.col("amount0In") - pl.col("amount0Out") > 0
        )  # token0 is the input token
        .join(
            pair_created_logs_df, left_on="address", right_on="pair", how="inner"
        )  # Inner join will exclude pairs from forks
        .join(
            sync_logs_df,
            left_on=[
                pl.col("transaction_hash"),
                pl.col("address"),
                pl.col("log_index"),
            ],
            right_on=[
                pl.col("transaction_hash"),
                pl.col("address"),
                (pl.col("log_index") + 1),
            ],
            how="left",
            suffix="_sync",
        )  # Join with the next log to get the reserve amounts
        .select(
            [
                pl.concat_str(
                    pl.lit("swap-0x"),
                    pl.col("transaction_hash").bin.encode("hex").str.to_lowercase(),
                    pl.lit("-"),
                    pl.col("log_index").cast(pl.Utf8),
                ).alias("id"),
                pl.lit(factory_address).alias("protocol"),
                pl.col("timestamp").alias("block_timestamp"),
                pl.concat_str(
                    pl.lit("0x"), pl.col("address").bin.encode("hex").str.to_lowercase()
                ).alias("liquidity_pool"),
                pl.col("token0").alias("token_sold"),
                pl.col("amount0In").alias("amount_sold_raw"),
                pl.col("token1").alias("token_bought"),
                pl.col("amount1Out").alias("amount_bought_raw"),
                pl.concat_list([pl.col("reserve0"), pl.col("reserve1")]).alias(
                    "reserve_amounts"
                ),
                pl.col("sender").alias("from"),
                pl.col("to").alias("to"),
                pl.col("from").alias("tx_from"),
                pl.col("to_right").alias("tx_to"),
                pl.col("transaction_hash").alias("tx_hash"),
                pl.col("block_number").alias("block_number"),
                pl.col("transaction_index").alias("tx_index"),
                pl.col("log_index").alias("log_index"),
                pl.lit(current_time).alias("exe_timestamp_utc"),
            ]
        )
    )
    token1_swap_df = (
        swap_logs_df.filter(
            pl.col("amount0In") - pl.col("amount0Out") < 0
        )  # token1 is the input token
        .join(
            pair_created_logs_df, left_on="address", right_on="pair", how="inner"
        )  # Inner join will exclude pairs from forks
        .join(
            sync_logs_df,
            left_on=[
                pl.col("transaction_hash"),
                pl.col("address"),
                pl.col("log_index"),
            ],
            right_on=[
                pl.col("transaction_hash"),
                pl.col("address"),
                (pl.col("log_index") + 1),
            ],
            how="left",
            suffix="_sync",
        )  # Join with the next log to get the reserve amounts
        .select(
            [
                pl.concat_str(
                    pl.lit("swap-0x"),
                    pl.col("transaction_hash").bin.encode("hex").str.to_lowercase(),
                    pl.lit("-"),
                    pl.col("log_index").cast(pl.Utf8),
                ).alias("id"),
                pl.lit(factory_address).alias("protocol"),
                pl.col("timestamp").alias("block_timestamp"),
                pl.concat_str(
                    pl.lit("0x"), pl.col("address").bin.encode("hex").str.to_lowercase()
                ).alias("liquidity_pool"),
                pl.col("token1").alias("token_sold"),
                pl.col("amount1In").alias("amount_sold_raw"),
                pl.col("token0").alias("token_bought"),
                pl.col("amount0Out").alias("amount_bought_raw"),
                pl.concat_list([pl.col("reserve0"), pl.col("reserve1")]).alias(
                    "reserve_amounts"
                ),
                pl.col("sender").alias("from"),
                pl.col("to").alias("to"),
                pl.col("from").alias("tx_from"),
                pl.col("to_right").alias("tx_to"),
                pl.col("transaction_hash").alias("tx_hash"),
                pl.col("block_number").alias("block_number"),
                pl.col("transaction_index").alias("tx_index"),
                pl.col("log_index").alias("log_index"),
                pl.lit(current_time).alias("exe_timestamp_utc"),
            ]
        )
    )
    swap_df = token0_swap_df.vstack(token1_swap_df)
    return swap_df


def get_deposit_df(
    mint_logs_df: pl.DataFrame,
    pair_created_logs_df: pl.DataFrame,
    sync_logs_df: pl.DataFrame,
    factory_address: str,
) -> pl.DataFrame:
    current_time = int(datetime.now(timezone.utc).timestamp())
    deposit_df = (
        mint_logs_df.join(
            pair_created_logs_df, left_on="address", right_on="pair", how="inner"
        )
        .join(
            sync_logs_df,
            left_on=[
                pl.col("transaction_hash"),
                pl.col("address"),
                pl.col("log_index"),
            ],
            right_on=[
                pl.col("transaction_hash"),
                pl.col("address"),
                (pl.col("log_index") + 1),
            ],
            how="left",
            suffix="_sync",
        )  # Join with the next log to get the reserve amounts
        .select(
            [
                pl.concat_str(
                    pl.lit("deposit-0x"),
                    pl.col("transaction_hash").bin.encode("hex").str.to_lowercase(),
                    pl.lit("-"),
                    pl.col("log_index").cast(pl.Utf8),
                ).alias("id"),
                pl.lit(factory_address).alias("protocol"),
                pl.col("timestamp").alias("block_timestamp"),
                pl.concat_str(
                    pl.lit("0x"), pl.col("address").bin.encode("hex").str.to_lowercase()
                ).alias("liquidity_pool"),
                pl.concat_list([pl.col("token0"), pl.col("token1")]).alias(
                    "input_tokens"
                ),
                pl.col("address").alias("output_token"),
                pl.concat_list([pl.col("amount0"), pl.col("amount1")]).alias(
                    "input_token_amounts"
                ),
                pl.concat_list([pl.col("reserve0"), pl.col("reserve1")]).alias(
                    "reserve_amounts"
                ),
                pl.col("sender").alias("from"),
                pl.lit(None).alias("to"),
                pl.col("from").alias("tx_from"),
                pl.col("to").alias("tx_to"),
                pl.col("transaction_hash").alias("tx_hash"),
                pl.col("block_number").alias("block_number"),
                pl.col("transaction_index").alias("tx_index"),
                pl.col("log_index").alias("log_index"),
                pl.lit(current_time).alias("exe_timestamp_utc"),
            ]
        )
    )
    return deposit_df


def get_withdraw_df(
    burn_logs_df: pl.DataFrame,
    pair_created_logs_df: pl.DataFrame,
    sync_logs_df: pl.DataFrame,
    factory_address: str,
) -> pl.DataFrame:
    current_time = int(datetime.now(timezone.utc).timestamp())
    withdraw_df = (
        burn_logs_df.join(
            pair_created_logs_df, left_on="address", right_on="pair", how="inner"
        )
        .join(
            sync_logs_df,
            left_on=[
                pl.col("transaction_hash"),
                pl.col("address"),
                pl.col("log_index"),
            ],
            right_on=[
                pl.col("transaction_hash"),
                pl.col("address"),
                (pl.col("log_index") + 1),
            ],
            how="left",
            suffix="_sync",
        )  # Join with the next log to get the reserve amounts
        .select(
            [
                pl.concat_str(
                    pl.lit("withdraw-0x"),
                    pl.col("transaction_hash").bin.encode("hex").str.to_lowercase(),
                    pl.lit("-"),
                    pl.col("log_index").cast(pl.Utf8),
                ).alias("id"),
                pl.lit(factory_address).alias("protocol"),
                pl.col("timestamp").alias("block_timestamp"),
                pl.concat_str(
                    pl.lit("0x"), pl.col("address").bin.encode("hex").str.to_lowercase()
                ).alias("liquidity_pool"),
                pl.concat_list([pl.col("token0"), pl.col("token1")]).alias(
                    "input_tokens"
                ),
                pl.col("address").alias("output_token"),
                pl.concat_list([pl.col("amount0"), pl.col("amount1")]).alias(
                    "input_token_amounts"
                ),
                pl.concat_list([pl.col("reserve0"), pl.col("reserve1")]).alias(
                    "reserve_amounts"
                ),
                pl.col("sender").alias("from"),
                pl.col("to").alias("to"),
                pl.col("from").alias("tx_from"),
                pl.col("to_right").alias("tx_to"),
                pl.col("transaction_hash").alias("tx_hash"),
                pl.col("block_number").alias("block_number"),
                pl.col("transaction_index").alias("tx_index"),
                pl.col("log_index").alias("log_index"),
                pl.lit(current_time).alias("exe_timestamp_utc"),
            ]
        )
    )
    return withdraw_df


def get_interaction_df(
    swap_df: pl.DataFrame,
    deposit_df: pl.DataFrame,
    withdraw_df: pl.DataFrame,
) -> pl.DataFrame:
    interaction_selection = [
        pl.col("id"),
        pl.col("protocol"),
        pl.col("block_timestamp"),
        pl.col("liquidity_pool"),
        pl.col("from"),
        pl.col("to"),
        pl.col("tx_from"),
        pl.col("tx_to"),
        pl.col("tx_hash"),
        pl.col("block_number"),
        pl.col("tx_index"),
        pl.col("log_index"),
        pl.col("exe_timestamp_utc"),
    ]

    swap_interaction_df = swap_df.select(interaction_selection)
    deposit_interaction_df = deposit_df.select(interaction_selection)
    withdraw_interaction_df = withdraw_df.select(interaction_selection)
    return pl.concat(
        [swap_interaction_df, deposit_interaction_df, withdraw_interaction_df],
        how="vertical",
    )
