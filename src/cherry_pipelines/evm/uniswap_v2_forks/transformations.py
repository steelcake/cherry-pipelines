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


def get_token_df(cfg: EvmConfig, token_address: List[str]) -> pl.DataFrame:
    chunk_size = 30
    chunks_dfs = []
    empty_df = pl.DataFrame(
        schema={
            "address": pl.Binary(),
            "decimals": pl.UInt8(),
            "symbol": pl.String(),
            "name": pl.String(),
        }
    )
    for i in range(0, len(token_address), chunk_size):
        addresses_chunk = token_address[i : i + chunk_size]

        try:
            token_metadata = get_token_metadata_as_table(
                cfg.rpc_provider_url,
                addresses_chunk,
            )
            chunk_df = pl.from_arrow(token_metadata)
        except Exception as e:
            logger.error(
                f"Error getting token metadata: {e}. Tokens affected: {addresses_chunk}"
            )
            chunk_df = empty_df
        chunks_dfs.append(chunk_df)

    if len(chunks_dfs) > 1:
        token_df = pl.concat(
            chunks_dfs,
            how="vertical",
        )
    elif len(chunks_dfs) == 1:
        token_df = chunks_dfs[0]
    else:
        token_df = empty_df

    assert isinstance(token_df, pl.DataFrame), "token_df must be a DataFrame"

    current_time = int(datetime.now(timezone.utc).timestamp())
    token_df = token_df.select(
        pl.concat_str(
            pl.lit("0x"), pl.col("address").bin.encode("hex").str.to_lowercase()
        ).alias("id"),
        pl.col("address").alias("address"),
        pl.col("name").alias("name"),
        pl.col("symbol").alias("symbol"),
        pl.col("decimals").alias("decimals"),
        pl.lit(current_time).alias("exe_timestamp_utc"),
    )

    return token_df


def get_new_tokens(
    pair_created_logs_df: pl.DataFrame, token_df: pl.DataFrame
) -> List[str]:
    token0 = pair_created_logs_df.select(pl.col("token0").alias("address"))
    token1 = pair_created_logs_df.select(pl.col("token1").alias("address"))
    new_tokens = token0.vstack(token1).unique().join(token_df, on="address", how="anti")
    new_tokens_list = (
        new_tokens.select(
            pl.concat_str(
                pl.lit("0x"), pl.col("address").bin.encode("hex").str.to_lowercase()
            ).alias("id")
        )
        .to_series()
        .to_list()
    )
    return new_tokens_list


def get_liquidity_pool_df(
    pair_created_logs_df: pl.DataFrame,
    token_df: pl.DataFrame,
    factory_address: str,
    protocol_name: str,
) -> pl.DataFrame:
    current_time = int(datetime.now(timezone.utc).timestamp())
    liquidity_pool_df = (
        pair_created_logs_df.join(
            token_df, left_on="token0", right_on="address", how="left"
        )
        .join(
            token_df, left_on="token1", right_on="address", how="left", suffix="_token1"
        )
        .select(
            pl.concat_str(
                pl.lit("0x"), pl.col("pair").bin.encode("hex").str.to_lowercase()
            ).alias("id"),
            pl.col("pair").alias("address"),
            pl.lit(factory_address).alias("protocol"),
            pl.concat_str(
                pl.lit(protocol_name),
                pl.lit("-"),
                pl.col("symbol"),
                pl.lit("/"),
                pl.col("symbol_token1"),
            ).alias("name"),
            pl.concat_str(pl.col("symbol"), pl.lit("/"), pl.col("symbol_token1")).alias(
                "symbol"
            ),
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
    token_df: pl.DataFrame,
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
            token_df, left_on="token0", right_on="address", how="left", suffix="_token0"
        )
        .join(
            token_df, left_on="token1", right_on="address", how="left", suffix="_token1"
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
                pl.col("symbol").alias("token_sold_symbol"),
                pl.col("amount0In").alias("amount_sold_raw"),
                (pl.col("amount0In") / pow(10, pl.col("decimals"))).alias(
                    "amount_sold"
                ),  # need to understand why this is not working
                pl.col("token1").alias("token_bought"),
                pl.col("symbol_token1").alias("token_bought_symbol"),
                pl.col("amount1Out").alias("amount_bought_raw"),
                (pl.col("amount1Out") / pow(10, pl.col("decimals_token1"))).alias(
                    "amount_bought"
                ),  # need to understand why this is not working
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
            token_df, left_on="token0", right_on="address", how="left", suffix="_token0"
        )
        .join(
            token_df, left_on="token1", right_on="address", how="left", suffix="_token1"
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
                pl.col("symbol_token1").alias("token_sold_symbol"),
                pl.col("amount1In").alias("amount_sold_raw"),
                (pl.col("amount1In") / pow(10, pl.col("decimals_token1"))).alias(
                    "amount_sold"
                ),  # need to understand why this is not working
                pl.col("token0").alias("token_bought"),
                pl.col("symbol").alias("token_bought_symbol"),
                pl.col("amount0Out").alias("amount_bought_raw"),
                (pl.col("amount0Out") / pow(10, pl.col("decimals"))).alias(
                    "amount_bought"
                ),  # need to understand why this is not working
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
    token_df: pl.DataFrame,
    sync_logs_df: pl.DataFrame,
    factory_address: str,
) -> pl.DataFrame:
    current_time = int(datetime.now(timezone.utc).timestamp())
    deposit_df = (
        mint_logs_df.join(
            pair_created_logs_df, left_on="address", right_on="pair", how="inner"
        )
        .join(
            token_df, left_on="token0", right_on="address", how="left", suffix="_token0"
        )
        .join(
            token_df, left_on="token1", right_on="address", how="left", suffix="_token1"
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
                pl.concat_list([pl.col("symbol"), pl.col("symbol_token1")]).alias(
                    "input_token_symbols"
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
    token_df: pl.DataFrame,
    sync_logs_df: pl.DataFrame,
    factory_address: str,
) -> pl.DataFrame:
    current_time = int(datetime.now(timezone.utc).timestamp())
    withdraw_df = (
        burn_logs_df.join(
            pair_created_logs_df, left_on="address", right_on="pair", how="inner"
        )
        .join(
            token_df, left_on="token0", right_on="address", how="left", suffix="_token0"
        )
        .join(
            token_df, left_on="token1", right_on="address", how="left", suffix="_token1"
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
                pl.concat_list([pl.col("symbol"), pl.col("symbol_token1")]).alias(
                    "input_token_symbols"
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
