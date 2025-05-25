from copy import deepcopy
from clickhouse_connect.driver.asyncclient import AsyncClient
from cherry_etl import config as cc, run_pipeline
from cherry_etl.utils import svm_anchor_discriminator
from cherry_core import ingest, base58_decode_string
import logging
from typing import Dict, Any
import polars as pl
from cherry_core.svm_decode import (
    InstructionSignature,
    DynType,
    ParamInput,
)


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


_CP_AMM_PROGRAM_ID = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG"
_CP_AMM_SWAP_DISCRIMINATOR = svm_anchor_discriminator("swap")
_CP_AMM_SWAP_CPI_DISCRIMINATOR = bytes.fromhex("e445a52e51cb9a1d1b3c15d58aaabb93")
_CP_AMM_SWAP_SIGNATURE = InstructionSignature(
    discriminator=_CP_AMM_SWAP_DISCRIMINATOR,
    params=[
        ParamInput(
            name="amount_in",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="minimum_amount_out",
            param_type=DynType.U64,
        ),
    ],
    accounts_names=[
        "pool_authority",
        "pool",
        "input_token_account",
        "output_token_account",
        "token_a_vault",
        "token_b_vault",
        "token_a_mint",
        "token_b_mint",
        "payer",
        "token_a_program",
        "token_b_program",
        "referral_token_account",
    ],
)
_CP_AMM_SWAP_CPI_SIGNATURE = InstructionSignature(
    discriminator=_CP_AMM_SWAP_CPI_DISCRIMINATOR,
    params=[
        ParamInput(
            name="pool",
            param_type=DynType.FixedArray(DynType.U8, 32),
        ),
        # This is represented as a u8 with 0 meaning a_to_b and 1 meaning b_to_a but
        # use boolean here to make sure it is either 0 or 1 and also get a more meaningful value
        ParamInput(
            name="b_to_a",
            param_type=DynType.Bool,
        ),
        ParamInput(
            name="has_referral",
            param_type=DynType.Bool,
        ),
        # Unpack the SwapParameters struct here so it is easier to manage in arrow format
        # It has the same borsh representation so we can just unpack it like this.
        ParamInput(
            name="amount_in",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="minimum_amount_out",
            param_type=DynType.U64,
        ),
        # unpack SwapResult struct
        ParamInput(
            name="output_amount",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="next_sqrt_price",
            param_type=DynType.U128,
        ),
        ParamInput(
            name="lp_fee",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="protocol_fee",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="partner_fee",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="referral_fee",
            param_type=DynType.U64,
        ),
        # rest of the top level fields
        ParamInput(
            name="actual_amount_in",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="current_timestamp",
            param_type=DynType.U64,
        ),
    ],
    accounts_names=[
        # Don't care about this
        # "event_authority",
    ],
)

_TABLE_NAME = "meteora_swaps"


async def init_db(client: AsyncClient):
    await client.command(f"""
CREATE TABLE IF NOT EXISTS {_TABLE_NAME} (
    block_slot UInt64,
    block_hash String,
    transaction_index UInt64,
    transaction_signature String,
    instruction_address Array(UInt32),
    program_id String,

    pool String,
    b_to_a Boolean,
    has_referral Boolean,
    amount_in UInt64,
    minimum_amount_out UInt64,
    output_amount UInt64,
    next_sqrt_price Decimal128(0),
    lp_fee UInt64,
    protocol_fee UInt64,
    partner_fee UInt64,
    referral_fee UInt64,
    input_amount UInt64,
    current_timestamp UInt64,
    found_cpi Boolean,

    pool_authority String,
    input_token_account String,
    output_token_account String,
    input_vault String,
    output_vault String,
    input_mint String,
    output_mint String,
    payer String,
    input_token_program String,
    output_token_program String,
    referral_token_account String,

    timestamp Int64,
    block_height UInt64,

    INDEX ts_idx timestamp TYPE minmax GRANULARITY 4,
    INDEX height_idx block_height TYPE minmax GRANULARITY 4,
    INDEX input_token_account_idx `input_token_account` TYPE bloom_filter(0.01) GRANULARITY 4, 
    INDEX output_token_account_idx `output_token_account` TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX pool_idx `pool` TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX input_mint_idx `input_mint` TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX output_mint_idx `output_mint` TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX input_vault_idx `input_vault` TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX output_vault_idx `output_vault` TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX payer_idx `payer` TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = MergeTree 
ORDER BY (block_slot, transaction_index, instruction_address); 
""")


def split_instructions(
    data: Dict[str, pl.DataFrame], _: Any
) -> Dict[str, pl.DataFrame]:
    out = deepcopy(data)

    instructions = data["instructions"]

    instructions = instructions.sort(
        ["block_slot", "transaction_index", "instruction_address"]
    )

    instructions = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_CP_AMM_PROGRAM_ID))
        & (
            pl.col("data").bin.starts_with(_CP_AMM_SWAP_DISCRIMINATOR)
            | pl.col("data").bin.starts_with(_CP_AMM_SWAP_CPI_DISCRIMINATOR)
        )
    )
    instructions = instructions.with_row_index(name="instruction_index")

    out["swaps"] = instructions.filter(
        pl.col("data").bin.starts_with(_CP_AMM_SWAP_DISCRIMINATOR)
    )
    out["cpi"] = instructions.filter(
        pl.col("data").bin.starts_with(_CP_AMM_SWAP_CPI_DISCRIMINATOR)
    )

    del out["instructions"]

    return out


def select_swaps_df(swaps: pl.DataFrame) -> pl.DataFrame:
    return swaps.select(
        "block_slot",
        "block_hash",
        "transaction_index",
        "instruction_address",
        "program_id",
        "pool",
        "b_to_a",
        "has_referral",
        "amount_in",
        "minimum_amount_out",
        "output_amount",
        "next_sqrt_price",
        "lp_fee",
        "protocol_fee",
        "partner_fee",
        "referral_fee",
        "input_amount",
        "current_timestamp",
        "pool_authority",
        "input_token_account",
        "output_token_account",
        "input_vault",
        "output_vault",
        "input_mint",
        "output_mint",
        "payer",
        "input_token_program",
        "output_token_program",
        "referral_token_account",
        "found_cpi",
    )


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    swaps = data["swaps"]
    cpi = data["cpi"].with_columns([pl.lit(True).alias("found_cpi")])

    swaps = swaps.with_columns(pl.col("instruction_index").add(1).alias("cpi_index"))

    swaps = swaps.join(
        cpi,
        left_on=["block_slot", "transaction_index", "cpi_index"],
        right_on=["block_slot", "transaction_index", "instruction_index"],
        how="left",
    )

    swaps = swaps.rename(
        {
            "actual_amount_in": "input_amount",
        }
    )

    a_to_b = swaps.filter(pl.col("b_to_a").eq(False))
    b_to_a = swaps.filter(pl.col("b_to_a").eq(True))

    swaps = select_swaps_df(
        a_to_b.rename(
            {
                "token_a_mint": "input_mint",
                "token_b_mint": "output_mint",
                "token_a_program": "input_token_program",
                "token_b_program": "output_token_program",
                "token_a_vault": "input_vault",
                "token_b_vault": "output_vault",
            }
        )
    ).vstack(
        select_swaps_df(
            b_to_a.rename(
                {
                    "token_a_mint": "output_mint",
                    "token_b_mint": "input_mint",
                    "token_a_program": "output_token_program",
                    "token_b_program": "input_token_program",
                    "token_a_vault": "output_vault",
                    "token_b_vault": "input_vault",
                }
            )
        )
    )

    transactions = data["transactions"].select(
        pl.col("block_slot"),
        pl.col("transaction_index"),
        pl.col("signature").alias("transaction_signature"),
    )

    blocks = data["blocks"].select(
        pl.col("slot").alias("block_slot"),
        pl.col("height").alias("block_height"),
        pl.col("timestamp"),
    )

    swaps = swaps.join(transactions, on=["block_slot", "transaction_index"], how="left")

    swaps = swaps.join(blocks, on="block_slot", how="left")

    out = {}
    out[_TABLE_NAME] = swaps
    out["token_decimals_table"] = data["token_balances"].select(
        pl.col("post_mint").alias("mint"),
        pl.col("post_decimals").alias("decimals"),
    )
    return out


async def run(cfg: SvmConfig):
    next_block = await db.get_next_block(cfg.client, _TABLE_NAME, "block_slot", None)
    from_block = max(cfg.from_block, next_block)
    logger.info(f"starting to ingest from block {from_block}")

    query = ingest.Query(
        kind=ingest.QueryKind.SVM,
        params=ingest.svm.Query(
            from_block=from_block,
            to_block=cfg.to_block,
            instructions=[
                ingest.svm.InstructionRequest(
                    program_id=[_CP_AMM_PROGRAM_ID],
                    discriminator=[
                        _CP_AMM_SWAP_DISCRIMINATOR,
                    ],
                    include_transactions=True,
                    include_blocks=True,
                    include_inner_instructions=True,
                    is_committed=True,
                    include_transaction_token_balances=True,
                ),
            ],
            fields=ingest.svm.Fields(
                block=ingest.svm.BlockFields(
                    timestamp=True,
                    height=True,
                    slot=True,
                ),
                transaction=ingest.svm.TransactionFields(
                    block_slot=True,
                    signature=True,
                    transaction_index=True,
                ),
                instruction=ingest.svm.InstructionFields(
                    a0=True,
                    a1=True,
                    a2=True,
                    a3=True,
                    a4=True,
                    a5=True,
                    a6=True,
                    a7=True,
                    a8=True,
                    a9=True,
                    data=True,
                    is_committed=True,
                    block_slot=True,
                    block_hash=True,
                    transaction_index=True,
                    instruction_address=True,
                    program_id=True,
                    rest_of_accounts=True,
                ),
                token_balance=ingest.svm.TokenBalanceFields(
                    block_slot=True,
                    transaction_index=True,
                    post_decimals=True,
                    post_mint=True,
                    account=True,
                ),
            ),
        ),
    )

    writer = cc.Writer(
        kind=cc.WriterKind.CLICKHOUSE,
        config=cc.ClickHouseWriterConfig(
            client=cfg.client,
            create_tables=False,
            anchor_table=_TABLE_NAME,
        ),
    )

    pipeline = cc.Pipeline(
        provider=cfg.provider,
        writer=writer,
        query=query,
        steps=[
            cc.Step(
                kind=cc.StepKind.POLARS,
                config=cc.PolarsStepConfig(
                    runner=split_instructions,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_CP_AMM_SWAP_SIGNATURE,
                    input_table="swaps",
                    output_table="swaps",
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_CP_AMM_SWAP_CPI_SIGNATURE,
                    input_table="cpi",
                    output_table="cpi",
                ),
            ),
            cc.Step(
                kind=cc.StepKind.POLARS,
                config=cc.PolarsStepConfig(
                    runner=process_data,
                ),
            ),
        ],
    )

    await run_pipeline(pipeline=pipeline, pipeline_name=_TABLE_NAME)
