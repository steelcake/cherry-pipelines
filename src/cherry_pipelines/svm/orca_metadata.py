from copy import deepcopy
from cherry_etl.utils import svm_anchor_discriminator
from clickhouse_connect.driver.asyncclient import AsyncClient
from cherry_etl import config as cc, run_pipeline
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


_PROGRAM_ID = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"

_DISCRIMINATOR_V1 = svm_anchor_discriminator("initialize_pool")
_INSTRUCTION_SIGNATURE_V1 = InstructionSignature(
    discriminator=_DISCRIMINATOR_V1,
    params=[
        # This is a single field struct in IDL, but it is flattened here.
        # Has the same ABI representation.
        ParamInput(
            name="whirlpool_bump",
            param_type=DynType.U8,
        ),
        ParamInput(
            name="tick_spacing",
            param_type=DynType.U16,
        ),
        ParamInput(
            name="initial_sqrt_price",
            param_type=DynType.U128,
        ),
    ],
    accounts_names=[
        "whirlpools_config",
        "token_mint_a",
        "token_mint_b",
        "funder",
        "whirlpool",
        "token_vault_a",
        "token_vault_b",
        "fee_tier",
        "token_program",
        "system_program",
        "rent",
    ],
)

_DISCRIMINATOR_V2 = svm_anchor_discriminator("initialize_pool_v2")
_INSTRUCTION_SIGNATURE_V2 = InstructionSignature(
    discriminator=_DISCRIMINATOR_V2,
    params=[
        ParamInput(
            name="tick_spacing",
            param_type=DynType.U16,
        ),
        ParamInput(
            name="initial_sqrt_price",
            param_type=DynType.U128,
        ),
    ],
    accounts_names=[
        "whirlpools_config",
        "token_mint_a",
        "token_mint_b",
        "token_badge_a",
        "token_badge_b",
        "funder",
        "whirlpool",
        "token_vault_a",
        "token_vault_b",
        "fee_tier",
        "token_program_a",
        "token_program_b",
        "system_program",
        "rent",
    ],
)

_METADATA_TABLE_NAME = "orca_metadata"


async def init_db(client: AsyncClient):
    await client.command(f"""
CREATE TABLE IF NOT EXISTS {_METADATA_TABLE_NAME} (
    block_slot UInt64,
    block_hash String,
    transaction_index UInt64,
    transaction_signature String,
    instruction_address Array(UInt32),
    program_id String,
    timestamp Int64,
    block_height UInt64,

    version UInt8,

    whirlpools_config String,
    token_mint_a String,
    token_mint_b String,
    token_badge_a String,
    token_badge_b String,
    funder String,
    whirlpool String,
    token_vault_a String,
    token_vault_b String,
    fee_tier String,
    token_program_a String,
    token_program_b String,
    system_program String,
    rent String,

    whirlpool_bump UInt8,
    tick_spacing UInt16,
    initial_sqrt_price Decimal128(0),

    INDEX ts_idx timestamp TYPE minmax GRANULARITY 4,
    INDEX height_idx block_height TYPE minmax GRANULARITY 4,
    INDEX block_slot_idx block_slot TYPE minmax GRANULARITY 4,
    INDEX token_mint_a_idx `token_mint_a` TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX token_mint_b_idx `token_mint_b` TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = MergeTree 
ORDER BY (whirlpool); 
""")


def split_instructions(
    data: Dict[str, pl.DataFrame], _: Any
) -> Dict[str, pl.DataFrame]:
    out = deepcopy(data)

    instructions = data["instructions"]

    out["inits_v1"] = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_PROGRAM_ID))
        & pl.col("data").bin.starts_with(_DISCRIMINATOR_V1)
    )
    out["inits_v2"] = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_PROGRAM_ID))
        & pl.col("data").bin.starts_with(_DISCRIMINATOR_V2)
    )

    del out["instructions"]

    return out


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    inits_v1 = data["inits_v1"]
    inits_v1 = inits_v1.select(
        pl.col("block_slot"),
        pl.col("block_hash"),
        pl.col("transaction_index"),
        pl.col("instruction_address"),
        pl.col("program_id"),
        pl.repeat(1, inits_v1.height, dtype=pl.UInt8).alias("version"),
        pl.col("whirlpools_config"),
        pl.col("token_mint_a"),
        pl.col("token_mint_b"),
        pl.lit(None).cast(pl.Binary).alias("token_badge_a"),
        pl.lit(None).cast(pl.Binary).alias("token_badge_b"),
        pl.col("funder"),
        pl.col("whirlpool"),
        pl.col("token_vault_a"),
        pl.col("token_vault_b"),
        pl.col("fee_tier"),
        pl.col("token_program").alias("token_program_a"),
        pl.col("token_program").alias("token_program_b"),
        pl.col("system_program"),
        pl.col("rent"),
        pl.col("whirlpool_bump"),
        pl.col("tick_spacing"),
        pl.col("initial_sqrt_price"),
    )
    inits_v2 = data["inits_v2"]
    inits_v2 = inits_v2.select(
        pl.col("block_slot"),
        pl.col("block_hash"),
        pl.col("transaction_index"),
        pl.col("instruction_address"),
        pl.col("program_id"),
        pl.repeat(2, inits_v2.height, dtype=pl.UInt8).alias("version"),
        pl.col("whirlpools_config"),
        pl.col("token_mint_a"),
        pl.col("token_mint_b"),
        pl.col("token_badge_a"),
        pl.col("token_badge_b"),
        pl.col("funder"),
        pl.col("whirlpool"),
        pl.col("token_vault_a"),
        pl.col("token_vault_b"),
        pl.col("fee_tier"),
        pl.col("token_program_a"),
        pl.col("token_program_b"),
        pl.col("system_program"),
        pl.col("rent"),
        pl.lit(None).cast(pl.UInt8).alias("whirlpool_bump"),
        pl.col("tick_spacing"),
        pl.col("initial_sqrt_price"),
    )

    inits = inits_v1.vstack(inits_v2)

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

    inits = inits.join(transactions, on=["block_slot", "transaction_index"], how="left")

    inits = inits.join(blocks, on="block_slot", how="left")

    out = {}

    out[_METADATA_TABLE_NAME] = inits

    return out


async def run(cfg: SvmConfig):
    next_block = await db.get_next_block(
        cfg.client, _METADATA_TABLE_NAME, "block_slot", None
    )
    from_block = max(cfg.from_block, next_block)
    logger.info(f"starting to ingest from block {from_block}")

    query = ingest.Query(
        kind=ingest.QueryKind.SVM,
        params=ingest.svm.Query(
            from_block=from_block,
            to_block=cfg.to_block,
            instructions=[
                ingest.svm.InstructionRequest(
                    program_id=[_PROGRAM_ID],
                    discriminator=[_DISCRIMINATOR_V1, _DISCRIMINATOR_V2],
                    include_transactions=True,
                    include_blocks=True,
                    include_inner_instructions=True,
                    is_committed=True,
                )
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
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=split_instructions,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_INSTRUCTION_SIGNATURE_V1,
                    input_table="inits_v1",
                    output_table="inits_v1",
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_INSTRUCTION_SIGNATURE_V2,
                    input_table="inits_v2",
                    output_table="inits_v2",
                ),
            ),
            cc.Step(
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=process_data,
                ),
            ),
        ],
    )

    await run_pipeline(pipeline=pipeline, pipeline_name="orca_metadata")
