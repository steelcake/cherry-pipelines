from copy import deepcopy
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
    Field,
    Variant,
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


_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
_TOKEN_TRANSFER_DISCRIMINATOR = bytes([12])
_TOKEN_TRANSFER_SIGNATURE = InstructionSignature(
    discriminator=_TOKEN_TRANSFER_DISCRIMINATOR,
    params=[
        ParamInput(
            name="amount",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="decimals",
            param_type=DynType.U8,
        ),
    ],
    accounts_names=[
        "authority",
        "destination",
        "mint",
        "source",
    ],
)

_PROGRAM_ID = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
_DISCRIMINATOR = bytes.fromhex("2b04ed0b1ac91e62")
_INSTRUCTION_SIGNATURE = InstructionSignature(
    discriminator=_DISCRIMINATOR,
    params=[
        ParamInput(
            name="amount",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="other_amount_threshold",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="sqrt_price_limit",
            param_type=DynType.U128,
        ),
        ParamInput(
            name="amount_specified_is_input",
            param_type=DynType.Bool,
        ),
        ParamInput(
            name="a_to_b",
            param_type=DynType.Bool,
        ),
        ParamInput(
            name="remaining_accounts_info",
            param_type=DynType.Option(
                DynType.Struct(
                    [
                        Field(
                            name="slices",
                            element_type=DynType.Array(
                                DynType.Struct(
                                    [
                                        Field(
                                            name="accounts_type",
                                            element_type=DynType.Enum(
                                                [
                                                    Variant("transfer_hook_a", None),
                                                    Variant("transfer_hook_b", None),
                                                    Variant(
                                                        "transfer_hook_reward", None
                                                    ),
                                                    Variant(
                                                        "transfer_hook_input", None
                                                    ),
                                                    Variant(
                                                        "transfer_hook_intermediate",
                                                        None,
                                                    ),
                                                    Variant(
                                                        "transfer_hook_output", None
                                                    ),
                                                    Variant(
                                                        "supplemental_tick_arrays", None
                                                    ),
                                                    Variant(
                                                        "supplemental_tick_arrays_one",
                                                        None,
                                                    ),
                                                    Variant(
                                                        "supplemental_tick_arrays_two",
                                                        None,
                                                    ),
                                                ]
                                            ),
                                        ),
                                        Field(name="length", element_type=DynType.U8),
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            ),
        ),
    ],
    accounts_names=[
        "token_program_a",
        "token_program_b",
        "memo_program",
        "token_authority",
        "whirlpool",
        "token_mint_a",
        "token_mint_b",
        "token_owner_account_a",
        "token_vault_a",
        "token_owner_account_b",
        "token_vault_b",
    ],
)


_TABLE_NAME = "orca_swaps"


async def init_db(client: AsyncClient):
    await client.command(f"""
CREATE TABLE IF NOT EXISTS {_TABLE_NAME} (
    block_slot UInt64,
    block_hash String,
    transaction_index UInt64,
    transaction_signature String,
    instruction_address Array(UInt32),
    program_id String,
    token_authority String,
    whirlpool String,
    input_token_account String,
    output_token_account String,
    input_mint String,
    input_vault String,
    input_amount UInt64,
    output_mint String,
    output_vault String,
    output_amount UInt64,
    amount UInt64,
    amount_specified_is_input Boolean,
    other_amount_threshold UInt64,
    sqrt_price_limit Decimal128(0),
    timestamp Int64,
    block_height UInt64,

    INDEX ts_idx timestamp TYPE minmax GRANULARITY 4,
    INDEX height_idx block_height TYPE minmax GRANULARITY 4,
    INDEX input_token_account_idx `input_token_account` TYPE bloom_filter(0.01) GRANULARITY 4, 
    INDEX output_token_account_idx `output_token_account` TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX wirlpool_idx `whirlpool` TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX input_mint_idx `input_mint` TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX output_mint_idx `output_mint` TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX input_vault_idx `input_vault` TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX output_vault_idx `output_vault` TYPE bloom_filter(0.01) GRANULARITY 4
) ENGINE = MergeTree 
ORDER BY (block_slot, transaction_index, instruction_address); 
""")


def split_instructions(
    data: Dict[str, pl.DataFrame], _: Any
) -> Dict[str, pl.DataFrame]:
    out = deepcopy(data)

    instructions = data["instructions"]

    out["swaps"] = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_PROGRAM_ID))
        & pl.col("data").bin.starts_with(_DISCRIMINATOR)
    )
    out["transfers"] = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_TOKEN_PROGRAM_ID))
        & pl.col("data").bin.starts_with(_TOKEN_TRANSFER_DISCRIMINATOR)
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
        "token_authority",
        "whirlpool",
        "input_token_account",
        "output_token_account",
        "input_mint",
        "output_mint",
        "input_vault",
        "output_vault",
        "amount",
        "other_amount_threshold",
        "sqrt_price_limit",
        "amount_specified_is_input",
    )


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    swaps = data["swaps"]
    transfers = data["transfers"]

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

    a_b = swaps.filter(pl.col("a_to_b").eq(True))
    b_a = swaps.filter(pl.col("a_to_b").eq(False))

    swaps = select_swaps_df(
        a_b.rename(
            {
                "token_mint_a": "input_mint",
                "token_mint_b": "output_mint",
                "token_owner_account_a": "input_token_account",
                "token_owner_account_b": "output_token_account",
                "token_vault_a": "input_vault",
                "token_vault_b": "output_vault",
            }
        )
    ).vstack(
        select_swaps_df(
            b_a.rename(
                {
                    "token_mint_a": "output_mint",
                    "token_mint_b": "input_mint",
                    "token_owner_account_a": "output_token_account",
                    "token_owner_account_b": "input_token_account",
                    "token_vault_a": "output_vault",
                    "token_vault_b": "input_vault",
                }
            )
        )
    )

    transfers = transfers.select(
        pl.col("block_slot"),
        pl.col("transaction_index"),
        pl.col("amount"),
        pl.col("instruction_address").list.last().alias("last_addr"),
        pl.col("instruction_address").list.reverse().list.slice(1).list.reverse(),
    )

    output_transfers = transfers.filter(pl.col("last_addr").eq(1)).select(
        pl.col("block_slot"),
        pl.col("transaction_index"),
        pl.col("instruction_address"),
        pl.col("amount").alias("output_amount"),
    )
    input_transfers = transfers.filter(pl.col("last_addr").eq(0)).select(
        pl.col("block_slot"),
        pl.col("transaction_index"),
        pl.col("instruction_address"),
        pl.col("amount").alias("input_amount"),
    )

    swaps = swaps.join(
        output_transfers, on=["block_slot", "transaction_index", "instruction_address"]
    )

    swaps = swaps.join(
        input_transfers, on=["block_slot", "transaction_index", "instruction_address"]
    )

    swaps = swaps.join(transactions, on=["block_slot", "transaction_index"])

    swaps = swaps.join(blocks, on="block_slot")

    out = {}
    out[_TABLE_NAME] = swaps
    return out


async def run(cfg: SvmConfig):
    next_block = await db.get_next_block(cfg.client, _TABLE_NAME, "block_slot", None)
    from_block = max(cfg.from_block, next_block)
    logger.info(f"starting to ingest from block {from_block}")

    query = ingest.Query(
        kind=ingest.QueryKind.SVM,
        params=ingest.svm.Query(
            from_block=from_block,
            instructions=[
                ingest.svm.InstructionRequest(
                    program_id=[_PROGRAM_ID],
                    discriminator=[_INSTRUCTION_SIGNATURE.discriminator],
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
                    instruction_signature=_INSTRUCTION_SIGNATURE,
                    input_table="swaps",
                    output_table="swaps",
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_TOKEN_TRANSFER_SIGNATURE,
                    input_table="transfers",
                    output_table="transfers",
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

    await run_pipeline(pipeline=pipeline, pipeline_name=_TABLE_NAME)
