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


_MEMO_PROGRAM_ID_V1 = "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo"
_MEMO_PROGRAM_ID_V2 = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr"

_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
_TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
_TOKEN_TRANSFER_DISCRIMINATOR = bytes([3])
_TOKEN_TRANSFER_CHECKED_DISCRIMINATOR = bytes([12])
_TOKEN_TRANSFER_SIGNATURE = InstructionSignature(
    discriminator=_TOKEN_TRANSFER_DISCRIMINATOR,
    params=[
        ParamInput(
            name="amount",
            param_type=DynType.U64,
        ),
    ],
    accounts_names=[
        "source",
        "destination",
        "authority",
    ],
)
_TOKEN_TRANSFER_CHECKED_SIGNATURE = InstructionSignature(
    discriminator=_TOKEN_TRANSFER_CHECKED_DISCRIMINATOR,
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
        "source",
        "mint",
        "destination",
        "authority",
    ],
)

_PROGRAM_ID = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
_DISCRIMINATOR_V1 = svm_anchor_discriminator("swap")
_INSTRUCTION_SIGNATURE_V1 = InstructionSignature(
    discriminator=_DISCRIMINATOR_V1,
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
    ],
    accounts_names=[
        "token_program",
        "token_authority",
        "whirlpool",
        "token_owner_account_a",
        "token_vault_a",
        "token_owner_account_b",
        "token_vault_b",
    ],
)

_DISCRIMINATOR_V2 = svm_anchor_discriminator("swap_v2")
_INSTRUCTION_SIGNATURE_V2 = InstructionSignature(
    discriminator=_DISCRIMINATOR_V2,
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
    version UInt8,
    a_to_b Boolean,
    found_input Boolean,
    found_output Boolean,

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

    instructions = instructions.sort(
        ["block_slot", "transaction_index", "instruction_address"]
    )

    instructions = instructions.filter(
        pl.col("program_id").ne(base58_decode_string(_MEMO_PROGRAM_ID_V1))
        & pl.col("program_id").ne(base58_decode_string(_MEMO_PROGRAM_ID_V2))
    )
    instructions = instructions.with_row_index(name="instruction_index")

    out["swaps_v1"] = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_PROGRAM_ID))
        & pl.col("data").bin.starts_with(_DISCRIMINATOR_V1)
    )
    out["swaps_v2"] = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_PROGRAM_ID))
        & pl.col("data").bin.starts_with(_DISCRIMINATOR_V2)
    )
    out["transfers"] = instructions.filter(
        (
            pl.col("program_id").eq(base58_decode_string(_TOKEN_PROGRAM_ID))
            | pl.col("program_id").eq(base58_decode_string(_TOKEN_2022_PROGRAM_ID))
        )
        & pl.col("data").bin.starts_with(_TOKEN_TRANSFER_DISCRIMINATOR)
    )
    out["checked_transfers"] = instructions.filter(
        (
            pl.col("program_id").eq(base58_decode_string(_TOKEN_PROGRAM_ID))
            | pl.col("program_id").eq(base58_decode_string(_TOKEN_2022_PROGRAM_ID))
        )
        & pl.col("data").bin.starts_with(_TOKEN_TRANSFER_CHECKED_DISCRIMINATOR)
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
        "input_vault",
        "output_vault",
        "amount",
        "amount_specified_is_input",
        "other_amount_threshold",
        "sqrt_price_limit",
        "a_to_b",
        "version",
        "instruction_index",
    )


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    swaps_v1 = data["swaps_v1"]
    swaps_v1 = swaps_v1.select(
        pl.col("block_slot"),
        pl.col("block_hash"),
        pl.col("transaction_index"),
        pl.col("instruction_address"),
        pl.col("program_id"),
        pl.col("token_authority"),
        pl.col("token_program").alias("token_program_a"),
        pl.col("token_program").alias("token_program_b"),
        pl.lit(None).cast(pl.Binary).alias("memo_program"),
        pl.col("whirlpool"),
        pl.lit(None).cast(pl.Binary).alias("token_mint_a"),
        pl.lit(None).cast(pl.Binary).alias("token_mint_b"),
        pl.col("token_owner_account_a"),
        pl.col("token_vault_a"),
        pl.col("token_owner_account_b"),
        pl.col("token_vault_b"),
        pl.col("amount"),
        pl.col("other_amount_threshold"),
        pl.col("sqrt_price_limit"),
        pl.col("amount_specified_is_input"),
        pl.col("a_to_b"),
        pl.col("instruction_index"),
        pl.repeat(1, swaps_v1.height, dtype=pl.UInt8).alias("version"),
    )
    swaps_v2 = data["swaps_v2"]
    swaps_v2 = swaps_v2.select(
        pl.col("block_slot"),
        pl.col("block_hash"),
        pl.col("transaction_index"),
        pl.col("instruction_address"),
        pl.col("program_id"),
        pl.col("token_authority"),
        pl.col("token_program_a"),
        pl.col("token_program_b"),
        pl.col("memo_program"),
        pl.col("whirlpool"),
        pl.col("token_mint_a"),
        pl.col("token_mint_b"),
        pl.col("token_owner_account_a"),
        pl.col("token_vault_a"),
        pl.col("token_owner_account_b"),
        pl.col("token_vault_b"),
        pl.col("amount"),
        pl.col("other_amount_threshold"),
        pl.col("sqrt_price_limit"),
        pl.col("amount_specified_is_input"),
        pl.col("a_to_b"),
        pl.col("instruction_index"),
        pl.repeat(2, swaps_v2.height, dtype=pl.UInt8).alias("version"),
    )

    swaps = swaps_v1.vstack(swaps_v2)

    transfers = data["transfers"].select(
        "block_slot",
        "transaction_index",
        "amount",
        "instruction_index",
    )
    checked_transfers = data["checked_transfers"].select(
        "block_slot",
        "transaction_index",
        "amount",
        "instruction_index",
    )
    transfers = transfers.vstack(checked_transfers)

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

    swaps = swaps.with_columns(
        [
            pl.col("instruction_index").add(1).alias("input_transfer_index"),
            pl.col("instruction_index").add(2).alias("output_transfer_index"),
        ]
    )

    input_transfers = transfers.select(
        pl.col("block_slot"),
        pl.col("transaction_index"),
        pl.col("amount").alias("input_amount"),
        pl.col("instruction_index"),
        pl.lit(True).alias("found_input"),
    )
    output_transfers = transfers.select(
        pl.col("block_slot"),
        pl.col("transaction_index"),
        pl.col("amount").alias("output_amount"),
        pl.col("instruction_index"),
        pl.lit(True).alias("found_output"),
    )

    swaps = swaps.join(
        input_transfers,
        left_on=["block_slot", "transaction_index", "input_transfer_index"],
        right_on=["block_slot", "transaction_index", "instruction_index"],
        how="left",
    )

    swaps = swaps.join(
        output_transfers,
        left_on=["block_slot", "transaction_index", "output_transfer_index"],
        right_on=["block_slot", "transaction_index", "instruction_index"],
        how="left",
    )

    token_balances = data["token_balances"]
    input_balances = token_balances.select(
        pl.col("block_slot"),
        pl.col("transaction_index"),
        pl.col("account"),
        pl.col("post_mint").alias("input_mint"),
    )
    output_balances = token_balances.select(
        pl.col("block_slot"),
        pl.col("transaction_index"),
        pl.col("account"),
        pl.col("post_mint").alias("output_mint"),
    )

    swaps = swaps.join(
        input_balances,
        left_on=["block_slot", "transaction_index", "input_vault"],
        right_on=["block_slot", "transaction_index", "account"],
        how="left",
    )
    swaps = swaps.join(
        output_balances,
        left_on=["block_slot", "transaction_index", "output_vault"],
        right_on=["block_slot", "transaction_index", "account"],
        how="left",
    )

    swaps = swaps.join(transactions, on=["block_slot", "transaction_index"], how="left")

    swaps = swaps.join(blocks, on="block_slot", how="left")

    swaps = swaps.drop(
        ["instruction_index", "input_transfer_index", "output_transfer_index"]
    )

    out = {}
    out[_TABLE_NAME] = swaps
    out["token_decimals_table"] = token_balances.select(
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
                    program_id=[_PROGRAM_ID],
                    discriminator=[_DISCRIMINATOR_V1, _DISCRIMINATOR_V2],
                    include_transactions=True,
                    include_blocks=True,
                    include_inner_instructions=True,
                    is_committed=True,
                    include_transaction_token_balances=True,
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
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=split_instructions,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_INSTRUCTION_SIGNATURE_V2,
                    input_table="swaps_v2",
                    output_table="swaps_v2",
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_INSTRUCTION_SIGNATURE_V1,
                    input_table="swaps_v1",
                    output_table="swaps_v1",
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
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_TOKEN_TRANSFER_CHECKED_SIGNATURE,
                    input_table="checked_transfers",
                    output_table="checked_transfers",
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
