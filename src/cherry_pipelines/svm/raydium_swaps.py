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


_AMM_PROGRAM_ID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
_SWAP_BASE_IN_DISCRIMINATOR = bytes([9])
_SWAP_BASE_OUT_DISCRIMINATOR = bytes([11])
_SWAP_BASE_IN_SIGNATURE = InstructionSignature(
    discriminator=_SWAP_BASE_IN_DISCRIMINATOR,
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
        "token_program",
        "amm",
        "amm_authority",
        "amm_open_orders",
        # "amm_target_orders",
        "pool_coin_token_account",
        "pool_pc_token_account",
        "serum_program",
        "serum_market",
        "serum_bids",
        "serum_asks",
        "serum_event_queue",
        "serum_coin_vault_account",
        "serum_pc_vault_account",
        "serum_vault_signer",
        "user_source_token_account",
        "user_destination_token_account",
        "user_source_owner",
    ],
)
_SWAP_BASE_OUT_SIGNATURE = InstructionSignature(
    discriminator=_SWAP_BASE_OUT_DISCRIMINATOR,
    params=[
        ParamInput(
            name="max_amount_in",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="amount_out",
            param_type=DynType.U64,
        ),
    ],
    accounts_names=[
        "token_program",
        "amm",
        "amm_authority",
        "amm_open_orders",
        # "amm_target_orders",
        "pool_coin_token_account",
        "pool_pc_token_account",
        "serum_program",
        "serum_market",
        "serum_bids",
        "serum_asks",
        "serum_event_queue",
        "serum_coin_vault_account",
        "serum_pc_vault_account",
        "serum_vault_signer",
        "user_source_token_account",
        "user_destination_token_account",
        "user_source_owner",
    ],
)

_CLMM_PROGRAM_ID = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
_SWAP_V1_DISCRIMINATOR = svm_anchor_discriminator("swap")
_SWAP_V2_DISCRIMINATOR = svm_anchor_discriminator("swap_v2")
_SWAP_V1_SIGNATURE = InstructionSignature(
    discriminator=_SWAP_V1_DISCRIMINATOR,
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
            name="sqrt_price_limit_x64",
            param_type=DynType.U128,
        ),
        ParamInput(
            name="is_base_input",
            param_type=DynType.Bool,
        ),
    ],
    accounts_names=[
        "payer",
        "amm_config",
        "pool_state",
        "input_token_account",
        "output_token_account",
        "input_vault",
        "output_vault",
        "observation_state",
        "token_program",
        "tick_array",
    ],
)
_SWAP_V2_SIGNATURE = InstructionSignature(
    discriminator=_SWAP_V2_DISCRIMINATOR,
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
            name="sqrt_price_limit_x64",
            param_type=DynType.U128,
        ),
        ParamInput(
            name="is_base_input",
            param_type=DynType.Bool,
        ),
    ],
    accounts_names=[
        "payer",
        "amm_config",
        "pool_state",
        "input_token_account",
        "output_token_account",
        "input_vault",
        "output_vault",
        "observation_state",
        "token_program",
        "token_program_2022",
        "memo_program",
        "input_vault_mint",
        "output_vault_mint",
    ],
)

_CP_SWAP_PROGRAM_ID = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
_SWAP_BASE_INPUT_DISCRIMINATOR = svm_anchor_discriminator("swap_base_input")
_SWAP_BASE_OUTPUT_DISCRIMINATOR = svm_anchor_discriminator("swap_base_output")
_SWAP_BASE_INPUT_SIGNATURE = InstructionSignature(
    discriminator=_SWAP_BASE_INPUT_DISCRIMINATOR,
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
        "payer",
        "authority",
        "amm_config",
        "pool_state",
        "input_token_account",
        "output_token_account",
        "input_vault",
        "output_vault",
        "input_token_program",
        "output_token_program",
        "input_token_mint",
        "output_token_mint",
        "observation_state",
    ],
)
_SWAP_BASE_OUTPUT_SIGNATURE = InstructionSignature(
    discriminator=_SWAP_BASE_OUTPUT_DISCRIMINATOR,
    params=[
        ParamInput(
            name="max_amount_in",
            param_type=DynType.U64,
        ),
        ParamInput(
            name="amount_out",
            param_type=DynType.U64,
        ),
    ],
    accounts_names=[
        "payer",
        "authority",
        "amm_config",
        "pool_state",
        "input_token_account",
        "output_token_account",
        "input_vault",
        "output_vault",
        "input_token_program",
        "output_token_program",
        "input_token_mint",
        "output_token_mint",
        "observation_state",
    ],
)

_TABLE_NAME = "raydium_swaps"


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
    output_mint String,
    output_vault String,
    output_amount UInt64,

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


def split_instructions(
    data: Dict[str, pl.DataFrame], _: Any
) -> Dict[str, pl.DataFrame]:
    out = deepcopy(data)

    instructions = data["instructions"]

    instructions = instructions.sort(
        ["block_slot", "transaction_index", "instruction_address"]
    )

    # This doesn't seem necessary for now since the code that would emit these is commented out
    # https://github.com/raydium-io/raydium-clmm/blob/master/programs/amm/src/instructions/swap_v2.rs#L85
    # But they might enable it?
    # It shouldn't harm indexing anyway so will leave it like this
    instructions = instructions.filter(
        pl.col("program_id").ne(base58_decode_string(_MEMO_PROGRAM_ID_V1))
        & pl.col("program_id").ne(base58_decode_string(_MEMO_PROGRAM_ID_V2))
    )
    instructions = instructions.with_row_index(name="instruction_index")

    out["amm_base_in_swaps"] = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_AMM_PROGRAM_ID))
        & pl.col("data").bin.starts_with(_SWAP_BASE_IN_DISCRIMINATOR)
    )
    out["amm_base_out_swaps"] = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_AMM_PROGRAM_ID))
        & pl.col("data").bin.starts_with(_SWAP_BASE_OUT_DISCRIMINATOR)
    )

    out["cp_swap_base_input_swaps"] = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_CP_SWAP_PROGRAM_ID))
        & pl.col("data").bin.starts_with(_SWAP_BASE_INPUT_DISCRIMINATOR)
    )
    out["cp_swap_base_output_swaps"] = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_CP_SWAP_PROGRAM_ID))
        & pl.col("data").bin.starts_with(_SWAP_BASE_OUTPUT_DISCRIMINATOR)
    )

    out["clmm_swaps_v1"] = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_CLMM_PROGRAM_ID))
        & pl.col("data").bin.starts_with(_SWAP_V1_DISCRIMINATOR)
    )
    out["clmm_swaps_v2"] = instructions.filter(
        pl.col("program_id").eq(base58_decode_string(_CLMM_PROGRAM_ID))
        & pl.col("data").bin.starts_with(_SWAP_V2_DISCRIMINATOR)
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


def process_data(data: Dict[str, pl.DataFrame], _: Any) -> Dict[str, pl.DataFrame]:
    swaps_v1 = data["clmm_swaps_v1"].select(
        pl.col("block_slot"),
        pl.col("block_hash"),
        pl.col("transaction_index"),
        pl.col("instruction_address"),
        pl.col("program_id"),
        pl.col("payer"),
        pl.col("pool_state").alias("pool"),
        pl.col("input_token_account"),
        pl.col("output_token_account"),
        pl.lit("clmm_v1").alias("swap_kind"),
        pl.lit(None).cast(pl.UInt64).alias("max_amount_in"),
        pl.lit(None).cast(pl.UInt64).alias("amount_out"),
        pl.lit(None).cast(pl.UInt64).alias("amount_in"),
        pl.lit(None).cast(pl.UInt64).alias("minimum_amount_out"),
        pl.col("amount"),
        pl.col("other_amount_threshold"),
        pl.col("sqrt_price_limit_x64"),
        pl.col("is_base_input"),
        pl.col("instruction_index"),
    )
    swaps_v2 = data["clmm_swaps_v2"].select(
        pl.col("block_slot"),
        pl.col("block_hash"),
        pl.col("transaction_index"),
        pl.col("instruction_address"),
        pl.col("program_id"),
        pl.col("payer"),
        pl.col("pool_state").alias("pool"),
        pl.col("input_token_account"),
        pl.col("output_token_account"),
        pl.lit("clmm_v2").alias("swap_kind"),
        pl.lit(None).cast(pl.UInt64).alias("max_amount_in"),
        pl.lit(None).cast(pl.UInt64).alias("amount_out"),
        pl.lit(None).cast(pl.UInt64).alias("amount_in"),
        pl.lit(None).cast(pl.UInt64).alias("minimum_amount_out"),
        pl.col("amount"),
        pl.col("other_amount_threshold"),
        pl.col("sqrt_price_limit_x64"),
        pl.col("is_base_input"),
        pl.col("instruction_index"),
    )
    cp_swap_base_input_swaps = data["cp_swap_base_input_swaps"].select(
        pl.col("block_slot"),
        pl.col("block_hash"),
        pl.col("transaction_index"),
        pl.col("instruction_address"),
        pl.col("program_id"),
        pl.col("payer"),
        pl.col("pool_state").alias("pool"),
        pl.col("input_token_account"),
        pl.col("output_token_account"),
        pl.lit("cp_swap_base_input").alias("swap_kind"),
        pl.lit(None).cast(pl.UInt64).alias("max_amount_in"),
        pl.lit(None).cast(pl.UInt64).alias("amount_out"),
        pl.col("amount_in"),
        pl.col("minimum_amount_out"),
        pl.lit(None).cast(pl.UInt64).alias("amount"),
        pl.lit(None).cast(pl.UInt64).alias("other_amount_threshold"),
        pl.lit(None).cast(pl.Decimal(38, 0)).alias("sqrt_price_limit_x64"),
        pl.lit(None).cast(pl.Boolean).alias("is_base_input"),
        pl.col("instruction_index"),
    )
    cp_swap_base_output_swaps = data["cp_swap_base_output_swaps"].select(
        pl.col("block_slot"),
        pl.col("block_hash"),
        pl.col("transaction_index"),
        pl.col("instruction_address"),
        pl.col("program_id"),
        pl.col("payer"),
        pl.col("pool_state").alias("pool"),
        pl.col("input_token_account"),
        pl.col("output_token_account"),
        pl.lit("cp_swap_base_output").alias("swap_kind"),
        pl.col("max_amount_in"),
        pl.col("amount_out"),
        pl.lit(None).cast(pl.UInt64).alias("amount_in"),
        pl.lit(None).cast(pl.UInt64).alias("minimum_amount_out"),
        pl.lit(None).cast(pl.UInt64).alias("amount"),
        pl.lit(None).cast(pl.UInt64).alias("other_amount_threshold"),
        pl.lit(None).cast(pl.Decimal(38, 0)).alias("sqrt_price_limit_x64"),
        pl.lit(None).cast(pl.Boolean).alias("is_base_input"),
        pl.col("instruction_index"),
    )
    amm_base_in_swaps = data["amm_base_in_swaps"].select(
        pl.col("block_slot"),
        pl.col("block_hash"),
        pl.col("transaction_index"),
        pl.col("instruction_address"),
        pl.col("program_id"),
        pl.col("user_source_owner").alias("payer"),
        pl.col("amm").alias("pool"),
        pl.col("user_source_token_account").alias("input_token_account"),
        pl.col("user_destination_token_account").alias("output_token_account"),
        pl.lit("amm_base_in").alias("swap_kind"),
        pl.lit(None).cast(pl.UInt64).alias("max_amount_in"),
        pl.lit(None).cast(pl.UInt64).alias("amount_out"),
        pl.col("amount_in"),
        pl.col("minimum_amount_out"),
        pl.lit(None).cast(pl.UInt64).alias("amount"),
        pl.lit(None).cast(pl.UInt64).alias("other_amount_threshold"),
        pl.lit(None).cast(pl.Decimal(38, 0)).alias("sqrt_price_limit_x64"),
        pl.lit(None).cast(pl.Boolean).alias("is_base_input"),
        pl.col("instruction_index"),
    )
    amm_base_out_swaps = data["amm_base_out_swaps"].select(
        pl.col("block_slot"),
        pl.col("block_hash"),
        pl.col("transaction_index"),
        pl.col("instruction_address"),
        pl.col("program_id"),
        pl.col("user_source_owner").alias("payer"),
        pl.col("amm").alias("pool"),
        pl.col("user_source_token_account").alias("input_token_account"),
        pl.col("user_destination_token_account").alias("output_token_account"),
        pl.lit("amm_base_out").alias("swap_kind"),
        pl.col("max_amount_in"),
        pl.col("amount_out"),
        pl.lit(None).cast(pl.UInt64).alias("amount_in"),
        pl.lit(None).cast(pl.UInt64).alias("minimum_amount_out"),
        pl.lit(None).cast(pl.UInt64).alias("amount"),
        pl.lit(None).cast(pl.UInt64).alias("other_amount_threshold"),
        pl.lit(None).cast(pl.Decimal(38, 0)).alias("sqrt_price_limit_x64"),
        pl.lit(None).cast(pl.Boolean).alias("is_base_input"),
        pl.col("instruction_index"),
    )

    swaps = pl.concat(
        [
            swaps_v1,
            swaps_v2,
            cp_swap_base_input_swaps,
            cp_swap_base_output_swaps,
            amm_base_in_swaps,
            amm_base_out_swaps,
        ]
    )

    transfers = data["transfers"].select(
        pl.col("block_slot"),
        pl.col("transaction_index"),
        pl.col("amount"),
        pl.col("instruction_index"),
        pl.col("destination"),
        pl.col("source"),
    )
    checked_transfers = data["checked_transfers"].select(
        pl.col("block_slot"),
        pl.col("transaction_index"),
        pl.col("amount"),
        pl.col("instruction_index"),
        pl.col("destination"),
        pl.col("source"),
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
        pl.col("destination").alias("input_vault"),
    )
    output_transfers = transfers.select(
        pl.col("block_slot"),
        pl.col("transaction_index"),
        pl.col("amount").alias("output_amount"),
        pl.col("instruction_index"),
        pl.lit(True).alias("found_output"),
        pl.col("source").alias("output_vault"),
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
                    program_id=[_CLMM_PROGRAM_ID],
                    discriminator=[
                        _SWAP_V1_DISCRIMINATOR,
                        _SWAP_V2_DISCRIMINATOR,
                    ],
                    include_transactions=True,
                    include_blocks=True,
                    include_inner_instructions=True,
                    is_committed=True,
                    include_transaction_token_balances=True,
                ),
                ingest.svm.InstructionRequest(
                    program_id=[_AMM_PROGRAM_ID],
                    discriminator=[
                        _SWAP_BASE_IN_DISCRIMINATOR,
                        _SWAP_BASE_OUT_DISCRIMINATOR,
                    ],
                    include_transactions=True,
                    include_blocks=True,
                    include_inner_instructions=True,
                    is_committed=True,
                    include_transaction_token_balances=True,
                ),
                ingest.svm.InstructionRequest(
                    program_id=[_CP_SWAP_PROGRAM_ID],
                    discriminator=[
                        _SWAP_BASE_INPUT_DISCRIMINATOR,
                        _SWAP_BASE_OUTPUT_DISCRIMINATOR,
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
                kind=cc.StepKind.CUSTOM,
                config=cc.CustomStepConfig(
                    runner=split_instructions,
                ),
            ),
            ###### AMM #######
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_SWAP_BASE_IN_SIGNATURE,
                    input_table="amm_base_in_swaps",
                    output_table="amm_base_in_swaps",
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_SWAP_BASE_OUT_SIGNATURE,
                    input_table="amm_base_out_swaps",
                    output_table="amm_base_out_swaps",
                ),
            ),
            ##### CP_SWAP #####
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_SWAP_BASE_INPUT_SIGNATURE,
                    input_table="cp_swap_base_input_swaps",
                    output_table="cp_swap_base_input_swaps",
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_SWAP_BASE_OUTPUT_SIGNATURE,
                    input_table="cp_swap_base_output_swaps",
                    output_table="cp_swap_base_output_swaps",
                ),
            ),
            ##### CLMM #####
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_SWAP_V1_SIGNATURE,
                    input_table="clmm_swaps_v1",
                    output_table="clmm_swaps_v1",
                ),
            ),
            cc.Step(
                kind=cc.StepKind.SVM_DECODE_INSTRUCTIONS,
                config=cc.SvmDecodeInstructionsConfig(
                    instruction_signature=_SWAP_V2_SIGNATURE,
                    input_table="clmm_swaps_v2",
                    output_table="clmm_swaps_v2",
                ),
            ),
            ##### TRANSFERS #####
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
