import pyarrow as pa
import polars as pl
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import asyncio

from cherry_etl import config as cc
from cherry_etl.pipeline import run_pipeline
from cherry_etl.writers.writer import create_writer
from cherry_core import evm_signature_to_topic0, get_token_metadata_as_table
from cherry_core.ingest import (
    Query as IngestQuery,
    QueryKind,
)
from cherry_core.ingest.evm import (
    Query,
    Fields,
    BlockFields,
    TransactionFields,
    LogFields,
    LogRequest,
)

from cherry_pipelines import db
from cherry_pipelines.config import (
    EvmConfig,
    make_evm_table_name,
)

from clickhouse_connect.driver.asyncclient import AsyncClient

EVENT_SIGNATURE = "Transfer(address indexed from, address indexed to, uint256 amount)"
SCHEMA_VERSION = "1.0.0"
PIPELINE_VERSION = "1.0.0"
STABLECOIN_LIST = {
    "circle_usdc_v1_ethereum": [
        "Circle USDC v1",
        "ethereum",
        "circle_usdc_v1",
        "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    ],
    "curve_finance_crvusd_ethereum_v1_ethereum": [
        "Curve Finance crvUSD v1",
        "ethereum",
        "curve_finance_crvusd_v1",
        "0xf939E0A03FB07F59A73314E73794Be0E57ac1b4E",
    ],
    "ethena_usde_v1_ethereum": [
        "Ethena USDe v1",
        "ethereum",
        "ethena_usde_v1",
        "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
    ],
    "first_digital_fdusd_v1_ethereum": [
        "First Digital USD v1",
        "ethereum",
        "first_digital_fdusd_v1",
        "0xc5f0f7b66764F6ec8C8Dff7BA683102295E16409",
    ],
    "frax_finance_frax_v1_ethereum": [
        "Frax Finance Frax v1",
        "ethereum",
        "frax_finance_frax_v1",
        "0x853d955aCEf822Db058eb8505911ED77F175b99e",
    ],
    "liquity_lusd_v1_ethereum": [
        "Liquity LUSD",
        "ethereum",
        "liquity_lusd_v1",
        "0x5f98805A4E8be255a32880FDeC7F6728C6568bA0",
    ],
    "maker_dai_v1_ethereum": [
        "Maker DAI v1",
        "ethereum",
        "maker_dai_v1",
        "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    ],
    "paxos_usdp_v1_ethereum": [
        "Paxos USDP v1",
        "ethereum",
        "paxos_usdp_v1",
        "0x8E870D67F660D95d5be530380D0eC0bd388289E1",
    ],
    "paypal_pyusd_v1_ethereum": [
        "Paypal pyUSD v1",
        "ethereum",
        "paypal_pyusd_v1",
        "0x6c3ea9036406852006290770BEdFcAbA0e23A0e8",
    ],
    "decentralized_usdd_v1_ethereum": [
        "Decentralized USDD v1",
        "ethereum",
        "decentralized_usdd_v1",
        "0x3D7975EcCFc61a2102b08925CbBa0a4D4dBB6555",
    ],
    "trueusd_tusd_v1_ethereum": [
        "TrueUSD TUSD v1",
        "ethereum",
        "trueusd_tusd_v1",
        "0x0000000000085d4780B73119b644AE5ecd22b376",
    ],
    "world_liberty_financial_usd1_v1_ethereum": [
        "World Liberty Financial USD1 v1",
        "ethereum",
        "world_liberty_financial_usd1_v1",
        "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",
    ],
}


async def create_protocol_table(
    cfg: EvmConfig, token_address: str, name: str, slug: str, network: str
) -> Optional[pa.Table]:
    table_name = make_evm_table_name(slug, network, "protocol")
    protocol_table_exist = await cfg.client.query(f"EXISTS TABLE evm.{table_name}")
    protocol_table_exist = bool(protocol_table_exist.result_rows[0][0])
    if not protocol_table_exist:
        protocol_df = pl.DataFrame(
            {
                "id": [token_address],
                "name": [name],
                "slug": [slug],
                "schema_version": [SCHEMA_VERSION],
                "pipelineVersion": [PIPELINE_VERSION],
                "network": [network],
                "type": ["stablecoin"],
            }
        )
        return protocol_df.to_arrow()
    return None


async def create_token_table(
    cfg: EvmConfig, token_address: str, slug: str, network: str
) -> Optional[pa.Table]:
    table_name = make_evm_table_name(slug, network, "token")
    token_table_exist = await cfg.client.query(f"EXISTS TABLE evm.{table_name}")
    token_table_exist = bool(token_table_exist.result_rows[0][0])
    if not token_table_exist:
        address = bytes.fromhex(token_address.strip("0x"))
        token_metadata = get_token_metadata_as_table(
            cfg.rpc_provider_url,
            [token_address],
        ).to_pydict()

        token_df = pl.DataFrame(
            {
                "id": token_address,
                "address": [address],
                "name": token_metadata["name"][0],
                "symbol": token_metadata["symbol"][0],
                "decimal": token_metadata["decimals"][0],
            }
        )
        return token_df.to_arrow()
    return None


def transformations(
    data: Dict[str, pl.DataFrame], context: Any
) -> Dict[str, pl.DataFrame]:
    """
    Transform the decoded_logs DataFrame into the Transfer, Mint and Burn schema format.

    Args:
        data: A dictionary with polars DataFrames generated by the query.

    Returns:
        A dictionary with polars DataFrames formatted according to the Transfer, Mint and Burn schema
    """
    decoded_logs_df = data["decoded_logs"]
    tables = context["tables"]
    current_time = int(datetime.now(timezone.utc).timestamp())

    # Synchronous wrapper for read_table
    def sync_query() -> pl.DataFrame:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        df = loop.run_until_complete(
            context["con"].client.query_arrow(f"SELECT * FROM evm.{tables['token']}")
        )
        return pl.DataFrame(pl.from_arrow(df))

    token_df = sync_query()
    token_df = token_df.with_columns(pl.col("address").cast(pl.Binary).alias("address"))

    # Create the transformation
    all_transfers_df = decoded_logs_df.join(token_df, on="address", how="left").select(
        [
            pl.concat_str(
                [
                    pl.lit("transfer-"),
                    pl.lit("0x"),
                    pl.col("transaction_hash").bin.encode("hex"),
                    pl.lit("-"),
                    pl.col("log_index"),
                ]
            ).alias("id"),
            pl.concat_str([pl.lit("0x"), pl.col("address").bin.encode("hex")]).alias(
                "protocol"
            ),
            pl.col("address").alias("token_address"),
            pl.col("name"),
            pl.col("symbol"),
            pl.col("transaction_hash").alias("tx_hash"),
            pl.col("transaction_index").alias("tx_index"),
            pl.col("log_index"),
            pl.col("amount").alias("amount_raw"),
            (pl.col("amount") / 10 ** pl.col("decimal")).alias("amount"),
            pl.col("from_right").alias("tx_from"),
            pl.col("to_right").alias("tx_to"),
            pl.col("from"),
            pl.col("to"),
            pl.col("block_number"),
            pl.col("timestamp").cast(pl.Int64).alias("timestamp"),
            pl.lit(current_time).alias("exe_timestamp_utc"),
        ]
    )

    mint_df = all_transfers_df.filter(
        pl.col("from")
        == pl.lit("0x0000000000000000000000000000000000000000")
        .str.strip_prefix("0x")
        .str.decode("hex")
    ).select(
        [
            pl.col("id").replace("transfer", "mint").alias("id"),
            pl.col("protocol"),
            pl.col("token_address"),
            pl.col("name"),
            pl.col("symbol"),
            pl.col("tx_hash"),
            pl.col("tx_index"),
            pl.col("log_index"),
            pl.col("amount_raw").alias("amount_minted_raw"),
            pl.col("amount").alias("amount_minted"),
            pl.col("tx_from"),
            pl.col("tx_to"),
            pl.col("from"),
            pl.col("to"),
            pl.col("block_number"),
            pl.col("timestamp"),
            pl.col("exe_timestamp_utc"),
        ]
    )
    burn_df = all_transfers_df.filter(
        pl.col("to")
        == pl.lit("0x0000000000000000000000000000000000000000")
        .str.strip_prefix("0x")
        .str.decode("hex")
    ).select(
        [
            pl.col("id").replace("transfer", "burn").alias("id"),
            pl.col("protocol"),
            pl.col("token_address"),
            pl.col("name"),
            pl.col("symbol"),
            pl.col("tx_hash"),
            pl.col("tx_index"),
            pl.col("log_index"),
            pl.col("amount_raw").alias("amount_burned_raw"),
            pl.col("amount").alias("amount_burned"),
            pl.col("tx_from"),
            pl.col("tx_to"),
            pl.col("from"),
            pl.col("to"),
            pl.col("block_number"),
            pl.col("timestamp"),
            pl.col("exe_timestamp_utc"),
        ]
    )
    all_transfers_df = all_transfers_df.filter(
        (
            pl.col("to")
            != pl.lit("0x0000000000000000000000000000000000000000")
            .str.strip_prefix("0x")
            .str.decode("hex")
        )
        & (
            pl.col("from")
            != pl.lit("0x0000000000000000000000000000000000000000")
            .str.strip_prefix("0x")
            .str.decode("hex")
        )
    )
    output = {}
    output[tables["transfer_evt"]] = all_transfers_df
    output[tables["mint_evt"]] = mint_df
    output[tables["burn_evt"]] = burn_df
    return output


def make_writer(client: AsyncClient) -> cc.Writer:
    skip_index = {}
    order_by = {}
    skip_index["paypal_pyusd_v1_ethereum_protocol"] = [
        cc.ClickHouseSkipIndex(
            name="paypal_pyusd_v1_ethereum_protocol_id_index",
            val="id",
            type_="minmax",
            granularity=4,
        ),
    ]
    skip_index["paypal_pyusd_v1_ethereum_token"] = [
        cc.ClickHouseSkipIndex(
            name="paypal_pyusd_v1_ethereum_token_id_index",
            val="id",
            type_="minmax",
            granularity=4,
        ),
    ]
    order_by["paypal_pyusd_v1_ethereum_protocol"] = [
        "id",
    ]
    order_by["paypal_pyusd_v1_ethereum_token"] = [
        "id",
    ]

    for table_name in [
        "paypal_pyusd_v1_ethereum_transfer_evt",
        "paypal_pyusd_v1_ethereum_mint_evt",
        "paypal_pyusd_v1_ethereum_burn_evt",
    ]:
        skip_index[table_name] = [
            cc.ClickHouseSkipIndex(
                name=f"{table_name}_tx_hash_index",
                val="tx_hash",
                type_="bloom_filter(0.01)",
                granularity=4,
            ),
            cc.ClickHouseSkipIndex(
                name=f"{table_name}_from_index",
                val="from",
                type_="bloom_filter(0.01)",
                granularity=4,
            ),
            cc.ClickHouseSkipIndex(
                name=f"{table_name}_to_index",
                val="to",
                type_="bloom_filter(0.01)",
                granularity=4,
            ),
            cc.ClickHouseSkipIndex(
                name=f"{table_name}_timestamp_index",
                val="timestamp",
                type_="minmax",
                granularity=4,
            ),
        ]

        order_by[table_name] = [
            "block_number",
            "log_index",
        ]

    writer = cc.Writer(
        kind=cc.WriterKind.CLICKHOUSE,
        config=cc.ClickHouseWriterConfig(
            client=client,
            order_by=order_by,
            skip_index=skip_index,
        ),
    )

    return writer


async def make_pipeline(cfg: EvmConfig, pipeline_name: str) -> cc.Pipeline:
    try:
        name = STABLECOIN_LIST[pipeline_name][0]
        network = STABLECOIN_LIST[pipeline_name][1]
        slug = STABLECOIN_LIST[pipeline_name][2]
        token_address = STABLECOIN_LIST[pipeline_name][3]
    except KeyError:
        raise ValueError(f"Pipeline name {pipeline_name} not found in stablecoin list")

    return await pipeline_factory(cfg, name, network, slug, token_address)


async def pipeline_factory(
    cfg: EvmConfig, name: str, network: str, slug: str, token_address: str
):
    tables = {
        "transfer_evt": make_evm_table_name(slug, network, "transfer_evt"),
        "mint_evt": make_evm_table_name(slug, network, "mint_evt"),
        "burn_evt": make_evm_table_name(slug, network, "burn_evt"),
        "protocol": make_evm_table_name(slug, network, "protocol"),
        "token": make_evm_table_name(slug, network, "token"),
    }
    writer = make_writer(cfg.client)

    protocol_arrow = await create_protocol_table(
        cfg=cfg, token_address=token_address, name=name, slug=slug, network=network
    )

    token_arrow = await create_token_table(
        cfg=cfg, token_address=token_address, slug=slug, network=network
    )

    created_tables_dict = (
        {
            tables["protocol"]: protocol_arrow,
            tables["token"]: token_arrow,
        }
        if protocol_arrow is not None and token_arrow is not None
        else {}
    )

    data_writer = create_writer(writer)
    await data_writer.push_data(created_tables_dict)

    topic0 = evm_signature_to_topic0(EVENT_SIGNATURE)

    last_from_block = await db.get_max_block(
        cfg.client, tables["transfer_evt"], "block_number"
    )
    from_block = max(cfg.from_block, last_from_block)
    to_block = cfg.to_block

    # Querying
    query = IngestQuery(
        kind=QueryKind.EVM,
        params=Query(
            from_block=from_block,
            to_block=to_block,
            include_all_blocks=False,
            logs=[
                LogRequest(
                    address=[token_address],
                    topic0=[topic0],
                    include_transactions=True,
                    include_blocks=True,
                )
            ],
            fields=Fields(
                block=BlockFields(number=True, timestamp=True),
                transaction=TransactionFields(
                    block_number=True,
                    transaction_index=True,
                    from_=True,
                    to=True,
                ),
                log=LogFields(
                    block_number=True,
                    block_hash=True,
                    transaction_index=True,
                    log_index=True,
                    transaction_hash=True,
                    address=True,
                    topic0=True,
                    topic1=True,
                    topic2=True,
                    topic3=True,
                    data=True,
                ),
            ),
        ),
    )

    # Transformation Steps
    steps = [
        # Decode the events
        cc.Step(
            kind=cc.StepKind.EVM_DECODE_EVENTS,
            config=cc.EvmDecodeEventsConfig(
                event_signature=EVENT_SIGNATURE,
                hstack=True,
                allow_decode_fail=True,
            ),
        ),
        # Handle decimal256 values
        cc.Step(
            kind=cc.StepKind.CAST_BY_TYPE,
            config=cc.CastByTypeConfig(
                from_type=pa.decimal256(76, 0),
                to_type=pa.float64(),
            ),
        ),
        # Join the transaction data
        cc.Step(
            kind=cc.StepKind.JOIN_EVM_TRANSACTION_DATA,
            config=cc.JoinEvmTransactionDataConfig(),
        ),
        # Join the block data
        cc.Step(
            kind=cc.StepKind.JOIN_BLOCK_DATA,
            config=cc.JoinBlockDataConfig(),
        ),
        # Tranformations
        cc.Step(
            kind=cc.StepKind.CUSTOM,
            config=cc.CustomStepConfig(
                runner=transformations,
                context={
                    "con": writer.config,
                    "tables": tables,
                },
            ),
        ),
        # cc.Step(
        #     kind=cc.StepKind.HEX_ENCODE,
        #     config=cc.HexEncodeConfig(),
        # ),
    ]

    # Running a Pipeline
    pipeline = cc.Pipeline(
        provider=cfg.provider,
        query=query,
        writer=writer,
        steps=steps,
    )
    await run_pipeline(pipeline_name="stablecoin", pipeline=pipeline)

    return pipeline
