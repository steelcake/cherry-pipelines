import logging
from clickhouse_connect.driver.asyncclient import AsyncClient
import clickhouse_connect
import os
from dotenv import load_dotenv
import asyncio
from cherry_core import ingest
from cherry_pipelines.config import EvmConfig, SvmConfig, SQDChainId, HyperSyncChainId
from typing import Awaitable, Callable, Optional
import requests
from cherry_pipelines import evm
from cherry_etl import config as cc, run_pipeline

logger = logging.getLogger(__name__)


def make_evm_provider(
    provider_kind: ingest.ProviderKind, chain_id: int
) -> ingest.ProviderConfig:
    """Create an evm provider config based on `config.ProviderKind` and `config.chain_id`"""
    url = ""
    if provider_kind == ingest.ProviderKind.HYPERSYNC:
        url = f"https://{chain_id}.hypersync.xyz"
    elif provider_kind == ingest.ProviderKind.SQD:
        url = f"https://portal.sqd.dev/datasets/{SQDChainId.get_sqd_name(chain_id=chain_id)}"

    return ingest.ProviderConfig(
        kind=provider_kind,
        url=url,
    )


# Use this because it has more fresh data and has block_number=block_slot
# whereas the solana-mainnet dataset is way behind and it has block_number=block_height
_SQD_SVM_URL = "https://portal.sqd.dev/datasets/solana-beta"


def make_svm_provider() -> ingest.ProviderConfig:
    """Create a solana provider config"""
    return ingest.ProviderConfig(
        kind=ingest.ProviderKind.SQD,
        url=_SQD_SVM_URL,
    )


def get_solana_start_block() -> int:
    """Fetch Solana dataset start_block from SQD portal"""
    return int(requests.get(f"{_SQD_SVM_URL}/metadata").json()["start_block"])


def _to_int(val: Optional[str]) -> Optional[int]:
    if val is not None:
        return int(val)
    else:
        return None


def _to_int_with_default(val: Optional[str], default: int) -> int:
    int_val = _to_int(val)
    if int_val is not None:
        return int_val
    else:
        return default


def _to_provider_kind(kind: str) -> ingest.ProviderKind:
    if kind == ingest.ProviderKind.SQD:
        return ingest.ProviderKind.SQD
    elif kind == ingest.ProviderKind.HYPERSYNC:
        return ingest.ProviderKind.HYPERSYNC
    else:
        raise Exception("invalid provider kind")


async def load_evm_config() -> EvmConfig:
    """Load EVM configuration from environment variables."""

    provider_kind = _to_provider_kind(os.environ["CHERRY_EVM_PROVIDER_KIND"])
    chain_id = int(os.environ["CHERRY_EVM_CHAIN_ID"])
    if provider_kind == ingest.ProviderKind.SQD:
        _ = SQDChainId.get_sqd_name(chain_id=chain_id)
    elif provider_kind == ingest.ProviderKind.HYPERSYNC:
        _ = HyperSyncChainId.get_hypersync_name(chain_id=chain_id)
    rpc_provider_url = os.environ["RPC_PROVIDER_URL"]
    provider = make_evm_provider(provider_kind, chain_id)
    client = await connect_evm()

    return EvmConfig(
        from_block=_to_int_with_default(os.environ.get("CHERRY_FROM_BLOCK"), 0),
        to_block=_to_int(os.environ.get("CHERRY_TO_BLOCK")),
        provider=provider,
        chain_id=chain_id,
        client=client,
        rpc_provider_url=rpc_provider_url,
    )


async def load_svm_config() -> SvmConfig:
    """Load SVM configuration from environment variables."""

    client = await connect_svm()
    provider = make_svm_provider()

    dataset_start_block = get_solana_start_block()

    logger.info(f"Solana dataset start block is: {dataset_start_block}")

    from_block = _to_int_with_default(os.environ.get("CHERRY_FROM_BLOCK"), 0)
    from_block = max(dataset_start_block, from_block)

    return SvmConfig(
        from_block=from_block,
        to_block=_to_int(os.environ.get("CHERRY_TO_BLOCK")),
        client=client,
        provider=provider,
    )


async def _connect(db_name: str) -> AsyncClient:
    return await clickhouse_connect.get_async_client(
        host=os.environ.get("CLICKHOUSE_HOST", "127.0.0.1"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=db_name,
    )


async def connect_evm() -> AsyncClient:
    return await _connect("evm")


async def connect_svm() -> AsyncClient:
    return await _connect("svm")


_EVM_PIPELINES: dict[str, Callable[[EvmConfig], Awaitable[cc.Pipeline]]] = {
    "erc20_transfers": evm.erc20_transfers.make_pipeline,
    "circle_usdc_v1_ethereum": lambda cfg: evm.stablecoin_factory.make_pipeline(
        cfg, "circle_usdc_v1_ethereum"
    ),
    "curve_finance_crvusd_ethereum_v1_ethereum": lambda cfg: evm.stablecoin_factory.make_pipeline(
        cfg, "curve_finance_crvusd_ethereum_v1_ethereum"
    ),
    "ethena_usde_v1_ethereum": lambda cfg: evm.stablecoin_factory.make_pipeline(
        cfg, "ethena_usde_v1_ethereum"
    ),
    "first_digital_fdusd_v1_ethereum": lambda cfg: evm.stablecoin_factory.make_pipeline(
        cfg, "first_digital_fdusd_v1_ethereum"
    ),
    "frax_finance_frax_v1_ethereum": lambda cfg: evm.stablecoin_factory.make_pipeline(
        cfg, "frax_finance_frax_v1_ethereum"
    ),
    "liquity_lusd_v1_ethereum": lambda cfg: evm.stablecoin_factory.make_pipeline(
        cfg, "liquity_lusd_v1_ethereum"
    ),
    "maker_dai_v1_ethereum": lambda cfg: evm.stablecoin_factory.make_pipeline(
        cfg, "maker_dai_v1_ethereum"
    ),
    "paxos_usdp_v1_ethereum": lambda cfg: evm.stablecoin_factory.make_pipeline(
        cfg, "paxos_usdp_v1_ethereum"
    ),
    "paypal_pyusd_v1_ethereum": lambda cfg: evm.stablecoin_factory.make_pipeline(
        cfg, "paypal_pyusd_v1_ethereum"
    ),
    "decentralized_usdd_v1_ethereum": lambda cfg: evm.stablecoin_factory.make_pipeline(
        cfg, "decentralized_usdd_v1_ethereum"
    ),
    "trueusd_tusd_v1_ethereum": lambda cfg: evm.stablecoin_factory.make_pipeline(
        cfg, "trueusd_tusd_v1_ethereum"
    ),
    "world_liberty_financial_usd1_v1_ethereum": lambda cfg: evm.stablecoin_factory.make_pipeline(
        cfg, "world_liberty_financial_usd1_v1_ethereum"
    ),
}

_SVM_PIPELINES = {}


async def main():
    load_dotenv()
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())

    pipeline_kind = os.environ["CHERRY_PIPELINE_KIND"]
    pipeline_names_str = os.environ["CHERRY_PIPELINE_NAME"].strip("[]")
    pipeline_names = [name.strip() for name in pipeline_names_str.split(",")]
    logger.info(f"Starting pipelines: {pipeline_names}")

    if pipeline_kind == "evm":
        cfg = await load_evm_config()
        if "all" in pipeline_names:
            # Run all pipelines concurrently
            pipeline_tasks = [
                _EVM_PIPELINES[name](cfg) for name in _EVM_PIPELINES.keys()
            ]
            pipelines = await asyncio.gather(*pipeline_tasks)
            # Run all pipelines
            await asyncio.gather(*(run_pipeline(p) for p in pipelines))
        else:
            # Run specified pipelines concurrently
            pipeline_tasks = [_EVM_PIPELINES[name](cfg) for name in pipeline_names]
            pipelines = await asyncio.gather(*pipeline_tasks)
            # Run all pipelines
            await asyncio.gather(*(run_pipeline(p) for p in pipelines))
    elif pipeline_kind == "svm":
        cfg = await load_svm_config()
        if "all" in pipeline_names:
            # Run all pipelines concurrently
            pipeline_tasks = [
                _SVM_PIPELINES[name](cfg) for name in _SVM_PIPELINES.keys()
            ]
            pipelines = await asyncio.gather(*pipeline_tasks)
            # Run all pipelines
            await asyncio.gather(*(run_pipeline(p) for p in pipelines))
        else:
            # Run specified pipelines concurrently
            pipeline_tasks = [_SVM_PIPELINES[name](cfg) for name in pipeline_names]
            pipelines = await asyncio.gather(*pipeline_tasks)
            # Run all pipelines
            await asyncio.gather(*(run_pipeline(p) for p in pipelines))
    else:
        raise Exception("unknown CHERRY_PIPELINE_KIND, allowed values are evm and svm.")


if __name__ == "__main__":
    asyncio.run(main())
