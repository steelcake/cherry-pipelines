import logging
from clickhouse_connect.driver.asyncclient import AsyncClient
import clickhouse_connect
import os
from dotenv import load_dotenv
import asyncio
from cherry_core import ingest
from cherry_pipelines.config import EvmConfig, SvmConfig
from typing import Awaitable, Callable, Optional
import requests
from cherry_pipelines import evm
from cherry_etl import config as cc, run_pipeline

logger = logging.getLogger(__name__)

# https://docs.sqd.ai/subsquid-network/reference/networks/
_SQD_EVM_CHAIN_NAME = {
    16600: "0g-testnet",
    2741: "abstract-mainnet",
    11124: "abstract-testnet",
    9990: "agung-evm",
    41455: "aleph-zero-evm-mainnet",
    42170: "arbitrum-nova",
    42161: "arbitrum-one",
    10242: "arthera-mainnet",
    592: "astar-mainnet",
    43114: "avalanche-mainnet",
    43113: "avalanche-testnet",
    8333: "b3-mainnet",
    1993: "b3-sepolia",
    8453: "base-mainnet",
    84532: "base-sepolia",
    80084: "berachain-bartio",
    80094: "berachain-mainnet",
    56: "binance-mainnet",
    97: "binance-testnet",
    355110: "bitfinity-mainnet",
    355113: "bitfinity-testnet",
    64668: "bitgert-testnet",
    964: "bittensor-mainnet-evm",
    945: "bittensor-testnet-evm",
    81457: "blast-l2-mainnet",
    168587773: "blast-sepolia",
    60808: "bob-mainnet",
    808813: "bob-sepolia",
    325000: "camp-network-testnet-v2",
    7700: "canto",
    7701: "canto-testnet",
    44787: "celo-alfajores-testnet",
    42220: "celo-mainnet",
    1116: "core-mainnet",
    4158: "crossfi-mainnet",
    4157: "crossfi-testnet",
    7560: "cyber-mainnet",
    111557560: "cyberconnect-l2-testnet",
    666666666: "degen-chain",
    53935: "dfk-chain",
    2000: "dogechain-mainnet",
    568: "dogechain-testnet",
    17000: "ethereum-holesky",
    1: "ethereum-mainnet",
    11155111: "ethereum-sepolia",
    42793: "etherlink-mainnet",
    128123: "etherlink-testnet",
    2109: "exosama",
    250: "fantom-mainnet",
    4002: "fantom-testnet",
    14: "flare-mainnet",
    43521: "formicarium-testnet",
    1625: "galxe-gravity",
    88153591557: "gelato-arbitrum-blueberry",
    100: "gnosis-mainnet",
    999: "hyperliquid-mainnet",
    998: "hyperliquid-testnet",
    13371: "immutable-zkevm-mainnet",
    13473: "immutable-zkevm-testnet",
    57073: "ink-mainnet",
    763373: "ink-sepolia",
    1998: "kyoto-testnet",
    59144: "linea-mainnet",
    42: "ozean-testnet",
    169: "manta-pacific",
    3441006: "manta-pacific-sepolia",
    5000: "mantle-mainnet",
    5003: "mantle-sepolia",
    6342: "mega-testnet",
    4352: "memecore-mainnet",
    4200: "merlin-mainnet",
    686868: "merlin-testnet",
    34443: "mode-mainnet",
    10143: "monad-testnet",
    1287: "moonbase-testnet",
    1284: "moonbeam-mainnet",
    1285: "moonriver-mainnet",
    42225: "nakachain",
    245022926: "neon-devnet",
    245022934: "neon-mainnet",
    204: "opbnb-mainnet",
    5611: "opbnb-testnet",
    11155420: "optimism-sepolia",
    3338: "peaq-mainnet",
    98866: "plume",
    98864: "plume-devnet",
    98865: "plume-legacy",
    98867: "plume-testnet",
    80002: "polygon-amoy-testnet",
    137: "polygon-mainnet",
    2442: "polygon-zkevm-cardona-testnet",
    1101: "polygon-zkevm-mainnet",
    31911: "poseidon-testnet",
    227: "prom-mainnet",
    157: "puppynet",
    11155931: "rise-sepolia",
    534352: "scroll-mainnet",
    534351: "scroll-sepolia",
    109: "shibarium",
    81: "shibuya-testnet",
    336: "shiden-mainnet",
    1482601649: "skale-nebula",
    1868: "soneium-mainnet",
    1946: "soneium-minato-testnet",
    57054: "sonic-blaze-testnet",
    146: "sonic-mainnet",
    64165: "sonic-testnet",
    93747: "stratovm-sepolia",
    5330: "superseed-mainnet",
    53302: "superseed-sepolia",
    167000: "taiko-mainnet",
    5678: "tanssi",
    130: "unichain-mainnet",
    1301: "unichain-sepolia",
    196: "xlayer-mainnet",
    195: "xlayer-testnet",
    810180: "zklink-nova-mainnet",
    300: "zksync-sepolia",
    7777777: "zora-mainnet",
    999999999: "zora-sepolia",
}


def make_evm_provider(
    provider_kind: ingest.ProviderKind, chain_id: int
) -> ingest.ProviderConfig:
    """Create an evm provider config based on `config.ProviderKind` and `config.chain_id`"""
    url = ""
    if provider_kind == ingest.ProviderKind.HYPERSYNC:
        url = f"https://{chain_id}.hypersync.xyz"
    elif provider_kind == ingest.ProviderKind.SQD:
        url = f"https://portal.sqd.dev/datasets/{_SQD_EVM_CHAIN_NAME[chain_id]}"

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

    provider = make_evm_provider(provider_kind, chain_id)
    client = await connect_evm()

    return EvmConfig(
        from_block=_to_int_with_default(os.environ.get("CHERRY_FROM_BLOCK"), 0),
        to_block=_to_int(os.environ.get("CHERRY_TO_BLOCK")),
        provider=provider,
        chain_id=chain_id,
        client=client,
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
}

_SVM_PIPELINES = {}


async def main():
    load_dotenv()
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())

    pipeline_kind = os.environ["CHERRY_PIPELINE_KIND"]
    pipeline_name = os.environ["CHERRY_PIPELINE_NAME"]

    pipeline = None
    if pipeline_kind == "evm":
        cfg = await load_evm_config()
        pipeline = await _EVM_PIPELINES[pipeline_name](cfg)
    elif pipeline_kind == "svm":
        cfg = await load_svm_config()
        pipeline = await _SVM_PIPELINES[pipeline_name](cfg)
    else:
        raise Exception("unknown CHERRY_PIPELINE_KIND, allowed values are evm and svm.")

    logger.info(f"Running pipeline with config: {pipeline}")
    await run_pipeline(pipeline)


if __name__ == "__main__":
    asyncio.run(main())
