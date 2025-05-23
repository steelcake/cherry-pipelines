import logging
from clickhouse_connect.driver.asyncclient import AsyncClient
import clickhouse_connect
import os
from dotenv import load_dotenv
import asyncio
from cherry_core import ingest
from cherry_pipelines.config import EVM_CHAIN_NAME, EvmConfig, SvmConfig
from typing import Optional
import requests
from cherry_pipelines import evm, svm

from cherry_pipelines.evm.pipeline import EvmPipeline

logger = logging.getLogger(__name__)

# https://docs.sqd.ai/subsquid-network/reference/networks/
_SQD_EVM_CHAIN_NAME = {
    1: "ethereum-mainnet",
    10: "optimism-mainnet",
    14: "flare-mainnet",
    30: "rootstock-mainnet",
    42: "ozean-testnet",
    44: "crab-mainnet",
    46: "darwinia-mainnet",
    50: "xdc-mainnet",
    51: "xdc-testnet",
    56: "binance-mainnet",
    81: "shibuya-testnet",
    97: "binance-testnet",
    100: "gnosis-mainnet",
    109: "shibarium",
    130: "unichain-mainnet",
    137: "polygon-mainnet",
    146: "sonic-mainnet",
    148: "shimmer-evm",
    157: "puppynet",
    169: "manta-pacific",
    195: "xlayer-testnet",
    196: "xlayer-mainnet",
    204: "opbnb-mainnet",
    227: "prom-mainnet",
    250: "fantom-mainnet",
    252: "fraxtal-mainnet",
    255: "kroma-mainnet",
    288: "boba-mainnet",
    324: "zksync-mainnet",
    300: "zksync-sepolia",
    336: "shiden-mainnet",
    480: "worldchain-mainnet",
    568: "dogechain-testnet",
    592: "astar-mainnet",
    945: "bittensor-testnet-evm",
    964: "bittensor-mainnet-evm",
    998: "hyperliquid-testnet",
    999: "hyperliquid-mainnet",
    1088: "metis-mainnet",
    1101: "polygon-zkevm-mainnet",
    1116: "core-mainnet",
    1135: "lisk-mainnet",
    1284: "moonbeam-mainnet",
    1285: "moonriver-mainnet",
    1287: "moonbase-testnet",
    1301: "unichain-sepolia",
    1625: "galxe-gravity",
    1750: "metall2-mainnet",
    1868: "soneium-mainnet",
    1946: "soneium-minato-testnet",
    1993: "b3-sepolia",
    1998: "kyoto-testnet",
    2000: "dogechain-mainnet",
    2109: "exosama",
    2442: "polygon-zkevm-cardona-testnet",
    2741: "abstract-mainnet",
    2818: "morph-mainnet",
    3338: "peaq-mainnet",
    4002: "fantom-testnet",
    4157: "crossfi-testnet",
    4158: "crossfi-mainnet",
    4200: "merlin-mainnet",
    4352: "memecore-mainnet",
    5000: "mantle-mainnet",
    5003: "mantle-sepolia",
    5330: "superseed-mainnet",
    5611: "opbnb-testnet",
    5678: "tanssi",
    6342: "mega-testnet",
    7560: "cyber-mainnet",
    7700: "canto",
    7701: "canto-testnet",
    8333: "b3-mainnet",
    8453: "base-mainnet",
    9990: "agung-evm",
    10143: "monad-testnet",
    10242: "arthera-mainnet",
    11124: "abstract-testnet",
    13371: "immutable-zkevm-mainnet",
    13473: "immutable-zkevm-testnet",
    16600: "0g-testnet",
    17000: "ethereum-holesky",
    31911: "poseidon-testnet",
    34443: "mode-mainnet",
    41455: "aleph-zero-evm-mainnet",
    42161: "arbitrum-one",
    42170: "arbitrum-nova",
    42220: "celo-mainnet",
    42225: "nakachain",
    42793: "etherlink-mainnet",
    43113: "avalanche-testnet",
    43114: "avalanche-mainnet",
    43521: "formicarium-testnet",
    44787: "celo-alfajores-testnet",
    53302: "superseed-sepolia",
    53935: "dfk-chain",
    57054: "sonic-blaze-testnet",
    57073: "ink-mainnet",
    59144: "linea-mainnet",
    60808: "bob-mainnet",
    64165: "sonic-testnet",
    64668: "bitgert-testnet",
    80002: "polygon-amoy-testnet",
    80084: "berachain-bartio",
    80094: "berachain-mainnet",
    81457: "blast-l2-mainnet",
    84532: "base-sepolia",
    93747: "stratovm-sepolia",
    98864: "plume-devnet",
    98865: "plume-legacy",
    98866: "plume",
    98867: "plume-testnet",
    128123: "etherlink-testnet",
    167000: "taiko-mainnet",
    325000: "camp-network-testnet-v2",
    355110: "bitfinity-mainnet",
    355113: "bitfinity-testnet",
    534351: "scroll-sepolia",
    534352: "scroll-mainnet",
    645749: "hyperliquid-mainnet",
    686868: "merlin-testnet",
    763373: "ink-sepolia",
    808813: "bob-sepolia",
    810180: "zklink-nova-mainnet",
    3441006: "manta-pacific-sepolia",
    7777777: "zora-mainnet",
    11155111: "ethereum-sepolia",
    11155420: "optimism-sepolia",
    11155931: "rise-sepolia",
    111557560: "cyberconnect-l2-testnet",
    168587773: "blast-sepolia",
    245022926: "neon-devnet",
    245022934: "neon-mainnet",
    666666666: "degen-chain",
    999999999: "zora-sepolia",
    1482601649: "skale-nebula",
    88153591557: "gelato-arbitrum-blueberry",
}

_DEFAULT_PROVIDER_BUFFER_SIZE = 2


def make_evm_provider(
    provider_kind: ingest.ProviderKind, chain_id: int, buffer_size: int
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
        buffer_size=buffer_size,
    )


# Use this because it has more fresh data and has block_number=block_slot
# whereas the solana-mainnet dataset is way behind and it has block_number=block_height
_SQD_SVM_URL = "https://portal.sqd.dev/datasets/solana-beta"


def make_svm_provider(buffer_size: int) -> ingest.ProviderConfig:
    """Create a solana provider config"""
    return ingest.ProviderConfig(
        kind=ingest.ProviderKind.SQD,
        url=_SQD_SVM_URL,
        buffer_size=buffer_size,
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
    provider_buffer_size = _to_int_with_default(
        os.environ.get("CHERRY_PROVIDER_BUFFER_SIZE"), _DEFAULT_PROVIDER_BUFFER_SIZE
    )

    provider = make_evm_provider(provider_kind, chain_id, provider_buffer_size)
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
    provider_buffer_size = _to_int_with_default(
        os.environ.get("CHERRY_PROVIDER_BUFFER_SIZE"), _DEFAULT_PROVIDER_BUFFER_SIZE
    )
    provider = make_svm_provider(provider_buffer_size)

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


_EVM_PIPELINES: dict[str, EvmPipeline] = {
    "erc20_transfers": evm.erc20_transfers.Pipeline(),
    "chain_name": evm.chain_name.Pipeline(),
    "chain_id": evm.chain_id.Pipeline(),
}

_SVM_PIPELINES = {
    "orca_swaps": svm.orca_swaps.Pipeline(),
    "orca_metadata": svm.orca_metadata.Pipeline(),
    "token_decimals": svm.token_decimals.Pipeline(),
    "raydium_swaps": svm.raydium_swaps.Pipeline(),
}


async def main():
    load_dotenv()
    logging.basicConfig(level=os.environ.get("PY_LOG", "INFO").upper())

    pipeline_kind = os.environ["CHERRY_PIPELINE_KIND"]
    pipeline_name = os.environ["CHERRY_PIPELINE_NAME"]

    is_init = os.environ.get("CHERRY_INIT_DB", "") == "true"

    if pipeline_kind == "evm":
        pp = _EVM_PIPELINES[pipeline_name]
        if not is_init:
            cfg = await load_evm_config()
            logger.info(
                f"Running pipeline {pipeline_name} on {EVM_CHAIN_NAME[cfg.chain_id]}"
            )
            await pp.run(cfg)
        else:
            logger.info("Running db init")
            await pp.init_db(await connect_evm())
    elif pipeline_kind == "svm":
        pp = _SVM_PIPELINES[pipeline_name]
        if not is_init:
            logger.info("Running pipeline")
            cfg = await load_svm_config()
            await pp.run(cfg)
        else:
            logger.info("Running db init")
            await pp.init_db(await connect_svm())
    else:
        raise Exception("unknown CHERRY_PIPELINE_KIND, allowed values are evm and svm.")


if __name__ == "__main__":
    asyncio.run(main())
