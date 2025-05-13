# Read config from environment variables
# the code is copy-pasted and slightly modified for EVM/SVM

import os
from dataclasses import dataclass
from typing import Optional
from cherry_core import ingest
import requests


@dataclass
class EvmConfig:
    provider_kind: ingest.ProviderKind
    from_block: int
    to_block: Optional[int]
    chain_id: int


@dataclass
class SvmConfig:
    from_block: int
    to_block: Optional[int]


EVM_DB_NAME = "evm"
SVM_DB_NAME = "svm"

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


def make_evm_table_name(base_name: str, chain_id: int) -> str:
    return f"{base_name}_chain{chain_id}"


def make_evm_provider(config: EvmConfig) -> ingest.ProviderConfig:
    """Create a evm provider config based on `config.ProviderKind` and `config.chain_id`"""
    url = ""
    if config.provider_kind == ingest.ProviderKind.HYPERSYNC:
        url = f"https://{config.chain_id}.hypersync.xyz"
    elif config.provider_kind == ingest.ProviderKind.SQD:
        url = f"https://portal.sqd.dev/datasets/{_SQD_EVM_CHAIN_NAME[config.chain_id]}"

    return ingest.ProviderConfig(
        kind=config.provider_kind,
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


def load_evm_config() -> EvmConfig:
    """Load EVM configuration from environment variables."""
    return EvmConfig(
        provider_kind=_to_provider_kind(os.environ["CHERRY_EVM_PROVIDER_KIND"]),
        from_block=_to_int_with_default(os.environ.get("CHERRY_FROM_BLOCK"), 0),
        to_block=_to_int(os.environ.get("CHERRY_TO_BLOCK")),
        chain_id=int(os.environ["CHERRY_EVM_CHAIN_ID"]),
    )


def load_svm_config() -> SvmConfig:
    """Load SVM configuration from environment variables."""
    return SvmConfig(
        from_block=_to_int_with_default(os.environ.get("CHERRY_FROM_BLOCK"), 0),
        to_block=_to_int(os.environ.get("CHERRY_TO_BLOCK")),
    )
