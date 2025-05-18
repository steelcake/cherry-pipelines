# Read config from environment variables
# the code is copy-pasted and slightly modified for EVM/SVM

from dataclasses import dataclass
from typing import Optional
from cherry_core import ingest
from clickhouse_connect.driver.asyncclient import AsyncClient


@dataclass
class EvmConfig:
    provider: ingest.ProviderConfig
    from_block: int
    to_block: Optional[int]
    chain_id: int
    client: AsyncClient


@dataclass
class SvmConfig:
    provider: ingest.ProviderConfig
    from_block: int
    to_block: Optional[int]
    client: AsyncClient


EVM_CHAIN_NAME = {
    2741: "abstract",
    42161: "arbitrum",
    42170: "arbitrum_nova",
    421614: "arbitrum_sepolia",
    1313161554: "aurora",
    43114: "avalanche",
    8453: "base",
    84532: "base_sepolia",
    80094: "berachain",
    80084: "berachain_bartio",
    168587773: "blast_sepolia",
    288: "boba",
    56: "bsc",
    97: "bsc_testnet",
    42220: "celo",
    8888: "chiliz",
    5115: "citrea_testnet",
    33111: "curtis",
    7560: "cyber",
    1: "ethereum",
    250: "fantom",
    14: "flare",
    252: "fraxtal",
    43113: "fuji",
    696969: "galadriel_devnet",
    100: "gnosis",
    10200: "gnosis_chiado",
    1666600000: "harmony_shard_0",
    17000: "holesky",
    645749: "hyperliquid_evm",
    57073: "ink",
    255: "kroma",
    59144: "linea",
    1135: "lisk",
    42: "lukso",
    4201: "lukso_testnet",
    169: "manta",
    5000: "mantle",
    6342: "megaeth_testnet",
    4200: "merlin",
    1750: "metall2",
    17864: "mev_commit",
    34443: "mode",
    10143: "monad_testnet",
    1287: "moonbase_alpha",
    2818: "morph",
    2810: "morph_holesky",
    204: "opbnb",
    10: "optimism",
    11155420: "optimism_sepolia",
    50002: "pharos_devnet",
    137: "polygon",
    80002: "polygon_amoy",
    1101: "polygon_zkevm",
    30: "rootstock",
    7225878: "saakuru",
    534352: "scroll",
    11155111: "sepolia",
    148: "shimmer_evm",
    1868: "soneium",
    146: "sonic",
    50104: "sophon",
    531050104: "sophon_testnet",
    5330: "superseed",
    130: "unichain",
    1301: "unichain_sepolia",
    480: "worldchain",
    50: "xdc",
    51: "xdc_testnet",
    7000: "zeta",
    48900: "zirciut",
    324: "zksync",
    7777777: "zora",
}
