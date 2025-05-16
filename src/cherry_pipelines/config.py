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
    rpc_provider_url: str


@dataclass
class SvmConfig:
    provider: ingest.ProviderConfig
    from_block: int
    to_block: Optional[int]
    client: AsyncClient


class ChainId:
    _id_to_name = {
        1: "ethereum",
        10: "optimism",
        14: "flare",
        30: "rootstock",
        42: "ozean-testnet",
        44: "crab",
        46: "darwinia",
        50: "xdc",
        51: "xdc-testnet",
        56: "binance",
        81: "shibuya-testnet",
        97: "binance-testnet",
        100: "gnosis",
        109: "shibarium",
        130: "unichain",
        137: "polygon",
        146: "sonic",
        148: "shimmer-evm",
        157: "puppynet",
        169: "manta-pacific",
        195: "xlayer-testnet",
        196: "xlayer",
        204: "opbnb",
        227: "prom",
        250: "fantom",
        252: "fraxtal",
        255: "kroma",
        288: "boba",
        324: "zksync",
        300: "zksync-sepolia",
        336: "shiden",
        480: "worldchain",
        568: "dogechain-testnet",
        592: "astar",
        945: "bittensor-testnet-evm",
        964: "bittensor-evm",
        998: "hyperliquid-testnet",
        999: "hyperliquid",
        1088: "metis",
        1101: "polygon-zkevm",
        1116: "core",
        1135: "lisk",
        1284: "moonbeam",
        1285: "moonriver",
        1287: "moonbase-testnet",
        1301: "unichain-sepolia",
        1625: "galxe-gravity",
        1750: "metall2",
        1868: "soneium",
        1946: "soneium-minato-testnet",
        1993: "b3-sepolia",
        1998: "kyoto-testnet",
        2000: "dogechain",
        2109: "exosama",
        2442: "polygon-zkevm-cardona-testnet",
        2741: "abstract",
        2818: "morph",
        3338: "peaq",
        4002: "fantom-testnet",
        4157: "crossfi-testnet",
        4158: "crossfi",
        4200: "merlin",
        4352: "memecore",
        5000: "mantle",
        5003: "mantle-sepolia",
        5330: "superseed",
        5611: "opbnb-testnet",
        5678: "tanssi",
        6342: "mega-testnet",
        7560: "cyber",
        7700: "canto",
        7701: "canto-testnet",
        8333: "b3",
        8453: "base",
        9990: "agung-evm",
        10143: "monad-testnet",
        10242: "arthera",
        11124: "abstract-testnet",
        13371: "immutable-zkevm",
        13473: "immutable-zkevm-testnet",
        16600: "0g-testnet",
        17000: "ethereum-holesky",
        31911: "poseidon-testnet",
        34443: "mode",
        41455: "aleph-zero-evm",
        42161: "arbitrum-one",
        42170: "arbitrum-nova",
        42220: "celo",
        42225: "nakachain",
        42793: "etherlink",
        43113: "avalanche-testnet",
        43114: "avalanche",
        43521: "formicarium-testnet",
        44787: "celo-alfajores-testnet",
        53302: "superseed-sepolia",
        53935: "dfk-chain",
        57054: "sonic-blaze-testnet",
        57073: "ink",
        59144: "linea",
        60808: "bob",
        64165: "sonic-testnet",
        64668: "bitgert-testnet",
        80002: "polygon-amoy-testnet",
        80084: "berachain-bartio",
        80094: "berachain",
        81457: "blast-l2",
        84532: "base-sepolia",
        93747: "stratovm-sepolia",
        98864: "plume-devnet",
        98865: "plume-legacy",
        98866: "plume",
        98867: "plume-testnet",
        128123: "etherlink-testnet",
        167000: "taiko",
        325000: "camp-network-testnet-v2",
        355110: "bitfinity",
        355113: "bitfinity-testnet",
        534351: "scroll-sepolia",
        534352: "scroll",
        645749: "hyperliquid",
        686868: "merlin-testnet",
        763373: "ink-sepolia",
        808813: "bob-sepolia",
        810180: "zklink-nova",
        3441006: "manta-pacific-sepolia",
        7777777: "zora",
        11155111: "ethereum-sepolia",
        11155420: "optimism-sepolia",
        11155931: "rise-sepolia",
        111557560: "cyberconnect-l2-testnet",
        168587773: "blast-sepolia",
        245022926: "neon-devnet",
        245022934: "neon",
        666666666: "degen-chain",
        999999999: "zora-sepolia",
        1482601649: "skale-nebula",
        88153591557: "gelato-arbitrum-blueberry",
    }
    _name_to_id = {v: k for k, v in _id_to_name.items()}

    @classmethod
    def get_name(cls, chain_id: int) -> str:
        name = cls._id_to_name.get(chain_id)
        if name is None:
            raise ValueError(f"Chain ID {chain_id} not found in ChainId")
        return name

    @classmethod
    def get_id(cls, chain_name: str) -> int:
        id = cls._name_to_id.get(chain_name)
        if id is None:
            raise ValueError(f"Chain name {chain_name} not found in ChainId")
        return id

    @classmethod
    def all_chains(cls) -> dict:
        return cls._id_to_name.copy()


class SQDChainId:
    _id_to_name = {
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
    _name_to_id = {v: k for k, v in _id_to_name.items()}

    @classmethod
    def get_sqd_name(cls, chain_id: int) -> str:
        name = cls._id_to_name.get(chain_id)
        if name is None:
            raise ValueError(f"Chain ID {chain_id} not found in SQDChainId")
        return name

    @classmethod
    def get_sqd_id(cls, chain_name: str) -> int:
        id = cls._name_to_id.get(chain_name)
        if id is None:
            raise ValueError(f"Chain name {chain_name} not found in SQDChainId")
        return id

    @classmethod
    def all_sqd_chains(cls) -> dict:
        return cls._id_to_name.copy()


class HyperSyncChainId:
    _id_to_name = {
        1: "Ethereum Mainnet",
        10: "Optimism",
        14: "Flare",
        30: "Rootstock",
        42: "Lukso",
        50: "Xdc",
        51: "Xdc Testnet",
        56: "Bsc",
        97: "Bsc Testnet",
        100: "Gnosis",
        130: "Unichain",
        137: "Polygon",
        146: "Sonic",
        148: "Shimmer Evm",
        169: "Manta",
        204: "Opbnb",
        250: "Fantom",
        252: "Fraxtal",
        255: "Kroma",
        288: "Boba",
        324: "ZKsync",
        480: "Worldchain",
        1101: "Polygon zkEVM",
        1135: "Lisk",
        1287: "Moonbase Alpha",
        1301: "Unichain Sepolia",
        1750: "Metall2",
        1868: "Soneium",
        2741: "Abstract",
        2810: "Morph Holesky",
        2818: "Morph",
        4200: "Merlin",
        4201: "Lukso Testnet",
        5000: "Mantle",
        5115: "Citrea Testnet",
        5330: "Superseed",
        6342: "Megaeth Testnet",
        7000: "Zeta",
        7560: "Cyber",
        8453: "Base",
        8888: "Chiliz",
        10143: "Monad Testnet",
        10200: "Gnosis Chiado",
        17000: "Holesky",
        17864: "Mev Commit",
        33111: "Curtis",
        34443: "Mode",
        42161: "Arbitrum",
        42170: "Arbitrum Nova",
        42220: "Celo",
        43113: "Fuji",
        43114: "Avalanche",
        48900: "Zircuit",
        50002: "Pharos Devnet",
        50104: "Sophon",
        57073: "Ink",
        59144: "Linea",
        80002: "Polygon Amoy",
        80084: "Berachain Bartio",
        80094: "Berachain",
        84532: "Base Sepolia",
        421614: "Arbitrum Sepolia",
        534352: "Scroll",
        645749: "Hyperliquid",
        696969: "Galadriel Devnet",
        7225878: "Saakuru",
        7777777: "Zora",
        11155111: "Sepolia",
        11155420: "Optimism Sepolia",
        21155111: "Sepolia Temp Eip 7702",
        168587773: "Blast Sepolia",
        531050104: "Sophon Testnet",
        1313161554: "Aurora",
        1666600000: "Harmony Shard 0",
        10143333333: "Monad Testnet Backup",
    }
    _name_to_id = {v: k for k, v in _id_to_name.items()}

    @classmethod
    def get_hypersync_name(cls, chain_id: int) -> str:
        name = cls._id_to_name.get(chain_id)
        if name is None:
            raise ValueError(f"Chain ID {chain_id} not found in HyperSyncChainId")
        return name

    @classmethod
    def get_hypersync_id(cls, chain_name: str) -> int:
        id = cls._name_to_id.get(chain_name)
        if id is None:
            raise ValueError(f"Chain name {chain_name} not found in HyperSyncChainId")
        return id

    @classmethod
    def all_hypersync_chains(cls) -> dict:
        return cls._id_to_name.copy()


def make_evm_table_name(slug: str, network: str, table: str) -> str:
    return f"{slug}_{network}_{table}"
