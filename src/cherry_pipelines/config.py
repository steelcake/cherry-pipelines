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


def make_evm_table_name(base_name: str, chain_id: int) -> str:
    return f"{base_name}_chain{chain_id}"
