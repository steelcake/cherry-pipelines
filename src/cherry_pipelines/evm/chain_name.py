from clickhouse_connect.driver.asyncclient import AsyncClient
from ..config import (
    EVM_CHAIN_NAME,
    EvmConfig,
)
import logging

from .pipeline import EvmPipeline

logger = logging.getLogger(__name__)


class Pipeline(EvmPipeline):
    async def run(self, cfg: EvmConfig):
        _ = cfg
        pass

    async def init_db(self, client: AsyncClient):
        await init_db(client)


async def init_db(client: AsyncClient):
    await client.command("""
        CREATE TABLE IF NOT EXISTS chain_name_table (
            chain_id UInt64,
            chain_name String,
            PRIMARY KEY chain_id 
        ) ENGINE = EmbeddedRocksDB;
    """)

    data = []

    for id, name in EVM_CHAIN_NAME.items():
        data.append([id, name])

    await client.insert(
        "chain_name_table", data, column_names=["chain_id", "chain_name"]
    )

    await client.command("""
        CREATE DICTIONARY IF NOT EXISTS chain_name (
            chain_id UInt64,
            chain_name String
        ) PRIMARY KEY chain_id 
        SOURCE(CLICKHOUSE(TABLE 'chain_name_table'))
        LAYOUT(DIRECT());
    """)
