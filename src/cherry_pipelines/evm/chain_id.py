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
        CREATE TABLE IF NOT EXISTS chain_id_table (
            chain_name String,
            chain_id UInt64,
            PRIMARY KEY chain_name
        ) ENGINE = EmbeddedRocksDB;
    """)

    data = []

    for id, name in EVM_CHAIN_NAME.items():
        data.append([name, id])

    await client.insert("chain_id_table", data, column_names=["chain_name", "chain_id"])

    await client.command("""
        CREATE DICTIONARY IF NOT EXISTS chain_id (
            chain_name String,
            chain_id UInt64
        ) PRIMARY KEY chain_name
        SOURCE(CLICKHOUSE(TABLE 'chain_id_table'))
        LAYOUT(DIRECT());
    """)
