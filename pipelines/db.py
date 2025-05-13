import clickhouse_connect
from clickhouse_connect.driver.asyncclient import AsyncClient
import os
import logging

logger = logging.getLogger(__name__)


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


async def get_start_block(
    client: AsyncClient, table_name: str, column_name: str
) -> int:
    try:
        res = await client.query(f"SELECT MAX({column_name}) FROM {table_name}")
        return res.result_rows[0][0] or 0
    except Exception:
        logger.warning("failed to get start block from db")
        return 0
