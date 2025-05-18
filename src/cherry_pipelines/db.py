from clickhouse_connect.driver.asyncclient import AsyncClient
from typing import Optional
import logging

logger = logging.getLogger(__name__)


async def get_next_block(
    client: AsyncClient, table_name: str, column_name: str, chain_id: Optional[int]
) -> int:
    """Gets next block to ingest based on max block number stored in the database."""
    try:
        query = f"SELECT MAX({column_name}) FROM {table_name}"
        if chain_id is None:
            query += f" WHERE chain_id = {chain_id}"
        res = await client.query(query)
        max_block = int(res.result_rows[0][0] or 0)
        logger.info(f"max_block from db is {max_block}")
        return max_block + 1 if max_block > 0 else 0

    except Exception:
        logger.warning("failed to get start block from db")
        return 0
