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
        if chain_id is not None:
            query += f" WHERE chain_id = {chain_id}"
        res = await client.query(query)
        max_block = int(res.result_rows[0][0] or 0)
        logger.info(f"max_block from db is {max_block}")
        return max_block + 1 if max_block > 0 else 0

    except Exception:
        logger.warning("failed to get start block from db")
        return 0


async def create_dict(
    client: AsyncClient, dict_name: str, field_definitions: list[str], primary_key: str
):
    field_defn = ",\n".join(field_definitions)

    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {dict_name}_table (
            {field_defn},
            PRIMARY KEY {primary_key}
        ) ENGINE = EmbeddedRocksDB()
        SETTINGS optimize_for_bulk_insert=0;
    """

    logger.info(
        f"Creating dict table named {dict_name}_table using sql:\n{create_table_sql}"
    )

    await client.command(create_table_sql)

    create_dict_sql = f"""
        CREATE DICTIONARY IF NOT EXISTS {dict_name} (
            {field_defn}
        ) PRIMARY KEY {primary_key} 
        SOURCE(CLICKHOUSE(TABLE '{dict_name}_table'))
        LAYOUT(DIRECT());
    """

    logger.info(f"Creating dict named {dict_name} using sql:\n{create_dict_sql}")

    await client.command(create_dict_sql)
