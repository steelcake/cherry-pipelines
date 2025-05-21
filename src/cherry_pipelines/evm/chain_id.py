from clickhouse_connect.driver.asyncclient import AsyncClient
from ..config import (
    EVM_CHAIN_NAME,
    EvmConfig,
)
import logging

from .pipeline import EvmPipeline
from ..db import create_dict

logger = logging.getLogger(__name__)


class Pipeline(EvmPipeline):
    async def run(self, cfg: EvmConfig):
        _ = cfg
        pass

    async def init_db(self, client: AsyncClient):
        await init_db(client)


_DICT_NAME = "chain_id"


async def init_db(client: AsyncClient):
    await create_dict(
        client,
        _DICT_NAME,
        [
            "chain_name String",
            "chain_id UInt64",
        ],
        primary_key="chain_name",
    )

    data = []

    for id, name in EVM_CHAIN_NAME.items():
        data.append([name, id])

    await client.insert(
        f"{_DICT_NAME}_table", data, column_names=["chain_name", "chain_id"]
    )
