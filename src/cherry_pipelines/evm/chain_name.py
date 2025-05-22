from clickhouse_connect.driver.asyncclient import AsyncClient
from ..config import (
    EVM_CHAIN_NAME,
    EvmConfig,
)
import logging
from typing import Optional
from .pipeline import EvmPipeline
from ..db import create_dict

logger = logging.getLogger(__name__)


class Pipeline(EvmPipeline):
    async def run(self, cfg: EvmConfig, pipeline_name: Optional[str] = None):
        _ = cfg
        _ = pipeline_name
        pass

    async def init_db(self, client: AsyncClient, pipeline_name: Optional[str] = None):
        _ = pipeline_name
        await init_db(client)


_DICT_NAME = "chain_name"


async def init_db(client: AsyncClient):
    await create_dict(
        client,
        _DICT_NAME,
        [
            "chain_id UInt64",
            "chain_name String",
        ],
        primary_key="chain_id",
    )

    data = []

    for id, name in EVM_CHAIN_NAME.items():
        data.append([id, name])

    await client.insert(
        f"{_DICT_NAME}_table", data, column_names=["chain_id", "chain_name"]
    )
