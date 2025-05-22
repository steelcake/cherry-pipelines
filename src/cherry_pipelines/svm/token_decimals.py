from clickhouse_connect.driver.asyncclient import AsyncClient
from ..config import (
    SvmConfig,
)
import logging

from .pipeline import SvmPipeline
from ..db import create_dict

logger = logging.getLogger(__name__)


class Pipeline(SvmPipeline):
    async def run(self, cfg: SvmConfig):
        _ = cfg
        pass

    async def init_db(self, client: AsyncClient):
        await init_db(client)


_DICT_NAME = "token_decimals"


async def init_db(client: AsyncClient):
    await create_dict(
        client,
        _DICT_NAME,
        [
            "mint String",
            "decimals UInt16",
        ],
        primary_key="mint",
    )
