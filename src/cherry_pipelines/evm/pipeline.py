from abc import ABC, abstractmethod
from cherry_etl import config as cc

from clickhouse_connect.driver.asyncclient import AsyncClient

from cherry_pipelines.config import EvmConfig


class EvmPipeline(ABC):
    @abstractmethod
    async def init_db(self, client: AsyncClient, chain_id: int):
        """initialize database tables/dictionaries etc."""
        pass

    @abstractmethod
    async def make_pipeline(self, cfg: EvmConfig) -> cc.Pipeline:
        """create a pipeline config based on the given config"""
        pass
