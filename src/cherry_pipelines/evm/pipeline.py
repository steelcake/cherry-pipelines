from abc import ABC, abstractmethod

from clickhouse_connect.driver.asyncclient import AsyncClient

from cherry_pipelines.config import EvmConfig


class EvmPipeline(ABC):
    @abstractmethod
    async def init_db(self, client: AsyncClient, pipeline_name: str):
        """initialize database tables/dictionaries etc."""
        pass

    @abstractmethod
    async def run(self, cfg: EvmConfig, pipeline_name: str):
        """run the pipeline config using the given config."""
        pass
