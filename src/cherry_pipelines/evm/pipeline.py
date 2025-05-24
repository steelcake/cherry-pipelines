from abc import ABC, abstractmethod
from typing import Optional
from clickhouse_connect.driver.asyncclient import AsyncClient

from cherry_pipelines.config import EvmConfig


class EvmPipeline(ABC):
    @abstractmethod
    async def init_db(self, client: AsyncClient, pipeline_name: Optional[str] = None):
        """initialize database tables/dictionaries etc."""
        pass

    @abstractmethod
    async def run(self, cfg: EvmConfig, pipeline_name: Optional[str] = None):
        """run the pipeline config using the given config."""
        pass
