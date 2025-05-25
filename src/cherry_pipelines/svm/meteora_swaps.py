from copy import deepcopy
from clickhouse_connect.driver.asyncclient import AsyncClient
from cherry_etl import config as cc, run_pipeline
from cherry_etl.utils import svm_anchor_discriminator
from cherry_core import ingest, base58_decode_string
import logging
from typing import Dict, Any
import polars as pl
from cherry_core.svm_decode import (
    InstructionSignature,
    DynType,
    ParamInput,
)


from .. import db
from .common_signatures import (
    _TOKEN_PROGRAM_ID,
    _TOKEN_2022_PROGRAM_ID,
    _MEMO_PROGRAM_ID_V1,
    _MEMO_PROGRAM_ID_V2,
    _TOKEN_TRANSFER_SIGNATURE,
    _TOKEN_TRANSFER_DISCRIMINATOR,
    _TOKEN_TRANSFER_CHECKED_SIGNATURE,
    _TOKEN_TRANSFER_CHECKED_DISCRIMINATOR,
)
from ..config import (
    SvmConfig,
)

from .pipeline import SvmPipeline

logger = logging.getLogger(__name__)


class Pipeline(SvmPipeline):
    async def run(self, cfg: SvmConfig):
        await run(cfg)

    async def init_db(self, client: AsyncClient):
        await init_db(client)


async def init_db(client: AsyncClient):
    pass


async def run(cfg: SvmConfig):
    pass
