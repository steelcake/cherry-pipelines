import logging
import os
from dotenv import load_dotenv


def setup_env():
    load_dotenv()
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
