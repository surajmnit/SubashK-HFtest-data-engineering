import os
import sys

sys.path.append("C:/HD/HelloFresh/SubashK-HFtest-data-engineering/src")
from .logger import Logger
from .decorators import benchmark, with_logging


logger = Logger("HelloFresh Recipes Processing")
MSG = "LOGGER: Warning: LogObj not available. Using the latest version of the local logger available at utils/logger.py)"
logger.warning(MSG)
