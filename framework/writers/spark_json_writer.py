import os
import logging
from .base import DataWriter

logger = logging.getLogger(__name__)

class SparkJsonWriter(DataWriter):
    def write(self, data, destination, mode="overwrite"):
        os.makedirs(os.path.dirname(os.path.abspath(destination)), exist_ok=True)
        data.write.mode(mode).json(destination)
        logger.info(f"Data written to {destination} (mode={mode})") 