from pyspark.sql import SparkSession
from .base import DataReader

class SparkJsonReader(DataReader):
    def __init__(self, multiline=True):
        self.spark = SparkSession.builder.appName("GenericDataPipeline").getOrCreate()
        self.multiline = multiline

    def read(self, source):
        return self.spark.read.option("multiline", str(self.multiline).lower()).json(source) 