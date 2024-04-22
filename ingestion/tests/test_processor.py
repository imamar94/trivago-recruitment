from ingestion.processor import ProcessorBase
from ingestion.config import IngestionConfig
from pyspark.sql import SparkSession


def test_spark_running():
    config = IngestionConfig()
    processor = ProcessorBase(config)
    rdd = processor.spark.sparkContext.parallelize(range(1, 100))
    assert isinstance(processor.spark, SparkSession)
    assert rdd.sum() == 4950


def test_processor():
    config = IngestionConfig()
    processor = ProcessorBase(config)
    assert isinstance(processor, ProcessorBase)
