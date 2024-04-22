from ingestion.processor import IngestionProcessor, ProcessorBase
from ingestion.config import get_config_from_yaml
from pyspark.sql import SparkSession
import os
import json

EXPECTED_NUM_RECORDS = 32
EXPECTED_NUM_RECORDS_AFTER_REMOVAL = 31


def test_spark_running():
    yaml_path = os.path.join(os.path.dirname(__file__), "test_data", "test_config.yaml")
    config = get_config_from_yaml(yaml_path)

    processor = ProcessorBase(config)
    rdd = processor.spark.sparkContext.parallelize(range(1, 100))

    assert isinstance(processor.spark, SparkSession), "SparkSession instance is not created"
    assert rdd.sum() == 4950, f"Expected sum to be 4950, but got {rdd.sum()}"


def test_processor():
    yaml_path = os.path.join(os.path.dirname(__file__), "test_data", "test_config.yaml")
    config = get_config_from_yaml(yaml_path)
    with open(config.REVIEW_SCHEMA, 'r') as f:
        review_json_schema = json.load(f)
    
    processor = IngestionProcessor(config)

    df = processor._read_json()
    assert isinstance(processor, ProcessorBase), "Processor is not an instance of ProcessorBase"
    assert set(df.columns) == set(review_json_schema.get('required')), \
        f"Columns do not match: expected {review_json_schema.get('required')} but got {df.columns}"
    assert df.count() == EXPECTED_NUM_RECORDS, f"Expected {EXPECTED_NUM_RECORDS} rows, but got {df.count()}"

    df = processor._process_inapprorpiate_words(df)
    cencored_review = df.toPandas().loc[:, "text"].values[-1]
    assert ("Fracking" not in cencored_review) and ("poop" not in cencored_review), \
        f"Inappropriate word isn't cencored properly ({cencored_review})"
    assert df.count() == EXPECTED_NUM_RECORDS_AFTER_REMOVAL, (
        "Inappropriate record are not removed, "
        f"expected {EXPECTED_NUM_RECORDS_AFTER_REMOVAL} rows left, but got {df.count()}"
    )
