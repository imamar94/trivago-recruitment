from ingestion.processor import IngestionProcessor, ProcessorBase, Pipeline, AggregationProcessor
from ingestion.config import get_config_from_yaml
from pyspark.sql import SparkSession, functions as sf
import os
import json

EXPECTED_NUM_RECORDS = 32
EXPECTED_NUM_RECORDS_AFTER_REMOVAL = 31
EXPECTED_RECORDS_AFTER_AGG = 14


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

    df_dedup = processor._deduplicate_by_unique_ids(df)
    assert df_dedup.count() == df.select(*config.PARAMS.get("unique_ids")).distinct().count(), \
        "Found duplicated record based on unique_ids parameters"

def test_aggregation():
    yaml_path = os.path.join(os.path.dirname(__file__), "test_data", "test_config.yaml")
    config = get_config_from_yaml(yaml_path)
    with open(config.AGGREGATION_SCHEMA, 'r') as f:
        agg_json_schema = json.load(f)
    required_column = agg_json_schema.get("required")
    
    processor = IngestionProcessor(config)
    agg = AggregationProcessor(config)
    pipeline = Pipeline([
        processor._read_json,
        processor._process_inapprorpiate_words,
        lambda df: df.withColumn("partition_date", sf.to_date("publishedAt")),
        agg._filter_old_review,
        agg._agg
    ])
    df_agg = pipeline.run()

    assert set(df_agg.columns) == set(required_column), \
        f"Columns do not match: expected {required_column} but got {df_agg.columns}"
    assert df_agg.count() == EXPECTED_RECORDS_AFTER_AGG, \
        f"Count of record after aggregation doesn't match: expected {EXPECTED_RECORDS_AFTER_AGG} but got {df_agg.count()}"
