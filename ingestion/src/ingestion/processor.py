from ingestion.spark_utils import get_spark_session, \
    _json_to_spark_schema
from ingestion.config import IngestionConfig
from pyspark.sql import DataFrame
from pyspark.sql import functions as sf
import json
import os
from delta.tables import DeltaTable


class Pipeline:
    """
    Run function in sequence with the output of previous function 
    as the input for the next function
    """
    def __init__(self, list_func: list) -> None:
        self.list_func = list_func
    
    def run(self, initial_args: any = None):
        input = initial_args
        for f in self.list_func:
            if input is None:
                input = f()
            else:
                input = f(input)
        return input


class ProcessorBase:
    def __init__(self, config: IngestionConfig) -> None:
        self.config = config
        self.spark = get_spark_session(config.SPARK_MASTER, config.SPARK_APP_NAME)
    
    def run(self):
        pass


class IngestionProcessor(ProcessorBase):
    """
    This class specifically expect the input data to be in jsonl format
    """
    def run(self):
        pipeline = Pipeline([
            self._read_json,
            self._cache_by_parquet,
            self._process_inapprorpiate_words,
            self._write_delta
        ])

        return pipeline.run()

    def _read_json(self) -> DataFrame:
        with open(self.config.REVIEW_SCHEMA, 'r') as f:
            review_json_schema = json.load(f)

        schema = _json_to_spark_schema(review_json_schema)
        reader = self.spark.read.schema(schema)

        if ".jsonl" in self.config.INPUT_PATH:
            df = reader.json(self.config.INPUT_PATH)
        else:
            df = reader.json(self.config.INPUT_PATH + "/" + "*.jsonl")
        return df
    
    def _cache_by_parquet(self, df: DataFrame) -> str:
        """
        Store the data in parquet format to make processing more efficient 
        """
        tmp_output_path = os.path.join(self.config.OUTPUT_PATH, "tmp")
        df.write.mode("overwrite").parquet(tmp_output_path)
        return self.spark.read.parquet(tmp_output_path)
    
    def _process_inapprorpiate_words(self, df: DataFrame) -> DataFrame:
        words = (
            self.spark.read.text(self.config.INAPPROPRIATE_WORDS_PATH)
            .withColumn("length", sf.length("value"))
            # sort by word length, so longer word will be prioritised (e.g. fracking > frack)
            .orderBy(sf.col("length").desc())
            .select(
                sf.concat(
                    sf.lit("((?i)"),
                    sf.array_join(sf.array_agg("value").alias("words"), "|(?i)"),
                    sf.lit(")")
                ).alias("replace_regex_args"),
                sf.lit(1).alias("dummy_jk")
            )
        )

        df = df.withColumn("dummy_jk", sf.lit(1)).join(words, "dummy_jk").drop("dummy_jk")
        df = df.withColumn("inappropruate_words_found", sf.regexp_extract_all("text", df.replace_regex_args))
        df = df.withColumn("text_word_count", sf.size(sf.split("text", "\s")))
        df = df.withColumn("inappropriate_words_length", sf.size("inappropruate_words_found"))
        df = df.withColumn("perc_inappropriate", df.inappropriate_words_length / df.text_word_count)
        df = df.withColumn("text", sf.regexp_replace("text", df.replace_regex_args, "****"))

        return df
    
    def _write_delta(self, df: DataFrame) -> None:
        table_path = os.path.join(self.config.OUTPUT_PATH, 'final')
        with open(self.config.REVIEW_SCHEMA, 'r') as f:
            select_columns = json.load(f).get("required")

        df = df.select(*select_columns).withColumn("partition_date", sf.to_date("publishedAt"))
        df.cache()
        min_dt, max_dt = df.selectExpr("min(partition_date) as min_dt", "max(partition_date) as max_dt").toPandas().values[0]

        DELTA_TABLE_EXIST = DeltaTable.isDeltaTable(self.spark, table_path)
        if DELTA_TABLE_EXIST:
            merge_args = [f"old.`{_id}` = new.`{_id}`" for _id in self.config.PARAMS.get("unique_ids")]

            table = DeltaTable.forPath(self.spark, table_path)
            table.alias("old").merge(
                df.alias("new"),
                " and ".join(merge_args) 
                + " and old.partition_date between date '{}' and date '{}'".format(min_dt, max_dt)
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        else:
            df.write.format("delta").partitionBy("partition_date").save(table_path)
