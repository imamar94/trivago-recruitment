from ingestion.spark_utils import get_spark_session, \
    _json_to_spark_schema
from ingestion.config import IngestionConfig
from pyspark.sql import DataFrame
from pyspark.sql import functions as sf, Window
import json
import os
from delta.tables import DeltaTable
import datetime


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

    def save_to_delta_table(self, df: DataFrame, table_path: str, partition_col: str) -> None:
        min_dt, max_dt = df.selectExpr(f"min(`{partition_col}`) as min_dt", f"max(`{partition_col}`) as max_dt").toPandas().values[0]

        DELTA_TABLE_EXIST = DeltaTable.isDeltaTable(self.spark, table_path)
        if DELTA_TABLE_EXIST:
            merge_args = [f"coalesce(old.`{_id}`, -1) = coalesce(new.`{_id}`, -1)" 
                          for _id in self.config.PARAMS.get("unique_ids")]

            table = DeltaTable.forPath(self.spark, table_path)
            table.alias("old").merge(
                df.alias("new"),
                " and ".join(merge_args) 
                + f" and old.`{partition_col}` between date '{min_dt}' and date '{max_dt}'"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        else:
            df.write.format("delta").partitionBy(partition_col).save(table_path)


class IngestionProcessor(ProcessorBase):
    """
    This class specifically expect the input data to be in jsonl format
    """
    def run(self):
        pipeline = Pipeline([
            self._read_json,
            self._cache_by_parquet,
            self._process_inapprorpiate_words,
            self._deduplicate_by_unique_ids,
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
        review_column = self.config.PARAMS.get("review_column")
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
        df = df.withColumn("inappropruate_words_found", sf.regexp_extract_all(review_column, df.replace_regex_args))
        df = df.withColumn("text_length", sf.length(review_column))
        df = df.withColumn("inappropriate_length", sf.length(sf.array_join("inappropruate_words_found","")))
        df = df.withColumn("perc_inappropriate", df.inappropriate_length / df.text_length)
        df = df.withColumn(review_column, sf.expr(
            f"reduce(inappropruate_words_found, `{review_column}`, "
            f"(acc, x) -> regexp_replace(acc, x, repeat('*', length(x))))"
        ))

        # remove record that pass percentage of inappropriate word to total text
        df = df.filter("perc_inappropriate < {}".format(self.config.PARAMS.get("max_perc_inappropriate_words")))

        return df
    
    def _deduplicate_by_unique_ids(self, df: DataFrame) -> DataFrame:
        unique_ids = self.config.PARAMS.get("unique_ids")
        ts_col = self.config.PARAMS.get("timestamp_column")
        window = Window.partitionBy(*unique_ids).orderBy(sf.col(ts_col).desc())
        df = df.withColumn("rn", sf.row_number().over(window))
        df = df.filter("rn = 1").drop("rn")
        return df
    
    def _write_delta(self, df: DataFrame) -> str:
        table_path = os.path.join(self.config.OUTPUT_PATH, 'final')
        with open(self.config.REVIEW_SCHEMA, 'r') as f:
            select_columns = json.load(f).get("required")

        df = df.select(*select_columns).withColumn("partition_date", sf.to_date("publishedAt"))
        df.cache()
        self.save_to_delta_table(
            df, 
            table_path=table_path,
            partition_col="partition_date"
        )
        return "SUCCESS"

class AggregationProcessor(ProcessorBase):
    def run(self):
        pipeline = Pipeline([
            self._read_data,
            self._filter_old_review,
            self._agg,
            self._write
        ])

        return pipeline.run()

    def _read_data(self) -> DataFrame:
        table_path = os.path.join(self.config.OUTPUT_PATH, 'final')
        df = DeltaTable.forPath(self.spark, table_path).toDF()
        return df
    
    def _filter_old_review(self, df: DataFrame) -> DataFrame:
        df = df.filter("partition_date >= current_date - interval '3' year")
        return df
    
    def _agg(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(
            "reviewAge", 
            sf.expr("date_diff(DAY, date(publishedAt), current_date)")
        )
        df = df.groupBy("restaurantId").agg(
            sf.count("*").alias("reviewCount"),
            sf.mean("rating").alias("averageRating"),
            sf.mean(sf.length(self.config.PARAMS.get('review_column'))).alias("averageReviewLength"),
            sf.expr("map('oldest', max(reviewAge), 'newest', min(reviewAge),"
                    "'average', mean(reviewAge))").alias("reviewAge")
        )
        return df
    
    def _write(self, df: DataFrame) -> DataFrame:
        result = df.toLocalIterator()
        output = os.path.join(
            self.config.OUTPUT_AGGREGATION_PATH, 
            datetime.datetime.now().strftime("%Y-%m-%d") + "_review_agg.jsonl"
        )

        if not os.path.exists(self.config.OUTPUT_AGGREGATION_PATH):
            os.makedirs(self.config.OUTPUT_AGGREGATION_PATH)

        with open(output, 'w') as f:
            for row in result:
                f.write(json.dumps(row.asDict()) + '\n')
