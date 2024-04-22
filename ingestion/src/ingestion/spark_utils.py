from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip


_type_mapper = {
    "integer": IntegerType(),
    "string": StringType(),
    "number": DoubleType()
}


def get_spark_session(master="local[*]", app_name="ingestion"):	
    """
    Create or get a spark session
    :param app_name: name of the spark app
    :return: SparkSession object
    """
    builder = SparkSession.builder \
        .master(master) \
        .appName(app_name) \
        .config(
            "spark.sql.extensions", 
            "io.delta.sql.DeltaSparkSessionExtension") \
        .config(
            "spark.sql.catalog.spark_catalog", 
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def _get_types_from_prop(col: str, prop: dict):
    column_properties = prop.get(col)
    
    # handle date / datetime
    if column_properties.get("format") == "date-time":
        return TimestampType()
    if t := _type_mapper.get(column_properties.get("type")):
        return t
        
    raise NotImplementedError

def _json_to_spark_schema(json_schema: dict):
    required_columns = json_schema.get("required")
    properties = json_schema.get("properties")
    schema = StructType([
        StructField(col, _get_types_from_prop(col, properties), nullable=True)
        for col in required_columns
    ])

    return schema
