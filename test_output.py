from delta import DeltaTable
from ingestion.spark_utils import get_spark_session

spark = get_spark_session()
df = DeltaTable.forPath(spark, 'data/output/final').toDF()
df.orderBy("restaurantId", "reviewId").show(100, False)
