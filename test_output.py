from delta import DeltaTable
from ingestion.processor import IngestionProcessor
from ingestion.config import IngestionConfig, get_config_from_yaml

p = IngestionProcessor(IngestionConfig())
df = DeltaTable.forPath(p.spark, 'data/output/final').toDF()
df.orderBy("restaurantId", "reviewId").show(100, False)
