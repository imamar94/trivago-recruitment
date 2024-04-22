from dataclasses import dataclass, field
import yaml


@dataclass
class IngestionConfig:
    """
    This class is used to store the ingestion configuration parameters
    """
    SPARK_MASTER: str = "local[*]"
    SPARK_APP_NAME: str = "ingestion"
    INPUT_PATH: str = "data/reviews.jsonl"
    INAPPROPRIATE_WORDS_PATH: str = "data/inappropriate_words.txt"
    REVIEW_SCHEMA: str = "schemas/review.json"
    AGGREGATION_SCHEMA: str = "schemas/aggregation.json"
    OUTPUT_PATH: str = "data/output"
    OUTPUT_AGGREGATION_PATH: str = "data/aggregations"
    PARAMS: dict = field(default_factory=lambda: {
        "max_perc_inappropriate_words": 0.2,
        "max_review_age_year": 3,
        "unique_ids": ["restaurantId", "reviewId"],
        "timestamp_column": "publishedAt"
    })

def yaml_config_parser(yaml_path: str):
    """
    This function is used to parse the yaml config file
    """
    with open(yaml_path, "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

def get_config_from_yaml(yaml_path: str):
    config = yaml_config_parser(yaml_path).get("config")
    config = {k.upper(): v for k, v in config.items()}
    return IngestionConfig(**config)
