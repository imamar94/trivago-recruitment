from ingestion.config import get_config_from_yaml, IngestionConfig
import os

def test_yaml_parser():
    yaml_path = os.path.join(os.path.dirname(__file__), "test_data", "test_config.yaml")
    config = get_config_from_yaml(yaml_path)

    assert isinstance(config.PARAMS, dict)
    assert config.INPUT_PATH == "ingestion/tests/test_data/input/reviews.jsonl"
    assert config.INAPPROPRIATE_WORDS_PATH == "ingestion/tests/test_data/input/inappropriate_words.txt"
    assert config.PARAMS.get('timestamp_column') == "publishedAt"

def test_default_config():
    yaml_path = os.path.join(os.path.dirname(__file__), "test_data", "test_config.yaml")
    config = get_config_from_yaml(yaml_path)
    default_config = IngestionConfig()

    assert isinstance(default_config.INPUT_PATH, str) and bool(default_config.INPUT_PATH)
    assert default_config.PARAMS.keys() == config.PARAMS.keys()
