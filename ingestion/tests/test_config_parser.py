from ingestion.config import yaml_config_parser
import os

def test_yaml_parser():
    yaml_path = os.path.join(os.path.dirname(__file__), "test_data", "test_config.yaml")
    config_yaml = yaml_config_parser(yaml_path)
    assert isinstance(config_yaml.get('ingestions'), list)
    assert isinstance(config_yaml.get('ingestions')[0], dict)
    assert config_yaml.get('ingestions')[0]['name'] == "test"
    assert config_yaml.get('ingestions')[0]['config']['input_path'] == "data/reviews.jsonl"
