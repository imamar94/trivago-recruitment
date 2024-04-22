from ingestion.processor import IngestionProcessor
from ingestion.config import IngestionConfig, get_config_from_yaml
import argparse


def run():
    parser = argparse.ArgumentParser(description="Review Ingestion Pipeline")
    parser.add_argument("--config-path", default="ingestion_config/default.yaml")
    parser.add_argument("--input")
    parser.add_argument("--inappropriate_words")
    parser.add_argument("--output")
    parser.add_argument("--aggregations")
    args = parser.parse_args()

    config = get_config_from_yaml(args.config_path)
    
    # override config with command line arguments
    if args.input:
        config.INPUT_PATH = args.input
    if args.inappropriate_words:
        config.INAPPROPRIATE_WORDS_PATH = args.inappropriate_words
    if args.output:
        config.OUTPUT_PATH = args.output
    if args.aggregations:
        config.OUTPUT_AGGREGATION_PATH = args.aggregations

    processor = IngestionProcessor(config)
    processor.run()
