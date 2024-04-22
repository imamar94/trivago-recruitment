from ingestion.processor import IngestionProcessor, AggregationProcessor
from ingestion.config import IngestionConfig, get_config_from_yaml
import argparse


def run():
    parser = argparse.ArgumentParser(
        description="Review Ingestion Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--config_path", default="ingestion_config/default.yaml",
                        help="Path to YAML config file")
    parser.add_argument("--input",
                        help="Local filesystem path to the JSONL file containing the reviews")
    parser.add_argument("--inappropriate_words",
                        help="Local file system path to the new-line delimited text file containing the words to filter out")
    parser.add_argument("--output",
                        help="Local filesystem path to write the successfully processed reviews to")
    parser.add_argument("--aggregations",
                        help="Local file system path to write the aggregations as JSONL")
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

    aggregator = AggregationProcessor(config)
    aggregator.run()
