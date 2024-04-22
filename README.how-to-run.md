# How to Run Guide

## Table of Contents

- [How to Run Guide](#how-to-run-guide)
  - [Prerequisite](#prerequisite)
  - [Installation](#installation)
  - [How to run](#how-to-run)
    - [Run ingestion with all default argument](#run-ingestion-with-all-default-argument)
    - [Run by overriding default argument](#run-by-overriding-default-argument)
    - [Use new config file](#use-new-config-file)
  - [Output](#output)
    - [`<output-path>/tmp/*`](#output-pathtmp)
    - [`<output-path>/final/*`](#output-pathfinal)
    - [`<aggregations-path>/<YYYY-MM-DD>_review_agg.jsonl`](#aggregations-pathyyyy-mm-dd_review_aggjsonl)

## Prerequisite
This project mainly relies on PySpark and utilizes a local machine as a Spark cluster. To run this project, you need to have the following installed:

- [Python](https://www.python.org/downloads/) (Python version >= 3.8)
- [Java](https://www.java.com/en/download/) (Java version >= 8, required for Apache Spark)

## Installation
It is recommended to do the installation inside a virtual environment, for example, use venv ([guide here](https://docs.python.org/3/library/venv.html)). Once everything set up:

Inside the root of this repository run the following command:
```
make install
```
or manually by using
```
python -m pip install -r requirements.txt
```

To run the test, you can simply run the following command
```
make test
```

## How to run
Once installed, the ingestion is accesible through `review-ingestion` command via terminal. You can simply run the ingestion by following command inside the root directory of this project.

### Run ingestion with all default argument
Default argument can be found in `ingestion_config/default.yaml`
```
review-ingestion
```

### Run by overriding default argument
Overriding default argument can be done by following arguments
```
review-ingestion --input INPUT --inappropriate_words INAPPROPRIATE_WORDS --output OUTPUT --aggregations AGGREGATIONS
```

example:
```
review-ingestion --input data/review.jsonl --inappropriate_words data/inappropriate_words.txt --output data/output --aggregations data/agg
```

### Use new config file
You can also run the ingestion with more flexible config by specifying location of config file
```
review-ingestion --config_path CONFIG_YAML_PATH
```

example:
```
review-ingestion --config_path ingestion_config/new_ingestion.yaml
```

## Output
There will be multiple output after running the ingestion command:
### `<output-path>/tmp/*`
The parquet version of JSONL input will be stored in this path.

This program are utilising the parquet format to make spark process much more efficiently, including making it possible for spark to process data which originally won't fit to the memory. In order doing so, there is preprocess to move jsonl format to parquet, whcih make the entire process something like this: jsonl --> convert to parquet --> process --> output.

### `<output-path>/final/*`
The actual output of cleaned and processed review in delta format. The example how to read delta format through pyspark as follow (also stored in `test_output.py`)
```python
from delta import DeltaTable
from ingestion.spark_utils import get_spark_session

spark = get_spark_session()
df = DeltaTable.forPath(spark, '<output-path>/final').toDF()
df.orderBy("restaurantId", "reviewId").show(100, False)
```

### `<aggregations-path>/<YYYY-MM-DD>_review_agg.jsonl`
The output of aggregated data, stored with YYYY-MM-DD prefix.
