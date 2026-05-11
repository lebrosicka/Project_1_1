from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

CSV_SEPARATOR = ";"


def extract_csv_raw_strings(spark: SparkSession, csv_path: Path | str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("sep", CSV_SEPARATOR)
        .csv(str(Path(csv_path).resolve()))
    )
