from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import EtlConfig
from etl.csv_extract import extract_csv_raw_strings
from etl.etl_phase_logging import (
    announce_etl_phase,
    announce_load_phase,
    ensure_etl_console_logging,
)
from etl.upsert import upsert_table

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PgConnection

CSV_BASENAME = "md_currency_d"
DS_TABLE = "md_currency_d"
PK_COLUMNS = ["currency_rk", "data_actual_date"]
COLUMNS = [
    "currency_rk",
    "data_actual_date",
    "data_actual_end_date",
    "currency_code",
    "code_iso_char",
]


def source_csv_path(config: EtlConfig) -> Path:
    return (config.csv_dir / f"{CSV_BASENAME}.csv").resolve()


def extract_raw(spark: SparkSession, config: EtlConfig) -> DataFrame:
    return extract_csv_raw_strings(spark, source_csv_path(config))


def transform(df: DataFrame) -> tuple[DataFrame, dict[str, int]]:
    rows_input = df.count()
    df = (
        df.withColumn(
            "_p_data_actual_date",
            F.to_date(F.col("DATA_ACTUAL_DATE"), "yyyy-M-d"),
        )
        .withColumn(
            "_p_data_actual_end_date",
            F.to_date(F.col("DATA_ACTUAL_END_DATE"), "yyyy-M-d"),
        )
        .drop("DATA_ACTUAL_DATE", "DATA_ACTUAL_END_DATE")
        .withColumnRenamed("_p_data_actual_date", "data_actual_date")
        .withColumnRenamed("_p_data_actual_end_date", "data_actual_end_date")
    )
    trim_cc = F.trim(F.col("CURRENCY_CODE"))
    trim_iso = F.trim(F.col("CODE_ISO_CHAR"))
    df = df.select(
        F.col("data_actual_date"),
        F.col("data_actual_end_date"),
        F.trim(F.col("CURRENCY_RK")).cast("int").alias("currency_rk"),
        F.substring(trim_cc, 1, 3).alias("currency_code"),
        F.substring(trim_iso, 1, 3).alias("code_iso_char"),
    )
    df = df.filter(
        F.col("currency_rk").isNotNull() & F.col("data_actual_date").isNotNull()
    )
    rows_after_filter = df.count()
    df = df.dropDuplicates()
    rows_output = df.count()
    return df, {
        "rows_input": rows_input,
        "rows_after_filter": rows_after_filter,
        "rows_output": rows_output,
    }


def _align_for_jdbc(df: DataFrame) -> DataFrame:
    return df.select(*[F.col(c) for c in COLUMNS])


def load(df: DataFrame, config: EtlConfig, postgres_conn: "PgConnection") -> dict:
    aligned_df = _align_for_jdbc(df)
    return upsert_table(
        aligned_df,
        config,
        postgres_conn,
        DS_TABLE,
        list(PK_COLUMNS),
        list(COLUMNS),
    )


def run_etl(
    spark: SparkSession, config: EtlConfig, postgres_conn: "PgConnection"
) -> dict:
    ensure_etl_console_logging()
    announce_etl_phase(logger, DS_TABLE, "EXTRACT")
    extracted_df = extract_raw(spark, config)
    announce_etl_phase(logger, DS_TABLE, "TRANSFORM")
    transformed_df, transform_stats = transform(extracted_df)
    announce_load_phase(logger, DS_TABLE, transform_stats)
    load_stats = load(transformed_df, config, postgres_conn)
    return {"table": DS_TABLE, "transform": transform_stats, "load": load_stats}
