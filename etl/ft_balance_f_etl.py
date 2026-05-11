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

CSV_BASENAME = "ft_balance_f"
DS_TABLE = "ft_balance_f"
PK_COLUMNS = ["on_date", "account_rk"]
COLUMNS = ["on_date", "account_rk", "currency_rk", "balance_out"]


def source_csv_path(config: EtlConfig) -> Path:
    return (config.csv_dir / f"{CSV_BASENAME}.csv").resolve()


def extract_raw(spark: SparkSession, config: EtlConfig) -> DataFrame:
    return extract_csv_raw_strings(spark, source_csv_path(config))


def transform(df: DataFrame) -> tuple[DataFrame, dict[str, int]]:
    rows_input = df.count()
    df = (
        df.withColumn("_p_on_date", F.to_date(F.col("ON_DATE"), "d.M.yyyy"))
        .drop("ON_DATE")
        .withColumnRenamed("_p_on_date", "on_date")
    )
    df = df.select(
        F.col("on_date"),
        F.col("ACCOUNT_RK").cast("bigint").alias("account_rk"),
        F.col("CURRENCY_RK").cast("int").alias("currency_rk"),
        F.col("BALANCE_OUT").cast("double").alias("balance_out"),
    )
    df = df.filter(F.col("on_date").isNotNull() & F.col("account_rk").isNotNull())
    rows_after_filter = df.count()
    df = df.dropDuplicates()
    rows_output = df.count()
    return df, {
        "rows_input": rows_input,
        "rows_after_filter": rows_after_filter,
        "rows_output": rows_output,
    }


def load(df: DataFrame, config: EtlConfig, postgres_conn: "PgConnection") -> dict:
    return upsert_table(
        df,
        config,
        postgres_conn,
        DS_TABLE,
        list(PK_COLUMNS),
        list(COLUMNS),
    )


def run_etl(
    spark: SparkSession, config: EtlConfig, postgres_conn: "PgConnection"
) -> dict[str, int]:
    ensure_etl_console_logging()
    announce_etl_phase(logger, DS_TABLE, "EXTRACT")
    extracted_df = extract_raw(spark, config)
    announce_etl_phase(logger, DS_TABLE, "TRANSFORM")
    transformed_df, transform_stats = transform(extracted_df)
    announce_load_phase(logger, DS_TABLE, transform_stats)
    load_stats = load(transformed_df, config, postgres_conn)
    return {"table": DS_TABLE, "transform": transform_stats, "load": load_stats}
