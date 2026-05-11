from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import EtlConfig
from etl.csv_extract import extract_csv_raw_strings
from etl.db import DS_SCHEMA
from etl.etl_phase_logging import (
    announce_etl_phase,
    announce_load_phase,
    ensure_etl_console_logging,
)
from etl.postgres_jdbc import jdbc_url, jdbc_write_properties, postgres_qualified_table

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PgConnection

CSV_BASENAME = "ft_posting_f"
DS_TABLE = "ft_posting_f"
_TARGET_QUALIFIED = postgres_qualified_table(DS_SCHEMA, DS_TABLE)


def source_csv_path(config: EtlConfig) -> Path:
    return (config.csv_dir / f"{CSV_BASENAME}.csv").resolve()


def extract_raw(spark: SparkSession, config: EtlConfig) -> DataFrame:
    return extract_csv_raw_strings(spark, source_csv_path(config))


def transform(df: DataFrame) -> tuple[DataFrame, dict[str, int]]:
    rows_input = df.count()
    df = (
        df.withColumn("_p_oper_date", F.to_date(F.col("OPER_DATE"), "d-M-yyyy"))
        .drop("OPER_DATE")
        .withColumnRenamed("_p_oper_date", "oper_date")
    )
    df = df.select(
        F.col("oper_date"),
        F.col("CREDIT_ACCOUNT_RK").cast("bigint").alias("credit_account_rk"),
        F.col("DEBET_ACCOUNT_RK").cast("bigint").alias("debet_account_rk"),
        F.col("CREDIT_AMOUNT").cast("double").alias("credit_amount"),
        F.col("DEBET_AMOUNT").cast("double").alias("debet_amount"),
    )
    df = df.filter(
        F.col("oper_date").isNotNull()
        & F.col("credit_account_rk").isNotNull()
        & F.col("debet_account_rk").isNotNull()
    )
    rows_output = df.count()
    return df, {"rows_input": rows_input, "rows_output": rows_output}


def load(df: DataFrame, config: EtlConfig, postgres_conn: "PgConnection") -> dict:
    df.write.mode("overwrite").jdbc(
        url=jdbc_url(config),
        table=_TARGET_QUALIFIED,
        properties=jdbc_write_properties(config),
    )
    with postgres_conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {_TARGET_QUALIFIED};")
        row_count = cursor.fetchone()[0]
    postgres_conn.commit()
    return {"table": DS_TABLE, "rows_loaded": int(row_count)}


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
