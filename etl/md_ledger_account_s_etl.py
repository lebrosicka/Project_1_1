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

CSV_BASENAME = "md_ledger_account_s"
DS_TABLE = "md_ledger_account_s"
PK_COLUMNS = ["ledger_account", "start_date"]

COLUMNS = [
    "chapter",
    "chapter_name",
    "section_number",
    "section_name",
    "subsection_name",
    "ledger1_account",
    "ledger1_account_name",
    "ledger_account",
    "ledger_account_name",
    "characteristic",
    "is_resident",
    "is_reserve",
    "is_reserved",
    "is_loan",
    "is_reserved_assets",
    "is_overdue",
    "is_interest",
    "pair_account",
    "start_date",
    "end_date",
    "is_rub_only",
    "min_term",
    "min_term_measure",
    "max_term",
    "max_term_measure",
    "ledger_acc_full_name_translit",
    "is_revaluation",
    "is_correct",
]

_LEDGER_INT_NULLABLE = frozenset(
    {
        "is_resident",
        "is_reserve",
        "is_reserved",
        "is_loan",
        "is_reserved_assets",
        "is_overdue",
        "is_interest",
        "is_rub_only",
    }
)


def source_csv_path(config: EtlConfig) -> Path:
    return (config.csv_dir / f"{CSV_BASENAME}.csv").resolve()


def extract_raw(spark: SparkSession, config: EtlConfig) -> DataFrame:
    return extract_csv_raw_strings(spark, source_csv_path(config))


def transform(df: DataFrame) -> tuple[DataFrame, dict[str, int]]:
    rows_input = df.count()
    df = (
        df.withColumn(
            "_p_start_date",
            F.to_date(F.col("START_DATE"), "yyyy-M-d"),
        )
        .withColumn(
            "_p_end_date",
            F.to_date(F.col("END_DATE"), "yyyy-M-d"),
        )
        .drop("START_DATE", "END_DATE")
        .withColumnRenamed("_p_start_date", "start_date")
        .withColumnRenamed("_p_end_date", "end_date")
    )
    chapter = F.trim(F.col("CHAPTER"))
    chapter_name = F.trim(F.col("CHAPTER_NAME"))
    section_name = F.trim(F.col("SECTION_NAME"))
    subsection_name = F.trim(F.col("SUBSECTION_NAME"))
    ledger1_name = F.trim(F.col("LEDGER1_ACCOUNT_NAME"))
    ledger_name = F.trim(F.col("LEDGER_ACCOUNT_NAME"))
    characteristic = F.trim(F.col("CHARACTERISTIC"))
    df = df.select(
        F.substring(chapter, 1, 1).alias("chapter"),
        F.substring(chapter_name, 1, 16).alias("chapter_name"),
        F.trim(F.col("SECTION_NUMBER")).cast("int").alias("section_number"),
        F.substring(section_name, 1, 22).alias("section_name"),
        F.substring(subsection_name, 1, 21).alias("subsection_name"),
        F.trim(F.col("LEDGER1_ACCOUNT")).cast("int").alias("ledger1_account"),
        F.substring(ledger1_name, 1, 47).alias("ledger1_account_name"),
        F.trim(F.col("LEDGER_ACCOUNT")).cast("int").alias("ledger_account"),
        F.substring(ledger_name, 1, 153).alias("ledger_account_name"),
        F.substring(characteristic, 1, 1).alias("characteristic"),
        F.col("start_date"),
        F.col("end_date"),
    )
    df = df.filter(
        F.col("ledger_account").isNotNull() & F.col("start_date").isNotNull()
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
    result = df
    for name in COLUMNS:
        if name not in result.columns:
            if name in _LEDGER_INT_NULLABLE:
                result = result.withColumn(name, F.lit(None).cast("int"))
            else:
                result = result.withColumn(name, F.lit(None).cast("string"))
    return result.select(*[F.col(c) for c in COLUMNS])


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
