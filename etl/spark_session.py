# SparkSession для ETL
from __future__ import annotations

from pyspark.sql import SparkSession

from config import EtlConfig
from etl.postgres_jdbc import POSTGRES_JDBC_MAVEN_COORD


def create_spark_session(config: EtlConfig) -> SparkSession:
    return (
        SparkSession.builder.appName("bank_ds_etl")
        .master(config.spark_master)
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.jars.packages", POSTGRES_JDBC_MAVEN_COORD)
        .getOrCreate()
    )
