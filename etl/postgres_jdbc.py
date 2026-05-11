# Общие параметры JDBC к PostgreSQL для Spark `DataFrame.write.jdbc`
from __future__ import annotations

from config import EtlConfig

POSTGRES_JDBC_MAVEN_COORD = "org.postgresql:postgresql:42.7.3"
POSTGRES_JDBC_DRIVER = "org.postgresql.Driver"


def jdbc_url(config: EtlConfig) -> str:
    # JDBC URL для ``write.jdbc`` / ``read.jdbc``.
    pg = config.postgres
    return f"jdbc:postgresql://{pg.host}:{pg.port}/{pg.database}"


def jdbc_write_properties(config: EtlConfig) -> dict[str, str]:
    pg = config.postgres
    return {
        "user": pg.user,
        "password": pg.password,
        "driver": POSTGRES_JDBC_DRIVER,
    }


def postgres_qualified_table(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'
