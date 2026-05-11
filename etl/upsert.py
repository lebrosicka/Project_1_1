# Upsert в ``DS`` через временную staging-таблицу и Spark JDBC.

from __future__ import annotations

import random
import string
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from config import EtlConfig
from etl.postgres_jdbc import jdbc_url, jdbc_write_properties, postgres_qualified_table

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PgConnection


def upsert_table(
    df: DataFrame,
    config: EtlConfig,
    postgres_conn: "PgConnection",
    table_name: str,
    pk_columns: list[str],
    column_list: list[str],
    schema: str = "DS",
) -> dict[str, str]:
    staging_table_name = (
        f"stg_{table_name}_{''.join(random.choices(string.ascii_lowercase, k=6))}"
    )
    target_qualified = postgres_qualified_table(schema, table_name)
    staging_qualified = postgres_qualified_table(schema, staging_table_name)

    with postgres_conn.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE {staging_qualified} (LIKE {target_qualified} "
            f"INCLUDING DEFAULTS EXCLUDING CONSTRAINTS EXCLUDING INDEXES);"
        )
        cursor.execute(f"TRUNCATE TABLE {staging_qualified};")
    postgres_conn.commit()

    df.select(*column_list).write.mode("append").jdbc(
        url=jdbc_url(config),
        table=staging_qualified,
        properties=jdbc_write_properties(config),
    )

    insert_columns = ", ".join(f'"{c}"' for c in column_list)
    primary_key_columns = ", ".join(f'"{c}"' for c in pk_columns)
    update_columns = [c for c in column_list if c not in pk_columns]
    if not update_columns:
        raise ValueError(
            f"upsert_table({table_name}): все колонки в PK, нечего обновлять"
        )

    set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_columns)
    upsert_sql = f"""
        INSERT INTO {target_qualified} ({insert_columns})
        SELECT {insert_columns} FROM {staging_qualified}
        ON CONFLICT ({primary_key_columns}) DO UPDATE SET {set_clause};
    """
    with postgres_conn.cursor() as cursor:
        cursor.execute(upsert_sql)
    postgres_conn.commit()

    with postgres_conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE {staging_qualified};")
    postgres_conn.commit()

    return {"table": table_name, "status": "upserted"}
