# Логирование прогона
from __future__ import annotations

from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import Json

LOGS_SCHEMA = "LOGS"
FULL_LOG_TABLE = f'"{LOGS_SCHEMA}".etl_log'
LEGACY_ETL_LOG_STEP = f'"{LOGS_SCHEMA}".etl_log_step'


def init_etl_log_table(conn: PgConnection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{LOGS_SCHEMA}";')
        cursor.execute(f"DROP TABLE IF EXISTS {LEGACY_ETL_LOG_STEP} CASCADE;")
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {FULL_LOG_TABLE} (
                etl_log_id  SERIAL PRIMARY KEY,
                started_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ,
                status      VARCHAR(32) NOT NULL,
                error_message TEXT,
                extra       JSONB
            );
            """)
        cursor.execute(f"""
            ALTER TABLE {FULL_LOG_TABLE}
            ALTER COLUMN started_at SET DEFAULT NOW();
            """)
    conn.commit()


def log_start(conn: PgConnection) -> int:
    with conn.cursor() as cursor:
        cursor.execute(
            f"INSERT INTO {FULL_LOG_TABLE} (started_at, status) "
            f"VALUES (NOW(), 'Started') RETURNING etl_log_id;"
        )
        row = cursor.fetchone()
    conn.commit()
    return int(row[0])


def verify_log_start_row(conn: PgConnection, etl_log_id: int) -> bool:
    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT 1
            FROM {FULL_LOG_TABLE}
            WHERE etl_log_id = %s
              AND status = 'Started'
              AND finished_at IS NULL
            """,
            (etl_log_id,),
        )
        return cursor.fetchone() is not None


def log_finish(
    conn: PgConnection,
    etl_log_id: int,
    success: bool,
    error_message: str | None = None,
    extra: dict | None = None,
) -> None:
    status = "Success" if success else "Failed"
    extra_json = Json(extra) if extra else None
    with conn.cursor() as cursor:
        cursor.execute(
            f"""UPDATE {FULL_LOG_TABLE}
                SET finished_at = NOW(), status = %s, error_message = %s, extra = %s
                WHERE etl_log_id = %s;""",
            (status, error_message, extra_json, etl_log_id),
        )
    conn.commit()
