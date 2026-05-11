from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any
from urllib.parse import quote

import psycopg2
from psycopg2.extensions import connection as PgConnection

from config import PostgresConfig

PG_SESSION_TIMEZONE = "Europe/Moscow"

DS_SCHEMA = "DS"


def _coerce_pg_param(value: str) -> str:
    # Строка только из символов, безопасных для UTF-8.
    return value.encode("utf-8", errors="replace").decode("utf-8")


def _postgres_connection_uri(config: PostgresConfig) -> str:
    # Строка подключения
    host = _coerce_pg_param(config.host)
    user = _coerce_pg_param(config.user)
    pw = _coerce_pg_param(config.password)
    db = _coerce_pg_param(config.database)
    return (
        f"postgresql://{quote(user, safe='')}:{quote(pw, safe='')}"
        f"@{host}:{config.port}/{quote(db, safe='')}"
    )


def connect_postgres(config: PostgresConfig) -> PgConnection:
    # Подключение через URI и без наследования ``PG*`` из окружения Windows
    dsn = _postgres_connection_uri(config)
    stripped: dict[str, str] = {}
    # Убираем дубликаты из окружения
    for key in (
        "PGPASSWORD",
        "PGUSER",
        "PGDATABASE",
        "PGHOST",
        "PGPORT",
        "PGSERVICE",
        "PGPASSFILE",
    ):
        if key in os.environ:
            stripped[key] = os.environ.pop(key)
    os.environ.setdefault("PGCLIENTENCODING", "UTF8")
    try:
        conn = psycopg2.connect(dsn)
    except UnicodeDecodeError as exc:
        raise RuntimeError(
            "Ошибка кодировки"
            "Python 3.11 или 3.12;`docker compose exec airflow-scheduler "
            'bash -lc "cd /opt/airflow/bank_etl && python clear_ds_and_logs.py"`. '
            f"Исходное исключение: {exc}"
        ) from exc
    finally:
        for key, val in stripped.items():
            os.environ[key] = val
    with conn.cursor() as cursor:
        cursor.execute("SET TIME ZONE %s", (PG_SESSION_TIMEZONE,))
    return conn


@contextmanager
def postgres_connection(config: PostgresConfig):
    conn = connect_postgres(config)
    try:
        yield conn
    finally:
        conn.close()


def init_ds_schema_and_tables(conn: Any) -> None:
    quoted_ds_schema = f'"{DS_SCHEMA}"'

    ddl_statements: list[str] = [f"CREATE SCHEMA IF NOT EXISTS {quoted_ds_schema};"]

    ddl_statements.append(f"""
        CREATE TABLE IF NOT EXISTS {quoted_ds_schema}.ft_balance_f (
            on_date       DATE NOT NULL,
            account_rk    BIGINT NOT NULL,
            currency_rk   INTEGER,
            balance_out   DOUBLE PRECISION,
            PRIMARY KEY (on_date, account_rk)
        );
        """)

    ddl_statements.append(f"""
        CREATE TABLE IF NOT EXISTS {quoted_ds_schema}.ft_posting_f (
            oper_date           DATE NOT NULL,
            credit_account_rk   BIGINT NOT NULL,
            debet_account_rk    BIGINT NOT NULL,
            credit_amount       DOUBLE PRECISION,
            debet_amount        DOUBLE PRECISION
        );
        """)

    ddl_statements.append(f"""
        CREATE TABLE IF NOT EXISTS {quoted_ds_schema}.md_account_d (
            data_actual_date     DATE NOT NULL,
            data_actual_end_date DATE NOT NULL,
            account_rk           BIGINT NOT NULL,
            account_number       VARCHAR(20) NOT NULL,
            char_type            CHAR(1) NOT NULL,
            currency_rk          INTEGER NOT NULL,
            currency_code        VARCHAR(3) NOT NULL,
            PRIMARY KEY (data_actual_date, account_rk)
        );
        """)

    ddl_statements.append(f"""
        CREATE TABLE IF NOT EXISTS {quoted_ds_schema}.md_currency_d (
            currency_rk          INTEGER NOT NULL,
            data_actual_date     DATE NOT NULL,
            data_actual_end_date DATE,
            currency_code        VARCHAR(3),
            code_iso_char        VARCHAR(3),
            PRIMARY KEY (currency_rk, data_actual_date)
        );
        """)

    ddl_statements.append(f"""
        CREATE TABLE IF NOT EXISTS {quoted_ds_schema}.md_exchange_rate_d (
            data_actual_date     DATE NOT NULL,
            data_actual_end_date DATE,
            currency_rk          INTEGER NOT NULL,
            reduced_cource       DOUBLE PRECISION,
            code_iso_num         VARCHAR(3),
            PRIMARY KEY (data_actual_date, currency_rk)
        );
        """)

    ddl_statements.append(f"""
        CREATE TABLE IF NOT EXISTS {quoted_ds_schema}.md_ledger_account_s (
            chapter                     CHAR(1),
            chapter_name                VARCHAR(16),
            section_number              INTEGER,
            section_name                VARCHAR(22),
            subsection_name             VARCHAR(21),
            ledger1_account             INTEGER,
            ledger1_account_name        VARCHAR(47),
            ledger_account              INTEGER NOT NULL,
            ledger_account_name         VARCHAR(153),
            characteristic              CHAR(1),
            is_resident                 INTEGER,
            is_reserve                  INTEGER,
            is_reserved                 INTEGER,
            is_loan                     INTEGER,
            is_reserved_assets          INTEGER,
            is_overdue                  INTEGER,
            is_interest                 INTEGER,
            pair_account                VARCHAR(5),
            start_date                  DATE NOT NULL,
            end_date                    DATE,
            is_rub_only                 INTEGER,
            min_term                    CHAR(1),
            min_term_measure            CHAR(1),
            max_term                    CHAR(1),
            max_term_measure            CHAR(1),
            ledger_acc_full_name_translit VARCHAR(1),
            is_revaluation              CHAR(1),
            is_correct                  CHAR(1),
            PRIMARY KEY (ledger_account, start_date)
        );
        """)

    with conn.cursor() as cursor:
        for ddl_sql in ddl_statements:
            cursor.execute(ddl_sql)
    conn.commit()
