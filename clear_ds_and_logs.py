# Очистка схемы DS (все обычные таблицы, включая ``stg_*``) и лога ``LOGS.etl_log``.
#
# Запуск из корня репозитория: ``python clear_ds_and_logs.py`` (нужен ``.env`` и PostgreSQL).
#
# Подгрузка ``.env`` — UTF-8 / UTF-16 / CP1251 при необходимости (см. ``_decode_env_file``).
# Подключение: ``POSTGRES_PORT``, при ошибке — ``POSTGRES_PUBLISH_PORT`` (хост → Docker).
from __future__ import annotations

import os
import sys
from dataclasses import replace
from io import StringIO
from pathlib import Path

from dotenv import dotenv_values
from psycopg2 import sql

from etl.db import connect_postgres
from etl.log_utils import init_etl_log_table


def _ensure_project_path() -> Path:
    here = Path(__file__).resolve().parent
    to_add: list[Path] = []
    if (here / "config.py").is_file():
        to_add.append(here)
    elif (here / "root" / "config.py").is_file():
        to_add.extend([here / "root", here])
    else:
        print(
            "Не найден config.py рядом со скриптом или в root/. "
            "Запуск: из корня проекта (рядом с config.py).",
            file=sys.stderr,
        )
        sys.exit(1)
    for p in reversed(to_add):
        s = str(p.resolve())
        if s not in sys.path:
            sys.path.insert(0, s)
    return here


_ROOT = _ensure_project_path()


def _dotenv_candidates() -> list[Path]:
    if (_ROOT / "config.py").is_file():
        return [_ROOT / ".env"]
    return [
        _ROOT.parent / ".env",
        _ROOT / ".env",
        _ROOT / "root" / ".env",
    ]


def _decode_env_file(raw: bytes) -> str:
    # Читает ``.env``: UTF-8 / UTF-8 BOM / UTF-16 LE|BE (часто после «Сохранить как Unicode» в Блокноте).
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return raw.decode("utf-16", errors="replace")
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw.decode("utf-8-sig", errors="replace")
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1251", errors="replace")


def _prime_dotenv_safe() -> None:
    for path in _dotenv_candidates():
        if not path.is_file():
            continue
        try:
            raw = path.read_bytes()
        except OSError:
            continue
        text = _decode_env_file(raw)
        for key, val in dotenv_values(stream=StringIO(text)).items():
            if not key or val is None:
                continue
            sval = str(val).strip()
            if sval == "":
                continue
            os.environ.setdefault(key, sval)


_prime_dotenv_safe()

from config import postgres_config_from_environ


def _postgres_ports_to_try(postgres_config) -> list[int]:
    ports: list[int] = [postgres_config.port]
    for key in ("POSTGRES_PUBLISH_PORT", "POSTGRES_PORT"):
        raw = os.getenv(key, "").strip()
        if raw.isdigit():
            p = int(raw)
            if p not in ports:
                ports.append(p)
    return ports


def _open_connection_clear():
    os.environ.setdefault("PGCLIENTENCODING", "UTF8")
    base_postgres_config = postgres_config_from_environ()
    last_err: BaseException | None = None
    for port in _postgres_ports_to_try(base_postgres_config):
        postgres_config = replace(base_postgres_config, port=port)
        try:
            return connect_postgres(postgres_config)
        except BaseException as exc:
            last_err = exc
    assert last_err is not None
    raise last_err


def truncate_all_ds_tables(postgres_conn) -> int:
    with postgres_conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT c.relname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'DS' AND c.relkind = 'r'
            ORDER BY c.relname;
            """
        )
        names = [row[0] for row in cursor.fetchall()]
        if not names:
            return 0
        qualified = [
            sql.SQL("{}.{}").format(sql.Identifier("DS"), sql.Identifier(t)) for t in names
        ]
        stmt = sql.SQL("TRUNCATE TABLE {} CASCADE").format(sql.SQL(", ").join(qualified))
        cursor.execute(stmt)
    postgres_conn.commit()
    return len(names)


def main() -> None:
    postgres_conn = _open_connection_clear()
    try:
        truncated_count = truncate_all_ds_tables(postgres_conn)
        with postgres_conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("TRUNCATE TABLE {} CASCADE").format(
                    sql.Identifier("LOGS", "etl_log")
                )
            )
        postgres_conn.commit()
        init_etl_log_table(postgres_conn)
    finally:
        postgres_conn.close()
    print(
        f"OK: в схеме DS очищено таблиц: {truncated_count} (целевые + stg_*), "
        "LOGS.etl_log обнулён, DDL логов проверен."
    )


if __name__ == "__main__":
    main()
