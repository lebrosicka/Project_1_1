from __future__ import annotations

import argparse

from config import load_etl_config
from etl.db import postgres_connection
from etl.log_utils import init_etl_log_table, log_start


def main() -> None:
    parser = argparse.ArgumentParser(description="Инициализация LOGS.etl_log")
    parser.add_argument(
        "--schema-only",
        action="store_true",
        help="Схема LOG и таблица LOGS",
    )
    args = parser.parse_args()

    etl_config = load_etl_config()
    with postgres_connection(etl_config.postgres) as conn:
        init_etl_log_table(conn)
        if args.schema_only:
            print("OK: схема LOGS и таблица etl_log готовы.")
            return
        etl_log_id = log_start(conn)
        print("OK: вставлена запись о старте (Started).")
        print(f"    etl_log_id = {etl_log_id}")


if __name__ == "__main__":
    main()
