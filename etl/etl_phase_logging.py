# Логирование фаз ETL процесса
from __future__ import annotations

import logging
import sys
import time

_etl_console_configured = False


def ensure_etl_console_logging(level: int = logging.INFO) -> None:
    # Настройка вывода логов в терминал
    global _etl_console_configured
    if _etl_console_configured:
        return
    etl_pkg = logging.getLogger("etl")
    if etl_pkg.handlers:
        etl_pkg.setLevel(level)
        _etl_console_configured = True
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    etl_pkg.addHandler(handler)
    etl_pkg.setLevel(level)
    _etl_console_configured = True


def announce_etl_phase(logger: logging.Logger, ds_table: str, phase: str) -> None:
    # По одному сообщению о фазе с паузой 1с
    logger.info("%s — %s", ds_table, phase)
    time.sleep(1)


def announce_load_phase(
    logger: logging.Logger,
    ds_table: str,
    transform_stats: dict[str, int],
) -> None:
    # Вывод колличества строк на входе и валидных.
    rows_input = transform_stats.get("rows_input")
    rows_output = transform_stats.get("rows_output")
    logger.info(
        "%s — LOAD | обычных строк (на входе transform): %s; "
        "валидных (после transform, к загрузке): %s",
        ds_table,
        rows_input,
        rows_output,
    )
    time.sleep(1)
