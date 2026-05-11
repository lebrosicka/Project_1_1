from __future__ import annotations

import logging
import time
import traceback
from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

from config import load_etl_config
from etl import (
    ft_balance_f_etl,
    ft_posting_f_etl,
    md_account_d_etl,
    md_currency_d_etl,
    md_exchange_rate_d_etl,
    md_ledger_account_s_etl,
)
from etl.db import init_ds_schema_and_tables, postgres_connection
from etl.log_utils import init_etl_log_table, log_finish, log_start
from etl.spark_session import create_spark_session


def init_logs(**context: Any) -> None:
    etl_config = load_etl_config()
    with postgres_connection(etl_config.postgres) as conn:
        init_etl_log_table(conn)
        init_ds_schema_and_tables(conn)


def start_log(**context: Any) -> int:
    etl_config = load_etl_config()
    with postgres_connection(etl_config.postgres) as conn:
        return log_start(conn)


def pause_5_sec(**context: Any) -> None:
    time.sleep(5)


def run_all_etl(**context: Any) -> dict[str, Any]:
    task_instance = context["ti"]
    task_instance.xcom_pull(task_ids="start_log", key="return_value")
    etl_config = load_etl_config()
    spark = create_spark_session(etl_config)
    try:
        etl_modules_in_order = (
            md_currency_d_etl,
            md_exchange_rate_d_etl,
            md_account_d_etl,
            md_ledger_account_s_etl,
            ft_balance_f_etl,
            ft_posting_f_etl,
        )
        metrics_by_ds_table: dict[str, Any] = {}
        with postgres_connection(etl_config.postgres) as conn:
            for etl_module in etl_modules_in_order:
                metrics_by_ds_table[etl_module.DS_TABLE] = etl_module.run_etl(
                    spark, etl_config, conn
                )
        return {"tables": metrics_by_ds_table, "spark_version": spark.version}
    finally:
        spark.stop()


def finish_log(**context: Any) -> None:
    task_instance = context["ti"]
    etl_log_id = int(task_instance.xcom_pull(task_ids="start_log", key="return_value"))
    run_payload = (
        task_instance.xcom_pull(task_ids="run_all_etl", key="return_value") or {}
    )
    etl_config = load_etl_config()
    extra = {
        "tables": run_payload.get("tables", {}),
        "spark_version": run_payload.get("spark_version", ""),
    }
    with postgres_connection(etl_config.postgres) as conn:
        log_finish(conn, etl_log_id, success=True, extra=extra)


def on_etl_failure(context: dict[str, Any]) -> None:
    log = logging.getLogger("airflow.task")
    exc = context.get("exception")
    task_instance = context.get("task_instance")
    log.error(
        "bank_ds_csv_etl: сбой %s: %s",
        task_instance.task_id if task_instance else "?",
        exc,
    )
    try:
        if task_instance is None:
            return
        etl_log_id = task_instance.xcom_pull(
            task_ids="start_log", key="return_value", default=None
        )
        if etl_log_id is None:
            return
        etl_config = load_etl_config()
        with postgres_connection(etl_config.postgres) as conn:
            log_finish(
                conn,
                int(etl_log_id),
                success=False,
                error_message=str(exc) if exc else "unknown",
                extra={
                    "error_type": type(exc).__name__ if exc else "Exception",
                    "failed_task_id": task_instance.task_id,
                    "traceback": traceback.format_exc(),
                },
            )
    except Exception as err:
        log.error("Не удалось записать Failed в LOGS.etl_log: %s", err)


with DAG(
    dag_id="bank_ds_csv_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bank_etl", "ds"],
    default_args={"on_failure_callback": on_etl_failure},
) as dag:
    task_init_logs = PythonOperator(
        task_id="init_logs",
        python_callable=init_logs,
    )
    task_start_log = PythonOperator(
        task_id="start_log",
        python_callable=start_log,
    )
    task_pause_5_sec = PythonOperator(
        task_id="pause_5_sec",
        python_callable=pause_5_sec,
    )
    task_run_all_etl = PythonOperator(
        task_id="run_all_etl",
        python_callable=run_all_etl,
    )
    task_finish_log = PythonOperator(
        task_id="finish_log",
        python_callable=finish_log,
    )
    (
        task_init_logs
        >> task_start_log
        >> task_pause_5_sec
        >> task_run_all_etl
        >> task_finish_log
    )
