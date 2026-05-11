ETL: CSV → PostgreSQL (DS), журнал LOGS. Запуск: Docker + Airflow, DAG вручную.

Перед запуском: скопируйте .env.example в .env (copy .env.example .env).

config.py + .env — настройки. docker-compose.yml — Postgres и Airflow.
dags/bank_ds_csv_etl.py — сценарий (5 задач). etl/*_etl.py — шесть таблиц. CSV/*.csv — данные.

Утилиты: ensure_etl_log_table_and_start.py, trigger_bank_ds_csv_etl.bat
