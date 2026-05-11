Ссылка на гугл диск где лежит видео по проекту: https://drive.google.com/drive/folders/1O-d8TUjcRQ9b8vTNXAWA8oVJnL7pIkc9?usp=drive_link

ETL: CSV → PostgreSQL (DS), журнал LOGS. Запуск: Docker и .bat файл вручную

config.py + .env — настройки. docker-compose.yml — Postgres и Airflow.
dags/bank_ds_csv_etl.py — DAG (5 задач). etl/*_etl.py — шесть таблиц. CSV/*.csv — данные.


Порядок запуска проекта после загрузки из репозитория:
1. Создать файл .env в корне проекта, скопировать в него содержимое .env.example, при необходимости заменить значения на свои
2. В корне проекта открыть терминал и прописать docker compose up -d
3. Дождаться окончания сборки
4. При подключении к БД в Dbeaver или другом приложении необходимо указать порт:55433, если вы не меняли настройки в .env
5. Запуск ETL процесса осуществляется: с помощью файла в корне проекта trigger_bank_ds_csv_etl.bat
6. Перед запуском .bat файла рекомендуется открыть интерфейс airflow по localhost:8080 по умолчанию. Параметры подключения: USERNAME=airflow PASSWORD=demo_airflow_change_me

Project_1_1/
├── docker-compose.yml           Стек Docker: PostgreSQL + Airflow, сеть, тома, Connection bank_postgres.
├── .env.example                 Шаблон переменных окружения (пароли, порты, CSV).
├── .dockerignore                Исключения при сборке образа Airflow.
├── .gitignore                   Правила игнора для Git.
├── README.txt                   Краткий текстовый README.
├── config.py                    Загрузка .env, PostgresConfig / EtlConfig, пути к CSV, Spark.
├── requirements.txt             Зависимости Python для ETL и Airflow в контейнере.
├── trigger_bank_ds_csv_etl.bat Ручной триггер DAG bank_ds_csv_etl.
├── ensure_etl_log_table_and_start.py  CLI: создание/проверка LOGS.etl_log и старт записи лога.


├── CSV/                         Исходные CSV файлы
│   ├── ft_balance_f.csv         
│   ├── ft_posting_f.csv         
│   ├── md_account_d.csv         
│   ├── md_currency_d.csv        
│   ├── md_exchange_rate_d.csv   
│   └── md_ledger_account_s.csv  
│
├── dags/
│   ├── .airflowignore           Что не парсить как DAG.
│   └── bank_ds_csv_etl.py       DAG
│
├── docker/
│   ├── airflow/
│   │   └── Dockerfile           Образ Airflow + JDK/PySpark для ETL.
│   └── postgres/
│       └── init/
│           └── 01-create-airflow-db.sql  CREATE DATABASE airflow (вторая БД на том же Postgres).
│
└── etl/                         ETL процесс по таблицам DS
    ├── __init__.py              Пакет etl (реэкспорт/ленивые импорты).
    ├── csv_extract.py           Чтение CSV для этапа Extract.
    ├── db.py                    DDL схемы DS, init_ds_schema_and_tables, подключение к БД.
    ├── log_utils.py             Таблица LOGS.etl_log, старт/финиш.
    ├── upsert.py                Staging таблицы + upsert в PostgreSQL.
    ├── postgres_jdbc.py         JDBC для Spark → Postgres.
    ├── spark_session.py         SparkSession и настройки дат/режимов.
    ├── etl_phase_logging.py     Логи фаз EXTRACT / TRANSFORM / LOAD.
    ├── ft_balance_f_etl.py      ETL для ft_balance_f.
    ├── ft_posting_f_etl.py      ETL для ft_posting_f.
    ├── md_account_d_etl.py      ETL для md_account_d.
    ├── md_currency_d_etl.py     ETL для md_currency_d.
    ├── md_exchange_rate_d_etl.py ETL для md_exchange_rate_d.
    └── md_ledger_account_s_etl.py ETL для md_ledger_account_s.
