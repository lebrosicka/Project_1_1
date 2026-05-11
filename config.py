from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _default_csv_dir() -> Path:
    return _project_root() / "CSV"


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    def __repr__(self) -> str:
        return (
            f"PostgresConfig(host={self.host!r}, port={self.port}, "
            f"database={self.database!r}, user={self.user!r}, password=***REDACTED***)"
        )


@dataclass(frozen=True)
class EtlConfig:
    # Параметры PostgreSQL, каталог CSV, Spark master

    postgres: PostgresConfig
    csv_dir: Path
    spark_master: str

    def __repr__(self) -> str:
        return f"EtlConfig(postgres={self.postgres!r}, csv_dir={self.csv_dir!r}, spark_master={self.spark_master!r})"


def _env_str(key: str, default: str) -> str:
    v = os.getenv(key)
    return v if (v is not None and v.strip() != "") else default


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _resolve_csv_dir(override: Optional[str]) -> Path:
    if override and override.strip():
        return Path(override).expanduser().resolve()
    return _default_csv_dir().resolve()


def load_etl_config(
    env_file: Optional[Path] = None,
) -> EtlConfig:
    # Загрузка переменных
    if env_file is None:
        env_file = _project_root() / ".env"
    if env_file.is_file():
        load_dotenv(env_file, override=False)

    pg = PostgresConfig(
        host=_env_str("POSTGRES_HOST", "localhost"),
        port=_env_int("POSTGRES_PORT", 5432),
        database=_env_str("POSTGRES_DB", "bank_ds"),
        user=_env_str("POSTGRES_USER", "postgres"),
        password=_env_str("POSTGRES_PASSWORD", "postgres"),
    )
    csv_dir = _resolve_csv_dir(os.getenv("ETL_CSV_DIR"))
    spark_master = _env_str("SPARK_MASTER", "local[*]")

    if not csv_dir.is_dir():
        raise FileNotFoundError(
            f"Каталог с CSV не найден: {csv_dir}. "
            f"Создайте его или задайте ETL_CSV_DIR в .env"
        )

    return EtlConfig(postgres=pg, csv_dir=csv_dir, spark_master=spark_master)


def load_postgres_config(env_file: Optional[Path] = None) -> PostgresConfig:
    if env_file is None:
        env_file = _project_root() / ".env"
    if env_file.is_file():
        load_dotenv(env_file, override=False)
    return PostgresConfig(
        host=_env_str("POSTGRES_HOST", "localhost"),
        port=_env_int("POSTGRES_PORT", 5432),
        database=_env_str("POSTGRES_DB", "bank_ds"),
        user=_env_str("POSTGRES_USER", "postgres"),
        password=_env_str("POSTGRES_PASSWORD", "postgres"),
    )


def postgres_config_from_environ() -> PostgresConfig:
    return PostgresConfig(
        host=_env_str("POSTGRES_HOST", "localhost"),
        port=_env_int("POSTGRES_PORT", 5432),
        database=_env_str("POSTGRES_DB", "bank_ds"),
        user=_env_str("POSTGRES_USER", "postgres"),
        password=_env_str("POSTGRES_PASSWORD", "postgres"),
    )


def load_config(env_file: Optional[Path] = None) -> EtlConfig:
    return load_etl_config(env_file=env_file)
