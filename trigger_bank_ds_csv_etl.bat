@echo off
setlocal
cd /d "%~dp0"

rem ASCII-only: cmd.exe misparses UTF-8 (Cyrillic) and breaks lines into bogus commands.
echo === Airflow DAG bank_ds_csv_etl: unpause + trigger ===
echo Directory: %CD%
echo.

docker info >nul 2>&1
if errorlevel 1 (
  echo ERROR: Docker is not available. Start Docker Desktop and retry.
  pause
  exit /b 1
)

docker compose ps --status running --services 2>nul | findstr /i "airflow-scheduler" >nul
if errorlevel 1 (
  echo ERROR: airflow-scheduler container is not running.
  echo From this folder run: docker compose up -d
  pause
  exit /b 1
)

echo Checking DAG import...
docker compose exec -T airflow-scheduler airflow dags list-import-errors >nul 2>&1
if errorlevel 1 (
  echo DAG import failed. Details:
  docker compose exec -T airflow-scheduler airflow dags list-import-errors
  pause
  exit /b 1
)

docker compose exec -T airflow-scheduler airflow dags unpause bank_ds_csv_etl
if errorlevel 1 (
  echo ERROR: unpause failed. Use docker compose from this repo root ^(see .env^).
  pause
  exit /b 1
)

docker compose exec -T airflow-scheduler airflow dags trigger bank_ds_csv_etl
if errorlevel 1 (
  echo ERROR: trigger failed.
  pause
  exit /b 1
)

echo.
echo OK: DAG bank_ds_csv_etl unpaused and triggered.
pause
endlocal
