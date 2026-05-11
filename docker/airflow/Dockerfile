FROM apache/airflow:2.8.4-python3.11

LABEL org.opencontainers.image.description="Bank ETL + Apache Airflow (LocalExecutor)"
LABEL airflow.version="2.8.4"
LABEL airflow.python="3.11"
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
ARG AIRFLOW_CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.11.txt"
COPY requirements.txt /tmp/project-requirements.txt
RUN pip install --no-cache-dir --default-timeout=180 --retries=10 \
    -r /tmp/project-requirements.txt \
    --constraint "${AIRFLOW_CONSTRAINTS_URL}" \
    || pip install --no-cache-dir --default-timeout=180 --retries=10 \
        -r /tmp/project-requirements.txt
