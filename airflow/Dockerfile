FROM apache/airflow:2.10.5

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt" \
    apache-airflow-providers-snowflake==6.0.0

RUN pip install --no-cache-dir \
    dbt-core==1.11.11 \
    dbt-snowflake==1.11.6 