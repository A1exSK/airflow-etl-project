FROM apache/airflow:3.2.1
RUN pip install --no-cache-dir apache-airflow-providers-sqlite
