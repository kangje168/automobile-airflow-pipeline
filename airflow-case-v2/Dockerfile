FROM apache/airflow:2.10.3-python3.11

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.11.txt"
COPY pytest.ini /opt/airflow/pytest.ini
COPY tests/ /opt/airflow/tests/
