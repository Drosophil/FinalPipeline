FROM apache/airflow:2.8.1

RUN pip install --upgrade pip
RUN pip uninstall -y psycopg2 psycopg2-binary
COPY requirements.txt .
RUN pip install apache-airflow[amazon,postgres]==${AIRFLOW_VERSION} -r requirements.txt

COPY dags /opt/airflow/dags
