from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='compute_morgan_fingerprints',
#    start_date=pendulum.datetime(2024, 6, 17),
    schedule=None,  #  '0 11 * * *',
    tags=['compute_morgan_fgprts']
) as dag:
    start_op = PythonOperator(
        task_id='download_chembl',
        python_callable=set_chembl_local
    )