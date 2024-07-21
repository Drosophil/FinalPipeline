import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from src import S3_interact as S3,\
    datamart, \
    compute_morgan, \
    views_p1, \
    tanimoto, \
    db_interact as db


def _if_tables_exist():
    db_obj = db.return_db_object()
    result = db_obj.check_and_construct_bronze_tables(construct=False)
    if result:
        return 'injest_target_files_and_compute_tanimoto'
    else:
        return 'construct_missing_tables'


def _construct_missing_tables():
    db_obj = db.return_db_object()
    db_obj.check_and_construct_bronze_tables(construct=True)


def _compute_source_fingerprints():
    db_obj = db.return_db_object()
    S3_obj = S3.return_S3_access_object()
    compute_morgan.compute_source_morgan_fingerprints(data_load=db_obj, S3_writer=S3_obj)


def _injest_targets():
    db_obj = db.return_db_object()
    S3_obj = S3.return_S3_access_object()
    tanimoto.get_similarities(data_load=db_obj, S3_writer=S3_obj)


def _create_or_update_datamart():
    db_obj = db.return_db_object()
    S3_obj = S3.return_S3_access_object()
    datamart.injest_silver_tables(data_load=db_obj, S3_writer=S3_obj)


def _create_or_replace_views_part_1():
    db_obj = db.return_db_object()
    views_p1.create_or_replace_view_7a(data_load=db_obj)
    views_p1.create_or_replace_view_7b_with_Nulls(data_load=db_obj)
    views_p1.create_or_replace_view_7b_without_Nulls(data_load=db_obj)


with DAG(
    dag_id='find_similarities_for_targets_from_files',
    start_date=pendulum.datetime(2024, 8, 1),
    schedule='0 0 1 * *',  # first day of each month
    tags=['process_targets_from_input_files'],
    catchup=False
) as dag:
    check_tables = BranchPythonOperator(
        task_id='check_if_tables_exist_in_db',
        python_callable=_if_tables_exist
    )

    construct_missing_tables = PythonOperator(
        task_id='construct_missing_tables',
        python_callable=_construct_missing_tables
    )

    compute_source_fingerprints = PythonOperator(
        task_id='compute_source_fingerprints',
        python_callable=_compute_source_fingerprints
    )

    injest_target_files_and_compute_tanimoto = PythonOperator(
        task_id='injest_target_files_and_compute_tanimoto',
        python_callable=_injest_targets,
        trigger_rule='none_failed_min_one_success'
    )

    create_or_update_datamart = PythonOperator(
        task_id='create_or_update_datamart',
        python_callable=_create_or_update_datamart
    )

    create_or_replace_views_part_1 = PythonOperator(
        task_id='create_or_replace_views_part_1',
        python_callable=_create_or_replace_views_part_1
    )

    check_tables >> construct_missing_tables >> compute_source_fingerprints >> injest_target_files_and_compute_tanimoto
    check_tables >> injest_target_files_and_compute_tanimoto
    injest_target_files_and_compute_tanimoto >> create_or_update_datamart >> create_or_replace_views_part_1
