from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'araev'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2010, 1, 1, 0, 0, 0),
    'depends_on_past': True
}

dag = DAG(
    USERNAME + '_final_dwh_etl_mdm',
    default_args=default_args,
    description='Final DWH ETL MDM',
    schedule_interval="0 0 1 1 *",
    max_active_runs = 1,
)

clear_ods = PostgresOperator(
    task_id="clear_ods",
    dag=dag,
    sql="""
        DELETE FROM araev.final_ods_mdm WHERE EXTRACT(YEAR FROM registered_at) = {{ execution_date.year }}
    """
)

fill_ods = PostgresOperator(
    task_id="fill_ods",
    dag=dag,
    sql="""
        INSERT INTO araev.final_ods_mdm
        SELECT *  
        FROM mdm.user 
        WHERE EXTRACT(YEAR FROM registered_at) = {{ execution_date.year }}
    """
)

clear_ods_hashed = PostgresOperator(
    task_id="clear_ods_hashed",
    dag=dag,
    sql="""
        DELETE FROM araev.final_ods_mdm_hashed WHERE EXTRACT(YEAR FROM registered_at) = {{ execution_date.year }}
    """
)

fill_ods_hashed = PostgresOperator(
    task_id="fill_ods_hashed",
    dag=dag,
    sql="""
        INSERT INTO araev.final_ods_mdm_hashed
        SELECT *, '{{ execution_date }}'::TIMESTAMP AS LOAD_DT FROM araev.project_ods_v_mdm 
        WHERE EXTRACT(YEAR FROM registered_at) = {{ execution_date.year }}
    """
)

ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)

clear_ods >> fill_ods >> clear_ods_hashed >> fill_ods_hashed >> ods_loaded
