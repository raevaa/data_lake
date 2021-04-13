from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'araev'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_data_lake_etl',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)

ods_billing = DataProcHiveOperator(
    task_id='ods_billing',
    dag=dag,
    query="""
        insert overwrite table araev.ods_billing partition (year='{{ execution_date.year }}') 
        select user_id, billing_period, service, tariff, cast(sum as DECIMAL(10,2)), cast(created_at as DATE) from araev.stg_billing where year(created_at) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_billing_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_issue = DataProcHiveOperator(
    task_id='ods_issue',
    dag=dag,
    query="""
        insert overwrite table araev.ods_issue partition (year='{{ execution_date.year }}') 
        select cast(user_id as INT), cast(start_time as TIMESTAMP), cast(end_time as TIMESTAMP), title, description, service from araev.stg_issue where year(start_time) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_issue_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_payment = DataProcHiveOperator(
    task_id='ods_payment',
    dag=dag,
    query="""
        insert overwrite table araev.ods_payment partition (year='{{ execution_date.year }}') 
        select user_id, pay_doc_type, pay_doc_num, account, phone, billing_period, cast(pay_date as DATE), cast(sum as DECIMAL(10,2)) from araev.stg_payment where year(pay_date) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_payment_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_traffic = DataProcHiveOperator(
    task_id='ods_traffic',
    dag=dag,
    query="""
        insert overwrite table araev.ods_traffic partition (year='{{ execution_date.year }}') 
        select user_id, cast(`timestamp` as TIMESTAMP), device_id, device_ip_addr, bytes_sent, bytes_received from araev.stg_traffic where year(from_unixtime(cast(`timestamp`/1000 as BIGINT))) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

dm_traffic = DataProcHiveOperator(
    task_id='dm_traffic',
    dag=dag,
    query="""
        insert overwrite table araev.dm_traffic partition (year='{{ execution_date.year }}') 
        select user_id, max(bytes_received), min(bytes_received), avg(bytes_received) from araev.stg_traffic where year(`timestamp`) = {{ execution_date.year }} GROUP BY user_id;
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_dm_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_traffic >> dm_traffic