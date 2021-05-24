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


fill_report_temp = PostgresOperator(
    task_id='fill_report_temp',
    dag=dag,
    sql="""
create table araev.final_payment_report_temp_{{ execution_date.year }} as
with mdm_data as (select
            cast(extract(year from to_date(billing_period_key, 'YYYY-MM')) as integer) as year, legal_type, district, billing_mode, cast(extract(year from registered_at) as integer) as registration_year, is_vip, billing_period_key, sum as payment_sum
        from araev.final_dds_link_account_pay a
        join araev.final_dds_hub_billing_period b on a.billing_period_pk=b.billing_period_pk
        join araev.final_dds_hub_user c on a.user_pk=c.user_pk
        join araev.final_dds_sat_pay d on a.pay_pk=d.pay_pk
        join araev.final_ods_mdm_hashed e on c.user_pk=e.user_pk)
    select year, legal_type, district, billing_mode, registration_year, is_vip, sum(payment_sum) as payment_sum
    from mdm_data
   group by year, legal_type, district, billing_mode, registration_year, is_vip
    order by year, legal_type, district, billing_mode, registration_year, is_vip;
    """
)

clear_ods >> fill_ods >> clear_ods_hashed >> fill_ods_hashed >> ods_loaded >> fill_report_temp

final_payment_report_dim_billing_year = PostgresOperator(
    task_id="final_payment_report_dim_billing_year",
    dag=dag,
    sql="""      
            insert into araev.final_payment_report_dim_billing_year(billing_year_key)
select distinct year as billing_year_key from araev.final_payment_report_temp 
left join araev.final_payment_report_dim_billing_year on billing_year_key=year
where billing_year_key is null;
            """
)
final_payment_report_dim_legal_type  = PostgresOperator(
    task_id="final_payment_report_dim_legal_type",
    dag=dag,
    sql="""
            insert into araev.final_payment_report_dim_legal_type(legal_type_key)
select distinct legal_type as legal_type_key from araev.final_payment_report_temp
left join araev.final_payment_report_dim_legal_type on legal_type_key=legal_type
where legal_type_key is null;
            """
)
final_payment_report_dim_district  = PostgresOperator(
    task_id="final_payment_report_dim_district",
    dag=dag,
    sql="""
            insert into araev.final_payment_report_dim_district(district_key)
select distinct district as district_key from araev.final_payment_report_temp 
left join araev.final_payment_report_dim_district on district_key=district
where district_key is null;
            """
)

final_payment_report_dim_billing_mode  = PostgresOperator(
    task_id="final_payment_report_dim_billing_mode",
    dag=dag,
    sql="""
            insert into araev.final_payment_report_dim_billing_mode(billing_mode_key)
select distinct billing_mode as billing_mode_key from araev.final_payment_report_temp 
left join araev.final_payment_report_dim_billing_mode on billing_mode_key=billing_mode
where billing_mode_key is null;
            """
)
final_payment_report_dim_registration_year  = PostgresOperator(
    task_id="final_payment_report_dim_registration_year",
    dag=dag,
    sql="""
            insert into araev.final_payment_report_dim_registration_year(registration_year_key)
select distinct registration_year as registration_year_key from araev.final_payment_report_temp
left join araev.final_payment_report_dim_registration_year on registration_year_key=registration_year
where registration_year_key is null;
            """
)


all_dims_loaded = DummyOperator(task_id="all_dims_loaded", dag=dag)


fill_report_temp >> final_payment_report_dim_billing_year >> all_dims_loaded
fill_report_temp >> final_payment_report_dim_legal_type >> all_dims_loaded
fill_report_temp >> final_payment_report_dim_district >> all_dims_loaded
fill_report_temp >> final_payment_report_dim_billing_mode >> all_dims_loaded
fill_report_temp >> final_payment_report_dim_registration_year >> all_dims_loaded


dim_fct  = PostgresOperator(
    task_id="dim_fct",
    dag=dag,
    sql="""
            INSERT INTO araev.final_payment_report_fct
select a.id, b.id, c.id, e.id, d.id, is_vip, tmp.payment_sum
from araev.final_payment_report_temp tmp
join araev.final_payment_report_dim_billing_year a on tmp.year=a.billing_year_key
join araev.final_payment_report_dim_legal_type b on tmp.legal_type=b.legal_type_key
join araev.final_payment_report_dim_district c on tmp.district=c.district_key
join araev.final_payment_report_dim_billing_mode e on tmp.billing_mode=e.billing_mode_key
join araev.final_payment_report_dim_registration_year d on tmp.registration_year=d.registration_year_key; 
            """
)

drop_payment_report_temp = PostgresOperator(
    task_id='drop_payment_report_temp',
    dag=dag,
    sql="""
          drop table if exists araev.final_payment_report_temp_{{ execution_date.year }};
     """
)
all_dims_loaded >> dim_fct >>  drop_payment_report_temp

