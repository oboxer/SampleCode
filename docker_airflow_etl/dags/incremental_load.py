import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from faker import Faker
from utils.s3_helper import write_to_file, upload_file_to_s3

log = logging.getLogger()
S3_BUCKET_PATH: str = 's3://com.comp.prod.data.etl/data/init/'
S3_BUCKET_METRIC_PATH: str = 's3://com.comp.prod.data.etl/data/final/report='

# May also want to implement below:
# 'on_failure_callback'
# 'email_on_failure'
default_args: dict = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2020, 6, 22),
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag: DAG = DAG(
    dag_id='on_demand_incremental_load_companies',
    default_args=default_args,
    schedule_interval='@daily',
    max_active_runs=1,
    concurrency=2,
    dagrun_timeout=timedelta(hours=5),
)


def insert_or_update_new_data(is_insert: bool, tbl_name: str, time_str: str):
    mode = ''
    if is_insert:
        mode = 'insert'
    else:
        mode = 'update'

    fake: Faker = Faker()

    res = []
    for i in range(6000, 7000):
        res.append(str(i) + ',' + fake.company())

    tmp_dir = f'/tmp/data/{mode}/{time_str}/'
    tmp_file_name = f'{tbl_name}.csv'
    write_to_file(str.join('\n', res), tmp_dir, tmp_file_name)

    s3_key = f'data/{mode}/{time_str}/{tmp_file_name}'
    upload_file_to_s3(bucket_name='com.comp.prod.data.etl', key=s3_key, file=tmp_dir + tmp_file_name)


insert_new_data = PythonOperator(
    task_id=f'insert_new_data',
    dag=dag,
    op_kwargs={'is_insert': True, 'tbl_name': 'companies', 'time_str': '{{ execution_date.strftime("%Y-%m-%d") }}'},
    python_callable=insert_or_update_new_data
)

update_data = PythonOperator(
    task_id=f'update_data',
    dag=dag,
    op_kwargs={'is_insert': False, 'tbl_name': 'companies', 'time_str': '{{ execution_date.strftime("%Y-%m-%d") }}'},
    python_callable=insert_or_update_new_data
)

insert_new_data >> update_data
