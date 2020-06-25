import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from utils.db_helper import INIT_TABLES, drop_and_create_table, copy_csv_to_tbl, S3_SETTINGS, REDSHIFT_CONN

log = logging.getLogger()
S3_BUCKET_PATH: str = 's3://com.comp.prod.data.etl/data/init/'

# May also want to implement below:
# 'on_failure_callback'
# 'email_on_failure'
default_args: dict = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2020, 6, 20),
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag: DAG = DAG(
    dag_id='initial_data_db_load',
    default_args=default_args,
    schedule_interval='@daily',
    max_active_runs=1,
    concurrency=2,
    dagrun_timeout=timedelta(hours=5),
)

mock_csv_data = BashOperator(
    task_id=f'mock_all_csv_data_tbl',
    dag=dag,
    bash_command='java -Xmx2048m -cp /usr/local/airflow/jars/spark_etl-assembly-0.1.jar '
                 'com.comp.etl.mock.MockDataGeneration '
                 '--init "true" '
                 '--range_end 5000 '
                 '--date {{ execution_date.strftime("%Y-%m-%d") }} '
                 '--path  "data/init" '
                 '--s3Bucket "com.comp.prod.data.etl" '
                 f"--s3key \"{S3_SETTINGS['aws_access_key_id']}\" "
                 f"--s3sec \"{S3_SETTINGS['aws_secret_access_key']}\" "
                 '--redshift-url "jdbc:redshift://com-comp-production-data-etl-cluster1.cdubsxtg3q7h.us-east-1.redshift.amazonaws.com:5439/prod" '
                 f"--redshift-username \"{REDSHIFT_CONN['user']}\" "
                 f"--redshift-password \"{REDSHIFT_CONN['password']}\"",
)

setup_raw_table_snapshots = BashOperator(
    task_id=f'setup_raw_table_snapshots',
    dag=dag,
    bash_command='java -Xmx2048m -cp /usr/local/airflow/jars/spark_etl-assembly-0.1.jar '
                 'com.comp.etl.spark.MetricsEtl '
                 '--init "true" '
                 '--date {{ execution_date.strftime("%Y-%m-%d") }} '
                 f"--s3key \"{S3_SETTINGS['aws_access_key_id']}\" "
                 f"--s3sec \"{S3_SETTINGS['aws_secret_access_key']}\"",
)

for k in INIT_TABLES.keys():
    drop_create_tbl = PythonOperator(
        task_id=f'drop_and_create_tbl_{k}',
        dag=dag,
        op_kwargs={'tbl_name': k},
        python_callable=drop_and_create_table,
    )

    load_tbl = PythonOperator(
        task_id=f'load_tbl_{k}',
        dag=dag,
        op_kwargs={'tbl_name': k, 's3_path': S3_BUCKET_PATH + k + '.csv'},
        python_callable=copy_csv_to_tbl,
    )

    drop_create_tbl >> mock_csv_data >> load_tbl >> setup_raw_table_snapshots
