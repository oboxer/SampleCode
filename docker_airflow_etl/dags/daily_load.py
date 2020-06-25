import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from utils.db_helper import S3_SETTINGS, REDSHIFT_CONN, METRIC_TABLES, drop_and_create_table, copy_metrics_csv_to_tbl
from utils.generate_web_logs import generate_web_log

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
    dag_id='daily_etl',
    default_args=default_args,
    schedule_interval='@daily',
    max_active_runs=1,
    concurrency=2,
    dagrun_timeout=timedelta(hours=5),
)

mock_raw_csv_data = BashOperator(
    task_id=f'mock_lead_and_company_csv_raw_data',
    dag=dag,
    bash_command='java -Xmx2048m -cp /usr/local/airflow/jars/spark_etl-assembly-0.1.jar '
                 'com.comp.etl.mock.MockDataGeneration '
                 '--init "false" '
                 '--range_end 2000 '
                 '--date {{ execution_date.strftime("%Y-%m-%d") }} '
                 '--path  "data/raw" '
                 '--s3Bucket "com.comp.prod.data.etl" '
                 f"--s3key \"{S3_SETTINGS['aws_access_key_id']}\" "
                 f"--s3sec \"{S3_SETTINGS['aws_secret_access_key']}\" "
                 '--redshift-url "jdbc:redshift://com-comp-production-data-etl-cluster1.cdubsxtg3q7h.us-east-1.redshift.amazonaws.com:5439/prod" '
                 f"--redshift-username \"{REDSHIFT_CONN['user']}\" "
                 f"--redshift-password \"{REDSHIFT_CONN['password']}\"",
)

mock_web_logs_data = PythonOperator(
    task_id=f'mock_web_logs_raw_data',
    dag=dag,
    op_kwargs={'seed_str': '{{ execution_date }}'},
    python_callable=generate_web_log
)

calculate_metrics = BashOperator(
    task_id=f'etl_calculate_metrics',
    dag=dag,
    bash_command='java -Xmx2048m -cp /usr/local/airflow/jars/spark_etl-assembly-0.1.jar '
                 'com.comp.etl.spark.MetricsEtl '
                 '--init "false" '
                 '--date {{ execution_date.strftime("%Y-%m-%d") }} '
                 f"--s3key \"{S3_SETTINGS['aws_access_key_id']}\" "
                 f"--s3sec \"{S3_SETTINGS['aws_secret_access_key']}\"",
)

for k in METRIC_TABLES.keys():
    drop_create_tbl = PythonOperator(
        task_id=f'drop_and_create_tbl_{k}',
        dag=dag,
        op_kwargs={'tbl_name': k},
        python_callable=drop_and_create_table,
    )

    load_tbl = PythonOperator(
        task_id=f'load_tbl_{k}',
        dag=dag,
        op_kwargs={'tbl_name': k, 's3_path': S3_BUCKET_METRIC_PATH + k + '/{{ execution_date.strftime("%Y-%m-%d") }}'},
        python_callable=copy_metrics_csv_to_tbl,
    )

    calculate_metrics >> drop_create_tbl >> load_tbl

mock_raw_csv_data >> mock_web_logs_data >> calculate_metrics
