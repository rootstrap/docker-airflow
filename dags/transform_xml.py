from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator

from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 9, 7),
    "email": ["mikaela.pisani@rootstrap.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("s3-dag", default_args=default_args, schedule_interval= '@once') as dag:
    t1 = BashOperator(
        task_id='bash_test',
        bash_command='echo 1',
        dag=dag
    )
    transformer = S3FileTransformOperator(
        task_id='S3_ETL_OP',
        source_s3_key='s3://medical-records/origin/105.xml',
        dest_s3_key='s3://medical-records/s3/105.parquet',
        replace=True,
        transform_script='/usr/local/airflow/dags/scripts/transform.py',
        source_aws_conn_id='s3_connection',
        dest_aws_conn_id='s3_connection'
    )
    t1.set_upstream(transformer)
