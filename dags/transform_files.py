from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta
from airflow.models import Variable


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


source_s3_path = Variable.get("raw_path")
dest_s3_path = Variable.get("cleaned_path")

def process_file(file):
    print('Processing file ', file)


def load_tasks(parent_dag_name, child_dag_name, args):
    subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@once",
    )
    with subdag:
        print('Processing files')
        s3 = S3Hook(aws_conn_id='s3_connection')
        s3.get_conn()
        files = s3.list_keys(bucket_name='patients-records', prefix='raw-files/*', delimiter='/')
        print('Files:', files)
        if files is None: 
            return subdag;
        print('Files:', files)
        for file in files:
            filename = file.split('.')[0]
            print('Create subdag for file: ', file)
            S3FileTransformOperator(
                task_id='transform_s3_data',
                source_s3_key=source_s3_path + '/' + file,
                dest_s3_key=dest_s3_path + filename + '.csv', 
                replace=True,
                transform_script='/opt/airflow/dags/scripts/transform.py',
                source_aws_conn_id='s3_connection',
                dest_aws_conn_id='s3_connection',
                dag=subdag
            )
    return subdag



with DAG("s3_transformer", default_args=default_args, schedule_interval= '@once') as dag:


    start = DummyOperator(task_id='start')


    load_tasks = SubDagOperator(
        task_id='load_tasks',
        subdag=load_tasks('s3_transformer',
                           'load_tasks', default_args),
        default_args=default_args
    )

    load_tasks.set_upstream(start)