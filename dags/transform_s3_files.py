from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task
from airflow.operators.s3_file_transform_operator import S3FileTransformOperator


from airflow.operators.bash_operator import BashOperator
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

def process_file(file):
    print('Processing file ', file)


source_s3_path = Variable.get("raw_path")
dest_s3_path = Variable.get("cleaned_path")

def load_files():
    s3 = S3Hook(aws_conn_id='s3_connection')
    s3.get_conn()
    files = s3.list_keys(bucket_name='patients-records', prefix='raw-files/', delimiter='/')
    if (len(files)>1):
        files = files[1:]
    else:
        files = []
    files = list(map(lambda x:x.split('/')[1], files))
    return files

def create_section():
    files = load_files()
    list_files = PythonOperator(task_id='list_files',
                    python_callable=load_files
        )
    process_files = [S3FileTransformOperator(
                task_id=f'transform_s3_data-{i}',
                source_s3_key= source_s3_path + '/' + file,
                dest_s3_key=dest_s3_path + '/' + file.split('.')[0] + '.csv', 
                replace=True,
                transform_script='/opt/airflow/dags/scripts/transform.py',
                source_aws_conn_id='s3_connection',
                dest_aws_conn_id='s3_connection'
            ) for file,i in zip(files,range(len(files)))
    ]
    
    list_files >> process_files

with DAG(dag_id="batchfiles", default_args=default_args, schedule_interval= '@once') as dag:

    start = DummyOperator(task_id='start')

    with TaskGroup("section", tooltip="Tasks for Section") as section:
        create_section()

    start  >> section 






