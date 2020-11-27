from airflow.models.dag import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


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


def load_files():
    s3 = S3Hook(aws_conn_id='s3_connection')
    s3.get_conn()
    files = s3.list_keys(bucket_name='patients-records', prefix='cleaned/', delimiter='/')
    if (len(files)>1):
        files = files[1:]
    else:
        files = []
    return list(map(lambda x:x.split('/')[1], files))

def create_section():
    files = load_files()
    list_files = PythonOperator(task_id='list_files',
                    python_callable=load_files
        )
    process_files = [S3ToRedshiftOperator(
            task_id = f's3_to_redshift_transformer-{i}',
            schema = 'PUBLIC',
            table = 'patient',
            s3_bucket = 'patients-records',
            s3_key = 'cleaned' + '/' + file,
            redshift_conn_id = 'redshift_connection',
            aws_conn_id = 's3_connection',
            copy_options = ["csv"],
            truncate_table  = False
        ) for file,i in zip(files,range(len(files)))
    ]

    list_files >> process_files

with DAG(dag_id="redshift_transformer", default_args=default_args, schedule_interval= '@once') as dag:

  
    start = DummyOperator(task_id='start')

    with TaskGroup("section", tooltip="Tasks for Section") as section:
        create_section()

    start  >> section 




