from airflow import DAG

from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

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

def process_file(file):
    print('Processing file ', file)


def load_subdag(parent_dag_name, child_dag_name, args):
    dag_subdag = DAG(
        dag_id='{0}.{1}'.format(parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@daily",
    )
    with dag_subdag:
        s3 = S3Hook(aws_conn_id='s3_connection')
        s3.get_conn()
        files = s3.list_keys(bucket_name='medical-records', prefix='origin/104.xml', delimiter='/')
        if files is None: 
            return dag_subdag;
        print('Files:', files)
        i = 0
        for file in files:
            SparkSubmitOperator(
                task_id='spark_local_job_' + str(i),
                conn_id='spark_local',
                java_class='org.apache.spark.examples.SparkPi',
                application='/usr/local/airflow/dags/scripts/parse_xml_2.py',
                name='airflowspark-test',
                packages='com.databricks:spark-xml_2.12:0.6.0,com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3',
                application_args=[file, "medical-records"],
                verbose=True,
                dag=dag_subdag)
            i=i+1

        print('Total tasks=', i)   

    return dag_subdag
    
dag = DAG("subdagtest", default_args=default_args, schedule_interval= '@once')

start_op = BashOperator(
    task_id='bash_test',
    bash_command='echo "Starting TEST"',
    dag=dag)


load_tasks = SubDagOperator(
        task_id='load_tasks',
        subdag=load_subdag('subdagtest',
                           'load_tasks', default_args),
        default_args=default_args,
        dag=dag,
    )

start_op >> load_tasks

