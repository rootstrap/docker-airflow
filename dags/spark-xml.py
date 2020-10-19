"""
SparkSubmit prototype
Launch a spark application in local mode to parse xml file  
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["mikaela.pisani@rootstrap.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("spark-xml", default_args=default_args,schedule_interval= '@once')

t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

spark_submit_task = SparkSubmitOperator(
    task_id='spark_local_job',
    conn_id='spark_local',
    java_class='org.apache.spark.examples.SparkPi',
    application='/usr/local/airflow/dags/scripts/parse_xml.py',
    name='airflowspark-test',
    packages='com.databricks:spark-xml_2.12:0.6.0',
    verbose=True,
    dag=dag,
)

spark_submit_task.set_upstream(t1)
