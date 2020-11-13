 
"""
LivyOperator prototype
Launch a spark application to a spark cluster with Apache Livy
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator

from datetime import datetime, timedelta
import json

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

dag = DAG("livy-test", default_args=default_args,schedule_interval= '@once')

t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)

spark_task = task_get_op = SimpleHttpOperator(
    task_id='spark-test-livy',
    method='POST',
    endpoint='batches',
    data=json.dumps({
        "name": "SparkPi-01",
        "className": "org.apache.spark.examples.SparkPi",
        "numExecutors": 2,
        "file": "local:///opt/spark/examples/src/main/python/pi.py",
        "args": ["10"],
        "conf": {
            "spark.kubernetes.container.image": "mikaelapisani/spark-py:1.0",
            "spark.kubernetes.authenticate.driver.serviceAccountName": "spark"
        }
      }),
    headers={'Content-Type': 'application/json'},
    http_conn_id='livy_conn_id',
    dag=dag
)


spark_task.set_upstream(t1)
