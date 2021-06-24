"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.docker_operator import DockerOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "description": "Use of the DockerOperator",
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("ctakes", default_args=default_args, schedule_interval=timedelta(120))


key = "844a62ef-ecf3-4cf7-ae8f-b3ddae2ff952" 
# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)
t2 = DockerOperator(
    task_id='docker_command',
    image='rootstrap/ctakes:latest',
    api_version='auto',
    auto_remove=True,
    environment={
        'AF_EXECUTION_DATE': "{{ ds }}",
        'AF_OWNER': "{{ task.owner }}",
        'CTAKES_KEY': "{{key}}"
    },
    command='/bin/bash -v /home/ubuntu/docker-airflow/input:/input -v /home/ubuntu/docker-airflow/output:/output -c \'echo "HI"\'',
    docker_url='unix://var/run/docker.sock',
    network_mode='bridge',
    dag=dag
)
t3 = BashOperator(
    task_id='print_hello',
    bash_command='echo "hello world"', 
    dag=dag
)
t1 >> t2 >> t3
