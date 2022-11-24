from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

default_args = {
    "owner": "galang"
}
# define DAG
with DAG(
    "bigdata_process",
    start_date = datetime(2022, 11, 23),
    schedule_interval = '*/10 * * * *',
    default_args = default_args
) as dag:
    job_start = DummyOperator(
        task_id = "job_start"
    )

    t1 = BashOperator(
        task_id = "Dump Data",
        bash_command = 'python3 E:\DigitalSkola - Data Engineer\airflow\dags\code_implementation',
        dag=dag
    )

    t2 = BashOperator(
        task_id = "Dump Data",
        bash_command = 'python3 E:\DigitalSkola - Data Engineer\airflow\dags\code_implementation',
        dag=dag
    )

    job_finish = DummyOperator(
        task_id = "job_finish"
    )

    # orchestration
    (
        job_start
        >> t1
        >> t2
        >> job_finish
    )