from ast import literal_eval

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


def print_date_func(**kwargs):
    partitions = kwargs["partitions"]
    partitions = literal_eval(partitions)
    print(partitions, type(partitions))



with DAG(
    'trigger_child_dag',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['osinin']
) as dag:
    start = DummyOperator(task_id='start', dag=dag)
    finish = DummyOperator(task_id='finish', dag=dag)

    print_date = PythonOperator(
        task_id='print_date',
        python_callable=print_date_func,
        op_kwargs={"partitions": "{{ dag_run.conf['partitions'] }}"}
    )


    start >> print_date >> finish

