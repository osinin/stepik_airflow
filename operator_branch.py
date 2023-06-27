import random

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator


def rand():
    rand_val = random.randint(1, 10)
    print(f"random value is {rand_val}")
    return rand_val


def branch_func(**kwargs):
    xcom_val = kwargs["ti"].xcom_pull(task_ids='push_random_val')
    if int(xcom_val) > 5:
        return 'big_task'
    else:
        return 'small_task'


with DAG(dag_id='operator_branch',
         default_args={'owner': 'u_m1dxa'},
         schedule_interval='30 22 * * *',
         start_date=days_ago(1),
         tags=['stepik']
    ) as dag:

    start_op = PythonOperator(
        task_id='push_random_val',
        python_callable=rand,
        dag=dag
    )

    branch_op = BranchPythonOperator(
        task_id='branch',
        python_callable=branch_func,
        dag=dag
    )

    big_op = DummyOperator(task_id='big_task', dag=dag)
    small_op = DummyOperator(task_id='small_task', dag=dag)

    start_op >> branch_op >> [big_op, small_op]
