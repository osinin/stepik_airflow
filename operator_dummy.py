from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain


with DAG(dag_id='stepik_dummy_operator_1',
        schedule_interval = '0 23 * * *',
        default_args={'owner':'u_m1dxa'},
        start_date=days_ago(0),
        tags=['stepik']
    ) as dag:

    t1 = DummyOperator(task_id='Dummy_1', dag=dag)
    t2 = DummyOperator(task_id='Dummy_2', dag=dag)
    t3 = DummyOperator(task_id='Dummy_3', dag=dag)
    t4 = DummyOperator(task_id='Dummy_4', dag=dag)
    t5 = DummyOperator(task_id='Dummy_5', dag=dag)
    t6 = DummyOperator(task_id='Dummy_6', dag=dag)
    t7 = DummyOperator(task_id='Dummy_7', dag=dag)

# [t1, t2, t3, t4]
# t5.set_upstream([t1, t2])
# t6.set_upstream([t2, t3, t4])
# t7.set_upstream([t4, t5, t6])

t1
t4.set_upstream(t1)
t5.set_upstream(t1)
t6.set_upstream(t1)
t3.set_upstream([t1, t5])
t2.set_upstream([t1, t3, t6])
t4.set_upstream([t1, t2])
t7.set_upstream(t2)


