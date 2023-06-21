from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def my_func(**kwargs):
    ds = kwargs["ds"]
    print(ds)
    return ds

with DAG('logical_date',
         schedule_interval='@daily',
         start_date=datetime(2023, 1, 1),
         tags=['u_m1dxa']) as dag:

     get_ds = PythonOperator(
        task_id="get_ds",
        python_callable=my_func,
        dag=dag,
        op_kwargs={"ds": "{{ ds }}"}
    )

get_ds