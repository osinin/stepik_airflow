from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable


# Результат будет перехвачен Airflow и попадет в лог файл задачи
def connection():
  host = BaseHook.get_connection("airflow_db").host
  password = BaseHook.get_connection("airflow_db").password
  print("Result:", host, password)


def variables():
  foo = Variable.get("key", deserialize_json=True)
  print(foo)


with DAG('connections_variables',
         schedule_interval=timedelta(days=1),
         start_date=days_ago(1),
         tags=['stepik']
         ) as dag:
  # Создадим оператор для исполнения python функции
  t1 = PythonOperator(task_id='connection',
                      python_callable=connection)
  t2 = PythonOperator(task_id='variables',
                      python_callable=variables)