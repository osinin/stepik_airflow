from airflow import DAG
from airflow.sensors.python import PythonSensor
from datetime import datetime
from airflow.utils.dates import days_ago


def true_func():
    return True


default_args = {
  'start_date': datetime(2021, 1, 1)
}

with DAG(dag_id='operator_sensor',
         default_args={'owner': 'u_m1dxa'},
         schedule_interval='30 22 * * *',
         start_date=days_ago(1),
         tags=['stepik']
    ) as dag:

    false_op_1 = PythonSensor(
      task_id='reschedule',
      poke_interval=5,
      timeout=10,
      mode="reschedule",
      python_callable=true_func,
      soft_fail=True,
      dag=dag
    )

    false_op_2 = PythonSensor(
      task_id='poke',
      poke_interval=5,
      timeout=10,
      mode="poke",
      python_callable=true_func,
      dag=dag
    )