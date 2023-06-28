import numpy as np


from airflow import DAG
from airflow.sensors.base import BaseSensorOperator
from datetime import datetime
from airflow.utils.dates import days_ago


class CustomSensor(BaseSensorOperator):
    def poke(self, context):
        return_value = np.random.binomial(1, 0.3)
        print(return_value)
        return bool(return_value)


default_args = {
  'start_date': datetime(2021, 1, 1)
}


with DAG(dag_id='operator_sensor_generator',
         default_args={'owner': 'airflow'},
         schedule_interval='30 22 * * *',
         start_date=days_ago(1),
         tags=['stepik']
    ) as dag:

    tasks_list = []
    for i in range(1, 4):
        sens = CustomSensor(
          task_id=f'reschedule_{i}',
          poke_interval=4,
          timeout=50,
          soft_fail=True,
          dag=dag
        )

        tasks_list.append(sens)

    tasks_list

