from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator


dag = DAG('generator_task',
          schedule_interval=timedelta(days=1),
          start_date=days_ago(1),
          tags=['stepik'])

tasks_list = []
for i in range(0, 10):
    tasks_list.append(DummyOperator(task_id=f'task_{i}', dag=dag))
    if i:
      tasks_list[i-1] >> tasks_list[i]