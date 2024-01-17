from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    'trigger_parent_dag',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['osinin']
) as dag:
    start = DummyOperator(task_id='start', dag=dag)
    finish = DummyOperator(task_id='finish', dag=dag)

    trigger_child_dag = TriggerDagRunOperator(
        task_id='trigger_child_dag',
        trigger_dag_id='trigger_child_dag',
        trigger_rule="none_failed",
        conf={
            "partitions": "{{ ds_nodash[:6] }}"
        }
    )

    start >> trigger_child_dag >> finish

