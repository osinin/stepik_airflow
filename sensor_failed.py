from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime
from airflow.utils.task_group import TaskGroup


table_names = ['ecosystem', 'ezy', 'lol']


def sql_date_checker(ti, **kwargs):
    table = kwargs["table"]
    xcom_key = f'kapital_target_actuality_{table}'
    print(f"xcom key: {xcom_key}")
    target_actuality = 1
    if table in ['ecosystem', 'ezy']:
        print(f'{table} yes')
        target_actuality = ti.xcom_pull(key=xcom_key, task_ids=f'{table}.sens_{table}') or 0
        print(f"initial target actuality: {target_actuality}")
        target_actuality += 1
        ti.xcom_push(key=xcom_key, value=target_actuality)

    print(f"target_actuality: {target_actuality}")
    if target_actuality < 3:
        return False
    else:
        return True


with DAG('sql_sens_failed',
         schedule_interval='@daily',
         start_date=datetime(2023, 1, 1),
         catchup=False,
         tags=['u_m1dxa']) as dag:

    script_group = []
    for table in table_names:
        with TaskGroup(group_id=table) as tg:

            finish = EmptyOperator(task_id=f"finish_{table}")

            sens = PythonSensor(
                task_id=f"sens_{table}",
                mode="poke",
                poke_interval=15,
                timeout=45,
                python_callable=sql_date_checker,
                op_kwargs={"table": table},
                dag=dag
            )

            sens >> finish
            script_group.append(tg)