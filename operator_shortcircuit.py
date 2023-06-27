from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.utils import dates

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='operator_shortcircuit',
    default_args=args,
    start_date=dates.days_ago(2),
    tags=['stepik'],
) as dag:

    cond_true = ShortCircuitOperator(
        task_id='condition_is_True',
        python_callable=lambda: True,
    )

    cond_false = ShortCircuitOperator(
        task_id='condition_is_False',
        python_callable=lambda: False,
    )

    ds_true = [DummyOperator(task_id='true_' + str(i)) for i in range(2)]
    ds_false = [DummyOperator(task_id='false_' + str(i)) for i in range(2)]

    chain(cond_true, *ds_true)
    chain(cond_false, *ds_false)