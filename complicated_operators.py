from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain


task_dict = {'first_seen_on_staging': ['raw_first_seen_on_staging_kb',
                                       'raw_first_seen_on_staging_nasiya',
                                       'raw_first_seen_on_staging_uzumbank'],
             'first_seen_on': ['raw_first_seen_on_firebase',
                               'raw_first_seen_on_market'],
             'ecosystem_identifiers_days': ['ecosystem_identifiers_days_kb_unauth',
                                            'ecosystem_identifiers_days_kb_auth',
                                            'ecosystem_identifiers_days_kb_customer',
                                            'ecosystem_identifiers_days_market_auth',
                                            'ecosystem_identifiers_days_market_customer',
                                            'ecosystem_identifiers_days_market_unauth',
                                            'ecosystem_identifiers_days_nasiya_auth',
                                            'ecosystem_identifiers_days_nasiya_unauth',
                                            'ecosystem_identifiers_days_nasiya_customer',
                                            'ecosystem_identifiers_days_uzumbank_auth',
                                            'ecosystem_identifiers_days_uzumbank_customer',
                                            'ecosystem_identifiers_days_uzumbank_unauth',
                                            'ecosystem_identifiers_days_tezkor_customer'],
             'ecosystem_activity': ['ecosystem_activity']
             }

checker_dict = {'ecosystem_identifiers_days_uzumbank_customer': [['ods.v_operations_b2c_w_finance', 'date_uz'],
                                                                 ['ods.v_uzumbank_users', 'first_registered_date']],
                'ecosystem_identifiers_days_uzumbank_unauth':   [['raw.firebase_events', 'event_date']],
                'ecosystem_identifiers_days_uzumbank_auth':     [['raw.firebase_events', 'event_date'],
                                                                 ['ods.uzumbank_users_first_event_dict', 'first_registered_date']],
                'ecosystem_identifiers_days_tezkor_customer':   [['tezkor.fact_orders', 'order_date']],
                'ecosystem_identifiers_days_kb_customer':       [['kapital.datamarts_v_client_portfolio', 'report_date'],
                                                                 ['kapital.datamarts_v_uzum_dimcontragent_all', 'registerdate']],
                'ecosystem_identifiers_days_kb_unauth':         [['kapital.datamarts_v_client_portfolio', 'report_date']],
                'ecosystem_identifiers_days_kb_auth':           [['raw.kapitalbank_firebase_events', 'event_date']],
                'ecosystem_identifiers_days_market_auth':       [['marts.sessions_f_market', 'date_uz']],
                'ecosystem_identifiers_days_market_unauth':     [['marts.sessions_f_market', 'date_uz']],
                'ecosystem_identifiers_days_market_customer':   [['marts.sessions_f_market', 'date_uz'],
                                                                 ['marts.orders_f_market', 'date_uz']],
                'ecosystem_identifiers_days_nasiya_auth':       [['raw.nasiya_firebase_events', 'event_date']],
                'ecosystem_identifiers_days_nasiya_unauth':     [['raw.nasiya_firebase_events', 'event_date']],
                'ecosystem_identifiers_days_nasiya_customer':   [['nasiya.marts_dds_contracts_portfolio', 'report_date'],
                                                                 ['nasiya.marts_dds_users_portfolio', 'report_date']]
                }

dags = ['apelsin__firebase_bigquery__ch_events__etl', 'kapitalbank__firebase_bigquery__ch_events__etl',
        'nasiya__firebase_bigquery__ch_events__etl', 'uzumbank__ch__operation_w_cashback__dag',
        'marts__s3__market__extraction', 'uzumbank_s3_tezkor_extraction_etl', 'kapital_s3_oracle_extraction_etl',
        'raw__ecosystem_identifiers_days__etl']



with DAG(
        'raw__ecosystem_identifiers_days__etl',
        start_date=days_ago(1),
        schedule_interval='0 3-15 * * *',
        max_active_runs=1,
        max_active_tasks=3,
        catchup=False
) as dag:
    start = DummyOperator(task_id="start", dag=dag)
    finish = DummyOperator(task_id="finish", dag=dag)

    get_dag_runs = DummyOperator(
        task_id="get_dag_runs",
        dag=dag
    )

    get_new_runs = DummyOperator(
        task_id="get_new_runs",
        dag=dag
    )

    listener = DummyOperator(
        task_id="listener",
        dag=dag
    )

    table_names = [table for table in task_dict.keys() if table != 'ecosystem_identifiers_days']
    groups = []
    for table in table_names:
        target_db = task_dict[table][0].split('_')[0]
        with TaskGroup(group_id=table) as tg:
            create_sandbox_task = DummyOperator(
                task_id=f'create_sandbox_{table}',
                dag=dag
            )

            script_group = []
            with TaskGroup(group_id=f"{table}_scripts") as inserts:
                for script_name in task_dict[table]:
                    insert_df_into_sandbox_task = DummyOperator(
                        task_id=f"insert_{script_name}",
                        dag=dag,
                        retries=0
                    )
                    insert_df_into_sandbox_task
                    script_group.append(inserts)

            create_sandbox_task >> script_group

            replace_partitions_task = DummyOperator(
                task_id='replace_partitions',
                dag=dag,
            )

            drop_sandbox_task = DummyOperator(
                task_id='drop_sandbox',
                dag=dag,
            )

            groups.append(tg)

    eid = []
    with TaskGroup(group_id='ecosystem_identifiers_days') as eid_tg:
        create_sandbox_task = DummyOperator(
            task_id=f'create_sandbox',
            dag=dag
        )

        script_group = []
        for table in checker_dict:
            tg_id = f'check_and_insert_{table}'
            with TaskGroup(group_id=tg_id) as tg:
                    sql_check_task = DummyOperator(
                        task_id=f"sql_check_{table}",
                        dag=dag
                    )

                    insert_df_into_sandbox_task = DummyOperator(
                        task_id=f"insert_{table}",
                        dag=dag
                    )

                    sql_check_task >> insert_df_into_sandbox_task
                    script_group.append(tg)

        replace_partitions_task = DummyOperator(
            task_id=f'replace_partitions',
            dag=dag
        )

        drop_sandbox_task = DummyOperator(
            task_id=f'drop_sandbox',
            dag=dag
        )

    eid.append(eid_tg)

start >> get_dag_runs >> get_new_runs >> listener >> groups[0] >> groups[1] >> eid >> groups[2] >> finish