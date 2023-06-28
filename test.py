for dag_number in range(1, 4):
    dag_id = f'generator_dag_{dag_number}'
    print(dag_id)
    globals()[dag_id] = f'lol_{dag_number}'
    print(globals()[dag_id])