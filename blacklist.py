# Загрузка данных в таблицу BigQuery
job = client.load_table_from_dataframe(my_dataframe, table_ref)
job.result()  # Ждем завершения загрузки данных

print(f'Data uploaded to {project_id}.{dataset_id}.{table_id}')



new_dataset_id = 'first_dataset_people'
new_table_id = 'first_table_people'
table_id = f'{new_dataset_id}.{new_table_id}'
