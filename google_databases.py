# Laboratory 1 #

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd


def create_pandas_dataset(names: list, ages: list):
    dict_from_lists = {'Name': names, 'Age': ages}
    dataframe = pd.DataFrame(dict_from_lists)
    return dataframe


credentials = service_account.Credentials.from_service_account_file('google_cloud_keys/keyfile.json')   # init key
project_id = 'first-project-7167108'        # init project id
client = bigquery.Client(credentials=credentials, project=project_id)      # init bigquery client

query = client.query(""" SELECT state_name FROM `bigquery-public-data.covid19_aha.staffing` LIMIT 100 """)  # init query
results = query.result()        # get result by query

result_df = query.to_dataframe()        # convert to pandas dataframe

# print(result_df)

# # # Create dataset # # #
new_dataset_id = 'first_dataset_people'
new_dataset_ref = client.dataset(dataset_id=new_dataset_id)
new_dataset = bigquery.Dataset(dataset_ref=new_dataset_ref)
new_dataset.location = 'US'
new_dataset.description = 'First dataset with names and ages people.'
try:
    client.create_dataset(new_dataset)
except:
    print(f'\nDataset with id: "{new_dataset_id}" already exist!')

# # # Define table # # #
scheme = [
    bigquery.SchemaField('Name', 'STRING', mode='REQUIRED'),
    bigquery.SchemaField('Age', 'INTEGER', mode='NULLABLE')
]

new_table_id_local = 'first_table_people'
new_table_id = f'{new_dataset_id}.{new_table_id_local}'
new_table_ref = client.dataset(dataset_id=new_dataset_id).table(table_id=new_table_id)
new_table = bigquery.Table(table_ref=new_table_ref, schema=scheme)
try:
    client.create_table(table=new_table)
except:
    print(f'\nTable with id: "{new_table_id}" already exist!')

# # # Load data to database # # #
names = ['Vadim', 'Sergey', 'Polina', 'Dasha', 'Andrey', 'Ilya', 'Karina']
ages = [21, 33, 17, 18, 23, 45, 10]
people_df = create_pandas_dataset(names, ages)
# print(people_df)

job = client.load_table_from_dataframe(people_df, new_table_id)
job.result()
print('Data was successfully added!')

# # # Read data from own database # # #
query = client.query(""" SELECT * FROM `first-project-7167108.first_dataset_people.first_table_people` LIMIT 100 """)  # init query
results = query.result()        # get result by query

result_df = query.to_dataframe()        # convert to pandas dataframe
print(result_df)
