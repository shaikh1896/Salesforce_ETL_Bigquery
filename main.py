from simple_salesforce import Salesforce
from google.cloud import bigquery
import requests
import os
import json
import pandas as pd

def loadtobq(table_id, df):
    try:
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.autodetect = True
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config)
        job.result()
        print(f"Data loaded to BigQuery table {table_id} successfully.")
    except Exception as e:
        print(f"Error loading data to BigQuery: {str(e)}")

def salesforce_connection(soql_query):
    try:
        url = "https://login.salesforce.com/services/oauth2/token"

        with open('config.json') as config_file:
            config = json.load(config_file)

        payload = "&".join([
            f"client_id={config['client_id']}",
            f"client_secret={config['client_secret']}",
            f"grant_type=refresh_token",
            f"refresh_token={config['refresh_token']}"
        ])
        headers = {
            'content-type': "application/x-www-form-urlencoded"
        }

        response = requests.request("POST", url, data=payload, headers=headers)
        credentials = response.json()

        sf = Salesforce(instance_url=credentials["instance_url"],
                        session_id=credentials["access_token"],
                        version="57.0")
        # Execute the query
        results = sf.query_all(soql_query)
        df = pd.DataFrame(results['records'])
        return df
    except Exception as e:
        print(f"Salesforce connection error: {str(e)}")
        return None


credentials_path = r'service_Acc.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path


def process_and_load(query, bq_table):
    df = salesforce_connection(query)
    if df is not None and not df.empty:
        loadtobq(bq_table, df)
    else:
        print(f"No rows to be ingested for {bq_table} or an error occurred.")

with open('queries.json') as queries_file:
            query_configa = json.load(queries_file)


def main_func():
  for query_name, config in query_configa.items():
      process_and_load(config['query'], config['table'])
  return 'Data ingested successfully'
    
    
