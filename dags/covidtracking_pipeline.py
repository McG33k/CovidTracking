import airflow
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta, date
from pendulum import date
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
from pandas import json_normalize

# Grab current date
current_date = datetime.today().strftime('%Y-%m-%d')

# Default settings for all the DAGs in the pipeline
default_args = {
    "owner": "Airflow", 
    "start_date": datetime(2020, 6, 1),
	"email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def _process_data(ti):
    data = ti.xcom_pull(task_ids='extract_user')  # Extract the JSON object from XCom
    if data:
        data = data['data']  # Extract the data section
        processed_data = pd.DataFrame(columns=['date', 'states', 'total_cases', 'total_testing', 
                                               'currently_hospitalized', 'currently_in_icu', 
                                               'currently_on_ventilator', 'total_deaths',
											   'CreatedBy', 'CreatedOn', 'ModifiedBy', 'ModifiedOn'])  # Create an empty DataFrame

        # Extract data from the JSON and append it to the DataFrame
        processed_data = processed_data.append({
            'date': data['date'],
            'states': data['states'],
            'total_cases': data['cases']['total'],
            'total_testing': data['testing']['total'],
            'currently_hospitalized': data['outcomes']['hospitalized']['currently'],
            'currently_in_icu': data['outcomes']['hospitalized']['in_icu']['currently'],
            'currently_on_ventilator': data['outcomes']['hospitalized']['on_ventilator']['currently'],
            'total_deaths': data['outcomes']['death']['total'],
            'CreatedBy': 'System',
            'CreatedOn': datetime.now(),
            'ModifiedBy': 'System',
            'ModifiedOn': datetime.now()
        }, ignore_index=True)

        # Export DataFrame to CSV
        processed_data.to_csv('/tmp/processed_data.csv', index=False)	


def _store_data():
    # Connect to the Postgres connection
    hook = PostgresHook(postgres_conn_id='postgres')

    # Load the CSV data into a DataFrame
    processed_data = pd.read_csv('/tmp/processed_data.csv')

    # Check if data for each date already exists in the database
    for index, row in processed_data.iterrows():
        date_value = row['date']
        # Query to check if data for the date already exists
        check_query = f"SELECT COUNT(*) FROM covidtracking WHERE date = '{date_value}'"
        result = hook.get_first(check_query)
        count = result[0] if result else 0
        # If data for the date already exists, skip insertion
        if count > 0:
            print(f"Data for date {date_value} already exists. Skipping insertion.")
            continue
        else:
            # Insert data into the database
            hook.insert_rows(table="covidtracking", rows=[row.tolist()])


	
with DAG('covidtracking_pipeline', default_args=default_args, schedule_interval=timedelta(minutes=60), catchup=False) as dag:
    # Dag #1 - Check if the API is available
    is_api_available = HttpSensor(
        task_id='is_api_available',
        method='GET',
        http_conn_id='is_api_available',
        endpoint='/v1/status.json',
        response_check=lambda response: response.json().get('production', False) == True,
        poke_interval=5
    )

    # Dag #2 - Create a table
    create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id='postgres',
        sql='''
            drop table if exists covidtracking;
            CREATE TABLE covidtracking (
				Date DATE PRIMARY KEY,
				States BIGINT,
				TotalCases BIGINT,
				TotalTesting BIGINT,
				CurrentlyHospitalized BIGINT,
				CurrentlyInICU BIGINT,
				CurrentlyOnVentilator BIGINT,
				TotalDeaths BIGINT,
				CreatedBy VARCHAR(50),
				CreatedOn TIMESTAMP,
				ModifiedBy VARCHAR(50),
				ModifiedOn TIMESTAMP
			);

        '''
    )

	# Dag #3 - Extract Data
    #def extract_data_func():
    #   url = 'https://api.covidtracking.com/v2/us/daily/'
    #    response = requests.get(url + date.today().strftime('%Y-%m-%d') + "/simple.json")
    #    return response.json()
	#
    #extract_data = PythonOperator(
    #    task_id='extract_data',
    #    python_callable=extract_data_func,
    #    provide_context=True
    #)


# DAG #3 - Extract Data
    extract_data = SimpleHttpOperator(
            task_id = 'extract_user',
            http_conn_id='is_api_available',
            method='GET',
            endpoint= '/v2/us/daily/2020-06-01'+'/simple.json',
            response_filter=lambda response: json.loads(response.text),
            log_response=True
    )


# Dag #4 - Process data
    transform_data = PythonOperator(
        task_id = 'transform_data',
        python_callable=_process_data

    )

# Dag #5 - Load data
    load_data = PythonOperator(
        task_id = 'load_data',
        python_callable=_store_data

    )
	
    # Dependencies
    is_api_available >> create_table >> extract_data >> transform_data >> load_data


#from datetime import datetime, timedelta

# Define start and end dates
#start_date = datetime(2020, 6, 1)
#end_date = datetime(2021, 3, 7)

# Define the format for the date string in the URL
#date_format = '%Y-%m-%d'

# Construct the endpoint URL
#endpoint = f'/v2/us/daily/{start_date.strftime(date_format)}/{end_date.strftime(date_format)}/simple.json'

# DAG #3 - Extract Data
#extract_data = SimpleHttpOperator(
#    task_id='extract_user',
#    http_conn_id='is_api_available',
#    method='GET',
#    endpoint=endpoint,
#    response_filter=lambda response: json.loads(response.text),
#    log_response=True
#)
