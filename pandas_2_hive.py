import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import requests
import json
import csv
import pandas as pd
from datetime import datetime


default_args = {
    'owner': 'airflow',
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    #'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    #'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def getDataToLocal():
  
    
    url = "https://newsapi.org/v2/everything?q=tesla&from=2022-06-11&sortBy=publishedAt&apiKey=c1a984dc8fb64f56b6b030f3991b6eb2"
    response = requests.get(url)
    
    newObj = {
        'sourcename': [],
        'author': [],
        'title': [],
        'description': [],
        'url':[],
        'publishedAt': [],
        'content': [],
            }

    for obj in json.loads(response.content).get('articles'):
        newObj['sourcename'].append(obj.get('source').get('name'))
        newObj['author'].append(obj.get('author'))
        newObj['title'].append(obj.get('title'))
        newObj['description'].append(obj.get('description'))
        newObj['url'].append(obj.get('url'))
        newObj['publishedAt'].append(datetime.strptime(obj.get('publishedAt'), '%Y-%m-%dT%H:%M:%SZ'))
        newObj['content'].append(obj.get('content'))
    
    df = pd.DataFrame(newObj)
    
    df.to_csv("/home/parallels/api.csv", sep=',' ,escapechar='\\', quoting=csv.QUOTE_ALL, encoding='utf-8' )

getDataToLocal()

dag_pandas_to_hive = DAG(
    dag_id = "airflow_db",
    default_args=default_args,
    #schedule_interval='0 0 * * *',
    schedule_interval='@once',  
    dagrun_timeout=timedelta(minutes=60),
    description='use case of pandas  in airflow',
    start_date = airflow.utils.dates.days_ago(1))

getDataToLocal = PythonOperator(
         task_id='getDataToLocal', 
         python_callable=getDataToLocal,
         dag=dag_pandas_to_hive
        )


create_table_hql_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS testdb.news (
sourcename STRING,
author STRING,
title VARCHAR(50),
description VARCHAR(1000),
url VARCHAR(1000),
publishedAt VARCHAR(255),
content VARCHAR(65000)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE; """

create_table = HiveOperator(
        hql = create_table_hql_query,
        task_id = "create_table_task",
        hive_cli_conn_id = "airflow_db",
        dag = dag_pandas_to_hive
        )

load_data_hql_query = """
LOAD DATA LOCAL INPATH '/home/parallels/api.csv' INTO TABLE testdb.news;
""" 
load_data_from_local =  HiveOperator(
        hql = load_data_hql_query,
        task_id = "load_data_from_local_task",
        hive_cli_conn_id = "airflow_db",
        dag = dag_pandas_to_hive
        )

getDataToLocal>>create_table>>load_data_from_local

if __name__ == '__main__ ':
  dag_pandas_to_hive.cli()

