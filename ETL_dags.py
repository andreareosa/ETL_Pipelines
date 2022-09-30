#Data Engineering Foundations: ETL Pipelines - Schedule the ETL using Airflow
#André Areosa - 30/09/2022

import airflow
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import dateime, timedelta
from transformation import *

#define the ETL Function
def etl():
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    agg_df = transform_avg_ratings(movies_df,users_df)
    load_aggdf_to_db(agg_df)


#define the arguments for the DAG
dafult_args = {
    'owner': 'andreareosa'
    ,'start_date': airflow.utils.dates.days_ago(1)
    ,'depends_on_past': True
    ,'email': ['andre_etl_case@example.com']
    ,'email_on_failure': True
    ,'email_on_retry': False
    ,'retries': 3
    ,'retry_delay': timedelta(minutes=30)
}

#instantiate the DAG
dag = DAG(
            dag_id = 'elt_pipeline'
            ,default_args = dafult_args
            ,schedule_interval='0 0 * * *' #cronn time expression: (minute, hour, day of the month, day of the month, week, day of the week) 
)

#define the etl task
etl_task = PythonOperator(
            task_id = 'elt_task'
            ,python_callable = etl
            ,dag=dag
)

etl()