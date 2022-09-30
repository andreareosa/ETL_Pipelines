# Data Engineering Foundations: Create an ETL pipeline


## Introduction

The aim of this project is to create an ETL pipeline to first extract data from a database, then transform and aggregate the data so that analytics can be derived easily and then load the transformed data back into the database.
I've created a SQL database with some dummy data to showcase the development of the ETL pipeline.

To keep things simple I've created a two table database schema using PostgreSQL for a movie rating application where movies table contains the information about the movies and users table contains the rating given by a user to a specific movie.


## Table of Contents

- [Create a database to use for the pipeline](#create-a-database)
- [Extract data from the database using pyspark](#Extract-data-from-the-database-using-pyspark)
- [Transform and optimize data](#Transform-and-optimize-data)
- [Load data back into the database](#Load-data-back-into-the-database)
- Scheduling ETL pipeline using Airflow

## Create a database

Create two different tables and populate them with some rows to use for the pipeline. As for the DBMS I used PostgreSQL but others can be used.

```sql
-- Create movies table
CREATE TABLE IF NOT EXISTS dbo.movies
(
    id int NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 )
    ,name varchar(40) NOT NULL
    ,description varchar(500) NOT NULL
    ,category varchar(40) NOT NULL
    ,CONSTRAINT movies_pkey PRIMARY KEY (id)
)
;

-- Insert data into movies table
INSERT INTO dbo.movies
VALUES
('Avatar','Avatar is a 2009 American epic science fiction film.','Sci-Fi')
,('Avengeres: Infinity War','Avengers: Infinity War is a 2018 American superhero film based on MCU','Sci-Fi')
,('Holidate','Holidate is a 2020 American romantic comedy holiday film','Romcom')
,('Extraction','Extraction is a 2020 American action-thriller film starring Chris Hemsworth','Action')	
,('Johm Wick','John Wick is a 2014 American neo-noir action film','Action')
;

-- Create users table
CREATE TABLE IF NOT EXISTS dbo.users
(
    id int NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 )
    ,movie_id int NOT NULL
    ,rating int NOT NULL
    ,CONSTRAINT users_pkey PRIMARY KEY (id)
    ,CONSTRAINT fk_movie 
		FOREIGN KEY (movie_id)
        	REFERENCES dbo.movies (id) MATCH SIMPLE
       			ON UPDATE NO ACTION
        		ON DELETE NO ACTION
)
;

-- Inser data into users table
INSERT INTO dbo.users
VALUES
(1,4)
,(2,5)
,(1,4)
,(3,3)
,(4,5)
```

<img src="https://i.ibb.co/F5D06Ff/Capture1.png">


## Extract data from the database using pyspark
You can use Spark to read the data from the tables and since I created the pipeline in Python I used the PySpark package.

The starting point is to create a spark session and pass some configuration parameters. Since I used the PostgreSQL database in the configuration parameters I needed to provide the path to where I have download the JDBC (allows Java programs to access database management systems) drivers for PostgreSQL to actually be able to extract data from the database. 
Then we use the spark session to read the jdbc file and we provide the url to our database and the table fromwhich we want to extract the data from.

More information about how to read data from PostgreSQL with Pyspark can be found [HERE!](https://www.projectpro.io/recipes/read-data-from-postgresql-pyspark)

```python
##import required libraries
import pyspark  

##create spark session
spark = pyspark.sql.SparkSession \
   .builder \
   .appName("Python Spark SQL basic example") \
   .config('spark.driver.extraClassPath', "C:/Users/andre/Downloads/postgresql-42.5.0.jar") \
   .getOrCreate()

##read the movies table from the database using spark jdbc (allows Java programs to access database management systems)
def extract_movies_to_df():

    movies_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/movie_ratings") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dbo.movies") \
        .option("user", "postgres") \
        .option("password", "areosa") \
        .load()

    return movies_df

##view the content of the table to validade the data
extract_movies_to_df().printSchema()
extract_movies_to_df().show()

##read the users table
def extract_users_to_df():

    users_df = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/movie_ratings") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "dbo.users") \
        .option("user", "postgres") \
        .option("password", "areosa") \
        .load()

    return users_df

extract_users_to_df().show()
```

## Transform and optimize data 
After the extraction from the two tables, we can perform a simple transformation to the extracted data. Since analytical databases are optimized for querying aggregated data I merged the two dataframes to get the average user rating per movie.

```python
def transform_avg_ratings(dataframe1,dataframe2):
    
    avg_rating = dataframe2.groupby('id').mean('rating')

    ##Create an aggregated dataframe by joining the movies_df and avg_ratings table on id column
    agg_df = dataframe1.join(
         avg_rating
        ,dataframe1.id == avg_rating.id
    ).drop(avg_rating.id) ##remove duplicated column id
    
    return agg_df
```

## Load data back into the database 
This step is to actually load the transformed data back into the database. We need to use the write function where we have to define the mode which in this case is overwrite, so if the table already exists, we would overwrite it, then provide the url to connect with our database and properties as we did while reading the data in the extraction step.

```python
def load_aggdf_to_db(agg_dataframe):
    
    mode = 'overwrite' #if the table already exits, we would override it
    url = 'jdbc:postgresql://localhost:5433/movie_ratings'
    properties = {
                  'user': 'postgres'
                 ,'password':'areosa'
                 ,'driver':'org.postgresql.Driver'
                 }

    agg_dataframe.write.jdbc(
                      url = url
                     ,table = 'dbo.average_ratings'
                     ,mode = mode
                     ,properties = properties
                     )

##Call the functions one by one in the correct order
if __name__ == '__main__':
    movies_df = extract_movies_to_df()
    users_df = extract_users_to_df()
    agg_df = transform_avg_ratings(movies_df,users_df)
    
    load_aggdf_to_db(agg_df)
```

At the beginning we had only two tables in the database but after the ETL process we now have dbo.average_ratings table avaiable as well.

<img src='https://i.ibb.co/ky36fwL/Capture.png'>

## Scheduling ETL pipeline using Airflow
In this last step, it's time to put everything together and schedule the jobs that have been defined so far. The scheduling tool used is Airflow that is the most commonly used scheduling tool at the moment.

Airflow's purpose is to automate and orchestrate our workflows for our data pipelines. The way it does this is it basically runs on a server all the time and when triggers happen or when schedules come in airflow will trigger these workflows or DAG's to happen.

First of all it's necessary to set up airflow on our machine.
NOTE: Airflow is built in Python but contains some libraries that will only work in Linux, so workarounds using virtual machines or Docker are required for fully-functional usage.

``` zsh
mkdir airflow
export AIRFLOW_HOME=~/ETL_data_engineering/airflow
sudo pip install apache-airflow
airflow db init

#Create an Admin user (https://airflow.apache.org/docs/apache-airflow/stable/security/webserver.html)
airflow users create \
    --username andreareosa \
    --firstname Andre \
    --lastname Areosa \
    --role Admin \
    --email andre_etl_case@example.com
```

After we need to create the airflow python script which is really just a configuration file. There's no processing happening here, everything that is related to data processing was already performed in the previous steps. This script is to configure our dag structure as code.

```python
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

```

We need to ensure that both the transformation and dag scripts are inside the airflow dags directory. We can simple create a dags directory under airflow and move both scripts there. Since the transformation script is being used inside dags both of the files need to be inside the dags folder.

Once all of this is done, we can start the web UI and we will receive an URL where the server ir running. Copy and paste in the browser.
```zsh
airflow webserver
```

Before entering the user and password we should start airflow scheduler as well because this is going to update or add our dag to the web UI.
```zsh
cd airflow
export AIRFLOW_HOME=~/ETL_data_engineering/airflow
airflow scheduler
```

<img src="https://i.ibb.co/ky1pnkH/Capture1.png">
