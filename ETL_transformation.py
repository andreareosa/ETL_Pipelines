#Data Engineering Foundations: ETL Pipelines
#André Areosa - 29/09/2022

##More information about how to read data from PostgreSQL with Pyspark:
##https://www.projectpro.io/recipes/read-data-from-postgresql-pyspark

##import required libraries
import pyspark  

#1 - DATA EXTRACTION
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

#2 - DATA TRANSFORMATION
##Analytical databases are optimized for querying aggregated data so we are going to merge the two dataframes so that we can have the average user rating per movie
def transform_avg_ratings(dataframe1,dataframe2):
    
    avg_rating = dataframe2.groupby('id').mean('rating')

    ##Create an aggregated dataframe by joining the movies_df and avg_ratings table on id column
    agg_df = dataframe1.join(
         avg_rating
        ,dataframe1.id == avg_rating.id
    ).drop(avg_rating.id) ##remove duplicated column id
    
    return agg_df


#3 - LOAD THE AGGREGATED DATA INTO THE DATABASE
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

