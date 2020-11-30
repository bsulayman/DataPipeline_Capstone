import configparser
import os
from pyspark.sql import SparkSession, types as T
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, date_format, dayofweek
from pyspark.sql.types import DateType, StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType
from pathlib import Path
import utils

config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS CREDS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS CREDS']['AWS_SECRET_ACCESS_KEY']
immigration_input_data = config['IO SOURCE']['immigration_input_data']
output_data = config['IO SOURCE']['output_data']


def create_spark_session():
    """
    Get/ create sas spark session
    
    Arguments: 
        None
    Returns: 
        Spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.3") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def process_immigration_data(spark):
    """
    Extract and load immigration data in local folder then transform it fact_immigration table in S3. 
    
    Arguments:
        spark - Spark session
        
    Returns:
        None
    """
    
    # create fact immigration table
    immigration_df = utils.create_immigration_data(spark, immigration_input_data)
    
    # save fact_immigration data to parquet files in S3
    utils.save_fact_immigration(immigration_df, output_data)
    

def process_city_demographics(spark):
    """
    Extract and load city demographics data from local storage then transform it to city_demographics table in S3. 
    
    Arguments:
        spark - Spark session
    
    Returns:
        None
    """

    # create city demographic table 
    city_demographics_df = utils.create_city_demographic(spark)
    
    # save city_demographic data to parquet files in S3
    utils.save_city_demographic(city_demographics_df, output_data)

def process_state_temperature(spark):
    """
    Extract and load state temperature data from local storage then transform it to state_temperature table in S3. 
    
    Arguments:
        spark - Spark session
    
    Returns:
        None
    """

    # create state_temp_table dataframe
    recent_state_temp_df = utils.create_state_temp_table(spark)
    
    # save state temperature to parquet files in S3
    utils.save_state_temp(recent_state_temp_df, output_data)
    
    
def process_country_code(spark):
    """
    Extract and load country code data from local storage to country_code table in S3. 
    
    Arguments:
        spark - Spark session
    
    Returns:
        None
    """
    
    # read country code from csv file
    df = spark.read.option("header",True).option("inferSchema", True).csv("data/country_code.csv")
    
    # save country code data to parquet files in S3
    utils.save_country_code(df, output_data)
    
    
def main():
    spark = create_spark_session()
    
    process_immigration_data(spark)    
    process_city_demographics(spark)
    process_state_temperature(spark)
    process_country_code(spark)


if __name__ == "__main__":
    main()

