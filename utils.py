from datetime import datetime, timedelta
from pyspark.sql import SparkSession, types as T
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, date_format, dayofweek
from pyspark.sql.types import DateType, StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType
from pathlib import Path

def convert_datetime(x):
    """
    Convert x to DateTime
    
    Arguments:
        x - numeric value
    
    Returns:
        DateTime
    """
    
    try:
        start = datetime(1960, 1, 1)
        return start + timedelta(days=int(x))
    except:
        return None

def create_immigration_data(spark, immigration_input_data):
    """
    Extract and load immigration SAS data from local folder then transform it to fact_immigration_table. 
    
    Arguments:
        spark - Spark Session
        immigration_input_data - Location of immigration data in local folder
    
    Returns:
        fact_immigration_table_df - Fact immigration table dataframe
    """
    
    # create initial fact_immigration dataframe with schema
    schema = StructType([
                          StructField('cicid', DoubleType(), False),
                          StructField('i94cit', DoubleType(), True),
                          StructField('i94res', DoubleType(), True),
                          StructField('arrdate', DoubleType(), True),
                          StructField('i94mode', DoubleType(), True),
                          StructField('i94addr', StringType(), True),
                          StructField('i94bir', DoubleType(), True),
                          StructField('i94visa', DoubleType(), True),
                          StructField('biryear', DoubleType(), True),
                          StructField('gender', StringType(), True),
                          StructField('visatype', StringType(), True),
                        ])
    fact_immigration_temp_df = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
    
    # get filepath to immigration data file
    paths = Path(immigration_input_data).glob('**/*.sas7bdat')
    for path in paths:
        # get string of the path
        immigration_data = str(path)
    
        # read immigration data file
        df = spark.read.format('com.github.saurfang.sas.spark').load(immigration_data)

        # extract columns to create fact_immigration table
        fact_immigration_df = df.select("cicid", 
                                        "i94cit", 
                                        "i94res", 
                                        "arrdate", 
                                        "i94mode", 
                                        "i94addr", 
                                        "i94bir", 
                                        "i94visa", 
                                        "biryear", 
                                        "gender", 
                                        "visatype").drop_duplicates()
        
        fact_immigration_temp_df = fact_immigration_temp_df.union(fact_immigration_df).distinct()
    
    fact_immigration_temp_df = fact_immigration_temp_df.union(fact_immigration_df).distinct()
    
    # convert arrdate to datetime data type and add it to new 'arrival_date' column
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())
    fact_immigration_temp_df = fact_immigration_temp_df.withColumn("arrival_date", udf_datetime_from_sas("arrdate"))
    
    # drop arrdate column
    fact_immigration_temp_df = fact_immigration_temp_df.drop("arrdate")
    
    #rename the columns to match with table schema in redshift
    fact_immigration_temp_df.createOrReplaceTempView("fact_immigration_temp")
    fact_immigration_table_df = spark.sql("""SELECT DISTINCT BIGINT(cicid) AS cic_id, 
                                                INT(i94cit) AS born_country,
                                                INT(i94res) AS res_country,
                                                INT(i94mode) AS i94_mode,
                                                STRING(i94addr) AS arr_state,
                                                INT(i94bir) AS age,
                                                INT(i94visa) AS i94_visa,
                                                INT(biryear) AS birth_year,
                                                STRING(gender),
                                                STRING(visatype) AS visa_type,
                                                DATE(arrival_date)
                                             FROM fact_immigration_temp
    """)
    
    return fact_immigration_table_df
        
def save_fact_immigration(immigration_df, output_data):
    """
    Save fact_immigration_table to parquet files in S3. 
    
    Arguments:
        immigration_df - Fact immigration table dataframe
        output_data - Location of parquet files output data
    
    Returns:
        None
    """
    
    # write fact_immigration data to parquet files partitioned by year and month in S3
    immigration_df = immigration_df.withColumn("month", month(immigration_df.arrival_date)).withColumn("year", year(immigration_df.arrival_date))
    
    immigration_df.write.partitionBy("year", "month").mode('overwrite').parquet(output_data + "immigration/immigrations.parquet")
    
def create_city_demographic(spark):
    """
    Transform city demographic data to city_demographics dimension table. 
    
    Arguments:
        df - City demographic dataframe
    
    Returns:
        city_demographics_df - City demographics table dimension dataframe
    """

    # read us cities demographics from csv file
    df = spark.read.option("delimiter",";").option("header",True).option("inferSchema", True).csv("data/us-cities-demographics.csv")
    
    # reorder and rename the column names  
    df = df.select(col("City").alias("city"),
                    col("State Code").alias("state_code"),
                    col("State").alias("state"),
                    col("Median Age").alias("med_age"),
                    col("Male Population").alias("male_pop"),
                    col("Female Population").alias("female_pop"),
                    col("Total Population").alias("tot_pop"),
                    col("Number of Veterans").alias("num_veterans"),
                    col("Foreign-born").alias("foreign_born"),
                    col("Average Household Size").alias("avg_household"),
                    col("Race").alias("race"),
                    col("Count").alias("count")
                  )
    
    # cast the table to the right data type
    df.createOrReplaceTempView("df_temp")
    city_demographics_df = spark.sql("""SELECT DISTINCT STRING(city),
                                                        STRING(state_code),
                                                        STRING(state),
                                                        FLOAT(med_age),
                                                        BIGINT(male_pop),
                                                        BIGINT(female_pop),
                                                        BIGINT(tot_pop),
                                                        BIGINT(num_veterans),
                                                        BIGINT(foreign_born),
                                                        FLOAT(avg_household),
                                                        STRING(race),
                                                        BIGINT(count)
                                        FROM df_temp""")

    return city_demographics_df
    
def save_city_demographic(city_dem_df, output_data):
    """
    Save city demographics data to parquet files in S3. 
    
    Arguments:
        city_dem_df - City demographics dataframe
        output_data - Location of parquet files output data
    
    Returns:
        None
    """
        
    # write city_demographic data to parquet files partitioned by state and city in S3
    city_dem_df.write.partitionBy("state_code").mode('overwrite').parquet(output_data + "city_demographic/cities.parquet")
    
def create_state_temp_table(spark):
    """
    Transform state temperature data to United States' state average temperature in the last 10 years dimension table. 
    
    Arguments:
        spark - Spark session
        df - State temperature dataframe
    
    Returns:
        recent_state_avg_temp - State temperature dimension table dataframe
    """
        
    # read state temperature from csv file
    df = spark.read.option("header",True).option("inferSchema", True).csv("data/GlobalLandTemperaturesByState.csv")
    
    # select only the United States temperature data
    df = df.select(col("dt"),
                   col("AverageTemperature").alias("avg_temp"),
                   col("State").alias("state"),
                  ).filter("Country == 'United States'")
    
    # add month and year column
    df = df.withColumn("month", month(df.dt))\
           .withColumn("year", year(df.dt))\
           .withColumn("partition_state", col("state"))
    
    # select only the last 10 year of data
    df.createOrReplaceTempView("temp_df")
    recent_state_temp_df = spark.sql("""
        SELECT DISTINCT DATE(dt), STRING(state), FLOAT(avg_temp), INT(month), INT(year), STRING(partition_state)
        FROM temp_df 
        WHERE year <= (SELECT MAX(year) FROM temp_df) AND year > (SELECT MAX(year) FROM temp_df)-10'
    """)
    
    return recent_state_temp_df

def save_state_temp(state_temp_df, output_data):
    """
    Save United States' state temperature dimension table dataframe to parquet files in S3. 
    
    Arguments:
        state_temp_df - State temperature dataframe
        output_data - Location of parquet files output data
    
    Returns:
        None
    """
        
    # write state tempearature data to parquet files partitioned by state and city in S3
    state_temp_df.write.partitionBy("partition_state").mode('overwrite').parquet(output_data + "state_temperature/states.parquet")
    
def save_country_code(country_code_df, output_data):
    """
    Save country code table dataframe to parquet files in S3. 
    
    Arguments:
        country_code_df - Country code dataframe
        output_data - Location of parquet files output data
    
    Returns:
        None
    """
        
    # write state tempearature data to parquet files partitioned by state and city in S3
    country_code_df.write.mode('overwrite').parquet(output_data + "country_code/countries.parquet")