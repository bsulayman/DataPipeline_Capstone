from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Budi Sulayman',
    'start_date': datetime(2020, 11,29),
    'Depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('load_transform_data_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 7 * * *'
        )

create_tables = PostgresOperator(
    task_id='Create_tables',
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)

load_immigration = StageToRedshiftOperator(
    task_id='Load_immigration',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="fact_immigration",
    s3_bucket="udacitycapstoneproj/data",
    s3_key="immigration/immigrations.parquet",
)

stage_state_temperature = StageToRedshiftOperator(
    task_id='Stage_state_temperature',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_state_temperature ",
    s3_bucket="udacitycapstoneproj/data",
    s3_key="state_temperature/steates.parquet",
)

load_city_demographic = StageToRedshiftOperator(
    task_id='Load_city_demographic',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="city_demographics",
    s3_bucket="udacitycapstoneproj/data",
    s3_key="city_demographic/cities.parquet",
)

load_country_code = StageToRedshiftOperator(
    task_id='LOad_country_code',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="country_code",
    s3_bucket="udacitycapstoneproj/data",
    s3_key="country_code/countries.parquet",
)

load_state_temperature = LoadDimensionOperator(
    task_id='Load_state_temp_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="state_temperature",
    append_data=False,
    sql_query=SqlQueries.state_temperature_table_insert
)

load_date_time_table = LoadDimensionOperator(
    task_id='Load_date_time_table',
    dag=dag,
    redshift_conn_id="redshift",
    destination_table="date_time",
    append_data=False,
    sql_query=SqlQueries.date_time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tests=[
              {"table":"fact_immigration",
              "sql":"SELECT COUNT(*) FROM fact_immigration WHERE cic_id is null;",
              "expected_result":0},
              {"table":"state_temperature",
              "sql":"SELECT COUNT(*) FROM state_temperature WHERE avg_temperature is null;",
              "expected_result":0},
              {"table":"fact_immigration",
              "sql":"SELECT code, country FROM country_code GROUP BY code, country HAVING count(code) > 1;",
              "expected_result":''}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

create_tables >> [load_immigration, stage_state_temperature, load_country_code, load_city_demographic]
stage_state_temperature >> load_state_temperature
load_immigration >> load_date_time_table
[load_immigration, load_country_code, load_city_demographic, load_state_temperature, load_date_time_table] >> run_quality_checks
run_quality_checks >> end_operator