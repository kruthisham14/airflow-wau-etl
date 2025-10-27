from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
#from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging 
import snowflake.connector

def connect_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id = 'snowflake_conn')

    conn = hook.get_conn()
    return conn.cursor()

@task
def set_stage():
    cur = connect_snowflake_conn()
    try:
        cur.execute("BEGIN;")

        # Tables creation
        cur.execute("""
                       CREATE TABLE IF NOT EXISTS raw.user_session_channel (
                        userId int not NULL,
                        sessionId varchar(32) primary key,
                        channel varchar(32) default 'direct'  
                                ); 
                       """)
        cur.execute("""
                        CREATE TABLE IF NOT EXISTS raw.session_timestamp (
                        sessionId varchar(32) primary key,
                        ts timestamp  
                                );
                       """)
        
        # Creating stage to load data
        cur.execute("""
                       CREATE OR REPLACE STAGE raw.blob_stage
                        url = 's3://s3-geospatial/readonly/'
                        file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"'); 
                       """)
        
        cur.execute("COMMIT;")

        print("Created Tables Successfully!!")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise 

@task
def load():
    cur = connect_snowflake_conn()
    try:    

        cur.execute("BEGIN;") 
        # To load the data using COPY INTO
        cur.execute("""
                       COPY INTO raw.user_session_channel
                        FROM @raw.blob_stage/user_session_channel.csv; 
                       """)
        
        cur.execute("""
                       COPY INTO raw.session_timestamp
                       FROM @raw.blob_stage/session_timestamp.csv; 
                       """)
        
        cur.execute("COMMIT;")

        #print("Created Tables Successfully!!")

        print("Loaded Data successfully!!")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise 

with DAG(
    dag_id = "SessionToSnowflake",
    start_date = datetime(2025,10,25),
    catchup=False,
    tags=['ETL'],
    schedule= '30 2 * * *'
) as dag:
    
    create_task = set_stage()
    load_task = load()