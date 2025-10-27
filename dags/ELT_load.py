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
def run_ctas(schema, table, select_sql, primary_key = None):
    logging.info(table)
    logging.info(select_sql)

    cur = connect_snowflake_conn()

    try:
        sql = f"CREATE OR REPLACE TABLE {schema}.temp_{table} AS {select_sql}"
        logging.info(sql)
        cur.execute(sql)

        if primary_key is not None:
            sql = f"""
                    SELECT {primary_key}, COUNT(1) AS cnt
                    FROM {schema}.temp_{table}
                    GROUP BY 1
                    ORDER BY 2 DESC
                    LIMIT 1
                    """
            print(sql)
            cur.execute(sql)
            result = cur.fetchone()
            print(result, result[1])
            if int(result[1]) > 1:
                print("!!!")
                raise Exception(f"Primary Key uniqueness failed: {result}")
            
            main_table_creation = f"""
                                    CREATE TABLE IF NOT EXISTS {schema}.{table} AS SELECT * FROM {schema}.temp_{table} WHERE 1 = 0;"""
            
            cur.execute(main_table_creation)

            swap_sql = f"""
                        ALTER TABLE {schema}.{table} SWAP WITH {schema}.temp_{table};
                        """
            cur.execute(swap_sql)

    except Exception as e:
        raise 


with DAG(
    dag_id= 'Build_ETL_CTAS',
    start_date= datetime(2025,10,25),
    catchup=False,
    tags=['ELT'],
    schedule= '30 1 * * *'
) as dag:
    
    schema = 'analytics'
    table = 'session_summary'
    select_sql = """
                    SELECT u.*, s.ts
                    FROM raw.user_session_channel u
                    JOIN raw.session_timestamp s ON u.sessionId = s.sessionId
                    """
    
    run_ctas(schema, table, select_sql, primary_key= 'sessionId')