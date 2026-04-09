from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException
from datetime import datetime
import pandas as pd
import requests
from io import StringIO

CSV_URL = "https://raw.githubusercontent.com/neha03020/astro_nba_pipeline/main/data/nba_fan_data.csv"

def extract_fan_data():
    """Extract fan data from CSV URL"""
    response = requests.get(CSV_URL, timeout=30)
    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text))
    print("Fan Data Preview:\n", df.head())
    print(f"Total rows extracted: {len(df)}")

    return df.to_json(orient='records', date_format='iso')


def load_to_snowflake(ti):
    """Load fan data to Snowflake using parameterized queries"""
    # Pull data from XCom
    data = ti.xcom_pull(task_ids="extract_task")
    df = pd.read_json(data, orient='records')
    
    print(f"Loading {len(df)} rows to Snowflake...")

    # Get Snowflake connection
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Use parameterized query to prevent SQL injection
    insert_query = """
    INSERT INTO nba_db.fan_schema.fan_engagement
    (fan_id, fan_name, team, engagement_type, platform, 
     interaction_time, region, spend_amount)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Prepare data for bulk insert
    records = [
        (
            int(row['fan_id']),
            str(row['fan_name']),
            str(row['team']),
            str(row['engagement_type']),
            str(row['platform']),
            str(row['interaction_time']),
            str(row['region']),
            float(row['spend_amount'])
        )
        for _, row in df.iterrows()
    ]
    
    # Bulk insert
    cursor.executemany(insert_query, records)
    
    # Commit the transaction
    conn.commit()
    
    print(f"Successfully loaded {len(records)} rows to Snowflake")
    
    cursor.close()
    conn.close()


with DAG(
    dag_id="nba_fan_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['nba', 'snowflake', 'etl'],
    description='ETL pipeline to load NBA fan data to Snowflake'
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_fan_data,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_to_snowflake,
    )

    extract_task >> load_task
