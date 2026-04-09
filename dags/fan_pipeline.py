from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import requests
from io import StringIO

CSV_URL = "https://raw.githubusercontent.com/YOUR_USERNAME/astro-mba-pipeline/main/data/mba_fan_data.csv"


def extract_fan_data():
    response = requests.get(CSV_URL)
    response.raise_for_status()

    df = pd.read_csv(StringIO(response.text))
    print("Fan Data Preview:\n", df.head())

    return df.to_json()


def load_to_snowflake(ti):
    data = ti.xcom_pull(task_ids="extract_task")
    df = pd.read_json(data)

    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        query = f"""
        INSERT INTO mba_db.fan_schema.fan_engagement
        VALUES (
            {int(row['fan_id'])},
            '{row['fan_name']}',
            '{row['team']}',
            '{row['engagement_type']}',
            '{row['platform']}',
            '{row['interaction_time']}',
            '{row['region']}',
            {float(row['spend_amount'])}
        )
        """
        cursor.execute(query)

    cursor.close()


with DAG(
    dag_id="mba_fan_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
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
