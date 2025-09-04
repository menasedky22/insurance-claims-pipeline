# etl_claims_pipeline.py
# DAG to automate ETL process using Airflow
# This DAG will run daily to extract, transform, and load data
# from PostgreSQL to MongoDB

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import pandas as pd
import logging

# -----------------------------
# 1️⃣ Import functions
# -----------------------------
sys.path.append('/opt/airflow/dags')  #
from postgresql_to_json import extract_data, transform_data, load_to_mongo
    
# -----------------------------
# 2️⃣ Default args & DAG
# -----------------------------
default_args = {
    'owner': 'mena',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'etl_claims_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for claims and policies',
    schedule="@daily",
    start_date=datetime(2025, 9, 3),
    catchup=False,
    tags=['etl', 'claims', 'insurance'],
)

# -----------------------------
# 🟢 Task 1 - Extract
# -----------------------------
def extract_task(**context):
    claims_df, policies_df = extract_data()
    logging.info(f"✅ Extracted {len(claims_df)} claims and {len(policies_df)} policies")
    context['ti'].xcom_push(key='claims_df', value=claims_df.to_json())
    context['ti'].xcom_push(key='policies_df', value=policies_df.to_json())

extract_op = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    dag=dag,
)

# -----------------------------
# 🟡 Task 2 - Transform
# -----------------------------
def transform_task(**context):
    claims_df = pd.read_json(context['ti'].xcom_pull(task_ids='extract', key='claims_df'))
    policies_df = pd.read_json(context['ti'].xcom_pull(task_ids='extract', key='policies_df'))
    
    json_data = transform_data(claims_df, policies_df)
    logging.info(f"🛠️ Transformed data into {len(json_data)} JSON records")
    context['ti'].xcom_push(key='json_data', value=json_data)

transform_op = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    dag=dag,
)

# -----------------------------
# 🔵 Task 3 - Load
# -----------------------------
def load_task(**context):
    json_data = context['ti'].xcom_pull(task_ids='transform', key='json_data')

    # ✅ Add created_at timestamp
    for record in json_data:
        record['created_at'] = datetime.utcnow().isoformat()

    load_to_mongo(json_data)
    logging.info(f"📥 Loaded {len(json_data)} records into MongoDB with created_at timestamp")

load_op = PythonOperator(
    task_id='load',
    python_callable=load_task,
    dag=dag,
)

# -----------------------------
# 4️⃣ Task Dependencies
# -----------------------------
extract_op >> transform_op >> load_op
