from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import logging

# 1. ฟังก์ชันดึงข้อมูลปี 2018 จาก API (1,000 แถวแรก)
def ingest_real_taxi_data(**context):
    """
    ดึงข้อมูล 2018 Yellow Taxi Trip Data จาก NYC Open Data API 
    จำนวน 1,000 แถวแรก
    """
    # เปลี่ยน Dataset ID เป็นของปี 2018 (t29m-gskq)
    api_url = "https://data.cityofnewyork.us/resource/t29m-gskq.csv?$limit=1000"
    raw_csv_path = "/tmp/raw_taxi_data_2018.csv"
    
    logging.info(f"Downloading data from: {api_url}")
    
    df = pd.read_csv(api_url)
    df.to_csv(raw_csv_path, index=False)
    
    context['ti'].xcom_push(key="raw_path", value=raw_csv_path)
    logging.info(f"Successfully downloaded {len(df)} rows. Saved to {raw_csv_path}")

# 2. ฟังก์ชันทำความสะอาดข้อมูล (ใช้ลอจิก Location ID ของปี 2018)
def clean_taxi_data(**context):
    ti = context['ti']
    
    raw_path = ti.xcom_pull(key="raw_path", task_ids="ingest_taxi_data")
    if not raw_path:
        raise ValueError("Failed to pull 'raw_path' from XCom.")
        
    logging.info(f"Loading raw data from: {raw_path}")
    df = pd.read_csv(raw_path)
    initial_count = len(df)
    
    # Drop rows with null values
    df = df.dropna(subset=['fare_amount', 'trip_distance', 'tpep_pickup_datetime'])
    
    # Parse Datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
    df = df.dropna(subset=['tpep_pickup_datetime'])
    
    # Filter Fare & Distance
    df = df[(df['fare_amount'] > 0) & (df['fare_amount'] <= 500)]
    df = df[(df['trip_distance'] > 0) & (df['trip_distance'] <= 100)]
    
    # Filter Location IDs (ข้อมูล 2018 ใช้ PULocationID / DOLocationID)
    # รหัสโซนที่ถูกต้องอยู่ระหว่าง 1 ถึง 265
    df = df[df['pulocationid'].between(1, 265) & df['dolocationid'].between(1, 265)]
    
    final_count = len(df)
    logging.info(f"Data cleaning complete. Removed: {initial_count - final_count} rows. Final count: {final_count}")

    # เปลี่ยนชื่อไฟล์ผลลัพธ์เป็น 2018
    clean_path = '/tmp/nyc_taxi_clean_2018.csv'
    df.to_csv(clean_path, index=False)
    ti.xcom_push(key="clean_path", value=clean_path)
    logging.info(f"Cleaned data saved to: {clean_path}")
    
    return clean_path

# 3. กำหนดโครงสร้าง DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'catchup': False
}

with DAG(
    dag_id='taxi_data_cleaning_pipeline',
    default_args=default_args,
    schedule='@daily',
    description='A pipeline to clean 1000 rows of NYC 2018 taxi data',
) as dag:
    
    task_ingest = PythonOperator(
        task_id='ingest_taxi_data',
        python_callable=ingest_real_taxi_data
    )

    task_clean = PythonOperator(
        task_id='clean_taxi_data',
        python_callable=clean_taxi_data
    )

    task_ingest >> task_clean