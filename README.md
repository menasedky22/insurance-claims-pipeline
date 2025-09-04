# Insurance ETL Pipeline

## Project Overview
This project simulates an ETL pipeline for insurance claims data in an insurance platform.  
The pipeline handles CSV files, transforms data, and loads it into PostgreSQL and MongoDB Atlas using Airflow for orchestration.

## Tools Used
- Python  
- PostgreSQL (`insurancedb`, local engine)  
- Docker (to run Airflow)  
- Airflow (orchestration)  
- MongoDB Atlas (NoSQL destination)

## Data Sources
- CSV files: claims.csv , policies.csv  
- PostgreSQL table: `insurancedb` (local)

## ETL Steps

1. **Data Cleaning & Load to PostgreSQL (Outside Airflow)**
   - Python script cleans CSV files:
     - Handles missing values (e.g., `claim_type`)  
     - Normalizes `claim_date` to UTC  
     - Flags potentially invalid claims (e.g., negative `claim_amount`)  
     - Create cleaned File (`claim_cleaned.csv`)  
   - Cleaned data is loaded into **local PostgreSQL database (`insurancedb`)**

2. **Transformation & Orchestration (Airflow in Docker)**
   - Airflow DAG does the following:
     - Extracts data from PostgreSQL (local engine)
     - Transforms it into JSON format
     - Pushes intermediate JSON to **.xcom**
     - Loads final data into **MongoDB Atlas**

## How to Run

1. **Start Airflow container via Docker**  
   - Ensure Airflow can connect to local PostgreSQL engine and MongoDB Atlas
2. **Run Airflow DAG**  
   - DAG extracts, transforms, and loads data from PostgreSQL → JSON → MongoDB Atlas

## Diagrams
- ETL Pipeline: `ETL_Diagram.png`
