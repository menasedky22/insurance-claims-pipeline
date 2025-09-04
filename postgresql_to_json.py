# postgresql_to_json.py
# this script will extract data from PostgreSQL and transform it into a list of dictionaries# and load it into MongoDB Atlas        
#  "mongodb+srv://menasedky22_db_user:Mena1234@cluster0.cfk3zsw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")

# postgresql_to_json.py




import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient

# -----------------------------
# 1️⃣ Extract from PostgreSQL
# -----------------------------

def extract_data():
    
    engine = create_engine("postgresql+psycopg2://postgres:Mena123@host.docker.internal:5432/insurance")
   # engine = create_engine('postgresql+psycopg2://postgres:Mena123@localhost:5432/insurance')
   # engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
    claims_df = pd.read_sql("SELECT * FROM claims", engine)
    policies_df = pd.read_sql("SELECT * FROM policies", engine)
    
    # convert IDs to string to avoid issues in MongoDB
    claims_df['policy_id'] = claims_df['policy_id'].astype(str)
    policies_df['policy_id'] = policies_df['policy_id'].astype(str)
    
    return claims_df, policies_df

# -----------------------------
# 2️⃣ Transform 
# -----------------------------
def transform_data(claims_df, policies_df):
    # merge
    merged_df = claims_df.merge(
        policies_df,
        on='policy_id',
        how='left',
        suffixes=('_claim', '_policy')
    )
    
    # transform dates to native Python datetime
    date_cols = ['claim_date', 'start_date', 'end_date']
    for col in date_cols:
        if col in merged_df.columns:
            merged_df[col] = pd.to_datetime(merged_df[col], errors='coerce')
            merged_df[col] = merged_df[col].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)
    
    # remove columns that are completely empty
    merged_df = merged_df.dropna(axis=1, how='all')
    
    # transform to list of dicts
    data_dict = merged_df.to_dict(orient='records')
    
    for record in data_dict:
         # set MongoDB _id to claim_id
        record['_id'] = record['claim_id']
        
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
            elif isinstance(value, pd.Timestamp):
                record[key] = value.to_pydatetime()
    
    return data_dict
# -----------------------------
# 3️⃣ Load -> MongoDB
# -----------------------------
def load_to_mongo(json_data):
    client = MongoClient("mongodb+srv://menasedky22_db_user:Mena1234@cluster0.cfk3zsw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = client["insurance_db"]
    collection = db["claims_with_policies"]
    
    # remove existing data
    collection.drop()
    
    # insert new data
    try:
        collection.insert_many(json_data)
        print("Data inserted successfully!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()

# -----------------------------
# 4️⃣ Main ETL Flow
# -----------------------------
if __name__ == "__main__":
    claims_df, policies_df = extract_data()
    json_data = transform_data(claims_df, policies_df)
    load_to_mongo(json_data)

    print("ETL process completed!")