import pandas as pd
from datetime import datetime
import pytz

# Load CSV
df = pd.read_csv("claims.csv")

# 1. Normalize claim_date to UTC
df["claim_date"] = pd.to_datetime(df["claim_date"], errors="coerce")
df["claim_date"] = df["claim_date"].dt.tz_localize("Africa/Cairo", ambiguous="NaT", nonexistent="NaT")  # assuming local Cairo
df["claim_date"] = df["claim_date"].dt.tz_convert("UTC")

# 2. Handle missing claim_type
df["claim_type"] = df["claim_type"].fillna("other")

# 3. Flag invalid claims (negative claim_amount)
df["invalid_claim"] = df["claim_amount"] < 0

# Save cleaned data
df.to_csv("claims_cleaned.csv", index=False)

print("✅ Cleaning done! File saved as claims_cleaned.csv")

print(df.head())