import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

raw = os.getenv("RAW_DIR")
bronze = os.getenv("BRONZE_DIR")


files = {
    "employees": "employees.csv",
    "salaries": "salaries.csv",
    "departments": "departments.csv"
}

def validate(df,name):
    if df.empty:
        raise ValueError(f"{name} is empty!")
    
    nulls = df.isnull().sum()
    if nulls.any():
        print(f"Null values found: {name}:\n{nulls}")
    
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        print(f"{duplicates} duplicate rows found: {name}")
    
    return df

def process_bronze():
    os.makedirs(bronze, exist_ok=True)

    for name, filename in files.items():
        path = os.path.join(raw, filename)
        df = pd.read_csv(path)
        df = validate(df, name)
        
        bronze_path = os.path.join(bronze, f"{name}.parquet")
        df.to_parquet(bronze_path, index=False)
        print(f"Saved {name} to {bronze_path}")

if __name__ == "__main__":
    process_bronze()