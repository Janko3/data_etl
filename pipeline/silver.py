import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv



load_dotenv()

bronze = os.getenv("BRONZE_DIR")
silver = os.getenv("SILVER_DIR")

reference_date = pd.to_datetime("2023-03-31")

def calculate_tenure_in_months(start_date):
    if pd.isnull(start_date):
        return None
    return (reference_date.year - start_date.year) * 12 + (reference_date.month - start_date.month)


def process_silver():
    os.makedirs(silver, exist_ok=True)
    employees = pd.read_parquet(os.path.join(bronze, "employees.parquet"))
    salaries = pd.read_parquet(os.path.join(bronze, "salaries.parquet"))
    departments = pd.read_parquet(os.path.join(bronze, "departments.parquet"))

    employees["start_date"] = pd.to_datetime(employees["start_date"], errors="coerce")
    employees["tenure_in_months"] = employees["start_date"].apply(calculate_tenure_in_months)

    silver_df = salaries.merge(employees, on="employee_id", how="left")
    silver_df = silver_df.merge(departments, on="department", how="left")

    silver_df.to_parquet(os.path.join(silver, "silver_data.parquet"), index=False)
    print("Silver layer:", os.path.join(silver, "silver_data.parquet"))

if __name__ == "__main__":
    process_silver()
