import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

silver = os.getenv("SILVER_DIR")
gold = os.getenv("GOLD_DIR")

def process_gold():
    os.makedirs(gold, exist_ok=True)

    df = pd.read_parquet(os.path.join(silver, "silver_data.parquet"))

    avg_salary = df.groupby("department")["gross_salary"].mean().reset_index()
    avg_salary.rename(columns={"gross_salary": "avg_salary"}, inplace=True)

    employee_count = df.groupby("location")["employee_id"].nunique().reset_index()
    employee_count.rename(columns={"employee_id": "employee_count"}, inplace=True)

    avg_tenure = df.groupby("department")["tenure_in_months"].mean().reset_index()
    avg_tenure_sorted = avg_tenure.sort_values("tenure_in_months", ascending=False)
    top_department = avg_tenure_sorted.iloc[0]

    summary = pd.DataFrame({
        "Top Department by Tenure": [top_department["department"]],
        "Avg Tenure (months)": [round(top_department["tenure_in_months"], 1)]
    })

    avg_salary.to_csv(os.path.join(gold, "summary_report.csv"), index=False)
    employee_count.to_csv(os.path.join(gold, "summary_report.csv"), mode='a', index=False)
    summary.to_csv(os.path.join(gold, "summary_report.csv"), mode='a', index=False)

    print("Gold layer:", os.path.join(gold, "summary_report.csv"))

if __name__ == "__main__":
    process_gold()
