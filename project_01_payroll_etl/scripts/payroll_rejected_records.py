import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from urllib.parse import quote_plus
import time

# =========================
# 1. Job Setup
# =========================
JOB_NAME = "payroll_rejected_records"
start_time = datetime.now()

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

status = None
row_processed = 0
error_message = None

# Retry loop
for attempt in range(1, MAX_RETRIES + 1):
    try:
        print(f"Attempt {attempt} started...")

    # =========================
    # 2. Database Connection
    # =========================
        password = quote_plus("Akhil@2024")
        engine = create_engine(
            f"mssql+pyodbc://sa:{password}@localhost/payroll_etl_db?driver=ODBC+Driver+17+for+SQL+Server"
        )

    # =========================
    # 3. Get Watermark (IMPORTANT)
    # =========================
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT MAX(last_updated_date)
                FROM payroll_final
            """))
            last_run = result.scalar()

    # =========================
    # 4. Extract
    # =========================
        df = pd.read_csv("data/payroll_data.csv")

     # =========================
    # 4. Validation / Cleaning
    # =========================
        df['salary'] = pd.to_numeric(df["salary"], errors="coerce")

        df["last_updated_date"] = pd.to_datetime(df["last_updated_date"],
                                                 format="%d-%m-%y",
                                                 errors="coerce")

        # rejected records logic
        rejected_df = pd.DataFrame()

        # missing salary
        missing_salary = df[df["salary"].isna()].copy()

        missing_salary["reject_reason"] = "Missing salary"

        # invalid salary
        invalid_salary = df[df["salary"] < 0].copy()

        invalid_salary["reject_reason"] = "Invalid salary"

        # invalid date
        invalid_date = df[
            df["last_updated_date"].isna()
        ].copy()

        invalid_date["reject_reason"] = "Invalid date"

        # Missing employee id
        missing_emp_id = df[
            df["emp_id"].isna()
        ].copy()

        missing_emp_id["reject_reason"] = "Missing emp_id"

        # combine rejected records
        rejected_df = pd.concat(
            [missing_salary,
                invalid_salary,
                invalid_date,
                missing_emp_id
             ]
        )

        # save rejected records
        if not rejected_df.empty:
            rejected_df.to_csv(
                "rejected_records/rejected_payroll.csv",
                index=False
            )

        # keep only valid records and removing all invalid rows from original df based on index

        df = df.drop(rejected_df.index)
        # --df.drop(1,2,3..)

        df = df.drop_duplicates(subset=["emp_id"], keep="last")

    # =========================
    # 6. Incremental Filter (MAIN LOGIC)
    # =========================
        if last_run is not None:
            last_run = pd.to_datetime(last_run)
            df = df[df["last_updated_date"] > last_run]

    # Stop if no new data
        if df.empty:
            status = "SUCCESS"
            row_processed = 0
            error_message = "No new data"
            print("No new data,Skipping ETL")
            break  # valid scenerio,like no data
            # raise Exception,when its real failure

        row_processed = len(df)

    # =========================
    # 7. Load to Staging
    # =========================
        with engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE staging_payroll"))

        df.to_sql(
            "staging_payroll",
            con=engine,
            if_exists="append",
            index=False
        )

    # =========================
    # 8. Merge Staging → Final
    # =========================
        merge_sql = """
        MERGE payroll_final AS target
        USING staging_payroll AS source
        ON target.emp_id = source.emp_id

        WHEN MATCHED
          AND source.last_updated_date>target.last_updated_date
        THEN
            UPDATE SET
                target.employee_name = source.employee_name,
                target.department = source.department,
                target.salary = source.salary,
                target.last_updated_date = source.last_updated_date

        WHEN NOT MATCHED THEN
            INSERT (emp_id, employee_name, department, salary, last_updated_date)
            VALUES (source.emp_id, source.employee_name, source.department, source.salary, source.last_updated_date);
        """

        with engine.begin() as conn:
            conn.execute(text(merge_sql))

        status = "SUCCESS"
        error_message = None
        print("ETL run successfully")
        break  # stop retry after success

    except Exception as e:
        error_message = str(e)
        print(f"Attempt{attempt} failed:{error_message}")

        if attempt < MAX_RETRIES:
            print("Retrying..")
            time.sleep(RETRY_DELAY)
        else:
            status = "FAILED"
            row_processed = 0

# =========================
# 9. Logging
# =========================
end_time = datetime.now()

log_sql = """
INSERT INTO etl_run_log
(job_name, start_time, end_time, status, row_processed, error_message)
VALUES (:job, :start, :end, :status, :rows, :error)
"""

with engine.begin() as conn:
    conn.execute(
        text(log_sql),
        {
            "job": JOB_NAME,
            "start": start_time,
            "end": end_time,
            "status": status,
            "rows": row_processed,
            "error": error_message
        }
    )

print("ETL pipeline finished")
