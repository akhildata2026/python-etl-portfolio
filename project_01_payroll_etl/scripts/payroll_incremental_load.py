import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from urllib.parse import quote_plus

# =========================
# 1. Job Setup
# =========================
JOB_NAME = "payroll_etl_incremental_v1"
start_time = datetime.now()

# default values for logging
status = 'failed'
row_processed = 0
error_message = None

try:
    # =========================
    # 2. Database Connection
    # =========================
    password = quote_plus("Akhil@2024")
    engine = create_engine(
        f"mssql+pyodbc://sa:{password}@localhost/payroll_etl_db?driver=ODBC+Driver+17+for+SQL+Server"
    )

    # =========================
    # 3. Get Watermark 
    # =========================
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT MAX(last_updated_date)
            FROM payroll_final
        """))
        last_run = result.scalar()
    # scalar to extract the first value from the rows of data, without scalar python won't understand the actual value.
    
    # =========================
        # 4. Extract
    # =========================
    df = pd.read_csv("data/payroll_data.csv")

    # validation
    df["salary"] = pd.to_numeric(df["salary"], errors='coerce')

    df["last_updated_date"] = pd.to_datetime(df["last_updated_date"],
                                             format="%d-%m-%y",
                                             errors="coerce"
                                             )
    df = df.dropna(subset=["salary", "emp_id", "last_updated_date"])

    df = df[df["salary"] > 0]  # remove negative salary
    df = df.drop_duplicates(
        subset=["emp_id"],
        keep="last"
    )

    # =========================
    # 6. Incremental Filter (MAIN LOGIC)
    # =========================
    if last_run is not None:
        # if condition is true, do this steps else 'pass'.
        last_run = pd.to_datetime(last_run)
        df = df[df["last_updated_date"] > last_run]
    # this is based on if-else condition and else is implicit here.else 'pass'.

    # Stop if no new data
    if df.empty:
        status = 'success'
        row_processed = 0
        error_message = "No new data to process"
    else:
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

    WHEN MATCHED AND 
    source.last_updated_date>target.last_updated_date
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

except Exception as e:
    status = "FAILED"
    row_processed = 0
    error_message = str(e)

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
            # this are python variable and sql wont understand directly hence mentioning in variable(:).
            "job": JOB_NAME,
            "start": start_time,
            "end": end_time,
            "status": status,
            "rows": row_processed,
            "error": error_message
        }
    )

print("ETL pipeline finished")
