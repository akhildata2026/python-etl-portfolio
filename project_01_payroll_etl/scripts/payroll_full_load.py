import pandas as pd
from sqlalchemy import create_engine, text
# create_engine->create DB engine,text->execute sql safely
from datetime import datetime
from urllib.parse import quote_plus
# to encode special characters in password

JOB_NAME = "payroll_etl_v1"

start_time = datetime.now()
# track etl start

# database connection
password = quote_plus("Akhil@2024")
engine = create_engine(
    f"mssql+pyodbc://sa:{password}@localhost/payroll_etl_db?driver=ODBC+Driver+17+for+SQL+Server"
)

try:
    # extract
    df = pd.read_csv(
        "data/payroll_data.csv"
    )
    # validation
    df["salary"] = pd.to_numeric(
        df["salary"],
        errors="coerce"
    )
    df["last_updated_date"] = pd.to_datetime(
        df["last_updated_date"], errors="coerce")

    df = df.dropna(
        subset=["salary", "emp_id", "last_updated_date"]
    )
    df = df[df["salary"] > 0]

    df = df.drop_duplicates(subset=["emp_id"])

    # row_count
    row_processed = len(df)
    # print(df.head())
    # print(df["last_updated_date"])
    # print(df[df["last_updated_date"].isna()])
    # exit()
    # loading
    df.to_sql(
        "staging_payroll",
        con=engine,
        if_exists="append",
        index=False
    )

    # merge staging to final
    merge_sql = """
    MERGE payroll_final as target
    using staging_payroll as source
    ON target.emp_id=source.emp_id

    WHEN MATCHED THEN
    UPDATE SET
    target.employee_name=source.employee_name,
    target.department=source.department,
    target.salary=source.salary,
    target.last_updated_date=source.last_updated_date

    WHEN NOT MATCHED THEN
    INSERT(emp_id,employee_name,department,salary,last_updated_date)
    VALUES(source.emp_id,source.employee_name,source.department,source.salary,source.last_updated_date);
    """
    with engine.begin() as conn:
        conn.execute(text(merge_sql))

    status = "success"
    error_message = None

except Exception as e:
    status = "failed"
    row_processed = 0
    error_message = str(e)

# logging
end_time = datetime.now()
log_sql = """
INSERT INTO etl_run_log
(job_name,start_time,end_time,status,row_processed,error_message)
VALUES(:job,:start,:end,:status,:rows,:error)
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
