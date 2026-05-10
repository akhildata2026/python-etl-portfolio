create DATABASE payroll_etl_db;
use payroll_etl_db;

CREATE TABLE staging_payroll(
    emp_id INT,
    employee_name VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10,2),
    last_updated_date DATE
);
CREATE TABLE payroll_final(
    emp_id INT PRIMARY KEY,
    employee_name VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10,2),
    last_updated_date DATE
);

CREATE TABLE etl_run_log (
    job_name VARCHAR(100),
    start_time DATETIME,
    end_time DATETIME,
    status VARCHAR(20),
    row_processed INT,
    error_message VARCHAR(500)
);

