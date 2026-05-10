# Payroll ETL Pipeline

## Overview
This project demonstrates an ETL pipeline built using Python and SQL Server.  
This pipeline processes payroll data from CSV files, validate records, loads data into a staging table, merge in to final table, and track execution logs.

### Features
- Full load ETL
- Incremental load ETL
- Retry logic
- Data Validation
- Duplicate handling
- Rejected Records Handling
- Logging and audit tracking

### Tech Stack
- Python
- Pandas
- SQLAlchemy
- SQL Server

### Project Versions
#### v1 - Full load
- Extract csv data
- Validate salary/date columns
- Load in to staging table
- Merge in to final table
  
#### v2 - Incremental Load     
- Fetch watermark from target table  
- Load only new/updated records  

#### v3 - Retry logic
- Retry ETL on failure
- Configure retry attempts
- Delay between retries

#### v4 - Rejected records handling
- Capture invalid records
- Save rejected records separately
- Process only valid records  

### Folder Structure
data/ - source files  
scripts/ - ETL scripts  
sql/ - table creation scripts  
rejected_records/rejected_payroll_csv

### Workflow  
Extract-> Validate -> Stage -> Merge -> Log  








