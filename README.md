# Payroll ETL Pipeline

## Overview
This project demonstrates an ETL pipeline built using Python and SQL Server.  
This pipeline processes payroll data from CSV files, validate records, loads data into a staging table, merge in to final table, and track execution logs.

### Features
- Full load ETL
- Incremental load ETL
- Data Validation
- Duplicate handling
- Logging and audit tracking

### Tech Stack
- Python
- Pandas
- SQLAlchemy
- SQL Server

### Project Versions
- full load
Loads all valid records into sql server
Script:
scripts/payroll_full_load.py
- Incremental Load
Loads only new/updated records based on last_updated_date
Script:
scripts/payroll_incremental_load.py

### Folder Structure
data/ - source files
scripts/ - ETL scripts
sql/ - table creation scripts

### Workflow
Extract-> Validate -> Stage -> Merge -> Log






