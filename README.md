# catalys-de-assignment

## Project Overview
This repository contains the solution for the CATALYS Data Engineering Assignment. The project demonstrates hands-on experience with ETL pipelines, data modeling for both relational and NoSQL stores, and data transformation using SQL. It is designed to showcase practical decision-making, pipeline reliability, and data quality best practices.

### Key Features
- End-to-end ETL pipeline for ingesting, transforming, and loading data from multiple sources
- Incremental/delta load support
- Data quality checks and idempotent loads using file checksum tracking
- Data modeling for both Snowflake (relational) and NoSQL (design/justification)
- SQL-based business transformations
- Automated file load tracking to prevent duplicate loads

## Setup Instructions

### 1. Clone the Repository
```bash
git clone <repo-url>
cd catalys-de-assignment
```

### 2. Install Python Dependencies
It is recommended to use a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Create a `.env` file in the root directory with the following variables:
```
SNOWFLAKE_USER=<your_user>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_ACCOUNT=<your_account>
SNOWFLAKE_WAREHOUSE=<your_warehouse>
SNOWFLAKE_DATABASE=<your_database>
SNOWFLAKE_SCHEMA=<your_schema>
SNOWFLAKE_TRACKING_TABLE=etl.file_load_tracking
```

### 4. Configure Ingestion
Edit `config/ingestion_config.yml` to specify the tables, file paths, and patterns for your data sources.

### 5. Prepare Snowflake
- Create the required raw tables and the file load tracking table using the provided DDLs in the `ddl/` folder.
- Example for file tracking table:
```sql
CREATE OR REPLACE TABLE etl.file_load_tracking (
	file_name VARCHAR,
	file_checksum VARCHAR,
	table_name VARCHAR,
	load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	status VARCHAR
);
```

### 6. Run the ETL Pipeline
To load all tables:
```bash
python etl/load_to_snowflake.py --all
```
To load a specific table:
```bash
python etl/load_to_snowflake.py --table <table_name>
```

## Project Structure
- `etl/` — ETL scripts and pipeline logic
- `ddl/` — DDL scripts for Snowflake tables
- `data/` — Sample input datasets
- `config/` — Ingestion configuration YAML
- `requirements.txt` — Python dependencies
- `README.md` — Project documentation

## Data Quality & Reliability
- File checksum tracking ensures idempotent loads (no duplicate ingestion)
- Validation checks for nulls, duplicates, and schema issues
- Error handling and logging for traceability

## Notes
- The ETL pipeline is designed for extensibility and can be adapted for additional sources or targets.
- NoSQL modeling and justification should be documented in the design folder as required by the assignment.

For any questions, please refer to the assignment requirements or contact the repository maintainer.
