# incremental-etl-todos-pipeline (Python + PostgreSQL)
## Overview
Incremental ETL pipelines using Python and PostgreSQL with raw/clean layers and metadata tracking.
The pipeline extracts todo data from a public API, loads raw records into a database, transforms it into a clean layer, and tracks pipeline execution using a metadata table.

## Architecture
API -> raw_todos -> clean_todos -> pipeline_runs

##code
- **raw_todos** -> stores raw ingested data
- **clean_todos** ->deduplicated, cleaned data
- **pipeline_runs** -> tracks execution and incremental state

## Features
- Extracts data from API
- Incremental raw loading using 'last_max_id'
- Deduplicates records in clean layer
- Track pipeline runs in metadata table
- Handles recovery when raw table is missing
- uses structure logging and validation

## Tables
- raw_todos : raw ingested data
- clean_todos : cleaned deduplicated data
- pipeline_runs : metadata and run history

## Tech Stack
- python
- pandas
- requests
- postgresql
- sqlalchemy

## how to run
1. Install dependencies:
   '''bash
   pip install -r requirments.txt
2- Update database config.py
3- Run pipeline

# Key Concepts Demonstrated
. Incremental ETL design
. Data pipeline structuring
. Idempotent data loading
. Data validation and logging
. Basic data warehousing patterns

# Future Improvements
. Add Airflow orchestration
. Add Docker support
. Add unit tests
. Add data quality checks

  
