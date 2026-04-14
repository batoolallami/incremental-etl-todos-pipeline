# incremental-etl-todos-pipeline
## Overview
Incremental ETL pipelines using python and postgresql with raw/clean layers and metadata tracking.
It extracts todo data from a public API, stores raw records in a raw table, creates a clean deduplicated table, and tracks pipeline run using a metadata table.

## Architecture
API -> raw_todos -> clean_todos -> pipeline_runs

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
- pipeline_run : metadata and run history

## Tech Stack
- python
- pandas
- requests
- postgresql
- sqlalchemy
  
