# user-posts-etl-pipeline (Multi-Source)

## Overview
This project is production style ETL pipeline built with Python and PostgreSQL.
It extracts data from multiple APIs (users and posts), stores raw data, transform it into clean tables, and create an aggregated analytics table.
This pipeline also handles nested JSON fields and tracks execution using metadata table.

## Architecture
Users API -> raw_users
Posts API -> raw_posts
|
transform
|
merge + aggregate
|
clean_user_post_summary
|
pipeline_runs
## code
- raw_users ->
- raw_posts ->
- clean_user_post_summary -> merge + deduplicate+ aggregate
- pipeline_runs -> track execution and incremental state

## Features
- Extract data from multiple APIs
- Incremental raw data loading
- Handle nested JSON fields ('address' + 'company')
- Transform and clean data
- Merge database (users + posts)
- Aggregate metrics (total posts, average title length)
- Metadata tracking (pipeline_runs)
- Logging and validation
  
  ---

   ## Tech Stack
  - Python
  - Pandas
  - Requests
  - PostgreSQL
  - SQLAlchemy

  ## How to Run
  1. Install dependencies
  ''' bash
  pip install -r requirements.txt
  2. Update database config in config.py
  3. Run

 ## Key Concepts Demonstrated
 . Multi-source ETL pipelines
 . Incremental data ingestion
 . Data transformation and cleaning
 . Handling nested JSON data
 . Data aggregation for analytics
 . Metadata tracking and logging

 ## Future Improvements
 . Add Airflow orchestration
 . Add SCD (Slowly Changing Dimension)
 . Add unit testing
 . Add Docker support

 Author
 Batool Hussain Allami


