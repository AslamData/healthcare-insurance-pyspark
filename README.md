# Healthcare Insurance Analytics Pipeline

PySpark pipeline for cleaning healthcare insurance data and generating analytics tables in Amazon Redshift.

## Source files

Set `S3_INPUT` to the S3 `input-data` folder. The pipeline expects:

- `Patient_records.csv`
- `subscriber.csv`
- `claims.json`
- `group.csv`
- `grpsubgrp.csv`
- `disease.csv`
- `hospital.csv`
- `subgroup.csv`

## Configuration

Configure these environment variables in the run environment:

- `S3_INPUT` — S3 input folder, such as `s3://aslam-healthcare-capstone/input-data`
- `PROJECT_AS_OF_DATE` — reporting date in `YYYY-MM-DD` format for age calculations
- `REDSHIFT_JDBC_URL` — Redshift JDBC connection URL
- `REDSHIFT_USER` and `REDSHIFT_PASSWORD` — credentials supplied securely by the run environment
- `REDSHIFT_DRIVER` — JDBC driver class, if needed
- `REDSHIFT_SOURCE_SCHEMA` — clean table schema; defaults to `healthcare_clean`
- `REDSHIFT_OUTPUT_SCHEMA` — result schema; defaults to `project-output`

Do not commit credentials or other secrets to GitHub. The Spark environment must have access to S3 and the Redshift JDBC driver.

## Run

From the repository root, with PySpark and the configuration above available:

```bash
export PYTHONPATH="$PWD/src:$PYTHONPATH"
spark-submit src/healthcare_pipeline/main.py
