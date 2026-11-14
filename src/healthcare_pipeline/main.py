import os
from pyspark.sql import SparkSession

from healthcare_pipeline.cleaning import clean_dataframe
from healthcare_pipeline.redshift_writer import write_redshift_table


def main() -> None:
    spark = SparkSession.builder.appName(
        "HealthcareInsuranceAnalytics"
    ).getOrCreate()

    input_path = os.environ["s3://aslam-healthcare-capstone/input-data"].rstrip("/")
    source_schema = os.environ.get("REDSHIFT_SOURCE_SCHEMA", "healthcare_clean")

    csv_tables = {
        "patient": "Patient_records.csv",
        "subscriber": "subscriber.csv",
        "policy_group": "group.csv",
        "group_subgroup": "grpsubgrp.csv",
        "disease": "disease.csv",
        "hospital": "hospital.csv",
        "subgroup": "subgroup.csv",
    }

    for table_name, filename in csv_tables.items():
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{input_path}/{filename}")
        )
        write_redshift_table(
            clean_dataframe(df),
            source_schema,
            table_name,
        )

    claims_df = (
        spark.read
        .option("multiLine", "true")
        .json(f"{input_path}/claims.json")
    )
    write_redshift_table(
        clean_dataframe(claims_df),
        source_schema,
        "claim",
    )

    spark.stop()


if __name__ == "__main__":
    main()
