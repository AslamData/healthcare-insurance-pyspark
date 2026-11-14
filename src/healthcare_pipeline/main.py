import os
from pyspark.sql import SparkSession

from healthcare_pipeline.cleaning import clean_dataframe
from healthcare_pipeline.redshift_writer import write_redshift_table


def main() -> None:
    spark = SparkSession.builder.appName("HealthcareInsuranceAnalytics").getOrCreate()

    input_path = os.environ["S3_INPUT"]
    source_schema = os.environ.get("REDSHIFT_SOURCE_SCHEMA", "healthcare_clean")

    csv_tables = {
        "patient": "patient.csv",
        "subscriber": "subscriber.csv",
        "claim": "claim.csv",
        "policy_group": "group.csv",
        "subgroup": "subgroup.csv",
        "group_subgroup": "grpsubgrp.csv",
        "disease": "disease.csv",
        "hospital": "hospital.csv",
    }

    for table, filename in csv_tables.items():
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(
            f"{input_path.rstrip('/')}/{filename}"
        )
        clean_df = clean_dataframe(df)
        write_redshift_table(clean_df, source_schema, table)

    spark.stop()


if __name__ == "__main__":
    main()
