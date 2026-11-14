import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from healthcare_pipeline.cleaning import clean_dataframe
from healthcare_pipeline.redshift_writer import write_redshift_table
from healthcare_pipeline.results import (
    average_monthly_premium,
    claim_count_by_city,
    disease_claim_count,
    empty_cashless_claims,
    empty_female_knee_surgery,
    group_profitability_proxy,
    groups_with_max_subgroups,
    hospital_patient_count,
    rejected_claim_count,
    subgroup_subscription_count,
    subscriber_count_by_group_type,
    subscribers_under_30,
    under_18_cancer_patients,
)


def main() -> None:
    spark = SparkSession.builder.appName(
        "HealthcareInsuranceAnalytics"
    ).getOrCreate()

    input_path = os.environ["S3_INPUT"].rstrip("/")
    as_of_date = os.environ["PROJECT_AS_OF_DATE"]
    source_schema = os.environ.get("REDSHIFT_SOURCE_SCHEMA", "healthcare_clean")
    output_schema = os.environ.get("REDSHIFT_OUTPUT_SCHEMA", "project-output")

    csv_files = {
        "patient": "Patient_records.csv",
        "subscriber": "subscriber.csv",
        "policy_group": "group.csv",
        "group_subgroup": "grpsubgrp.csv",
        "disease": "disease.csv",
        "hospital": "hospital.csv",
        "subgroup": "subgroup.csv",
    }

    clean = {}

    for table_name, filename in csv_files.items():
        raw_df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{input_path}/{filename}")
        )
        clean[table_name] = clean_dataframe(raw_df)

    claims_raw = (
        spark.read
        .option("multiLine", "true")
        .json(f"{input_path}/claims.json")
    )
    clean["claim"] = clean_dataframe(claims_raw)

    for table_name, df in clean.items():
        write_redshift_table(df, source_schema, table_name)

    patients = clean["patient"]
    subscribers = clean["subscriber"]
    claims = clean["claim"]
    groups = clean["policy_group"]
    group_subgroups = clean["group_subgroup"]
    hospitals = clean["hospital"]
    subgroups = clean["subgroup"]

    results = {
        "disease_claim_count": disease_claim_count(claims),
        "subscribers_under_30_with_subgroup": subscribers_under_30(
            subscribers, as_of_date
        ),
        "group_with_max_subgroups": groups_with_max_subgroups(
            groups, group_subgroups
        ),
        "hospital_patient_count": hospital_patient_count(patients, hospitals),
        "subgroup_subscription_count": subgroup_subscription_count(
            subscribers, subgroups
        ),
        "rejected_claim_count": rejected_claim_count(claims).withColumn(
            "metric_id", F.lit(1).cast("smallint")
        ),
        "claim_count_by_city": claim_count_by_city(claims, patients),
        "subscriber_count_by_group_type": subscriber_count_by_group_type(
            subscribers, group_subgroups, groups
        ),
        "average_monthly_premium": average_monthly_premium(
            subscribers, subgroups
        ).withColumn("metric_id", F.lit(1).cast("smallint")),
        "group_profitability_proxy": group_profitability_proxy(
            claims, subscribers, group_subgroups, groups
        ),
        "under_18_cancer_patients": under_18_cancer_patients(
            patients, as_of_date
        ),
        "cashless_claims_over_50000": empty_cashless_claims(spark),
        "female_over_40_knee_surgery_past_year": (
            empty_female_knee_surgery(spark)
        ),
    }

    for table_name, df in results.items():
        write_redshift_table(df, output_schema, table_name)

    spark.stop()


if __name__ == "__main__":
    main()
