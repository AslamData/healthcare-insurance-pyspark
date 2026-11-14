from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StructField,
    StructType,
)


def subscribers_under_30(
    subscribers: DataFrame,
    as_of_date: str,
) -> DataFrame:
    """Return subscribers under 30 with a subgroup, as of YYYY-MM-DD."""
    age = F.floor(F.months_between(F.to_date(F.lit(as_of_date)), F.col("birth_date")) / 12)

    return (
        subscribers
        .filter(F.col("subgrp_id").isNotNull())
        .withColumn("age", age)
        .filter(F.col("age") < 30)
        .select("sub_id", "age", "subgrp_id")
    )


def groups_with_max_subgroups(
    groups: DataFrame,
    group_subgroups: DataFrame,
) -> DataFrame:
    """Return every group tied for the largest number of distinct subgroups."""
    counts = (
        group_subgroups
        .groupBy("group_id")
        .agg(F.countDistinct("subgroup_id").alias("subgroup_count"))
    )
    max_count = counts.agg(F.max("subgroup_count").alias("max_count"))

    return (
        counts
        .crossJoin(max_count)
        .filter(F.col("subgroup_count") == F.col("max_count"))
        .join(groups.select("group_id", "group_name"), "group_id", "left")
        .select("group_id", "subgroup_count", "max_count", "group_name")
    )


def subscriber_count_by_group_type(
    subscribers: DataFrame,
    group_subgroups: DataFrame,
    groups: DataFrame,
) -> DataFrame:
    """Count distinct subscribers for each policy group type."""
    memberships = (
        subscribers
        .join(group_subgroups, subscribers.subgrp_id == group_subgroups.subgroup_id, "inner")
        .join(groups, "group_id", "inner")
    )

    return memberships.groupBy("group_type").agg(
        F.countDistinct("sub_id").alias("subscriber_count")
    )


def group_profitability_proxy(
    claims: DataFrame,
    subscribers: DataFrame,
    group_subgroups: DataFrame,
    groups: DataFrame,
) -> DataFrame:
    """Calculate premium written minus approved claim amounts; this is a proxy."""
    approved = (
        claims
        .filter(F.upper(F.col("claim_status")) == "Y")
        .join(subscribers.select("sub_id", "subgrp_id"), "sub_id", "inner")
        .join(group_subgroups, subscribers.subgrp_id == group_subgroups.subgroup_id, "inner")
        .groupBy("group_id")
        .agg(F.sum("claim_amount").alias("approved_claim_amount"))
    )

    return (
        groups
        .join(approved, "group_id", "left")
        .fillna({"approved_claim_amount": 0})
        .withColumn(
            "profitability_proxy",
            F.col("premium_written") - F.col("approved_claim_amount"),
        )
        .select(
            "group_id",
            "group_name",
            "group_type",
            "reporting_year",
            "premium_written",
            "approved_claim_amount",
            "profitability_proxy",
        )
    )
    def under_18_cancer_patients(patients: DataFrame, as_of_date: str) -> DataFrame:
    """Find patients under 18 with cancer in their recorded disease name."""
    age = F.floor(
        F.months_between(
            F.to_date(F.lit(as_of_date)),
            F.col("patient_birth_date"),
        ) / 12
    )

    return (
        patients
        .withColumn("age", age)
        .filter(
            (F.col("age") < 18)
            & F.lower(F.col("disease_name")).contains("cancer")
        )
        .select("patient_id", "age", "disease_name", "hospital_id")
        .dropDuplicates()
    )


def empty_cashless_claims(spark) -> DataFrame:
    """Return the typed empty output; source data has no cashless indicator."""
    schema = StructType([
        StructField("claim_id", LongType(), True),
        StructField("patient_id", LongType(), True),
        StructField("claim_amount", DecimalType(18, 2), True),
    ])
    return spark.createDataFrame([], schema)


def empty_female_knee_surgery(spark) -> DataFrame:
    """Return the typed empty output; procedure and service-date fields are absent."""
    schema = StructType([
        StructField("patient_id", LongType(), True),
        StructField("age", IntegerType(), True),
        StructField("service_date", DateType(), True),
    ])
    return spark.createDataFrame([], schema)
