from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def disease_claim_count(claims: DataFrame) -> DataFrame:
    return claims.groupBy("disease_name").agg(F.count("*").alias("claim_count"))


def claim_count_by_city(claims: DataFrame, patients: DataFrame) -> DataFrame:
    return (
        claims.join(patients.select("patient_id", "city"), "patient_id", "left")
        .groupBy(F.col("city").alias("patient_city"))
        .agg(F.count("*").alias("claim_count"))
    )


def hospital_patient_count(patients: DataFrame, hospitals: DataFrame) -> DataFrame:
    return (
        patients.join(hospitals.select("hospital_id", "hospital_name", "city"), "hospital_id", "left")
        .groupBy("hospital_id", "hospital_name", "city")
        .agg(F.countDistinct("patient_id").alias("patient_count"))
    )


def subgroup_subscription_count(
    subscribers: DataFrame,
    subgroups: DataFrame,
) -> DataFrame:
    return (
        subscribers.join(subgroups, subscribers.subgrp_id == subgroups.subgroup_id, "left")
        .groupBy("subgroup_id", "subgroup_name", "monthly_premium")
        .agg(F.countDistinct("sub_id").alias("subscriber_count"))
    )


def rejected_claim_count(claims: DataFrame) -> DataFrame:
    return claims.filter(F.upper(F.col("claim_status")) == "N").agg(
        F.count("*").alias("rejected_claim_count")
    )


def average_monthly_premium(subscribers: DataFrame, subgroups: DataFrame) -> DataFrame:
    linked = subscribers.join(
        subgroups,
        subscribers.subgrp_id == subgroups.subgroup_id,
        "inner",
    )
    return linked.agg(
        F.avg("monthly_premium").alias("average_monthly_premium"),
        F.countDistinct("sub_id").alias("subscriber_count"),
    )
