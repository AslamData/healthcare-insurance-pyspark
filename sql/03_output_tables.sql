-- Create these result tables before running the Spark job. The job truncates and appends
-- on each run so the declared keys remain present. Redshift does not enforce the keys.
CREATE TABLE IF NOT EXISTS "Project-Output".disease_claim_count (
  disease_name VARCHAR(200) NOT NULL, claim_count BIGINT NOT NULL,
  PRIMARY KEY (disease_name)
);
CREATE TABLE IF NOT EXISTS "Project-Output".subscribers_under_30_with_subgroup (
  sub_id VARCHAR(40) NOT NULL, age INTEGER, subgrp_id VARCHAR(30), PRIMARY KEY (sub_id)
);
CREATE TABLE IF NOT EXISTS "Project-Output".group_with_max_subgroups (
  group_id VARCHAR(30) NOT NULL, subgroup_count BIGINT, max_count BIGINT, group_name VARCHAR(200), PRIMARY KEY (group_id)
);
CREATE TABLE IF NOT EXISTS "Project-Output".hospital_patient_count (
  hospital_id VARCHAR(30) NOT NULL, patient_count BIGINT, hospital_name VARCHAR(200), city VARCHAR(120), PRIMARY KEY (hospital_id)
);
CREATE TABLE IF NOT EXISTS "Project-Output".subgroup_subscription_count (
  subgroup_id VARCHAR(30) NOT NULL, subscriber_count BIGINT, subgroup_name VARCHAR(200), monthly_premium DECIMAL(18,2), PRIMARY KEY (subgroup_id)
);
CREATE TABLE IF NOT EXISTS "Project-Output".rejected_claim_count (
  rejected_claim_count BIGINT NOT NULL, metric_id SMALLINT NOT NULL DEFAULT 1, PRIMARY KEY (metric_id)
);
CREATE TABLE IF NOT EXISTS "Project-Output".claim_count_by_city (
  patient_city VARCHAR(120) NOT NULL, claim_count BIGINT, PRIMARY KEY (patient_city)
);
CREATE TABLE IF NOT EXISTS "Project-Output".subscriber_count_by_group_type (
  group_type VARCHAR(50) NOT NULL, subscriber_count BIGINT, PRIMARY KEY (group_type)
);
CREATE TABLE IF NOT EXISTS "Project-Output".average_monthly_premium (
  average_monthly_premium DECIMAL(22,6), subscriber_count BIGINT, metric_id SMALLINT NOT NULL DEFAULT 1, PRIMARY KEY (metric_id)
);
CREATE TABLE IF NOT EXISTS "Project-Output".group_profitability_proxy (
  group_id VARCHAR(30) NOT NULL, group_name VARCHAR(200), group_type VARCHAR(50), reporting_year SMALLINT,
  premium_written DECIMAL(18,2), approved_claim_amount DECIMAL(18,2), profitability_proxy DECIMAL(22,2),
  PRIMARY KEY (group_id, reporting_year)
);
CREATE TABLE IF NOT EXISTS "Project-Output".under_18_cancer_patients (
  patient_id BIGINT NOT NULL, age INTEGER, disease_name VARCHAR(200), hospital_id VARCHAR(30), PRIMARY KEY (patient_id)
);
CREATE TABLE IF NOT EXISTS "Project-Output".cashless_claims_over_50000 (
  claim_id BIGINT, patient_id BIGINT, claim_amount DECIMAL(18,2), PRIMARY KEY (claim_id)
);
CREATE TABLE IF NOT EXISTS "Project-Output".female_over_40_knee_surgery_past_year (
  patient_id BIGINT, age INTEGER, service_date DATE, PRIMARY KEY (patient_id, service_date)
);
