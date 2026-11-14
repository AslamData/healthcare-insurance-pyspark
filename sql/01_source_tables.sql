-- Redshift source/clean target table design. Constraints are informational in Redshift.
CREATE TABLE IF NOT EXISTS healthcare_clean.policy_group (
  group_id VARCHAR(30) NOT NULL, group_name VARCHAR(200), group_type VARCHAR(50),
  country VARCHAR(100), premium_written DECIMAL(18,2), zip_code VARCHAR(20),
  city VARCHAR(120), reporting_year SMALLINT,
  PRIMARY KEY (group_id)
) DISTSTYLE AUTO;
CREATE TABLE IF NOT EXISTS healthcare_clean.subgroup (
  subgroup_id VARCHAR(30) NOT NULL, subgroup_name VARCHAR(200), monthly_premium DECIMAL(18,2),
  PRIMARY KEY (subgroup_id)
) DISTSTYLE AUTO;
CREATE TABLE IF NOT EXISTS healthcare_clean.hospital (
  hospital_id VARCHAR(30) NOT NULL, hospital_name VARCHAR(200), city VARCHAR(120),
  state VARCHAR(120), country VARCHAR(100), PRIMARY KEY (hospital_id)
) DISTSTYLE AUTO;
CREATE TABLE IF NOT EXISTS healthcare_clean.patient (
  patient_id BIGINT NOT NULL,
  patient_name VARCHAR(200), patient_gender VARCHAR(30), patient_birth_date DATE,
  patient_phone VARCHAR(40), disease_name VARCHAR(200), city VARCHAR(120), hospital_id VARCHAR(30),
  PRIMARY KEY (patient_id),
  FOREIGN KEY (hospital_id) REFERENCES healthcare_clean.hospital(hospital_id)
) DISTSTYLE AUTO;
CREATE TABLE IF NOT EXISTS healthcare_clean.subscriber (
  sub_id VARCHAR(40) NOT NULL, first_name VARCHAR(120), last_name VARCHAR(120), street VARCHAR(250),
  birth_date DATE, gender VARCHAR(30), phone VARCHAR(40), country VARCHAR(100),
  city VARCHAR(120), zip_code VARCHAR(20), subgrp_id VARCHAR(30),
  elig_ind VARCHAR(10), eff_date DATE, term_date DATE,
  PRIMARY KEY (sub_id),
  FOREIGN KEY (subgrp_id) REFERENCES healthcare_clean.subgroup(subgroup_id)
) DISTSTYLE AUTO;
CREATE TABLE IF NOT EXISTS healthcare_clean.claim (
  claim_id BIGINT NOT NULL, patient_id BIGINT, disease_name VARCHAR(200),
  sub_id VARCHAR(40), claim_status VARCHAR(20), claim_type VARCHAR(100),
  claim_amount DECIMAL(18,2), claim_date DATE,
  PRIMARY KEY (claim_id),
  FOREIGN KEY (patient_id) REFERENCES healthcare_clean.patient(patient_id),
  FOREIGN KEY (sub_id) REFERENCES healthcare_clean.subscriber(sub_id)
) DISTSTYLE AUTO;
CREATE TABLE IF NOT EXISTS healthcare_clean.group_subgroup (
  subgroup_id VARCHAR(30) NOT NULL, group_id VARCHAR(30) NOT NULL,
  PRIMARY KEY (group_id, subgroup_id),
  FOREIGN KEY (group_id) REFERENCES healthcare_clean.policy_group(group_id),
  FOREIGN KEY (subgroup_id) REFERENCES healthcare_clean.subgroup(subgroup_id)
) DISTSTYLE AUTO;
CREATE TABLE IF NOT EXISTS healthcare_clean.disease (
  subgroup_id VARCHAR(30), disease_id BIGINT NOT NULL, disease_name VARCHAR(200),
  PRIMARY KEY (disease_id),
  FOREIGN KEY (subgroup_id) REFERENCES healthcare_clean.subgroup(subgroup_id)
) DISTSTYLE AUTO;
