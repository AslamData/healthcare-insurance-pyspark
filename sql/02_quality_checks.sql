-- Referential and output spot checks. Redshift PK/FK declarations are informational.
SELECT COUNT(*) AS claims_without_patient FROM healthcare_clean.claim c
LEFT JOIN healthcare_clean.patient p ON p.patient_id=c.patient_id WHERE p.patient_id IS NULL;
SELECT COUNT(*) AS subscribers_without_subgroup FROM healthcare_clean.subscriber s
LEFT JOIN healthcare_clean.subgroup g ON g.subgroup_id=s.subgrp_id WHERE s.subgrp_id IS NOT NULL AND g.subgroup_id IS NULL;
SELECT * FROM "Project-Output".disease_claim_count ORDER BY claim_count DESC;
SELECT * FROM "Project-Output".rejected_claim_count;
SELECT * FROM "Project-Output".hospital_patient_count ORDER BY patient_count DESC;
SELECT * FROM "Project-Output".group_profitability_proxy ORDER BY profitability_proxy DESC;
