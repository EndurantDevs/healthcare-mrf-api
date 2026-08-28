# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Long SQL statements shared by hospital-price storage operations."""

ADMIT_ATTEMPTS_SQL = """WITH locked AS MATERIALIZED (
SELECT current.hospital_id, current.generation, current.latest_attempt_id
FROM {schema}.hospital_price_current current JOIN {stage} staged USING (hospital_id)
ORDER BY current.hospital_id FOR UPDATE OF current), latest_locked AS MATERIALIZED (
SELECT locked.hospital_id, locked.generation, locked.latest_attempt_id,
latest.status, latest.lease_expires_at
FROM locked JOIN {schema}.hospital_price_import_attempt latest
ON latest.attempt_id=locked.latest_attempt_id
ORDER BY locked.hospital_id FOR UPDATE OF latest), expired AS (
UPDATE {schema}.hospital_price_import_attempt attempt
SET status='failed', finished_at=clock_timestamp(),
error_code='lease_expired',
error_detail='worker lease expired before completion'
FROM latest_locked WHERE attempt.attempt_id=latest_locked.latest_attempt_id
AND latest_locked.status IN ('queued', 'running', 'verified')
AND latest_locked.lease_expires_at <= clock_timestamp()
RETURNING attempt.attempt_id), eligible AS (
SELECT staged.*, locked.generation FROM {stage} staged JOIN locked USING (hospital_id)
LEFT JOIN latest_locked latest USING (hospital_id) WHERE latest.status IS NULL
OR latest.status NOT IN ('queued', 'running', 'verified')
OR EXISTS (SELECT 1 FROM expired
           WHERE expired.attempt_id=latest.latest_attempt_id)), inserted AS (
INSERT INTO {schema}.hospital_price_import_attempt(
attempt_id, hospital_id, locator_id, locator_observation_id, registry_version,
requested_source_url, expected_generation, status, lease_owner,
heartbeat_at, lease_expires_at)
SELECT attempt_id, hospital_id, locator_id, observation_id, :registry_version,
source_url, generation, 'running', :lease_owner, clock_timestamp(),
clock_timestamp() + make_interval(secs => :lease_seconds) FROM eligible
RETURNING hospital_id, attempt_id, expected_generation)
UPDATE {schema}.hospital_price_current current
SET latest_attempt_id=inserted.attempt_id, updated_at=transaction_timestamp()
FROM inserted WHERE current.hospital_id=inserted.hospital_id
RETURNING current.hospital_id, inserted.attempt_id, inserted.expected_generation"""

STALE_VERSIONS_SQL = """INSERT INTO {stage}(version_id)
SELECT version.version_id FROM {schema}.hospital_price_version version
WHERE NOT EXISTS (SELECT 1 FROM {schema}.hospital_price_current current
WHERE current.version_id=version.version_id)
AND NOT EXISTS (SELECT 1 FROM {schema}.hospital_price_import_attempt attempt
WHERE attempt.version_id=version.version_id
AND attempt.status IN ('queued', 'running', 'verified'))
FOR UPDATE OF version"""

RENEW_ATTEMPTS_SQL = """WITH lease_clock AS MATERIALIZED (
SELECT clock_timestamp() AS now), renewed AS (
UPDATE {schema}.hospital_price_import_attempt attempt
SET heartbeat_at=lease_clock.now,
lease_expires_at=lease_clock.now + make_interval(secs => :lease_seconds)
FROM lease_clock WHERE attempt.attempt_id = ANY(CAST(:attempt_ids AS varchar[]))
AND attempt.lease_owner=:lease_owner
AND attempt.status IN ('queued', 'running', 'verified')
AND attempt.lease_expires_at > lease_clock.now RETURNING attempt.attempt_id)
SELECT (SELECT COUNT(*) FROM renewed),
COUNT(*) FILTER (WHERE attempt.status IN ('queued', 'running', 'verified')
                 AND attempt.lease_owner=:lease_owner
                 AND attempt.lease_expires_at <= lease_clock.now),
COUNT(*) FILTER (WHERE attempt.status IN ('queued', 'running', 'verified')
                 AND attempt.lease_owner<>:lease_owner)
FROM {schema}.hospital_price_import_attempt attempt CROSS JOIN lease_clock
WHERE attempt.attempt_id = ANY(CAST(:attempt_ids AS varchar[]))"""

PUBLISH_ATTEMPTS_SQL = """WITH unchanged AS (
UPDATE {schema}.hospital_price_current current SET
latest_attempt_id=staged.attempt_id,
tax_identity_count=(SELECT COUNT(*) FROM {schema}.hospital_price_hospital_tax_identity tax
  WHERE tax.hospital_id=current.hospital_id AND tax.version_id=:version),
updated_at=transaction_timestamp()
FROM {stage} staged WHERE current.hospital_id=staged.hospital_id
AND current.generation=staged.expected_generation
AND current.latest_attempt_id=staged.attempt_id
AND current.version_id=:version RETURNING current.hospital_id),
published AS (
UPDATE {schema}.hospital_price_current current SET version_id=:version,
generation=current.generation+1, published_attempt_id=staged.attempt_id,
latest_attempt_id=staged.attempt_id, service_count=version.service_count,
charge_count=version.charge_count, payer_charge_count=version.payer_charge_count,
npi_count=(SELECT COUNT(*) FROM {schema}.hospital_price_hospital_npi npi
  WHERE npi.hospital_id=current.hospital_id AND npi.version_id=:version),
tax_identity_count=(SELECT COUNT(*) FROM {schema}.hospital_price_hospital_tax_identity tax
  WHERE tax.hospital_id=current.hospital_id AND tax.version_id=:version),
last_success_at=clock_timestamp(), updated_at=transaction_timestamp()
FROM {stage} staged, {schema}.hospital_price_version version
WHERE current.hospital_id=staged.hospital_id
AND current.generation=staged.expected_generation
AND current.latest_attempt_id=staged.attempt_id
AND current.version_id IS DISTINCT FROM :version
AND version.version_id=:version
RETURNING current.hospital_id)
UPDATE {schema}.hospital_price_import_attempt attempt SET
status=CASE WHEN unchanged.hospital_id IS NOT NULL THEN 'unchanged'
            WHEN published.hospital_id IS NOT NULL THEN 'published'
            ELSE 'superseded' END,
finished_at=clock_timestamp() FROM {stage} staged
LEFT JOIN unchanged ON unchanged.hospital_id=staged.hospital_id
LEFT JOIN published ON published.hospital_id=staged.hospital_id
WHERE attempt.attempt_id=staged.attempt_id RETURNING attempt.status"""

EXISTING_VERSION_SQL = """SELECT version.content_sha256,
version.parser_contract_sha256, content.byte_count,
root.version_id IS NOT NULL, root.format_version=2
AND root.service_count=version.service_count
AND root.charge_count=version.charge_count
AND root.fact_count=version.payer_charge_count
AND root.service_block_count=(SELECT COUNT(*) FROM
    {schema}.hospital_price_data_block block
    WHERE block.version_id=version.version_id AND block.block_kind=1)
AND root.fact_block_count=(SELECT COUNT(*) FROM
    {schema}.hospital_price_data_block block
    WHERE block.version_id=version.version_id AND block.block_kind=2)
AND root.code_selector_block_count=(SELECT COUNT(*) FROM
    {schema}.hospital_price_data_block block
    WHERE block.version_id=version.version_id AND block.block_kind=3)
AND root.payer_plan_selector_block_count=(SELECT COUNT(*) FROM
    {schema}.hospital_price_data_block block
    WHERE block.version_id=version.version_id AND block.block_kind=4)
FROM {schema}.hospital_price_version version
JOIN {schema}.hospital_price_content content USING (content_sha256)
LEFT JOIN {schema}.hospital_price_packed_root root USING (version_id)
WHERE version.version_id=:version"""
