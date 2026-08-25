# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional COPY staging and publication for hospital prices."""

from __future__ import annotations

import uuid
from typing import Any, Sequence

from db.models import db
from process.hospital_hpt_locator import normalized_hospital_location_name
from process.hospital_price_acquisition import (
    REGISTRY_VERSION,
    Attempt,
    Candidate,
    schema_name,
)
from process.hospital_price_native import (
    HOSPITAL_MRF_COPY_COLUMNS,
    HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
    HospitalParserReceipt,
    hospital_ein_from_mrf_url,
)
from process.hospital_price_store_copy import (
    _CHILDREN,
    _TARGET,
    copy_stages,
    validate_stages,
)
from process.ptg_parts.db_tables import _quote_ident


async def admit_attempts(candidates: Sequence[Candidate], *, lease_owner: str, lease_seconds: int) -> Sequence[Any]:
    """Fence and create running attempts for eligible hospital candidates."""

    lease_owner = lease_owner.strip()
    if not lease_owner or len(lease_owner) > 128 or lease_seconds < 2:
        raise ValueError("hospital attempt lease is invalid")
    stage_name = f"hospital_attempt_candidates_{uuid.uuid4().hex[:12]}"
    stage = _quote_ident(stage_name)
    schema = _quote_ident(schema_name())
    async with db.acquire() as connection:
        await connection.status(
            f"CREATE TEMP TABLE {stage} (hospital_id varchar(64), "
            "attempt_id varchar(64), locator_id varchar(64), observation_id "
            "varchar(64), source_url text) ON COMMIT DROP"
        )
        driver = getattr(connection.raw_connection, "driver_connection", connection.raw_connection)
        await driver.copy_records_to_table(
            stage_name,
            columns=["hospital_id", "attempt_id", "locator_id", "observation_id", "source_url"],
            records=[
                (candidate.hospital_id, uuid.uuid4().hex, candidate.locator_id,
                 candidate.observation_id, candidate.source_url)
                for candidate in candidates
            ],
        )
        return await connection.all(
            f"""WITH locked AS MATERIALIZED (
            SELECT current.hospital_id, current.generation, current.latest_attempt_id
            FROM {schema}.hospital_price_current current JOIN {stage} staged USING (hospital_id)
            ORDER BY current.hospital_id FOR UPDATE OF current), expired AS (
            UPDATE {schema}.hospital_price_import_attempt attempt
            SET status='failed', finished_at=clock_timestamp(),
            error_code='lease_expired',
            error_detail='worker lease expired before completion'
            FROM locked WHERE attempt.attempt_id=locked.latest_attempt_id
            AND attempt.status IN ('queued', 'running', 'verified')
            AND attempt.lease_expires_at <= clock_timestamp()
            RETURNING attempt.attempt_id), eligible AS (
            SELECT staged.*, locked.generation FROM {stage} staged JOIN locked USING (hospital_id)
            LEFT JOIN {schema}.hospital_price_import_attempt latest
            ON latest.attempt_id=locked.latest_attempt_id WHERE latest.status IS NULL
            OR latest.status NOT IN ('queued', 'running', 'verified')
            OR EXISTS (SELECT 1 FROM expired
                       WHERE expired.attempt_id=latest.attempt_id)), inserted AS (
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
            RETURNING current.hospital_id, inserted.attempt_id, inserted.expected_generation""",
            registry_version=REGISTRY_VERSION,
            lease_owner=lease_owner,
            lease_seconds=lease_seconds,
        )


async def fail_attempts(
    attempts: Sequence[Attempt], error_code: str, error_detail: str | None
) -> int:
    """Fail running attempts with their final bounded source evidence."""

    if not attempts:
        return 0
    schema = _quote_ident(schema_name())
    stage_name = f"hospital_failed_attempts_{uuid.uuid4().hex[:12]}"
    stage = _quote_ident(stage_name)
    async with db.acquire() as connection:
        await connection.status(
            f"CREATE TEMP TABLE {stage} (attempt_id varchar(64), final_source_url text, "
            "source_http_status integer) ON COMMIT DROP"
        )
        driver = getattr(
            connection.raw_connection, "driver_connection", connection.raw_connection
        )
        await driver.copy_records_to_table(
            stage_name, columns=["attempt_id", "final_source_url", "source_http_status"],
            records=[
                (attempt.attempt_id, attempt.final_source_url, attempt.source_http_status)
                for attempt in attempts
            ],
        )
        return int(await connection.status(
            f"UPDATE {schema}.hospital_price_import_attempt attempt SET status='failed', "
            "finished_at=clock_timestamp(), final_source_url=staged.final_source_url, "
            "source_http_status=staged.source_http_status, error_code=:code, "
            f"error_detail=:detail FROM {stage} staged "
            "WHERE attempt.attempt_id=staged.attempt_id "
            "AND attempt.status IN ('running', 'verified')",
            code=error_code[:64], detail=(error_detail or error_code)[:2000],
        ) or 0)


async def renew_attempt_leases(
    attempts: Sequence[Attempt], *, lease_owner: str, lease_seconds: int
) -> int:
    """Renew this worker's unexpired active attempts without resurrection."""

    attempt_ids = tuple(dict.fromkeys(attempt.attempt_id for attempt in attempts))
    if not attempt_ids:
        return 0
    schema = _quote_ident(schema_name())
    renewed, expired, foreign = await db.first(
        f"""WITH lease_clock AS MATERIALIZED (SELECT clock_timestamp() AS now),
        renewed AS (UPDATE {schema}.hospital_price_import_attempt attempt
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
        WHERE attempt.attempt_id = ANY(CAST(:attempt_ids AS varchar[]))""",
        attempt_ids=attempt_ids,
        lease_owner=lease_owner,
        lease_seconds=lease_seconds,
    )
    if int(expired) or int(foreign):
        raise RuntimeError("hospital price attempt lease was lost")
    return int(renewed)


async def _copy_stages(
    connection: Any, receipt: HospitalParserReceipt, stages: dict[str, str]
) -> None:
    await copy_stages(connection, receipt, stages, schema_name())


async def _validate_stages(
    connection: Any, receipt: HospitalParserReceipt, stages: dict[str, str]
) -> None:
    await validate_stages(connection, receipt, stages)


async def _insert_content(
    connection: Any, content_sha256: str, content_bytes: int, media_type: str | None
) -> None:
    schema = _quote_ident(schema_name())
    await connection.status(
        f"INSERT INTO {schema}.hospital_price_content(content_sha256, byte_count, media_type) "
        "VALUES (:sha256, :bytes, :media_type) ON CONFLICT DO NOTHING",
        sha256=content_sha256, bytes=content_bytes, media_type=media_type,
    )
    stored = await connection.scalar(
        f"SELECT byte_count FROM {schema}.hospital_price_content WHERE content_sha256=:sha256",
        sha256=content_sha256,
    )
    if int(stored or 0) != content_bytes:
        raise RuntimeError("hospital content identity has a conflicting byte count")


async def _insert_version(
    connection: Any,
    receipt: HospitalParserReceipt,
    stages: dict[str, str],
    content_sha256: str,
) -> None:
    schema = _quote_ident(schema_name())
    count_by_kind = {artifact.kind: artifact.rows for artifact in receipt.artifacts}
    await connection.status(
        f"""INSERT INTO {schema}.hospital_price_version(
        version_id, content_sha256, parser_contract_sha256, semantic_sha256,
        source_format, source_hospital_name, last_updated_on, template_version,
        attestation_text, confirm_attestation, attester_name, location_count,
        npi_count, license_count, service_count, charge_count, payer_charge_count,
        financial_aid_policy)
        SELECT version_id, :content, :parser, :semantic, :format,
        source_hospital_name, last_updated_on, template_version, attestation_text,
        confirm_attestation, attester_name, :locations, :npis, :licenses,
        :services, :charges, :payer_charges, financial_aid_policy
        FROM {_quote_ident(stages['mrf'])} ON CONFLICT DO NOTHING""",
        content=content_sha256, parser=HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
        semantic=receipt.semantic_sha256, format=receipt.source_format,
        locations=count_by_kind["location"], npis=count_by_kind["npi"],
        licenses=count_by_kind["license"], services=count_by_kind["service"],
        charges=count_by_kind["charge"],
        payer_charges=count_by_kind["payer_charge"],
    )
    stored = await connection.first(
        f"SELECT content_sha256, parser_contract_sha256, semantic_sha256, source_format, "
        f"location_count, npi_count, license_count, service_count, charge_count, "
        f"payer_charge_count FROM {schema}.hospital_price_version WHERE version_id=:version",
        version=receipt.version_id,
    )
    expected = (
        content_sha256, HOSPITAL_MRF_PARSER_CONTRACT_SHA256, receipt.semantic_sha256,
        receipt.source_format, count_by_kind["location"], count_by_kind["npi"],
        count_by_kind["license"], count_by_kind["service"],
        count_by_kind["charge"], count_by_kind["payer_charge"],
    )
    if stored is None or tuple(stored) != expected:
        raise RuntimeError("hospital version conflicts with stored projection")


async def _insert_children(connection: Any, stages: dict[str, str]) -> None:
    schema = _quote_ident(schema_name())
    for kind in _CHILDREN:
        columns = ", ".join(map(_quote_ident, HOSPITAL_MRF_COPY_COLUMNS[kind]))
        await connection.status(
            f"INSERT INTO {schema}.{_quote_ident(_TARGET[kind])} ({columns}) "
            f"SELECT {columns} FROM {_quote_ident(stages[kind])} ON CONFLICT DO NOTHING"
        )


async def _validate_stored_counts(
    connection: Any, receipt: HospitalParserReceipt
) -> None:
    schema = _quote_ident(schema_name())
    expected_count_by_kind = {
        artifact.kind: artifact.rows for artifact in receipt.artifacts
    }
    count_rows = await connection.all(
        " UNION ALL ".join(
            f"SELECT '{kind}', COUNT(*) FROM "
            f"{schema}.{_quote_ident(_TARGET[kind])} WHERE version_id=:version"
            for kind in expected_count_by_kind
        ),
        version=receipt.version_id,
    )
    stored_count_by_kind = {
        str(kind): int(stored_count) for kind, stored_count in count_rows
    }
    for kind, expected_count in expected_count_by_kind.items():
        if stored_count_by_kind.get(kind) != expected_count:
            raise RuntimeError(f"stored hospital {kind} count is invalid")


def _location_ordinals(
    attempts: Sequence[Attempt], rows: Sequence[Any]
) -> dict[str, int | None]:
    by_name: dict[str, list[int]] = {}
    for ordinal, name in rows:
        if name is not None:
            by_name.setdefault(normalized_hospital_location_name(str(name)), []).append(int(ordinal))
    return {
        attempt.hospital_id: (values[0] if len(values) == 1 else None)
        for attempt in attempts
        for values in [by_name.get(normalized_hospital_location_name(
            attempt.locator_name or attempt.hospital_name
        ), [])]
    }


async def _publication_stage(
    connection: Any, attempts: Sequence[Attempt], location_rows: Sequence[Any]
) -> tuple[str, str]:
    stage_name = f"hospital_publications_{uuid.uuid4().hex[:12]}"
    stage = _quote_ident(stage_name)
    locations = _location_ordinals(attempts, location_rows)
    await connection.status(
        f"CREATE TEMP TABLE {stage} (hospital_id varchar(64), attempt_id varchar(64), "
        "expected_generation bigint, source_location_ordinal integer, "
        "final_source_url text, source_http_status integer, ein varchar(64)) ON COMMIT DROP"
    )
    driver = getattr(connection.raw_connection, "driver_connection", connection.raw_connection)
    await driver.copy_records_to_table(
        stage_name,
        columns=["hospital_id", "attempt_id", "expected_generation",
                 "source_location_ordinal", "final_source_url", "source_http_status", "ein"],
        records=[
            (attempt.hospital_id, attempt.attempt_id, attempt.expected_generation,
             locations[attempt.hospital_id], attempt.final_source_url,
             attempt.source_http_status, hospital_ein_from_mrf_url(attempt.source_url))
            for attempt in attempts
        ],
    )
    return stage_name, stage


async def _bind_evidence(
    connection: Any, stage: str, version_id: str, content_sha256: str, count: int
) -> None:
    schema = _quote_ident(schema_name())
    await connection.status(
        f"INSERT INTO {schema}.hospital_price_version_hospital "
        "(version_id, hospital_id, source_location_ordinal) "
        f"SELECT :version, hospital_id, source_location_ordinal FROM {stage} "
        "ON CONFLICT DO NOTHING", version=version_id,
    )
    if await connection.scalar(
        f"SELECT EXISTS (SELECT 1 FROM {stage} staged LEFT JOIN "
        f"{schema}.hospital_price_version_hospital bound ON "
        "bound.version_id=:version AND bound.hospital_id=staged.hospital_id "
        "WHERE bound.hospital_id IS NULL OR bound.source_location_ordinal "
        "IS DISTINCT FROM staged.source_location_ordinal)",
        version=version_id,
    ):
        raise RuntimeError("hospital version binding conflicts with stored evidence")
    verified = await connection.status(
        f"UPDATE {schema}.hospital_price_import_attempt attempt SET status='verified', "
        "content_sha256=:content, version_id=:version, "
        "final_source_url=staged.final_source_url, "
        "source_http_status=staged.source_http_status, error_code=NULL, error_detail=NULL "
        f"FROM {stage} staged WHERE attempt.attempt_id=staged.attempt_id "
        "AND attempt.status='running'", content=content_sha256, version=version_id,
    )
    if int(verified or 0) != count:
        raise RuntimeError("hospital attempt changed before publication")
    await connection.status(
        f"INSERT INTO {schema}.hospital_price_hospital_npi "
        "(hospital_id, version_id, source_ordinal, npi, source_kind) "
        f"SELECT staged.hospital_id, :version, npi.npi_ordinal, npi.npi, "
        f"'mrf_header_file' FROM {stage} staged "
        f"JOIN {schema}.hospital_price_version_npi npi ON npi.version_id=:version "
        "ON CONFLICT DO NOTHING", version=version_id,
    )
    if await connection.scalar(
        f"SELECT EXISTS (SELECT staged.hospital_id FROM {stage} staged "
        f"JOIN {schema}.hospital_price_version version ON version.version_id=:version "
        f"LEFT JOIN {schema}.hospital_price_hospital_npi evidence ON "
        "evidence.hospital_id=staged.hospital_id AND evidence.version_id=:version "
        "GROUP BY staged.hospital_id, version.npi_count "
        "HAVING COUNT(evidence.source_ordinal) <> version.npi_count)",
        version=version_id,
    ):
        raise RuntimeError("hospital NPI provenance count is invalid")
    await connection.status(
        f"INSERT INTO {schema}.hospital_price_hospital_tax_identity "
        "(hospital_id, version_id, attempt_id, tin_type, tin_value, source_kind, "
        "source_ordinal) "
        f"SELECT hospital_id, :version, attempt_id, 'ein', ein, 'filename', 0 "
        f"FROM {stage} WHERE ein IS NOT NULL ON CONFLICT DO NOTHING", version=version_id,
    )


async def _cas_publish(
    connection: Any, stage: str, version_id: str
) -> tuple[int, int, int]:
    schema = _quote_ident(schema_name())
    statuses = await connection.all(
        f"""WITH unchanged AS (
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
        WHERE attempt.attempt_id=staged.attempt_id RETURNING attempt.status""",
        version=version_id,
    )
    published = sum(str(status_row[0]) == "published" for status_row in statuses)
    unchanged = sum(str(status_row[0]) == "unchanged" for status_row in statuses)
    await connection.status(
        f"DELETE FROM {schema}.hospital_price_hospital_tax_identity tax "
        f"USING {stage} staged, {schema}.hospital_price_import_attempt attempt "
        "WHERE tax.hospital_id=staged.hospital_id AND tax.version_id=:version "
        "AND tax.attempt_id=staged.attempt_id AND attempt.attempt_id=staged.attempt_id "
        "AND attempt.status='superseded'",
        version=version_id,
    )
    return published, len(statuses) - published - unchanged, unchanged


async def _bind_and_publish(
    connection: Any,
    version_id: str,
    content_sha256: str,
    attempts: Sequence[Attempt],
    location_rows: Sequence[Any],
) -> tuple[int, int, int]:
    if not attempts:
        return 0, 0, 0
    _, stage = await _publication_stage(connection, attempts, location_rows)
    await _bind_evidence(connection, stage, version_id, content_sha256, len(attempts))
    outcome = await _cas_publish(connection, stage, version_id)
    if sum(outcome) != len(attempts):
        raise RuntimeError("hospital publication result count is invalid")
    return outcome


async def has_existing_version(
    version_id: str, content_sha256: str, byte_count: int
) -> bool:
    """Check that an immutable parsed version already matches this source."""

    schema = _quote_ident(schema_name())
    stored_version = await db.first(
        f"SELECT version.content_sha256, version.parser_contract_sha256, content.byte_count "
        f"FROM {schema}.hospital_price_version version JOIN {schema}.hospital_price_content "
        f"content USING (content_sha256) WHERE version.version_id=:version", version=version_id,
    )
    if stored_version is None:
        return False
    if tuple(stored_version) != (
        content_sha256, HOSPITAL_MRF_PARSER_CONTRACT_SHA256, byte_count
    ):
        raise RuntimeError("stored hospital version conflicts with source content")
    return True


async def publish_existing(
    version_id: str, content_sha256: str, attempts: Sequence[Attempt]
) -> tuple[int, int, int]:
    """Bind and CAS-publish a previously stored immutable version."""

    schema = _quote_ident(schema_name())
    async with db.acquire() as connection:
        locations = await connection.all(
            f"SELECT location_ordinal, location_name FROM "
            f"{schema}.hospital_price_version_location WHERE version_id=:version "
            "ORDER BY location_ordinal", version=version_id,
        )
        return await _bind_and_publish(
            connection, version_id, content_sha256, attempts, locations
        )


async def stage_content(receipt: HospitalParserReceipt, raw: Any) -> None:
    """COPY, validate, and store one immutable parsed version."""

    stage_by_kind = {
        kind: f"hospital_{kind}_stage_{uuid.uuid4().hex[:12]}"
        for kind in HOSPITAL_MRF_COPY_COLUMNS
    }
    media_type = str(raw.head.content_type) if raw.head and raw.head.content_type else None
    async with db.acquire() as connection:
        await _copy_stages(connection, receipt, stage_by_kind)
        await _validate_stages(connection, receipt, stage_by_kind)
        await _insert_content(connection, raw.raw_sha256, raw.byte_count, media_type)
        await _insert_version(connection, receipt, stage_by_kind, raw.raw_sha256)
        await _insert_children(connection, stage_by_kind)
        await _validate_stored_counts(connection, receipt)
