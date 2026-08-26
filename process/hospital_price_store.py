# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Transactional COPY staging and publication for hospital prices."""

from __future__ import annotations

import uuid
from typing import Any, Sequence

from db.models import db
from process.hospital_hpt_locator import normalized_hospital_location_name
from process.hospital_price_acquisition import (
    Attempt,
    schema_name,
)
from process.hospital_price_attempt_store import (
    admit_attempts,
    fail_attempts,
    rebind_attempt_sources,
    renew_attempt_leases,
)
from process.hospital_price_native import (
    HOSPITAL_MRF_PARSER_CONTRACT_SHA256,
    HOSPITAL_MRF_TEXT_COPY_COLUMNS,
    HospitalParserReceipt,
    hospital_ein_from_mrf_url,
)
from process.hospital_price_store_copy import (
    _CHILDREN,
    _TARGET,
    copy_packed_blocks,
    copy_stages,
    validate_packed_storage,
    validate_stages,
)
from process.hospital_price_store_sql import (
    EXISTING_VERSION_SQL,
    PUBLISH_ATTEMPTS_SQL,
    STALE_VERSIONS_SQL,
)
from process.ptg_parts.db_tables import _quote_ident


async def garbage_collect_superseded_versions() -> int:
    """Remove packed versions no current hospital or active attempt can use."""

    schema = _quote_ident(schema_name())
    stage = _quote_ident(
        f"hospital_stale_versions_{uuid.uuid4().hex[:12]}"
    )
    async with db.acquire() as connection:
        await connection.status(
            f"CREATE TEMP TABLE {stage} (version_id varchar(64) PRIMARY KEY) "
            "ON COMMIT DROP"
        )
        await connection.status(
            STALE_VERSIONS_SQL.format(schema=schema, stage=stage)
        )
        await connection.status(
            f"DELETE FROM {schema}.hospital_price_hospital_tax_identity tax "
            f"USING {stage} stale WHERE tax.version_id=stale.version_id"
        )
        await connection.status(
            f"UPDATE {schema}.hospital_price_import_attempt attempt "
            f"SET version_id=NULL FROM {stage} stale "
            "WHERE attempt.version_id=stale.version_id "
            "AND attempt.status IN "
            "('published', 'unchanged', 'failed', 'superseded')"
        )
        return int(await connection.status(
            f"DELETE FROM {schema}.hospital_price_version version "
            f"USING {stage} stale WHERE version.version_id=stale.version_id"
        ) or 0)


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


async def _has_inserted_version(
    connection: Any,
    receipt: HospitalParserReceipt,
    stages: dict[str, str],
    content_sha256: str,
) -> bool:
    schema = _quote_ident(schema_name())
    count_by_kind = {artifact.kind: artifact.rows for artifact in receipt.artifacts}
    root = receipt.root
    inserted = await connection.status(
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
        licenses=count_by_kind["license"], services=root.service_count,
        charges=root.charge_count, payer_charges=root.fact_count,
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
        count_by_kind["license"], root.service_count,
        root.charge_count, root.fact_count,
    )
    if stored is None or tuple(stored) != expected:
        raise RuntimeError("hospital version conflicts with stored projection")
    return int(inserted or 0) == 1


async def _insert_children(connection: Any, stages: dict[str, str]) -> None:
    schema = _quote_ident(schema_name())
    for kind in _CHILDREN:
        columns = ", ".join(
            map(_quote_ident, HOSPITAL_MRF_TEXT_COPY_COLUMNS[kind])
        )
        await connection.status(
            f"INSERT INTO {schema}.{_quote_ident(_TARGET[kind])} ({columns}) "
            f"SELECT {columns} FROM {_quote_ident(stages[kind])} ON CONFLICT DO NOTHING"
        )


async def _insert_packed_root(
    connection: Any, receipt: HospitalParserReceipt
) -> None:
    schema = _quote_ident(schema_name())
    root = receipt.root
    await connection.status(
        f"""INSERT INTO {schema}.hospital_price_packed_root(
        version_id, format_version, service_count, charge_count, fact_count,
        code_selector_key_count, payer_plan_selector_key_count,
        code_selector_ref_count, payer_plan_selector_ref_count,
        service_block_count, fact_block_count, code_selector_page_count,
        payer_plan_selector_page_count)
        VALUES (:version, 1, :services, :charges, :facts, :code_keys,
        :payer_keys, :code_refs, :payer_refs, :service_blocks, :fact_blocks,
        :code_pages, :payer_pages)""",
        version=receipt.version_id,
        services=root.service_count,
        charges=root.charge_count,
        facts=root.fact_count,
        code_keys=root.code_selector_key_count,
        payer_keys=root.payer_plan_selector_key_count,
        code_refs=root.code_selector_ref_count,
        payer_refs=root.payer_plan_selector_ref_count,
        service_blocks=root.service_block_count,
        fact_blocks=root.fact_block_count,
        code_pages=root.code_selector_page_count,
        payer_pages=root.payer_plan_selector_page_count,
    )


async def _validate_stored_counts(
    connection: Any, receipt: HospitalParserReceipt
) -> None:
    schema = _quote_ident(schema_name())
    expected_count_by_kind = {
        artifact.kind: artifact.rows
        for artifact in receipt.artifacts
        if artifact.kind in HOSPITAL_MRF_TEXT_COPY_COLUMNS
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
    await validate_packed_storage(connection, receipt, schema_name())


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
        PUBLISH_ATTEMPTS_SQL.format(schema=schema, stage=stage),
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
    stored_version_fields = await db.first(
        EXISTING_VERSION_SQL.format(schema=schema),
        version=version_id,
    )
    if stored_version_fields is None:
        return False
    stored_version_fields = tuple(stored_version_fields)
    if stored_version_fields[:3] != (
        content_sha256, HOSPITAL_MRF_PARSER_CONTRACT_SHA256, byte_count
    ):
        raise RuntimeError("stored hospital version conflicts with source content")
    if tuple(stored_version_fields[3:]) != (True, True):
        raise RuntimeError("stored hospital packed version is incomplete")
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
        for kind in HOSPITAL_MRF_TEXT_COPY_COLUMNS
    }
    media_type = str(raw.head.content_type) if raw.head and raw.head.content_type else None
    async with db.acquire() as connection:
        await _copy_stages(connection, receipt, stage_by_kind)
        await _validate_stages(connection, receipt, stage_by_kind)
        await _insert_content(connection, raw.raw_sha256, raw.byte_count, media_type)
        inserted = await _has_inserted_version(
            connection, receipt, stage_by_kind, raw.raw_sha256
        )
        if inserted:
            await _insert_children(connection, stage_by_kind)
            await _insert_packed_root(connection, receipt)
            await copy_packed_blocks(
                connection, receipt, schema_name()
            )
        await _validate_stored_counts(connection, receipt)
