# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic durable admission for one complete NPPES listing-chain receipt."""

from __future__ import annotations

import asyncio

from db.models import db
from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_catalog import assert_nppes_candidate_catalog
from process.nppes_public_evidence_chain import (
    NppesPublicEvidenceChainReceipt,
    validate_nppes_public_evidence_chain_receipt,
)
from process.nppes_public_evidence_chain_rows import (
    CHAIN_ADMISSION_COLUMNS,
    CHAIN_ARCHIVE_COLUMNS,
    NppesChainAdmissionRow,
    NppesChainArchiveRow,
    build_nppes_chain_storage_rows,
    chain_admission_values,
    chain_archive_values,
)
from process.nppes_public_evidence_storage import resolve_nppes_evidence_family
from process.nppes_public_evidence_writer import (
    _insert_single,
    _invoke,
    _qualified,
    _quoted,
    _schema_name,
    writer_error,
)
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction

_CHAIN_TABLE = "public_evidence_nppes_registry_chain_admission"
_CHAIN_SEAL_TABLE = "public_evidence_nppes_registry_chain_admission_seal"
_ARCHIVE_TABLE = "public_evidence_nppes_registry_chain_archive"
_RELEASE_ADMISSION_TABLE = "public_evidence_nppes_registry_admission"
_RELEASE_SEAL_TABLE = "public_evidence_nppes_registry_admission_seal"
_RELEASE_OWNER_COLUMNS = (
    "admission_ref",
    "source_release_ref",
    "archive_name",
    "snapshot_at",
    "artifact_sha256",
    "manifest_sha256",
    "source_record_count",
    "projected_record_count",
    "excluded_record_count",
)


async def _assert_catalog(connection: object, schema: str, policy: object) -> None:
    await assert_nppes_candidate_catalog(connection, schema, policy)


async def _stored_rows(
    connection: object,
    schema: str,
    admission: NppesChainAdmissionRow,
) -> tuple[object | None, tuple[object, ...]]:
    parent_columns = ", ".join(_quoted(column) for column in CHAIN_ADMISSION_COLUMNS)
    child_columns = ", ".join(_quoted(column) for column in CHAIN_ARCHIVE_COLUMNS)
    parent = await connection.fetchrow(
        f"SELECT {parent_columns} FROM {_qualified(schema, _CHAIN_TABLE)} WHERE chain_ref=$1",
        admission.chain_ref,
    )
    children = await connection.fetch(
        f"SELECT {child_columns} FROM {_qualified(schema, _ARCHIVE_TABLE)} WHERE chain_ref=$1 ORDER BY archive_ordinal",
        admission.chain_ref,
    )
    return parent, tuple(children)


def _row_tuple(row: object, columns: tuple[str, ...]) -> tuple[object, ...]:
    return tuple(row[column] for column in columns)


async def _assert_release_admissions(
    connection: object,
    schema: str,
    archives: tuple[NppesChainArchiveRow, ...],
) -> None:
    column_sql = ", ".join(f"admitted.{_quoted(column)}" for column in _RELEASE_OWNER_COLUMNS)
    stored_rows = await connection.fetch(
        f"SELECT {column_sql} FROM "
        f"{_qualified(schema, _RELEASE_ADMISSION_TABLE)} AS admitted "
        f"JOIN {_qualified(schema, _RELEASE_SEAL_TABLE)} AS sealed "
        "ON sealed.admission_ref=admitted.admission_ref "
        f"JOIN {_qualified(schema, 'public_evidence_source_release')} AS released "
        "ON (released.source_release_ref,released.contract_sha256,released.source_kind)="
        "(admitted.source_release_ref,admitted.source_release_contract_sha256,admitted.source_kind) "
        f"JOIN {_qualified(schema, 'public_evidence_source_identity')} AS artifact "
        "ON (artifact.identity_ref,artifact.identity_kind,artifact.content_identity_kind,artifact.content_sha256)="
        "(released.artifact_identity_ref,released.artifact_identity_kind,released.artifact_content_identity_kind,"
        "released.artifact_content_sha256) WHERE admitted.admission_ref=ANY($1::text[]) "
        "AND ROW(released.artifact_content_sha256,released.expected_record_count,released.observed_record_count,"
        "released.evidence_root_sha256,released.rights_proof_sha256,released.observed_start_at,released.observed_end_at,"
        "released.effective_start_at,released.effective_end_at) IS NOT DISTINCT FROM "
        "ROW(admitted.artifact_sha256,admitted.source_record_count,admitted.source_record_count,"
        "admitted.evidence_root_sha256,admitted.rights_proof_sha256,admitted.snapshot_at,admitted.snapshot_at,"
        "admitted.minimum_effective_start_at,admitted.snapshot_at)",
        [archive.admission_ref for archive in archives],
    )
    stored_by_ref = {stored_release_row["admission_ref"]: stored_release_row for stored_release_row in stored_rows}
    if len(stored_by_ref) != len(archives):
        raise writer_error()
    for archive in archives:
        values_by_column = dict(zip(CHAIN_ARCHIVE_COLUMNS, chain_archive_values(archive), strict=True))
        expected_owner_values = tuple(values_by_column[column] for column in _RELEASE_OWNER_COLUMNS)
        stored_owner_row = stored_by_ref.get(archive.admission_ref)
        if stored_owner_row is None or _row_tuple(stored_owner_row, _RELEASE_OWNER_COLUMNS) != expected_owner_values:
            raise writer_error()
        await resolve_nppes_evidence_family(connection, schema, archive.admission_ref)


async def _has_existing_chain(
    connection: object,
    schema: str,
    admission: NppesChainAdmissionRow,
    archives: tuple[NppesChainArchiveRow, ...],
) -> bool:
    parent_row, child_rows = await _stored_rows(connection, schema, admission)
    if parent_row is None:
        if child_rows:
            raise writer_error()
        listing_owner = await connection.fetchval(
            f"SELECT chain_ref FROM {_qualified(schema, _CHAIN_TABLE)} WHERE listing_sha256=$1",
            bytes.fromhex(admission.listing_sha256),
        )
        if listing_owner is not None:
            raise writer_error()
        return False
    expected_parent = chain_admission_values(admission)
    expected_child_rows = tuple(chain_archive_values(archive_row) for archive_row in archives)
    if (
        _row_tuple(parent_row, CHAIN_ADMISSION_COLUMNS) != expected_parent
        or tuple(_row_tuple(child_row, CHAIN_ARCHIVE_COLUMNS) for child_row in child_rows) != expected_child_rows
    ):
        raise writer_error()
    is_sealed = await connection.fetchval(
        f"SELECT EXISTS (SELECT 1 FROM {_qualified(schema, _CHAIN_SEAL_TABLE)} WHERE chain_ref=$1)",
        admission.chain_ref,
    )
    if is_sealed is not True:
        raise writer_error()
    return True


async def _insert_chain(
    connection: object,
    schema: str,
    admission: NppesChainAdmissionRow,
    archives: tuple[NppesChainArchiveRow, ...],
) -> None:
    await _insert_single(
        connection,
        schema,
        _CHAIN_TABLE,
        CHAIN_ADMISSION_COLUMNS,
        chain_admission_values(admission),
    )
    for archive in archives:
        await _insert_single(
            connection,
            schema,
            _ARCHIVE_TABLE,
            CHAIN_ARCHIVE_COLUMNS,
            chain_archive_values(archive),
        )
    await connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


async def _admit_chain(
    receipt: NppesPublicEvidenceChainReceipt,
    schema: str,
    database: object,
    cancel_check,
) -> NppesPublicEvidenceChainReceipt:
    from process.nppes_isolated_catalog import issued_isolated_nppes_catalog, publish_isolated_nppes_chain

    isolated_catalog = issued_isolated_nppes_catalog()
    if isolated_catalog is not None:
        if isolated_catalog.schema != schema:
            raise writer_error()
        await _invoke(cancel_check)
        await publish_isolated_nppes_chain(database, receipt)
        await _invoke(cancel_check)
        return receipt
    admission, archives = build_nppes_chain_storage_rows(receipt)
    async with source_transaction(database):
        publisher = bound_publisher()
        if publisher.policy.schema != schema:
            raise writer_error()
        connection = publisher.connection
        await _assert_catalog(connection, schema, publisher.policy)
        await _invoke(cancel_check)
        await connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended('healthporta.public-evidence-nppes-chain:' || $1, 0))",
            receipt.listing_sha256,
        )
        await _invoke(cancel_check)
        await _assert_release_admissions(connection, schema, archives)
        if not await _has_existing_chain(connection, schema, admission, archives):
            await _insert_chain(connection, schema, admission, archives)
        await _invoke(cancel_check)
    return receipt


async def admit_nppes_public_evidence_chain(
    receipt: object,
    *,
    schema: str = "mrf",
    database: object = db,
    cancel_check=None,
) -> NppesPublicEvidenceChainReceipt:
    """Persist the exact listing and selected release vector before publication."""

    try:
        fixed = validate_nppes_public_evidence_chain_receipt(receipt)
        admitted_receipt = await _admit_chain(
            fixed,
            _schema_name(schema),
            database,
            cancel_check,
        )
    except asyncio.CancelledError, ImportCancelledError:
        raise
    except Exception:
        normalized_error = writer_error()
    else:
        return admitted_receipt
    raise normalized_error


__all__ = ("admit_nppes_public_evidence_chain",)
