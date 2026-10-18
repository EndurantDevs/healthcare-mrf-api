# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only, exact set validation of a closed isolated NPPES table family."""

from __future__ import annotations

import re

from process.nppes_public_evidence_import import NppesEvidenceRuntimeConfig
from process.nppes_public_evidence_prepared_chunks import COLUMNS
from process.nppes_public_evidence_replay import PreparedNppesRegistryReplay, validate_prepared_nppes_registry_replay
from process.nppes_public_evidence_writer_contract import writer_error

_IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]{0,62}", re.ASCII)
_COLUMNS = COLUMNS
_KEYS = {root: "evidence_ref" for root in _COLUMNS}
_KEYS.update(source_record="source_record_ref", member="source_row_ordinal")
_DATES = {
    "provider_enumeration_date",
    "last_update_date",
    "npi_deactivation_date",
    "npi_reactivation_date",
}
_TIMESTAMPS = {"observed_at", "effective_start_at", "effective_end_at"}
_BOOLEANS = {"positive_evidence_only", "publication_enabled"}
_INTEGERS = {"source_row_ordinal", "source_record_ordinal", "source_record_count"}


_LIFECYCLE_SQL = """
WITH eligible_dates AS (
    SELECT member.*,
           CASE WHEN npi_deactivation_date <= ($1::text::timestamptz AT TIME ZONE 'UTC')::date
                THEN npi_deactivation_date END AS deactivation,
           CASE WHEN npi_reactivation_date <= ($1::text::timestamptz AT TIME ZONE 'UTC')::date
                THEN npi_reactivation_date END AS reactivation
      FROM {member} member
), lifecycle AS (
    SELECT eligible_dates.*,
           deactivation IS NOT NULL AND (reactivation IS NULL OR reactivation < deactivation) AS deactivated,
           CASE WHEN deactivation IS NOT NULL AND (reactivation IS NULL OR reactivation < deactivation)
                THEN deactivation ELSE COALESCE(reactivation, provider_enumeration_date) END AS effective_date
      FROM eligible_dates
)
SELECT COALESCE(bool_or(
           member.provider_enumeration_date > ($1::text::timestamptz AT TIME ZONE 'UTC')::date
           OR member.provider_enumeration_date > member.deactivation
           OR member.provider_enumeration_date > member.reactivation
           OR member.exclusion_reason IS DISTINCT FROM CASE
                WHEN member.effective_date IS NULL THEN 'effective_start_not_disclosed'
                WHEN member.entity_type_code IS NULL THEN 'entity_type_not_disclosed' END
           OR member.projection_state IS DISTINCT FROM CASE
                WHEN member.effective_date IS NULL OR member.entity_type_code IS NULL
                THEN 'excluded_v1' ELSE 'projected_v1' END
           OR (member.projection_state='projected_v1' AND (
                typed.enumeration_state IS DISTINCT FROM CASE
                    WHEN member.deactivated THEN 'deactivated' ELSE 'active' END
                OR common.observed_at IS DISTINCT FROM $1::text::timestamptz
                OR common.effective_start_at IS DISTINCT FROM
                    (member.effective_date::timestamp AT TIME ZONE 'UTC')
                OR common.effective_end_at IS DISTINCT FROM $1::text::timestamptz
           ))
       ), false)
       OR COALESCE(min(member.effective_date)::timestamp AT TIME ZONE 'UTC', $1::text::timestamptz)
          IS DISTINCT FROM $2::text::timestamptz
  FROM lifecycle member
  LEFT JOIN {common} common ON common.evidence_ref=member.evidence_ref
       AND common.nppes_admission_ref=member.admission_ref
  LEFT JOIN {typed} typed ON typed.evidence_ref=member.evidence_ref
       AND typed.nppes_admission_ref=member.admission_ref
"""


async def _assert_lifecycle(connection, tables, prepared) -> None:
    """Keep the independent native UTC lifecycle proof before comparing retained chunks."""
    invalid = await connection.fetchval(
        _LIFECYCLE_SQL.format(**tables),
        prepared.admission_row.snapshot_at,
        prepared.admission_row.minimum_effective_start_at,
    )
    if invalid is not False:
        raise writer_error()


def _qualified(schema: object, name: object) -> str:
    if any(type(value) is not str or _IDENTIFIER.fullmatch(value) is None for value in (schema, name)):
        raise writer_error()
    return f'"{schema}"."{name}"'


def _owner(root: str) -> str:
    return "admission_ref" if root == "member" else "nppes_admission_ref"


def _column_type(column: str) -> str:
    if column.endswith("_sha256"):
        return "bytea"
    if column in _DATES:
        return "date"
    if column in _TIMESTAMPS:
        return "timestamptz"
    if column in _BOOLEANS:
        return "boolean"
    return "bigint" if column in _INTEGERS else "text"


async def _assert_inventory(connection, tables, prepared) -> None:
    admitted = prepared.admission_row
    owner = (
        admitted.source_release_ref,
        bytes.fromhex(admitted.source_release_contract_sha256),
        admitted.source_kind,
        admitted.admission_ref,
    )
    for root, relation in tables.items():
        count = admitted.source_record_count if root in {"source_record", "member"} else admitted.projected_record_count
        invalid = await connection.fetchval(
            f"SELECT (SELECT count(*) FROM {relation}) IS DISTINCT FROM $5::bigint OR EXISTS ("
            f"SELECT 1 FROM {relation} WHERE ROW(source_release_ref, source_release_contract_sha256, "
            f"source_kind, {_owner(root)}) IS DISTINCT FROM ROW($1::text, $2::bytea, $3::text, $4::text))",
            *owner,
            count,
        )
        if invalid is not False:
            raise writer_error()
    member = tables["member"]
    duplicates = " UNION ALL ".join(
        f"SELECT 1 FROM {member} WHERE {column} IS NOT NULL GROUP BY source_release_ref, {column} HAVING count(*) > 1"
        for column in ("source_row_ordinal", "npi", "source_record_ref", "evidence_ref")
    )
    if await connection.fetchval(f"SELECT EXISTS ({duplicates})") is not False:
        raise writer_error()
    missing_entries = []
    for root in ("source_record", "common", "source_link", "typed"):
        key = "source_record_ref" if root == "source_record" else "evidence_ref"
        projected = "" if root == "source_record" else " AND m.projection_state='projected_v1'"
        join = (
            f"r.{key}=m.{key} AND r.{_owner(root)}=m.admission_ref "
            "AND r.source_release_ref=m.source_release_ref "
            "AND r.source_release_contract_sha256=m.source_release_contract_sha256 "
            "AND r.source_kind=m.source_kind"
        )
        missing_entries.extend(
            (
                f"SELECT 1 FROM {member} m LEFT JOIN {tables[root]} r ON {join} WHERE r.{key} IS NULL{projected}",
                f"SELECT 1 FROM {tables[root]} r LEFT JOIN {member} m ON {join}{projected} "
                "WHERE m.source_row_ordinal IS NULL",
            )
        )
    if await connection.fetchval("SELECT EXISTS (" + " UNION ALL ".join(missing_entries) + ")") is not False:
        raise writer_error()


async def _assert_prepared_chunk(connection, tables, chunks, ordinal, admission_ref) -> None:
    for root, columns in _COLUMNS.items():
        column_sql = ", ".join(f'"{column}"' for column in columns)
        definition = ", ".join(f'"{column}" {_column_type(column)}' for column in columns)
        key = _KEYS[root]
        statement = (
            f"WITH expected AS (SELECT * FROM jsonb_to_recordset((SELECT "
            f"convert_from({root}_payload,'UTF8')::jsonb FROM {chunks} WHERE chunk_ordinal=$1)) "
            f"AS prepared({definition})), actual AS (SELECT {column_sql} FROM {tables[root]} "
            f'WHERE {_owner(root)}=$2::text AND "{key}" IN (SELECT "{key}" FROM expected)) '
            "SELECT EXISTS ((TABLE actual EXCEPT ALL TABLE expected) "
            "UNION ALL (TABLE expected EXCEPT ALL TABLE actual))"
        )
        if await connection.fetchval(statement, ordinal, admission_ref) is not False:
            raise writer_error()


async def validate_nppes_evidence_family(
    connection,
    schema,
    tables_by_root,
    prepared,
    *,
    prepared_chunks,
    chunk_count,
) -> None:
    """Compare the closed family to its retained, initially canonical chunks.

    Storage verifies complete chunk custody, digests and ordered source identity
    before this set comparison. Archive decoding belongs only to preparation.
    """
    if (
        type(tables_by_root) is not dict
        or set(tables_by_root) != set(_COLUMNS)
        or type(chunk_count) is not int
        or chunk_count < 0
    ):
        raise writer_error()
    tables_by_key = {root: _qualified(schema, tables_by_root[root]) for root in _COLUMNS}
    chunks = _qualified(schema, prepared_chunks)
    if len(set(tables_by_key.values())) != len(_COLUMNS) or chunks in tables_by_key.values():
        raise writer_error()
    if type(prepared) is not PreparedNppesRegistryReplay:
        raise writer_error()
    prepared = validate_prepared_nppes_registry_replay(
        prepared,
        NppesEvidenceRuntimeConfig("required", prepared.manifest.identity.rights_proof_sha256),
    )
    await _assert_inventory(connection, tables_by_key, prepared)
    await _assert_lifecycle(connection, tables_by_key, prepared)
    for ordinal in range(1, chunk_count + 1):
        await _assert_prepared_chunk(connection, tables_by_key, chunks, ordinal, prepared.admission_row.admission_ref)


__all__ = ("validate_nppes_evidence_family",)
