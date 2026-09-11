# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain complete literal RI registry occurrences; publication remains caller-owned."""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections import defaultdict
from pathlib import Path

import asyncpg

from db.models import db
from process.kentucky_profile_acquisition import _reject_symlinks
from process.massachusetts_profile_acquisition import encoded_json
from process.provider_directory_projection_json import decoded_json_object
from process.rhode_island_profile_binding import REGISTRY_COLUMNS, bind_profile

SNAPSHOT_SCHEMA = "ri-nppes-retained-snapshot/v1"
COVERAGE_SCOPE = "all_current_literal_ri_license_occurrences"
# Resource bounds, not measured RI coverage or performance guarantees.
MAX_SNAPSHOT_BYTES = 96 * 1024 * 1024
MAX_REGISTRY_ROWS = 250_000
MAX_ROW_BYTES = 64 * 1024
COLUMNS = (
    "npi",
    "taxonomy_occurrence_checksum",
    "license_number",
    "license_state",
    "taxonomy",
    "primary_taxonomy_switch",
    "joined_npi",
    "entity_type_code",
    "first_name",
    "middle_name",
    "last_name",
    "suffix",
    "joined_taxonomy_code",
    "taxonomy_grouping",
)
CAPTURE_QUERY = """SELECT t.npi, t.checksum AS taxonomy_occurrence_checksum,
       t.provider_license_number AS license_number,
       t.provider_license_number_state_code AS license_state,
       t.healthcare_provider_taxonomy_code AS taxonomy,
       t.healthcare_provider_primary_taxonomy_switch AS primary_taxonomy_switch,
       n.npi AS joined_npi, n.entity_type_code,
       n.provider_first_name AS first_name, n.provider_middle_name AS middle_name,
       n.provider_last_name AS last_name, n.provider_name_suffix_text AS suffix,
       u.code AS joined_taxonomy_code, u.grouping AS taxonomy_grouping
  FROM mrf.npi_taxonomy t
  LEFT JOIN mrf.npi n ON n.npi = t.npi
  LEFT JOIN mrf.nucc_taxonomy u ON u.code = t.healthcare_provider_taxonomy_code
 WHERE t.provider_license_number_state_code = 'RI'
 ORDER BY t.npi, t.checksum"""
SNAPSHOT_SQL = """SELECT current_setting('transaction_read_only') AS read_only,
    current_setting('transaction_isolation') AS isolation,
    pg_current_snapshot()::text AS snapshot_id, transaction_timestamp()::text AS snapshot_started_at,
    current_setting('server_version') AS server_version, current_database() AS database_name,
    pg_backend_pid() AS backend_pid"""


def _require(condition, reason):
    if not condition:
        raise ValueError("rhode_island_registry_" + reason)


def capture_query(source_schema="mrf"):
    """Select every literal RI occurrence, including invalid and nullable joins."""
    _require(
        isinstance(source_schema, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", source_schema), "schema_invalid"
    )
    return CAPTURE_QUERY.replace("mrf.", source_schema + ".")


def _validate_metadata(snapshot, source_schema):
    """Validate retained claims; an input hash alone is not capture authority."""
    _require(
        snapshot.get("schema_version") == SNAPSHOT_SCHEMA
        and snapshot.get("coverage_scope") == COVERAGE_SCOPE
        and snapshot.get("source_schema") == source_schema
        and snapshot.get("source_state") == "RI"
        and snapshot.get("columns") == list(COLUMNS)
        and snapshot.get("query_sha256") == hashlib.sha256(capture_query(source_schema).encode()).hexdigest(),
        "snapshot_scope_invalid",
    )
    _require(
        snapshot.get("status") == "passed"
        and snapshot.get("all_rows_received") is True
        and snapshot.get("connection_closed") is True,
        "snapshot_incomplete",
    )
    transaction = snapshot.get("snapshot")
    _require(
        isinstance(transaction, dict)
        and transaction.get("read_only") == "on"
        and transaction.get("isolation") == "repeatable read"
        and isinstance(transaction.get("snapshot_id"), str)
        and re.fullmatch(r"[0-9]+:[0-9]+:(?:[0-9]+(?:,[0-9]+)*)?", transaction["snapshot_id"])
        and all(
            isinstance(transaction.get(field), str) and transaction[field].strip()
            for field in ("snapshot_started_at", "server_version", "database_name")
        )
        and type(transaction.get("backend_pid")) is int
        and transaction["backend_pid"] > 0,
        "snapshot_transaction_invalid",
    )
    relations_by_name = snapshot.get("registry_relations")
    _require(
        isinstance(relations_by_name, dict)
        and set(relations_by_name) == {source_schema + "." + name for name in ("npi", "npi_taxonomy", "nucc_taxonomy")}
        and all(type(oid) is int and oid > 0 for oid in relations_by_name.values()),
        "snapshot_relations_invalid",
    )


def _validate_occurrence(candidate_by_field, previous):
    _require(
        isinstance(candidate_by_field, dict)
        and set(candidate_by_field) == REGISTRY_COLUMNS
        and candidate_by_field["license_state"] == "RI"
        and (candidate_by_field["license_number"] is None or isinstance(candidate_by_field["license_number"], str))
        and type(candidate_by_field["npi"]) is int
        and type(candidate_by_field["taxonomy_occurrence_checksum"]) is int,
        "row_invalid",
    )
    occurrence = candidate_by_field["npi"], candidate_by_field["taxonomy_occurrence_checksum"]
    _require(previous is None or previous <= occurrence, "row_order_changed")
    content = encoded_json(candidate_by_field)
    _require(len(content) <= MAX_ROW_BYTES, "row_too_large")
    return occurrence, content


def read_registry_snapshot(path: Path, *, snapshot_sha256: str, source_schema="mrf") -> dict:
    """Check complete canonical bytes without asserting their truth or freshness.

    A separately accepted capture must bind this digest to the exact reader and
    closed database snapshot before any conditional NPI decision can publish.
    """
    _require(isinstance(snapshot_sha256, str) and re.fullmatch(r"[a-f0-9]{64}", snapshot_sha256), "pin_invalid")
    path = Path(path)
    _reject_symlinks(path)
    _require(path.is_file(), "snapshot_file_invalid")
    with path.open("rb") as stream:
        content = stream.read(MAX_SNAPSHOT_BYTES + 1)
    _require(0 < len(content) <= MAX_SNAPSHOT_BYTES, "snapshot_too_large")
    _require(hashlib.sha256(content).hexdigest() == snapshot_sha256, "snapshot_changed")
    snapshot = decoded_json_object(content)
    _require(encoded_json(snapshot) == content, "snapshot_not_canonical")
    _validate_metadata(snapshot, source_schema)
    registry_rows = snapshot.get("registry_rows")
    _require(isinstance(registry_rows, list) and len(registry_rows) <= MAX_REGISTRY_ROWS, "rows_invalid")
    _require(
        type(snapshot.get("row_count")) is int
        and type(snapshot.get("expected_source_row_count")) is int
        and snapshot["expected_source_row_count"] == snapshot["row_count"] == len(registry_rows),
        "count_changed",
    )
    previous = None
    for candidate_by_field in registry_rows:
        previous, _ = _validate_occurrence(candidate_by_field, previous)
    registry_bytes = encoded_json(registry_rows)
    _require(
        type(snapshot.get("registry_rows_bytes")) is int
        and snapshot["registry_rows_bytes"] == len(registry_bytes)
        and len(registry_bytes) <= MAX_SNAPSHOT_BYTES - 1024 * 1024
        and snapshot.get("registry_rows_sha256") == hashlib.sha256(registry_bytes).hexdigest(),
        "rows_changed",
    )
    return snapshot


async def _open_connection():
    if db.engine is None:
        await db.connect()
    return await asyncpg.connect(
        dsn=db.engine.url.set(drivername="postgresql").render_as_string(hide_password=False),
        timeout=10,
        server_settings={"statement_timeout": "90000", "lock_timeout": "5000"},
    )


async def _registry_rows(connection, query, expected_count, progress):
    digest = hashlib.sha256(b"[")
    registry_rows, row_bytes, previous = [], 2, None
    async for source_row in connection.cursor(query, prefetch=500):
        candidate_by_field = dict(source_row)
        previous, content = _validate_occurrence(candidate_by_field, previous)
        piece = (b", " if registry_rows else b"") + content
        row_bytes += len(piece)
        _require(
            len(registry_rows) < MAX_REGISTRY_ROWS and row_bytes <= MAX_SNAPSHOT_BYTES - 1024 * 1024,
            "snapshot_too_large",
        )
        digest.update(piece)
        registry_rows.append(candidate_by_field)
        if len(registry_rows) % 500 == 0:
            await progress(len(registry_rows), expected_count)
    digest.update(b"]")
    _require(len(registry_rows) == expected_count, "count_changed")
    await progress(len(registry_rows), expected_count)
    return {
        "registry_rows": registry_rows,
        "row_count": len(registry_rows),
        "registry_rows_bytes": row_bytes,
        "registry_rows_sha256": digest.hexdigest(),
    }


async def capture_registry_snapshot(source_schema, progress):
    """Count and consume the same joined relation before closing the owned snapshot.

    No identity, physician or license-format filters may narrow the capture.
    Success here does not authorize source acquisition or profile publication.
    """
    query = capture_query(source_schema)
    await progress(0, 0)
    connection = None
    try:
        async with asyncio.timeout(180):
            connection = await _open_connection()
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                snapshot_by_field = dict(await connection.fetchrow(SNAPSHOT_SQL))
                expected_count = await connection.fetchval("SELECT count(*) FROM (" + query + ") AS occurrences")
                _require(type(expected_count) is int and 0 <= expected_count <= MAX_REGISTRY_ROWS, "count_invalid")
                retained = await _registry_rows(connection, query, expected_count, progress)
                names = [source_schema + "." + name for name in ("npi", "npi_taxonomy", "nucc_taxonomy")]
                relations_by_name = {
                    relation_row["name"]: relation_row["oid"]
                    for relation_row in await connection.fetch(
                        "SELECT name, to_regclass(name)::oid::bigint AS oid FROM unnest($1::text[]) name", names
                    )
                }
                _require(dict(await connection.fetchrow(SNAPSHOT_SQL)) == snapshot_by_field, "snapshot_changed")
                retained.update(
                    schema_version=SNAPSHOT_SCHEMA,
                    coverage_scope=COVERAGE_SCOPE,
                    source_schema=source_schema,
                    source_state="RI",
                    snapshot=snapshot_by_field,
                    query_sha256=hashlib.sha256(query.encode()).hexdigest(),
                    columns=list(COLUMNS),
                    registry_relations=relations_by_name,
                    expected_source_row_count=expected_count,
                    all_rows_received=True,
                )
    finally:
        if connection is not None:
            try:
                await connection.close(timeout=10)
            finally:
                if not connection.is_closed():
                    connection.terminate()
    _require(connection is not None and connection.is_closed(), "connection_not_closed")
    retained.update(connection_closed=True, status="passed")
    _validate_metadata(retained, source_schema)
    _require(len(encoded_json(retained)) <= MAX_SNAPSHOT_BYTES, "snapshot_too_large")
    return retained


def bind_snapshot_profiles(profiles, *, snapshot_path, snapshot_sha256, source_schema="mrf"):
    """Bind (license, raw profile bytes, evidence) triples against one retained index.

    Every exact-license occurrence survives with its original snapshot index.
    Lists alone never prove completeness: replay integrity is explicit, while
    independent capture acceptance and the supplied profile scope remain outside
    this helper. NULL and unprefixed licenses stay in the complete artifact.
    """
    snapshot = read_registry_snapshot(snapshot_path, snapshot_sha256=snapshot_sha256, source_schema=source_schema)
    candidates_by_license = defaultdict(list)
    for index, candidate_by_field in enumerate(snapshot["registry_rows"]):
        candidates_by_license[candidate_by_field["license_number"]].append((index, candidate_by_field))
    for license_number, content, evidence in profiles:
        candidates = candidates_by_license[license_number]
        source_record, facts = bind_profile(
            content,
            license_number=license_number,
            evidence=evidence,
            candidates=[candidate for _, candidate in candidates],
        )
        decision_by_field = source_record["match_evidence"]["registry_binding"]
        decision_by_field["retained_snapshot"] = {
            "snapshot_sha256": snapshot_sha256,
            "schema_version": SNAPSHOT_SCHEMA,
            "coverage_scope": COVERAGE_SCOPE,
            "query_sha256": snapshot["query_sha256"],
            "row_count": snapshot["row_count"],
            "candidate_occurrence_indexes": [index for index, _ in candidates],
            "integrity_verified": True,
            "capture_acceptance": "not_established_by_replay",
        }
        yield source_record, facts
