# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Complete NY registry capture and supported NPPES-derived acquisition scope."""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections import Counter, defaultdict
from pathlib import Path

import asyncpg

from db.models import db
from process.massachusetts_profile_acquisition import encoded_json
from process.new_york_profile_binding import (
    COVERAGE_SCOPE,
    MAX_SNAPSHOT_BYTES,
    QUERY_SHA256,
    REGISTRY_COLUMNS,
    SNAPSHOT_SCHEMA,
    _physician_candidate,
    _validate_snapshot_metadata,
    read_registry_snapshot,
)

# Resource bounds, not a physician census or acquisition-performance guarantee.
MAX_REGISTRY_ROWS = 1_000_000
MAX_ROW_BYTES = 64 * 1024
REGISTRY_PRECONDITIONS = (
    "registry_occurrence_or_name_conflict",
    "multiple_npis_even_if_source_names_agree",
    "single_npi_source_identity_unverified",
)
COHORT_DIAGNOSTICS = (
    "excluded_no_physician_root_count",
    "excluded_no_physician_row_count",
    "selected_row_count",
    "selected_physician_row_count",
    "selected_conflicting_occurrence_count",
    "selected_roots_with_multiple_occurrences",
    "selected_roots_with_multiple_npis",
    "selected_roots_with_conflicting_name_components",
    "selected_roots_with_invalid_or_unreported_names",
)
CAPTURE_QUERY = """SELECT t.npi,
       t.checksum AS taxonomy_occurrence_checksum,
       t.provider_license_number AS license_number,
       t.provider_license_number_state_code AS license_state,
       t.healthcare_provider_taxonomy_code AS taxonomy,
       t.healthcare_provider_primary_taxonomy_switch AS primary_taxonomy_switch,
       n.npi AS joined_npi,
       n.entity_type_code,
       n.provider_first_name AS first_name,
       n.provider_middle_name AS middle_name,
       n.provider_last_name AS last_name,
       n.provider_name_suffix_text AS suffix,
       u.code AS joined_taxonomy_code,
       u.grouping AS taxonomy_grouping
  FROM mrf.npi_taxonomy t
  LEFT JOIN mrf.npi n ON n.npi = t.npi
  LEFT JOIN mrf.nucc_taxonomy u ON u.code = t.healthcare_provider_taxonomy_code
 WHERE t.provider_license_number_state_code = 'NY'
 ORDER BY t.npi, t.checksum;
"""
COUNT_QUERY = """SELECT count(*) AS expected_taxonomy_occurrences FROM mrf.npi_taxonomy WHERE provider_license_number_state_code = 'NY';
"""
SNAPSHOT_SQL = """SELECT current_setting('transaction_read_only') AS read_only,
       current_setting('transaction_isolation') AS isolation,
       txid_current_snapshot()::text AS snapshot_id,
       transaction_timestamp()::text AS snapshot_started_at,
       pg_backend_pid() AS backend_pid,
       current_setting('server_version') AS server_version,
       current_database() AS database_name;
"""
RELATIONS_SQL = """SELECT name, to_regclass(name)::oid::bigint AS oid
  FROM unnest(ARRAY['mrf.npi', 'mrf.npi_taxonomy', 'mrf.nucc_taxonomy']::text[]) AS name;
"""


def _require(condition, reason):
    if not condition:
        raise ValueError("new_york_registry_" + reason)


def _validate_occurrence(candidate, previous):
    _require(
        set(candidate) == set(REGISTRY_COLUMNS)
        and candidate["license_state"] == "NY"
        and (candidate["license_number"] is None or isinstance(candidate["license_number"], str))
        and type(candidate["npi"]) is int
        and type(candidate["taxonomy_occurrence_checksum"]) is int,
        "row_invalid",
    )
    occurrence = candidate["npi"], candidate["taxonomy_occurrence_checksum"]
    _require(previous is None or previous <= occurrence, "row_order_changed")
    content = encoded_json(candidate)
    _require(len(content) <= MAX_ROW_BYTES, "row_too_large")
    return occurrence, content


async def _open_connection():
    if db.engine is None:
        await db.connect()
    return await asyncpg.connect(
        dsn=db.engine.url.set(drivername="postgresql").render_as_string(hide_password=False),
        timeout=10,
        server_settings={"statement_timeout": "90000", "lock_timeout": "5000"},
    )


async def _registry_rows(connection, expected_count, progress):
    digest = hashlib.sha256(b"[")
    registry_rows, row_bytes, previous = [], 2, None
    async for source_row in connection.cursor(CAPTURE_QUERY, prefetch=500):
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
        if progress is not None and len(registry_rows) % 500 == 0:
            await progress(len(registry_rows), expected_count)
    digest.update(b"]")
    _require(len(registry_rows) == expected_count, "count_changed")
    if progress is not None:
        await progress(len(registry_rows), expected_count)
    return {
        "registry_rows": registry_rows,
        "row_count": len(registry_rows),
        "registry_rows_bytes": row_bytes,
        "registry_rows_sha256": digest.hexdigest(),
    }


async def capture_registry_snapshot(progress=None):
    """Receive every literal NY occurrence in one owned read-only transaction.

    The source-table count also detects join multiplication. Nullable joins,
    duplicate occurrences and unsupported license formats survive unchanged.
    Capture success does not authorize source acquisition or publication.
    """
    _require(hashlib.sha256(CAPTURE_QUERY.encode()).hexdigest() == QUERY_SHA256, "query_changed")
    if progress is not None:
        await progress(0, 0)
    connection = None
    try:
        async with asyncio.timeout(180):
            connection = await _open_connection()
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                transaction_by_field = dict(await connection.fetchrow(SNAPSHOT_SQL))
                _require(
                    transaction_by_field.get("read_only") == "on"
                    and transaction_by_field.get("isolation") == "repeatable read",
                    "transaction_invalid",
                )
                expected_count = await connection.fetchval(COUNT_QUERY)
                _require(type(expected_count) is int and 0 <= expected_count <= MAX_REGISTRY_ROWS, "count_invalid")
                retained = await _registry_rows(connection, expected_count, progress)
                relations_by_name = {
                    relation["name"]: relation["oid"] for relation in await connection.fetch(RELATIONS_SQL)
                }
                _require(dict(await connection.fetchrow(SNAPSHOT_SQL)) == transaction_by_field, "snapshot_changed")
                retained.update(
                    schema_version=SNAPSHOT_SCHEMA,
                    coverage_scope=COVERAGE_SCOPE,
                    source_schema="mrf",
                    source_state="NY",
                    snapshot=transaction_by_field,
                    query_sha256=QUERY_SHA256,
                    columns=list(REGISTRY_COLUMNS),
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
    _validate_snapshot_metadata(retained)
    _require(len(encoded_json(retained)) <= MAX_SNAPSHOT_BYTES, "snapshot_too_large")
    return retained


def _license_format(license_number):
    if license_number is None:
        return "null"
    if re.fullmatch(r"[0-9]{6}", license_number):
        return "exact_six_digits"
    if not license_number:
        return "empty"
    if not license_number.strip():
        return "whitespace_only"
    if re.fullmatch(r"[0-9]{6}", license_number.strip()):
        return "padded_six_digits"
    if re.fullmatch(r"[0-9]+", license_number):
        return "other_digit_lengths"
    return "other_text"


def _registry_names(candidate):
    names = []
    for field in ("first_name", "middle_name", "last_name", "suffix"):
        if field not in candidate:
            return None
        value = candidate[field]
        if value is None and field in {"middle_name", "suffix"}:
            value = ""
        if not isinstance(value, str):
            return None
        value = " ".join(value.split()).casefold()
        if not value and field in {"first_name", "last_name"}:
            return None
        names.append(value)
    return tuple(names)


def _literal_license_inventory(registry_rows):
    indexes_by_license = defaultdict(list)
    physician_indexes = set()
    format_counts = Counter()
    unsupported_physician_count = 0
    for index, candidate in enumerate(registry_rows):
        license_number = candidate["license_number"]
        license_format = _license_format(license_number)
        format_counts[license_format] += 1
        if license_format == "exact_six_digits":
            indexes_by_license[license_number].append(index)
            if _physician_candidate(candidate):
                physician_indexes.add(index)
        elif _physician_candidate(candidate):
            unsupported_physician_count += 1
    return (
        indexes_by_license,
        physician_indexes,
        {
            "exact_six_digit_row_count": format_counts["exact_six_digits"],
            "exact_six_digit_root_count": len(indexes_by_license),
            "unsupported_license_row_count": len(registry_rows) - format_counts["exact_six_digits"],
            "unsupported_valid_physician_row_count": unsupported_physician_count,
            "unsupported_license_rows_by_format": {
                key: format_counts[key]
                for key in (
                    "null",
                    "empty",
                    "whitespace_only",
                    "padded_six_digits",
                    "other_digit_lengths",
                    "other_text",
                )
            },
        },
    )


def _select_acquisition_roots(registry_rows, indexes_by_license, physician_indexes):
    roots = []
    preconditions = Counter(dict.fromkeys(REGISTRY_PRECONDITIONS, 0))
    diagnostics = Counter(dict.fromkeys(COHORT_DIAGNOSTICS, 0))
    selected_physician_npis = set()
    for license_number, indexes in sorted(indexes_by_license.items()):
        valid_indexes = [index for index in indexes if index in physician_indexes]
        if not valid_indexes:
            diagnostics["excluded_no_physician_root_count"] += 1
            diagnostics["excluded_no_physician_row_count"] += len(indexes)
            continue
        names = {_registry_names(registry_rows[index]) for index in indexes}
        npis = {registry_rows[index]["npi"] for index in indexes if type(registry_rows[index].get("npi")) is int}
        if len(valid_indexes) != len(indexes) or None in names:
            precondition = "registry_occurrence_or_name_conflict"
        elif len(npis) != 1:
            precondition = "multiple_npis_even_if_source_names_agree"
        elif len(names) != 1:
            precondition = "registry_occurrence_or_name_conflict"
        else:
            precondition = "single_npi_source_identity_unverified"
        preconditions[precondition] += 1
        roots.append(
            {
                "license_number": license_number,
                "registry_occurrence_indexes": indexes,
                "registry_only_precondition": precondition,
            }
        )
        diagnostics["selected_row_count"] += len(indexes)
        diagnostics["selected_physician_row_count"] += len(valid_indexes)
        diagnostics["selected_conflicting_occurrence_count"] += len(indexes) - len(valid_indexes)
        diagnostics["selected_roots_with_multiple_occurrences"] += len(indexes) > 1
        diagnostics["selected_roots_with_multiple_npis"] += len(npis) > 1
        diagnostics["selected_roots_with_conflicting_name_components"] += len(names - {None}) > 1
        diagnostics["selected_roots_with_invalid_or_unreported_names"] += None in names
        selected_physician_npis.update(registry_rows[index]["npi"] for index in valid_indexes)
    return roots, {
        **diagnostics,
        "acquisition_root_count": len(roots),
        "selected_distinct_valid_physician_npis": len(selected_physician_npis),
        "registry_only_preconditions": dict(preconditions),
        "registry_only_conflict_or_ambiguity_root_count": len(roots)
        - preconditions["single_npi_source_identity_unverified"],
    }


def build_acquisition_cohort(snapshot_path: Path, *, snapshot_sha256: str) -> dict:
    """Select supported roots from one pinned complete NPPES-derived snapshot.

    Any valid individual physician occurrence selects its exact six-digit root;
    every occurrence for that root remains referenced, including conflicts.
    Registry-only diagnostics never assign an NPI or decide source identity.
    Integrity verification does not authenticate capture, freshness or a state
    census. Unsupported formats remain in the complete input artifact.
    """
    snapshot = read_registry_snapshot(snapshot_path, snapshot_sha256=snapshot_sha256)
    indexes_by_license, physician_indexes, format_summary = _literal_license_inventory(snapshot["registry_rows"])
    roots, root_summary = _select_acquisition_roots(snapshot["registry_rows"], indexes_by_license, physician_indexes)
    return {
        "schema_version": "ny-nppes-acquisition-cohort/v1",
        "selection_rule": "exact_six_digit_license_with_any_valid_individual_physician_occurrence",
        "snapshot_sha256": snapshot_sha256,
        "query_sha256": snapshot["query_sha256"],
        "registry_rows_sha256": snapshot["registry_rows_sha256"],
        "integrity_verified": True,
        "capture_acceptance": "not_established_by_replay",
        "source_identity": "unverified",
        "state_census": False,
        "roots": roots,
        "summary": {"registry_row_count": snapshot["row_count"], **format_summary, **root_summary},
    }
