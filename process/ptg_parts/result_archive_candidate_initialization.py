# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Initialize destination-owned logical state for one PTG result archive.

The restored schema is evidence, not destination authority.  This module
authenticates its selected source snapshot against the source callback receipt,
then creates a new local building attempt from caller-admitted frozen inputs.
It does not copy the source run, activation state, candidate attestation, pins,
or current pointers.
"""

from __future__ import annotations

import copy
import datetime as dt
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from sqlalchemy import text

from db.connection import db
from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.domain import PTG2_CANDIDATE_ACTIVATION_CONTRACT
from process.ptg_parts.frozen_rate_binding import (
    FROZEN_RATE_FILE_BINDING_OPTION,
    INVALID_PRICE_EXCLUSION_POLICY_FIELD,
    frozen_internal_run_id,
    frozen_rate_binding_from_params,
    frozen_rate_binding_sha256,
)
from process.ptg_parts.frozen_rate_binding_store import insert_or_compare_frozen_binding
from process.ptg_parts.frozen_rate_candidate import validate_frozen_candidate_evidence
from process.ptg_parts.ptg2_candidate_attestation import CANDIDATE_SOURCE_RECORDS_SQL
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.result_archive_source_authority import (
    result_archive_manifest_sha256,
    validate_ptg_result_archive_source_authority,
)

RESULT_ARCHIVE_CANDIDATE_INITIALIZATION_CONTRACT = "ptg_result_archive_candidate_initialization_v1"
_SNAPSHOT_ARCH = "postgres_binary_v3"
_STORAGE_GENERATION = "shared_blocks_v4"
_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_REMOTE_AUTHORITY_FIELDS = frozenset(
    {
        "activation",
        "activation_status",
        "already_published",
        "import_run_id",
        "publish_status",
        "published_at",
        "serving_index",
        "snapshot_status",
        "validated_at",
    }
)
_SOURCE_GRAPH_TABLES = (
    "ptg2_source_identity",
    "ptg2_content_identity",
    "ptg2_source_file_version",
    "ptg2_source_trace",
    "ptg2_source_trace_set",
)
_SOURCE_GRAPH_COLUMNS_BY_TABLE = {
    "ptg2_source_identity": (
        "source_identity_hash",
        "hash_prefix",
        "source_type",
        "canonical_url",
    ),
    "ptg2_content_identity": (
        "content_hash",
        "hash_prefix",
        "domain",
        "logical_sha256",
        "canonical_payload",
    ),
    "ptg2_source_file_version": (
        "source_file_version_id",
        "source_identity_hash",
        "content_hash",
        "raw_sha256",
        "logical_sha256",
        "content_length",
        "etag",
        "last_modified",
        "verification_mode",
        "payload",
    ),
    "ptg2_source_trace": (
        "source_trace_hash",
        "source_file_version_id",
        "original_url",
        "canonical_url",
        "json_pointer",
        "line_number",
    ),
    "ptg2_source_trace_set": (
        "source_trace_set_hash",
        "source_trace_hashes",
    ),
}
_SOURCE_GRAPH_PORTABLE_EXPRESSIONS_BY_TABLE = {
    "ptg2_source_file_version": {
        "payload": (
            "jsonb_build_object("
            "'raw_byte_count', ({alias}.\"payload\"::jsonb)->'raw_byte_count', "
            "'logical_hash_deferred', ({alias}.\"payload\"::jsonb)->'logical_hash_deferred'"
            ")::json"
        )
    }
}


class ResultArchiveCandidateInitializationError(RuntimeError):
    """The restored source or destination retry is not the admitted candidate."""


@dataclass(frozen=True)
class InitializedResultArchiveCandidate:
    """One local building candidate ready for logical evidence preparation."""

    contract: str
    destination_snapshot_id: str
    destination_import_run_id: str
    source_snapshot_id: str
    source_count: int
    plan_scope_count: int
    frozen_binding_sha256: str
    reused: bool
    requires_fresh_destination_attestation: bool = True


@dataclass(frozen=True)
class _AuthenticatedStagedCandidate:
    source_snapshot_id: str
    source_manifest: Mapping[str, Any]
    primary_plan_id: str
    primary_plan_market_type: str
    coverage_scope_id: bytes
    plan_scopes: tuple[tuple[str, str], ...]
    source_records: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class _LocalCandidateAdmission:
    destination_schema: str
    staging_schema: str
    source_snapshot_key: int
    destination_snapshot_id: str
    import_run_id: str
    binding: Mapping[str, Any]
    source_receipt: Mapping[str, Any]


@dataclass(frozen=True)
class _SelectedRelationSql:
    destination: str
    staging: str
    table: str
    column_list: str
    source_column_list: str
    predicate: str
    key_match: str
    missing_key: str
    selected_values: str
    destination_values: str


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return dict(getattr(value, "_mapping", value) or {})


def _required_text(value: Any, *, field_name: str, maximum: int) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized.encode("utf-8")) > maximum:
        raise ResultArchiveCandidateInitializationError(f"archive candidate initialization {field_name} is invalid")
    return normalized


def _required_snapshot_id(value: Any, *, field_name: str) -> str:
    return _required_text(value, field_name=field_name, maximum=96)


def _required_source_key(value: Any) -> str:
    return _required_text(value, field_name="source key", maximum=96).lower()


def _required_coverage_scope(value: Any) -> bytes:
    scope_id = bytes(value or b"")
    if len(scope_id) != 32:
        raise ResultArchiveCandidateInitializationError("archive candidate initialization coverage scope is invalid")
    return scope_id


def _require_transaction(session: Any) -> None:
    in_transaction = getattr(session, "in_transaction", None)
    if not callable(in_transaction) or not in_transaction():
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization requires an already-open caller transaction"
        )


def _required_schema(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not _IDENTIFIER_RE.fullmatch(normalized):
        raise ValueError(f"{field_name} must be a simple PostgreSQL identifier")
    return normalized


async def _rows(session: Any, statement: str, parameters: Mapping[str, Any]) -> list[dict[str, Any]]:
    result = await session.execute(text(statement), dict(parameters))
    return [_mapping(row) for row in result]


async def _one(session: Any, statement: str, parameters: Mapping[str, Any], label: str) -> dict[str, Any]:
    rows = await _rows(session, statement, parameters)
    if len(rows) != 1:
        raise ResultArchiveCandidateInitializationError(
            f"archive candidate initialization {label} is missing or ambiguous"
        )
    return rows[0]


def _source_binding_for_receipt(
    local_binding: Mapping[str, Any],
    source_file_import_id: str,
) -> dict[str, Any]:
    source_binding_by_name = dict(local_binding)
    source_binding_by_name["source_file_import_id"] = source_file_import_id
    return source_binding_by_name


def _manifest_coverage_scope(manifest: Mapping[str, Any]) -> bytes:
    serving_index = _mapping(manifest.get("serving_index"))
    raw_scope = str(serving_index.get("coverage_scope_id") or "").strip().lower()
    try:
        scope_id = bytes.fromhex(raw_scope)
    except ValueError as error:
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization source manifest coverage scope is invalid"
        ) from error
    return _required_coverage_scope(scope_id)


def _assert_plan_scope(
    staged: _AuthenticatedStagedCandidate,
    local_binding: Mapping[str, Any],
) -> None:
    plan_ids = tuple(str(value) for value in local_binding.get("plan_ids") or ())
    market_types = tuple(str(value).lower() for value in local_binding.get("plan_market_types") or ())
    observed_ids = tuple(sorted({plan_id for plan_id, _market_type in staged.plan_scopes}))
    observed_markets = tuple(sorted({market_type for _plan_id, market_type in staged.plan_scopes}))
    if (
        not staged.plan_scopes
        or (staged.primary_plan_id, staged.primary_plan_market_type) not in staged.plan_scopes
        or observed_ids != tuple(sorted(plan_ids))
        or observed_markets != tuple(sorted(market_types))
    ):
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization source plan scope differs from local admission"
        )


def _local_candidate_manifest(
    staged: _AuthenticatedStagedCandidate,
    *,
    destination_snapshot_id: str,
    local_binding: Mapping[str, Any],
    source_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    manifest = copy.deepcopy(dict(staged.source_manifest))
    for field_name in _REMOTE_AUTHORITY_FIELDS:
        manifest.pop(field_name, None)
    manifest.update(
        {
            "snapshot_id": destination_snapshot_id,
            "source_key": local_binding["source_key"],
            "import_month": local_binding["import_month"],
            "source_file_import_id": local_binding["source_file_import_id"],
            FROZEN_RATE_FILE_BINDING_OPTION: dict(local_binding),
            "activation": {
                "contract": PTG2_CANDIDATE_ACTIVATION_CONTRACT,
                "state": "building",
                "source_key": local_binding["source_key"],
                "plan_id": staged.primary_plan_id,
                "plan_market_type": staged.primary_plan_market_type,
                "expected_previous_snapshot_id": None,
            },
            "result_archive_source": {
                "contract": RESULT_ARCHIVE_CANDIDATE_INITIALIZATION_CONTRACT,
                "snapshot_manifest_sha256": source_receipt["snapshot_manifest_sha256"],
                "frozen_binding_sha256": source_receipt["frozen_binding_sha256"],
            },
        }
    )
    return manifest


def _local_run_options(
    frozen_binding_params: Mapping[str, Any],
    local_binding: Mapping[str, Any],
    source_receipt: Mapping[str, Any],
) -> dict[str, Any]:
    options_by_name = {
        "source_file_import_id": local_binding["source_file_import_id"],
        "source_key": local_binding["source_key"],
        "plan_ids": list(local_binding["plan_ids"]),
        "plan_market_types": list(local_binding["plan_market_types"]),
        "frozen_rate_file_set_contract": local_binding["frozen_rate_file_set_contract"],
        "frozen_rate_files": copy.deepcopy(frozen_binding_params["frozen_rate_files"]),
        "frozen_rate_file_set_sha256": local_binding["frozen_rate_file_set_sha256"],
        "frozen_rate_file_count": local_binding["frozen_rate_file_count"],
        "snapshot_arch": _SNAPSHOT_ARCH,
        "storage_generation": _STORAGE_GENERATION,
        "auto_activate_candidates": False,
        FROZEN_RATE_FILE_BINDING_OPTION: dict(local_binding),
        "result_archive_source": {
            "contract": RESULT_ARCHIVE_CANDIDATE_INITIALIZATION_CONTRACT,
            "snapshot_manifest_sha256": source_receipt["snapshot_manifest_sha256"],
            "frozen_binding_sha256": source_receipt["frozen_binding_sha256"],
        },
    }
    if INVALID_PRICE_EXCLUSION_POLICY_FIELD in local_binding:
        options_by_name[INVALID_PRICE_EXCLUSION_POLICY_FIELD] = copy.deepcopy(
            local_binding[INVALID_PRICE_EXCLUSION_POLICY_FIELD]
        )
    return options_by_name


async def _lock_staging_source_family(session: Any, staging_schema: str) -> None:
    staging = _quote_ident(staging_schema)
    for table_name in (*_SOURCE_GRAPH_TABLES, "ptg2_v3_snapshot_source"):
        await session.execute(text(f"LOCK TABLE {staging}.{_quote_ident(table_name)} IN SHARE MODE"))


async def _locked_staged_candidate_row(
    session: Any,
    *,
    staging_schema: str,
    source_snapshot_key: int,
) -> dict[str, Any]:
    """Lock the staged logical snapshot, scope, and frozen input binding."""

    staging = _quote_ident(staging_schema)
    return await _one(
        session,
        f"""
        SELECT snapshot.snapshot_id, snapshot.import_run_id, snapshot.status,
               snapshot.manifest, scope.plan_id, scope.plan_market_type,
               scope.coverage_scope_id, frozen.source_file_import_id,
               frozen.internal_run_id, frozen.binding_sha256,
               frozen.binding_payload
          FROM {staging}.ptg2_v3_snapshot_binding AS binding
          JOIN {staging}.ptg2_snapshot AS snapshot
            ON snapshot.snapshot_id = binding.snapshot_id
          JOIN {staging}.ptg2_v3_snapshot_scope AS scope
            ON scope.snapshot_id = snapshot.snapshot_id
          JOIN {staging}.ptg2_frozen_source_file_binding AS frozen
            ON frozen.internal_run_id = snapshot.import_run_id
         WHERE binding.snapshot_key = :source_snapshot_key
         FOR SHARE OF binding, snapshot, scope, frozen
        """,
        {"source_snapshot_key": source_snapshot_key},
        "staged source snapshot",
    )


async def _staged_plan_scopes(
    session: Any,
    *,
    staging_schema: str,
    source_snapshot_id: str,
) -> tuple[tuple[str, str], ...]:
    """Lock and normalize every plan scope owned by the staged snapshot."""

    plan_scope_rows = await _rows(
        session,
        f"""
        SELECT plan_id, plan_market_type
          FROM {_quote_ident(staging_schema)}.ptg2_v3_snapshot_plan_scope
         WHERE snapshot_id = :snapshot_id
         ORDER BY plan_id, plan_market_type
         FOR SHARE
        """,
        {"snapshot_id": source_snapshot_id},
    )
    return tuple(
        (
            _required_text(plan_scope_record.get("plan_id"), field_name="plan id", maximum=64),
            _required_text(
                plan_scope_record.get("plan_market_type"), field_name="plan market type", maximum=32
            ).lower(),
        )
        for plan_scope_record in plan_scope_rows
    )


def _validated_staged_identity(
    staged: Mapping[str, Any],
    *,
    local_binding: Mapping[str, Any],
    source_receipt: Mapping[str, Any],
) -> tuple[str, Mapping[str, Any], bytes]:
    """Validate staged identity, binding, manifest digest, and coverage scope."""

    source_snapshot_id = _required_snapshot_id(staged.get("snapshot_id"), field_name="source snapshot")
    source_file_import_id = _required_text(
        source_receipt.get("source_file_import_id"), field_name="source file identity", maximum=64
    )
    source_binding = _source_binding_for_receipt(local_binding, source_file_import_id)
    source_manifest = _mapping(staged.get("manifest"))
    stored_binding = _mapping(staged.get("binding_payload"))
    if (
        source_snapshot_id != source_receipt.get("snapshot_id")
        or str(staged.get("status") or "").lower() not in {"validated", "published"}
        or staged.get("import_run_id") != frozen_internal_run_id(source_file_import_id)
        or staged.get("internal_run_id") != staged.get("import_run_id")
        or staged.get("source_file_import_id") != source_file_import_id
        or _required_source_key(source_receipt.get("source_key")) != local_binding["source_key"]
        or stored_binding != source_binding
        or staged.get("binding_sha256") != frozen_rate_binding_sha256(stored_binding)
        or staged.get("binding_sha256") != source_receipt.get("frozen_binding_sha256")
        or _mapping(source_manifest.get(FROZEN_RATE_FILE_BINDING_OPTION)) != source_binding
        or result_archive_manifest_sha256(source_manifest) != source_receipt.get("snapshot_manifest_sha256")
    ):
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization restored source authority does not match"
        )
    coverage_scope_id = _required_coverage_scope(staged.get("coverage_scope_id"))
    if coverage_scope_id != _manifest_coverage_scope(source_manifest):
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization source coverage scope differs from its manifest"
        )
    return source_snapshot_id, source_manifest, coverage_scope_id


async def _authenticated_staged_candidate(
    session: Any,
    *,
    staging_schema: str,
    source_snapshot_key: int,
    local_binding: Mapping[str, Any],
    source_receipt: Mapping[str, Any],
) -> _AuthenticatedStagedCandidate:
    """Authenticate the immutable staged candidate against local admission."""

    staged = await _locked_staged_candidate_row(
        session,
        staging_schema=staging_schema,
        source_snapshot_key=source_snapshot_key,
    )
    source_snapshot_id, source_manifest, coverage_scope_id = _validated_staged_identity(
        staged,
        local_binding=local_binding,
        source_receipt=source_receipt,
    )
    plan_scopes = await _staged_plan_scopes(
        session,
        staging_schema=staging_schema,
        source_snapshot_id=source_snapshot_id,
    )
    await _lock_staging_source_family(session, staging_schema)
    source_records = tuple(
        await _rows(
            session,
            CANDIDATE_SOURCE_RECORDS_SQL.format(schema=_quote_ident(staging_schema)),
            {"snapshot_id": source_snapshot_id},
        )
    )
    authenticated = _AuthenticatedStagedCandidate(
        source_snapshot_id=source_snapshot_id,
        source_manifest=source_manifest,
        primary_plan_id=_required_text(staged.get("plan_id"), field_name="primary plan id", maximum=64),
        primary_plan_market_type=_required_text(
            staged.get("plan_market_type"), field_name="primary plan market type", maximum=32
        ).lower(),
        coverage_scope_id=coverage_scope_id,
        plan_scopes=plan_scopes,
        source_records=source_records,
    )
    _assert_plan_scope(authenticated, local_binding)
    return authenticated


def _selected_relation_comparison_sql(
    sql: _SelectedRelationSql,
) -> str:
    """Build the exact selected-column comparison for one copied relation."""

    return f"""
        WITH selected AS (
            SELECT {sql.source_column_list}
              FROM {sql.staging}.{sql.table} AS source
             WHERE {sql.predicate}
        )
        SELECT (SELECT COUNT(*) FROM selected)::integer AS selected_count,
               EXISTS (
                   SELECT 1
                     FROM selected
                    LEFT JOIN {sql.destination}.{sql.table} AS destination
                       ON {sql.key_match}
                    WHERE {sql.missing_key}
                       OR jsonb_build_array({sql.selected_values})
                          IS DISTINCT FROM jsonb_build_array({sql.destination_values})
               ) AS differs
    """


def _selected_relation_sql_parts(
    *,
    destination_schema: str,
    staging_schema: str,
    table_name: str,
    columns: Sequence[str],
    predicate: str,
    portable_expressions_by_column: Mapping[str, str] | None,
) -> _SelectedRelationSql:
    """Resolve quoted SQL fragments for one portable relation copy."""

    expressions_by_column = dict(portable_expressions_by_column or {})
    column_list = ", ".join(_quote_ident(column) for column in columns)
    source_column_list = ", ".join(
        (
            f"{expressions_by_column[column].format(alias='source')} AS {_quote_ident(column)}"
            if column in expressions_by_column
            else f"source.{_quote_ident(column)}"
        )
        for column in columns
    )
    destination_values = ", ".join(
        (
            f"to_jsonb({expressions_by_column[column].format(alias='destination')})"
            if column in expressions_by_column
            else f"to_jsonb(destination.{_quote_ident(column)})"
        )
        for column in columns
    )
    quoted_key = _quote_ident(columns[0])
    return _SelectedRelationSql(
        destination=_quote_ident(destination_schema),
        staging=_quote_ident(staging_schema),
        table=_quote_ident(table_name),
        column_list=column_list,
        source_column_list=source_column_list,
        predicate=predicate,
        key_match=f"destination.{quoted_key} = selected.{quoted_key}",
        missing_key=f"destination.{quoted_key} IS NULL",
        selected_values=", ".join(f"to_jsonb(selected.{_quote_ident(column)})" for column in columns),
        destination_values=destination_values,
    )


async def _copy_selected_relation(
    session: Any,
    *,
    destination_schema: str,
    staging_schema: str,
    table_name: str,
    columns: Sequence[str],
    predicate: str,
    parameters: Mapping[str, Any],
    portable_expressions_by_column: Mapping[str, str] | None = None,
) -> int:
    """Copy one selected immutable relation and reject non-exact reuse."""

    relation_sql = _selected_relation_sql_parts(
        destination_schema=destination_schema,
        staging_schema=staging_schema,
        table_name=table_name,
        columns=columns,
        predicate=predicate,
        portable_expressions_by_column=portable_expressions_by_column,
    )
    await session.execute(
        text(
            f"""
            INSERT INTO {relation_sql.destination}.{relation_sql.table} ({relation_sql.column_list})
            SELECT {relation_sql.source_column_list}
              FROM {relation_sql.staging}.{relation_sql.table} AS source
             WHERE {predicate}
            ON CONFLICT DO NOTHING
            """
        ),
        dict(parameters),
    )
    comparison = await _one(
        session,
        _selected_relation_comparison_sql(relation_sql),
        parameters,
        f"{table_name} comparison",
    )
    if bool(comparison["differs"]):
        raise ResultArchiveCandidateInitializationError(
            f"archive candidate initialization {table_name} conflicts with destination state"
        )
    return int(comparison["selected_count"])


async def _copy_source_graph(
    session: Any,
    *,
    destination_schema: str,
    staging_schema: str,
    source_snapshot_id: str,
    destination_snapshot_id: str,
) -> int:
    """Copy the selected source graph and bind it to the local snapshot."""

    staging = _quote_ident(staging_schema)
    source_parameters_by_name = {"source_snapshot_id": source_snapshot_id}
    trace_sets = (
        f"SELECT source_trace_set_hash FROM {staging}.ptg2_v3_snapshot_source WHERE snapshot_id = :source_snapshot_id"
    )
    traces = (
        f"SELECT unnest(source_trace_hashes) FROM {staging}.ptg2_source_trace_set "
        f"WHERE source_trace_set_hash IN ({trace_sets})"
    )
    versions = f"SELECT source_file_version_id FROM {staging}.ptg2_source_trace WHERE source_trace_hash IN ({traces})"
    predicates_by_table = {
        "ptg2_source_identity": (
            f"source_identity_hash IN (SELECT source_identity_hash FROM {staging}.ptg2_source_file_version "
            f"WHERE source_file_version_id IN ({versions}))"
        ),
        "ptg2_content_identity": (
            f"content_hash IN (SELECT content_hash FROM {staging}.ptg2_source_file_version "
            f"WHERE source_file_version_id IN ({versions}))"
        ),
        "ptg2_source_file_version": f"source_file_version_id IN ({versions})",
        "ptg2_source_trace": f"source_trace_hash IN ({traces})",
        "ptg2_source_trace_set": f"source_trace_set_hash IN ({trace_sets})",
    }
    for table_name in _SOURCE_GRAPH_TABLES:
        columns = _SOURCE_GRAPH_COLUMNS_BY_TABLE[table_name]
        await _copy_selected_relation(
            session,
            destination_schema=destination_schema,
            staging_schema=staging_schema,
            table_name=table_name,
            columns=columns,
            predicate=predicates_by_table[table_name],
            parameters=source_parameters_by_name,
            portable_expressions_by_column=_SOURCE_GRAPH_PORTABLE_EXPRESSIONS_BY_TABLE.get(table_name),
        )

    return await _copy_snapshot_sources(
        session,
        destination_schema=destination_schema,
        staging_schema=staging_schema,
        source_snapshot_id=source_snapshot_id,
        destination_snapshot_id=destination_snapshot_id,
    )


async def _copy_snapshot_sources(
    session: Any,
    *,
    destination_schema: str,
    staging_schema: str,
    source_snapshot_id: str,
    destination_snapshot_id: str,
) -> int:
    """Copy and exactly verify snapshot-scoped source assignments."""

    staging = _quote_ident(staging_schema)
    await _rows(
        session,
        f"""
        INSERT INTO {_quote_ident(destination_schema)}.ptg2_v3_snapshot_source
            (snapshot_id, source_key, source_type, identity_kind, identity_sha256,
             raw_container_sha256, logical_json_sha256, logical_hash_deferred,
             source_trace_set_hash)
        SELECT :destination_snapshot_id, source_key, source_type, identity_kind,
               identity_sha256, raw_container_sha256, logical_json_sha256,
               logical_hash_deferred, source_trace_set_hash
          FROM {staging}.ptg2_v3_snapshot_source
         WHERE snapshot_id = :source_snapshot_id
        ON CONFLICT DO NOTHING
        RETURNING source_key
        """,
        {
            "source_snapshot_id": source_snapshot_id,
            "destination_snapshot_id": destination_snapshot_id,
        },
    )
    expected_source_rows = await _rows(
        session,
        f"""
        SELECT source_key, source_type, identity_kind, identity_sha256,
               raw_container_sha256, logical_json_sha256, logical_hash_deferred,
               source_trace_set_hash
          FROM {staging}.ptg2_v3_snapshot_source
         WHERE snapshot_id = :source_snapshot_id
        ORDER BY source_key
        """,
        {"source_snapshot_id": source_snapshot_id},
    )
    observed_source_rows = await _rows(
        session,
        f"""
        SELECT source_key, source_type, identity_kind, identity_sha256,
               raw_container_sha256, logical_json_sha256, logical_hash_deferred,
               source_trace_set_hash
          FROM {_quote_ident(destination_schema)}.ptg2_v3_snapshot_source
         WHERE snapshot_id = :destination_snapshot_id
        ORDER BY source_key
        """,
        {"destination_snapshot_id": destination_snapshot_id},
    )
    if not expected_source_rows or observed_source_rows != expected_source_rows:
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization snapshot source records conflict with destination state"
        )
    return len(observed_source_rows)


async def _is_new_run_after_insert(
    session: Any,
    *,
    schema_name: str,
    import_run_id: str,
    import_month: str,
    options: Mapping[str, Any],
) -> bool:
    schema = _quote_ident(schema_name)
    inserted = await _rows(
        session,
        f"""
        INSERT INTO {schema}.ptg2_import_run
            (import_run_id, import_month, status, started_at, heartbeat_at,
             options, report, finished_at, error)
        VALUES (:import_run_id, CAST(:import_month AS date), 'running',
                transaction_timestamp(), transaction_timestamp(),
                CAST(:options AS jsonb), '{{}}'::jsonb, NULL, NULL)
        ON CONFLICT (import_run_id) DO NOTHING
        RETURNING import_run_id
        """,
        {
            "import_run_id": import_run_id,
            "import_month": dt.date.fromisoformat(import_month),
            "options": canonical_json_dumps(dict(options)),
        },
    )
    run = await _one(
        session,
        f"""
        SELECT import_run_id, import_month, status, started_at, finished_at,
               heartbeat_at, options, report, error
          FROM {schema}.ptg2_import_run
         WHERE import_run_id = :import_run_id
         FOR UPDATE
        """,
        {"import_run_id": import_run_id},
        "local import run",
    )
    if (
        run.get("import_run_id") != import_run_id
        or str(run.get("import_month")) != import_month
        or run.get("status") != "running"
        or run.get("started_at") is None
        or run.get("heartbeat_at") is None
        or run.get("finished_at") is not None
        or _mapping(run.get("options")) != dict(options)
        or _mapping(run.get("report")) != {}
        or run.get("error") is not None
    ):
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization local import run conflicts with retry"
        )
    return bool(inserted)


async def _is_new_snapshot_after_insert(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    import_run_id: str,
    import_month: str,
    manifest: Mapping[str, Any],
) -> bool:
    schema = _quote_ident(schema_name)
    inserted = await _rows(
        session,
        f"""
        INSERT INTO {schema}.ptg2_snapshot
            (snapshot_id, import_run_id, import_month, status, created_at,
             validated_at, published_at, previous_snapshot_id, manifest)
        VALUES (:snapshot_id, :import_run_id, CAST(:import_month AS date),
                'building', transaction_timestamp(), NULL, NULL, NULL,
                CAST(:manifest AS jsonb))
        ON CONFLICT (snapshot_id) DO NOTHING
        RETURNING snapshot_id
        """,
        {
            "snapshot_id": snapshot_id,
            "import_run_id": import_run_id,
            "import_month": dt.date.fromisoformat(import_month),
            "manifest": canonical_json_dumps(dict(manifest)),
        },
    )
    snapshot = await _one(
        session,
        f"""
        SELECT snapshot_id, import_run_id, import_month, status, created_at,
               validated_at, published_at, previous_snapshot_id, manifest
          FROM {schema}.ptg2_snapshot
         WHERE snapshot_id = :snapshot_id
         FOR UPDATE
        """,
        {"snapshot_id": snapshot_id},
        "local snapshot",
    )
    if (
        snapshot.get("snapshot_id") != snapshot_id
        or snapshot.get("import_run_id") != import_run_id
        or str(snapshot.get("import_month")) != import_month
        or snapshot.get("status") != "building"
        or snapshot.get("created_at") is None
        or snapshot.get("validated_at") is not None
        or snapshot.get("published_at") is not None
        or snapshot.get("previous_snapshot_id") is not None
        or _mapping(snapshot.get("manifest")) != dict(manifest)
    ):
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization local snapshot conflicts with retry"
        )
    return bool(inserted)


async def _insert_or_verify_scope(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    staged: _AuthenticatedStagedCandidate,
) -> None:
    """Insert or exactly verify the local primary and expanded plan scope."""

    schema = _quote_ident(schema_name)
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_scope
                (snapshot_id, plan_id, plan_market_type, coverage_scope_id)
            VALUES (:snapshot_id, :plan_id, :plan_market_type, :coverage_scope_id)
            ON CONFLICT (snapshot_id) DO NOTHING
            """
        ),
        {
            "snapshot_id": snapshot_id,
            "plan_id": staged.primary_plan_id,
            "plan_market_type": staged.primary_plan_market_type,
            "coverage_scope_id": staged.coverage_scope_id,
        },
    )
    scope = await _one(
        session,
        f"""
        SELECT plan_id, plan_market_type, coverage_scope_id
          FROM {schema}.ptg2_v3_snapshot_scope
         WHERE snapshot_id = :snapshot_id
         FOR UPDATE
        """,
        {"snapshot_id": snapshot_id},
        "local snapshot scope",
    )
    if (
        scope.get("plan_id") != staged.primary_plan_id
        or str(scope.get("plan_market_type") or "").lower() != staged.primary_plan_market_type
        or bytes(scope.get("coverage_scope_id") or b"") != staged.coverage_scope_id
    ):
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization local snapshot scope conflicts with retry"
        )
    await _insert_or_verify_plan_scopes(
        session,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        plan_scopes=staged.plan_scopes,
    )


async def _insert_or_verify_plan_scopes(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    plan_scopes: tuple[tuple[str, str], ...],
) -> None:
    """Insert or exactly verify every locally admitted plan scope."""

    schema = _quote_ident(schema_name)
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_plan_scope
                (snapshot_id, plan_id, plan_market_type)
            VALUES (:snapshot_id, :plan_id, :plan_market_type)
            ON CONFLICT DO NOTHING
            """
        ),
        [
            {"snapshot_id": snapshot_id, "plan_id": plan_id, "plan_market_type": market_type}
            for plan_id, market_type in plan_scopes
        ],
    )
    observed_plan_scope_rows = await _rows(
        session,
        f"""
        SELECT plan_id, plan_market_type
          FROM {schema}.ptg2_v3_snapshot_plan_scope
         WHERE snapshot_id = :snapshot_id
         ORDER BY plan_id, plan_market_type
         FOR UPDATE
        """,
        {"snapshot_id": snapshot_id},
    )
    observed_plan_scopes = tuple(
        (plan_scope_record["plan_id"], plan_scope_record["plan_market_type"])
        for plan_scope_record in observed_plan_scope_rows
    )
    if observed_plan_scopes != plan_scopes:
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization local plan scope conflicts with retry"
        )


async def _assert_local_attempt_isolated(
    session: Any,
    *,
    schema_name: str,
    snapshot_id: str,
    import_run_id: str,
) -> None:
    schema = _quote_ident(schema_name)
    reverse = await _rows(
        session,
        f"""
        SELECT snapshot_id
          FROM {schema}.ptg2_snapshot
         WHERE import_run_id = :import_run_id
         ORDER BY snapshot_id
         FOR UPDATE
        """,
        {"import_run_id": import_run_id},
    )
    if [snapshot_record["snapshot_id"] for snapshot_record in reverse] != [snapshot_id]:
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization local run is attached to unrelated state"
        )
    attestations = await _rows(
        session,
        f"""
        SELECT snapshot_id
          FROM {schema}.ptg2_v3_candidate_audit_attestation
         WHERE snapshot_id = :snapshot_id
         FOR UPDATE
        """,
        {"snapshot_id": snapshot_id},
    )
    if attestations:
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization refuses a pre-attested candidate"
        )


def _validated_local_admission(
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_id: str,
    frozen_binding_params: Mapping[str, Any],
    authenticated_source_archive_metadata: Mapping[str, Any],
) -> _LocalCandidateAdmission:
    """Normalize caller-admitted local identity and authenticated source proof."""

    destination_schema = _required_schema(schema_name, field_name="schema_name")
    staging_schema = _required_schema(staging_schema_name, field_name="staging_schema_name")
    if destination_schema == staging_schema:
        raise ValueError("staging_schema_name must differ from schema_name")
    if destination_schema != resolve_ptg2_schema():
        raise ValueError("schema_name must match the configured PTG schema")
    if isinstance(source_snapshot_key, bool) or int(source_snapshot_key) < 0:
        raise ValueError("source_snapshot_key must be non-negative")
    destination_snapshot = _required_snapshot_id(destination_snapshot_id, field_name="destination snapshot")
    try:
        local_binding = frozen_rate_binding_from_params(frozen_binding_params)
    except ValueError as error:
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization local frozen input is invalid"
        ) from error
    if local_binding is None:
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization requires frozen source-file evidence"
        )
    source_receipt = validate_ptg_result_archive_source_authority(authenticated_source_archive_metadata)
    if (
        destination_snapshot == source_receipt["snapshot_id"]
        or local_binding["source_file_import_id"] == source_receipt["source_file_import_id"]
    ):
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization requires new local attempt identities"
        )
    return _LocalCandidateAdmission(
        destination_schema=destination_schema,
        staging_schema=staging_schema,
        source_snapshot_key=int(source_snapshot_key),
        destination_snapshot_id=destination_snapshot,
        import_run_id=frozen_internal_run_id(str(local_binding["source_file_import_id"])),
        binding=local_binding,
        source_receipt=source_receipt,
    )


async def _persist_local_candidate(
    session: Any,
    *,
    admission: _LocalCandidateAdmission,
    staged: _AuthenticatedStagedCandidate,
    manifest: Mapping[str, Any],
    frozen_binding_params: Mapping[str, Any],
) -> tuple[int, bool]:
    """Persist or exactly replay the destination-owned building attempt."""

    import_month = str(admission.binding["import_month"])
    is_new_run = await _is_new_run_after_insert(
        session,
        schema_name=admission.destination_schema,
        import_run_id=admission.import_run_id,
        import_month=import_month,
        options=_local_run_options(frozen_binding_params, admission.binding, admission.source_receipt),
    )
    is_new_snapshot = await _is_new_snapshot_after_insert(
        session,
        schema_name=admission.destination_schema,
        snapshot_id=admission.destination_snapshot_id,
        import_run_id=admission.import_run_id,
        import_month=import_month,
        manifest=manifest,
    )
    await _insert_or_verify_scope(
        session,
        schema_name=admission.destination_schema,
        snapshot_id=admission.destination_snapshot_id,
        staged=staged,
    )
    source_count = await _copy_source_graph(
        session,
        destination_schema=admission.destination_schema,
        staging_schema=admission.staging_schema,
        source_snapshot_id=staged.source_snapshot_id,
        destination_snapshot_id=admission.destination_snapshot_id,
    )
    async with db.bind_existing_session(session):
        stored_binding = await insert_or_compare_frozen_binding(db, frozen_binding_params)
    if stored_binding != admission.binding:
        raise ResultArchiveCandidateInitializationError("archive candidate initialization local frozen binding changed")
    await _assert_local_attempt_isolated(
        session,
        schema_name=admission.destination_schema,
        snapshot_id=admission.destination_snapshot_id,
        import_run_id=admission.import_run_id,
    )
    if is_new_run != is_new_snapshot:
        raise ResultArchiveCandidateInitializationError(
            "archive candidate initialization retry has incomplete attempt state"
        )
    return source_count, is_new_snapshot


def _initialized_candidate_result(
    admission: _LocalCandidateAdmission,
    staged: _AuthenticatedStagedCandidate,
    *,
    source_count: int,
    is_new_snapshot: bool,
) -> InitializedResultArchiveCandidate:
    """Return the explicit non-authoritative local initialization receipt."""

    return InitializedResultArchiveCandidate(
        contract=RESULT_ARCHIVE_CANDIDATE_INITIALIZATION_CONTRACT,
        destination_snapshot_id=admission.destination_snapshot_id,
        destination_import_run_id=admission.import_run_id,
        source_snapshot_id=staged.source_snapshot_id,
        source_count=source_count,
        plan_scope_count=len(staged.plan_scopes),
        frozen_binding_sha256=frozen_rate_binding_sha256(admission.binding),
        reused=not is_new_snapshot,
    )


async def _lock_local_candidate(session: Any, snapshot_id: str) -> None:
    """Serialize initialization retries for one destination snapshot."""

    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
        {"lock_key": f"{RESULT_ARCHIVE_CANDIDATE_INITIALIZATION_CONTRACT}:{snapshot_id}"},
    )


async def initialize_result_archive_candidate(
    session: Any,
    *,
    schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_id: str,
    frozen_binding_params: Mapping[str, Any],
    authenticated_source_archive_metadata: Mapping[str, Any],
) -> InitializedResultArchiveCandidate:
    """Create or exactly reuse one destination-local building candidate.

    The caller owns this bounded transaction and its staging-schema admission.
    A successful result is only input to result-archive evidence/layout
    preparation.  It grants no registration, validation, attestation, pointer,
    or activation authority.
    """

    _require_transaction(session)
    admission = _validated_local_admission(
        schema_name=schema_name,
        staging_schema_name=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_id=destination_snapshot_id,
        frozen_binding_params=frozen_binding_params,
        authenticated_source_archive_metadata=authenticated_source_archive_metadata,
    )
    await _lock_local_candidate(session, admission.destination_snapshot_id)
    staged = await _authenticated_staged_candidate(
        session,
        staging_schema=admission.staging_schema,
        source_snapshot_key=admission.source_snapshot_key,
        local_binding=admission.binding,
        source_receipt=admission.source_receipt,
    )
    local_manifest_by_name = _local_candidate_manifest(
        staged,
        destination_snapshot_id=admission.destination_snapshot_id,
        local_binding=admission.binding,
        source_receipt=admission.source_receipt,
    )
    validate_frozen_candidate_evidence(
        local_manifest_by_name,
        candidate_run_id=admission.import_run_id,
        database_binding=admission.binding,
        database_sources=staged.source_records,
    )
    source_count, is_new_snapshot = await _persist_local_candidate(
        session,
        admission=admission,
        staged=staged,
        manifest=local_manifest_by_name,
        frozen_binding_params=frozen_binding_params,
    )
    return _initialized_candidate_result(
        admission,
        staged,
        source_count=source_count,
        is_new_snapshot=is_new_snapshot,
    )


__all__ = [
    "InitializedResultArchiveCandidate",
    "RESULT_ARCHIVE_CANDIDATE_INITIALIZATION_CONTRACT",
    "ResultArchiveCandidateInitializationError",
    "initialize_result_archive_candidate",
]
