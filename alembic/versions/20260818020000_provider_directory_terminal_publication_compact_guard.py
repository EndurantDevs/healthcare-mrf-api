# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bound terminal publication validation to sealed compact metadata.

Revision ID: 20260818020000_provider_directory_terminal_publication_compact_guard
Revises: 20260816020000_address_evidence_alias
"""

from __future__ import annotations

from functools import lru_cache
import hashlib
import importlib.util
from pathlib import Path
import re
from types import ModuleType

from alembic import op


revision = "20260818020000_provider_directory_terminal_publication_compact_guard"
down_revision = "20260816020000_address_evidence_alias"
branch_labels = None
depends_on = None


_TERMINAL_FILE = "20260816010000_provider_directory_terminal_publication_guard.py"
_ADMISSION_FILE = (
    "20260812020000_provider_directory_endpoint_dataset_admission_seal.py"
)
_GENERIC_PROOF_SUMMARY_KEY = (
    "provider_directory_content_proof_admission_summary_v1"
)
_SUBSET_ADMISSION_SUMMARY_KEY = (
    "provider_directory_subset_admission_summary_v1"
)
_CONTENT_PROOF_KEY = "provider_directory_content_proof_v1"
_SOURCE_SUMMARY_KEY = "source_summary_v1"
_OUTCOME_COUNTS_KEY = "outcome_resource_counts_v1"
_SOURCE_SUMMARY_CONTRACT_ID = "healthporta.provider-directory.source-summary.v1"
_SOURCE_SUMMARY_SEMANTIC_CONTRACT_ID = (
    "healthporta.provider-directory.fhir-normalized-resource.v1"
)
_CONTENT_PROOF_CONTRACT_IDS = (
    "healthporta.provider-directory.content-proof.v1",
    "healthporta.provider-directory.content-proof.v2",
    "healthporta.provider-directory.content-proof.v3",
)
_DUPLICATE_REPLAY_ERROR = "provider_directory_subset_replay_evidence_invalid"


def _load_sibling(filename: str, module_name: str) -> ModuleType:
    path = Path(__file__).with_name(filename)
    module_spec = importlib.util.spec_from_file_location(module_name, path)
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("Provider Directory guard predecessor is unavailable")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


@lru_cache(maxsize=1)
def _terminal() -> ModuleType:
    return _load_sibling(_TERMINAL_FILE, "_pd_terminal_compact_predecessor")


@lru_cache(maxsize=1)
def _admission() -> ModuleType:
    return _load_sibling(_ADMISSION_FILE, "_pd_terminal_compact_admission")


def _replace_once(sql: str, old: str, new: str, label: str) -> str:
    if sql.count(old) != 1:
        raise RuntimeError(f"Provider Directory {label} predecessor changed")
    return sql.replace(old, new, 1)


def _function_body(create_sql: str) -> str:
    start_marker = "AS $function$"
    end_marker = "$function$;"
    if create_sql.count(start_marker) != 1 or create_sql.count(end_marker) != 1:
        raise RuntimeError("Provider Directory guard function renderer changed")
    return create_sql.split(start_marker, 1)[1].rsplit(end_marker, 1)[0]


def _normalized_body_md5(create_sql: str) -> str:
    normalized = re.sub(r"\s+", " ", _function_body(create_sql).strip())
    return hashlib.md5(
        normalized.encode("utf-8"),
        usedforsecurity=False,
    ).hexdigest()


def _duplicate_replay_block(guard_sql: str) -> str:
    if guard_sql.count(_DUPLICATE_REPLAY_ERROR) != 1:
        raise RuntimeError("Provider Directory replay guard predecessor changed")
    marker_position = guard_sql.index(_DUPLICATE_REPLAY_ERROR)
    start = guard_sql.rfind("\n        IF ", 0, marker_position)
    end = guard_sql.find("\n\n        IF ", marker_position)
    if start < 0 or end < 0:
        raise RuntimeError("Provider Directory replay guard predecessor changed")
    block = guard_sql[start + 1 : end]
    required_fragments = (
        "provider_directory_subset_replay_evidence_shape_valid",
        "provider_directory_subset_coverage_shape_valid",
        _DUPLICATE_REPLAY_ERROR,
    )
    if any(fragment not in block for fragment in required_fragments):
        raise RuntimeError("Provider Directory replay guard predecessor changed")
    return block


def _sealed_publication_metadata_sql(schema: str) -> str:
    terminal = _terminal()
    subset = terminal._subset()
    admission = _admission()
    summary = "NEW.publication_metadata_summary_json"
    generic = f"{summary} -> {subset._ql(_GENERIC_PROOF_SUMMARY_KEY)}"
    sealed_completion = (
        f"{summary} -> {subset._ql(_SUBSET_ADMISSION_SUMMARY_KEY)}"
        " -> 'completion_proof'"
    )
    receipt = "NEW.artifact_selection_receipt_json"
    receipt_proof = f"{receipt} -> {subset._ql(_CONTENT_PROOF_KEY)}"
    receipt_source = f"{receipt} -> {subset._ql(_SOURCE_SUMMARY_KEY)}"
    receipt_outcome = f"{receipt} -> {subset._ql(_OUTCOME_COUNTS_KEY)}"
    digest = subset._qf(schema, admission._DIGEST_FUNCTION)
    seal_unchanged = "ROW(" + ", ".join(
        f"NEW.{column_name}" for column_name in admission._SEAL_COLUMNS
    ) + ") IS NOT DISTINCT FROM ROW(" + ", ".join(
        f"OLD.{column_name}" for column_name in admission._SEAL_COLUMNS
    ) + ")"
    seal_absent = " AND\n             ".join(
        f"{row_name}.{column_name} IS NULL"
        for row_name in ("OLD", "NEW")
        for column_name in admission._SEAL_COLUMNS
    )
    policy = subset._REVIEWED_ROOT_POLICY_KEY
    policy_version = subset._REVIEWED_ROOT_POLICY_VERSION
    proof_contracts = ", ".join(
        subset._ql(contract_id) for contract_id in _CONTENT_PROOF_CONTRACT_IDS
    )
    return f"""
        CASE
            WHEN TG_OP = 'UPDATE'
             AND {seal_unchanged}
             AND NEW.content_proof_admission_version = 1
             AND NEW.content_proof_admission_kind = 'generic'
             AND NEW.content_proof_admission_sha256 ~ '^[0-9a-f]{{64}}$'
             AND NEW.publication_metadata_sha256 ~ '^[0-9a-f]{{64}}$'
             AND NEW.content_proof_resource_types IS NOT NULL
             AND pg_catalog.jsonb_typeof({summary}) = 'object'
             AND NEW.publication_metadata_sha256 = {digest}(
                    {summary},
                    NEW.content_proof_admission_version,
                    NEW.content_proof_admission_kind::text,
                    NEW.content_proof_admission_sha256,
                    NEW.content_proof_resource_types
                 )
             AND {summary} -> {subset._ql(policy)} =
                    pg_catalog.jsonb_build_object(
                        'policy_version', {subset._ql(policy_version)},
                        'required_root_count', 1
                    )
             AND pg_catalog.jsonb_typeof({generic}) = 'object'
             AND {generic} -> 'dataset_hash' =
                    pg_catalog.to_jsonb(NEW.dataset_hash)
             AND {generic} -> 'resource_count' =
                    pg_catalog.to_jsonb(NEW.resource_count)
             AND {generic} -> 'resource_hashes' =
                    NEW.completion_proof_json -> 'dataset' -> 'resource_hashes'
             AND {generic} -> 'resource_counts' =
                    NEW.completion_proof_json -> 'dataset' -> 'resource_counts'
             AND pg_catalog.jsonb_typeof({sealed_completion}) = 'object'
             AND {sealed_completion} ->> 'proof_sha256' =
                    NEW.completion_proof_sha256
             AND {sealed_completion} -> 'dataset' ->> 'hash' =
                    NEW.dataset_hash
             AND {sealed_completion} -> 'dataset' -> 'count' =
                    pg_catalog.to_jsonb(NEW.resource_count)
             AND {sealed_completion} -> 'dataset' -> 'resource_hashes' =
                    {generic} -> 'resource_hashes'
             AND {sealed_completion} -> 'dataset' -> 'resource_counts' =
                    {generic} -> 'resource_counts'
             AND pg_catalog.jsonb_typeof({receipt}) = 'object'
             AND pg_catalog.jsonb_typeof({receipt_proof}) = 'object'
             AND {receipt_proof} -> 'complete' = 'true'::jsonb
             AND {receipt_proof} ->> 'contract_id' IN ({proof_contracts})
             AND {receipt_proof} ->> 'proof_sha256' =
                    NEW.content_proof_admission_sha256
             AND {summary} @> ({receipt} - {subset._ql(_CONTENT_PROOF_KEY)})
             AND {receipt_source} -> 'complete' = 'true'::jsonb
             AND {receipt_source} ->> 'contract_id' =
                    {subset._ql(_SOURCE_SUMMARY_CONTRACT_ID)}
             AND {receipt_source} -> 'contract_version' = '1'::jsonb
             AND {receipt_source} ->> 'semantic_contract_id' =
                    {subset._ql(_SOURCE_SUMMARY_SEMANTIC_CONTRACT_ID)}
             AND {receipt_source} ->> 'dataset_id' = NEW.dataset_id
             AND {receipt_source} ->> 'endpoint_id' = NEW.endpoint_id
             AND {receipt_source} ->> 'acquisition_root_run_id' =
                    NEW.acquisition_root_run_id
             AND {receipt_source} -> 'source_ids' = {summary} -> 'source_ids'
             AND {receipt_source} -> 'selected_resources' =
                    {summary} -> 'selected_resources'
             AND {receipt_source} ->> 'dataset_hash' =
                    {generic} ->> 'dataset_hash'
             AND {receipt_source} -> 'total_resources' =
                    {generic} -> 'resource_count'
             AND {receipt_source} -> 'resource_hashes' =
                    {generic} -> 'resource_hashes'
             AND {receipt_source} -> 'resource_counts' =
                    {generic} -> 'resource_counts'
             AND {receipt_outcome} -> 'complete' = 'true'::jsonb
             AND {receipt_outcome} -> 'version' = '1'::jsonb
             AND {receipt_outcome} ->> 'dataset_id' = NEW.dataset_id
             AND {receipt_outcome} ->> 'endpoint_id' = NEW.endpoint_id
             AND {receipt_outcome} ->> 'acquisition_root_run_id' =
                    NEW.acquisition_root_run_id
             AND {receipt_outcome} ->> 'dataset_hash' = NEW.dataset_hash
             AND {receipt_outcome} -> 'source_ids' = {summary} -> 'source_ids'
             AND {receipt_outcome} -> 'selected_resources' =
                    {summary} -> 'selected_resources'
             AND {receipt_outcome} -> 'resource_count' =
                    {generic} -> 'resource_count'
             AND {receipt_outcome} -> 'resource_counts' =
                    {generic} -> 'resource_counts'
            THEN {summary}
            WHEN TG_OP = 'UPDATE'
             AND {seal_absent}
            THEN NEW.publication_metadata_json::jsonb
            ELSE NULL::jsonb
        END
    """


def _published_source_sql(schema: str) -> str:
    subset = _terminal()._subset()
    return subset._subset_source_sql(
        schema,
        require_verified=True,
        use_configured_endpoint_identity=True,
        require_physical_match=True,
        reviewed_root_policy_aware=True,
        reviewed_subset_profile_aware=True,
        reviewed_subset_terminal_window_profile_aware=True,
    )


def _endpoint_guard_sql(schema: str, *, compact: bool) -> str:
    terminal = _terminal()
    guard_sql = terminal._subset_guard_sql(schema, transition_only=True)
    if not compact:
        return guard_sql

    raw_source_sql = _published_source_sql(schema)
    compact_source_sql = raw_source_sql.replace(
        "NEW.publication_metadata_json::jsonb",
        "published_source_metadata",
    )
    if compact_source_sql == raw_source_sql:
        raise RuntimeError("Provider Directory source guard predecessor changed")
    guard_sql = _replace_once(
        guard_sql,
        _duplicate_replay_block(guard_sql),
        "",
        "replay guard",
    )
    guard_sql = _replace_once(
        guard_sql,
        "AS $function$\n    BEGIN",
        "AS $function$\n    DECLARE\n"
        "        published_source_metadata jsonb;\n"
        "    BEGIN",
        "endpoint guard declaration",
    )
    guard_sql = _replace_once(
        guard_sql,
        raw_source_sql,
        compact_source_sql,
        "published source guard",
    )
    source_lock = (
        "            LOCK TABLE "
        + terminal._subset()._qf(schema, terminal._subset()._SOURCE)
        + " IN SHARE MODE;"
    )
    guard_sql = _replace_once(
        guard_sql,
        source_lock,
        "            published_source_metadata := "
        + _sealed_publication_metadata_sql(schema).strip()
        + ";\n"
        + source_lock,
        "published source metadata assignment",
    )
    return guard_sql


def _function_body_fence_sql(schema: str, *, compact: bool) -> str:
    terminal = _terminal()
    subset = terminal._subset()
    admission = _admission()
    expected_rows = ",\n            ".join(
        "(" + ", ".join((subset._ql(function_name), subset._ql(expected_md5))) + ")"
        for function_name, expected_md5 in (
            (
                subset._ENDPOINT_DATASET_GUARD,
                _normalized_body_md5(
                    _endpoint_guard_sql(schema, compact=compact)
                ),
            ),
            (
                admission._REPLAY_GUARD_FUNCTION,
                _normalized_body_md5(
                    admission._replay_guard_function_sql(schema)
                ),
            ),
        )
    )
    return f"""
    DO $migration$
    DECLARE
        matched_functions bigint;
    BEGIN
        SELECT pg_catalog.count(*)
          INTO matched_functions
          FROM (VALUES
            {expected_rows}
          ) AS expected(function_name, normalized_body_md5)
          JOIN pg_catalog.pg_namespace AS function_namespace
            ON function_namespace.nspname = {subset._ql(schema)}
          JOIN pg_catalog.pg_proc AS function_row
            ON function_row.pronamespace = function_namespace.oid
           AND function_row.proname = expected.function_name
           AND function_row.pronargs = 0
           AND function_row.prorettype = 'pg_catalog.trigger'::regtype
           AND function_row.prosecdef IS TRUE
           AND function_row.proconfig IS NOT DISTINCT FROM
                   ARRAY['search_path=pg_catalog']::text[]
           AND NOT pg_catalog.has_function_privilege(
                   'public', function_row.oid, 'EXECUTE'
               )
           AND pg_catalog.md5(pg_catalog.btrim(pg_catalog.regexp_replace(
                   function_row.prosrc,
                   '[[:space:]]+', ' ', 'g'
               ))) = expected.normalized_body_md5;
        IF matched_functions <> 2 THEN
            RAISE EXCEPTION
                'provider_directory_terminal_publication_guard_body_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _shape_fences(schema: str, *, compact: bool) -> tuple[str, ...]:
    terminal = _terminal()
    subset = terminal._subset()
    admission = _admission()
    return (
        *terminal._relation_shape_fences(schema),
        subset._subset_column_shape_fence_sql(schema),
        admission._legacy_surface_fence_sql(schema, scoped=True),
        terminal._endpoint_guard_function_shape_fence_sql(schema),
        terminal._resource_guard_shape_fence_sql(schema),
        subset._source_guard_shape_fence_sql(schema, expect_installed=True),
        _function_body_fence_sql(schema, compact=compact),
    )


def _replace_guard(schema: str, *, compact: bool) -> None:
    terminal = _terminal()
    subset = terminal._subset()
    relations = (
        subset._ENDPOINT_DATASET,
        subset._DATASET_RESOURCE,
        subset._SOURCE,
    )
    op.execute(
        "LOCK TABLE "
        + ", ".join(subset._qf(schema, relation) for relation in relations)
        + " IN ACCESS EXCLUSIVE MODE;"
    )
    for statement in _shape_fences(schema, compact=not compact):
        op.execute(statement)
    op.execute(_endpoint_guard_sql(schema, compact=compact))
    op.execute(
        "REVOKE ALL ON FUNCTION "
        f"{subset._qf(schema, subset._ENDPOINT_DATASET_GUARD)}() FROM PUBLIC;"
    )
    for statement in _shape_fences(schema, compact=compact):
        op.execute(statement)


def upgrade() -> None:
    _replace_guard(_terminal()._subset()._schema(), compact=True)


def downgrade() -> None:
    _replace_guard(_terminal()._subset()._schema(), compact=False)
