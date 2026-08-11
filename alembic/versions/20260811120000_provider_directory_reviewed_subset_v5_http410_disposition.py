# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit one exact reviewed v5 HTTP-410 terminal disposition.

Revision ID: 20260811120000_provider_directory_reviewed_subset_v5_http410_disposition
Revises: 20260811110000_address_formatted_display
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = (
    "20260811120000_provider_directory_reviewed_subset_v5_http410_disposition"
)
down_revision = "20260811110000_address_formatted_display"
branch_labels = None
depends_on = None


_DIRECT_FILE = (
    "20260810110000_provider_directory_reviewed_subset_direct_v4_disposition.py"
)
_TERMINAL_WINDOW_FILE = (
    "20260810130000_provider_directory_reviewed_subset_terminal_window.py"
)
_CONTRACT = (
    "healthporta.provider-directory.reviewed-subset-terminal-disposition.v3"
)
_REASON = "reviewed_current_version_census_http_410"
_CAMPAIGN = "provider-directory-reviewed-subset-2026-08-10-v5"
_HELPER = "provider_directory_subset_terminal_disposition_v5_valid"
_IDENTITY_SNAPSHOT = "provider_directory_terminal_v5_identity_snapshot"
_MARKER_SHA256 = (
    "87f1c25625562037f9544b30a62e8b1bbf625018c73076bb083b8680225b23d9"
)
_HTTP410_RESOURCES = ("HealthcareService",)


@lru_cache(maxsize=1)
def _direct() -> ModuleType:
    path = Path(__file__).with_name(_DIRECT_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_v5_http410_direct",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("provider directory direct disposition unavailable")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


@lru_cache(maxsize=1)
def _terminal_window() -> ModuleType:
    path = Path(__file__).with_name(_TERMINAL_WINDOW_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_v5_http410_terminal_window",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("provider directory terminal-window revision unavailable")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _subset() -> ModuleType:
    return _direct()._subset()


def _abandonment() -> ModuleType:
    return _direct()._abandonment()


def _qf(schema: str, name: str) -> str:
    return _direct()._qf(schema, name)


def _ql(value: str) -> str:
    return _direct()._ql(value)


def _replace_exact(
    source: str,
    needle: str,
    replacement: str,
    *,
    expected_count: int = 1,
) -> str:
    if source.count(needle) != expected_count:
        raise RuntimeError("provider directory v5 HTTP-410 renderer changed")
    return source.replace(needle, replacement)


def _v5_valid_sql(schema: str) -> str:
    """Derive the private v3 validator from the installed v2 validator."""

    direct = _direct()
    old_http_resources = direct._resource_array(direct._DRIFT_RESOURCE_TYPES)
    new_http_resources = direct._resource_array(_HTTP410_RESOURCES)
    retained_delta = f"""               OR (
                    current_resource_type = ANY({old_http_resources})
                    AND (disposition ->> 'returned_unique')::numeric
                          - relation_resource_count
                        IS DISTINCT FROM
                          (disposition ->> 'terminal_page_entry_count')::numeric
               )"""
    null_terminal_counts = f"""               OR (
                    current_resource_type = ANY({new_http_resources})
                    AND (
                        disposition -> 'advertised_post'
                            IS DISTINCT FROM 'null'::jsonb
                        OR disposition -> 'returned_unique'
                            IS DISTINCT FROM 'null'::jsonb
                        OR disposition -> 'deficit'
                            IS DISTINCT FROM 'null'::jsonb
                    )
               )"""
    sql = _replace_exact(
        direct._direct_valid_sql(schema),
        retained_delta,
        null_terminal_counts,
    )
    replacements = (
        (direct._DIRECT_VALID, _HELPER, 1),
        (direct._CONTRACT, _CONTRACT, 1),
        (direct._REASON, _REASON, 1),
        (direct._CAMPAIGN, _CAMPAIGN, 2),
        (direct._MARKER_SHA256, _MARKER_SHA256, 1),
        (old_http_resources, new_http_resources, 2),
        ("terminal_census_drift", "terminal_http_410", 1),
        (
            "disposition -> 'page_delta' <> '1'::jsonb",
            "disposition -> 'page_delta' <> '0'::jsonb",
            1,
        ),
    )
    for needle, replacement, count in replacements:
        sql = _replace_exact(sql, needle, replacement, expected_count=count)
    return sql


def _v5_transition_sql(schema: str) -> str:
    """Bind the one-time v3 transition to the exact live source packet."""

    direct = _direct()
    source_metadata = "source.metadata_json::jsonb"
    old_source_identity = direct._subset()._subset_source_fixed_identity_sql(
        source_metadata,
        "terminal_profile",
        reviewed_subset_profile_aware=True,
    )
    v5_source_identity = direct._subset()._subset_source_fixed_identity_sql(
        source_metadata,
        "terminal_profile",
        reviewed_subset_profile_aware=True,
        reviewed_subset_terminal_window_profile_aware=True,
    )
    replacements = (
        (direct._CONTRACT, _CONTRACT, 1),
        (direct._CAMPAIGN, _CAMPAIGN, 2),
        (direct._MARKER_SHA256, _MARKER_SHA256, 1),
        (old_source_identity, v5_source_identity, 1),
        (
            "provider_directory_subset_terminal_v4_transition_invalid",
            "provider_directory_subset_terminal_v5_http410_transition_invalid",
            1,
        ),
    )
    sql = direct._direct_transition_sql(schema)
    for needle, replacement, count in replacements:
        sql = _replace_exact(sql, needle, replacement, expected_count=count)
    return sql


def _shared_valid_sql(schema: str) -> str:
    """Dispatch v3 before the unchanged v2 and v1 validators."""

    direct = _direct()
    marker = direct._MARKER
    needle = f"        marker := candidate_metadata -> '{marker}';"
    replacement = needle + f"""
        IF marker ->> 'contract_version' = '{_CONTRACT}' THEN
            RETURN {_qf(schema, _HELPER)}(candidate_dataset_id);
        END IF;"""
    return _replace_exact(direct._shared_valid_sql(schema), needle, replacement)


def _dataset_guard_sql(schema: str) -> str:
    """Add the exact v3 transition before unchanged v2 and v1 branches."""

    direct = _direct()
    needle = f"""        IF NEW.publication_metadata_json::jsonb #>> ARRAY[
             '{direct._MARKER}', 'contract_version'
           ]::text[] = '{direct._CONTRACT}' THEN"""
    return _replace_exact(
        direct._dataset_guard_sql(schema),
        needle,
        _v5_transition_sql(schema) + needle,
    )


def _lock_relations(schema: str) -> None:
    subset = _subset()
    abandonment = _abandonment()
    relation_names = (
        subset._ENDPOINT_DATASET,
        subset._DATASET_RESOURCE,
        subset._SOURCE,
        abandonment._PROOF_SHARD,
        abandonment._CHECKPOINT,
        abandonment._BULK_CHECKPOINT,
        "import_run",
    )
    relation_list = ", ".join(
        _qf(schema, relation_name) for relation_name in relation_names
    )
    op.execute(
        f"""
        DO $migration$
        DECLARE
            attempt integer;
        BEGIN
            FOR attempt IN 1..150 LOOP
                BEGIN
                    LOCK TABLE {relation_list}
                        IN ACCESS EXCLUSIVE MODE NOWAIT;
                    RETURN;
                EXCEPTION WHEN lock_not_available THEN
                    IF attempt = 150 THEN
                        RAISE EXCEPTION
                            'provider_directory_v5_http410_lock_unavailable'
                            USING ERRCODE = '55P03';
                    END IF;
                    PERFORM pg_catalog.pg_sleep(0.2);
                END;
            END LOOP;
        END;
        $migration$;
        """
    )


def _helper_acl_sql(schema: str) -> str:
    return _replace_exact(
        _direct()._helper_acl_sql(schema),
        _direct()._DIRECT_VALID,
        _HELPER,
        expected_count=4,
    )


def _body_shape_fence_sql(schema: str, *, installed: bool) -> str:
    """Fence all retained function bodies and both private helper shapes."""

    direct = _direct()
    abandonment = _abandonment()
    expected_sql_by_signature = {
        _qf(schema, direct._VALID) + "(text)": (
            _shared_valid_sql(schema) if installed else direct._shared_valid_sql(schema)
        ),
        _qf(schema, abandonment._DATASET_GUARD) + "()": (
            _dataset_guard_sql(schema) if installed else direct._dataset_guard_sql(schema)
        ),
        _qf(schema, abandonment._CHECKPOINT_GUARD) + "()": (
            direct._terminal()._checkpoint_guard_sql(schema)
        ),
    }
    function_values = ", ".join(
        "(" + _ql(signature) + ", "
        + _ql(direct._normalized_body_sha256(sql)) + ")"
        for signature, sql in expected_sql_by_signature.items()
    )
    helper_sql_by_signature = {
        _qf(schema, direct._DIRECT_VALID) + "(text)": direct._direct_valid_sql(schema),
    }
    if installed:
        helper_sql_by_signature[_qf(schema, _HELPER) + "(text)"] = _v5_valid_sql(
            schema
        )
    helper_values = ", ".join(
        "(" + _ql(signature) + ", "
        + _ql(direct._normalized_body_sha256(sql)) + ")"
        for signature, sql in helper_sql_by_signature.items()
    )
    expected_helper_count = len(helper_sql_by_signature)
    raw_v5_count = 1 if installed else 0
    return f"""
    DO $migration$
    DECLARE
        matched_function_count bigint;
        matched_helper_count bigint;
        raw_v5_helper_count bigint;
    BEGIN
        SELECT pg_catalog.count(*) INTO matched_function_count
          FROM (VALUES {function_values}) AS expected(signature, body_sha256)
          JOIN pg_catalog.pg_proc AS function_row
            ON function_row.oid = pg_catalog.to_regprocedure(expected.signature)
         WHERE pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                   pg_catalog.btrim(pg_catalog.regexp_replace(
                       function_row.prosrc, '[[:space:]]+', ' ', 'g'
                   )), 'UTF8'
               )), 'hex') = expected.body_sha256;
        SELECT pg_catalog.count(*) INTO matched_helper_count
          FROM (VALUES {helper_values}) AS expected(signature, body_sha256)
          JOIN pg_catalog.pg_proc AS helper
            ON helper.oid = pg_catalog.to_regprocedure(expected.signature)
          JOIN pg_catalog.pg_proc AS shared
            ON shared.oid = pg_catalog.to_regprocedure(
                 {_ql(_qf(schema, direct._VALID) + '(text)')}
               )
          JOIN pg_catalog.pg_language AS language_row
            ON language_row.oid = helper.prolang
         WHERE helper.prorettype = 'pg_catalog.bool'::regtype
           AND helper.prokind = 'f'
           AND language_row.lanname = 'plpgsql'
           AND helper.provolatile = 's'
           AND helper.proisstrict IS FALSE
           AND helper.proparallel = 'u'
           AND helper.prosecdef IS TRUE
           AND helper.proowner = shared.proowner
           AND helper.proconfig IS NOT DISTINCT FROM
                ARRAY['search_path=pg_catalog']::text[]
           AND pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
                 pg_catalog.btrim(pg_catalog.regexp_replace(
                     helper.prosrc, '[[:space:]]+', ' ', 'g'
                 )), 'UTF8'
               )), 'hex') = expected.body_sha256
           AND NOT EXISTS (
                SELECT 1 FROM pg_catalog.aclexplode(COALESCE(
                     helper.proacl,
                     pg_catalog.acldefault('f', helper.proowner)
                )) AS helper_acl
                 WHERE helper_acl.privilege_type = 'EXECUTE'
                   AND helper_acl.grantee <> helper.proowner
           );
        SELECT pg_catalog.count(*) INTO raw_v5_helper_count
         WHERE pg_catalog.to_regprocedure(
                   {_ql(_qf(schema, _HELPER) + '(text)')}
               ) IS NOT NULL;
        IF matched_function_count <> 3
           OR matched_helper_count <> {expected_helper_count}
           OR raw_v5_helper_count <> {raw_v5_count} THEN
            RAISE EXCEPTION
                'provider_directory_v5_http410_shape_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _v3_evidence_fence_sql(schema: str, *, downgrade: bool) -> str:
    dataset_ref = _qf(schema, _subset()._ENDPOINT_DATASET)
    predicate = f"""dataset.publication_metadata_json::jsonb #>> ARRAY[
                         '{_direct()._MARKER}', 'contract_version'
                     ]::text[] = '{_CONTRACT}'"""
    phase = "downgrade" if downgrade else "adoption"
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (SELECT 1 FROM {dataset_ref} AS dataset WHERE {predicate}) THEN
            RAISE EXCEPTION
                'provider_directory_v5_http410_{phase}_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _identity_snapshot_sql(schema: str) -> str:
    return _replace_exact(
        _direct()._identity_snapshot_sql(schema),
        _direct()._IDENTITY_SNAPSHOT,
        _IDENTITY_SNAPSHOT,
        expected_count=1,
    )


def _identity_continuity_sql(schema: str) -> str:
    sql = _replace_exact(
        _direct()._identity_continuity_sql(schema),
        _direct()._IDENTITY_SNAPSHOT,
        _IDENTITY_SNAPSHOT,
        expected_count=2,
    )
    return _replace_exact(
        sql,
        "provider_directory_subset_terminal_v4_identity_changed",
        "provider_directory_v5_http410_identity_changed",
    )


def _drop_identity_snapshot() -> None:
    op.execute(f"DROP TABLE {_IDENTITY_SNAPSHOT};")


def _predecessor_fences(schema: str) -> None:
    direct = _direct()
    original_op = direct.op
    try:
        direct.op = op
        direct._predecessor_shape_fences(schema)
    finally:
        direct.op = original_op
    op.execute(
        _terminal_window()._terminal_window_shape_fence_sql(
            schema,
            installed=True,
        )
    )


def upgrade() -> None:
    schema = _subset()._schema()
    _lock_relations(schema)
    _predecessor_fences(schema)
    op.execute(_direct()._body_shape_fence_sql(schema, installed=True))
    op.execute(_body_shape_fence_sql(schema, installed=False))
    op.execute(_v3_evidence_fence_sql(schema, downgrade=False))
    op.execute(_identity_snapshot_sql(schema))
    op.execute(_v5_valid_sql(schema))
    op.execute(_helper_acl_sql(schema))
    op.execute(_shared_valid_sql(schema))
    op.execute(_dataset_guard_sql(schema))
    op.execute(_body_shape_fence_sql(schema, installed=True))
    op.execute(_identity_continuity_sql(schema))
    _drop_identity_snapshot()
    _predecessor_fences(schema)


def downgrade() -> None:
    schema = _subset()._schema()
    _lock_relations(schema)
    _predecessor_fences(schema)
    op.execute(_body_shape_fence_sql(schema, installed=True))
    op.execute(_v3_evidence_fence_sql(schema, downgrade=True))
    op.execute(_identity_snapshot_sql(schema))
    op.execute(_direct()._shared_valid_sql(schema))
    op.execute(_direct()._dataset_guard_sql(schema))
    op.execute(f"DROP FUNCTION {_qf(schema, _HELPER)}(text);")
    op.execute(_body_shape_fence_sql(schema, installed=False))
    op.execute(_direct()._body_shape_fence_sql(schema, installed=True))
    op.execute(_identity_continuity_sql(schema))
    _drop_identity_snapshot()
    _predecessor_fences(schema)
