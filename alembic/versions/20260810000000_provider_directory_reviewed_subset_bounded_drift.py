# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Admit one bounded advertised-count drift profile for reviewed subsets.

Revision ID: 20260810000000_provider_directory_reviewed_subset_bounded_drift
Revises: 20260809030000_provider_directory_reviewed_root_policy
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = "20260810000000_provider_directory_reviewed_subset_bounded_drift"
down_revision = "20260809030000_provider_directory_reviewed_root_policy"
branch_labels = None
depends_on = None


_POLICY_FILE = "20260809030000_provider_directory_reviewed_root_policy.py"
_LEGACY_STRATEGY_VERSION = (
    "provider-directory-fhir-server-issued-traversal-subset-v3"
)
_BOUNDED_STRATEGY_VERSION = (
    "provider-directory-fhir-server-issued-traversal-subset-v4"
)
_LEGACY_COMPLETION_SCOPES_JSON = (
    '["advertised-count-stability","source-issued-continuation",'
    '"returned-resource-content"]'
)
_BOUNDED_COMPLETION_SCOPES_JSON = (
    '["advertised-count-monotone-decrease-at-most-one",'
    '"source-issued-continuation","returned-resource-content"]'
)


@lru_cache(maxsize=1)
def _policy() -> ModuleType:
    path = Path(__file__).with_name(_POLICY_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_reviewed_subset_bounded_drift_policy",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(
            "provider directory root policy revision is unavailable"
        )
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _subset() -> ModuleType:
    return _policy()._subset()


def _activation() -> ModuleType:
    return _policy()._activation()


def _identity() -> ModuleType:
    return _policy()._identity()


def _abandonment() -> ModuleType:
    return _policy()._abandonment()


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
    )
    op.execute(
        "LOCK TABLE "
        + ", ".join(
            subset._qf(schema, relation_name)
            for relation_name in relation_names
        )
        + " IN ACCESS EXCLUSIVE MODE;"
    )


def _installed_shape_fences(schema: str) -> None:
    abandonment = _abandonment()
    for fence_sql in abandonment._shape_fence_sqls(schema):
        op.execute(fence_sql)
    op.execute(abandonment._preflight_sql(schema, expect_installed=True))


def _profile_sql(metadata_sql: str, *, allow_bounded: bool) -> str:
    subset = _subset()
    strategy_field = subset._ql(
        "provider_directory_current_version_census_strategy_version"
    )
    scopes_field = subset._ql(
        "provider_directory_current_version_census_completion_scopes"
    )
    legacy = f"""
        {metadata_sql} ->> {strategy_field} =
            {subset._ql(_LEGACY_STRATEGY_VERSION)}
        AND {metadata_sql} -> {scopes_field} =
            {subset._ql(_LEGACY_COMPLETION_SCOPES_JSON)}::jsonb
    """
    if not allow_bounded:
        return legacy
    bounded = f"""
        {metadata_sql} ->> {strategy_field} =
            {subset._ql(_BOUNDED_STRATEGY_VERSION)}
        AND {metadata_sql} -> {scopes_field} =
            {subset._ql(_BOUNDED_COMPLETION_SCOPES_JSON)}::jsonb
    """
    return f"(({legacy}) OR ({bounded}))"


def _proof_profile_sql(proof_sql: str, *, allow_bounded: bool) -> str:
    subset = _subset()
    legacy_scopes = subset._ql(_LEGACY_COMPLETION_SCOPES_JSON)
    bounded_scopes = subset._ql(_BOUNDED_COMPLETION_SCOPES_JSON)
    legacy = f"""
        {proof_sql} ->> 'strategy_version' =
            {subset._ql(_LEGACY_STRATEGY_VERSION)}
        AND {proof_sql} -> 'completion_scopes' =
            {legacy_scopes}::jsonb
    """
    if not allow_bounded:
        return legacy
    bounded = f"""
        {proof_sql} ->> 'strategy_version' =
            {subset._ql(_BOUNDED_STRATEGY_VERSION)}
        AND {proof_sql} -> 'completion_scopes' =
            {bounded_scopes}::jsonb
    """
    return f"(({legacy}) OR ({bounded}))"


def _profile_adoption_fence_sql(schema: str) -> str:
    subset = _subset()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    source_ref = subset._qf(schema, subset._SOURCE)
    proof_valid_ref = subset._qf(
        schema,
        subset._PROOF_SHAPE_VALID_FUNCTION,
    )
    source_metadata = "source.metadata_json::jsonb"
    proof = "dataset.completion_proof_json"
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1
              FROM {dataset_ref} AS dataset
             WHERE dataset.completion_proof_json IS NOT NULL
               AND (
                    NOT ({_proof_profile_sql(proof, allow_bounded=True)})
                    OR {proof_valid_ref}(
                        dataset.completion_proof_json,
                        dataset.dataset_hash,
                        dataset.resource_count
                    ) IS DISTINCT FROM TRUE
               )
        ) OR EXISTS (
            SELECT 1
              FROM {source_ref} AS source
             WHERE {source_metadata}
                       -> 'provider_directory_manual_only' = 'true'::jsonb
               AND {source_metadata}
                       -> 'provider_directory_acquisition_enabled' =
                    'true'::jsonb
               AND {source_metadata}
                       ->> 'provider_directory_coverage_mode' =
                    'server-issued-traversal-subset'
               AND ({_profile_sql(source_metadata, allow_bounded=True)})
                    IS DISTINCT FROM TRUE
        ) THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_profile_adoption_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _downgrade_evidence_fence_sql(schema: str) -> str:
    subset = _subset()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    source_ref = subset._qf(schema, subset._SOURCE)
    source_metadata = "source.metadata_json::jsonb"
    strategy_field = subset._ql(
        "provider_directory_current_version_census_strategy_version"
    )
    scopes_field = subset._ql(
        "provider_directory_current_version_census_completion_scopes"
    )
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1
             FROM {dataset_ref} AS dataset
             WHERE dataset.completion_proof_json ->> 'strategy_version' =
                    {subset._ql(_BOUNDED_STRATEGY_VERSION)}
                OR dataset.completion_proof_json -> 'completion_scopes' =
                    {subset._ql(_BOUNDED_COMPLETION_SCOPES_JSON)}::jsonb
        ) OR EXISTS (
            SELECT 1
              FROM {source_ref} AS source
             WHERE {source_metadata} ->> {strategy_field} =
                    {subset._ql(_BOUNDED_STRATEGY_VERSION)}
                OR {source_metadata} -> {scopes_field} =
                    {subset._ql(_BOUNDED_COMPLETION_SCOPES_JSON)}::jsonb
        ) THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_profile_downgrade_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _proof_check_shape_fence_sql(
    schema: str,
    *,
    bounded_profile_installed: bool,
) -> str:
    subset = _subset()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    constraint_name = subset._PARENT_CHECKS[4]
    bounded_condition = (
        "bounded_position = 0"
        if bounded_profile_installed
        else "bounded_position <> 0"
    )
    return f"""
    DO $migration$
    DECLARE
        constraint_count bigint;
        bounded_position integer;
    BEGIN
        SELECT pg_catalog.count(*),
               COALESCE(pg_catalog.max(pg_catalog.strpos(
                   pg_catalog.pg_get_constraintdef(constraint_row.oid),
                   {subset._ql(_BOUNDED_STRATEGY_VERSION)}
               )), 0)
          INTO constraint_count, bounded_position
          FROM pg_catalog.pg_constraint AS constraint_row
         WHERE constraint_row.conrelid = {subset._ql(dataset_ref)}::regclass
           AND constraint_row.conname = {subset._ql(constraint_name)}
           AND constraint_row.contype = 'c'
           AND constraint_row.convalidated IS TRUE
           AND constraint_row.condeferrable IS FALSE
           AND constraint_row.condeferred IS FALSE;
        IF constraint_count <> 1 OR {bounded_condition} THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_profile_check_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _function_profile_fence_sql(
    schema: str,
    *,
    bounded_profile_installed: bool,
) -> str:
    subset = _subset()
    activation = _activation()
    function_signatures = (
        subset._qf(schema, subset._PROOF_SHAPE_VALID_FUNCTION)
        + "(jsonb,text,bigint)",
        subset._qf(schema, subset._ENDPOINT_DATASET_GUARD) + "()",
        subset._qf(schema, subset._SOURCE_GUARD) + "()",
        activation._qf(schema, activation._ACTIVATION_VALID_FUNCTION)
        + "(text)",
    )
    signature_values = ", ".join(
        f"({subset._ql(signature)}::regprocedure)"
        for signature in function_signatures
    )
    bounded_comparison = (
        "bounded_function_count <> 4"
        if bounded_profile_installed
        else "bounded_function_count <> 0"
    )
    return f"""
    DO $migration$
    DECLARE
        function_count bigint;
        bounded_function_count bigint;
    BEGIN
        SELECT pg_catalog.count(*),
               pg_catalog.count(*) FILTER (
                   WHERE pg_catalog.strpos(
                       pg_catalog.pg_get_functiondef(function_row.oid),
                       {subset._ql(_BOUNDED_STRATEGY_VERSION)}
                   ) <> 0
               )
          INTO function_count, bounded_function_count
          FROM (VALUES {signature_values}) AS expected(function_oid)
          JOIN pg_catalog.pg_proc AS function_row
            ON function_row.oid = expected.function_oid;
        IF function_count <> 4 OR {bounded_comparison} THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_subset_profile_function_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _replace_profile_objects(
    schema: str,
    *,
    profile_aware: bool,
) -> None:
    subset = _subset()
    activation = _activation()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    check_name = subset._q(subset._PARENT_CHECKS[4])
    op.execute(
        subset._proof_shape_valid_function_sql(
            schema,
            replace_existing=True,
            reviewed_subset_profile_aware=profile_aware,
        )
    )
    op.execute(f"ALTER TABLE {dataset_ref} DROP CONSTRAINT {check_name};")
    op.execute(
        f"ALTER TABLE {dataset_ref} ADD CONSTRAINT {check_name} CHECK ("
        + subset._subset_proof_shape_check(
            schema,
            reviewed_subset_profile_aware=profile_aware,
        )
        + ");"
    )
    op.execute(
        subset._subset_endpoint_dataset_guard_sql(
            schema,
            use_configured_endpoint_identity=True,
            reviewed_root_policy_aware=True,
            reviewed_subset_profile_aware=profile_aware,
        )
    )
    op.execute(
        subset._subset_published_source_guard_sql(
            schema,
            use_configured_endpoint_identity=True,
            replace_existing=True,
            reviewed_root_policy_aware=True,
            reviewed_subset_profile_aware=profile_aware,
        )
    )
    op.execute(
        activation._activation_valid_function_sql(
            schema,
            use_configured_endpoint_identity=True,
            replace_existing=True,
            reviewed_root_policy_aware=True,
            reviewed_subset_profile_aware=profile_aware,
        )
    )


def _revoke_execute(schema: str) -> None:
    subset = _subset()
    activation = _activation()
    signatures = (
        subset._qf(schema, subset._PROOF_SHAPE_VALID_FUNCTION)
        + "(jsonb,text,bigint)",
        subset._qf(schema, subset._ENDPOINT_DATASET_GUARD) + "()",
        subset._qf(schema, subset._SOURCE_GUARD) + "()",
        activation._qf(schema, activation._ACTIVATION_VALID_FUNCTION)
        + "(text)",
    )
    for signature in signatures:
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC;")


def _shape_fences(schema: str, *, bounded_profile_installed: bool) -> None:
    subset = _subset()
    activation = _activation()
    _installed_shape_fences(schema)
    op.execute(subset._proof_function_shape_fence_sql(schema))
    op.execute(
        activation._activation_shape_fence_sql(
            schema,
            expect_installed=True,
        )
    )
    op.execute(
        _proof_check_shape_fence_sql(
            schema,
            bounded_profile_installed=bounded_profile_installed,
        )
    )
    op.execute(
        _function_profile_fence_sql(
            schema,
            bounded_profile_installed=bounded_profile_installed,
        )
    )


def upgrade() -> None:
    subset = _subset()
    schema = subset._schema()
    _lock_relations(schema)
    _shape_fences(schema, bounded_profile_installed=False)
    op.execute(_profile_adoption_fence_sql(schema))
    _replace_profile_objects(schema, profile_aware=True)
    _revoke_execute(schema)
    op.execute(_profile_adoption_fence_sql(schema))
    op.execute(
        _identity()._adoption_state_fence_sql(
            schema,
            reviewed_root_policy_aware=True,
            reviewed_subset_profile_aware=True,
        )
    )
    _shape_fences(schema, bounded_profile_installed=True)


def downgrade() -> None:
    subset = _subset()
    schema = subset._schema()
    _lock_relations(schema)
    _shape_fences(schema, bounded_profile_installed=True)
    op.execute(_downgrade_evidence_fence_sql(schema))
    _replace_profile_objects(schema, profile_aware=False)
    _revoke_execute(schema)
    op.execute(
        _identity()._adoption_state_fence_sql(
            schema,
            reviewed_root_policy_aware=True,
        )
    )
    _shape_fences(schema, bounded_profile_installed=False)
