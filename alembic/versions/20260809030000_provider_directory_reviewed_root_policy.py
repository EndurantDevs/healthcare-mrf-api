# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind reviewed Provider Directory lifecycle evidence to root policy.

Revision ID: 20260809030000_provider_directory_reviewed_root_policy
Revises: 20260809020000_nppes_lifecycle_date_tolerance
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = "20260809030000_provider_directory_reviewed_root_policy"
down_revision = "20260809020000_nppes_lifecycle_date_tolerance"
branch_labels = None
depends_on = None


_IDENTITY_FILE = (
    "20260809010000_provider_directory_effective_endpoint_identity.py"
)


@lru_cache(maxsize=1)
def _identity() -> ModuleType:
    path = Path(__file__).with_name(_IDENTITY_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_reviewed_root_policy_identity",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError(
            "provider directory effective endpoint revision is unavailable"
        )
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _subset() -> ModuleType:
    return _identity()._subset()


def _activation() -> ModuleType:
    return _identity()._activation()


def _abandonment() -> ModuleType:
    return _identity()._predecessor()


def _content_proof_shape_fence_sql(
    schema: str,
    *,
    expect_installed: bool | None,
) -> str:
    subset = _subset()
    function_ref = subset._qf(
        schema,
        subset._CONTENT_PROOF_VALID_FUNCTION,
    )
    if expect_installed is True:
        shape_condition = "signature_oid IS NULL OR function_count <> 1"
    elif expect_installed is False:
        shape_condition = "signature_oid IS NOT NULL"
    else:
        shape_condition = "signature_oid IS NOT NULL AND function_count <> 1"
    return f"""
    DO $migration$
    DECLARE
        signature_oid oid;
        function_count bigint;
    BEGIN
        signature_oid := pg_catalog.to_regprocedure(
            {subset._ql(function_ref + '(jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb)')}
        );
        SELECT pg_catalog.count(*)
          INTO function_count
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS function_namespace
            ON function_namespace.oid = function_row.pronamespace
          JOIN pg_catalog.pg_language AS function_language
            ON function_language.oid = function_row.prolang
         WHERE function_row.oid = signature_oid
           AND function_namespace.nspname = {subset._ql(schema)}
           AND function_row.pronargs = 10
           AND function_row.prorettype = 'pg_catalog.bool'::regtype
           AND function_language.lanname = 'plpgsql'
           AND function_row.provolatile = 'i'
           AND function_row.proisstrict IS TRUE
           AND function_row.proparallel = 's'
           AND function_row.prosecdef IS TRUE
           AND function_row.proconfig IS NOT DISTINCT FROM
                ARRAY['search_path=pg_catalog']::text[]
           AND NOT EXISTS (
                SELECT 1
                  FROM pg_catalog.aclexplode(
                       COALESCE(
                           function_row.proacl,
                           pg_catalog.acldefault('f', function_row.proowner)
                       )
                  ) AS function_acl
                 WHERE function_acl.grantee = 0
                   AND function_acl.privilege_type = 'EXECUTE'
           );
        IF {shape_condition} THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_root_policy_function_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _replay_check_shape_fence_sql(schema: str) -> str:
    subset = _subset()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    constraint_name = subset._PARENT_CHECKS[5]
    return f"""
    DO $migration$
    DECLARE
        constraint_count bigint;
    BEGIN
        SELECT pg_catalog.count(*)
          INTO constraint_count
          FROM pg_catalog.pg_constraint AS constraint_row
         WHERE constraint_row.conrelid = {subset._ql(dataset_ref)}::regclass
           AND constraint_row.conname = {subset._ql(constraint_name)}
           AND constraint_row.contype = 'c'
           AND constraint_row.convalidated IS TRUE
           AND constraint_row.condeferrable IS FALSE
           AND constraint_row.condeferred IS FALSE;
        IF constraint_count <> 1 THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_root_policy_check_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _policy_adoption_fence_sql(schema: str) -> str:
    subset = _subset()
    activation = _activation()
    source_ref = subset._qf(schema, subset._SOURCE)
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    policy_key = subset._ql(subset._REVIEWED_ROOT_POLICY_KEY)
    activation_key = subset._ql(activation._ACTIVATION_KEY_V2)
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1
              FROM {source_ref} AS source
             WHERE source.metadata_json::jsonb ? {policy_key}
                OR source.metadata_json::jsonb ? {activation_key}
                OR source.metadata_json::jsonb
                     ->> 'provider_directory_candidate_status' IN (
                        'pending_reviewed_subset_acquisition',
                        'verified_reviewed_subset_acquisition'
                     )
        ) OR EXISTS (
            SELECT 1
              FROM {dataset_ref} AS dataset
             WHERE dataset.publication_metadata_json::jsonb ? {policy_key}
        ) THEN
            RAISE EXCEPTION
                'provider_directory_reviewed_root_policy_adoption_blocked'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


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


def _replace_policy_functions(schema: str) -> None:
    subset = _subset()
    activation = _activation()
    abandonment = _abandonment()
    op.execute(
        subset._content_proof_valid_function_sql(
            schema,
            replace_existing=True,
        )
    )
    op.execute(
        subset._coverage_shape_valid_function_sql(
            schema,
            replace_existing=True,
            reviewed_root_policy_aware=True,
        )
    )
    op.execute(
        subset._subset_endpoint_dataset_guard_sql(
            schema,
            use_configured_endpoint_identity=True,
            reviewed_root_policy_aware=True,
        )
    )
    op.execute(
        subset._subset_published_source_guard_sql(
            schema,
            use_configured_endpoint_identity=True,
            replace_existing=True,
            reviewed_root_policy_aware=True,
        )
    )
    op.execute(
        activation._activation_valid_function_sql(
            schema,
            use_configured_endpoint_identity=True,
            replace_existing=True,
            reviewed_root_policy_aware=True,
        )
    )
    op.execute(
        activation._source_guard_function_sql(
            schema,
            allow_effective_endpoint_cutover=True,
            replace_existing=True,
            reviewed_root_policy_aware=True,
        )
    )
    op.execute(
        activation._dataset_guard_function_sql(
            schema,
            replace_existing=True,
            reviewed_root_policy_aware=True,
        )
    )
    op.execute(
        abandonment._dataset_guard_sql(
            schema,
            reviewed_root_policy_aware=True,
        )
    )


def _replace_replay_check(schema: str) -> None:
    subset = _subset()
    dataset_ref = subset._qf(schema, subset._ENDPOINT_DATASET)
    constraint_name = subset._q(subset._PARENT_CHECKS[5])
    op.execute(
        f"ALTER TABLE {dataset_ref} DROP CONSTRAINT {constraint_name};"
    )
    op.execute(
        f"ALTER TABLE {dataset_ref} ADD CONSTRAINT {constraint_name} "
        "CHECK ("
        + subset._subset_replay_evidence_check(
            schema,
            reviewed_root_policy_aware=True,
        )
        + ");"
    )


def _revoke_execute(schema: str) -> None:
    subset = _subset()
    activation = _activation()
    abandonment = _abandonment()
    signatures = (
        subset._qf(schema, subset._CONTENT_PROOF_VALID_FUNCTION)
        + "(jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb)",
        subset._qf(schema, subset._COVERAGE_SHAPE_VALID_FUNCTION)
        + "(jsonb,jsonb,text,text)",
        subset._qf(schema, subset._ENDPOINT_DATASET_GUARD) + "()",
        subset._qf(schema, subset._SOURCE_GUARD) + "()",
        activation._qf(schema, activation._ACTIVATION_VALID_FUNCTION)
        + "(text)",
        activation._qf(schema, activation._SOURCE_GUARD_FUNCTION) + "()",
        activation._qf(schema, activation._DATASET_GUARD_FUNCTION) + "()",
        subset._qf(schema, abandonment._DATASET_GUARD) + "()",
    )
    for signature in signatures:
        op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC;")


def upgrade() -> None:
    subset = _subset()
    schema = subset._schema()
    _lock_relations(schema)
    _installed_shape_fences(schema)
    op.execute(_content_proof_shape_fence_sql(schema, expect_installed=None))
    op.execute(_replay_check_shape_fence_sql(schema))
    op.execute(_policy_adoption_fence_sql(schema))
    _replace_policy_functions(schema)
    _replace_replay_check(schema)
    _revoke_execute(schema)
    op.execute(_content_proof_shape_fence_sql(schema, expect_installed=True))
    op.execute(_replay_check_shape_fence_sql(schema))
    _installed_shape_fences(schema)
    op.execute(_identity()._adoption_state_fence_sql(schema))


def downgrade() -> None:
    # Reinstalling policy-unaware proof bodies would invalidate evidence
    # accepted by this revision. Preserve the hardened bodies fail closed.
    return None
