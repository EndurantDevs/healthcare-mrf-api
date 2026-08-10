# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Repair terminal-root evidence resource-count aggregation.

Revision ID: 20260810100000_provider_directory_terminal_root_retirement_resource_count_repair
Revises: 20260809040000_ptg_import_wave_ordinary_cutover
"""

from __future__ import annotations

from hashlib import sha256
import os

from alembic import op

from db import (
    migration_provider_directory_terminal_root_retirement_evidence as evidence,
)


revision = (
    "20260810100000_provider_directory_terminal_root_retirement_resource_count_repair"
)
down_revision = "20260809040000_ptg_import_wave_ordinary_cutover"
branch_labels = None
depends_on = None


_DATASET = "provider_directory_endpoint_dataset"
_CORRECTED_COUNT_SQL = (
    "COALESCE(pg_catalog.sum(grouped.row_count), 0)::bigint\n"
    "                   AS actual_count"
)
_DEPLOYED_COUNT_SQL = "pg_catalog.count(*)::bigint AS actual_count"


def _schema() -> str:
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _function_signature(schema: str) -> str:
    return f"{evidence._qf(schema, evidence.EVIDENCE_FUNCTION)}(text)"


def _function_body_sha256(schema: str, *, corrected: bool) -> str:
    function_sql = evidence.evidence_function_sql(schema)
    prefix = "AS $function$\n"
    suffix = "\n    $function$;"
    if function_sql.count(prefix) != 1 or function_sql.count(suffix) != 1:
        raise RuntimeError("terminal retirement evidence function body changed")
    function_body = function_sql.split(prefix, 1)[1].rsplit(suffix, 1)[0]
    if function_body.count(_CORRECTED_COUNT_SQL) != 1:
        raise RuntimeError("terminal retirement evidence count SQL changed")
    if not corrected:
        function_body = function_body.replace(
            _CORRECTED_COUNT_SQL,
            _DEPLOYED_COUNT_SQL,
            1,
        )
    normalized_body = " ".join(function_body.split())
    return sha256(normalized_body.encode("utf-8")).hexdigest()


def _unused_fence_sql(schema: str) -> str:
    dataset = evidence._qf(schema, _DATASET)
    return f"""
    DO $migration$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM {dataset} AS row
             WHERE row.status = {evidence._ql(evidence.STATUS)}
                OR COALESCE(row.publication_metadata_json::jsonb, '{{}}'::jsonb)
                     ? {evidence._ql(evidence.MARKER)}
        ) THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_resource_count_repair_used'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _function_shape_fence_sql(
    schema: str,
    *,
    expect_corrected_body: bool,
) -> str:
    signature = _function_signature(schema)
    accepted_body_hashes = [_function_body_sha256(schema, corrected=True)]
    if not expect_corrected_body:
        accepted_body_hashes.append(_function_body_sha256(schema, corrected=False))
    accepted_hashes_sql = ", ".join(
        evidence._ql(body_hash) for body_hash in accepted_body_hashes
    )
    return f"""
    DO $migration$
    DECLARE
        function_oid oid;
        matching_count bigint;
    BEGIN
        function_oid := pg_catalog.to_regprocedure(
            {evidence._ql(signature)}
        );
        SELECT pg_catalog.count(*)
          INTO matching_count
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = function_row.pronamespace
          JOIN pg_catalog.pg_language AS language_row
            ON language_row.oid = function_row.prolang
         WHERE function_row.oid = function_oid
           AND namespace_row.nspname = {evidence._ql(schema)}
           AND function_row.prokind = 'f'
           AND function_row.pronargs = 1
           AND function_row.prorettype = 'pg_catalog.jsonb'::regtype
           AND language_row.lanname = 'sql'
           AND function_row.provolatile = 's'
           AND function_row.proisstrict IS FALSE
           AND function_row.proparallel = 'u'
           AND function_row.prosecdef IS TRUE
           AND function_row.proconfig IS NOT DISTINCT FROM ARRAY[
                'search_path=pg_catalog', 'TimeZone=UTC'
           ]::text[]
           AND pg_catalog.encode(
                   pg_catalog.sha256(
                       pg_catalog.convert_to(
                           pg_catalog.btrim(
                               pg_catalog.regexp_replace(
                                   function_row.prosrc,
                                   '[[:space:]]+',
                                   ' ',
                                   'g'
                               )
                           ),
                           'UTF8'
                       )
                   ),
                   'hex'
               ) IN ({accepted_hashes_sql})
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
        IF matching_count <> 1 THEN
            RAISE EXCEPTION
                'provider_directory_terminal_root_retirement_evidence_function_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _replacement_sql(schema: str) -> str:
    create_sql = evidence.evidence_function_sql(schema)
    if "CREATE FUNCTION" not in create_sql:
        raise RuntimeError("terminal retirement evidence function DDL changed")
    return create_sql.replace(
        "CREATE FUNCTION",
        "CREATE OR REPLACE FUNCTION",
        1,
    )


def upgrade() -> None:
    schema = _schema()
    dataset = evidence._qf(schema, _DATASET)
    signature = _function_signature(schema)
    op.execute(f"LOCK TABLE {dataset} IN SHARE ROW EXCLUSIVE MODE;")
    op.execute(_unused_fence_sql(schema))
    op.execute(_function_shape_fence_sql(schema, expect_corrected_body=False))
    op.execute(_replacement_sql(schema))
    op.execute(f"REVOKE ALL ON FUNCTION {signature} FROM PUBLIC;")
    op.execute(_function_shape_fence_sql(schema, expect_corrected_body=True))


def downgrade() -> None:
    # Reinstalling the deployed count-of-groups body would undercount datasets
    # with multiple resources of one type and invalidate repaired evidence.
    return None
