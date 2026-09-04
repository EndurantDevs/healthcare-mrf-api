# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scope exact logical-current validation to exact endpoint changes.

Revision ID: 20260904163000_provider_directory_exact_guard_scope
Revises: 20260903160000_plan_pricing_state_scan
"""

from __future__ import annotations

from functools import lru_cache
import hashlib
import importlib.util
import os
from pathlib import Path
import re
from types import ModuleType

from alembic import op


revision = "20260904163000_provider_directory_exact_guard_scope"
down_revision = "20260903160000_plan_pricing_state_scan"
branch_labels = None
depends_on = None


_ROOTED_FILE = "20260811020000_provider_directory_rooted_graph_acquisition.py"


@lru_cache(maxsize=1)
def _rooted() -> ModuleType:
    path = Path(__file__).with_name(_ROOTED_FILE)
    spec = importlib.util.spec_from_file_location(
        "_provider_directory_exact_guard_scope_rooted",
        path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("Provider Directory rooted predecessor unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qf(schema: str, identifier: str) -> str:
    return f"{_q(schema)}.{_q(identifier)}"


def _ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _replace_once(sql: str, old: str, new: str, label: str) -> str:
    if sql.count(old) != 1:
        raise RuntimeError(f"Provider Directory {label} predecessor changed")
    return sql.replace(old, new, 1)


def _function_body_md5(create_sql: str) -> str:
    start = "AS $guard$"
    end = "$guard$;"
    if create_sql.count(start) != 1 or create_sql.count(end) != 1:
        raise RuntimeError("Provider Directory logical-current guard body changed")
    body = create_sql.split(start, 1)[1].rsplit(end, 1)[0]
    normalized = re.sub(r"\s+", " ", body.strip())
    return hashlib.md5(
        normalized.encode("utf-8"),
        usedforsecurity=False,
    ).hexdigest()


def _logical_current_guard_sql(schema: str, *, scoped: bool) -> str:
    rooted = _rooted()
    sql = _replace_once(
        rooted._logical_current_guard_sql(schema),
        "CREATE FUNCTION",
        "CREATE OR REPLACE FUNCTION",
        "logical-current guard",
    )
    if not scoped:
        return sql
    exact_endpoints = (
        f"{_ql(rooted._LEGACY_ENDPOINT_ID)}, "
        f"{_ql(rooted._ROOTED_ENDPOINT_ID)}"
    )
    begin = "    BEGIN\n"
    scoped_begin = f"""    BEGIN
        IF TG_TABLE_NAME = {_ql(rooted._DATASET)} THEN
            IF TG_OP = 'INSERT'
               AND NEW.endpoint_id NOT IN ({exact_endpoints}) THEN
                RETURN NULL;
            ELSIF TG_OP = 'UPDATE'
              AND OLD.endpoint_id NOT IN ({exact_endpoints})
              AND NEW.endpoint_id NOT IN ({exact_endpoints}) THEN
                RETURN NULL;
            ELSIF TG_OP = 'DELETE'
              AND OLD.endpoint_id NOT IN ({exact_endpoints}) THEN
                RETURN NULL;
            END IF;
        END IF;
"""
    return _replace_once(
        sql,
        begin,
        scoped_begin,
        "logical-current guard entry",
    )


def _guard_fence_sql(schema: str, *, scoped: bool) -> str:
    rooted = _rooted()
    expected_triggers = ",\n            ".join(
        "(" + ", ".join(
            (
                _ql(relation),
                _ql(f"pd_exact_logical_current_{relation}_guard"),
            )
        ) + ")"
        for relation in (
            rooted._DATASET,
            rooted._LEGACY_DATASET,
            rooted._ROOTED_DATASET,
        )
    )
    expected_body_md5 = _function_body_md5(
        _logical_current_guard_sql(schema, scoped=scoped)
    )
    return f"""
    DO $migration$
    DECLARE
        matched_triggers bigint;
    BEGIN
        SELECT pg_catalog.count(*)
          INTO matched_triggers
          FROM (VALUES
            {expected_triggers}
          ) AS expected(relation_name, trigger_name)
          JOIN pg_catalog.pg_namespace AS relation_namespace
            ON relation_namespace.nspname = {_ql(schema)}
          JOIN pg_catalog.pg_class AS relation_row
            ON relation_row.relnamespace = relation_namespace.oid
           AND relation_row.relname = expected.relation_name
           AND relation_row.relkind IN ('r', 'p')
          JOIN pg_catalog.pg_trigger AS trigger_row
            ON trigger_row.tgrelid = relation_row.oid
           AND trigger_row.tgname = expected.trigger_name::name
           AND trigger_row.tgenabled = 'A'
           AND trigger_row.tgtype = 29
           AND trigger_row.tgconstraint <> 0
           AND trigger_row.tgdeferrable IS TRUE
           AND trigger_row.tginitdeferred IS TRUE
           AND trigger_row.tgisinternal IS FALSE
           AND trigger_row.tgattr = ''::int2vector
           AND trigger_row.tgqual IS NULL
           AND trigger_row.tgnargs = 0
           AND pg_catalog.octet_length(trigger_row.tgargs) = 0
           AND trigger_row.tgoldtable IS NULL
           AND trigger_row.tgnewtable IS NULL
          JOIN pg_catalog.pg_proc AS function_row
            ON function_row.oid = trigger_row.tgfoid
           AND function_row.pronamespace = relation_namespace.oid
           AND function_row.proname = {_ql(rooted._LOGICAL_CURRENT_GUARD)}
           AND function_row.pronargs = 0
           AND function_row.prokind = 'f'
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
               ))) = {_ql(expected_body_md5)}
          JOIN pg_catalog.pg_language AS function_language
            ON function_language.oid = function_row.prolang
           AND function_language.lanname = 'plpgsql';
        IF matched_triggers <> 3 THEN
            RAISE EXCEPTION
                'provider_directory_exact_logical_current_guard_shape_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _lock_sql(schema: str) -> str:
    rooted = _rooted()
    relations = (
        rooted._DATASET,
        rooted._LEGACY_DATASET,
        rooted._ROOTED_DATASET,
    )
    return (
        "LOCK TABLE "
        + ", ".join(_qf(schema, relation) for relation in relations)
        + " IN ACCESS EXCLUSIVE MODE;"
    )


def _replace_guard(schema: str, *, scoped: bool) -> None:
    rooted = _rooted()
    op.execute(_guard_fence_sql(schema, scoped=not scoped))
    op.execute(_logical_current_guard_sql(schema, scoped=scoped))
    op.execute(
        f"REVOKE ALL ON FUNCTION "
        f"{_qf(schema, rooted._LOGICAL_CURRENT_GUARD)}() FROM PUBLIC;"
    )
    op.execute(_guard_fence_sql(schema, scoped=scoped))


def upgrade() -> None:
    schema = _schema()
    op.execute("SET LOCAL lock_timeout = '5s';")
    op.execute(_lock_sql(schema))
    _replace_guard(schema, scoped=True)


def downgrade() -> None:
    schema = _schema()
    op.execute("SET LOCAL lock_timeout = '5s';")
    op.execute(_lock_sql(schema))
    _replace_guard(schema, scoped=False)
