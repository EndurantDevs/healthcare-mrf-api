# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Make exact-Practitioner result hashing independent of database locale.

Revision ID: 20260811130000_provider_directory_exact_practitioner_resource_order_repair
Revises: 20260811120000_provider_directory_reviewed_subset_v5_http410_disposition
"""

from __future__ import annotations

from functools import lru_cache
from hashlib import sha256
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = (
    "20260811130000_provider_directory_exact_practitioner_resource_order_repair"
)
down_revision = (
    "20260811120000_provider_directory_reviewed_subset_v5_http410_disposition"
)
branch_labels = None
depends_on = None


_PREDECESSOR_FILE = (
    "20260810060000_provider_directory_uhc_flex_practitioner_acquisition.py"
)
_DEPLOYED_ORDERING = "',' ORDER BY resource.resource_id"
_CORRECTED_ORDERING = (
    "',' ORDER BY resource.resource_id COLLATE pg_catalog.\"C\""
)


@lru_cache(maxsize=1)
def _predecessor() -> ModuleType:
    path = Path(__file__).with_name(_PREDECESSOR_FILE)
    module_spec = importlib.util.spec_from_file_location(
        "_provider_directory_exact_practitioner_acquisition_predecessor",
        path,
    )
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("exact practitioner acquisition revision is unavailable")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _rendered_guard_sql(schema: str, *, corrected: bool) -> str:
    predecessor = _predecessor()
    rendered_sql = predecessor._work_guard_function_sql(schema)
    if rendered_sql.count(_DEPLOYED_ORDERING) != 1:
        raise RuntimeError("exact practitioner resource ordering changed")
    if corrected:
        rendered_sql = rendered_sql.replace(
            _DEPLOYED_ORDERING,
            _CORRECTED_ORDERING,
            1,
        )
    return rendered_sql


def _function_body_sha256(schema: str, *, corrected: bool) -> str:
    rendered_sql = _rendered_guard_sql(schema, corrected=corrected)
    prefix = "AS $guard$\n"
    suffix = "\n    $guard$;"
    if rendered_sql.count(prefix) != 1 or rendered_sql.count(suffix) != 1:
        raise RuntimeError("exact practitioner work guard body changed")
    function_body = rendered_sql.split(prefix, 1)[1].rsplit(suffix, 1)[0]
    normalized_body = " ".join(function_body.split())
    return sha256(normalized_body.encode("utf-8")).hexdigest()


def _function_shape_fence_sql(schema: str, *, expect_corrected: bool) -> str:
    predecessor = _predecessor()
    guard_name = predecessor._WORK_GUARD
    guard_ref = predecessor._qualified(schema, guard_name)
    accepted_hashes = [_function_body_sha256(schema, corrected=True)]
    if not expect_corrected:
        accepted_hashes.append(_function_body_sha256(schema, corrected=False))
    accepted_hashes_sql = ", ".join(
        "'" + body_hash + "'" for body_hash in accepted_hashes
    )
    return f"""
    DO $migration$
    DECLARE matching_count bigint;
    BEGIN
        SELECT pg_catalog.count(*)
          INTO matching_count
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = function_row.pronamespace
          JOIN pg_catalog.pg_language AS language_row
            ON language_row.oid = function_row.prolang
         WHERE function_row.oid = pg_catalog.to_regprocedure('{guard_ref}()')
           AND namespace_row.nspname = '{schema}'
           AND function_row.prokind = 'f'
           AND function_row.pronargs = 0
           AND function_row.prorettype = 'pg_catalog.trigger'::regtype
           AND language_row.lanname = 'plpgsql'
           AND function_row.provolatile = 'v'
           AND function_row.proisstrict IS FALSE
           AND function_row.proparallel = 'u'
           AND function_row.prosecdef IS TRUE
           AND function_row.proconfig IS NOT DISTINCT FROM
               ARRAY['search_path=pg_catalog']::text[]
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
            RAISE EXCEPTION 'provider_directory_exact_practitioner_order_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _replacement_sql(schema: str) -> str:
    rendered_sql = _rendered_guard_sql(schema, corrected=True)
    if rendered_sql.count("CREATE FUNCTION") != 1:
        raise RuntimeError("exact practitioner work guard declaration changed")
    return rendered_sql.replace(
        "CREATE FUNCTION",
        "CREATE OR REPLACE FUNCTION",
        1,
    )


def upgrade() -> None:
    predecessor = _predecessor()
    schema = predecessor._schema()
    work_ref = predecessor._qualified(schema, predecessor._WORK)
    guard_ref = predecessor._qualified(schema, predecessor._WORK_GUARD)
    op.execute(f"LOCK TABLE {work_ref} IN SHARE ROW EXCLUSIVE MODE;")
    op.execute(_function_shape_fence_sql(schema, expect_corrected=False))
    op.execute(_replacement_sql(schema))
    op.execute(f"REVOKE ALL ON FUNCTION {guard_ref}() FROM PUBLIC;")
    op.execute(_function_shape_fence_sql(schema, expect_corrected=True))


def downgrade() -> None:
    # Restoring locale-dependent ordering could reject exact replays and would
    # make result identities depend on the database host's collation.
    return None
