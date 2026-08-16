# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Avoid revalidating sealed Provider Directory content at publication.

Revision ID: 20260816010000_provider_directory_terminal_publication_guard
Revises: 20260815010000_address_formatted_display_v2
"""

from __future__ import annotations

from functools import lru_cache
import importlib.util
from pathlib import Path
from types import ModuleType

from alembic import op


revision = "20260816010000_provider_directory_terminal_publication_guard"
down_revision = "20260815010000_address_formatted_display_v2"
branch_labels = None
depends_on = None


_SUBSET_FILE = "20260808190000_provider_directory_subset_completion_proof.py"
_ADMISSION_FILE = (
    "20260812020000_provider_directory_endpoint_dataset_admission_seal.py"
)


def _load_sibling(filename: str, module_name: str) -> ModuleType:
    path = Path(__file__).with_name(filename)
    module_spec = importlib.util.spec_from_file_location(module_name, path)
    if module_spec is None or module_spec.loader is None:
        raise RuntimeError("provider directory guard revision is unavailable")
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


@lru_cache(maxsize=1)
def _subset() -> ModuleType:
    return _load_sibling(
        _SUBSET_FILE,
        "_provider_directory_terminal_publication_subset",
    )


@lru_cache(maxsize=1)
def _admission() -> ModuleType:
    return _load_sibling(
        _ADMISSION_FILE,
        "_provider_directory_terminal_publication_admission",
    )


def _subset_guard_sql(schema: str, *, transition_only: bool) -> str:
    return _subset()._subset_endpoint_dataset_guard_sql(
        schema,
        use_configured_endpoint_identity=True,
        reviewed_root_policy_aware=True,
        reviewed_subset_profile_aware=True,
        reviewed_subset_terminal_window_profile_aware=True,
        terminal_content_transition_only=transition_only,
    )


def _resource_guard_shape_fence_sql(schema: str) -> str:
    subset = _subset()
    resource = subset._qf(schema, subset._DATASET_RESOURCE)
    resource_guard = subset._qf(schema, subset._DATASET_RESOURCE_GUARD)
    return f"""
    DO $migration$
    DECLARE
        trigger_count bigint;
        function_count bigint;
    BEGIN
        SELECT pg_catalog.count(*)
          INTO trigger_count
          FROM (
                VALUES
                    (
                        'tin_npi_connector_dataset_resource_insert_guard',
                        4::smallint,
                        NULL::name,
                        'new_rows'::name
                    ),
                    (
                        'tin_npi_connector_dataset_resource_update_guard',
                        16::smallint,
                        'old_rows'::name,
                        'new_rows'::name
                    ),
                    (
                        'tin_npi_connector_dataset_resource_delete_guard',
                        8::smallint,
                        'old_rows'::name,
                        NULL::name
                    ),
                    (
                        'tin_npi_connector_dataset_resource_truncate_guard',
                        34::smallint,
                        NULL::name,
                        NULL::name
                    )
               ) AS expected(
                   trigger_name,
                   trigger_type,
                   old_table,
                   new_table
               )
          JOIN pg_catalog.pg_trigger AS trigger_row
            ON trigger_row.tgrelid = {subset._ql(resource)}::regclass
           AND trigger_row.tgname = expected.trigger_name
           AND trigger_row.tgenabled = 'A'
           AND trigger_row.tgisinternal IS FALSE
           AND trigger_row.tgfoid =
                   {subset._ql(resource_guard + '()')}::regprocedure
           AND trigger_row.tgtype = expected.trigger_type
           AND trigger_row.tgattr = ''::int2vector
           AND trigger_row.tgqual IS NULL
           AND trigger_row.tgnargs = 0
           AND pg_catalog.octet_length(trigger_row.tgargs) = 0
           AND trigger_row.tgoldtable IS NOT DISTINCT FROM expected.old_table
           AND trigger_row.tgnewtable IS NOT DISTINCT FROM expected.new_table;
        IF trigger_count <> 4 THEN
            RAISE EXCEPTION
                'tin_npi_connector_dataset_resource_guard_changed'
                USING ERRCODE = '55000';
        END IF;

        SELECT pg_catalog.count(*)
          INTO function_count
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS function_namespace
            ON function_namespace.oid = function_row.pronamespace
          JOIN pg_catalog.pg_language AS function_language
            ON function_language.oid = function_row.prolang
         WHERE function_namespace.nspname = {subset._ql(schema)}
           AND function_row.proname =
                   {subset._ql(subset._DATASET_RESOURCE_GUARD)}
           AND function_row.pronargs = 0
           AND function_row.prorettype = 'pg_catalog.trigger'::regtype
           AND function_language.lanname = 'plpgsql'
           AND function_row.prosecdef IS TRUE
           AND function_row.proconfig IS NOT DISTINCT FROM
                   ARRAY['search_path=pg_catalog']::text[]
           AND NOT pg_catalog.has_function_privilege(
                   'public', function_row.oid, 'EXECUTE'
               );
        IF function_count <> 1 THEN
            RAISE EXCEPTION
                'tin_npi_connector_dataset_resource_guard_function_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _endpoint_guard_function_shape_fence_sql(schema: str) -> str:
    subset = _subset()
    return f"""
    DO $migration$
    DECLARE
        function_count bigint;
    BEGIN
        SELECT pg_catalog.count(*)
          INTO function_count
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS function_namespace
            ON function_namespace.oid = function_row.pronamespace
          JOIN pg_catalog.pg_language AS function_language
            ON function_language.oid = function_row.prolang
         WHERE function_namespace.nspname = {subset._ql(schema)}
           AND function_row.proname =
                   {subset._ql(subset._ENDPOINT_DATASET_GUARD)}
           AND function_row.pronargs = 0
           AND function_row.prorettype = 'pg_catalog.trigger'::regtype
           AND function_language.lanname = 'plpgsql'
           AND function_row.prosecdef IS TRUE
           AND function_row.proconfig IS NOT DISTINCT FROM
                   ARRAY['search_path=pg_catalog']::text[]
           AND NOT pg_catalog.has_function_privilege(
                   'public', function_row.oid, 'EXECUTE'
               );
        IF function_count <> 1 THEN
            RAISE EXCEPTION
                'tin_npi_connector_endpoint_dataset_guard_function_changed'
                USING ERRCODE = '55000';
        END IF;
    END;
    $migration$;
    """


def _relation_shape_fences(schema: str) -> tuple[str, ...]:
    subset = _subset()
    return (
        subset._relation_schema_fence_sql(
            schema,
            subset._ENDPOINT_DATASET,
            subset._COMBINED_ENDPOINT_DATASET_COLUMNS,
        ),
        subset._relation_schema_fence_sql(
            schema,
            subset._DATASET_RESOURCE,
            subset._SUBSET_DATASET_RESOURCE_COLUMNS,
        ),
    )


def _replace_guards(schema: str, *, optimized: bool) -> None:
    subset = _subset()
    admission = _admission()
    lock_relations = (
        subset._ENDPOINT_DATASET,
        subset._DATASET_RESOURCE,
        subset._SOURCE,
    )
    op.execute(
        "LOCK TABLE "
        + ", ".join(subset._qf(schema, relation) for relation in lock_relations)
        + " IN ACCESS EXCLUSIVE MODE;"
    )
    for statement in (
        *_relation_shape_fences(schema),
        subset._subset_column_shape_fence_sql(schema),
        admission._legacy_surface_fence_sql(schema, scoped=True),
        _endpoint_guard_function_shape_fence_sql(schema),
        _resource_guard_shape_fence_sql(schema),
        subset._source_guard_shape_fence_sql(schema, expect_installed=True),
    ):
        op.execute(statement)
    op.execute(subset._dataset_resource_guard_sql(schema, subset_aware=True))
    op.execute(
        f"REVOKE ALL ON FUNCTION "
        f"{subset._qf(schema, subset._DATASET_RESOURCE_GUARD)}() FROM PUBLIC;"
    )
    op.execute(_subset_guard_sql(schema, transition_only=optimized))
    op.execute(
        f"REVOKE ALL ON FUNCTION "
        f"{subset._qf(schema, subset._ENDPOINT_DATASET_GUARD)}() FROM PUBLIC;"
    )
    for statement in (
        subset._subset_column_shape_fence_sql(schema),
        admission._legacy_surface_fence_sql(schema, scoped=True),
        _endpoint_guard_function_shape_fence_sql(schema),
        _resource_guard_shape_fence_sql(schema),
        subset._source_guard_shape_fence_sql(schema, expect_installed=True),
    ):
        op.execute(statement)


def upgrade() -> None:
    _replace_guards(_subset()._schema(), optimized=True)


def downgrade() -> None:
    _replace_guards(_subset()._schema(), optimized=False)
