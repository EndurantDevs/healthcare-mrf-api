# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native allocator callable semantics only; no issuer, finalizer or live cut."""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from db.plan_release_storage import storage_relation
from process import provider_directory_cms_allocator_callables as checks
from tests.test_provider_dataset_candidates_postgres import _prepare_copy_guard_catalog, _update_canonical_copy_guard
from tests.test_provider_directory_cms_allocation_semantics_postgres import _native_allocations


@asynccontextmanager
async def _native_callables(monkeypatch):
    """Reuse the exact registered database/role lifecycle and its cleanup."""
    async with _native_allocations(monkeypatch) as case:
        catalog = case.catalog
        administrator = await catalog.connection.fetchval("SELECT session_user::text")
        publisher = await case.publisher.fetchval("SELECT session_user::text")
        await _prepare_copy_guard_catalog(catalog, dict(owner=case.owner, catalog=administrator, leak=publisher))
        await _update_canonical_copy_guard(catalog, add_column=True)
        owner_oids_by_name = {
            name: await catalog.connection.fetchval(
                "SELECT oid::bigint FROM pg_catalog.pg_roles WHERE rolname=$1",
                case.owner if name == "guard_pd_dataset_candidate_parent" else administrator,
            )
            for name in checks._SPECS
        }
        yield SimpleNamespace(catalog=catalog, publisher=case.publisher, owners=owner_oids_by_name)


async def _verify(case, connection=None):
    return await checks.require_cms_allocator_callables(
        connection or case.publisher,
        catalog_schema=case.catalog.schema_name,
        preparation_schema=case.catalog.schema_name,
        expected_owner_oids=case.owners,
    )


@pytest.mark.asyncio
async def test_native_requires_exact_cms_guard(monkeypatch):
    async with _native_callables(monkeypatch) as case:
        async with case.publisher.transaction(readonly=True):
            with pytest.raises(ValueError, match="definition_changed"):
                await _verify(case)
        await _update_canonical_copy_guard(case.catalog, include_cms=True)
        async with case.publisher.transaction(readonly=True):
            result = await _verify(case)
            assert len(result["functions"]) == 3
            assert all(row["trusted"] for row in result["dependencies"])
            assert await case.publisher.fetchval("SELECT session_user=current_user") is True


def _native_faults(case, owner):
    schema = case.catalog.schema_name
    tables = storage_relation(schema, "pd_dataset_candidate_tables") + "(text)"
    bulk = storage_relation(schema, "validate_pd_dataset_bulk_relationships") + "(text,text)"
    guard = storage_relation(schema, "guard_pd_dataset_candidate_parent") + "()"
    return (
        f"ALTER FUNCTION {tables} STABLE",
        f"ALTER FUNCTION {tables} STRICT",
        f"ALTER FUNCTION {tables} SECURITY DEFINER",
        f"ALTER FUNCTION {tables} LEAKPROOF",
        f"ALTER FUNCTION {tables} PARALLEL SAFE",
        f"ALTER FUNCTION {tables} COST 99",
        f"ALTER FUNCTION {tables} SET search_path=public,pg_catalog",
        f"ALTER FUNCTION {tables} SET work_mem='4MB'",
        f"ALTER FUNCTION {tables} DEPENDS ON EXTENSION plpgsql",
        f"GRANT EXECUTE ON FUNCTION {tables} TO PUBLIC",
        f"ALTER FUNCTION {bulk} SECURITY DEFINER",
        f'ALTER FUNCTION {bulk} OWNER TO "{owner}"',
        f"ALTER FUNCTION {guard} SECURITY INVOKER",
        f"CREATE FUNCTION {storage_relation(schema, 'pd_dataset_candidate_tables')}(integer) RETURNS text[] "
        "LANGUAGE sql AS $$SELECT ARRAY['unapproved']::text[]$$",
    )


@pytest.mark.asyncio
async def test_native_mutations_and_overload_refuse(monkeypatch):
    async with _native_callables(monkeypatch) as case:
        await _update_canonical_copy_guard(case.catalog, include_cms=True)
        owner = await case.catalog.connection.fetchval(
            "SELECT rolname FROM pg_catalog.pg_roles WHERE oid=$1", case.owners["guard_pd_dataset_candidate_parent"]
        )
        for command in _native_faults(case, owner):
            transaction = case.catalog.connection.transaction()
            await transaction.start()
            try:
                await case.catalog.connection.execute(command)
                with pytest.raises(ValueError, match="cms_allocator_callable_"):
                    await _verify(case, case.catalog.connection)
            finally:
                await transaction.rollback()
        async with case.publisher.transaction(readonly=True):
            assert len((await _verify(case))["functions"]) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ("pd_dataset_candidate_tables", "validate_pd_dataset_bulk_relationships"))
async def test_native_body_replacement_refuses(monkeypatch, name):
    async with _native_callables(monkeypatch) as case:
        await _update_canonical_copy_guard(case.catalog, include_cms=True)
        arguments = "kind text" if name == "pd_dataset_candidate_tables" else "requested_dataset text,kind text"
        result = "text[]" if name == "pd_dataset_candidate_tables" else "void"
        body = "SELECT ARRAY['unapproved']::text[]" if result == "text[]" else "BEGIN RETURN; END;"
        language = "sql IMMUTABLE" if result == "text[]" else "plpgsql"
        await case.catalog.connection.execute(
            f"CREATE OR REPLACE FUNCTION {storage_relation(case.catalog.schema_name, name)}({arguments}) "
            f"RETURNS {result} LANGUAGE {language} SET search_path=pg_catalog AS $$ {body} $$"
        )
        async with case.publisher.transaction(readonly=True):
            with pytest.raises(ValueError, match="definition_changed"):
                await _verify(case)
