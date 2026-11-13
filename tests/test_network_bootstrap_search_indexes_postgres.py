# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare copied bootstrap search paths before custody closes, never on replay."""

from unittest.mock import AsyncMock

import pytest

from process import network_bootstrap_sources as bootstrap
from process import network_membership_serving_indexes as serving
from process.entity_address_unified import _required_geo_taxonomy_stage_index_sql
from process.network_compact_layout import CompactAddressRelation
from tests.test_network_custom_address_source_postgres import (
    _bootstrap_prepare,
    _bootstrap_specification,
    bootstrap_db,
)
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema

pytestmark = pytest.mark.asyncio


async def _parent_indexes(fixture):
    return tuple(
        tuple(row)
        for row in await fixture.connection.fetch(
            "SELECT i.indexrelid::bigint,pg_get_indexdef(i.indexrelid) FROM pg_index i "
            "JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace n ON n.oid=t.relnamespace "
            "WHERE n.nspname=$1 ORDER BY i.indexrelid",
            fixture.source_schema,
        )
    )


async def test_bootstrap_search_profile_precedes_native_close_and_receipt(bootstrap_db, monkeypatch):
    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    namespace = bootstrap._schema_name(specification)
    expected_names = {name for name, _ in serving._serving_plan(namespace)}
    assert "entity_address_unified_idx_canonical_network_ids" not in expected_names
    original_close = bootstrap.freeze_fhir_source_snapshot
    observed_names = []
    parent_indexes = await _parent_indexes(fixture)

    async def require_indexes_before_close(connection, schema_name, *arguments, **options):
        if schema_name == namespace:
            indexes = await connection.fetch(
                "SELECT index_relation.relname,i.indisvalid,i.indisready,i.indislive "
                "FROM pg_index i JOIN pg_class index_relation ON index_relation.oid=i.indexrelid "
                "JOIN pg_namespace n ON n.oid=index_relation.relnamespace WHERE n.nspname=$1",
                namespace,
            )
            assert {row["relname"] for row in indexes} == expected_names | {"entity_address_unified_pkey", "npi_pkey"}
            assert all(row["indisvalid"] and row["indisready"] and row["indislive"] for row in indexes)
            assert await connection.fetchval(
                _required_geo_taxonomy_stage_index_sql(namespace, "entity_address_unified")
            )
            observed_names.append(tuple(sorted(row["relname"] for row in indexes)))
        return await original_close(connection, schema_name, *arguments, **options)

    monkeypatch.setattr(bootstrap, "freeze_fhir_source_snapshot", require_indexes_before_close)
    receipt = await _bootstrap_prepare(fixture, specification)
    assert observed_names and expected_names | {"entity_address_unified_pkey", "npi_pkey"} == set(observed_names[0])
    assert await _parent_indexes(fixture) == parent_indexes
    assert await _bootstrap_prepare(fixture, specification) == receipt


async def test_bootstrap_search_index_failure_rolls_back_copy_and_preserves_caller(bootstrap_db, monkeypatch):
    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    namespace = bootstrap._schema_name(specification)
    original_build = serving._build_serving_indexes
    parent_indexes = await _parent_indexes(fixture)

    async def fail_after_first_index(connection, schema_name, plan):
        await original_build(connection, schema_name, plan[:1])
        assert await connection.fetchval("SELECT to_regclass($1)", f'"{schema_name}"."{plan[0][0]}"')
        raise RuntimeError("synthetic search index failure")

    monkeypatch.setattr(serving, "_build_serving_indexes", fail_after_first_index)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        await fixture.connection.execute("CREATE TEMP TABLE bootstrap_search_caller(value int) ON COMMIT DROP")
        await fixture.connection.execute("INSERT INTO bootstrap_search_caller VALUES(9)")
        with pytest.raises(RuntimeError, match="synthetic search index failure"):
            await _bootstrap_prepare(fixture, specification)
        assert await fixture.connection.fetchval("SELECT value FROM bootstrap_search_caller") == 9
        assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", namespace) is None
    assert await _parent_indexes(fixture) == parent_indexes


@pytest.mark.parametrize("mutation", ["partial", "wrong_heap"])
async def test_bootstrap_search_readiness_rejects_missing_or_wrong_heap_index(bootstrap_db, mutation):
    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    receipt = await _bootstrap_prepare(fixture, specification)
    namespace = bootstrap._schema_name(specification)
    index_name = serving._serving_plan(namespace)[0][0]
    async with fixture.connection.transaction(isolation="repeatable_read"):
        if mutation in {"partial", "wrong_heap"}:
            await fixture.connection.execute(f'DROP INDEX "{namespace}"."{index_name}"')
        if mutation == "wrong_heap":
            await fixture.connection.execute(f'CREATE INDEX "{index_name}" ON "{namespace}".npi(npi)')
        assert await bootstrap.verify_network_bootstrap_sources(fixture.connection, receipt) == receipt
        with pytest.raises(serving.NetworkServingIndexesError, match="missing or invalid"):
            await serving._require_address_search_indexes(
                fixture.connection, CompactAddressRelation(namespace, "entity_address_unified")
            )


async def test_legacy_unindexed_bootstrap_replay_remains_readable_but_not_search_ready(bootstrap_db, monkeypatch):
    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    # Model the genuine old constructor: only its native primary indexes existed.
    with monkeypatch.context() as legacy:
        legacy.setattr(serving, "_build_serving_indexes", AsyncMock())
        receipt = await _bootstrap_prepare(fixture, specification)
    indexes = await fixture.connection.fetch(
        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relkind='i'",
        receipt.address_source.schema_name,
    )
    assert {row["relname"] for row in indexes} == {"entity_address_unified_pkey", "npi_pkey"}
    refuse_rebuild = AsyncMock(side_effect=AssertionError("A retained bootstrap must not be indexed or rewritten"))
    monkeypatch.setattr(serving, "_build_serving_indexes", refuse_rebuild)
    assert await _bootstrap_prepare(fixture, specification) == receipt
    refuse_rebuild.assert_not_awaited()
    relation = CompactAddressRelation(
        receipt.address_source.schema_name,
        "entity_address_unified",
    )
    async with fixture.connection.transaction(isolation="repeatable_read"):
        assert await bootstrap.verify_network_bootstrap_sources(fixture.connection, receipt) == receipt
        assert (
            await fixture.connection.fetchval(f'SELECT count(*) FROM "{relation.schema_name}".entity_address_unified')
            == 1
        )
        with pytest.raises(serving.NetworkServingIndexesError, match="geo and taxonomy"):
            await serving._require_address_search_indexes(fixture.connection, relation, compact_base=True)
