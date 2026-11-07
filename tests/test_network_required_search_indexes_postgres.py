# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native profiles precede new head changes without changing retained readers."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_membership_serving_indexes as indexes
from process.network_compact_layout import CompactAddressRelation
from process.network_membership_publication import NetworkPublicationError
from process.network_serving_reactivation import NetworkServingReactivationError, reactivate_network_serving_generation
from tests import test_network_compact_publication_postgres as compact
from tests.test_network_compact_custom_publication_postgres import _approved_candidate
from tests.test_network_compact_publication_postgres import compact_publication_db
from tests.test_network_custom_address_source_postgres import _seed
from tests.test_network_membership_serving_indexes_postgres import (
    _prepare,
    projection_db,
    serving_indexes_db,
    serving_schema,
    validation_db,
)

pytestmark = pytest.mark.asyncio


async def _head_and_sequence(fixture):
    schema = '"' + fixture.control_schema + '"'
    return (
        await fixture.connection.fetchval(f"SELECT generation_id FROM {schema}.network_serving_control"),
        await fixture.connection.fetchval(f"SELECT count(*) FROM {schema}.network_serving_manifest"),
        tuple(
            await fixture.connection.fetchrow(
                f"SELECT last_value,is_called FROM {schema}.network_serving_manifest_generation_id_seq"
            )
        ),
    )


@pytest.fixture
async def archived_unindexed_db(monkeypatch):
    original = compact._full_two_offices

    async def unindexed(connection, roles, prepare):
        schema = await original(connection, roles, prepare)
        await connection.execute(f'DROP INDEX "{schema}".entity_address_unified_idx_geo_taxonomy')
        return schema

    monkeypatch.setattr(compact, "_full_two_offices", unindexed)
    async with asynccontextmanager(compact.compact_publication_db.__wrapped__)(monkeypatch) as fixture:
        yield fixture


async def test_ready_unindexed_base_refuses_before_manifest_or_sequence(archived_unindexed_db):
    fixture = archived_unindexed_db
    copy_target = await compact._candidate(fixture, (42, 88), 0)
    await compact._prepare(fixture, copy_target)
    await compact._seal(fixture, copy_target)
    before = await _head_and_sequence(fixture)
    with pytest.raises(NetworkPublicationError, match="geo and taxonomy indexes are not ready"):
        await compact._publish(fixture, copy_target)
    assert await _head_and_sequence(fixture) == before


async def test_old_pinned_read_and_replay_survive_but_unindexed_reactivation_refuses(
    archived_unindexed_db, monkeypatch
):
    fixture = archived_unindexed_db
    copy_targets, publications = [], []
    # Model two previously accepted archives; only the new activation uses the new gate.
    with monkeypatch.context() as historical:
        historical.setattr(indexes, "verify_network_search_indexes", AsyncMock())
        for head in (0, None):
            copy_target = await compact._candidate(
                fixture, (42, 88), head if head is not None else publications[0]["generation_id"]
            )
            await compact._prepare(fixture, copy_target)
            await compact._seal(fixture, copy_target)
            copy_targets.append(copy_target)
            publications.append(await compact._publish(fixture, copy_target))
    before = await _head_and_sequence(fixture)
    manifest, office_rows = await compact._read(fixture, generation_id=publications[0]["generation_id"])
    assert manifest.generation_id == publications[0]["generation_id"] and office_rows
    assert await compact._publish(fixture, copy_targets[0]) == publications[0] | {"replayed": True}
    assert await _head_and_sequence(fixture) == before
    async with fixture.execution_connection.transaction(isolation="repeatable_read"):
        with pytest.raises(NetworkServingReactivationError) as refusal:
            await reactivate_network_serving_generation(
                fixture.execution_connection,
                generation_id=publications[0]["generation_id"],
                expected_head=publications[1]["generation_id"],
                expected_approved_revision=0,
                control_schema=fixture.control_schema,
            )
    assert isinstance(refusal.value.__cause__, indexes.NetworkServingIndexesError)
    assert "geo and taxonomy indexes are not ready" in str(refusal.value.__cause__)
    assert await _head_and_sequence(fixture) == before


@pytest.mark.parametrize("damage", ["missing_geo", "taxonomy_predicate", "taxonomy_method"])
@pytest.mark.parametrize("compact_base", [False, True])
async def test_native_profile_rejects_missing_or_wrong_geo_taxonomy(serving_indexes_db, damage, compact_base):
    fixture = serving_indexes_db
    async with fixture.connection.transaction():
        await _prepare(fixture)
    schema = fixture.copy_target.schema_name
    relation = CompactAddressRelation(
        schema,
        "entity_address_unified",
        await fixture.connection.fetchval("SELECT to_regclass($1)::oid::bigint", schema + ".entity_address_unified"),
    )
    options = {"compact_base": True} if compact_base else {"canonical": True}
    if compact_base:
        await fixture.connection.execute(f'DROP INDEX "{schema}".canonical_network_ids_gin')
    async with fixture.connection.transaction():
        await indexes._require_address_search_indexes(fixture.connection, relation, **options)
    name = "entity_address_unified_idx_" + ("geo_idx" if damage == "missing_geo" else "geo_taxonomy")
    definition = await fixture.connection.fetchval("SELECT pg_get_indexdef(to_regclass($1))", schema + "." + name)
    await fixture.connection.execute(f'DROP INDEX "{schema}"."{name}"')
    if damage == "taxonomy_predicate":
        await fixture.connection.execute(definition.split(" WHERE ")[0] + " WHERE false")
    if damage == "taxonomy_method":
        await fixture.connection.execute(f'CREATE INDEX "{name}" ON "{schema}".entity_address_unified(npi)')
    async with fixture.connection.transaction():
        with pytest.raises(indexes.NetworkServingIndexesError):
            await indexes._require_address_search_indexes(fixture.connection, relation, **options)


async def test_custom_indexes_build_before_seal_and_partial_ddl_rolls_back(compact_publication_db, monkeypatch):
    fixture = compact_publication_db
    await fixture.connection.execute(f'CREATE TABLE "{fixture.address.schema}".npi(npi bigint PRIMARY KEY)')
    approval = SimpleNamespace(
        connection=fixture.connection,
        engine=fixture.engine,
        control_schema=fixture.control_schema,
        source_schema=fixture.address.schema,
    )
    seed = await _seed(approval)
    copy_target, members = await _approved_candidate(fixture, seed.revision, 0)
    original = indexes._build_serving_indexes

    async def fail_after_two(connection, schema, plan):
        await original(connection, schema, plan[:2])
        raise RuntimeError("synthetic index build failure")

    with monkeypatch.context() as failure:
        failure.setattr(indexes, "_build_serving_indexes", fail_after_two)
        with pytest.raises(RuntimeError, match="synthetic index"):
            await compact._prepare(fixture, copy_target)
    assert (
        await fixture.connection.fetchval("SELECT to_regclass($1)", copy_target.schema_name + ".entity_address_unified")
        is None
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT state FROM "{fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        == "sealed"
    )
    await compact._prepare(fixture, copy_target)
    async with fixture.execution_connection.transaction():
        custom = CompactAddressRelation(
            copy_target.schema_name,
            "entity_address_unified",
            await fixture.connection.fetchval(
                "SELECT to_regclass($1)::oid::bigint", copy_target.schema_name + ".entity_address_unified"
            ),
        )
        await indexes._require_address_search_indexes(fixture.execution_connection, custom)
    await compact._seal(fixture, copy_target)
    published = await compact._publish(fixture, copy_target)
    pinned, office_rows = await compact._read(fixture, network_ids=(members[0]["network_id"],))
    assert published["generation_id"] == pinned.generation_id and len(office_rows) == 1
    assert pinned.compact_layout.custom == custom
