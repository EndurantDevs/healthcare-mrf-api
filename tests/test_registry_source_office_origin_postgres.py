# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native source-office origin survives rotation under the original independent seal."""

from types import SimpleNamespace
from uuid import UUID

import pytest

from process import entity_address_snapshot_adoption as adoption
from process.registry_source_office_origin import resolve_source_office_origin
from tests.test_network_address_artifact_postgres import _NativeDB
from tests.test_network_compact_publication_postgres import (
    _candidate,
    _prepare,
    _publish,
    _read,
    _seal,
)
from tests.test_network_compact_publication_postgres import compact_publication_db as compact_publication_db

pytestmark = pytest.mark.asyncio


async def _origin(fixture, manifest, *, roles=None, locator=None):
    async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
        resolved = await resolve_source_office_origin(
            fixture.execution_connection,
            fixture.source,
            runtime_roles=tuple(
                sorted(set(fixture.writer_roles["loader_roles"] + fixture.writer_roles["reader_roles"]))
            )
            if roles is None
            else roles,
            control_schema=fixture.control_schema,
            origin_manifest_sha256=manifest.manifest_sha256 if locator is None else locator,
            first_parameter=2,
        )
        rows = await fixture.execution_connection.fetch(
            f"SELECT location_key,canonical_network_ids FROM {resolved.relation_sql} office WHERE entity_id=$1 ORDER BY location_key",
            "1000000491",
            *resolved.arguments,
        )
    return resolved, [dict(row) for row in rows]


async def _rotate(fixture, monkeypatch, *, old_key="old-office", new_key="new-office"):
    address = fixture.address
    schema = '"' + address.schema + '"'
    await address.admin.execute(
        f"CREATE TABLE {schema}.entity_address_unified_next (LIKE {schema}.entity_address_unified INCLUDING ALL)"
    )
    await address.admin.execute(f'ALTER TABLE {schema}.entity_address_unified_next OWNER TO "{address.roles["owner"]}"')
    for role in fixture.runtime_roles:
        await address.admin.execute(f'GRANT SELECT ON {schema}.entity_address_unified_next TO "{role}"')
    await address.admin.execute(
        f"INSERT INTO {schema}.entity_address_unified_next SELECT * FROM {schema}.entity_address_unified"
    )
    await address.admin.execute(
        f"UPDATE {schema}.entity_address_unified_next SET location_key=$2 WHERE location_key=$1", old_key, new_key
    )
    monkeypatch.setattr(adoption.entity_address_unified, "db", _NativeDB(address.publisher))
    async with address.publisher.transaction():
        await adoption._swap_sealed_stage_table(
            address.schema,
            SimpleNamespace(__main_table__="entity_address_unified"),
            SimpleNamespace(__tablename__="entity_address_unified_next", __my_additional_indexes__=[]),
            address.descriptor["protected_owner_oid"],
        )
        oid = await address.publisher.fetchval(
            "SELECT to_regclass($1)::oid::bigint", schema + ".entity_address_unified"
        )
        await address.publisher.execute(
            f"UPDATE {schema}.entity_address_result_generation SET local_generation=2,origin_generation=2,relation_oids[1]=$1",
            oid,
        )
        await address.publisher.execute(f"UPDATE {schema}.address_alias_state_v1 SET generation=generation+1")


async def test_retained_office_origin_is_sealed_logical_scope_after_rotation(compact_publication_db, monkeypatch):
    fixture = compact_publication_db
    copy_target = await _candidate(fixture, (42, 88), 0)
    await _prepare(fixture, copy_target)
    await _seal(fixture, copy_target)
    await _publish(fixture, copy_target)
    manifest, _ = await _read(fixture)
    first, expected = await _origin(fixture, manifest)
    assert first.address_identity[3] == manifest.manifest_sha256
    assert expected == [
        dict(location_key="old-office", canonical_network_ids=[42]),
        dict(location_key="other-office", canonical_network_ids=[88]),
    ]
    await _rotate(fixture, monkeypatch)
    second, actual = await _origin(fixture, manifest)
    assert actual == expected and second.address_identity == first.address_identity
    assert "entity_address_unified_retained_" in second.relation_sql
    with pytest.raises(ValueError):
        async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
            await resolve_source_office_origin(
                fixture.execution_connection,
                fixture.source,
                runtime_roles=fixture.runtime_roles,
                control_schema=fixture.control_schema,
            )
    with pytest.raises(ValueError, match="origin_differs"):
        await _origin(fixture, manifest, roles=fixture.runtime_roles)
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE candidate_id=$1',
        UUID(copy_target.candidate_id),
    )
    assert (await _origin(fixture, manifest))[1] == expected
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        await fixture.connection.execute(
            f'DELETE FROM "{fixture.control_schema}".network_candidate_custody WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
        with pytest.raises(ValueError):
            await resolve_source_office_origin(
                fixture.connection,
                fixture.source,
                runtime_roles=fixture.runtime_roles,
                control_schema=fixture.control_schema,
                origin_manifest_sha256=manifest.manifest_sha256,
            )
    finally:
        await transaction.rollback()
