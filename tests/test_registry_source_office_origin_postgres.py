# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native source-office origin survives rotation under the original independent seal."""

from dataclasses import replace
from uuid import UUID

import pytest

from process.registry_source_office_origin import resolve_source_office_origin
from tests.test_network_address_artifact_postgres import _native_swap
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
    await _native_swap(address, monkeypatch)


async def test_retained_office_origin_is_sealed_logical_scope_after_rotation(compact_publication_db, monkeypatch):
    fixture = compact_publication_db
    copy_target = await _candidate(fixture, (42, 88), 0)
    await _prepare(fixture, copy_target)
    await _seal(fixture, copy_target)
    await _publish(fixture, copy_target)
    manifest, _ = await _read(fixture)
    first, expected = await _origin(fixture, manifest)
    assert first.address_scope[3] == manifest.manifest_sha256
    assert expected == [
        dict(location_key="old-office", canonical_network_ids=[42]),
        dict(location_key="other-office", canonical_network_ids=[88]),
    ]
    await _rotate(fixture, monkeypatch)
    second, actual = await _origin(fixture, manifest)
    assert actual == expected and second.address_scope == first.address_scope
    assert "entity_address_unified_retained_" in second.relation_sql
    with pytest.raises(ValueError):
        async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
            await resolve_source_office_origin(
                fixture.execution_connection,
                fixture.source,
                runtime_roles=fixture.runtime_roles,
                control_schema=fixture.control_schema,
            )
    # Runtime roles grant no origin authority; a different logical address edition does not match the pin.
    with pytest.raises(ValueError, match="origin_differs"):
        async with fixture.execution_connection.transaction(isolation="repeatable_read", readonly=True):
            await resolve_source_office_origin(
                fixture.execution_connection,
                replace(fixture.source, generation_id="0" * 64),
                runtime_roles=fixture.runtime_roles,
                control_schema=fixture.control_schema,
                origin_manifest_sha256=manifest.manifest_sha256,
            )
    await fixture.connection.execute(
        f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE candidate_id=$1',
        UUID(copy_target.candidate_id),
    )
    assert (await _origin(fixture, manifest))[1] == expected
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    try:
        # Retained reuse follows the published logical state, not a custody row.
        await fixture.connection.execute(
            f"UPDATE \"{fixture.control_schema}\".network_membership_candidate SET state='ready' WHERE candidate_id=$1",
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
