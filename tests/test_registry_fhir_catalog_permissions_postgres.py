# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A singleton-key grant permits publication locks but no revision changes."""

import asyncio

import asyncpg
import pytest

from process.network_membership_publication import _lock_publication_controls
from process.registry_fhir_catalog_permissions import _verify_revision_lock
from tests.test_registry_fhir_catalog_custody_postgres import metadata_custody as metadata_custody


@pytest.fixture
async def revision_lock(metadata_custody):
    fixture = metadata_custody
    namespace = '"' + fixture.control_schema + '"'
    await fixture.connection.execute(f"""
      CREATE TABLE {namespace}.registry_revision_control(
        id integer PRIMARY KEY CHECK(id=1),draft_revision bigint NOT NULL,approved_revision bigint NOT NULL);
      INSERT INTO {namespace}.registry_revision_control VALUES(1,6,4);
      CREATE TABLE {namespace}.network_serving_control(
        id integer PRIMARY KEY CHECK(id=1),generation_id bigint,metadata_generation text);
      INSERT INTO {namespace}.network_serving_control VALUES(1,9,NULL)
    """)
    role = '"' + fixture.roles["reader"] + '"'
    await fixture.connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {role}")
    await fixture.connection.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {namespace} TO {role}")
    await fixture.connection.execute(f"GRANT UPDATE(id) ON {namespace}.registry_revision_control TO {role}")
    await fixture.connection.execute(
        f"GRANT UPDATE(metadata_generation) ON {namespace}.network_serving_control TO {role}"
    )
    fixture.namespace = namespace
    return fixture


@pytest.mark.asyncio
async def test_lock_capability_cannot_change_singleton_or_revisions(revision_lock):
    fixture = revision_lock
    async with fixture.reader.transaction():
        await _verify_revision_lock(fixture.reader, fixture.control_schema)
        assert await _lock_publication_controls(fixture.reader, fixture.namespace) == (4, 9)
        await fixture.reader.execute(f"UPDATE {fixture.namespace}.registry_revision_control SET id=1")
    for expression, error in (
        ("draft_revision=7", asyncpg.InsufficientPrivilegeError),
        ("approved_revision=5", asyncpg.InsufficientPrivilegeError),
        ("id=2", asyncpg.CheckViolationError),
        ("id=NULL", asyncpg.NotNullViolationError),
    ):
        with pytest.raises(error):
            async with fixture.reader.transaction():
                await fixture.reader.execute(f"UPDATE {fixture.namespace}.registry_revision_control SET {expression}")
    assert tuple(await fixture.reader.fetchrow(f"SELECT * FROM {fixture.namespace}.registry_revision_control")) == (
        1,
        6,
        4,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["rename", "missing", "weakened", "unvalidated", "rule"])
async def test_lock_capability_requires_enforced_native_singleton(revision_lock, damage):
    fixture = revision_lock
    relation = fixture.namespace + ".registry_revision_control"
    if damage == "rule":
        await fixture.connection.execute(f"CREATE RULE ignore_update AS ON UPDATE TO {relation} DO INSTEAD NOTHING")
    elif damage == "rename":
        await fixture.connection.execute(
            f"ALTER TABLE {relation} RENAME CONSTRAINT registry_revision_control_id_check TO renamed_singleton"
        )
    else:
        await fixture.connection.execute(f"ALTER TABLE {relation} DROP CONSTRAINT registry_revision_control_id_check")
        if damage in {"weakened", "unvalidated"}:
            predicate = "id>=1" if damage == "weakened" else "id=1"
            await fixture.connection.execute(f"ALTER TABLE {relation} ADD CHECK ({predicate}) NOT VALID")
    if damage == "rename":
        await _verify_revision_lock(fixture.reader, fixture.control_schema)
    else:
        with pytest.raises(ValueError, match="revision_lock_invalid"):
            await _verify_revision_lock(fixture.reader, fixture.control_schema)


@pytest.mark.asyncio
async def test_existing_lock_order_serializes_approval_and_rejects_stale_snapshot(revision_lock):
    fixture = revision_lock
    relation = fixture.namespace + ".registry_revision_control"
    transaction = fixture.reader.transaction(isolation="repeatable_read")
    await transaction.start()
    writer = None
    try:
        await _lock_publication_controls(fixture.reader, fixture.namespace)
        writer = asyncio.create_task(fixture.connection.execute(f"UPDATE {relation} SET approved_revision=5"))
        await asyncio.sleep(0.03)
        assert not writer.done()
        await transaction.commit()
        assert await asyncio.wait_for(writer, 3) == "UPDATE 1"
    finally:
        if fixture.reader.is_in_transaction():
            await transaction.rollback()
        if writer is not None:
            await asyncio.gather(writer, return_exceptions=True)
    async with fixture.reader.transaction(isolation="repeatable_read"):
        assert await fixture.reader.fetchval(f"SELECT approved_revision FROM {relation}") == 5
        await fixture.connection.execute(f"UPDATE {relation} SET approved_revision=6")
        with pytest.raises(asyncpg.SerializationError):
            await _lock_publication_controls(fixture.reader, fixture.namespace)


@pytest.mark.asyncio
async def test_readonly_reader_cannot_take_a_publication_lock_or_change_revisions(revision_lock):
    fixture = revision_lock
    before = await fixture.reader.fetchrow(f"SELECT * FROM {fixture.namespace}.registry_revision_control")
    with pytest.raises(asyncpg.ReadOnlySQLTransactionError):
        async with fixture.reader.transaction(readonly=True):
            await _lock_publication_controls(fixture.reader, fixture.namespace)
    assert await fixture.reader.fetchrow(f"SELECT * FROM {fixture.namespace}.registry_revision_control") == before
