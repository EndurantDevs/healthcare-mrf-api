# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native predecessor rotation component; not enrolled-runtime publication proof."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import cms_doctors_immutable as immutable
from process import provider_directory_generic_source_policy as source
from process import reference_family_archive as archive
from tests.cms_doctors_preparation_postgres_support import doctors_database


async def _incoming_family(session, component):
    ownership = await archive.precreate_reference_family_restore(
        session,
        importer_id="cms-doctors",
        dataset_id=uuid4(),
    )
    for model in immutable._models():
        name = str(model.__tablename__)
        columns = tuple(str(column.name) for column in model.__table__.columns)
        query = "SELECT " + ",".join('"' + name + '"' for name in columns)
        query += f' FROM "{component.policy.schema}"."{name}"'
        await archive.native_copy_projection(
            session,
            query,
            schema_name=ownership.schema_name,
            table_name=name,
            columns=columns,
            max_bytes=1024 * 1024,
            timeout=30,
        )
    await archive.complete_reference_family_restore(session, ownership)
    await archive.seal_model_family_storage(session, ownership, await immutable._writer(session))
    return ownership


async def _assert_bound_owners(native, component, pid):
    assert await native.fetchval("SELECT pg_backend_pid()") == pid
    assert dict(await native.fetchrow("SELECT session_user::text s,current_user::text c")) == {
        "s": component.publisher,
        "c": component.owner,
    }
    assert not await native.fetchval("SELECT has_database_privilege(current_user,current_database(),'CREATE')")
    assert await native.fetchval(
        "SELECT pg_has_role($1,$2,'SET') AND pg_has_role($1,$3,'USAGE') AND NOT pg_has_role($1,$3,'SET')",
        component.retained_publisher,
        component.retention_owner,
        component.owner,
    )


async def _assert_rotation(session, component, incoming, incumbent, predecessor):
    native = source.bound_publisher().connection
    assert await native.fetchval(
        "SELECT nspowner=$2::regrole FROM pg_namespace WHERE nspname=$1",
        predecessor,
        component.owner,
    )
    current = await archive.capture_reference_family_incumbent(
        session,
        importer_id="cms-doctors",
        schema_name=component.policy.schema,
    )
    prior = await archive.capture_reference_family_incumbent(
        session,
        importer_id="cms-doctors",
        schema_name=predecessor,
    )
    assert dict(current.relation_oids) == dict(incoming.relation_oids)
    assert dict(prior.relation_oids) == dict(incumbent.relation_oids)
    all_oids = [oid for _, oid in (*incoming.relation_oids, *incumbent.relation_oids)]
    assert len(set(all_oids)) == 6
    assert await native.fetchval(
        "SELECT bool_and(relowner=$2::regrole) FROM pg_class WHERE oid=ANY($1::oid[])",
        all_oids,
        component.owner,
    )
    assert not await native.fetchval("SELECT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=ANY($1::oid[]))", all_oids)


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", [False, True])
async def test_doctors_predecessor_rotation_and_rollback_keep_three_exact_oids(monkeypatch, rollback):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component = fixture.component
        async with component.database.transaction() as session:
            native = source.bound_publisher().connection
            pid = await native.fetchval("SELECT pg_backend_pid()")
            await _assert_bound_owners(native, component, pid)
            incoming = await _incoming_family(session, component)
            incumbent = await archive.capture_reference_family_incumbent(
                session,
                importer_id="cms-doctors",
                schema_name=component.policy.schema,
            )
            predecessor = archive.reference_family_predecessor_schema(incoming.dataset_id)
            spec = archive.reference_family_spec("cms-doctors")
            if rollback:
                with pytest.raises(RuntimeError, match="reject completed rotation"):
                    await _reject_rotation(session, component, incoming, incumbent, predecessor)
                assert await native.fetchval("SELECT to_regnamespace($1)", predecessor) is None
                await archive._verify_incumbent(session, incumbent)
                await archive.verify_reference_family_stage_ownership(session, incoming)
                await archive.cleanup_reference_family_stage(session, incoming)
            else:
                assert await archive._rotate_family_relations(session, spec, incoming, incumbent) == predecessor
                await _assert_rotation(session, component, incoming, incumbent, predecessor)
                for name, oid in incumbent.relation_oids:
                    assert await native.fetchval("SELECT to_regclass($1)::oid", f'"{predecessor}"."{name}"') == oid
                    await session.execute(text(f'DROP TABLE "{predecessor}"."{name}" RESTRICT'))
                await session.execute(text(f'DROP SCHEMA "{predecessor}" RESTRICT'))
                await archive._drop_empty_stage_schema(session, incoming)
            await _assert_bound_owners(native, component, pid)
            assert session is source.source_copy_session() and await native.fetchval("SELECT 17") == 17


async def _reject_rotation(session, component, incoming, incumbent, predecessor):
    async with session.begin_nested():
        assert (
            await archive._rotate_family_relations(
                session,
                archive.reference_family_spec("cms-doctors"),
                incoming,
                incumbent,
            )
            == predecessor
        )
        await _assert_rotation(session, component, incoming, incumbent, predecessor)
        raise RuntimeError("reject completed rotation")


@pytest.mark.asyncio
async def test_doctors_predecessor_collision_preserves_original_three_heaps(monkeypatch):
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        component = fixture.component
        async with component.database.transaction() as session:
            incoming = await _incoming_family(session, component)
            incumbent = await archive.capture_reference_family_incumbent(
                session,
                importer_id="cms-doctors",
                schema_name=component.policy.schema,
            )
            predecessor = archive.reference_family_predecessor_schema(incoming.dataset_id)
            async with source.source_publisher_body(session):
                await session.execute(text(f'CREATE SCHEMA "{predecessor}" AUTHORIZATION "{component.owner}"'))
            oid = await session.scalar(text("SELECT to_regnamespace(:schema)::oid"), {"schema": predecessor})
            with pytest.raises(DBAPIError) as collision:
                await archive._rotate_family_relations(
                    session,
                    archive.reference_family_spec("cms-doctors"),
                    incoming,
                    incumbent,
                )
            assert collision.value.orig.sqlstate == "42P06"
            assert await session.scalar(text("SELECT to_regnamespace(:schema)::oid"), {"schema": predecessor}) == oid
            await archive._verify_incumbent(session, incumbent)
            await archive.verify_reference_family_stage_ownership(session, incoming)
            await archive.cleanup_reference_family_stage(session, incoming)
            await session.execute(text(f'DROP SCHEMA "{predecessor}" RESTRICT'))
