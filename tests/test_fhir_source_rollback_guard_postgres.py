# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real guard component checks; sealed-context/native-pointer proof is separate."""

import json

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from db import migration_fhir_source_rollback as rollback
from tests.test_cms_noncurrent_seal_guard_postgres import (
    _metadata,
    _seal,
    _seed,
    _upgrade,
)
from tests.test_cms_noncurrent_seal_guard_postgres import (
    header_case as header_case,
)
from tests.test_provider_directory_cms_prepared_context_postgres import native_case as native_case


async def _install_inverse(case):
    engine = create_async_engine(case.url.set(drivername="postgresql+asyncpg"))
    try:
        async with engine.begin() as connection:
            await connection.exec_driver_sql("SET LOCAL search_path=pg_catalog,pg_temp")
            await connection.run_sync(
                lambda sync: rollback.upgrade_fhir_source_rollback_guard(
                    sync, case.storage.catalog_schema, expected=case.guard
                )
            )
    finally:
        await engine.dispose()


async def _superseded(case, *, cms=True):
    if cms:
        await _upgrade(case)
        await _seed(case)
    else:
        assert (
            await case.admin.fetchval("SELECT prosrc FROM pg_proc WHERE oid=$1", case.guard["oid"])
            == (rollback.rollback_guard_bodies(case.storage.catalog_schema)[0])
        )
        await _seed(case, metadata_update=lambda metadata: metadata.update(source_ids=["ordinary-source"]))
    async with case.admin.transaction():
        if cms:
            await _seal(case)
            await case.admin.execute(f"UPDATE {case.header} SET is_current=true WHERE dataset_id='candidate'")
        else:
            await case.admin.execute(
                f"UPDATE {case.header} SET status='published',is_current=true,published_at=transaction_timestamp() "
                "WHERE dataset_id='candidate'"
            )
    async with case.admin.transaction():
        await case.admin.execute(
            f"UPDATE {case.header} SET status='superseded',is_current=false,superseded_at=transaction_timestamp() "
            "WHERE dataset_id='candidate'"
        )


async def _context(case, *, payload_changed=False, connection=None):
    changes = '{"status":"published","is_current":true,"superseded_at":null}'
    changed_count = ", 'resource_count', h.resource_count+1" if payload_changed else ""
    return json.loads(
        await (connection or case.admin).fetchval(
            "SELECT jsonb_build_object('contract',$1::text,'database_oid',"
            "(SELECT oid::text FROM pg_database WHERE datname=current_database()),"
            "'relation_oid',h.tableoid::text,'backend_pid',pg_backend_pid()::text,"
            "'transaction_id',pg_current_xact_id()::text,'dataset_id',h.dataset_id,'endpoint_id',h.endpoint_id,"
            "'context_sha256',$2::text,'old_sha256',encode(sha256(convert_to(to_jsonb(h)::text,'UTF8')),'hex'),"
            "'new_sha256',encode(sha256(convert_to((to_jsonb(h)||$3::jsonb||jsonb_build_object("
            "'dataset_id',h.dataset_id" + changed_count + "))::text,'UTF8')),'hex')) "
            f"FROM {case.header} h WHERE dataset_id='candidate'",
            rollback.CONTRACT,
            "a" * 64,
            changes,
        )
    )


async def _restore(case, *, payload_changed=False, connection=None):
    extra = ",resource_count=resource_count+1" if payload_changed else ""
    await (connection or case.admin).execute(
        f"UPDATE {case.header} SET status='published',is_current=true,superseded_at=NULL{extra} "
        "WHERE dataset_id='candidate'"
    )


def _mutate_context(context, fault):
    changes_by_fault = {
        "transaction": ("transaction_id", "1"),
        "before": ("old_sha256", "0" * 64),
        "after": ("new_sha256", "0" * 64),
        "relation": ("relation_oid", "0"),
        "extra": ("unrecognized", True),
        "backend": ("backend_pid", "mismatch"),
        "database": ("database_oid", "mismatch"),
        "endpoint": ("endpoint_id", "mismatch"),
        "context_digest": ("context_sha256", "mismatch"),
    }
    if fault in changes_by_fault:
        key, replacement = changes_by_fault[fault]
        context[key] = replacement


def _encoded_context(context, fault):
    if fault == "json":
        return "{"
    if fault == "large":
        return "x" * 4097
    return json.dumps(context)


@pytest.mark.parametrize(
    ("cms", "fault"),
    [
        *[
            pytest.param(True, fault, id=str(fault))
            for fault in (
                None,
                "missing",
                "json",
                "transaction",
                "before",
                "after",
                "relation",
                "extra",
                "payload",
                "backend",
                "database",
                "endpoint",
                "context_digest",
                "large",
                "read_committed",
            )
        ],
        pytest.param(False, None, id="original-success"),
        pytest.param(False, "missing", id="original-missing-context"),
    ],
)
async def test_exact_inverse_and_fail_closed_context(header_case, cms, fault):
    case = header_case
    await _superseded(case, cms=cms)
    before = await _metadata(case)
    async with case.admin.transaction(isolation="serializable"):
        with pytest.raises(asyncpg.PostgresError, match="endpoint_dataset_transition_invalid"):
            async with case.admin.transaction():
                await _restore(case)
    await _install_inverse(case)
    assert await _metadata(case) == before
    assert (
        await case.admin.fetchval("SELECT prosrc FROM pg_proc WHERE oid=$1", case.guard["oid"])
        == (rollback.rollback_guard_bodies(case.storage.catalog_schema)[3 if cms else 2])
    )
    installed = await case.admin.fetchval(
        "SELECT jsonb_build_object('function',to_jsonb(p),'xmin',p.xmin::text,'ctid',p.ctid::text) "
        "FROM pg_proc p WHERE p.oid=$1",
        case.guard["oid"],
    )
    await _install_inverse(case)
    assert (
        await case.admin.fetchval(
            "SELECT jsonb_build_object('function',to_jsonb(p),'xmin',p.xmin::text,'ctid',p.ctid::text) "
            "FROM pg_proc p WHERE p.oid=$1",
            case.guard["oid"],
        )
        == installed
    )
    assert await _metadata(case) == before
    isolation = "read_committed" if fault == "read_committed" else "serializable"
    async with case.admin.transaction(isolation=isolation):
        context = await _context(case, payload_changed=fault == "payload")
        _mutate_context(context, fault)
        if fault != "missing":
            await case.admin.execute(
                "SELECT set_config($1,$2,true)", rollback.SELECTOR, _encoded_context(context, fault)
            )
        if fault is None:
            await _restore(case)
        else:
            with pytest.raises(asyncpg.PostgresError, match="endpoint_dataset_transition_invalid"):
                async with case.admin.transaction():
                    await _restore(case, payload_changed=fault == "payload")
    header_by_field = await case.admin.fetchrow(f"SELECT status,is_current,resource_count FROM {case.header}")
    assert dict(header_by_field) == {
        "status": "published" if fault is None else "superseded",
        "is_current": fault is None,
        "resource_count": 8,
    }


async def test_outer_failure_preserves_the_superseded_edition(header_case):
    case = header_case
    await _superseded(case)
    await _install_inverse(case)
    before = await case.admin.fetchval(f"SELECT to_jsonb(h) FROM {case.header} h")
    with pytest.raises(RuntimeError, match="after inverse"):
        async with case.admin.transaction(isolation="serializable"):
            await case.admin.execute(
                "SELECT set_config($1,$2,true)", rollback.SELECTOR, json.dumps(await _context(case))
            )
            await _restore(case)
            raise RuntimeError("after inverse")
    assert await case.admin.fetchval(f"SELECT to_jsonb(h) FROM {case.header} h") == before


@pytest.mark.parametrize("actor", ["genuine_publisher", "assumed_publisher"])
async def test_ordinary_actor_cannot_activate_the_admin_context(header_case, actor):
    from db.plan_release_storage import storage_identifier

    case = header_case
    await _superseded(case)
    await _install_inverse(case)
    role = storage_identifier(case.roles["prepared_publisher"])
    await case.admin.execute(f"GRANT SELECT,UPDATE ON {case.header} TO {role}")
    connection = case.admin
    if actor == "genuine_publisher":
        connection = await asyncpg.connect(
            case.url.set(username=case.roles["prepared_publisher"], password=case.password).render_as_string(
                hide_password=False
            )
        )
    try:
        async with connection.transaction(isolation="serializable"):
            if actor == "assumed_publisher":
                await connection.execute(f"SET LOCAL ROLE {role}")
            context = await _context(case, connection=connection)
            await connection.execute("SELECT set_config($1,$2,true)", rollback.SELECTOR, json.dumps(context))
            with pytest.raises(asyncpg.PostgresError, match="endpoint_dataset_transition_invalid"):
                async with connection.transaction():
                    await _restore(case, connection=connection)
    finally:
        if connection is not case.admin:
            await connection.close()
    assert await case.admin.fetchval(f"SELECT status FROM {case.header}") == "superseded"
