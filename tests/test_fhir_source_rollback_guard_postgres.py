# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native data-only header inverse; full rollback CAS and pin checks are separate."""

import json

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from db import migration_fhir_source_rollback as rollback
from db.plan_release_storage import storage_identifier
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding
from tests.test_cms_noncurrent_seal_guard_postgres import _seal, _seed
from tests.test_cms_noncurrent_seal_guard_postgres import header_case as header_case
from tests.test_provider_directory_cms_prepared_context_postgres import native_input_resources


@pytest_asyncio.fixture
async def native_case():
    """Use real native roles and qualified storage names for the data-only inverse."""
    async with native_input_resources() as case:
        case.storage = FHIRSourceStorageBinding("accepted_catalog", "private_source")
        yield case


async def _install_inverse(case):
    engine = create_async_engine(case.url.set(drivername="postgresql+asyncpg"))
    try:
        async with engine.begin() as connection:
            await connection.run_sync(rollback.upgrade_fhir_source_rollback_guard, case.storage.catalog_schema)
    finally:
        await engine.dispose()


async def _superseded(case, *, cms=True):
    await _install_inverse(case)
    metadata_update = None if cms else lambda metadata: metadata.update(source_ids=["ordinary-source"])
    await _seed(case, metadata_update=metadata_update)
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


async def _restore(case, *, changes=None, connection=None):
    expressions_by_column = {"status": "'published'", "is_current": "true", "superseded_at": "NULL", **(changes or {})}
    assignments = ",".join(
        storage_identifier(column) + "=" + expression for column, expression in expressions_by_column.items()
    )
    await (connection or case.admin).execute(f"UPDATE {case.header} SET {assignments} WHERE dataset_id='candidate'")


@pytest.mark.parametrize("cms", [False, True], ids=["ordinary", "cms"])
@pytest.mark.parametrize("isolation", ["serializable", "read_committed"])
async def test_exact_inverse_preserves_immutable_header_and_original_publication(header_case, cms, isolation):
    case = header_case
    await _superseded(case, cms=cms)
    before = await case.admin.fetchval(f"SELECT to_jsonb(h) FROM {case.header} h")
    await _install_inverse(case)
    assert await case.admin.fetchval(f"SELECT to_jsonb(h) FROM {case.header} h") == before
    async with case.admin.transaction(isolation=isolation):
        await _restore(case)
    restored = await case.admin.fetchval(f"SELECT to_jsonb(h) FROM {case.header} h")
    assert json.loads(restored) == {
        **json.loads(before),
        "status": "published",
        "is_current": True,
        "superseded_at": None,
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "'failed'"},
        {"is_current": "false"},
        {"superseded_at": "transaction_timestamp()"},
        {"published_at": "published_at+interval '1 second'"},
        {"resource_count": "resource_count+1"},
        {"dataset_hash": "repeat('f',64)"},
        {"dataset_id": "'other-dataset'"},
        {"endpoint_id": "'other-endpoint'"},
        {"validated_at": "validated_at+interval '1 second'"},
        {"publication_metadata_json": "publication_metadata_json::jsonb||'{\"drift\":true}'::jsonb"},
    ],
    ids=[
        "status",
        "current",
        "superseded-time",
        "publication-time",
        "count",
        "hash",
        "dataset",
        "endpoint",
        "validation-time",
        "metadata",
    ],
)
async def test_inverse_rejects_every_change_outside_its_exact_lifecycle(header_case, changes):
    case = header_case
    await _superseded(case)
    before = await case.admin.fetchval(f"SELECT to_jsonb(h) FROM {case.header} h")
    with pytest.raises(asyncpg.PostgresError):
        async with case.admin.transaction(isolation="serializable"):
            await _restore(case, changes=changes)
    assert await case.admin.fetchval(f"SELECT to_jsonb(h) FROM {case.header} h") == before


async def test_outer_failure_preserves_the_superseded_edition(header_case):
    case = header_case
    await _superseded(case)
    before = await case.admin.fetchval(f"SELECT to_jsonb(h) FROM {case.header} h")
    with pytest.raises(RuntimeError, match="after inverse"):
        async with case.admin.transaction(isolation="serializable"):
            await _restore(case)
            raise RuntimeError("after inverse")
    assert await case.admin.fetchval(f"SELECT to_jsonb(h) FROM {case.header} h") == before


@pytest.mark.parametrize("assumed", [False, True], ids=["login", "set-role"])
@pytest.mark.parametrize("can_update", [False, True], ids=["read-only", "existing-writer"])
async def test_actual_postgresql_update_privilege_controls_the_inverse(header_case, assumed, can_update):
    case = header_case
    await _superseded(case)
    role = storage_identifier(case.roles["prepared_publisher"])
    await case.admin.execute(f"GRANT SELECT ON {case.header} TO {role}")
    if can_update:
        await case.admin.execute(f"GRANT UPDATE(status,is_current,superseded_at) ON {case.header} TO {role}")
    connection = case.admin
    if not assumed:
        connection = await asyncpg.connect(
            case.url.set(username=case.roles["prepared_publisher"], password=case.password).render_as_string(
                hide_password=False
            )
        )
    try:
        async with connection.transaction(isolation="serializable"):
            if assumed:
                await connection.execute(f"SET LOCAL ROLE {role}")
            await _restore_with_privilege(case, connection, can_update)
    finally:
        if connection is not case.admin:
            await connection.close()
    assert await case.admin.fetchval(f"SELECT status FROM {case.header}") == (
        "published" if can_update else "superseded"
    )


async def _restore_with_privilege(case, connection, can_update):
    """Use PostgreSQL's existing column grant, preserving an aborted-write savepoint."""
    if can_update:
        await _restore(case, connection=connection)
        return
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction():
            await _restore(case, connection=connection)
