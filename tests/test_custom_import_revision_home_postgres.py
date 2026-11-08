# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native batch homes exercise stored producers, OIDs, replay and real commits."""

from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import CustomImportChildRevision, CustomImportRootRevision
from process.custom_import.materialization import persist_scalar_projections
from process.custom_import.storage_layout import snapshot_schema
from tests.custom_import_build_scratch_postgres import (
    test_cleanup_is_atomic_pin_safe_and_preserves_retained_base_continuation as test_cleanup_is_atomic_pin_safe_and_preserves_retained_base_continuation,
)
from tests.custom_import_build_scratch_postgres import (
    test_cleanup_owner_custody_and_changed_registry_refusal as test_cleanup_owner_custody_and_changed_registry_refusal,
)
from tests.custom_import_build_scratch_postgres import (
    test_cleanup_refuses_phase_index_rebound_to_a_serving_table as test_cleanup_refuses_phase_index_rebound_to_a_serving_table,
)
from tests.custom_import_build_scratch_postgres import (
    test_cleanup_refuses_unverified_and_manual_accepted_scratch as test_cleanup_refuses_unverified_and_manual_accepted_scratch,
)
from tests.custom_import_postgres_support import (
    _migration,
    isolated_publication_case,
    lease_digest,
)
from tests.test_custom_import_materialization_postgres import (
    _definition,
    _root_projection_rows,
)
from tests.test_custom_import_materialization_set_postgres import _seed


def _install(connection, schema):
    """Also permit this additive migration to be tested before guard retirement."""
    path = (
        Path(__file__).resolve().parents[1] / "alembic/versions/20261005070000_custom_import_materialization_storage.py"
    )
    migration = _migration(path, "revision_home_native_migration")
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


@asynccontextmanager
async def _case():
    async with isolated_publication_case(migration_through="20261005060000") as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        yield case


async def _snapshot(session, case, attempt):
    await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
    return await session.scalar(
        text(f'SELECT "{case.schema_name}".create_custom_import_snapshot_family(:execution,:fence,:token)'),
        dict(execution=attempt.execution_id, fence=attempt.fence, token=lease_digest(attempt.token)),
    )


async def _copy_revisions(session, case, storage, attempt, material, roots, children=()):
    """Bounded owner fixture: fresh IDs keep the actual immutable producer row."""
    namespace = snapshot_schema(storage)
    await session.execute(
        text(
            f'INSERT INTO "{namespace}".custom_import_pack '
            f'SELECT * FROM "{case.schema_name}".custom_import_pack WHERE execution_id=:execution'
        ),
        dict(execution=attempt.execution_id),
    )
    for model, ids, original in (
        (CustomImportRootRevision, roots, material.root_revision_id),
        (CustomImportChildRevision, children, material.child_revision_ids[0]),
    ):
        key = next(iter(model.__table__.primary_key.columns)).name
        columns = ",".join(
            "ids.id" if column.name == key else f'src."{column.name}"' for column in model.__table__.columns
        )
        await session.execute(
            text(
                f'INSERT INTO "{namespace}".{model.__tablename__} '
                f'SELECT {columns} FROM "{case.schema_name}".{model.__tablename__} src '
                f'CROSS JOIN unnest(CAST(:ids AS bigint[])) ids(id) WHERE src."{key}"=:original'
            ),
            dict(ids=list(ids), original=original),
        )


async def _append(session, case, storage, roots=(), children=()):
    await session.execute(
        text(
            f'SELECT "{case.schema_name}".append_custom_import_revision_home'
            "(:storage,CAST(:roots AS bigint[]),CAST(:children AS bigint[]))"
        ),
        dict(storage=storage, roots=list(roots), children=list(children)),
    )


async def _lookup(session, case, roots=(), children=()):
    result = await session.execute(
        text(
            f'SELECT * FROM "{case.schema_name}".lookup_custom_import_revision_home'
            "(CAST(:roots AS bigint[]),CAST(:children AS bigint[])) ORDER BY revision_kind,revision_id"
        ),
        dict(roots=list(roots), children=list(children)),
    )
    return result.all()


async def test_native_homes_preserve_exact_holes_kind_domains_maximum_and_replay():
    async with _case() as case:
        async with case.sessions() as session, session.begin():
            _, graph, attempt, material = await _seed(session, uuid4().hex)
            storage = await _snapshot(session, case, attempt)
            maximum = 2**63 - 1
            await _copy_revisions(session, case, storage, attempt, material, (9001, 9003, maximum), (9001, maximum))
            await _append(session, case, storage, (9001, 9003, maximum), (9001, maximum))
            assert await _lookup(session, case, (9001, 9001, 9002, maximum), (9001, 9002, maximum)) == [
                (1, 9001, storage),
                (1, 9002, None),
                (1, maximum, storage),
                (2, 9001, storage),
                (2, 9002, None),
                (2, maximum, storage),
            ]
            assert await _lookup(session, case) == []
            await _append(session, case, storage)
            projections = _root_projection_rows(
                _definition(), graph, replace(material, root_revision_id=9001), "synthetic"
            )
            assert await persist_scalar_projections(session, _definition(), root_scalars=projections) == 1
            assert await persist_scalar_projections(session, _definition(), root_scalars=projections) == 1
            assert (
                await session.scalar(text(f'SELECT count(*) FROM "{case.schema_name}".custom_import_revision_home'))
                == 2
            )
            with pytest.raises(DBAPIError, match="exclusion|duplicate key"):
                async with session.begin_nested():
                    await _append(session, case, storage, (9001,))
        async with case.sessions() as session, session.begin():
            assert (
                await session.scalar(
                    text(f'SELECT count(*) FROM "{case.schema_name}".custom_import_materialization_page')
                )
                == 0
            )
            assert (
                await session.scalar(
                    text(f'SELECT count(*) FROM "{snapshot_schema(storage)}".custom_import_root_scalar')
                )
                == 1
            )


@pytest.mark.parametrize("failure", ("missing", "canonical", "producer", "oid", "frozen", "bounds"))
async def test_native_append_rejects_unverified_rows_and_closed_or_replaced_storage(failure):
    async with _case() as case:
        async with case.sessions() as session, session.begin():
            _, _, attempt, material = await _seed(session, uuid4().hex)
            storage = await _snapshot(session, case, attempt)
            await _copy_revisions(session, case, storage, attempt, material, (9001,))
            with pytest.raises(DBAPIError, match="revision_home_|snapshot_"):
                async with session.begin_nested():
                    ids = await _invalid_append_ids(session, case, storage, material, failure)
                    await _append(session, case, storage, ids)
            assert await _lookup(session, case, (9001,)) == [(1, 9001, None)]


async def _invalid_append_ids(session, case, storage, material, failure):
    if failure == "missing":
        return (9002,)
    if failure == "canonical":
        return (material.root_revision_id,)
    if failure == "bounds":
        return tuple(range(1, 100002))
    namespace = snapshot_schema(storage)
    mutations_by_failure = {
        "producer": f'UPDATE "{namespace}".custom_import_pack SET producing_fence=producing_fence+1',
        "oid": f'ALTER TABLE "{namespace}".custom_import_root_revision RENAME TO replaced_root',
        "frozen": f'UPDATE "{case.schema_name}".custom_import_snapshot_family SET frozen_at=clock_timestamp() WHERE family_id=:family',
    }
    await session.execute(text(mutations_by_failure[failure]), dict(family=storage))
    return (9001,)


async def test_native_registered_producer_with_missing_home_cannot_fall_back_to_canonical():
    async with _case() as case:
        async with case.sessions() as session, session.begin():
            _, graph, attempt, material = await _seed(session, uuid4().hex)
            await _snapshot(session, case, attempt)
            rows = _root_projection_rows(_definition(), graph, material, "synthetic")
            with pytest.raises(DBAPIError, match="materialization_deadline|materialization_authority_lost"):
                async with session.begin_nested():
                    await persist_scalar_projections(session, _definition(), root_scalars=rows)


async def test_native_materialization_functions_have_no_nonowner_execute_acl():
    async with _case() as case:
        async with case.sessions() as session, session.begin():
            assert not await session.scalar(
                text(
                    "SELECT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace, "
                    "LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a "
                    "WHERE n.nspname=:schema AND p.proname IN "
                    "('append_custom_import_revision_home','lookup_custom_import_revision_home',"
                    "'custom_import_materialization_origins','install_custom_import_materialization_writers') "
                    "AND a.grantee<>p.proowner)"
                ),
                dict(schema=case.schema_name),
            )


async def test_inventory_supports_two_atomic_retirements_on_one_backend(monkeypatch):
    from process.custom_import import retirement, retirement_inventory
    from tests import custom_import_retirement_postgres as retirement_fixture
    from tests.test_custom_import_snapshot_storage_postgres import (
        _bounded,
        _sealed_current_snapshot,
    )

    async with isolated_publication_case() as case, case.engine.connect() as connection:
        _, _, first = await retirement_fixture._superseded_snapshot(case)
        _, _, second = await retirement_fixture._superseded_snapshot(case)
        _, _, kept = await _sealed_current_snapshot(case)
        try:
            async with AsyncSession(bind=connection) as session, session.begin():
                await _bounded(session)
                backend = await session.scalar(text("SELECT pg_backend_pid()"))
                await retirement_inventory.prepare_frozen_reference_inventory(session)
            original = retirement._revision_references
            scans = []

            async def record_reference_scan(session, family, *args, **kwargs):
                scans.append((family.family_id, kwargs.get("include_canonical", True)))
                await original(session, family, *args, **kwargs)

            monkeypatch.setattr(retirement, "_revision_references", record_reference_scan)
            for family_id in (first, second):
                async with AsyncSession(bind=connection) as session, session.begin():
                    await _bounded(session)
                    receipt = await retirement.retire_snapshot_family(
                        session,
                        family_id,
                        engine_id="synthetic",
                        assert_external_unreferenced=retirement_fixture._published_fixture_check,
                    )
                    assert receipt.reason == "superseded" and not receipt.replayed
                    assert await session.scalar(text("SELECT pg_backend_pid()")) == backend
            assert scans == [(first, False), (first, True), (second, False), (second, True)]
            async with AsyncSession(bind=connection) as session, session.begin():
                assert (
                    await session.scalar(text("SELECT to_regnamespace(:name)"), {"name": snapshot_schema(kept)})
                    is not None
                )
        finally:
            if connection.in_transaction():
                await connection.rollback()
            async with AsyncSession(bind=connection) as session, session.begin():
                await retirement_inventory.cleanup_frozen_reference_inventory(session)
