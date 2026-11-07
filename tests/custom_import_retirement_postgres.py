# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native retirement proofs delegated through the existing source CI entrypoint."""

import asyncio
import uuid
from copy import deepcopy
from types import SimpleNamespace

import pytest
from sqlalchemy import BigInteger, Column, MetaData, Table, select, text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import_storage import CustomImportSnapshotFamily, CustomImportSnapshotRetirement
from process.custom_import import publication, retirement
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_snapshot_storage_postgres import (
    _attempt,
    _bounded,
    _call,
    _create_snapshot,
    _external_fixture_check,
    _install_retirement_with_application_resolver_owner,
    _ordinary_retirement_roles,
    _read_binding,
    _refuse_actual_retirement_login,
    _remember_retirement,
    _replaced_fence,
    _sealed_current_snapshot,
    _wait_for_registry_pin,
)


async def test_retired_read_is_refused_before_legacy_fallback_without_reader_grants():
    async with isolated_publication_case() as case:
        graph, attempt, family_id = await _sealed_current_snapshot(case)
        role = "retirement_reader_" + uuid.uuid4().hex[:12]
        try:
            async with case.sessions() as session, session.begin():
                await _remember_retirement(session, family_id)
                await session.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
                await session.execute(text(f'GRANT USAGE ON SCHEMA "{case.schema_name}" TO "{role}"'))
                await session.execute(
                    text(
                        f'GRANT EXECUTE ON FUNCTION "{case.schema_name}".'
                        f'resolve_custom_import_generation_snapshot(bigint,bigint,bigint,bigint) TO "{role}"'
                    )
                )
            with pytest.raises(DBAPIError, match="permission denied"):
                async with case.sessions() as session, session.begin():
                    await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                    await session.execute(select(CustomImportSnapshotRetirement))
            with pytest.raises(DBAPIError, match="custom_import_snapshot_retired"):
                async with case.sessions() as session, session.begin():
                    await session.execute(text("SET TRANSACTION READ ONLY"))
                    await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                    await _read_binding(session, case, graph, attempt.generation_id)
        finally:
            async with case.sessions() as session, session.begin():
                await session.execute(text(f'DROP OWNED BY "{role}"'))
                await session.execute(text(f'DROP ROLE "{role}"'))


async def test_retired_producer_cannot_recreate_or_resume_the_same_fence():
    async with isolated_publication_case() as case:
        _, attempt = await _attempt(case)
        family_id = await _create_snapshot(case, attempt)
        async with case.sessions() as session, session.begin():
            await _remember_retirement(session, family_id, generation=False)
        for entry in ("create_custom_import_snapshot_family", "lock_custom_import_writable_snapshot"):
            with pytest.raises(DBAPIError, match="custom_import_snapshot_retired"):
                async with case.sessions() as session, session.begin():
                    await _call(session, case, attempt, entry)


async def test_retirement_migration_preserves_existing_function_metadata_and_legacy_reads():
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    from tests.custom_import_postgres_support import (
        _ROOT,
        _migration,
        install_snapshot_retirement_migration,
        seed_publication_graph,
    )

    async with isolated_publication_case() as case:
        async with case.sessions() as session, session.begin():
            graph = await seed_publication_graph(session)
            assert await _read_binding(session, case, graph, graph.first_generation_id) is None
        query = text(
            "SELECT p.oid,to_jsonb(p)-'prosrc' metadata FROM pg_proc p JOIN pg_namespace n "
            "ON n.oid=p.pronamespace WHERE n.nspname=:schema AND p.proname IN "
            "('resolve_custom_import_generation_snapshot','lock_custom_import_snapshot_attempt') ORDER BY p.oid"
        )
        async with case.engine.begin() as connection:
            before = (await connection.execute(query, {"schema": case.schema_name})).all()

            def downgrade(sync):
                module = _migration(
                    _ROOT / "alembic/versions/20261102010000_custom_import_snapshot_retirement.py",
                    "retirement_downgrade_test",
                )
                module._schema = lambda: case.schema_name
                module.op = Operations(MigrationContext.configure(sync))
                module.downgrade()

            await connection.run_sync(downgrade)
            await connection.run_sync(install_snapshot_retirement_migration, case.schema_name)
            after = (await connection.execute(query, {"schema": case.schema_name})).all()
            assert after == before


async def test_actual_api_owner_and_worker_logins_cannot_retire_storage():
    async with isolated_publication_case() as case, _ordinary_retirement_roles(case) as (roles, password):
        for role in roles:
            await _refuse_actual_retirement_login(case, role, password)


async def test_application_owned_definer_reads_retirement_marker_without_new_grants():
    async with isolated_publication_case() as case:
        graph, attempt, family_id = await _sealed_current_snapshot(case)
        async with _ordinary_retirement_roles(case) as (roles, _password):
            await _install_retirement_with_application_resolver_owner(case, roles[0])
            async with case.sessions() as session, session.begin():
                await _remember_retirement(session, family_id)
            with pytest.raises(DBAPIError, match="custom_import_snapshot_retired"):
                async with case.sessions() as session, session.begin():
                    await _read_binding(session, case, graph, attempt.generation_id)


async def test_retirement_removes_only_replaced_storage_and_replays_exactly():
    async with isolated_publication_case() as case:
        attempt, family_id = await _replaced_fence(case)
        async with case.sessions() as session, session.begin():
            await _bounded(session)
            result = await retirement.retire_snapshot_family(
                session,
                family_id,
                engine_id="synthetic",
                assert_external_unreferenced=_external_fixture_check,
            )
            assert result.reason == "replaced_fence" and not result.replayed
            assert await session.get(CustomImportSnapshotFamily, family_id) is not None
            assert await session.get(CustomImportSnapshotRetirement, family_id) is not None
        async with case.sessions() as session, session.begin():
            await _bounded(session)
            replay = await retirement.retire_snapshot_family(
                session,
                family_id,
                engine_id="synthetic",
                assert_external_unreferenced=_external_fixture_check,
            )
            assert replay.replayed and replay.retained_bytes == result.retained_bytes
        with pytest.raises(DBAPIError, match="custom_import_snapshot_retired"):
            async with case.sessions() as session, session.begin():
                await _call(session, case, attempt, "create_custom_import_snapshot_family")


async def test_retirement_rolls_back_native_drops_and_tombstone_on_interruption(monkeypatch):
    async with isolated_publication_case() as case:
        _, family_id = await _replaced_fence(case)
        original = retirement._physical_drop

        async def interrupted(*args):
            await original(*args)
            raise ConnectionError("synthetic retirement acknowledgement interruption")

        with monkeypatch.context() as patch:
            patch.setattr(retirement, "_physical_drop", interrupted)
            with pytest.raises(ConnectionError, match="acknowledgement interruption"):
                async with case.sessions() as session, session.begin():
                    await _bounded(session)
                    await retirement.retire_snapshot_family(
                        session,
                        family_id,
                        engine_id="synthetic",
                        assert_external_unreferenced=_external_fixture_check,
                    )
        async with case.sessions() as session, session.begin():
            assert await session.get(CustomImportSnapshotFamily, family_id) is not None
            assert await session.get(CustomImportSnapshotRetirement, family_id) is None
            assert (
                await session.scalar(
                    text("SELECT to_regnamespace(:namespace)"), {"namespace": retirement.snapshot_schema(family_id)}
                )
                is not None
            )


async def _superseded_snapshot(case):
    graph, attempt, family_id = await _sealed_current_snapshot(case)
    previous, version = None, 0
    for operation, generation_id in (
        (publication.activate_generation, attempt.generation_id),
        (publication.rollback_generation, graph.first_generation_id),
        (publication.activate_generation, graph.second_generation_id),
    ):
        async with case.sessions() as session, session.begin():
            await operation(
                session,
                dataset_id=graph.dataset_id,
                target_generation_id=generation_id,
                expected_generation_id=previous,
                expected_pointer_version=version,
            )
        previous, version = generation_id, version + 1
    return graph, attempt, family_id


async def test_retirement_refuses_reader_pins_and_external_native_dependents():
    async with isolated_publication_case() as case:
        graph, attempt, family_id = await _superseded_snapshot(case)
        async with case.sessions() as reader, reader.begin():
            await reader.execute(text("SET TRANSACTION READ ONLY"))
            assert await _read_binding(reader, case, graph, attempt.generation_id) == family_id
            with pytest.raises(DBAPIError, match="lock"):
                async with case.sessions() as session, session.begin():
                    await _bounded(session)
                    await retirement.retire_snapshot_family(
                        session,
                        family_id,
                        engine_id="synthetic",
                        assert_external_unreferenced=_published_fixture_check,
                    )
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(
                    f'CREATE VIEW "{case.schema_name}".external_snapshot_reader AS '
                    f'SELECT * FROM "{retirement.snapshot_schema(family_id)}".custom_import_root_record'
                )
            )
        with pytest.raises(DBAPIError, match="depend"):
            async with case.sessions() as session, session.begin():
                await _bounded(session)
                await retirement.retire_snapshot_family(
                    session,
                    family_id,
                    engine_id="synthetic",
                    assert_external_unreferenced=_published_fixture_check,
                )
        async with case.sessions() as session, session.begin():
            assert await session.get(CustomImportSnapshotFamily, family_id) is not None
            assert await session.get(CustomImportSnapshotRetirement, family_id) is None


async def test_reference_proof_checks_wrong_parent_membership_and_every_bounded_page(monkeypatch):
    async with isolated_publication_case() as case:
        _, family_id = await _replaced_fence(case)
        other_graph, _other_attempt, other_family = await _sealed_current_snapshot(case)
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(
                    f'INSERT INTO "{case.schema_name}".custom_import_revision_home '
                    "(family_id,revision_kind,revision_ids) VALUES (:family,2,'{[9000000,9000001)}')"
                ),
                {"family": family_id},
            )
            for identity in (1, 2, 9000000):
                await session.execute(
                    text(
                        f'INSERT INTO "{retirement.snapshot_schema(other_family)}".custom_import_family_child '
                        "(dataset_id,schema_revision_id,root_record_id,family_revision_id,collection_slot,child_revision_id) "
                        "VALUES (:dataset,:schema,999,777,1,:child)"
                    ),
                    {"dataset": other_graph.dataset_id, "schema": other_graph.schema_revision_id, "child": identity},
                )
        monkeypatch.setattr(retirement, "REFERENCE_PAGE_ROWS", 2)
        with pytest.raises(retirement.SnapshotRetirementRefused, match="revision reference"):
            async with case.sessions() as session, session.begin():
                await _bounded(session)
                await retirement.retire_snapshot_family(
                    session,
                    family_id,
                    engine_id="synthetic",
                    assert_external_unreferenced=_external_fixture_check,
                )
        await _verify_sparse_remaining_reference_pages(case, family_id, monkeypatch)


async def _prepare_sparse_reference_table(session, publication_case, family_id):
    """Keep indexed references outside the family while paging sparse nullable bases."""
    await session.execute(
        text(
            "CREATE TEMP TABLE custom_import_build_occurrence (occurrence_id bigint PRIMARY KEY,"
            "root_revision_id bigint NOT NULL,child_revision_id bigint NOT NULL,"
            "base_root_revision_id bigint,base_child_revision_id bigint) ON COMMIT DROP"
        )
    )
    for column in ("root_revision_id", "child_revision_id"):
        await session.execute(
            text(f"CREATE INDEX sparse_{column} ON pg_temp.custom_import_build_occurrence ({column})")
        )
    await session.execute(
        text(
            f'INSERT INTO "{publication_case.schema_name}".custom_import_revision_home '
            "(family_id,revision_kind,revision_ids) VALUES (:family,1,'{[9100000,9100001)}')"
        ),
        {"family": family_id},
    )
    indexed_value = 50_000_000_000
    assert not await session.scalar(
        text(
            f'SELECT EXISTS (SELECT FROM "{publication_case.schema_name}".custom_import_revision_home '
            "WHERE family_id=:family AND revision_ids @> CAST(:value AS bigint))"
        ),
        {"family": family_id, "value": indexed_value},
    )
    await session.execute(
        text("INSERT INTO pg_temp.custom_import_build_occurrence VALUES (:identity,:indexed,:indexed,NULL,NULL)"),
        [
            {"identity": identity, "indexed": indexed_value}
            for identity in (1, 3, 8, 10, 12, 15, 20, 21, 22, 23, 31, 40, 41, 63)
        ],
    )
    return Table(
        "custom_import_build_occurrence",
        MetaData(),
        Column("occurrence_id", BigInteger, primary_key=True),
        *(Column(name, BigInteger) for name in retirement._REFERENCES["custom_import_build_occurrence"]),
        schema="pg_temp",
    )


def _observe_sparse_reference_pages(session, monkeypatch):
    """Require the remaining nullable references to filter before the keyset limit."""
    page_queries = []
    original_execute = session.execute

    async def observe_page(statement, *args, **kwargs):
        sql = str(statement)
        if "reference_page" in sql:
            page_queries.append(sql)
            for name in ("base_root_revision_id", "base_child_revision_id"):
                assert f".{name} IS NOT NULL" in sql[: sql.index("ORDER BY")]
            for name in ("root_revision_id", "child_revision_id"):
                assert f".{name} IS NOT NULL" not in sql
        return await original_execute(statement, *args, **kwargs)

    monkeypatch.setattr(session, "execute", observe_page)
    return page_queries


async def _verify_sparse_remaining_reference_pages(publication_case, family_id, monkeypatch):
    """Preserve empty-page, keyset-gap, EOF, and independent base-reference refusals."""
    async with publication_case.sessions() as session, session.begin():
        await _bounded(session)
        table = await _prepare_sparse_reference_table(session, publication_case, family_id)
        page_queries = _observe_sparse_reference_pages(session, monkeypatch)
        await retirement._reference_pages(session, table, family_id)
        assert len(page_queries) == 1  # Indexed values must not keep NULL-only remaining references.
        await session.execute(
            text(
                "UPDATE pg_temp.custom_import_build_occurrence SET "
                "base_root_revision_id=CASE WHEN occurrence_id IN (10,40) THEN 900 END,"
                "base_child_revision_id=CASE WHEN occurrence_id IN (21,40) THEN 901 END"
            )
        )
        page_queries.clear()
        await retirement._reference_pages(session, table, family_id)
        assert len(page_queries) == 3  # Two qualifying pages and EOF, across NULL gaps and a NULL tail.
        for column, revision_id in (("base_root_revision_id", 9100000), ("base_child_revision_id", 9000000)):
            await session.execute(
                text(
                    "UPDATE pg_temp.custom_import_build_occurrence SET "
                    "base_root_revision_id=NULL,base_child_revision_id=NULL WHERE occurrence_id=63"
                )
            )
            await session.execute(
                text(f"UPDATE pg_temp.custom_import_build_occurrence SET {column}=:value WHERE occurrence_id=63"),
                {"value": revision_id},
            )
            with pytest.raises(retirement.SnapshotRetirementRefused, match="revision reference"):
                await retirement._reference_pages(session, table, family_id)
        assert await session.get(CustomImportSnapshotFamily, family_id) is not None
        assert await session.get(CustomImportSnapshotRetirement, family_id) is None


async def test_indexed_reference_proof_preserves_multirange_holes_and_unbounded_ranges(monkeypatch):
    async with isolated_publication_case() as case:
        _, family_id = await _replaced_fence(case)
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(
                    "CREATE TEMP TABLE custom_import_child_scalar (child_revision_id bigint PRIMARY KEY) ON COMMIT DROP"
                )
            )
            await session.execute(text("INSERT INTO pg_temp.custom_import_child_scalar VALUES (902)"))
            home_insert = text(
                f'INSERT INTO "{case.schema_name}".custom_import_revision_home '
                "(family_id,revision_kind,revision_ids) VALUES (:family,2,CAST(CAST(:ranges AS text) AS int8multirange))"
            )
            await session.execute(home_insert, {"family": family_id, "ranges": "{[900,902),[904,906)}"})
            table = Table(
                "custom_import_child_scalar", MetaData(), Column("child_revision_id", BigInteger), schema="pg_temp"
            )
            original_execute = session.execute

            async def reject_full_page(statement, *args, **kwargs):
                assert "reference_page" not in str(statement)
                return await original_execute(statement, *args, **kwargs)

            monkeypatch.setattr(session, "execute", reject_full_page)
            await retirement._reference_pages(session, table, family_id)
            await session.execute(text("UPDATE pg_temp.custom_import_child_scalar SET child_revision_id=904"))
            with pytest.raises(retirement.SnapshotRetirementRefused, match="revision reference"):
                await retirement._reference_pages(session, table, family_id)
            await session.execute(text("UPDATE pg_temp.custom_import_child_scalar SET child_revision_id=2000"))
            await session.execute(home_insert, {"family": family_id, "ranges": "{[1900,)}"})
            with pytest.raises(retirement.SnapshotRetirementRefused, match="revision reference"):
                await retirement._reference_pages(session, table, family_id)


async def test_retirement_preserves_current_and_previous_publication_targets():
    async with isolated_publication_case() as case:
        graph, attempt, family_id = await _sealed_current_snapshot(case)
        with pytest.raises(retirement.SnapshotRetirementRefused, match="no publication history"):
            async with case.sessions() as session, session.begin():
                await _bounded(session)
                await retirement.retire_snapshot_family(
                    session, family_id, engine_id="synthetic", assert_external_unreferenced=_external_fixture_check
                )
        async with case.sessions() as session, session.begin():
            await publication.activate_generation(
                session,
                dataset_id=graph.dataset_id,
                target_generation_id=attempt.generation_id,
                expected_generation_id=None,
                expected_pointer_version=0,
            )
        for is_rollback in (False, True):
            if is_rollback:
                async with case.sessions() as session, session.begin():
                    await publication.rollback_generation(
                        session,
                        dataset_id=graph.dataset_id,
                        target_generation_id=graph.first_generation_id,
                        expected_generation_id=attempt.generation_id,
                        expected_pointer_version=1,
                    )
            with pytest.raises(retirement.SnapshotRetirementRefused, match="current or retained for rollback"):
                async with case.sessions() as session, session.begin():
                    await _bounded(session)
                    await retirement.retire_snapshot_family(
                        session,
                        family_id,
                        engine_id="synthetic",
                        assert_external_unreferenced=_external_fixture_check,
                    )


async def test_read_retirement_race_never_takes_missing_family_legacy_fallback():
    async with isolated_publication_case() as case:
        graph, attempt, family_id = await _superseded_snapshot(case)
        async with case.sessions() as reader, reader.begin(), case.sessions() as owner:
            await reader.execute(text("SET TRANSACTION READ ONLY"))
            await reader.execute(text("SET LOCAL statement_timeout='4000ms'"))
            reader_pid = await reader.scalar(text("SELECT pg_backend_pid()"))
            async with owner.begin():
                await _bounded(owner)
                await owner.execute(
                    text(f'LOCK TABLE "{case.schema_name}".custom_import_snapshot_family IN ACCESS EXCLUSIVE MODE')
                )
                pending = asyncio.create_task(_read_binding(reader, case, graph, attempt.generation_id))
                try:
                    await _wait_for_registry_pin(owner, reader_pid, case.schema_name)
                    await retirement.retire_snapshot_family(
                        owner,
                        family_id,
                        engine_id="synthetic",
                        assert_external_unreferenced=_published_fixture_check,
                    )
                except BaseException:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                    raise
            with pytest.raises(DBAPIError, match="custom_import_snapshot_retired"):
                await pending


@pytest.mark.parametrize(
    "kind,column,revision_kind",
    (
        ("root", "revision_id", 1),
        ("child", "revision_id", 2),
        ("root", "base_root_revision_id", 1),
        ("child", "base_child_revision_id", 2),
        ("child", "root_revision_id", 1),
    ),
)
async def test_retirement_checks_retained_legacy_origin_references(kind, column, revision_kind):
    async with isolated_publication_case() as case:
        _, family_id = await _replaced_fence(case)
        kept_graph, _, kept_family = await _sealed_current_snapshot(case)
        origin_by_column = dict(
            kind=kind,
            revision_id=200,
            root_revision_id=200,
            base_root_revision_id=201,
            base_child_revision_id=202 if kind == "child" else None,
            collection_slot=1 if kind == "child" else None,
            base_generation_id=kept_graph.first_generation_id,
        )
        origin_by_column[column] = 9000000
        if kind == "root" and column == "revision_id":
            origin_by_column["root_revision_id"] = 9000000
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(
                    f'INSERT INTO "{case.schema_name}".custom_import_revision_home '
                    "(family_id,revision_kind,revision_ids) VALUES (:family,:kind,'{[9000000,9000001)}')"
                ),
                {"family": family_id, "kind": revision_kind},
            )
            await session.execute(
                text(
                    f'INSERT INTO "{retirement.snapshot_schema(kept_family)}".legacy_copy_origin '
                    "(kind,revision_id,family_revision_id,root_record_id,root_revision_id,collection_slot,"
                    "base_generation_id,base_family_revision_id,base_root_revision_id,base_child_revision_id) "
                    "VALUES (:kind,:revision_id,777,999,:root_revision_id,:collection_slot,"
                    ":base_generation_id,888,:base_root_revision_id,:base_child_revision_id)"
                ),
                origin_by_column,
            )
        with pytest.raises(retirement.SnapshotRetirementRefused, match="revision reference"):
            async with case.sessions() as session, session.begin():
                await _bounded(session)
                await retirement.retire_snapshot_family(
                    session,
                    family_id,
                    engine_id="synthetic",
                    assert_external_unreferenced=_external_fixture_check,
                )


async def test_retirement_does_not_hold_registry_write_lock_during_reference_proof(monkeypatch):
    async with isolated_publication_case() as case:
        _, family_id = await _replaced_fence(case)
        _, concurrent_attempt = await _attempt(case)
        original = retirement._revision_references
        created_families = []

        async def changed_census(*args, **kwargs):
            await original(*args, **kwargs)
            created_families.append(await _create_snapshot(case, concurrent_attempt))

        monkeypatch.setattr(retirement, "_revision_references", changed_census)
        with pytest.raises(retirement.SnapshotRetirementRefused, match="census changed"):
            async with case.sessions() as session, session.begin():
                await _bounded(session)
                await retirement.retire_snapshot_family(
                    session,
                    family_id,
                    engine_id="synthetic",
                    assert_external_unreferenced=_external_fixture_check,
                )
        async with case.sessions() as session, session.begin():
            assert await session.get(CustomImportSnapshotFamily, family_id) is not None
            assert await session.get(CustomImportSnapshotRetirement, family_id) is None
            assert len(created_families) == 1
            assert await session.get(CustomImportSnapshotFamily, created_families[0]) is not None


@pytest.mark.parametrize("budget", ["MAX_RETIREMENT_SECONDS", "MAX_REFERENCE_CLOSURE_SECONDS"])
async def test_retirement_total_deadline_rolls_back_its_caller_transaction(monkeypatch, budget):
    assert retirement.MAX_REFERENCE_CLOSURE_SECONDS == 1
    async with isolated_publication_case() as case:
        _, family_id = await _replaced_fence(case)
        caller_write = Table("caller_write", MetaData(), Column("value", BigInteger), schema=case.schema_name)
        async with case.engine.begin() as connection:
            await connection.run_sync(caller_write.create)
        monkeypatch.setattr(retirement, budget, 0)
        with pytest.raises(TimeoutError):
            async with case.sessions() as session, session.begin():
                await _bounded(session)
                await session.execute(caller_write.insert().values(value=1))
                await retirement.retire_snapshot_family(
                    session,
                    family_id,
                    engine_id="synthetic",
                    assert_external_unreferenced=_external_fixture_check,
                )
        async with case.sessions() as session, session.begin():
            assert await session.scalar(select(caller_write.c.value)) is None
            assert await session.get(CustomImportSnapshotFamily, family_id) is not None
            assert await session.get(CustomImportSnapshotRetirement, family_id) is None


async def test_retirement_allows_an_unrelated_idle_mutable_family():
    async with isolated_publication_case() as case:
        _, family_id = await _replaced_fence(case)
        _, other_attempt = await _attempt(case)
        other_family = await _create_snapshot(case, other_attempt)
        async with case.sessions() as session, session.begin():
            await _bounded(session)
            receipt = await retirement.retire_snapshot_family(
                session, family_id, engine_id="synthetic", assert_external_unreferenced=_external_fixture_check
            )
            assert not receipt.replayed
            assert await session.get(CustomImportSnapshotFamily, other_family) is not None


async def test_retirement_never_waits_behind_an_unrelated_mutable_writer():
    async with isolated_publication_case() as case:
        _, family_id = await _replaced_fence(case)
        _, other_attempt = await _attempt(case)
        other_family = await _create_snapshot(case, other_attempt)
        async with case.sessions() as writer, writer.begin():
            await writer.execute(
                text(
                    f'LOCK TABLE "{retirement.snapshot_schema(other_family)}".custom_import_family_child '
                    "IN ROW EXCLUSIVE MODE"
                )
            )
            with pytest.raises(DBAPIError, match="could not obtain lock"):
                async with case.sessions() as session, session.begin():
                    await _bounded(session)
                    await retirement.retire_snapshot_family(
                        session, family_id, engine_id="synthetic", assert_external_unreferenced=_external_fixture_check
                    )


async def test_retirement_refuses_origin_registered_during_the_frozen_proof(monkeypatch):
    async with isolated_publication_case() as case:
        _, family_id = await _replaced_fence(case)
        _, other_attempt = await _attempt(case)
        other_family = await _create_snapshot(case, other_attempt)
        async with case.sessions() as session, session.begin():
            await _call(
                session,
                case,
                other_attempt,
                "bind_custom_import_snapshot_generation",
                generation_id=other_attempt.generation_id,
            )
        original = retirement._revision_references

        async def register_origin(*args, **kwargs):
            await original(*args, **kwargs)
            async with case.sessions() as writer, writer.begin():
                await _bounded(writer)
                await writer.scalar(
                    text(f'SELECT "{case.schema_name}".install_custom_import_legacy_snapshot_writers(:family)'),
                    {"family": other_family},
                )

        monkeypatch.setattr(retirement, "_revision_references", register_origin)
        with pytest.raises(retirement.SnapshotRetirementRefused, match="census changed"):
            async with case.sessions() as session, session.begin():
                await _bounded(session)
                await retirement.retire_snapshot_family(
                    session, family_id, engine_id="synthetic", assert_external_unreferenced=_external_fixture_check
                )
        async with case.sessions() as session, session.begin():
            assert await session.get(CustomImportSnapshotFamily, family_id) is not None
            assert await session.get(CustomImportSnapshotRetirement, family_id) is None
            assert (await session.get(CustomImportSnapshotFamily, other_family)).origin_table_oid is not None


async def _published_fixture_check(session, family, current, rollback):
    assert session.in_transaction() and family.dataset_id > 0
    assert current is not None and rollback is not None
    assert family.generation_id not in (current, rollback)


async def _supersede_build(case, request, generation_id, records):
    """Publish the target and two genuinely rebuilt successors before retirement."""
    from tests import test_custom_import_build_output_postgres as output_fixture

    seed = SimpleNamespace(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
    )
    await output_fixture._activate(case, request, generation_id)
    for version in (1, 2):
        changed = deepcopy(records)
        changed["rates"][0][0]["amount"] = str(int(changed["rates"][0][0]["amount"]) + version)
        successor = await output_fixture._request_for(
            case,
            changed,
            seed=seed,
            base=generation_id,
            version=version,
            page_rows=64,
            definition=request.definition,
        )
        _, sealed = await output_fixture._complete(case, successor)
        assert sealed.no_change is None
        await output_fixture._activate(case, successor, sealed.generation_id, base=generation_id, version=version)
        generation_id = sealed.generation_id


async def test_retirement_preserves_completed_projection_history_and_removes_its_storage():
    from db.models.custom_import_query_projection import CustomImportQueryProjection
    from tests import custom_import_query_projection_build_postgres as projection
    from tests import test_custom_import_build_output_postgres as output_fixture
    from tests.custom_import_build_scratch_postgres import install_current_verification
    from tests.test_custom_import_build_source_postgres import _source_case

    async with _source_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(install_current_verification, case.schema_name)
        request = await output_fixture._request_for(
            case,
            projection._records(),
            definition=projection._definition(),
            page_rows=64,
        )
        _, sealed = await output_fixture._complete(case, request)
        async with case.sessions() as session, session.begin():
            family = await session.scalar(
                select(CustomImportSnapshotFamily).where(
                    CustomImportSnapshotFamily.generation_id == sealed.generation_id,
                )
            )
            family_id = family.family_id
            manifest = await session.get(CustomImportQueryProjection, family_id)
            assert manifest.phase == "complete"
            history_values = tuple(getattr(manifest, column.name) for column in manifest.__table__.columns)
        await _supersede_build(case, request, sealed.generation_id, projection._records())
        async with case.sessions() as session, session.begin():
            await _bounded(session)
            await retirement.retire_snapshot_family(
                session,
                family_id,
                engine_id="synthetic",
                assert_external_unreferenced=_published_fixture_check,
            )
        async with case.sessions() as session, session.begin():
            manifest = await session.get(CustomImportQueryProjection, family_id)
            assert tuple(getattr(manifest, column.name) for column in manifest.__table__.columns) == history_values
            assert await session.get(CustomImportSnapshotFamily, family_id) is not None
            assert await session.get(CustomImportSnapshotRetirement, family_id) is not None
            assert (
                await session.scalar(
                    text("SELECT to_regnamespace(:name)"), {"name": retirement.snapshot_schema(family_id)}
                )
                is None
            )


async def _assert_commit_closure_deadline(case, family_id, monkeypatch):
    """Exhaust the real commit deadline and verify its transaction rolls back."""
    original_closure = retirement._commit_reference_closure
    original_reference_check = retirement._revision_references
    flags_by_phase = {"closure": False, "blocked": False}

    async def tracked_closure(*args, **kwargs):
        flags_by_phase["closure"] = True
        return await original_closure(*args, **kwargs)

    async def blocked_references(*args, **kwargs):
        if flags_by_phase["closure"]:
            flags_by_phase["blocked"] = True
            await asyncio.Event().wait()
        return await original_reference_check(*args, **kwargs)

    assert retirement.MAX_REFERENCE_CLOSURE_SECONDS == 1
    with monkeypatch.context() as deadline:
        deadline.setattr(retirement, "_commit_reference_closure", tracked_closure)
        deadline.setattr(retirement, "_revision_references", blocked_references)
        # An outer witness must fail if the inner deadline is missing; it cannot satisfy raises.
        async with asyncio.timeout(10):
            with pytest.raises(TimeoutError):
                async with case.sessions() as session, session.begin():
                    await _bounded(session)
                    await retirement.retire_snapshot_family(
                        session, family_id, engine_id="synthetic", assert_external_unreferenced=_published_fixture_check
                    )
    assert flags_by_phase == {"closure": True, "blocked": True}
    async with case.sessions() as session, session.begin():
        assert await session.get(CustomImportSnapshotFamily, family_id) is not None
        assert await session.get(CustomImportSnapshotRetirement, family_id) is None
        assert (
            await session.scalar(text("SELECT to_regnamespace(:name)"), {"name": retirement.snapshot_schema(family_id)})
            is not None
        )


async def test_retirement_preserves_closed_continuation_history_but_refuses_active_authority(monkeypatch):
    from db.models.custom_import_continuation import CustomImportBuildContinuation
    from process.custom_import import build_output
    from process.custom_import.retirement_history import assert_closed_continuations
    from tests.custom_import_build_scratch_postgres import install_current_verification
    from tests.test_custom_import_build_source_postgres import _source_case
    from tests.test_custom_import_staged_verification_postgres import _frozen

    assert retirement.MAX_REFERENCE_CLOSURE_SECONDS == 1
    async with _source_case() as case:
        async with case.engine.begin() as connection:
            await connection.run_sync(install_current_verification, case.schema_name)
        request, build_id, _ = await _frozen(case, monkeypatch, continuation=True)
        async with case.sessions() as session, session.begin():
            family = await session.scalar(
                select(CustomImportSnapshotFamily).where(
                    CustomImportSnapshotFamily.execution_id == request.execution_id
                )
            )
            family_id = family.family_id
            history = await session.scalar(select(CustomImportBuildContinuation))
            original_values = tuple(getattr(history, column.name) for column in history.__table__.columns)
            with pytest.raises(retirement.SnapshotRetirementRefused, match="active continuation"):
                await assert_closed_continuations(session, family)
        await build_output.build_output(case.sessions, request, build_id)
        from tests.test_custom_import_build_output_postgres import _records

        await _supersede_build(case, request, family.generation_id, _records(2, 3))
        await _assert_commit_closure_deadline(case, family_id, monkeypatch)
        # Keep bounded DDL headroom while the separate deadline proof checks refusal.
        with monkeypatch.context() as patch:
            patch.setattr(retirement, "MAX_REFERENCE_CLOSURE_SECONDS", 3)
            async with case.sessions() as session, session.begin():
                await _bounded(session)
                await retirement.retire_snapshot_family(
                    session, family_id, engine_id="synthetic", assert_external_unreferenced=_published_fixture_check
                )
        async with case.sessions() as session, session.begin():
            history = await session.scalar(select(CustomImportBuildContinuation))
            assert tuple(getattr(history, column.name) for column in history.__table__.columns) == original_values
            assert await session.get(CustomImportSnapshotFamily, family_id) is not None
            assert await session.get(CustomImportSnapshotRetirement, family_id) is not None
