# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Scoped preparation, immutable seals and CAS preserve unrelated native rows."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from api import provider_profile_states as serving
from db import models
from db.connection import Database
from process import source_profile_result_archive as archive
from tests.source_profile_archive_support import _create_family, _database_url, _drop_family, _seed


@asynccontextmanager
async def _prepared_case(importer, *, with_ancestry=False, contract=archive.LEGACY_CONTRACT):
    """Own isolated source/destination families and their exact clone cleanup."""
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    source_schema = "profile_test_" + uuid4().hex
    destination_schema = "profile_test_" + uuid4().hex
    prepared = None
    created_schemas = []
    try:
        async with sessions() as session, session.begin():
            for schema in (source_schema, destination_schema):
                await _create_family(session, schema)
                created_schemas.append(schema)
            incoming, ancestors, incumbent, other, unrelated, owner = await _seed_case(
                session, source_schema, destination_schema, importer, with_ancestry
            )
            await _assert_reference_free_tables(session, created_schemas)
        async with sessions() as session, session.begin():
            prepared = await archive.prepare_source(
                session,
                importer_id=importer,
                schema=source_schema,
                run_id=incoming,
                dataset_id=uuid4(),
                contract=contract,
            )
        activation_by_field = dict(
            prepared=prepared,
            destination_schema=destination_schema,
            expected_current_run_id=incumbent,
            package_id="a" * 64,
            sealed_owner_oid=owner,
            pin_id=uuid4(),
        )
        yield SimpleNamespace(
            engine=engine,
            sessions=sessions,
            source_schema=source_schema,
            destination_schema=destination_schema,
            importer=importer,
            incoming=incoming,
            ancestors=ancestors,
            incumbent=incumbent,
            other=other,
            unrelated=unrelated,
            prepared=prepared,
            activation_by_field=activation_by_field,
        )
    finally:
        await _cleanup_prepared_case(sessions, prepared, source_schema, created_schemas)
        await engine.dispose()


async def _cleanup_prepared_case(sessions, prepared, source_schema, created_schemas):
    async with sessions() as session, session.begin():
        if prepared is not None:
            await _drop_prepared_stage(session, prepared, created_schemas[-1])
            await archive.release_source_pin(
                session,
                schema=source_schema,
                importer_id=prepared.ownership.importer_id,
                run_id=prepared.manifest["run_id"],
                pin_id=prepared.ownership.dataset_id,
            )
        for schema in reversed(created_schemas):
            await _drop_family(session, schema)


async def _drop_prepared_stage(session, prepared, destination_schema):
    """Remove only fixture-owned attachments and their seals before exact stage cleanup."""
    if prepared.manifest["contract"] == archive.LEGACY_CONTRACT:
        await archive.cleanup_stage(session, prepared.ownership)
        return
    oid_by_name = dict(prepared.ownership.relation_oids)
    publication_by_field = {
        "parents": [
            [name, await archive.native._relation_oid(session, destination_schema, name)] for name in archive.TABLES
        ],
        "children": [
            [name, child, oid_by_name[child]]
            for name, child in zip(archive.TABLES, archive.PUBLICATION_TABLES, strict=True)
        ],
    }
    if await session.scalar(
        text("SELECT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=ANY(CAST(:oids AS oid[])))"),
        {"oids": [child[2] for child in publication_by_field["children"]]},
    ):
        await archive._detach_publication(
            session,
            destination_schema,
            {
                "ownership": archive.ownership_dict(prepared.ownership),
                "destination_schema": destination_schema,
                "publication": publication_by_field,
            },
        )
    await session.execute(
        text(
            f"DELETE FROM {archive._table(destination_schema, archive.pins.TABLE)} "
            "WHERE authority_json->'validation'->'ownership'->>'dataset_id'=:dataset"
        ),
        {"dataset": str(prepared.ownership.dataset_id)},
    )
    await archive.cleanup_stage(
        session, prepared.ownership, publication=publication_by_field, destination_schema=destination_schema
    )


async def _assert_no_canonical_adoption(case, pin_id):
    async with case.sessions() as session, session.begin():
        assert await archive._run(session, case.destination_schema, case.incoming) is None
        assert await archive._pin_group(session, case.destination_schema, pin_id) == []
        assert (await archive._pointer(session, case.destination_schema, case.importer))[
            "current_run_id"
        ] == case.incumbent


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", archive.SOURCES)
async def test_set_validated_publication_has_no_row_hashes_or_early_adoption(importer, monkeypatch):
    """Require indexed heaps, physical OID publication and late-failure rollback without row validators."""

    async def forbidden_row_validation(*_args, **_kwargs):
        raise AssertionError("v2 must not hash or stream payload rows")

    monkeypatch.setattr(archive, "_projected_row_identity", forbidden_row_validation)
    monkeypatch.setattr(AsyncSession, "stream", forbidden_row_validation)
    integrity = archive._integrity

    async def indexed_stage_integrity(session, schema, importer_id, run_id):
        assert schema.startswith("source_profile_result_")
        for name in archive.TABLES:
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_index WHERE indrelid=to_regclass(:name) AND indisready AND indisvalid"
                    ),
                    {"name": f"{schema}.{name}"},
                )
                > 0
            )
        await integrity(session, schema, importer_id, run_id)

    monkeypatch.setattr(archive, "_integrity", indexed_stage_integrity)
    async with _prepared_case(importer, contract=archive.CONTRACT) as case:
        monkeypatch.setattr(archive, "_integrity", integrity)
        assert all(
            set(table) == {"table_name", "row_count", "schema_sha256"} for table in case.prepared.manifest["tables"]
        )
        async with case.sessions() as session, session.begin():
            validation = await archive.prepare_activation(session, **case.activation_by_field)
            assert validation["publication"]["created_run_ids"] == [case.incoming]
            await _assert_indexed_stage_without_row_guards(session, case.prepared.ownership)
        pin_id = case.activation_by_field["pin_id"]
        await _assert_no_canonical_adoption(case, pin_id)
        with pytest.raises(RuntimeError, match="synthetic installation failure"):
            async with case.sessions() as session, session.begin():
                await archive.activate_validated_result(session, validation=validation, **case.activation_by_field)
                assert await archive._run(session, case.destination_schema, case.incoming) is not None
                assert await archive._pin_group(session, case.destination_schema, pin_id)
                raise RuntimeError("synthetic installation failure")
        await _assert_no_canonical_adoption(case, pin_id)
        async with case.sessions() as session, session.begin():
            await archive.activate_validated_result(session, validation=validation, **case.activation_by_field)
            await _assert_physical_publication(session, case, validation["publication"])
        await _assert_source_guards(case)
        await _assert_reference_free_read(case, monkeypatch)
        async with case.sessions() as session, session.begin():
            await archive.rollback_result(
                session,
                schema=case.destination_schema,
                importer_id=importer,
                expected_current_run_id=case.incoming,
                expected_previous_run_id=case.incumbent,
                contract=archive.CONTRACT,
            )


async def _assert_indexed_stage_without_row_guards(session, ownership):
    """Every prepared heap has finished indexes, no FK and no payload-row trigger."""
    for _name, oid in ownership.relation_oids:
        assert not await session.scalar(
            text("SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=:oid AND contype='f')"), {"oid": oid}
        )
        assert not await session.scalar(
            text("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=:oid AND (tgtype & 1)=1)"), {"oid": oid}
        )
        assert await session.scalar(
            text("SELECT EXISTS(SELECT 1 FROM pg_index WHERE indrelid=:oid AND indisready AND indisvalid)"),
            {"oid": oid},
        )


async def _assert_physical_publication(session, case, publication):
    """Serving queries use the exact indexed alias OIDs, with only recorded leaf attachments."""
    await archive._require_stage_topology(session, case.prepared.ownership, publication)
    for name, _child, oid in publication["children"]:
        assert (
            await session.scalar(
                text(f"SELECT tableoid::oid FROM {archive._table(case.destination_schema, name)} WHERE run_id=:run"),
                {"run": case.incoming},
            )
            == oid
        )


@pytest.mark.asyncio
async def test_v2_publication_preserves_inflight_api_snapshot_and_locked_attachment(monkeypatch):
    """Readers see one complete generation, and detach cannot remove their locked physical rows."""
    async with _prepared_case("massachusetts-borim-profile", contract=archive.CONTRACT) as case:
        database = Database(engine=case.engine, session_factory=case.sessions)
        monkeypatch.setattr(serving, "db", database)
        monkeypatch.setattr(serving.ProviderProfileSourcePublication.__table__, "schema", case.destination_schema)
        validation = await _prepare_with_incumbent_api_reads(case, monkeypatch)
        async with _held_source_api_read(case, database, case.incumbent):
            with pytest.raises(RuntimeError, match="synthetic installation failure"):
                async with case.sessions() as publisher, publisher.begin():
                    await archive.activate_validated_result(
                        publisher, validation=validation, **case.activation_by_field
                    )
                    await _assert_api_generation(case, case.incumbent)
                    raise RuntimeError("synthetic installation failure")
            await _assert_no_canonical_adoption(case, case.activation_by_field["pin_id"])
            await _assert_api_generation(case, case.incumbent)
            async with case.sessions() as publisher, publisher.begin():
                await archive.activate_validated_result(publisher, validation=validation, **case.activation_by_field)
                await _assert_physical_publication(publisher, case, validation["publication"])
            await _assert_api_generation(case, case.incoming)
        async with _held_source_api_read(case, database, case.incoming):
            await _assert_reader_blocks_detach(case, validation)
        async with case.sessions() as session, session.begin():
            assert (
                await archive.cleanup_adoption(
                    session,
                    schema=case.destination_schema,
                    importer_id=case.importer,
                    run_id=case.incoming,
                    pin_id=case.activation_by_field["pin_id"],
                )
                == "released"
            )
            await archive._require_stage_topology(session, case.prepared.ownership)
            assert await archive._run(session, case.destination_schema, case.incoming) is None


async def _assert_api_generation(case, expected_run):
    """Exercise the production joined read and compare every exposed fact against one run."""
    projections = await serving.fetch_additional_state_profile_projections(1234567890)
    projection = next(
        value for value in projections if value["source"]["source_key"] == archive.SOURCES[case.importer][0]
    )
    assert projection["generation_id"] == projection["evidence"]["generation_id"] == expected_run
    records = projection["evidence"]["records"]
    assert records and all(record["run_id"] == expected_run for record in records)
    async with case.sessions() as session, session.begin():
        schema = case.prepared.ownership.schema_name if expected_run == case.incoming else case.destination_schema
        expected_ids = set(
            await session.scalars(
                text(f'SELECT fact_id FROM "{schema}".provider_profile_fact WHERE run_id=:run'),
                {"run": expected_run},
            )
        )
    assert {record["fact_id"] for record in records} == expected_ids
    assert projection["categories"]["education"]["items"][0]["display"] == "Synthetic school"
    return projection


async def _prepare_with_incumbent_api_reads(case, monkeypatch):
    """The actual isolated load, completed index build and set validation never hide the incumbent."""
    checkpoints = []
    create_indexes, validate_sets = archive.native._create_model_indexes, archive._validate_publication_sets

    async def indexed(session, spec, schema, **options):
        assert schema == case.prepared.ownership.schema_name and spec.table_names == archive.PUBLICATION_TABLES
        for table, child in zip(case.prepared.manifest["tables"], archive.PUBLICATION_TABLES, strict=True):
            assert await session.scalar(text(f'SELECT count(*) FROM "{schema}"."{child}"')) == table["row_count"]
        await _assert_api_generation(case, case.incumbent)
        checkpoints.append("loaded")
        await create_indexes(session, spec, schema, **options)
        await _assert_indexed_stage_without_row_guards(session, case.prepared.ownership)
        await _assert_api_generation(case, case.incumbent)
        checkpoints.append("indexed")

    async def validated(session, prepared, publication):
        await validate_sets(session, prepared, publication)
        await _assert_api_generation(case, case.incumbent)
        checkpoints.append("validated")

    with monkeypatch.context() as patch:
        patch.setattr(archive.native, "_create_model_indexes", indexed)
        patch.setattr(archive, "_validate_publication_sets", validated)
        async with case.sessions() as session, session.begin():
            validation = await archive.prepare_activation(session, **case.activation_by_field)
    assert checkpoints == ["loaded", "indexed", "validated"]
    await _assert_api_generation(case, case.incumbent)
    return validation


@asynccontextmanager
async def _held_source_api_read(case, database, expected_run):
    """Keep one real API request's repeatable snapshot and ACCESS SHARE locks until explicitly released."""
    reached_read, release_read = asyncio.get_running_loop().create_future(), asyncio.Event()

    async def read():
        async with database.transaction() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            assert await session.scalar(text("SELECT current_user=session_user"))
            reader_pid = await session.scalar(text("SELECT pg_backend_pid()"))
            first = await _assert_api_generation(case, expected_run)
            reached_read.set_result(reader_pid)
            await release_read.wait()
            assert await _assert_api_generation(case, expected_run) == first

    reader = asyncio.create_task(read())
    try:
        reader_pid = await asyncio.wait_for(reached_read, 3)
        await _assert_api_relation_locks(case, reader_pid, expected_run)
        yield
    finally:
        release_read.set()
        await asyncio.wait_for(reader, 3)


async def _assert_api_relation_locks(case, reader_pid, expected_run):
    """Attest actual pointer, parent and attached-leaf read locks rather than assumed transaction pins."""
    async with case.sessions() as session, session.begin():
        names = ("provider_profile_source_publication", "provider_profile_import_run", "provider_profile_fact")
        oids = [await archive.native._relation_oid(session, case.destination_schema, name) for name in names]
        if expected_run == case.incoming:
            owned_by_name = dict(case.prepared.ownership.relation_oids)
            oids.extend(owned_by_name[name + "_published"] for name in names[1:])
        assert await session.scalar(
            text(
                "SELECT count(DISTINCT relation)=:count FROM pg_locks WHERE pid=:pid AND granted "
                "AND mode='AccessShareLock' AND relation=ANY(CAST(:oids AS oid[]))"
            ),
            {"count": len(oids), "pid": reader_pid, "oids": oids},
        )


async def _assert_reader_blocks_detach(case, validation):
    """A no-longer-current attachment still cannot detach while its API reader holds a leaf lock."""
    async with case.sessions() as publisher, publisher.begin():
        await _seed(publisher, case.destination_schema, case.importer)
        newest = await _seed(publisher, case.destination_schema, case.importer)
    await _assert_api_generation(case, newest)
    with pytest.raises(DBAPIError) as blocked:
        async with case.sessions() as publisher, publisher.begin():
            await archive.cleanup_adoption(
                publisher,
                schema=case.destination_schema,
                importer_id=case.importer,
                run_id=case.incoming,
                pin_id=case.activation_by_field["pin_id"],
            )
    assert blocked.value.orig.sqlstate == "55P03"
    async with case.sessions() as session, session.begin():
        await archive._require_stage_topology(session, case.prepared.ownership, validation["publication"])
        assert await archive._pin_group(session, case.destination_schema, case.activation_by_field["pin_id"])


@pytest.mark.asyncio
async def test_set_validated_source_rejects_equal_count_clone_mutation(monkeypatch):
    async def forbidden_row_validation(*_args, **_kwargs):
        raise AssertionError("v2 must not hash payload rows")

    monkeypatch.setattr(archive, "_projected_row_identity", forbidden_row_validation)
    async with _prepared_case("massachusetts-borim-profile", contract=archive.CONTRACT) as case:
        with pytest.raises(archive.SourceProfileArchiveError, match="content differs"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    text(f'UPDATE "{case.prepared.ownership.schema_name}".provider_profile_fact SET display=:value'),
                    {"value": "same-count-corruption"},
                )
                await archive.validate_source_stage(session, prepared=case.prepared, source_schema=case.source_schema)
        async with case.sessions() as session, session.begin():
            await archive.validate_source_stage(session, prepared=case.prepared, source_schema=case.source_schema)


@pytest.mark.asyncio
async def test_attached_publication_detaches_only_after_last_reference_and_binds_cleanup_destination():
    async with _prepared_case("massachusetts-borim-profile", contract=archive.CONTRACT) as case:
        async with case.sessions() as session, session.begin():
            validation = await archive.prepare_activation(session, **case.activation_by_field)
        async with case.sessions() as session, session.begin():
            await archive.activate_validated_result(session, validation=validation, **case.activation_by_field)
            await _seed(session, case.destination_schema, case.importer)
            await _seed(session, case.destination_schema, case.importer)
            export_pin = uuid4()
            await archive.pins.record_pin(
                session,
                schema=case.destination_schema,
                source_key=archive.SOURCES[case.importer][0],
                run_id=case.incoming,
                pin_id=export_pin,
                purpose="export",
                authority={"root_run_id": case.incoming, "run_ids": [case.incoming]},
            )
            cleanup_by_field = dict(
                schema=case.destination_schema,
                importer_id=case.importer,
                run_id=case.incoming,
                pin_id=case.activation_by_field["pin_id"],
            )
            assert await archive.cleanup_adoption(session, **cleanup_by_field) == "retained"
            await archive.release_source_pin(
                session,
                schema=case.destination_schema,
                importer_id=case.importer,
                run_id=case.incoming,
                pin_id=export_pin,
            )
            assert await archive.cleanup_adoption(session, **cleanup_by_field) == "released"
            assert await archive._run(session, case.destination_schema, case.incoming) is None
            await archive._require_stage_topology(session, case.prepared.ownership)
            with pytest.raises(archive.SourceProfileArchiveError, match="parent catalog changed"):
                async with session.begin_nested():
                    await archive.cleanup_stage(
                        session,
                        case.prepared.ownership,
                        publication=validation["publication"],
                        destination_schema=case.source_schema,
                    )
            savepoint = await session.begin_nested()
            await archive.cleanup_stage(
                session,
                case.prepared.ownership,
                publication=validation["publication"],
                destination_schema=case.destination_schema,
            )
            assert await session.scalar(
                text("SELECT to_regnamespace(:schema) IS NULL"), {"schema": case.prepared.ownership.schema_name}
            )
            await savepoint.rollback()


@pytest.mark.asyncio
async def test_retained_attachment_rollback_refuses_detached_physical_predecessor():
    async with _prepared_case("massachusetts-borim-profile", contract=archive.CONTRACT) as case:
        async with case.sessions() as session, session.begin():
            validation = await archive.prepare_activation(session, **case.activation_by_field)
        async with case.sessions() as session, session.begin():
            await archive.activate_validated_result(session, validation=validation, **case.activation_by_field)
            newest = await _seed(session, case.destination_schema, case.importer)
            rollback_by_field = dict(
                schema=case.destination_schema,
                importer_id=case.importer,
                expected_current_run_id=newest,
                expected_previous_run_id=case.incoming,
                pin_id=case.activation_by_field["pin_id"],
                manifest=case.prepared.manifest,
                package_id=case.activation_by_field["package_id"],
            )
            with pytest.raises(archive.SourceProfileArchiveError, match="attachment topology differs"):
                async with session.begin_nested():
                    name, child, _oid = validation["publication"]["children"][-1]
                    await (await session.connection()).exec_driver_sql(
                        f"ALTER TABLE {archive._table(case.prepared.ownership.schema_name, child)} "
                        f"NO INHERIT {archive._table(case.destination_schema, name)}"
                    )
                    await archive.rollback_validated_result(session, **rollback_by_field)
            assert (await archive._pointer(session, case.destination_schema, case.importer))["current_run_id"] == newest
            await archive.rollback_validated_result(session, **rollback_by_field)
            assert (await archive._pointer(session, case.destination_schema, case.importer))[
                "current_run_id"
            ] == case.incoming


async def _seed_case(session, source_schema, destination_schema, importer, with_ancestry):
    ancestors = []
    if with_ancestry:
        ancestors.append(await _seed(session, source_schema, importer))
        ancestors.insert(0, await _seed(session, source_schema, importer, parent_run_id=ancestors[0]))
    incoming = await _seed(session, source_schema, importer, parent_run_id=ancestors[0] if ancestors else None)
    incumbent = await _seed(session, destination_schema, importer)
    other = next(name for name in archive.SOURCES if name != importer)
    unrelated = await _seed(session, destination_schema, other)
    await _seed(session, source_schema, other)
    owner = await session.scalar(text("SELECT oid FROM pg_roles WHERE rolname=current_user"))
    return incoming, ancestors, incumbent, other, unrelated, owner


async def _assert_reference_free_tables(session, schemas):
    for schema in schemas:
        for name in ("npi", "nucc_taxonomy", "import_run", "provider_profile_projection"):
            assert await session.scalar(text("SELECT to_regclass(:name)"), {"name": f"{schema}.{name}"}) is None


async def _assert_stage_refusals(case):
    with pytest.raises(archive.SourceProfileArchiveError, match="restored result differs"):
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(f"UPDATE \"{case.prepared.ownership.schema_name}\".provider_profile_fact SET display='changed'")
            )
            await archive.validate_stage(session, case.prepared.ownership, case.prepared.manifest)
    with pytest.raises(archive.SourceProfileArchiveError, match="stage ownership changed"):
        async with case.sessions() as session, session.begin():
            await archive.verify_ownership(
                session, replace(case.prepared.ownership, schema_oid=case.prepared.ownership.schema_oid + 1)
            )


async def _assert_source_guards(case):
    projection = ", ".join(
        ":fact" if column.name == "fact_id" else f'"{column.name}"'
        for column in models.ProviderProfileFact.__table__.columns
    )
    for schema in (case.source_schema, case.destination_schema):
        for statement in (
            f"UPDATE \"{schema}\".provider_profile_fact SET display='changed' WHERE run_id=:run",
            f'DELETE FROM "{schema}".provider_profile_fact WHERE run_id=:run',
            f'TRUNCATE "{schema}".provider_profile_fact',
            f'INSERT INTO "{schema}".provider_profile_fact SELECT {projection} FROM "{schema}".provider_profile_fact WHERE run_id=:run',
        ):
            with pytest.raises(DBAPIError, match="retained run is pinned"):
                async with case.sessions() as session, session.begin():
                    await session.execute(text(statement), {"run": case.incoming, "fact": uuid4().hex * 2})


async def _assert_pointer_fences(case, validation):
    with pytest.raises(archive.SourceProfileArchiveError, match="destination predecessor changed"):
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(
                    f'UPDATE "{case.destination_schema}".provider_profile_source_publication SET current_run_id=:run WHERE source_key=:source'
                ),
                {"run": "f" * 32, "source": archive.SOURCES[case.importer][0]},
            )
            await archive.activate_validated_result(session, validation=validation, **case.activation_by_field)
    with pytest.raises(RuntimeError, match="synthetic recording failure"):
        async with case.sessions() as session, session.begin():
            await archive.activate_validated_result(session, validation=validation, **case.activation_by_field)
            raise RuntimeError("synthetic recording failure")
    async with case.sessions() as session, session.begin():
        assert (await archive._pointer(session, case.destination_schema, case.importer))[
            "current_run_id"
        ] == case.incumbent


async def _assert_reference_free_read(case, monkeypatch):
    with monkeypatch.context() as patch:
        patch.setattr(serving, "db", Database(engine=case.engine, session_factory=case.sessions))
        patch.setattr(serving.ProviderProfileSourcePublication.__table__, "schema", case.destination_schema)
        projections = await serving.fetch_additional_state_profile_projections(1234567890)
        restored = next(
            projection
            for projection in projections
            if projection["source"]["source_key"] == archive.SOURCES[case.importer][0]
        )
        assert restored["generation_id"] == case.incoming
        assert restored["categories"]["education"]["items"][0]["display"] == "Synthetic school"
        assert restored["source"]["registry_generation"] == "d" * 64


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", archive.SOURCES)
@pytest.mark.parametrize("fault", ["run_id", "agency", "jurisdiction", "fact_type"])
async def test_unservable_fact_semantics_are_rejected(importer, fault):
    async with _prepared_case(importer) as case:
        schema = case.prepared.ownership.schema_name
        with pytest.raises(archive.SourceProfileArchiveError):
            async with case.sessions() as session, session.begin():
                if fault == "fact_type":
                    await session.execute(text(f"UPDATE \"{schema}\".provider_profile_fact SET fact_type='incorrect'"))
                else:
                    await session.execute(
                        text(
                            f'UPDATE "{schema}".provider_profile_fact SET source_json='
                            "jsonb_set(source_json::jsonb,CAST(:field AS text[]),CAST(:value AS jsonb))::json"
                        ),
                        {"field": [fault], "value": json.dumps("incorrect")},
                    )
                await archive.describe_result(session, importer_id=importer, schema=schema, run_id=case.incoming)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", archive.SOURCES)
async def test_scoped_adoption_and_reference_free_read(importer, monkeypatch):
    async with _prepared_case(importer) as case:
        assert [table["row_count"] for table in case.prepared.manifest["tables"]] == [1, 1, 1, 1]
        assert case.prepared.manifest["dependencies"] == {}
        assert case.prepared.manifest["source_completed_at"] == "2026-01-02T00:00:00+00:00"
        await _assert_stage_refusals(case)
        async with case.sessions() as session, session.begin():
            unrelated_before = await archive.describe_result(
                session, importer_id=case.other, schema=case.destination_schema, run_id=case.unrelated
            )
            validation = await archive.prepare_activation(session, **case.activation_by_field)
            assert (await archive._pointer(session, case.destination_schema, importer))[
                "current_run_id"
            ] == case.incumbent
            assert await archive._run(session, case.destination_schema, case.incoming) is not None
        await _assert_source_guards(case)
        await _assert_pointer_fences(case, validation)

        async def no_content_scan(*_args, **_kwargs):
            raise AssertionError("short activation must not hash or recount content")

        with monkeypatch.context() as patch:
            patch.setattr(archive, "_projected_row_identity", no_content_scan)
            async with case.sessions() as session, session.begin():
                receipt = await archive.activate_validated_result(
                    session, validation=validation, **case.activation_by_field
                )
                assert receipt["previous_run_id"] == case.incumbent
        await _assert_reference_free_read(case, monkeypatch)
        async with case.sessions() as session, session.begin():
            assert (
                await archive.describe_result(
                    session, importer_id=case.other, schema=case.destination_schema, run_id=case.unrelated
                )
                == unrelated_before
            )
            assert (
                await archive.cleanup_adoption(
                    session,
                    schema=case.destination_schema,
                    importer_id=importer,
                    run_id=case.incoming,
                    pin_id=case.activation_by_field["pin_id"],
                )
                == "retained"
            )
            await archive.rollback_result(
                session,
                schema=case.destination_schema,
                importer_id=importer,
                expected_current_run_id=case.incoming,
                expected_previous_run_id=case.incumbent,
            )
            pointer = await archive._pointer(session, case.destination_schema, importer)
            assert (pointer["current_run_id"], pointer["previous_run_id"]) == (case.incumbent, case.incoming)
            assert (await archive._pointer(session, case.destination_schema, case.other))[
                "current_run_id"
            ] == case.unrelated


@pytest.mark.asyncio
async def test_native_source_retention_excludes_exact_pins_until_release(monkeypatch, tmp_path):
    import importlib
    from datetime import datetime

    from process import massachusetts_profile_store
    from process import provider_profile_source_store as shared

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    database = Database(engine=engine, session_factory=sessions)
    monkeypatch.setattr(shared, "db", database)
    monkeypatch.setattr(importlib.import_module("process.florida_mqa_profile"), "db", database)
    importer = "massachusetts-borim-profile"
    prepared = None
    try:
        async with sessions() as session, session.begin():
            await _create_family(session, "mrf")
            incoming = await _seed(session, "mrf", importer)
        async with sessions() as session, session.begin():
            prepared = await archive.prepare_source(
                session, importer_id=importer, schema="mrf", run_id=incoming, dataset_id=uuid4()
            )
        async with sessions() as session, session.begin():
            await _seed(session, "mrf", importer)
            newest = await _seed(session, "mrf", importer)
            await session.execute(
                text("UPDATE mrf.provider_profile_import_run SET started_at=:started WHERE run_id=:run"),
                {"started": datetime(2026, 1, 3), "run": newest},
            )
        receipt = await massachusetts_profile_store._store.retain_source_history(tmp_path)
        assert incoming in receipt["protected_audit_run_ids"]
        assert incoming not in receipt["deleted_run_ids"]
        async with sessions() as session, session.begin():
            await archive.release_source_pin(
                session, schema="mrf", importer_id=importer, run_id=incoming, pin_id=prepared.ownership.dataset_id
            )
        receipt = await massachusetts_profile_store._store.retain_source_history(tmp_path)
        assert incoming in receipt["deleted_run_ids"]
        async with sessions() as session, session.begin():
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM mrf.provider_profile_fact WHERE run_id=:run"), {"run": incoming}
                )
                == 0
            )
            assert await archive._run(session, "mrf", incoming) is not None
    finally:
        async with sessions() as session, session.begin():
            if prepared is not None:
                await archive.cleanup_stage(session, prepared.ownership)
            await _drop_family(session, "mrf")
        await engine.dispose()


async def _assign_adoption_roles(case, ordinary_role, publisher_role):
    """Give an inheriting publisher stage ownership and ordinary native ownership."""
    async with case.sessions() as session, session.begin():
        for role in (ordinary_role, publisher_role):
            await session.execute(text(f'CREATE ROLE "{role}" NOLOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE'))
        await session.execute(text(f'GRANT "{ordinary_role}" TO "{publisher_role}"'))
        for schema, owner, names in (
            (
                case.destination_schema,
                ordinary_role,
                (*archive.TABLES, "provider_profile_source_publication", archive.pins.TABLE),
            ),
            (
                case.prepared.ownership.schema_name,
                publisher_role,
                tuple(name for name, _ in case.prepared.ownership.relation_oids),
            ),
        ):
            await session.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{owner}"'))
            for name in names:
                await session.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{owner}"'))
        for function in (
            "provider_profile_pinned_run_guard",
            "provider_profile_pinned_truncate_guard",
            "provider_profile_attached_pin_guard",
        ):
            await session.execute(
                text(f'ALTER FUNCTION "{case.destination_schema}".{function}() OWNER TO "{ordinary_role}"')
            )
        await session.execute(
            text(
                f'UPDATE "{case.destination_schema}".provider_profile_import_run '
                "SET started_at=:started WHERE run_id=:run"
            ),
            {"started": datetime(2026, 1, 3), "run": case.incumbent},
        )
        case.activation_by_field["sealed_owner_oid"] = await session.scalar(
            text("SELECT oid FROM pg_roles WHERE rolname=:role"), {"role": publisher_role}
        )


async def _exercise_inherited_adoption(case, ordinary_role, publisher_role):
    """Prepare, clean, reactivate and roll back under the role that owns the stage."""
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{publisher_role}"'))
        await archive.prepare_activation(session, **case.activation_by_field)
        assert [
            pin["purpose"]
            for pin in await archive._pin_group(session, case.destination_schema, case.activation_by_field["pin_id"])
        ] == ["adoption"]
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{ordinary_role}"'))
        assert (
            await session.scalar(
                text(
                    f'SELECT count(*) FROM "{case.destination_schema}".provider_profile_source_pin '
                    "WHERE purpose='adoption'"
                )
            )
            == 1
        )
        assert await archive._run(session, case.destination_schema, case.incoming) is not None
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{publisher_role}"'))
        assert (
            await archive.cleanup_adoption(
                session,
                schema=case.destination_schema,
                importer_id=case.importer,
                run_id=case.incoming,
                pin_id=case.activation_by_field["pin_id"],
            )
            == "released"
        )
        assert await archive._run(session, case.destination_schema, case.incoming) is None
        validation = await archive.prepare_activation(session, **case.activation_by_field)
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{publisher_role}"'))
        await archive.activate_validated_result(session, validation=validation, **case.activation_by_field)
        await archive.rollback_result(
            session,
            schema=case.destination_schema,
            importer_id=case.importer,
            expected_current_run_id=case.incoming,
            expected_previous_run_id=case.incumbent,
        )
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{ordinary_role}"'))
        await session.execute(
            text(
                f'UPDATE "{case.destination_schema}".provider_profile_source_publication '
                "SET previous_run_id=NULL WHERE source_key=:source"
            ),
            {"source": archive.SOURCES[case.importer][0]},
        )


async def _assert_ordinary_adoption_retention(case, ordinary_role, publisher_role, monkeypatch, tmp_path):
    """The native source GC protects a sealed run until publisher release."""
    import importlib

    from process import massachusetts_profile_store as massachusetts
    from process import provider_profile_source_store as shared

    florida = importlib.import_module("process.florida_mqa_profile")
    role_engine = create_async_engine(_database_url(), connect_args={"server_settings": {"role": ordinary_role}})
    role_sessions = async_sessionmaker(role_engine, expire_on_commit=False)
    try:
        with monkeypatch.context() as patch:
            runtime_db = Database(engine=role_engine, session_factory=role_sessions)
            patch.setattr(shared, "db", runtime_db)
            patch.setattr(florida, "db", runtime_db)
            for model in (*archive.MODELS, models.ProviderProfileSourcePublication, models.ProviderProfileSourcePin):
                patch.setattr(model.__table__, "schema", case.destination_schema)
            receipt = await massachusetts._store.retain_source_history(tmp_path)
            assert case.incoming in receipt["protected_audit_run_ids"]
            assert case.incoming not in receipt["deleted_run_ids"]
            async with case.sessions() as session, session.begin():
                await session.execute(text(f'SET LOCAL ROLE "{publisher_role}"'))
                assert (
                    await archive.release_source_pin(
                        session,
                        schema=case.destination_schema,
                        importer_id=case.importer,
                        run_id=case.incoming,
                        pin_id=case.activation_by_field["pin_id"],
                    )
                    == "released"
                )
            receipt = await massachusetts._store.retain_source_history(tmp_path)
            assert case.incoming in receipt["deleted_run_ids"]
    finally:
        await role_engine.dispose()


async def _drop_adoption_roles(case, ordinary_role, publisher_role):
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'REASSIGN OWNED BY "{publisher_role}","{ordinary_role}" TO CURRENT_USER'))
        await session.execute(text(f'DROP OWNED BY "{publisher_role}","{ordinary_role}"'))
        await session.execute(text(f'DROP ROLE "{publisher_role}"'))
        await session.execute(text(f'DROP ROLE "{ordinary_role}"'))


@pytest.mark.asyncio
async def test_inherited_publisher_adoption_rollback_and_ordinary_retention(monkeypatch, tmp_path):
    """Native publisher seals and ordinary retention coexist across activation."""
    async with _prepared_case("massachusetts-borim-profile") as case:
        token = uuid4().hex
        ordinary_role = "profile_owner_" + token
        publisher_role = "profile_publisher_" + token
        await _assign_adoption_roles(case, ordinary_role, publisher_role)
        try:
            await _exercise_inherited_adoption(case, ordinary_role, publisher_role)
            await _assert_ordinary_adoption_retention(case, ordinary_role, publisher_role, monkeypatch, tmp_path)
        finally:
            await _drop_adoption_roles(case, ordinary_role, publisher_role)


@pytest.mark.asyncio
async def test_unpublished_cleanup_and_active_conflict():
    async with _prepared_case("massachusetts-borim-profile") as case:
        with pytest.raises(archive.SourceProfileArchiveError, match="source is active"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    text(
                        f"UPDATE \"{case.destination_schema}\".provider_profile_import_run SET status='running' WHERE run_id=:run"
                    ),
                    {"run": case.incumbent},
                )
                await archive.prepare_activation(session, **case.activation_by_field)
        async with case.sessions() as session, session.begin():
            await archive.prepare_activation(session, **case.activation_by_field)
        async with case.sessions() as session, session.begin():
            assert (
                await archive.cleanup_adoption(
                    session,
                    schema=case.destination_schema,
                    importer_id=case.importer,
                    run_id=case.incoming,
                    pin_id=case.activation_by_field["pin_id"],
                )
                == "released"
            )
            assert await archive._run(session, case.destination_schema, case.incoming) is None
            assert (await archive._pointer(session, case.destination_schema, case.importer))[
                "current_run_id"
            ] == case.incumbent
            assert (
                await archive.cleanup_adoption(
                    session,
                    schema=case.destination_schema,
                    importer_id=case.importer,
                    run_id=case.incoming,
                    pin_id=case.activation_by_field["pin_id"],
                )
                == "already_released"
            )
