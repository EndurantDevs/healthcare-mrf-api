# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real SELECT-only projection parity; publication/profile custody is separate."""

import datetime as dt
import importlib
import os
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import event, text

from db.models import (
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryLocation,
    ProviderDirectorySourceResource,
)
from process.provider_directory_dataset_rehydrate_pin import pinned_current_source
from process.provider_directory_dataset_rehydrate_projection import compatibility_projection
from process.provider_directory_generic_source_policy import source_transaction
from tests.test_provider_directory_dataset_rehydrate_pin_db import _case as source_case

fhir = importlib.import_module("process.provider_directory_fhir")

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        os.getenv("HP_REHYDRATION_PROJECTION_NATIVE") != "1", reason="requires explicit disposable projection fixture"
    ),
]
_MODELS = (
    ProviderDirectoryLocation,
    ProviderDirectoryCanonicalResource,
    ProviderDirectorySourceResource,
)
_WHEN = dt.datetime(2026, 7, 20, 12, 30)


@asynccontextmanager
async def _case(monkeypatch, count=3):
    async with source_case(monkeypatch) as case:
        async with source_transaction(case.database, isolation="repeatable read"):
            for model in _MODELS:
                statement = fhir._provider_directory_artifact_scope_table_sql(model, case.schema, model.__tablename__)
                await case.database.status(statement.replace("CREATE UNLOGGED TABLE", "CREATE TABLE", 1))
                for statement in fhir._artifact_scope_pk_sql(model, case.schema, model.__tablename__):
                    await case.database.status(statement)
            await case.database.status(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{case.schema}" TO "{case.reader}"')
            await _seed(case.database, case.schema)
            await _replace_source_count(case, count)
            await case.database.status(
                f'UPDATE "{case.schema}".provider_directory_endpoint_dataset SET resource_count=:count', count=count
            )
        async with pinned_current_source(case.runtime, case.request, directory=fhir) as pinned:
            case.pinned, case.scope, case.engine = pinned, pinned.scope, case.database._reader_database.engine
            yield case


async def _seed(database, schema):
    for resource_id in ("location-1", "location-2", "location-3"):
        await database.status(
            f'INSERT INTO "{schema}".provider_directory_dataset_resource '
            "(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
            "VALUES ('dataset','Location',CAST(:id AS varchar),repeat('a',64),json_build_object('resource_id',CAST(:id AS varchar),'name','restored'))",
            id=resource_id,
        )
    for resource_id, source_id, base, root, digest in (
        ("location-1", "source", "https://fixture.test/fhir", "old", "b"),
        ("location-2", "source", "https://fixture.test/fhir", "old", "a"),
        ("stale", "source", "https://fixture.test/fhir", "root", "c"),
        ("foreign-run", "source", "https://fixture.test/fhir", "another-root", "d"),
        ("location-1", "unrelated", "https://other.test/fhir", "other-root", "e"),
    ):
        await database.status(
            f'INSERT INTO "{schema}".provider_directory_location '
            "(source_id,resource_id,name,last_seen_run_id,observed_at,updated_at) "
            "VALUES (:source,:id,'incumbent',:root,:observed,:observed)",
            source=source_id,
            id=resource_id,
            root=root,
            observed=_WHEN - dt.timedelta(days=1),
        )
        await database.status(
            f'INSERT INTO "{schema}".provider_directory_canonical_resource '
            "(canonical_api_base,resource_type,resource_id,payload_hash,payload_json,first_seen_run_id,last_seen_run_id) "
            "VALUES (:base,'Location',:id,repeat(:digest,64),json_build_object('cached',true),'first',:root)",
            base=base,
            id=resource_id,
            digest=digest,
            root=root,
        )
        await database.status(
            f'INSERT INTO "{schema}".provider_directory_source_resource '
            "(source_id,canonical_api_base,resource_type,resource_id,last_seen_run_id,observed_at) "
            "VALUES (:source,:base,'Location',:id,:root,:observed)",
            source=source_id,
            base=base,
            id=resource_id,
            root=root,
            observed=_WHEN - dt.timedelta(days=1),
        )


async def _read(case, model, connection):
    statement, params = compatibility_projection(fhir, case.pinned, model, ("Location",))
    result = await connection.execute(text(statement), params)
    return [dict(row_by_column) for row_by_column in result.mappings()]


@pytest.mark.parametrize(
    "model", [ProviderDirectoryLocation, ProviderDirectoryCanonicalResource, ProviderDirectorySourceResource]
)
async def test_native_projection_repairs_exact_keys_and_preserves_unrelated_rows(monkeypatch, model):
    async with _case(monkeypatch) as case, case.engine.connect() as connection, connection.begin():
        await connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        assert (
            await connection.execute(
                text("SELECT session_user=current_user AND current_user=:reader"), {"reader": case.reader}
            )
        ).scalar()
        assert not (
            await connection.execute(
                text("SELECT has_table_privilege(current_user,:relation,'INSERT,UPDATE,DELETE')"),
                {"relation": f"{case.schema}.{model.__tablename__}"},
            )
        ).scalar()
        projected_rows = await _read(case, model, connection)
        assert len(projected_rows) == 5
        selected_rows = [
            row_by_column
            for row_by_column in projected_rows
            if row_by_column.get("source_id", "source") == "source"
            and row_by_column.get("canonical_api_base", case.scope.canonical_api_base) == case.scope.canonical_api_base
        ]
        by_id = {row_by_column["resource_id"]: row_by_column for row_by_column in selected_rows}
        assert set(by_id) == {"location-1", "location-2", "location-3", "foreign-run"}
        assert by_id["foreign-run"]["last_seen_run_id"] == "another-root"
        assert all(by_id[key]["last_seen_run_id"] == "root" for key in ("location-1", "location-2", "location-3"))
        if model is ProviderDirectoryLocation:
            assert all(
                by_id[key]["name"] == "restored" and by_id[key]["observed_at"] == _WHEN
                for key in ("location-1", "location-2", "location-3")
            )
        if model is ProviderDirectoryCanonicalResource:
            assert by_id["location-1"]["payload_json"] is None
            assert by_id["location-2"]["payload_json"] == {"cached": True}
            assert by_id["location-1"]["first_seen_run_id"] == "first"
            assert by_id["location-3"]["first_seen_run_id"] == "root"
        assert (
            await connection.execute(text(f'SELECT count(*) FROM "{case.schema}"."{model.__tablename__}"'))
        ).scalar() == 5


async def test_native_projection_preserves_unselected_resource_type(monkeypatch):
    async with _case(monkeypatch) as case:
        await case.admin.status(
            f'INSERT INTO "{case.schema}".provider_directory_canonical_resource '
            "(canonical_api_base,resource_type,resource_id,last_seen_run_id,payload_hash) "
            "VALUES (:base,'Practitioner','practitioner','root',repeat('f',64))",
            base=case.scope.canonical_api_base,
        )
        async with case.engine.connect() as connection:
            projected_rows = await _read(case, ProviderDirectoryCanonicalResource, connection)
        untouched_rows = [
            row_by_column for row_by_column in projected_rows if row_by_column["resource_type"] == "Practitioner"
        ]
        assert len(untouched_rows) == 1 and untouched_rows[0]["payload_hash"] == "f" * 64


@pytest.mark.parametrize("native_type", ["json", "jsonb"])
async def test_native_projection_preserves_both_supported_json_storage_types(monkeypatch, native_type):
    async with _case(monkeypatch) as case:
        target = f'"{case.schema}".provider_directory_location'
        if native_type == "jsonb":
            await case.admin.status(f"ALTER TABLE {target} ALTER COLUMN fhir_meta TYPE jsonb USING fhir_meta::jsonb")
        await case.admin.status(
            f"UPDATE {target} SET fhir_meta=CAST(:value AS {native_type}) WHERE resource_id='foreign-run'",
            value='{"preserved":[1,true]}',
        )
        async with case.engine.connect() as connection:
            projected_rows = await _read(case, ProviderDirectoryLocation, connection)
        preserved = next(
            row_by_column for row_by_column in projected_rows if row_by_column["resource_id"] == "foreign-run"
        )
        assert preserved["fhir_meta"] == {"preserved": [1, True]}
        assert any(row_by_column["resource_id"] == "location-3" for row_by_column in projected_rows)


async def _replace_source_count(case, count):
    await case.database.status(f'DELETE FROM "{case.schema}".provider_directory_dataset_resource')
    await case.database.status(
        f'INSERT INTO "{case.schema}".provider_directory_dataset_resource '
        "(dataset_id,resource_type,resource_id,payload_hash,payload_json) "
        "SELECT 'dataset','Location','location-'||ordinal,repeat('a',64),"
        "json_build_object('resource_id','location-'||ordinal,'name','restored') "
        "FROM generate_series(1,CAST(:count AS integer)) AS ordinal",
        count=count,
    )


async def test_native_one_and_125_records_are_one_indexable_set_read(monkeypatch):
    counts = []
    for count in (1, 125):
        async with _case(monkeypatch, count=count) as case:
            trips = []

            def observe(_connection, _cursor, statement, _parameters, _context, _many):
                trips.append(statement)

            event.listen(case.engine.sync_engine, "before_cursor_execute", observe)
            async with case.engine.connect() as connection:
                projected_rows = await _read(case, ProviderDirectoryCanonicalResource, connection)
                counts.append(len(trips))
                repaired_rows = [
                    row_by_column for row_by_column in projected_rows if row_by_column["last_seen_run_id"] == "root"
                ]
                assert len(repaired_rows) == count
                assert len({row_by_column["resource_id"] for row_by_column in repaired_rows}) == count
                assert any(row_by_column["resource_id"] == "foreign-run" for row_by_column in projected_rows)
                await connection.execute(text("SET LOCAL enable_seqscan=off"))
                statement, params = compatibility_projection(
                    fhir, case.pinned, ProviderDirectoryCanonicalResource, ("Location",)
                )
                plan = (
                    await connection.execute(text("EXPLAIN (FORMAT JSON,COSTS OFF) " + statement), params)
                ).scalar()[0]
                serialized = str(plan)
                assert "SubPlan" not in serialized and "Index" in serialized
                assert case.pinned.reader.tables_by_root["provider_directory_dataset_resource"]["name"] in serialized
    assert counts == [1, 1]


async def test_native_rr_preservation_does_not_hide_a_concurrent_incumbent_writer(monkeypatch):
    async with _case(monkeypatch) as case:
        async with case.engine.connect() as connection, connection.begin():
            await connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            before = await _read(case, ProviderDirectoryLocation, connection)
            await case.admin.status(
                f"UPDATE \"{case.schema}\".provider_directory_location SET name='changed-concurrently' "
                "WHERE source_id='source' AND resource_id='foreign-run'",
            )
            assert await _read(case, ProviderDirectoryLocation, connection) == before
        async with case.engine.connect() as connection:
            after = await _read(case, ProviderDirectoryLocation, connection)
        changed = next(row_by_column for row_by_column in after if row_by_column["resource_id"] == "foreign-run")
        assert changed["name"] == "changed-concurrently"


async def test_native_projection_uses_pinned_timestamp_after_header_drift(monkeypatch):
    async with _case(monkeypatch) as case:
        await case.admin.status(
            f"UPDATE \"{case.schema}\".provider_directory_endpoint_dataset SET published_at=timestamp '2026-07-21 12:30:00'"
        )
        async with case.engine.connect() as connection:
            projected = await _read(case, ProviderDirectoryLocation, connection)
        repaired_rows = [row_by_column for row_by_column in projected if row_by_column["last_seen_run_id"] == "root"]
        assert repaired_rows and all(row_by_column["observed_at"] == _WHEN for row_by_column in repaired_rows)
        assert case.pinned.scope.published_at == _WHEN


async def test_native_preservation_reader_releases_output_before_cut(monkeypatch):
    async with _case(monkeypatch) as case:
        async with case.engine.connect() as connection:
            await _read(case, ProviderDirectoryLocation, connection)
            async with case.admin.engine.connect() as probe, probe.begin():
                with pytest.raises(Exception, match="could not obtain lock"):
                    await probe.execute(
                        text(f'LOCK TABLE "{case.schema}".provider_directory_location IN ACCESS EXCLUSIVE MODE NOWAIT')
                    )
        async with source_transaction(case.database, isolation="read committed"):
            await case.database.status(
                f'LOCK TABLE "{case.schema}".provider_directory_location IN ACCESS EXCLUSIVE MODE NOWAIT'
            )
        assert await case.pinned.reader.scalar("SELECT 1") == 1


async def test_native_projection_refuses_altered_or_expired_pin(monkeypatch):
    from dataclasses import replace

    from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError

    async with _case(monkeypatch) as case:
        forged = replace(case.pinned, scope=replace(case.scope, dataset_id="different"))
        with pytest.raises(DatasetRehydrationError, match="reader_unpinned"):
            compatibility_projection(fhir, forged, ProviderDirectoryLocation, ("Location",))
        pinned = case.pinned
    with pytest.raises(DatasetRehydrationError, match="reader_owner_changed"):
        compatibility_projection(fhir, pinned, ProviderDirectoryLocation, ("Location",))
