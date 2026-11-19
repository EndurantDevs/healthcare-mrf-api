# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native publication keeps candidates separate and exchanges complete logical families."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import re
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from api.reference_family_reads import catalog_read_table, lock_scoped_catalog_reader
from db.models import CodeCatalog, ImportRun
from process import reference_family_archive as native
from process import scoped_catalog_handoff as handoff
from process import scoped_catalog_named_generation as generations
from process import scoped_catalog_named_storage as storage
from process.scoped_catalog_bootstrap import bootstrap_catalog_slice
from process.scoped_catalog_layout import DATA_SCHEMA, DATA_SCHEMA_ENV, FENCE_OWNER_ROLE, FENCE_SCHEMA, FENCE_VIEW
from process.scoped_catalog_named_cutover import require_no_legacy_collision, rollback
from process.scoped_catalog_publisher import PREFIX, ManagedAttempt, PublisherSettings, run_publisher
from tests.scoped_catalog_native_fixture import catalog_actors
from tests.scoped_catalog_split_fixture import _split_roles


def _dsn():
    value = os.getenv("HLTHPRT_SCOPED_CATALOG_TEST_DSN") or os.getenv("HLTHPRT_CODE_SETS_PUBLICATION_TEST_DSN")
    if not value:
        pytest.skip("HLTHPRT_SCOPED_CATALOG_TEST_DSN is not set")
    url = make_url(value)
    if (
        url.host not in {"127.0.0.1", "localhost"}
        or not 1024 <= (url.port or 0) <= 65535
        or re.fullmatch(r"hc_scoped_reference_[0-9a-f]{32}", url.database or "") is None
    ):
        pytest.fail("scoped catalog tests require a dedicated local database")
    return url.set(drivername="postgresql+asyncpg")


def _migrations(connection):
    root = Path(__file__).parents[1] / "alembic" / "versions"
    for filename in ("20260923010000_code_sets_result_generation.py", "20260923021000_ms_drg_result_generation.py"):
        spec = importlib.util.spec_from_file_location("synthetic_catalog_migration", root / filename)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with Operations.context(MigrationContext.configure(connection)):
            module.upgrade()


async def _seed(engine, schema):
    metadata = MetaData()
    for model in (*storage.MODELS, ImportRun):
        model.__table__.to_metadata(metadata, schema=schema)
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        await connection.run_sync(metadata.create_all)
        await connection.run_sync(_migrations)
        await connection.execute(
            text(
                f"INSERT INTO \"{schema}\".code_catalog (code_system,code,source) VALUES ('OTHER','1','synthetic-reference')"
            )
        )


def _payload(importer):
    if importer == "code-sets":
        return {
            "code_catalog": [
                {
                    "code_system": "POS",
                    "code": "23",
                    "source": "cms_place_of_service_code_set",
                    "display_name": "Emergency",
                },
                {
                    "code_system": "RC",
                    "code": "0450",
                    "source": "cms_bluebutton_revenue_center_code",
                    "display_name": "Emergency",
                },
                {
                    "code_system": "MODIFIER",
                    "code": "26",
                    "source": "cms_hcpcs_modifier_reference",
                    "display_name": "Professional",
                },
            ]
        }
    return {
        "code_catalog": [
            {
                "code_system": "MS_DRG",
                "code": "001",
                "source": "cms_ms_drg_definitions_manual",
                "display_name": "Synthetic group",
            }
        ],
        "code_synonym": [],
        "code_relationship": [],
    }


async def _prepare(actors, schema, importer):
    context_by_field = {
        "control_run_id": uuid4().hex,
        "_control_attempt_id": uuid4().hex,
        "_control_attempt_started_at": "2026-01-01T00:00:00+00:00",
    }
    async with actors.publisher.begin() as session:
        await session.execute(
            text(
                f'INSERT INTO "{schema}".import_run (run_id,engine,importer,node_id,status,progress,params) '
                "VALUES (:run,'healthcare-mrf-api',:importer,'synthetic-node','running',CAST(:progress AS json),'{}')"
            ),
            {
                "run": context_by_field["control_run_id"],
                "importer": importer,
                "progress": json.dumps(
                    {
                        "attempt_id": context_by_field["_control_attempt_id"],
                        "attempt_started_at": context_by_field["_control_attempt_started_at"],
                    }
                ),
            },
        )
    database = SimpleNamespace(transaction=actors.builder.begin)
    ctx_by_field = {"context": context_by_field}
    await handoff.capture_catalog_expected(database, ctx_by_field, importer=importer, schema=schema)
    return await handoff.prepare_catalog_handoff(
        database,
        ctx_by_field,
        importer=importer,
        schema=schema,
        payloads=_payload(importer),
        options={"test_mode": False, "include_relationships": False},
        metrics={"rows": 1},
    )


def _attempt(candidate):
    generation = candidate["expected_generations"][candidate["importer_id"]]
    return ManagedAttempt(
        *(candidate[key] for key in ("run_id", "attempt_id", "attempt_started_at", "node_id")),
        generation["local_lineage_id"],
        generation["local_generation"],
    )


async def _configure_roles(engine, schema, actors):
    async with engine.begin() as connection:
        await connection.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{actors.roles["owner"]}"'))
        for table in (*(model.__tablename__ for model in storage.MODELS), "import_run", *generations.TABLES.values()):
            await connection.execute(text(f'ALTER TABLE "{schema}"."{table}" OWNER TO "{actors.roles["owner"]}"'))
            await connection.execute(text(f'GRANT SELECT ON "{schema}"."{table}" TO "{actors.roles["builder"]}"'))
        await connection.execute(text(f'GRANT UPDATE ON "{schema}".import_run TO "{actors.roles["builder"]}"'))
        await connection.execute(
            text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{actors.roles["builder"]}","{actors.roles["publisher"]}"')
        )
        await connection.execute(text(f'GRANT CREATE ON SCHEMA "{schema}" TO "{actors.roles["owner"]}"'))


@asynccontextmanager
async def _resources(monkeypatch):
    engine = create_async_engine(_dsn(), poolclass=NullPool, hide_parameters=True)
    schema = "catalog_" + uuid4().hex
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    try:
        async with catalog_actors(engine, (schema,)) as actors:
            await _seed(engine, schema)
            await _configure_roles(engine, schema, actors)
            publisher_url = actors.publisher.kw["bind"].url
            settings = PublisherSettings(
                publisher_url.render_as_string(hide_password=False),
                schema,
                actors.roles["owner"],
                (actors.roles["builder"],),
                1024**2,
                30,
            )
            yield actors, settings
    finally:
        await engine.dispose()


async def _assert_cancellation_during_copy(actors, candidate, settings):
    from process.scoped_catalog_named_publication import publish_managed_attempt

    entered, release = asyncio.Event(), asyncio.Event()

    async def held_copy(*args, **kwargs):
        entered.set()
        await release.wait()
        return await native.native_copy_projection(*args, **kwargs)

    async def publish():
        async with actors.publisher.begin() as session:
            return await publish_managed_attempt(
                session,
                _attempt(candidate),
                settings,
                source_copy=native.ReferenceFamilySourceCopy(held_copy, 1024**2, 30),
            )

    pending = asyncio.create_task(publish())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        async with asyncio.timeout(2), actors.builder.begin() as session:
            current = (
                (
                    await session.execute(
                        text(f'SELECT * FROM "{settings.schema}".import_run WHERE run_id=:run'),
                        {"run": candidate["run_id"]},
                    )
                )
                .mappings()
                .one()
            )
            await handoff.request_catalog_cancel(session, current)
    except BaseException:
        release.set()
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        raise
    finally:
        release.set()
    with pytest.raises(RuntimeError, match="attempt changed"):
        await pending
    assert (await run_publisher(settings, _attempt(candidate)))["status"] == "canceled"
    async with actors.builder.begin() as session:
        assert await session.scalar(text(f'SELECT count(*) FROM "{settings.schema}".code_catalog')) == 1
        assert (await generations.read_generations(session, settings.schema, importer=candidate["importer_id"]))[
            candidate["importer_id"]
        ]["local_generation"] == 0


async def _assert_publication_refusals(actors, candidate, settings):
    attempt = _attempt(candidate)
    with pytest.raises(RuntimeError, match="controller generation"):
        await run_publisher(settings, replace(attempt, expected_generation=99))
    with pytest.raises(RuntimeError):
        await run_publisher(replace(settings, max_copy_bytes=1), attempt)
    companion = "ms-drg" if candidate["importer_id"] == "code-sets" else "code-sets"
    async with actors.publisher.begin() as session:
        await session.execute(
            text(f'UPDATE "{settings.schema}"."{generations.TABLES[companion]}" SET local_generation=1')
        )
    with pytest.raises(RuntimeError, match="generation vector changed"):
        await run_publisher(settings, attempt)
    async with actors.publisher.begin() as session:
        await session.execute(
            text(f'UPDATE "{settings.schema}"."{generations.TABLES[companion]}" SET local_generation=0')
        )
    with pytest.raises(RuntimeError, match="synthetic late failure"):
        async with actors.publisher.begin() as session:
            await handoff.publish_catalog_handoff(
                session,
                candidate,
                settings=settings,
                source_copy=native.ReferenceFamilySourceCopy(native.native_copy_projection, 1024**2, 30),
            )
            raise RuntimeError("synthetic late failure")
    async with actors.publisher.begin() as session:
        assert await handoff.read_catalog_handoff_outcome(session, candidate) is None
        assert await session.scalar(text(f'SELECT count(*) FROM "{settings.schema}".code_catalog')) == 1


async def _assert_copied_metadata(actors, settings, importer):
    schema = settings.schema
    async with actors.engine.begin() as connection:
        for model in storage.MODELS:
            await connection.execute(
                text(f'ALTER TABLE "{schema}"."{model.__tablename__}" OWNER TO "{actors.roles["publisher"]}"')
            )
        if importer == "code-sets":
            await connection.execute(text(f'UPDATE "{schema}".code_sets_result_generation SET code_catalog_oid=999'))
        else:
            await connection.execute(
                text(
                    f"UPDATE \"{schema}\".ms_drg_result_generation SET receipt=jsonb_set(receipt,'{{tables,0,relation_oid}}','999')"
                )
            )
    async with actors.builder.begin() as session:
        await generations.validate_published_content(
            session, schema, await generations.read_generations(session, schema, importer=importer)
        )
        assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".code_catalog')) == 1 + len(
            _payload(importer)["code_catalog"]
        )
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(text(f'DELETE FROM "{schema}".code_catalog'))


async def _assert_populated_rollback(actors, settings, importer):
    first = await _prepare(actors, settings.schema, importer)
    await run_publisher(settings, _attempt(first[handoff.METRIC]))
    second = await _prepare(actors, settings.schema, importer)
    await run_publisher(settings, _attempt(second[handoff.METRIC]))
    async with actors.publisher.begin() as session:
        receipt = await handoff.read_catalog_handoff_outcome(session, second[handoff.METRIC])
        restored = await rollback(session, receipt, settings)
        assert restored[importer]["local_generation"] == 5


async def _copy_archive_tables(session, source, destination, models):
    for model in models:
        columns = tuple(model.__table__.columns.keys())
        projection = (
            "SELECT "
            + ",".join(native._quoted(name) for name in columns)
            + " FROM "
            + storage.qualified(source, model.__tablename__)
        )
        await native.native_copy_projection(
            session,
            projection,
            schema_name=destination,
            table_name=model.__tablename__,
            columns=columns,
            max_bytes=1024**2,
            timeout=30,
        )


async def _codes_archive_roundtrip(actors, source_schema, destination):
    from process import code_sets_result_archive as archive

    capability = native.ReferenceFamilySourceCopy(native.native_copy_projection, 1024**2, 30)
    async with actors.builder.begin() as session:
        original = await archive.prepare_source(session, source_schema, uuid4(), source_copy=capability)
    original = replace(original, schema_oid=1, catalog_oid=2)
    async with actors.publisher.begin() as session:
        await archive.verify_stage(session, original)
        incoming, predecessor = await archive.precreate_restore(
            session, destination=destination, dataset_id=uuid4(), manifest=original.source_generation.as_dict()
        )
        await _copy_archive_tables(session, original.schema_name, incoming.schema_name, storage.MODELS[:1])
        await archive.complete_restore(session, incoming, predecessor_oid=predecessor)
        expected = await archive.read_generation(session, destination)
        prepared = await archive.prepare_predecessor(
            session,
            destination=destination,
            stage=incoming,
            expected=expected,
            precreated_predecessor_oid=predecessor,
            source_copy=capability,
        )
    async with actors.publisher.begin() as session:
        activation = await archive.activate_stage(
            session, destination=destination, prepared=prepared, source_copy=capability
        )
        assert activation.current.origin_lineage_id == original.source_generation.origin_lineage_id
    async with actors.builder.begin() as session:
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(text(f'DELETE FROM "{destination}".code_catalog'))
    async with actors.publisher.begin() as session:
        restored = await archive.rollback_activation(
            session, destination=destination, activation=activation, source_copy=capability
        )
        assert restored.local_generation == 2 and restored.origin_generation is None
        await archive.cleanup_stage(session, incoming, predecessor_oid=predecessor)
        await archive.cleanup_stage(session, original)
    assert original.schema_oid == 1 and original.catalog_oid == 2


async def _drg_archive_roundtrip(actors, source, destination):
    from process import ms_drg_result_archive as archive

    capability = native.ReferenceFamilySourceCopy(native.native_copy_projection, 1024**2, 30)
    async with actors.builder.begin() as session:
        original, manifest = await archive.prepare_source(session, source, uuid4(), source_copy=capability)
    original.update(schema_oid=1, relation_oids=(2, 3, 4))
    async with actors.publisher.begin() as session:
        await archive.verify_stage(session, original, manifest, receiving=False)
        incoming = await archive.precreate_restore(session, destination, uuid4(), manifest)
        await _copy_archive_tables(session, original["schema_name"], incoming["schema_name"], storage.MODELS)
        await archive.complete_restore(session, incoming, manifest)
        prepared = await archive.prepare_predecessor(session, destination, incoming, manifest, source_copy=capability)
    async with actors.publisher.begin() as session:
        activation = await archive.activate_stage(
            session, destination, incoming, manifest, prepared, source_copy=capability
        )
        assert str(activation["current"]["origin_lineage_id"]) == manifest["origin_lineage_id"]
    async with actors.builder.begin() as session:
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(text(f'DELETE FROM "{destination}".code_catalog'))
    async with actors.publisher.begin() as session:
        restored = await archive.rollback_activation(session, destination, activation, source_copy=capability)
        assert restored["local_generation"] == 2 and restored["origin_generation"] is None
        await archive.cleanup_stage(session, incoming, receiving=True)
        await archive.cleanup_stage(session, original, receiving=False)
    assert original["schema_oid"] == 1 and original["relation_oids"] == (2, 3, 4)


async def _assert_archive_roundtrip(monkeypatch, actors, settings, importer):
    destination = settings.schema + "_copy"
    async with actors.builder.begin() as session:
        before = await session.scalar(
            text(
                f'SELECT row_to_json(record)::text FROM "{settings.schema}"."{generations.TABLES[importer]}" record WHERE id=1'
            )
        )
    with monkeypatch.context() as environment:
        environment.setenv("HLTHPRT_DB_SCHEMA", destination)
        await _seed(actors.engine, destination)
        await _configure_roles(actors.engine, destination, actors)
        values_by_name = {
            "DSN": settings.dsn,
            "SCHEMA": destination,
            "OWNER_ROLE": settings.owner_role,
            "READER_ROLES": json.dumps(settings.reader_roles),
            "COPY_MAX_BYTES": str(settings.max_copy_bytes),
            "TIMEOUT_SECONDS": str(settings.timeout_seconds),
        }
        for name, setting in values_by_name.items():
            environment.setenv(PREFIX + name, setting)
        if importer == "code-sets":
            await _codes_archive_roundtrip(actors, settings.schema, destination)
        else:
            await _drg_archive_roundtrip(actors, settings.schema, destination)
    async with actors.builder.begin() as session:
        after = await session.scalar(
            text(
                f'SELECT row_to_json(record)::text FROM "{settings.schema}"."{generations.TABLES[importer]}" record WHERE id=1'
            )
        )
        assert before == after


async def _old_handoff(actors, candidate):
    from copy import deepcopy

    from process import code_sets_result_archive as codes
    from process.ms_drg_result_generation import _table_shape
    from process.scoped_catalog_named_publication import digest
    from process.scoped_catalog_retention import generation_value

    old = deepcopy(candidate)
    old.update(contract="scoped-catalog-handoff.v1", database_oid=991, import_run_oid=992, input_owner_oid=993)
    old.pop("expected_tables")
    models = handoff.publication.input_spec(old["importer_id"]).model_types
    old["input"].update(
        schema_oid=994, relation_oids=[[model.__tablename__, 995 + i] for i, model in enumerate(models)]
    )
    async with actors.publisher.begin() as session:
        if old["importer_id"] == "code-sets":
            # The v1 producer serialized the full CodeSets record but omitted inactive MS-DRG.
            old["expected_generations"]["code-sets"] = generation_value(
                await codes.read_generation(session, old["schema_name"])
            )
            if old["expected_generations"].get("ms-drg", {}).get("origin_generation") is None:
                old["expected_generations"].pop("ms-drg", None)
        for entry, model in zip(old["tables"], models, strict=True):
            oid = await native._relation_oid(session, old["input"]["schema_name"], model.__tablename__)
            entry["schema_sha256"] = await _table_shape(session, oid, model)
        old["handoff_sha256"] = digest(
            {key: field_value for key, field_value in old.items() if key != "handoff_sha256"}
        )
        await session.execute(
            text(
                f"UPDATE {storage.qualified(old['schema_name'], 'import_run')} "
                "SET metrics=jsonb_set(metrics::jsonb,'{scoped_catalog_handoff}',CAST(:handoff AS jsonb))::json "
                "WHERE run_id=:run"
            ),
            {"handoff": json.dumps(old), "run": old["run_id"]},
        )
    return old


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
@pytest.mark.parametrize("old_contract", [False, True])
async def test_named_managed_publication_rollback_and_native_permissions(monkeypatch, importer, old_contract):
    async with _resources(monkeypatch) as (actors, settings):
        prepared = await _prepare(actors, settings.schema, importer)
        if old_contract:
            prepared[handoff.METRIC] = await _old_handoff(actors, prepared[handoff.METRIC])
        await _assert_publication_refusals(actors, prepared[handoff.METRIC], settings)
        await _assert_cancellation_during_copy(actors, prepared[handoff.METRIC], settings)
        outcome = await _prepare(actors, settings.schema, importer)
        candidate = outcome[handoff.METRIC]
        assert candidate["contract"] == "scoped-catalog-handoff.v2"
        assert set(candidate["input"]) == {"dataset_id", "schema_name"}
        if old_contract:
            candidate = await _old_handoff(actors, candidate)
        publication = await run_publisher(settings, _attempt(candidate))
        assert publication["status"] == "succeeded" and publication["replayed"] is False
        assert (await run_publisher(settings, _attempt(candidate)))["replayed"] is True
        await _assert_copied_metadata(actors, settings, importer)
        await _assert_archive_roundtrip(monkeypatch, actors, settings, importer)
        async with actors.publisher.begin() as session:
            receipt = await handoff.read_catalog_handoff_outcome(session, candidate)
            restored = await rollback(session, receipt, settings)
            assert restored[importer]["local_generation"] == 2
        async with actors.builder.begin() as session:
            assert await session.scalar(text(f'SELECT count(*) FROM "{settings.schema}".code_catalog')) == 1
        await _assert_populated_rollback(actors, settings, importer)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
async def test_legacy_rows_without_generation_receipts_are_fenced_and_restored(monkeypatch, importer):
    """Legacy data survives first publication and rollback under the complete content fence."""
    async with _resources(monkeypatch) as (actors, settings):
        schema = settings.schema
        async with actors.publisher.begin() as session:
            legacy_rows = [
                {**catalog_row, "display_name": "Legacy " + catalog_row["display_name"]}
                for family in generations.TABLES
                for catalog_row in _payload(family)["code_catalog"]
            ]
            await session.execute(
                text(
                    f'INSERT INTO "{schema}".code_catalog (code_system,code,source,display_name) '
                    "VALUES (:code_system,:code,:source,:display_name)"
                ),
                legacy_rows,
            )
            previous_tables = await storage.table_receipts(session, schema, storage.MODELS)
            previous_generations = await generations.read_generations(session, schema, importer=importer)
        prepared = await _prepare(actors, schema, importer)
        candidate = prepared[handoff.METRIC]
        assert candidate["expected_tables"] == previous_tables
        assert candidate["expected_generations"] == previous_generations
        async with actors.publisher.begin() as session:
            await session.execute(text(f"UPDATE \"{schema}\".code_catalog SET display_name='changed' WHERE code='23'"))
        with pytest.raises(RuntimeError, match="predecessor data or structure changed"):
            await run_publisher(settings, _attempt(candidate))
        async with actors.publisher.begin() as session:
            await session.execute(
                text(f"UPDATE \"{schema}\".code_catalog SET display_name='Legacy Emergency' WHERE code='23'")
            )
        assert (await run_publisher(settings, _attempt(candidate)))["status"] == "succeeded"
        async with actors.publisher.begin() as session:
            receipt = await handoff.read_catalog_handoff_outcome(session, candidate)
            restored = await rollback(session, receipt, settings)
            assert restored[importer] == {**previous_generations[importer], "local_generation": 2}
            assert await storage.table_receipts(session, schema, storage.MODELS) == previous_tables
            await generations.validate_published_content(session, schema, restored)
        next_prepared = await _prepare(actors, schema, importer)
        assert next_prepared[handoff.METRIC]["expected_tables"] == previous_tables
        assert (await run_publisher(settings, _attempt(next_prepared[handoff.METRIC])))["status"] == "succeeded"


async def _persist_old_retained(session, candidate, receipt):
    from copy import deepcopy

    from process.scoped_catalog_named_publication import digest

    importer = candidate["importer_id"]
    old = deepcopy(receipt["retained"])
    old.update(
        contract="scoped-catalog-retained.v1",
        schema_oid=901,
        owner_oid=902,
        database_oid=903,
        generation_oid=904,
        published_oids=[[entry["table"], 910 + i] for i, entry in enumerate(old["tables"])],
    )
    old["current_generation"] = old.pop("published_generations")[importer]
    companion = "ms-drg" if importer == "code-sets" else "code-sets"
    if companion not in candidate["expected_generations"]:
        old["previous_generations"].pop(companion, None)
    for i, entry in enumerate(old["tables"]):
        entry["relation_oid"] = 920 + i
    old["receipt_sha256"] = digest(old)
    prior_schema = old["schema_name"]
    await session.execute(text(f"DROP TABLE {storage.qualified(prior_schema, generations.TABLES[companion])} RESTRICT"))
    await session.execute(
        text(
            f"UPDATE {storage.qualified(prior_schema, old['generation_table'])} SET retained_family=CAST(:receipt AS jsonb) WHERE id=1"
        ),
        {"receipt": json.dumps(old)},
    )
    receipt["retained"] = old
    await session.execute(
        text(
            f"UPDATE {storage.qualified(candidate['schema_name'], 'import_run')} SET metrics=jsonb_set(metrics::jsonb,'{{scoped_catalog_publication}}',CAST(:receipt AS jsonb))::json WHERE run_id=:run"
        ),
        {"receipt": json.dumps(receipt), "run": candidate["run_id"]},
    )
    return old


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
async def test_old_retained_family_keeps_metadata_and_companion_fence(monkeypatch, importer):
    from copy import deepcopy

    async with _resources(monkeypatch) as (actors, settings):
        prepared = await _prepare(actors, settings.schema, importer)
        candidate = await _old_handoff(actors, prepared[handoff.METRIC])
        if importer == "code-sets":
            assert set(candidate["expected_generations"]) == {"code-sets"}
        frozen_candidate = deepcopy(candidate)
        await run_publisher(settings, _attempt(candidate))
        assert candidate == frozen_candidate
        async with actors.publisher.begin() as session:
            receipt = await handoff.read_catalog_handoff_outcome(session, candidate)
            old = await _persist_old_retained(session, candidate, receipt)
            frozen_receipt = deepcopy(receipt)
            if importer == "code-sets":
                assert set(old["previous_generations"]) == {"code-sets"}
            prior_schema = old["schema_name"]
            companion = "ms-drg" if importer == "code-sets" else "code-sets"
        async with actors.builder.begin() as session:
            assert await handoff.read_catalog_handoff_outcome(session, candidate) == receipt
            assert (
                await session.scalar(
                    text(
                        f"SELECT retained_family FROM {storage.qualified(prior_schema, old['generation_table'])} WHERE id=1"
                    )
                )
                == old
            )
        async with actors.publisher.begin() as session:
            await session.execute(
                text(
                    f"UPDATE {storage.qualified(settings.schema, generations.TABLES[companion])} SET local_generation=1"
                )
            )
        if importer == "code-sets":
            with pytest.raises(RuntimeError, match="source vector differs"):
                async with actors.builder.begin() as session:
                    await handoff.read_catalog_handoff_outcome(session, candidate)
        with pytest.raises(RuntimeError, match="generation changed"):
            async with actors.publisher.begin() as session:
                await rollback(session, receipt, settings)
        async with actors.publisher.begin() as session:
            await session.execute(
                text(
                    f"UPDATE {storage.qualified(settings.schema, generations.TABLES[companion])} SET local_generation=0"
                )
            )
            restored_generations = await rollback(session, receipt, settings)
            assert receipt == frozen_receipt and candidate == frozen_candidate
            assert restored_generations[importer]["local_generation"] == 2
            assert restored_generations[companion]["local_generation"] == 0
            assert (
                await session.scalar(text(f"SELECT count(*) FROM {storage.qualified(settings.schema, 'code_catalog')}"))
                == 1
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("importer,system,code", [("code-sets", "RC", "0450"), ("ms-drg", "MS_DRG", "001")])
async def test_scoped_roles_protect_publication_and_preserve_legacy_writes(monkeypatch, importer, system, code):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setenv(DATA_SCHEMA_ENV, DATA_SCHEMA)
    engine = create_async_engine(_dsn())
    try:
        async with catalog_actors(engine, ["mrf"]) as actors:
            await _seed(engine, "mrf")
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO mrf.code_catalog (code_system,code,display_name,source) "
                        "VALUES ('POS','99','Original','cms_place_of_service_code_set')"
                    )
                )
            async with _split_roles(engine, actors.roles["builder"]) as (settings, policy, publisher):
                await _bootstrap_split_result(engine, publisher, policy)
                builder = actors.builder
                await _assert_split_fence(builder, publisher)
                prepared = await _prepare(SimpleNamespace(publisher=builder, builder=builder), "mrf", importer)
                candidate = prepared[handoff.METRIC]
                assert candidate["schema_name"] == DATA_SCHEMA
                initial = await _legacy_codes(builder)
                await _assert_split_publication(builder, settings, candidate, system, code)
                assert await _legacy_codes(builder) == initial
                await _assert_split_writer_access(builder, publisher, system, code)
                async with publisher.begin() as session:
                    receipt = await handoff.read_catalog_handoff_outcome(session, candidate)
                    assert (await rollback(session, receipt, settings))[importer]["local_generation"] == 2
                assert await _legacy_codes(builder) == initial
    finally:
        await engine.dispose()


async def _legacy_codes(sessions):
    async with sessions.begin() as session:
        return (await session.execute(text("SELECT code_system,code,source FROM mrf.code_catalog ORDER BY 1,2"))).all()


async def _bootstrap_split_result(engine, publisher, policy):
    with pytest.raises(DBAPIError, match="does not exist"):
        async with publisher.begin() as session:
            await require_no_legacy_collision(session, DATA_SCHEMA, storage.MODELS)
    async with async_sessionmaker(engine).begin() as owner:
        await bootstrap_catalog_slice(owner, policy)


async def _assert_split_fence(builder, publisher):
    view = storage.qualified(FENCE_SCHEMA, FENCE_VIEW)
    async with publisher.begin() as session:
        await require_no_legacy_collision(session, DATA_SCHEMA, storage.MODELS)
        locked = (
            await session.execute(
                text(
                    "SELECT c.relname FROM pg_locks l JOIN pg_class c ON l.relation=c.oid "
                    "JOIN pg_namespace n ON c.relnamespace=n.oid "
                    "WHERE l.pid=pg_backend_pid() AND n.nspname='mrf' AND l.granted AND l.mode='ShareLock'"
                )
            )
        ).scalars()
        assert set(locked) == {model.__tablename__ for model in storage.MODELS}
        assert (
            await session.scalar(text("SELECT pg_has_role(current_user,:owner,'MEMBER')"), {"owner": FENCE_OWNER_ROLE})
            is False
        )
    for sessions in (builder, publisher):
        with pytest.raises(DBAPIError, match="permission denied"):
            async with sessions.begin() as session:
                await session.execute(text(f"CREATE OR REPLACE VIEW {view} AS SELECT 2 AS marker"))
    with pytest.raises(DBAPIError, match="permission denied"):
        async with publisher.begin() as session:
            await session.execute(text("LOCK TABLE mrf.code_catalog IN SHARE MODE NOWAIT"))
    with pytest.raises(RuntimeError, match="requires read committed"):
        async with publisher.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            await require_no_legacy_collision(session, DATA_SCHEMA, storage.MODELS)


async def _assert_split_publication(builder, settings, candidate, system, code):
    async with builder.begin() as session:
        await session.execute(
            text("INSERT INTO mrf.code_catalog(code_system,code,source) VALUES(:system,:code,'legacy')"),
            {"system": system, "code": code},
        )
        with pytest.raises(DBAPIError, match="could not obtain lock"):
            await run_publisher(settings, _attempt(candidate))
    with pytest.raises(RuntimeError, match="belongs to a legacy source"):
        await run_publisher(settings, _attempt(candidate))
    async with builder.begin() as session:
        await session.execute(text("DELETE FROM mrf.code_catalog WHERE source='legacy'"))
        await lock_scoped_catalog_reader(session)
        assert await session.scalar(text(f"SELECT count(*) FROM {DATA_SCHEMA}.code_catalog")) == 1
        with pytest.raises(DBAPIError, match="could not obtain lock"):
            await run_publisher(settings, _attempt(candidate))
    assert (await run_publisher(settings, _attempt(candidate)))["status"] == "succeeded"
    assert (await run_publisher(settings, _attempt(candidate)))["replayed"] is True


async def _assert_split_writer_access(builder, publisher, system, code):
    with pytest.raises(DBAPIError, match="permission denied"):
        async with builder.begin() as session:
            await session.execute(text(f"UPDATE {DATA_SCHEMA}.code_catalog SET display_name='forbidden'"))
    with pytest.raises(DBAPIError, match="permission denied"):
        async with publisher.begin() as session:
            await session.execute(text("UPDATE mrf.code_catalog SET display_name='forbidden'"))
    async with builder.begin() as session:
        await session.execute(text("UPDATE mrf.code_catalog SET display_name='still writable'"))
        composed = catalog_read_table(CodeCatalog)
        assert (
            await session.scalar(select(composed.c.display_name).where(composed.c.code_system == "OTHER"))
            == "still writable"
        )
        assert await session.scalar(
            select(composed.c.display_name).where(composed.c.code_system == system, composed.c.code == code)
        )
        assert (
            await session.scalar(
                select(composed.c.display_name).where(composed.c.code_system == "POS", composed.c.code == "99")
            )
            == "Original"
        )
