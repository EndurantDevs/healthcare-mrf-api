# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import os
import re
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import (
    ARRAY,
    BigInteger,
    CheckConstraint,
    Column,
    ForeignKey,
    Identity,
    Index,
    MetaData,
    Table,
    Text,
    select,
    text,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from process import reference_family_archive as archive
from process import reference_family_result_generation as result_generation
from tests.reference_family_generation_fixture import (
    generation_shape_check,
    install_source_generation_guards,
    native_reference_source,
)

_DSN_ENV = "HLTHPRT_REFERENCE_FAMILY_ARCHIVE_TEST_DSN"
_LOCAL_DATABASE = re.compile(r"^hc_reference_family_[0-9a-f]{32}$")


def _database_url():
    raw = os.environ.get(_DSN_ENV, "")
    if not raw:
        pytest.skip(f"{_DSN_ENV} is not set")
    url = make_url(raw)
    if (
        not url.drivername.startswith("postgresql")
        or url.host not in {"127.0.0.1", "localhost"}
        or url.port not in {5432, 5440}
        or not _LOCAL_DATABASE.fullmatch(str(url.database or ""))
    ):
        pytest.fail(f"{_DSN_ENV} must identify a UUID-owned local PostgreSQL 18 test database")
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _create_live_family(session, importer_id: str, schema_name: str) -> None:
    await session.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    spec = archive.reference_family_spec(importer_id)
    metadata = archive.MetaData(schema=schema_name)
    for model_type in spec.model_types:
        table = model_type.__table__.to_metadata(metadata, schema=schema_name)
        ddl = str(archive.CreateTable(table).compile(dialect=archive.postgresql.dialect()))
        await session.execute(text(ddl))
        indexes = getattr(model_type, "__my_additional_indexes__", ()) or ()
        if importer_id == "mrf":
            # Keep the exact ordinary copied-index subset, including the address checksum index.
            indexes = (
                tuple(getattr(model_type, "__my_initial_indexes__", ()) or ()) + tuple(indexes)
                if model_type.__tablename__
                in {"plan_benefits_marketplace", "mrf_address", "mrf_address_evidence", "plan_search_summary"}
                else ()
            )
        for index in indexes:
            await session.execute(text(archive._additional_index_sql(schema_name, model_type, index)))
    await session.execute(
        text(
            f'CREATE TABLE "{schema_name}".reference_family_result_generation ('
            "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, "
            "local_generation bigint NOT NULL, origin_lineage_id uuid, "
            "origin_generation bigint, published_at timestamptz, relation_oids bigint[], "
            "CONSTRAINT reference_family_result_generation_shape_check CHECK ("
            f"{generation_shape_check()}))"
        )
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema_name}".reference_family_result_generation '
            "(importer_id, local_lineage_id, local_generation) VALUES (:importer_id, :lineage_id, 0)"
        ),
        {"importer_id": importer_id, "lineage_id": uuid4()},
    )
    connection = await session.connection() if isinstance(session, AsyncSession) else session
    await install_source_generation_guards(connection, schema_name)


async def _assert_serves_stage(session, authority, ownership, schema_name):
    """Named authority plus the test's own catalog observation of the heaps now serving."""
    assert authority.relations == result_generation.declared_reference_family_relations(
        ownership.importer_id, schema_name
    )
    for name, stage_oid in ownership.relation_oids:
        relation = f'"{schema_name}"."{name}"'
        assert await session.scalar(text("SELECT to_regclass(:name)::oid::bigint"), {"name": relation}) == stage_oid


async def _manifest(sessions, importer_id: str, schema_name: str, *, dependencies=None):
    async with sessions() as session, session.begin():
        capture = await archive.capture_reference_family_source(
            session,
            importer_id=importer_id,
            schema_name=schema_name,
            source_metadata={"source_release": "synthetic-2026", "receipt": importer_id},
            dependencies=dependencies,
        )
    return capture.manifest


async def _copy_live_to_stage(session, importer_id: str, live_schema: str, stage_schema: str) -> None:
    for table_name in archive.reference_family_spec(importer_id).table_names:
        await session.execute(
            text(f'INSERT INTO "{stage_schema}"."{table_name}" SELECT * FROM "{live_schema}"."{table_name}"')
        )


async def _assert_cancelled_export_cleanup(sessions, live_schema: str, unrelated_schema: str, custody) -> None:
    cancelled_dataset_id = uuid4()
    cancelled_stage = archive.reference_family_stage_schema(cancelled_dataset_id)

    async def cancel_archive_copy(_capture):
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await archive.export_reference_family_archive(
            sessions,
            importer_id="places-zcta",
            schema_name=live_schema,
            source_metadata={"source_release": "synthetic-2026", "receipt": "cancelled"},
            dataset_id=cancelled_dataset_id,
            archive_copy=cancel_archive_copy,
            source_copy=custody.source_copy,
            on_precreated=custody.precreate,
            verify_custody=custody.verify,
        )
    async with sessions() as session, session.begin():
        assert await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": cancelled_stage}) is None
        assert await session.scalar(text(f'SELECT count(*) FROM "{unrelated_schema}".keep_me')) == 1


async def _assert_prepared_source_reuse(sessions, live_schema: str, custody) -> None:
    dataset_id = uuid4()
    prepared_sources = []

    async def persist(session, prepared):
        await custody.retain(session, prepared)
        prepared_sources.append(prepared)

    prepared = await archive.prepare_reference_family_archive_source(
        sessions,
        importer_id="places-zcta",
        schema_name=live_schema,
        source_metadata={"run_id": "synthetic-prepare"},
        dataset_id=dataset_id,
        on_prepared=persist,
        source_copy=custody.source_copy,
        on_precreated=custody.precreate,
    )
    assert prepared_sources == [prepared]
    async with sessions() as session, session.begin():
        await session.execute(text(f"UPDATE \"{live_schema}\".pricing_places_zcta SET measure_name='new-live'"))
    captures = []

    async def copy(capture):
        captures.append(capture)

    try:
        await archive.export_prepared_reference_family_archive(
            sessions, prepared=prepared, archive_copy=copy, verify_custody=custody.verify
        )
        await archive.export_prepared_reference_family_archive(
            sessions, prepared=prepared, archive_copy=copy, verify_custody=custody.verify
        )
        assert [capture.manifest for capture in captures] == [prepared.manifest, prepared.manifest]
        assert all(capture.ownership == prepared.ownership for capture in captures)
        assert prepared.manifest.tables[0].row_count == 1
    finally:
        async with sessions() as session, session.begin():
            await archive.cleanup_reference_family_stage(session, prepared.ownership)


async def _assert_prepare_callback_rollback(sessions, live_schema: str, custody) -> None:
    dataset_id = uuid4()

    async def reject(session, prepared):
        await custody.retain(session, prepared)
        raise RuntimeError("synthetic persistence failure")

    with pytest.raises(RuntimeError, match="persistence failure"):
        await archive.prepare_reference_family_archive_source(
            sessions,
            importer_id="places-zcta",
            schema_name=live_schema,
            source_metadata={"run_id": "synthetic-rollback"},
            dataset_id=dataset_id,
            on_prepared=reject,
            source_copy=custody.source_copy,
            on_precreated=custody.precreate,
        )
    async with sessions() as session, session.begin():
        assert (
            await session.scalar(
                text("SELECT to_regnamespace(:schema)"),
                {"schema": archive.reference_family_stage_schema(dataset_id)},
            )
            is None
        )


async def _create_places_fixture(session, live_schema: str, unrelated_schema: str) -> None:
    await _create_live_family(session, "places-zcta", live_schema)
    await session.execute(text(f'CREATE SCHEMA "{unrelated_schema}"'))
    await session.execute(text(f'CREATE TABLE "{unrelated_schema}".keep_me (id integer PRIMARY KEY)'))
    await session.execute(text(f'INSERT INTO "{unrelated_schema}".keep_me VALUES (1)'))
    await session.execute(
        text(
            f'INSERT INTO "{live_schema}".pricing_places_zcta '
            "(zcta, year, measure_id, measure_name, data_value, source) "
            "VALUES ('10001', 2026, 'A', 'old', 1.0, 'synthetic')"
        )
    )


@pytest.mark.asyncio
async def test_native_single_table_activation_cas_rollback_and_cleanup():
    """Prove cancellation cleanup, stage validation, and scope preservation."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:10]
    live_schema = f"rf_places_{token}"
    unrelated_schema = f"rf_keep_{token}"
    dataset_id = uuid4()
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    cleanup = AsyncExitStack()
    try:
        custody = await cleanup.enter_async_context(native_reference_source(sessions))
        async with sessions() as session, session.begin():
            await _create_places_fixture(session, live_schema, unrelated_schema)
        await _assert_prepare_callback_rollback(sessions, live_schema, custody)
        await _assert_prepared_source_reuse(sessions, live_schema, custody)
        await _assert_cancelled_export_cleanup(sessions, live_schema, unrelated_schema, custody)
        manifest = await _manifest(sessions, "places-zcta", live_schema)
        async with sessions() as session, session.begin():
            owner = await archive.precreate_reference_family_restore(
                session,
                importer_id="places-zcta",
                dataset_id=dataset_id,
            )
            await _copy_live_to_stage(session, "places-zcta", live_schema, stage_schema)
            await archive.complete_reference_family_restore(session, owner)
        # A same-name replacement of the live table is not a physical-identity refusal under named publication.
        async with sessions() as session, session.begin():
            await archive.validate_reference_family_stage(session, ownership=owner, manifest=manifest)
        async with sessions() as session, session.begin():
            assert (
                await session.scalar(text(f'SELECT measure_name FROM "{live_schema}".pricing_places_zcta'))
                == "new-live"
            )
            assert await session.scalar(text(f'SELECT count(*) FROM "{stage_schema}".pricing_places_zcta')) == 1
            assert await session.scalar(text(f'SELECT count(*) FROM "{unrelated_schema}".keep_me')) == 1
            await archive.cleanup_reference_family_stage(session, owner)
    finally:
        await cleanup.aclose()
        async with engine.begin() as connection:
            for schema_name in (stage_schema, live_schema, unrelated_schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_source_capture_binds_tracked_serving_generation():
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:10]
    live_schema = f"rf_capture_{token}"
    unrelated_schema = f"rf_capture_keep_{token}"
    try:
        async with sessions() as session, session.begin():
            await _create_places_fixture(session, live_schema, unrelated_schema)
            authority = await result_generation.publish_local_reference_family_generation(
                session,
                importer_id="places-zcta",
                schema_name=live_schema,
            )

        manifest = await _manifest(sessions, "places-zcta", live_schema)

        assert manifest.publication_authority == "tracked-generation"
        assert manifest.source_capture_contract == archive.GUARDED_SOURCE_CAPTURE_CONTRACT
        assert manifest.source_serving_generation == authority.serving_generation
        assert archive.validate_reference_family_manifest(manifest.as_dict()) == manifest
    finally:
        async with engine.begin() as connection:
            for schema_name in (live_schema, unrelated_schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


async def _validated_places_candidate(
    sessions,
    live_schema,
    unrelated_schema,
    dataset_id,
    package_id,
    *,
    populated=True,
):
    async with sessions() as session, session.begin():
        if populated:
            await _create_places_fixture(session, live_schema, unrelated_schema)
        else:
            await _create_live_family(session, "places-zcta", live_schema)
            await session.execute(text(f'CREATE SCHEMA "{unrelated_schema}"'))
    manifest = await _manifest(sessions, "places-zcta", live_schema)
    async with sessions() as session, session.begin():
        ownership = await archive.precreate_reference_family_restore(
            session, importer_id="places-zcta", dataset_id=dataset_id
        )
        await _copy_live_to_stage(session, "places-zcta", live_schema, ownership.schema_name)
        await archive.complete_reference_family_restore(session, ownership)
        sealed_owner_oid = await session.scalar(text("SELECT oid FROM pg_catalog.pg_roles WHERE rolname=current_user"))
    async with sessions() as session, session.begin():
        incumbent = await archive.capture_reference_family_incumbent(
            session, importer_id="places-zcta", schema_name=live_schema
        )
        validation = await archive.prepare_reference_family_activation(
            session,
            ownership=ownership,
            manifest=manifest,
            package_id=package_id,
            profile_contract=archive.CONTRACT,
            sealed_owner_oid=sealed_owner_oid,
        )
    return ownership, manifest, incumbent, validation, sealed_owner_oid


async def _candidate_with_generation(session, candidate, package_id, source_generation, *, guarded=True):
    ownership, manifest, incumbent, _validation, owner = candidate
    manifest = replace(
        manifest,
        publication_authority="tracked-generation",
        source_serving_generation=result_generation.validate_reference_family_serving_generation(source_generation),
        source_capture_contract=archive.GUARDED_SOURCE_CAPTURE_CONTRACT if guarded else None,
    )
    validation = await archive.prepare_reference_family_activation(
        session,
        ownership=ownership,
        manifest=manifest,
        package_id=package_id,
        profile_contract=archive.CONTRACT,
        sealed_owner_oid=owner,
    )
    return ownership, manifest, incumbent, validation, owner


async def _activate_validated_candidate(session, candidate, cutover):
    ownership, manifest, incumbent, validation, _owner = candidate
    return await archive.activate_validated_reference_family_stage(
        session,
        ownership=ownership,
        manifest=manifest,
        expected_incumbent=incumbent,
        validation_receipt=validation,
        cutover=cutover,
    )


@pytest.mark.asyncio
async def test_legacy_tracked_archive_stays_manual_and_untracked_until_local_publication():
    """Legacy provenance cannot authorize automatic activation or trusted re-export."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:10]
    live_schema, unrelated_schema = f"rf_legacy_{token}", f"rf_legacy_keep_{token}"
    dataset_id, package_id = uuid4(), "f" * 64
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    try:
        candidate = await _validated_places_candidate(sessions, live_schema, unrelated_schema, dataset_id, package_id)
        source_generation_by_field = {
            "origin_lineage_id": str(uuid4()),
            "origin_generation": 7,
            "published_at": "2026-09-14T10:00:00Z",
        }
        async with sessions.begin() as session:
            candidate = await _candidate_with_generation(
                session, candidate, package_id, source_generation_by_field, guarded=False
            )
        cutover = archive.ReferenceFamilyCutoverAuthority(
            package_id, archive.CONTRACT, "automatic", source_generation_by_field
        )
        async with sessions.begin() as session:
            with pytest.raises(archive.ReferenceFamilyArchiveError, match="requires guarded source capture"):
                await _activate_validated_candidate(session, candidate, cutover)
        async with sessions.begin() as session:
            await _activate_validated_candidate(session, candidate, replace(cutover, authority="manual"))
            adopted = await result_generation.read_reference_family_result_generation_authority(
                session, importer_id="places-zcta", schema_name=live_schema
            )
            assert adopted.serving_generation.as_dict() == source_generation_by_field
            assert not await session.scalar(
                text(f'SELECT source_revision_tracked FROM "{live_schema}".reference_family_result_generation')
            )
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="source generation is unavailable"):
            await _manifest(sessions, "places-zcta", live_schema)
        async with sessions.begin() as session:
            published = await result_generation.publish_local_reference_family_generation(
                session, importer_id="places-zcta", schema_name=live_schema
            )
            assert published.local_generation == 1
            assert published.serving_generation.origin_lineage_id == published.local_lineage_id
        recaptured = await _manifest(sessions, "places-zcta", live_schema)
        assert recaptured.source_capture_contract == archive.GUARDED_SOURCE_CAPTURE_CONTRACT
        assert recaptured.source_serving_generation == published.serving_generation
    finally:
        async with engine.begin() as connection:
            for schema_name in (
                stage_schema,
                archive.reference_family_predecessor_schema(dataset_id),
                live_schema,
                unrelated_schema,
            ):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("populated", [False, True])
async def test_automatic_generationless_bootstrap_requires_empty_incumbent(populated):
    """Only an actually empty legacy family can bootstrap without prior origin."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:10]
    live_schema, unrelated_schema = f"rf_boot_{token}", f"rf_boot_keep_{token}"
    dataset_id, package_id = uuid4(), "d" * 64
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    try:
        candidate = await _validated_places_candidate(
            sessions,
            live_schema,
            unrelated_schema,
            dataset_id,
            package_id,
            populated=populated,
        )
        cutover = archive.ReferenceFamilyCutoverAuthority(
            package_id,
            archive.CONTRACT,
            "automatic",
            {
                "origin_lineage_id": str(uuid4()),
                "origin_generation": 1,
                "published_at": "2026-09-14T10:00:00Z",
            },
        )
        async with sessions() as session, session.begin():
            # Acquire fixture locks before exercising the adoption decision.
            await archive._lock_family(
                session,
                live_schema,
                archive.reference_family_spec("places-zcta").archive_names,
                "ACCESS EXCLUSIVE",
                nowait=True,
            )
            candidate = await _candidate_with_generation(
                session, candidate, package_id, cutover.source_serving_generation
            )
            if populated:
                with pytest.raises(archive.ReferenceFamilyArchiveError, match="requires manual adoption"):
                    await _activate_validated_candidate(session, candidate, cutover)
            else:
                await _activate_validated_candidate(session, candidate, cutover)
    finally:
        async with engine.begin() as connection:
            for schema_name in (stage_schema, live_schema, unrelated_schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


async def _assert_absent_bootstrap_rollback(sessions, candidate, incumbent, cutover, live_schema, stage_schema):
    ownership, manifest, _, validation, _ = candidate
    async with sessions() as session:
        transaction = await session.begin()
        receipt = await archive.activate_validated_reference_family_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            expected_incumbent=incumbent,
            validation_receipt=validation,
            cutover=cutover,
        )
        assert receipt.predecessor_schema_name is None
        assert await session.scalar(text(f'SELECT count(*) FROM "{live_schema}".pricing_places_zcta')) == 1
        await transaction.rollback()
    async with sessions() as session, session.begin():
        assert (
            await session.scalar(
                text("SELECT to_regclass(:relation)"),
                {"relation": f"{live_schema}.pricing_places_zcta"},
            )
            is None
        )
        authority = await result_generation.read_reference_family_result_generation_authority(
            session,
            importer_id="places-zcta",
            schema_name=live_schema,
        )
        assert authority.serving_generation is None
        assert await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": stage_schema})


@pytest.mark.asyncio
async def test_automatic_bootstrap_accepts_all_absent_incumbent():
    """Install a source generation when the destination family is wholly absent."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:10]
    live_schema, unrelated_schema = f"rf_absent_{token}", f"rf_absent_keep_{token}"
    dataset_id, package_id = uuid4(), "e" * 64
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    try:
        candidate = await _validated_places_candidate(sessions, live_schema, unrelated_schema, dataset_id, package_id)
        ownership, manifest, _, validation, sealed_owner_oid = candidate
        async with sessions() as session, session.begin():
            await session.execute(text(f'DROP TABLE "{live_schema}".pricing_places_zcta'))
            incumbent = await archive.capture_reference_family_incumbent(
                session, importer_id="places-zcta", schema_name=live_schema
            )
        assert incumbent.relation_oids == (("pricing_places_zcta", None),)
        source_generation_by_field = {
            "origin_lineage_id": str(uuid4()),
            "origin_generation": 1,
            "published_at": "2026-09-14T10:00:00Z",
        }
        cutover = archive.ReferenceFamilyCutoverAuthority(
            package_id,
            archive.CONTRACT,
            "automatic",
            source_generation_by_field,
        )
        async with sessions() as session, session.begin():
            candidate = await _candidate_with_generation(session, candidate, package_id, source_generation_by_field)
        ownership, manifest, _, validation, _ = candidate
        await _assert_absent_bootstrap_rollback(sessions, candidate, incumbent, cutover, live_schema, stage_schema)
        async with sessions() as session, session.begin():
            receipt = await archive.activate_validated_reference_family_stage(
                session,
                ownership=ownership,
                manifest=manifest,
                expected_incumbent=incumbent,
                validation_receipt=validation,
                cutover=cutover,
            )
            assert receipt.predecessor_schema_name is None
        async with sessions() as session, session.begin():
            authority = await result_generation.read_reference_family_result_generation_authority(
                session, importer_id="places-zcta", schema_name=live_schema
            )
            assert authority.serving_generation.as_dict() == source_generation_by_field
            await _assert_serves_stage(session, authority, ownership, live_schema)
            assert await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": stage_schema}) is None
            assert await session.scalar(text(f'SELECT count(*) FROM "{unrelated_schema}".keep_me')) == 1
    finally:
        async with engine.begin() as connection:
            for schema_name in (stage_schema, live_schema, unrelated_schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_result_generation_validator_accepts_actual_asyncpg_record():
    """Accept the concrete record type returned to the archive controller."""

    sqlalchemy_url = make_url(_database_url())
    connection = await asyncpg.connect(
        sqlalchemy_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    lineage_id = uuid4()
    try:
        authority_row = await connection.fetchrow(
            "SELECT 'places-zcta'::text AS importer_id, $1::uuid AS local_lineage_id, "
            "0::bigint AS local_generation, NULL::uuid AS origin_lineage_id, "
            "NULL::bigint AS origin_generation, NULL::timestamptz AS published_at, "
            "NULL::varchar AS serving_schema, NULL::text[] AS serving_relation_names",
            lineage_id,
        )
        authority = result_generation.validate_reference_family_result_generation_authority(authority_row)
        assert authority.importer_id == "places-zcta"
        assert authority.local_lineage_id == str(lineage_id)
        assert authority.serving_generation is None
        assert authority.relations is None
    finally:
        await connection.close()


async def _assert_invalid_validation_cutovers(sessions, candidate, package_id):
    ownership, manifest, incumbent, validation, sealed_owner_oid = candidate
    forged_receipt = validation.as_dict()
    forged_receipt["package_id"] = "b" * 64
    # The cutover carries no owner identity; a forged validation digest is still refused.
    for validation_receipt, error_message in ((forged_receipt, "digest differs"),):
        async with sessions() as session, session.begin():
            with pytest.raises(archive.ReferenceFamilyArchiveError, match=error_message):
                await archive.activate_validated_reference_family_stage(
                    session,
                    ownership=ownership,
                    manifest=manifest,
                    expected_incumbent=incumbent,
                    validation_receipt=validation_receipt,
                    cutover=archive.ReferenceFamilyCutoverAuthority(package_id, archive.CONTRACT, "manual"),
                )


async def _assert_short_cutover_rollback(sessions, monkeypatch, candidate, package_id):
    ownership, manifest, incumbent, validation, sealed_owner_oid = candidate

    async def reject_recount(*_args, **_kwargs):
        raise AssertionError("short activation recounted the stage")

    monkeypatch.setattr(archive, "_validate_stage_manifest", reject_recount)
    async with sessions() as session, session.begin():
        initial_generation = await result_generation.publish_local_reference_family_generation(
            session,
            importer_id=ownership.importer_id,
            schema_name=incumbent.schema_name,
        )
    async with sessions() as session:
        transaction = await session.begin()
        receipt = await archive.activate_validated_reference_family_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            expected_incumbent=incumbent,
            validation_receipt=validation,
            cutover=archive.ReferenceFamilyCutoverAuthority(package_id, archive.CONTRACT, "manual"),
        )
        assert receipt.tables == manifest.tables
        cleared = await result_generation.read_reference_family_result_generation_authority(
            session,
            importer_id=ownership.importer_id,
            schema_name=incumbent.schema_name,
        )
        assert cleared.local_generation == initial_generation.local_generation
        assert cleared.serving_generation is None
        assert cleared.relations is None
        await transaction.rollback()
    async with sessions() as session, session.begin():
        assert (
            await result_generation.read_reference_family_result_generation_authority(
                session,
                importer_id=ownership.importer_id,
                schema_name=incumbent.schema_name,
            )
        ) == initial_generation


async def _capture_absent_incumbent_and_reject(sessions, live_schema, candidate, package_id):
    """A genuinely captured absent QName cannot replace a subsequently present family."""
    ownership, manifest, _incumbent, validation, _legacy_owner = candidate
    async with sessions() as session, session.begin():
        await session.execute(
            text(f'ALTER TABLE "{live_schema}".pricing_places_zcta RENAME TO pricing_places_zcta_previous')
        )
        absent = await archive.capture_reference_family_incumbent(
            session, importer_id="places-zcta", schema_name=live_schema
        )
        await session.execute(
            text(f'ALTER TABLE "{live_schema}".pricing_places_zcta_previous RENAME TO pricing_places_zcta')
        )
    async with sessions() as session, session.begin():
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="incumbent changed"):
            await archive.activate_validated_reference_family_stage(
                session,
                ownership=ownership,
                manifest=manifest,
                expected_incumbent=absent,
                validation_receipt=validation,
                cutover=archive.ReferenceFamilyCutoverAuthority(package_id, archive.CONTRACT, "manual"),
            )
        await archive.cleanup_reference_family_stage(session, ownership)


async def _install_places_generation_authority(session, live_schema: str):
    return await result_generation.publish_local_reference_family_generation(
        session,
        importer_id="places-zcta",
        schema_name=live_schema,
    )


async def _activate_automatic_candidate(session, candidate, package_id, source_generation_by_field):
    """Activate one trusted candidate with automatic generation authority."""

    ownership, manifest, incumbent, validation, sealed_owner_oid = await _candidate_with_generation(
        session, candidate, package_id, source_generation_by_field
    )
    return await archive.activate_validated_reference_family_stage(
        session,
        ownership=ownership,
        manifest=manifest,
        expected_incumbent=incumbent,
        validation_receipt=validation,
        cutover=archive.ReferenceFamilyCutoverAuthority(
            package_id,
            archive.CONTRACT,
            "automatic",
            source_generation_by_field,
        ),
    )


async def _assert_automatic_rollback(sessions, candidate, package_id, live_schema, initial, source_generation_by_field):
    """Prove data rotation and generation adoption share one rollback boundary."""

    ownership = candidate[0]
    async with sessions() as session:
        transaction = await session.begin()
        await _activate_automatic_candidate(session, candidate, package_id, source_generation_by_field)
        adopted = await result_generation.read_reference_family_result_generation_authority(
            session,
            importer_id="places-zcta",
            schema_name=live_schema,
        )
        assert adopted.local_generation == 1
        assert adopted.serving_generation.origin_generation == 2
        await _assert_serves_stage(session, adopted, ownership, live_schema)
        await transaction.rollback()
    async with sessions() as session, session.begin():
        rolled_back = await result_generation.read_reference_family_result_generation_authority(
            session,
            importer_id="places-zcta",
            schema_name=live_schema,
        )
        assert rolled_back == initial
        assert await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": ownership.schema_name})


async def _assert_automatic_generation_rejections(sessions, candidate, package_id, initial, source_generation_by_field):
    """Reject equal and unrelated source generations before rotation."""

    rejected_generations = (
        initial.serving_generation.as_dict(),
        {**source_generation_by_field, "origin_lineage_id": str(uuid4())},
    )
    for rejected_generation in rejected_generations:
        async with sessions() as session, session.begin():
            with pytest.raises(archive.ReferenceFamilyArchiveError, match="stale or unrelated"):
                await _activate_automatic_candidate(session, candidate, package_id, rejected_generation)


@pytest.mark.asyncio
async def test_automatic_cutover_preserves_generation_and_rolls_back_atomically():
    """Keep serving OIDs and adopted origin in the same activation transaction."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:10]
    live_schema, unrelated_schema = f"rf_auto_{token}", f"rf_auto_keep_{token}"
    dataset_id, package_id = uuid4(), "c" * 64
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    try:
        candidate = await _validated_places_candidate(sessions, live_schema, unrelated_schema, dataset_id, package_id)
        ownership = candidate[0]
        async with sessions() as session, session.begin():
            initial = await _install_places_generation_authority(session, live_schema)
        source_generation_by_field = {
            "origin_lineage_id": initial.local_lineage_id,
            "origin_generation": 2,
            "published_at": "2026-09-14T10:00:00Z",
        }
        await _assert_automatic_rollback(
            sessions, candidate, package_id, live_schema, initial, source_generation_by_field
        )
        await _assert_automatic_generation_rejections(
            sessions, candidate, package_id, initial, source_generation_by_field
        )
        async with sessions() as session, session.begin():
            await _activate_automatic_candidate(session, candidate, package_id, source_generation_by_field)
        async with sessions() as session, session.begin():
            committed = await result_generation.read_reference_family_result_generation_authority(
                session,
                importer_id="places-zcta",
                schema_name=live_schema,
            )
            assert committed.local_generation == 1
            assert committed.serving_generation.origin_generation == 2
            await _assert_serves_stage(session, committed, ownership, live_schema)
            assert await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": stage_schema}) is None
    finally:
        async with engine.begin() as connection:
            for schema_name in (
                stage_schema,
                archive.reference_family_predecessor_schema(dataset_id),
                live_schema,
                unrelated_schema,
            ):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_publisher_validation_receipt_fences_short_cutover(monkeypatch):
    """Reject forged evidence, wrong ownership, and stale incumbent without recounting."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:10]
    live_schema, unrelated_schema = f"rf_validated_{token}", f"rf_validated_keep_{token}"
    dataset_id, package_id = uuid4(), "a" * 64
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    try:
        candidate = await _validated_places_candidate(sessions, live_schema, unrelated_schema, dataset_id, package_id)
        await _assert_invalid_validation_cutovers(sessions, candidate, package_id)
        await _assert_short_cutover_rollback(sessions, monkeypatch, candidate, package_id)
        await _capture_absent_incumbent_and_reject(sessions, live_schema, candidate, package_id)
    finally:
        async with engine.begin() as connection:
            for schema_name in (stage_schema, live_schema, unrelated_schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


async def _activate_then_rollback(sessions, owner, manifest, incumbent, stage_schema: str) -> None:
    async with sessions() as session, session.begin():
        initial_generation = await result_generation.publish_local_reference_family_generation(
            session,
            importer_id=owner.importer_id,
            schema_name=incumbent.schema_name,
        )
    async with sessions() as session:
        transaction = await session.begin()
        receipt = await archive.activate_reference_family_stage(
            session,
            ownership=owner,
            manifest=manifest,
            expected_incumbent=incumbent,
            authority="manual",
        )
        assert len(receipt.relation_oids) == 2
        assert await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": stage_schema}) is None
        cleared = await result_generation.read_reference_family_result_generation_authority(
            session,
            importer_id=owner.importer_id,
            schema_name=incumbent.schema_name,
        )
        assert cleared.local_generation == initial_generation.local_generation
        assert cleared.serving_generation is None
        assert cleared.relations is None
        await transaction.rollback()
    async with sessions() as session, session.begin():
        assert (
            await result_generation.read_reference_family_result_generation_authority(
                session,
                importer_id=owner.importer_id,
                schema_name=incumbent.schema_name,
            )
        ) == initial_generation


async def _assert_unowned_predecessor_is_preserved(sessions, owner, manifest, incumbent) -> None:
    predecessor_schema = archive.reference_family_predecessor_schema(owner.dataset_id)
    async with sessions() as session, session.begin():
        await session.execute(text(f'CREATE SCHEMA "{predecessor_schema}"'))
        await session.execute(text(f'CREATE TABLE "{predecessor_schema}".keep_me (marker integer PRIMARY KEY)'))
        await session.execute(text(f'INSERT INTO "{predecessor_schema}".keep_me VALUES (9)'))
    async with sessions() as session, session.begin():
        with pytest.raises(ProgrammingError, match="already exists"):
            await archive.activate_reference_family_stage(
                session,
                ownership=owner,
                manifest=manifest,
                expected_incumbent=incumbent,
                authority="manual",
            )
    async with sessions() as session, session.begin():
        assert await session.scalar(text(f'SELECT marker FROM "{predecessor_schema}".keep_me')) == 9
        assert await session.scalar(text(f'SELECT count(*) FROM "{owner.schema_name}".medicare_enrollment_stats')) == 1
        await session.execute(text(f'DROP SCHEMA "{predecessor_schema}" CASCADE'))


async def _assert_stage_restored_after_rollback(sessions, stage_schema: str) -> None:
    async with sessions() as session, session.begin():
        assert await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": stage_schema}) is not None
        index_total = await session.scalar(
            text(
                "SELECT count(*) FROM pg_catalog.pg_class relation "
                "JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace "
                "WHERE namespace.nspname=:schema AND relation.relkind='i'"
            ),
            {"schema": stage_schema},
        )
        assert index_total >= 4


async def _commit_and_assert_medicare_activation(sessions, owner, manifest, incumbent, live_schema: str) -> None:
    async with sessions() as session, session.begin():
        receipt = await archive.activate_reference_family_stage(
            session,
            ownership=owner,
            manifest=manifest,
            expected_incumbent=incumbent,
            authority="manual",
        )
        assert receipt.source_metadata_sha256 == manifest.source_metadata_sha256
    async with sessions() as session, session.begin():
        assert (
            await session.scalar(
                text("SELECT to_regnamespace(:schema)"),
                {"schema": owner.schema_name},
            )
            is None
        )
        assert await session.scalar(text(f'SELECT count(*) FROM "{live_schema}".medicare_enrollment_county_stats')) == 1
        assert await session.scalar(text(f'SELECT count(*) FROM "{live_schema}".medicare_enrollment_stats')) == 1
        predecessor_schema = archive.reference_family_predecessor_schema(owner.dataset_id)
        assert receipt.predecessor_schema_name == predecessor_schema
        assert (
            await session.scalar(text(f'SELECT count(*) FROM "{predecessor_schema}".medicare_enrollment_county_stats'))
            == 1
        )
        assert await session.scalar(text(f'SELECT count(*) FROM "{predecessor_schema}".medicare_enrollment_stats')) == 1
        assert await session.scalar(text(f'SELECT marker FROM "{live_schema}".medicare_enrollment_stats_old')) == 7


@pytest.mark.asyncio
async def test_native_multi_table_activation_is_atomic_and_preserves_indexes():
    """Prove multi-table rollback, index retention, and atomic commit."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:10]
    live_schema = f"rf_medicare_{token}"
    dataset_id = uuid4()
    stage_schema = archive.reference_family_stage_schema(dataset_id)
    try:
        async with sessions() as session, session.begin():
            await _create_live_family(session, "medicare-enrollment", live_schema)
            await session.execute(
                text(
                    f'INSERT INTO "{live_schema}".medicare_enrollment_county_stats '
                    "(county_fips, year, part_d_beneficiaries, total_beneficiaries) VALUES ('01001', 2026, 5, 10)"
                )
            )
            await session.execute(
                text(
                    f'INSERT INTO "{live_schema}".medicare_enrollment_stats '
                    "(zcta_code, year, part_d_beneficiaries, total_beneficiaries) VALUES ('36001', 2026, 4, 8)"
                )
            )
            await session.execute(
                text(f'CREATE TABLE "{live_schema}".medicare_enrollment_stats_old (marker integer PRIMARY KEY)')
            )
            await session.execute(text(f'INSERT INTO "{live_schema}".medicare_enrollment_stats_old VALUES (7)'))
        manifest = await _manifest(sessions, "medicare-enrollment", live_schema)
        async with sessions() as session, session.begin():
            owner = await archive.precreate_reference_family_restore(
                session,
                importer_id="medicare-enrollment",
                dataset_id=dataset_id,
            )
            await _copy_live_to_stage(session, "medicare-enrollment", live_schema, stage_schema)
            await archive.complete_reference_family_restore(session, owner)
        async with sessions() as session, session.begin():
            incumbent = await archive.capture_reference_family_incumbent(
                session,
                importer_id="medicare-enrollment",
                schema_name=live_schema,
            )
        await _assert_unowned_predecessor_is_preserved(sessions, owner, manifest, incumbent)
        await _activate_then_rollback(sessions, owner, manifest, incumbent, stage_schema)
        await _assert_stage_restored_after_rollback(sessions, stage_schema)
        await _commit_and_assert_medicare_activation(sessions, owner, manifest, incumbent, live_schema)
    finally:
        async with engine.begin() as connection:
            for schema_name in (
                stage_schema,
                archive.reference_family_predecessor_schema(dataset_id),
                live_schema,
            ):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


@pytest.fixture
async def source_reader():
    """Own one source family and a separate SELECT-only role; always remove both."""
    database_url = _database_url()
    namespace_token, dataset_id = uuid4().hex, uuid4()
    source_schema = f"rf_read_{namespace_token}"
    unrelated, reader = f"rf_keep_{namespace_token}", f"rf_reader_{namespace_token}"
    stage = archive.reference_family_stage_schema(dataset_id)
    admin_engine = create_async_engine(database_url)
    admin_sessions = async_sessionmaker(admin_engine, expire_on_commit=False)
    reader_engine = create_async_engine(database_url, connect_args={"server_settings": {"role": reader}})
    reader_sessions = async_sessionmaker(reader_engine, expire_on_commit=False)
    cleanup = AsyncExitStack()
    try:
        async with admin_sessions.begin() as session:
            await _create_places_fixture(session, source_schema, unrelated)
            authority = await result_generation.publish_local_reference_family_generation(
                session, importer_id="places-zcta", schema_name=source_schema
            )
            await session.execute(text(f'CREATE ROLE "{reader}" NOLOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB'))
            await session.execute(text(f'GRANT USAGE ON SCHEMA "{source_schema}" TO "{reader}"'))
            await session.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{source_schema}" TO "{reader}"'))

        custody = await cleanup.enter_async_context(native_reference_source(admin_sessions, readers=(reader,)))

        yield SimpleNamespace(
            sessions=reader_sessions,
            admin=admin_sessions,
            source=source_schema,
            unrelated=unrelated,
            reader=reader,
            stage=stage,
            dataset_id=dataset_id,
            authority=authority,
            custody=custody,
        )
    finally:
        await reader_engine.dispose()
        await cleanup.aclose()
        async with admin_sessions.begin() as session:
            for schema in (stage, source_schema, unrelated):
                await session.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            exists = await session.scalar(text("SELECT 1 FROM pg_roles WHERE rolname=:role"), {"role": reader})
            if exists:
                await session.execute(text(f'DROP OWNED BY "{reader}"'))
                await session.execute(text(f'DROP ROLE "{reader}"'))
        await admin_engine.dispose()


async def _prepare_reader_clone(source_reader, concurrent_update):
    """Capture old row versions while proving replacement waits for clone commit."""

    async def metadata(_source_session):
        if concurrent_update:
            async with source_reader.admin.begin() as session:
                await session.execute(text("SET LOCAL lock_timeout='500ms'"))
                await session.execute(
                    text(f"UPDATE \"{source_reader.source}\".pricing_places_zcta SET measure_name='new'")
                )
        return {"source_release": "synthetic"}

    async def persist(session, prepared):
        await source_reader.custody.retain(session, prepared)
        assert prepared.ownership.schema_name == source_reader.stage
        assert (
            await session.scalar(text(f'SELECT measure_name FROM "{source_reader.stage}".pricing_places_zcta')) == "old"
        )
        with pytest.raises(DBAPIError) as blocked:
            async with source_reader.admin.begin() as other:
                await other.execute(
                    text(f'LOCK TABLE "{source_reader.source}".pricing_places_zcta IN ACCESS EXCLUSIVE MODE NOWAIT')
                )
        assert blocked.value.orig.sqlstate == "55P03"

    return await archive.prepare_reference_family_archive_source(
        source_reader.admin,
        source_sessions=source_reader.sessions,
        source_copy=source_reader.custody.source_copy,
        on_precreated=source_reader.custody.precreate,
        importer_id="places-zcta",
        schema_name=source_reader.source,
        source_metadata=None,
        source_metadata_factory=metadata,
        dataset_id=source_reader.dataset_id,
        on_prepared=persist,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrent_update", [False, True])
async def test_readonly_source_export(source_reader, concurrent_update):
    """A SELECT-only source can clone, export its stable snapshot, and release only the clone."""
    async with source_reader.sessions.begin() as session:
        assert await session.scalar(text("SELECT current_user")) == source_reader.reader
        assert not await session.scalar(
            text("SELECT has_table_privilege(current_user, :table, 'INSERT,UPDATE,DELETE,TRUNCATE,MAINTAIN')"),
            {"table": f'"{source_reader.source}".pricing_places_zcta'},
        )

    prepared = await _prepare_reader_clone(source_reader, concurrent_update)
    assert prepared.manifest.source_serving_generation == source_reader.authority.serving_generation
    async with source_reader.sessions.begin() as session:
        with pytest.raises(DBAPIError) as forbidden:
            async with session.begin_nested():
                await session.execute(
                    text(f"UPDATE \"{source_reader.stage}\".pricing_places_zcta SET measure_name='forbidden'")
                )
        assert forbidden.value.orig.sqlstate == "42501"
    async with source_reader.admin.begin() as session:
        await session.execute(
            text(f'LOCK TABLE "{source_reader.source}".pricing_places_zcta IN ACCESS EXCLUSIVE MODE NOWAIT')
        )

    async def copy(capture):
        async with source_reader.sessions.begin() as session:
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            await session.execute(text(f"SET TRANSACTION SNAPSHOT '{capture.postgres_snapshot}'"))
            assert (
                await session.scalar(text(f'SELECT measure_name FROM "{source_reader.stage}".pricing_places_zcta'))
                == "old"
            )

    await archive.export_prepared_reference_family_archive(
        source_reader.sessions, prepared=prepared, archive_copy=copy, verify_custody=source_reader.custody.verify
    )
    async with source_reader.admin.begin() as session:
        await archive.cleanup_reference_family_stage(session, prepared.ownership)
    async with source_reader.admin.begin() as session:
        assert await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": source_reader.stage}) is None
        assert await session.scalar(text(f'SELECT count(*) FROM "{source_reader.source}".pricing_places_zcta')) == 1
        assert await session.scalar(text(f'SELECT count(*) FROM "{source_reader.unrelated}".keep_me')) == 1


def _scoped_model_spec():
    metadata = MetaData(schema="synthetic_source")
    parent = Table(
        "scoped_parent",
        metadata,
        Column("id", BigInteger, Identity(), primary_key=True),
        Column("label", Text, nullable=False),
        CheckConstraint("length(label)>0", name="scoped_label_check"),
    )
    child = Table(
        "scoped_child",
        metadata,
        Column("id", BigInteger, primary_key=True),
        Column("parent_id", BigInteger, ForeignKey("synthetic_source.scoped_parent.id"), nullable=False),
        Column("parent_ids", ARRAY(BigInteger), nullable=False),
    )
    Index("scoped_child_parent_idx", child.c.parent_id)
    parent_model = type("ScopedParent", (), {"__tablename__": parent.name, "__table__": parent})
    child_model = type("ScopedChild", (), {"__tablename__": child.name, "__table__": child})
    return archive.ReferenceFamilySpec(
        "synthetic-scoped",
        (parent_model, child_model),
        relationships=((child_model, "parent_ids", parent_model, "id", True, False),),
    )


async def _copy_scoped_model_rows(session, spec, ownership, *, parent_id=1, parent_ids=(1,)):
    rows_by_table = {"scoped_parent": [(1, "one")], "scoped_child": [(1, parent_id, list(parent_ids))]}
    for model in spec.model_types:
        assert (
            await archive.native_copy_record_batch(
                session,
                model,
                schema_name=ownership.schema_name,
                table_name=model.__tablename__,
                columns=tuple(model.__table__.columns.keys()),
                records=rows_by_table[model.__tablename__],
            )
            == 1
        )


async def _assert_completed_scoped_catalog(session, ownership):
    assert (
        await session.scalar(
            text(
                "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid "
                "WHERE c.relnamespace=:oid AND i.indisvalid AND i.indisready"
            ),
            {"oid": ownership.schema_oid},
        )
        == 3
    )
    assert (
        await session.scalar(
            text(
                "SELECT count(*) FROM pg_constraint c JOIN pg_class r ON r.oid=c.conrelid "
                "WHERE r.relnamespace=:oid AND c.contype='f'"
            ),
            {"oid": ownership.schema_oid},
        )
        == 0
    )
    assert (
        await session.scalar(
            text("SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid WHERE c.relnamespace=:oid"),
            {"oid": ownership.schema_oid},
        )
        == 0
    )


@pytest.mark.asyncio
async def test_scoped_models_native_copy_deferred_indexes_and_exact_custody():
    """Unregistered model families share native heaps, set checks and restrictive cleanup."""
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    spec = _scoped_model_spec()
    dataset_id = uuid4()
    stage = archive.reference_family_stage_schema(dataset_id)
    try:
        async with sessions.begin() as session:
            ownership = await archive.precreate_model_family_stage(session, spec, dataset_id, include_identity=True)
            assert len(ownership.sequence_oids) == 1
            sequence_name, _sequence_oid, parent_table, parent_column = ownership.sequence_oids[0]
            assert (sequence_name, parent_table, parent_column) == ("scoped_parent_id_seq", "scoped_parent", "id")
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid WHERE c.relnamespace=:oid"
                    ),
                    {"oid": ownership.schema_oid},
                )
                == 0
            )
            await _copy_scoped_model_rows(session, spec, ownership)
            await archive.complete_model_family_stage(session, spec, ownership, include_identity=True)
            assert (
                await archive.verify_model_family_stage_ownership(session, spec, ownership, include_identity=True)
                == ownership
            )
            await _assert_completed_scoped_catalog(session, ownership)
        async with sessions.begin() as session:
            assert await session.scalar(text(f'SELECT count(*) FROM "{stage}".scoped_child')) == 1
            await archive.cleanup_model_family_stage(session, spec, ownership, include_identity=True)
            assert await session.scalar(text("SELECT to_regnamespace(:stage)"), {"stage": stage}) is None
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{stage}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(("parent_id", "parent_ids"), ((99, (1,)), (1, (99,)), (1, (None,))))
async def test_scoped_model_invalid_relationships_roll_back_complete_candidate(parent_id, parent_ids):
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    spec = _scoped_model_spec()
    dataset_id = uuid4()
    stage = archive.reference_family_stage_schema(dataset_id)
    try:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="relationship"):
            async with sessions.begin() as session:
                ownership = await archive.precreate_model_family_stage(session, spec, dataset_id, include_identity=True)
                await _copy_scoped_model_rows(session, spec, ownership, parent_id=parent_id, parent_ids=parent_ids)
                await archive.complete_model_family_stage(session, spec, ownership, include_identity=True)
        async with sessions.begin() as session:
            assert await session.scalar(text("SELECT to_regnamespace(:stage)"), {"stage": stage}) is None
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{stage}" CASCADE'))
        await engine.dispose()


@asynccontextmanager
async def _claims_family_database():
    """Publish the physician family under its fixed names for a SELECT-only reader role."""
    from db.models import CodeCatalog, CodeCrosswalk

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    reader = "claims_reader_" + uuid4().hex
    async with sessions.begin() as session:
        await session.execute(text('CREATE SCHEMA "mrf"'))
    try:
        async with sessions.begin() as session:
            for model in (*archive.reference_family_spec("claims-pricing").model_types, CodeCatalog, CodeCrosswalk):
                table = model.__table__.to_metadata(MetaData(), schema="mrf")
                await session.execute(
                    text(str(archive.CreateTable(table).compile(dialect=archive.postgresql.dialect())))
                )
            await session.execute(text(f'CREATE ROLE "{reader}"'))
            await session.execute(text(f'GRANT USAGE ON SCHEMA "mrf" TO "{reader}"'))
            await session.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "mrf" TO "{reader}"'))
        yield sessions, reader
    finally:
        async with sessions.begin() as session:
            await session.execute(text('DROP SCHEMA IF EXISTS "mrf" CASCADE'))
            await session.execute(text(f'DROP ROLE IF EXISTS "{reader}"'))
        await engine.dispose()


async def _pinned_claims_read(sessions, reader, after_snapshot=None):
    """Run the pin as the reader role inside a read-only repeatable-read snapshot."""
    from api.reference_family_reads import claims_dictionary_tables, pin_claims_reader

    async with sessions() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await session.execute(text(f'SET LOCAL ROLE "{reader}"'))
        await session.execute(text("SELECT 1"))
        if after_snapshot is not None:
            async with sessions.begin() as admin:
                for statement in (after_snapshot,) if isinstance(after_snapshot, str) else after_snapshot:
                    await admin.execute(text(statement))
        try:
            importers = await pin_claims_reader(session)
            await session.execute(select(claims_dictionary_tables(session)[0]).limit(1))
            return importers
        finally:
            await session.rollback()


@pytest.mark.asyncio
async def test_claims_pin_serves_a_copy_by_name_and_refuses_changed_tables():
    async with _claims_family_database() as (sessions, reader):
        assert await _pinned_claims_read(sessions, reader) == ("claims-pricing",)
        async with sessions.begin() as admin:
            original_oid = await admin.scalar(text("SELECT to_regclass('mrf.pricing_provider')::oid"))
            await admin.execute(text("ALTER TABLE mrf.pricing_provider RENAME TO pricing_provider_source"))
            await admin.execute(text("CREATE TABLE mrf.pricing_provider (LIKE mrf.pricing_provider_source)"))
            await admin.execute(text("DROP TABLE mrf.pricing_provider_source"))
            await admin.execute(text(f'GRANT SELECT ON mrf.pricing_provider TO "{reader}"'))
            assert await admin.scalar(text("SELECT to_regclass('mrf.pricing_provider')::oid")) != original_oid
        assert await _pinned_claims_read(sessions, reader) == ("claims-pricing",)
        for change, restore in (
            (
                "ALTER TABLE mrf.pricing_provider ENABLE ROW LEVEL SECURITY",
                "ALTER TABLE mrf.pricing_provider DISABLE ROW LEVEL SECURITY",
            ),
            ("ALTER TABLE mrf.claims_code_catalog DROP COLUMN source_release", None),
        ):
            async with sessions.begin() as admin:
                await admin.execute(text(change))
            with pytest.raises(RuntimeError, match="table is refused"):
                await _pinned_claims_read(sessions, reader)
            if restore is not None:
                async with sessions.begin() as admin:
                    await admin.execute(text(restore))


@pytest.mark.asyncio
async def test_claims_pin_refuses_row_security_committed_after_its_snapshot():
    async with _claims_family_database() as (sessions, reader):
        with pytest.raises(RuntimeError, match="table is refused"):
            await _pinned_claims_read(
                sessions,
                reader,
                after_snapshot="ALTER TABLE mrf.pricing_provider ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY",
            )


@pytest.mark.asyncio
async def test_claims_pin_refuses_a_stage_moved_in_after_its_snapshot():
    """A stage that existed before the snapshot but took the live name after it would read as empty."""
    async with _claims_family_database() as (sessions, reader):
        async with sessions.begin() as admin:
            await admin.execute(text("CREATE TABLE mrf.pricing_provider_stage (LIKE mrf.pricing_provider)"))
            await admin.execute(text(f'GRANT SELECT ON mrf.pricing_provider_stage TO "{reader}"'))
        with pytest.raises(RuntimeError, match="table is refused"):
            await _pinned_claims_read(
                sessions,
                reader,
                after_snapshot=(
                    "ALTER TABLE mrf.pricing_provider RENAME TO pricing_provider_old",
                    "ALTER TABLE mrf.pricing_provider_stage RENAME TO pricing_provider",
                ),
            )
        assert await _pinned_claims_read(sessions, reader) == ("claims-pricing",)


@pytest.mark.asyncio
async def test_code_pricing_family_serves_by_name_in_this_lane(monkeypatch):
    """Run the code pricing by-name read here: CI provisions this lane's database, so the case never skips."""
    from tests import test_plan_pricing_code_postgres as code_pricing

    monkeypatch.setattr(code_pricing, "_url", lambda: make_url(_database_url()))
    await code_pricing.serve_copied_family_by_name(monkeypatch)


async def _create_frozen_lock_case(session, dataset_id, live_schema, predecessor_schema, role):
    """Own fixture schemas and freeze ordinary table/column DML without removing DDL ownership."""
    spec = archive.reference_family_spec("places-zcta")
    await session.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
    ownership = await archive.precreate_reference_family_restore(
        session, importer_id=spec.importer_id, dataset_id=dataset_id
    )
    await archive.complete_reference_family_restore(session, ownership)
    await archive._create_model_family(session, spec, live_schema)
    await session.execute(text(f'CREATE SCHEMA "{predecessor_schema}"'))
    for schema, value in ((ownership.schema_name, "new"), (live_schema, "old")):
        table = f'"{schema}".pricing_places_zcta'
        await session.execute(
            text(
                f"INSERT INTO {table}(zcta,year,measure_id,measure_name,data_value,source) VALUES ('10001',2026,'A',:name,1.0,'synthetic')"
            ),
            {"name": value},
        )
        await session.execute(text(f'ALTER TABLE {table} OWNER TO "{role}"'))
        await session.execute(text(f'REVOKE ALL PRIVILEGES ON TABLE {table} FROM "{role}"'))
        await session.execute(text(f'GRANT SELECT ON TABLE {table} TO "{role}"'))
    for schema in (ownership.schema_name, live_schema, predecessor_schema):
        await session.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{role}"'))
    incumbent = await archive.capture_reference_family_incumbent(
        session, importer_id=spec.importer_id, schema_name=live_schema
    )
    return spec, ownership, incumbent


async def _assert_frozen_reader_wait(sessions, case, role, predecessor_schema):
    """Structural read locks coexist with a reader; the actual DDL waits and fails safely."""
    spec, ownership, incumbent = case
    async with sessions() as reader, reader.begin(), sessions() as session, session.begin():
        await reader.execute(text(f'SELECT * FROM "{incumbent.schema_name}".pricing_places_zcta'))
        await session.execute(text(f'SET LOCAL ROLE "{role}"'))
        with pytest.raises(DBAPIError, match="permission denied"):
            async with session.begin_nested():
                await session.execute(
                    text(f'LOCK TABLE "{ownership.schema_name}".pricing_places_zcta IN ACCESS EXCLUSIVE MODE NOWAIT')
                )
        async with archive._frozen_activation_lock_wait(session, True):
            await archive._lock_and_verify_activation(session, spec, ownership, incumbent, stage_read_only=True)
            with pytest.raises(DBAPIError) as blocked:
                async with session.begin_nested():
                    await session.execute(
                        text(
                            f'ALTER TABLE "{incumbent.schema_name}".pricing_places_zcta SET SCHEMA "{predecessor_schema}"'
                        )
                    )
            assert blocked.value.orig.sqlstate == "55P03"
        assert (
            await session.scalar(text(f'SELECT measure_name FROM "{incumbent.schema_name}".pricing_places_zcta'))
            == "old"
        )


async def _assert_frozen_activation_ddl_rolls_back(sessions, case, role, predecessor_schema):
    """The same SELECT-only owner can perform real DDL, and caller rollback restores both heaps."""
    spec, ownership, incumbent = case
    async with sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{role}"'))
        with pytest.raises(RuntimeError, match="synthetic cutover failure"):
            async with session.begin_nested(), archive._frozen_activation_lock_wait(session, True):
                await archive._lock_and_verify_activation(session, spec, ownership, incumbent, stage_read_only=True)
                await session.execute(
                    text(f'ALTER TABLE "{incumbent.schema_name}".pricing_places_zcta SET SCHEMA "{predecessor_schema}"')
                )
                await session.execute(
                    text(
                        f'ALTER TABLE "{ownership.schema_name}".pricing_places_zcta SET SCHEMA "{incumbent.schema_name}"'
                    )
                )
                assert (
                    await session.scalar(
                        text(f'SELECT measure_name FROM "{incumbent.schema_name}".pricing_places_zcta')
                    )
                    == "new"
                )
                raise RuntimeError("synthetic cutover failure")
        assert (
            await session.scalar(text(f'SELECT measure_name FROM "{incumbent.schema_name}".pricing_places_zcta'))
            == "old"
        )
        assert (
            await session.scalar(text(f'SELECT measure_name FROM "{ownership.schema_name}".pricing_places_zcta'))
            == "new"
        )


async def _drop_frozen_lock_case(sessions, schemas, role):
    """Remove only the committed UUID fixture schemas and their private role."""
    async with sessions() as session, session.begin():
        for schema in schemas:
            await session.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await session.execute(text(f'DROP ROLE "{role}"'))


@pytest.mark.asyncio
async def test_frozen_select_only_activation_preserves_readers_and_ddl_rollback():
    """No UPDATE grant is needed for the immutable path's actual swap and rollback."""
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex
    dataset_id = uuid4()
    role = "rf_frozen_" + token
    live_schema, predecessor_schema = "rf_read_live_" + token, "rf_read_prev_" + token
    schemas = (archive.reference_family_stage_schema(dataset_id), live_schema, predecessor_schema)
    is_created = False
    try:
        async with sessions() as session, session.begin():
            case = await _create_frozen_lock_case(session, dataset_id, live_schema, predecessor_schema, role)
        is_created = True
        await _assert_frozen_reader_wait(sessions, case, role, predecessor_schema)
        await _assert_frozen_activation_ddl_rolls_back(sessions, case, role, predecessor_schema)
    finally:
        try:
            if is_created:
                await _drop_frozen_lock_case(sessions, schemas, role)
        finally:
            await engine.dispose()


async def _nucc_generic_source_capture(resources, roles):
    """Freeze a real source clone only after its native COPY and model completion."""

    async def precreated(session, ownership):
        await archive.verify_reference_family_stage_ownership(session, ownership)
        assert not await session.scalar(text(f'SELECT EXISTS(SELECT FROM "{ownership.schema_name}".nucc_taxonomy)'))

    async def retain(session, prepared):
        await archive.seal_model_family_storage(session, prepared.ownership, roles=roles)

    async def metadata(_session):
        return {"source_release": "generic-restore-native"}

    async def contract(_session):
        return archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT

    return await archive.prepare_nucc_reference_archive_source(
        resources.publisher.session_factory,
        dataset_id=uuid4(),
        on_prepared=retain,
        on_precreated=precreated,
        source_copy=archive.ReferenceFamilySourceCopy(archive.native_copy_projection, 8 * 1024**2, 30),
        source_metadata_factory=metadata,
        source_capture_contract_factory=contract,
    )


async def _nucc_receive_captured_model(session, prepared, roles):
    """Receive only the captured model columns through bounded native COPY, then freeze."""
    received = await archive.precreate_reference_family_restore(session, importer_id="nucc", dataset_id=uuid4())
    model = archive.reference_family_spec("nucc").model_types[0]
    columns = tuple(model.__table__.columns.keys())
    query = (
        "SELECT "
        + ",".join(f'"{column}"' for column in columns)
        + f' FROM "{prepared.ownership.schema_name}".nucc_taxonomy ORDER BY code COLLATE "C"'
    )
    await archive.native_copy_projection(
        session,
        query,
        schema_name=received.schema_name,
        table_name="nucc_taxonomy",
        columns=columns,
        max_bytes=8 * 1024**2,
        timeout=30,
    )
    await archive.complete_reference_family_restore(session, received)
    await archive.seal_model_family_storage(session, received, roles=roles)
    return received


async def _nucc_generic_restore_candidate(resources):
    """Validate a real captured package over a separate frozen receive stage."""
    import hashlib

    roles = SimpleNamespace(
        owner_role=resources.roles["owner"],
        publisher_role=resources.roles["publisher"],
        runtime_roles=(resources.roles["writer"],),
    )
    prepared = await _nucc_generic_source_capture(resources, roles)
    async with resources.publisher.transaction() as session:
        received = await _nucc_receive_captured_model(session, prepared, roles)
        package = hashlib.sha256(archive._canonical_json(prepared.manifest.as_dict())).hexdigest()
        validation = await archive.prepare_reference_family_activation(
            session,
            ownership=received,
            manifest=prepared.manifest,
            package_id=package,
            profile_contract=archive.NUCC_CONTRACT,
            roles=roles,
        )
    cutover = archive.ReferenceFamilyCutoverAuthority(
        package,
        archive.NUCC_CONTRACT,
        "manual",
        prepared.manifest.source_serving_generation.as_dict(),
        roles=roles,
        stage_read_only=True,
    )
    return received, prepared.manifest, validation, cutover


async def _nucc_generic_activate(resources, candidate):
    """Use the actual generic publisher validation and incumbent-generation CAS."""
    ownership, manifest, validation, cutover = candidate
    async with resources.publisher.transaction() as session:
        incumbent = await archive.capture_reference_family_incumbent(session, importer_id="nucc", schema_name="mrf")
        return await archive.activate_validated_reference_family_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            expected_incumbent=incumbent,
            validation_receipt=validation,
            cutover=cutover,
        )


_NUCC_EVIDENCE_REVISION = (
    "SELECT input_revision FROM taxonomy_evidence_control.procedure_taxonomy_publication WHERE singleton"
)


@asynccontextmanager
async def _nucc_archive_evidence_role(resources, monkeypatch):
    """Provision the separate evidence reader and the writer's revision-only access."""
    admin = resources.admin
    evidence_role = f"taxonomy_evidence_{uuid4().hex}"
    publisher_role = resources.roles["publisher"]
    state_table = "taxonomy_evidence_control.procedure_taxonomy_publication"
    async with AsyncExitStack() as cleanup:
        await admin.execute(f'CREATE ROLE "{evidence_role}" NOLOGIN')
        cleanup.push_async_callback(admin.execute, f'DROP ROLE "{evidence_role}"')
        await admin.execute(f'GRANT USAGE ON SCHEMA mrf TO "{evidence_role}"')
        cleanup.push_async_callback(admin.execute, f'REVOKE USAGE ON SCHEMA mrf FROM "{evidence_role}"')
        cleanup.push_async_callback(admin.execute, f'REVOKE SELECT ON mrf.nucc_taxonomy FROM "{evidence_role}"')
        await admin.execute("CREATE SCHEMA taxonomy_evidence_control")
        cleanup.push_async_callback(admin.execute, "DROP SCHEMA taxonomy_evidence_control")
        await admin.execute(f"CREATE TABLE {state_table} (singleton boolean PRIMARY KEY, input_revision bigint)")
        cleanup.push_async_callback(admin.execute, f"DROP TABLE {state_table}")
        await admin.execute(f"INSERT INTO {state_table} VALUES (true, 0)")
        await admin.execute(f'GRANT USAGE ON SCHEMA taxonomy_evidence_control TO "{publisher_role}"')
        await admin.execute(
            f'GRANT SELECT (singleton, input_revision), UPDATE (input_revision) ON {state_table} TO "{publisher_role}"'
        )
        with monkeypatch.context() as evidence_environment:
            evidence_environment.setenv("HLTHPRT_PRICING_TAXONOMY_PUBLICATION_SCHEMA", "taxonomy_evidence")
            evidence_environment.setenv("HLTHPRT_PRICING_TAXONOMY_PUBLISHER_ROLE", evidence_role)
            yield evidence_role


async def _nucc_assert_guard_and_rollback(resources, candidate, published_state):
    """Refuse an actual additional hook and roll back a later completed generic rotation."""
    from tests.test_nucc_plain_publication_postgres import _published_state

    await resources.admin.execute(
        "CREATE TRIGGER unknown_nucc_hook AFTER UPDATE ON mrf.nucc_taxonomy FOR EACH STATEMENT EXECUTE FUNCTION mrf.cms_native_input_advance()"
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="hooks differ"):
        await _nucc_generic_activate(resources, candidate)
    await resources.admin.execute("DROP TRIGGER unknown_nucc_hook ON mrf.nucc_taxonomy")
    assert await _published_state(resources) == published_state
    assert await resources.admin.fetchval(_NUCC_EVIDENCE_REVISION) == 0
    ownership, manifest, validation, cutover = candidate
    with pytest.raises(RuntimeError, match="caller rejected rotation"):
        async with resources.publisher.transaction() as session:
            incumbent = await archive.capture_reference_family_incumbent(session, importer_id="nucc", schema_name="mrf")
            await archive.activate_validated_reference_family_stage(
                session,
                ownership=ownership,
                manifest=manifest,
                expected_incumbent=incumbent,
                validation_receipt=validation,
                cutover=cutover,
            )
            assert await session.scalar(text("SELECT count(*) FROM mrf.nucc_taxonomy")) == 2
            assert await session.scalar(text(_NUCC_EVIDENCE_REVISION)) == 1
            assert await resources.admin.fetchval(_NUCC_EVIDENCE_REVISION) == 0
            raise RuntimeError("caller rejected rotation")
    assert await _published_state(resources) == published_state
    assert await resources.admin.fetchval(_NUCC_EVIDENCE_REVISION) == 0
    assert await resources.admin.fetchval("SELECT to_regnamespace($1) IS NOT NULL", ownership.schema_name)


async def _nucc_assert_retired_cleanup(resources, retained_schema):
    """A pinned reader blocks native DROP; release permits guarded named retirement."""
    from process.nucc_retention_read_view import require_unretained_nucc_relations

    inventory_by_field = {"relations": [{"schema_name": retained_schema, "relation_name": "nucc_taxonomy"}]}

    async def is_unreferenced(session, recorded):
        assert recorded == inventory_by_field
        await require_unretained_nucc_relations(session, schema_name=retained_schema, relation_names=("nucc_taxonomy",))
        return True

    async with resources.publisher.session_factory() as reader, reader.begin():
        assert await reader.scalar(text(f'SELECT count(*) FROM "{retained_schema}".nucc_taxonomy')) == 3
        async with resources.publisher.transaction() as session:
            await session.execute(text("SET LOCAL lock_timeout='100ms'"))
            with pytest.raises(DBAPIError) as blocked:
                await archive.cleanup_nucc_retained_publication(
                    session, inventory=inventory_by_field, assert_unreferenced=is_unreferenced
                )
            assert blocked.value.orig.sqlstate == "55P03"
            assert await session.scalar(text(f'SELECT count(*) FROM "{retained_schema}".nucc_taxonomy')) == 3
    async with resources.publisher.transaction() as session:
        cleaned = await archive.cleanup_nucc_retained_publication(
            session, inventory=inventory_by_field, assert_unreferenced=is_unreferenced
        )
        assert cleaned["removed_schema"] == retained_schema
    assert await resources.admin.fetchval("SELECT to_regnamespace($1) IS NULL", retained_schema)


async def _nucc_assert_role_readonly(resources, schema, role):
    """Exercise real SELECT and native DML denial under one configured role."""
    async with resources.admin.transaction():
        await resources.admin.execute(f'SET LOCAL ROLE "{role}"')
        assert await resources.admin.fetchval(f'SELECT count(*) FROM "{schema}".nucc_taxonomy') > 0
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with resources.admin.transaction():
                await resources.admin.execute(
                    f'UPDATE "{schema}".nucc_taxonomy SET display_name=display_name WHERE FALSE'
                )
        await resources.admin.execute("RESET ROLE")


async def _nucc_assert_named_readonly_relations(resources, schema_names):
    """Exercise native access on both named snapshots for every configured role."""
    for schema in schema_names:
        for role in resources.roles.values():
            await _nucc_assert_role_readonly(resources, schema, role)


async def _nucc_assert_generic_activation(resources, candidate, activated, captured, incumbent):
    """Verify both complete row sets and logical generations after guard retirement."""
    retained = activated.predecessor_schema_name
    async with resources.publisher.transaction() as session:
        authority = await result_generation.read_reference_family_result_generation_authority(
            session, importer_id="nucc", schema_name="mrf"
        )
        assert authority.local_generation == 2 and authority.relations == (("mrf", "nucc_taxonomy"),)
        assert authority.serving_generation == candidate[1].source_serving_generation
        assert (
            await session.scalar(
                text("SELECT jsonb_agg(to_jsonb(row_value) ORDER BY code) FROM mrf.nucc_taxonomy row_value")
            )
            == captured[1]
        )
        assert (
            await session.scalar(
                text(f'SELECT jsonb_agg(to_jsonb(row_value) ORDER BY code) FROM "{retained}".nucc_taxonomy row_value')
            )
            == incumbent[1]
        )
    assert (
        await resources.admin.fetchval(
            "SELECT count(*) FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal",
            f'"{retained}".nucc_taxonomy',
        )
        == 0
    )
    assert (
        await resources.admin.fetchval(
            "SELECT revision FROM mrf.cms_native_input_revision WHERE schema_name='mrf' AND table_name='nucc_taxonomy'"
        )
        == 1
    )
    return retained


@pytest.mark.asyncio
async def test_native_nucc_generic_receive_retires_known_guard_and_cleans_retained_snapshot(monkeypatch):
    """Real capture/receive/manual restore preserves data, counters, readers and rollback."""
    from tests.test_nucc_plain_publication_postgres import (
        _bootstrap_plain,
        _install_existing_counter,
        _new_attempt,
        _published_state,
        nucc,
    )
    from tests.test_nucc_protected_flow_postgres import _native_candidates

    monkeypatch.setenv("HLTHPRT_ALIAS_GUARD_TEST_DSN", _database_url())
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    async with _native_candidates(monkeypatch) as resources:
        assert not await resources.admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace='hp_snapshot_retention'::regnamespace)"
        )
        await resources.admin.execute("DROP SCHEMA hp_snapshot_retention")
        await resources.admin.execute(f'CREATE SCHEMA mrf AUTHORIZATION "{resources.roles["owner"]}"')
        await resources.admin.execute(
            f'GRANT USAGE ON SCHEMA mrf TO "{resources.roles["publisher"]}","{resources.roles["writer"]}"'
        )
        await _install_existing_counter(resources)
        await _bootstrap_plain(resources)
        captured = await _published_state(resources)
        candidate = await _nucc_generic_restore_candidate(resources)
        await nucc._publish_plain_generation(await _new_attempt(resources, 3))
        before = await _published_state(resources)
        assert before[0]["native_input"]["revision"] == 1 and before[0]["family_generation"]["local_generation"] == 2
        async with _nucc_archive_evidence_role(resources, monkeypatch) as evidence_role:
            can_read_live = "SELECT has_table_privilege($1, 'mrf.nucc_taxonomy', 'SELECT')"
            assert not await resources.admin.fetchval(can_read_live, evidence_role)
            await _nucc_assert_guard_and_rollback(resources, candidate, before)
            assert not await resources.admin.fetchval(can_read_live, evidence_role)
            activated = await _nucc_generic_activate(resources, candidate)
            await _nucc_assert_role_readonly(resources, "mrf", evidence_role)
            assert await resources.admin.fetchval(_NUCC_EVIDENCE_REVISION) == 1
        retained = await _nucc_assert_generic_activation(resources, candidate, activated, captured, before)
        await _nucc_assert_named_readonly_relations(resources, ("mrf", retained))
        await _nucc_assert_retired_cleanup(resources, retained)


async def _prepare_nucc_handoff_attempt(session, predecessor):
    """Create and validate one bounded candidate on the actual controlled attempt."""
    from db.models import ImportRun
    from process.ext.utils import make_class
    from tests.test_nucc_protected_flow_postgres import _records

    run_id = "taxonomy_" + uuid4().hex
    ctx_by_field = {
        "control_run_id": run_id,
        "context": {
            "_control_attempt_id": run_id + ":" + uuid4().hex,
            "_control_attempt_started_at": datetime.now(UTC).isoformat(),
            "nucc_native_predecessor": predecessor.as_dict(),
        },
    }
    run, attempt, started, suffix = result_generation._nucc_attempt(ctx_by_field)
    await session.execute(
        ImportRun.__table__.insert().values(
            run_id=run,
            engine="healthcare-mrf-api",
            node_id="taxonomy-test",
            importer="nucc",
            status="running",
            params={},
            progress={"attempt_id": attempt, "attempt_started_at": started},
            metrics={"retained_metric": 1},
        )
    )
    stage = await result_generation._precreate_nucc_attempt(
        session, "mrf", run, attempt, started, suffix, incumbent=predecessor.as_dict()
    )
    ctx_by_field["import_date"] = suffix
    ctx_by_field["context"]["nucc_native_stage"] = stage
    model = make_class(archive.models.NUCCTaxonomy, suffix)
    columns = tuple(archive.models.NUCCTaxonomy.__table__.columns.keys())
    candidate_rows = [dict(zip(columns, taxonomy_values, strict=True)) for taxonomy_values in _records(1)]
    assert await archive.copy_nucc_native_batch(session, stage, model, candidate_rows) == 1
    ready = await archive.prepare_nucc_native_publication_stage(
        session, stage_model=model, schema_name="mrf", native_stage=stage
    )
    return ctx_by_field, model, ready


@pytest.mark.asyncio
async def test_native_nucc_handoff_rejects_stale_predecessor_and_records_exact_attempt(monkeypatch):
    """A rejected handoff leaves the isolated candidate and running attempt reusable."""
    from db.models import ImportRun
    from tests.test_nucc_plain_publication_postgres import _bootstrap_plain, _install_existing_counter
    from tests.test_nucc_protected_flow_postgres import _native_candidates

    monkeypatch.setenv("HLTHPRT_ALIAS_GUARD_TEST_DSN", _database_url())
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    async with _native_candidates(monkeypatch) as resources:
        await resources.admin.execute("DROP SCHEMA hp_snapshot_retention")
        await resources.admin.execute(f'CREATE SCHEMA mrf AUTHORIZATION "{resources.roles["owner"]}"')
        await resources.admin.execute(
            f'GRANT USAGE ON SCHEMA mrf TO "{resources.roles["publisher"]}","{resources.roles["writer"]}"'
        )
        await _install_existing_counter(resources)
        await _bootstrap_plain(resources)
        async with resources.publisher.transaction() as session:
            predecessor = await result_generation.read_reference_family_result_generation_authority(
                session, importer_id="nucc", schema_name="mrf"
            )
            ctx, model, ready = await _prepare_nucc_handoff_attempt(session, predecessor)
            saved_predecessor = ctx["context"]["nucc_native_predecessor"]
            ctx["context"]["nucc_native_predecessor"] = replace(
                predecessor, local_generation=0, serving_generation=None, relations=None
            ).as_dict()
            with pytest.raises(archive.ReferenceFamilyArchiveError, match="^NUCC native predecessor changed$"):
                await archive.record_nucc_native_handoff(
                    session, ctx, schema_name="mrf", stage_model=model, ready=ready
                )
            row_query = select(ImportRun.status, ImportRun.metrics).where(ImportRun.run_id == ctx["control_run_id"])
            status, metrics = (await session.execute(row_query)).one()
            assert status == "running" and "nucc_handoff" not in metrics
            ctx["context"]["nucc_native_predecessor"] = saved_predecessor
            handoff = await archive.record_nucc_native_handoff(
                session, ctx, schema_name="mrf", stage_model=model, ready=ready
            )
            assert handoff["row_count"] == 1 and handoff["incumbent"] == predecessor.as_dict()

        async with resources.publisher.transaction() as session:
            assert await archive.read_nucc_native_handoff(session, handoff) == handoff
            status, metrics = (await session.execute(row_query)).one()
            assert status == "finalizing" and metrics["retained_metric"] == 1
            assert metrics["nucc_handoff"] == handoff


@pytest.mark.asyncio
async def test_ordinary_nucc_rotation_check_passes_after_the_named_preparation_upgrade():
    """The upgrade adds the retention namespace alone; the next ordinary NUCC import must still rotate."""
    from db.migration_named_preparation import upgrade_named_preparation_catalog
    from process.nucc_retention_read_view import NuccRetentionReadError, require_unretained_nucc_relations

    engine = create_async_engine(_database_url())
    schema = "nucc_rotation_" + uuid4().hex
    relation_names = ("nucc_taxonomy", "nucc_taxonomy_old")
    is_upgraded = False

    async def check_rotation():
        async with AsyncSession(engine) as session, session.begin():
            await require_unretained_nucc_relations(session, schema_name=schema, relation_names=relation_names)

    try:
        async with engine.begin() as connection:
            assert await connection.scalar(text("SELECT to_regnamespace('hp_snapshot_retention') IS NULL"))
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.execute(text(f'CREATE TABLE "{schema}".nucc_taxonomy (code text)'))
        await check_rotation()
        is_upgraded = True
        async with engine.begin() as connection:
            await connection.run_sync(upgrade_named_preparation_catalog)
        await check_rotation()
        for claim_table, owner_column in (("relation", "generation_id"), ("preparation_relation", "preparation_id")):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        f"INSERT INTO hp_snapshot_retention.{claim_table} (schema_name,relation_name,{owner_column}) "
                        "VALUES (:schema,'nucc_taxonomy_old',CAST(:owner AS uuid))"
                    ),
                    {"schema": schema, "owner": str(uuid4())},
                )
            with pytest.raises(NuccRetentionReadError, match="retained claim exists"):
                await check_rotation()
            async with engine.begin() as connection:
                await connection.execute(text(f"DELETE FROM hp_snapshot_retention.{claim_table}"))
            await check_rotation()
        async with engine.begin() as connection:
            await connection.execute(text("CREATE TABLE hp_snapshot_retention.legacy_archive (receipt jsonb)"))
        with pytest.raises(NuccRetentionReadError, match="catalogue is incomplete"):
            await check_rotation()
    finally:
        async with engine.begin() as connection:
            if is_upgraded:
                await connection.execute(text("DROP SCHEMA IF EXISTS hp_snapshot_retention CASCADE"))
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_nucc_import_role_reads_claims_after_separate_migration_role_upgrade():
    """A distinct migration role installs the catalog; the ordinary import role still rotates."""
    from db.migration_named_preparation import upgrade_named_preparation_catalog
    from process.nucc_retention_read_view import NuccRetentionReadError, require_unretained_nucc_relations

    engine = create_async_engine(_database_url())
    suffix = uuid4().hex
    schema, migrator, importer = "nucc_rotation_" + suffix, "nucc_migrator_" + suffix, "nucc_importer_" + suffix
    relation_names = ("nucc_taxonomy", "nucc_taxonomy_old")

    async def check_rotation():
        async with AsyncSession(engine) as session, session.begin():
            await session.execute(text(f'SET LOCAL ROLE "{importer}"'))
            await require_unretained_nucc_relations(session, schema_name=schema, relation_names=relation_names)

    try:
        async with engine.begin() as connection:
            assert await connection.scalar(text("SELECT to_regnamespace('hp_snapshot_retention') IS NULL"))
            database = await connection.scalar(text("SELECT quote_ident(current_database())"))
            for role in (migrator, importer):
                await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE'))
            await connection.execute(text(f'GRANT CREATE ON DATABASE {database} TO "{migrator}"'))
            await connection.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{importer}"'))
            await connection.execute(text(f'CREATE TABLE "{schema}".nucc_taxonomy (code text)'))
            await connection.execute(text(f'ALTER TABLE "{schema}".nucc_taxonomy OWNER TO "{importer}"'))
        await check_rotation()
        async with engine.begin() as connection:
            await connection.execute(text(f'SET LOCAL ROLE "{migrator}"'))
            await connection.run_sync(upgrade_named_preparation_catalog)
        await check_rotation()
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO hp_snapshot_retention.relation (schema_name,relation_name,generation_id) "
                    "VALUES (:schema,'nucc_taxonomy_old',CAST(:owner AS uuid))"
                ),
                {"schema": schema, "owner": str(uuid4())},
            )
        with pytest.raises(NuccRetentionReadError, match="retained claim exists"):
            await check_rotation()
        async with AsyncSession(engine) as session, session.begin():
            await session.execute(text(f'SET LOCAL ROLE "{importer}"'))
            with pytest.raises(ProgrammingError, match="permission denied"):
                await session.execute(text("SELECT generation_id FROM hp_snapshot_retention.relation"))
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DROP SCHEMA IF EXISTS hp_snapshot_retention CASCADE"))
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            for role in (migrator, importer):
                if await connection.scalar(text("SELECT to_regrole(:role) IS NOT NULL"), {"role": role}):
                    await connection.execute(text(f'DROP OWNED BY "{role}"'))
                    await connection.execute(text(f'DROP ROLE "{role}"'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_address_owner_running_named_catalog_upgrade_keeps_ordinary_rotation(monkeypatch):
    """Owning both the catalog and the address table does not make the ordinary table a protected snapshot."""
    from db.connection import Database
    from db.migration_named_preparation import upgrade_named_preparation_catalog
    from process import entity_address_snapshot_adoption as adoption

    engine = create_async_engine(_database_url())
    suffix = uuid4().hex
    schema, owner = "address_rotation_" + suffix, "address_owner_" + suffix
    swap = SimpleNamespace(live_cls=SimpleNamespace(__main_table__="entity_address_unified"))
    monkeypatch.setattr(
        adoption.entity_address_unified, "db", Database(engine=engine, session_factory=async_sessionmaker(engine))
    )
    try:
        async with engine.begin() as connection:
            assert await connection.scalar(text("SELECT to_regnamespace('hp_snapshot_retention') IS NULL"))
            database = await connection.scalar(text("SELECT quote_ident(current_database())"))
            await connection.execute(text(f'CREATE ROLE "{owner}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE'))
            await connection.execute(text(f'GRANT CREATE ON DATABASE {database} TO "{owner}"'))
            await connection.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{owner}"'))
            await connection.execute(text(f'CREATE TABLE "{schema}".entity_address_unified (location_key text)'))
            await connection.execute(text(f'ALTER TABLE "{schema}".entity_address_unified OWNER TO "{owner}"'))
            await connection.execute(text(f'SET LOCAL ROLE "{owner}"'))
            await connection.run_sync(upgrade_named_preparation_catalog)
        await adoption._require_protected_cutover(schema, [swap], {})
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO hp_snapshot_retention.relation (schema_name,relation_name,generation_id) "
                    "VALUES (:schema,'entity_address_unified',CAST(:generation AS uuid))"
                ),
                {"schema": schema, "generation": str(uuid4())},
            )
        with pytest.raises(RuntimeError, match="cannot replace a protected snapshot"):
            await adoption._require_protected_cutover(schema, [swap], {})
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DROP SCHEMA IF EXISTS hp_snapshot_retention CASCADE"))
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            if await connection.scalar(text("SELECT to_regrole(:role) IS NOT NULL"), {"role": owner}):
                await connection.execute(text(f'DROP OWNED BY "{owner}"'))
                await connection.execute(text(f'DROP ROLE "{owner}"'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_catalog_foreign_key_triggers_pass_tiger_preflight_user_triggers_refuse():
    """Internal triggers from the catalog's existing foreign keys are expected; a user trigger is not."""
    from process import tiger_held_inputs as held

    engine = create_async_engine(_database_url())
    try:
        async with engine.begin() as connection:
            assert await connection.scalar(text("SELECT to_regnamespace('hp_snapshot_retention') IS NULL"))
            await connection.execute(text("CREATE SCHEMA hp_snapshot_retention"))
            await connection.execute(
                text("CREATE TABLE hp_snapshot_retention.generation (generation_id uuid PRIMARY KEY)")
            )
            # The existing retention migrations bind the current pointer to its generation.
            await connection.execute(
                text(
                    "CREATE TABLE hp_snapshot_retention.current_generation (dataset_key text PRIMARY KEY,"
                    "generation_id uuid NOT NULL REFERENCES hp_snapshot_retention.generation(generation_id))"
                )
            )
        async with AsyncSession(engine) as session, session.begin():
            for relation in (held._GENERATION, held._CURRENT):
                selected = await held._protected_relation(session, relation, catalog=True)
                assert (selected["schema_name"], selected["relation_name"]) == tuple(relation.split("."))
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "CREATE TRIGGER user_guard AFTER INSERT ON hp_snapshot_retention.generation "
                    "FOR EACH STATEMENT EXECUTE FUNCTION suppress_redundant_updates_trigger()"
                )
            )
        with pytest.raises(RuntimeError, match="protected TIGER input changed"):
            async with AsyncSession(engine) as session, session.begin():
                await held._protected_relation(session, held._GENERATION, catalog=True)
    finally:
        async with engine.begin() as connection:
            await connection.execute(text("DROP SCHEMA IF EXISTS hp_snapshot_retention CASCADE"))
        await engine.dispose()


async def _restore_dictionary_family(session, importer_id, system):
    ownership = await archive.precreate_reference_family_restore(session, importer_id=importer_id, dataset_id=uuid4())
    schema = f'"{ownership.schema_name}"'
    prefix = "drug_claims" if importer_id == "drug-claims" else "claims"
    catalog, crosswalk = f"{schema}.{prefix}_code_catalog", f"{schema}.{prefix}_code_crosswalk"
    parameters_by_field = {"system": system, "code": "synthetic-code", "source": "cms_physician_provider_service"}
    await session.execute(
        text(f"INSERT INTO {catalog} (code_system, code, source) VALUES (:system, :code, :source)"), parameters_by_field
    )
    await session.execute(
        text(f"""
            INSERT INTO {crosswalk} (from_system, from_code, to_system, to_code, match_type, confidence, source)
            VALUES (:system, :code, :system, :code, 'exact', 1, :source)
        """),
        parameters_by_field,
    )
    if importer_id == "drug-claims":
        await _restore_dictionary_effect_rows(session, schema, parameters_by_field)
    receive_spec = archive.reference_family_receive_spec(importer_id)
    await archive.complete_model_family_stage(session, receive_spec, ownership)
    manifest = await archive._family_manifest(
        session,
        spec=archive.reference_family_spec(importer_id),
        schema_name=ownership.schema_name,
        source_metadata={"release": "synthetic"},
    )
    return ownership, manifest, crosswalk


async def _restore_dictionary_effect_rows(session, schema, parameters_by_field):
    await session.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    await session.execute(
        text(f"INSERT INTO {schema}.pricing_prescription (rx_code_system, rx_code) VALUES (:system, :code)"),
        parameters_by_field,
    )
    await session.execute(
        text(f"""
            INSERT INTO {schema}.drug_claims_catalog_effect
                (code_system, code, after_image, destination_schema, destination_name)
            SELECT code_system, code, to_jsonb(catalog), 'mrf', 'code_catalog'
            FROM {schema}.drug_claims_code_catalog catalog
        """)
    )
    await session.execute(
        text(f"""
            INSERT INTO {schema}.drug_claims_crosswalk_effect
                (from_system, from_code, to_system, to_code, after_image, destination_schema, destination_name)
            SELECT from_system, from_code, to_system, to_code, to_jsonb(edge), 'mrf', 'code_crosswalk'
            FROM {schema}.drug_claims_code_crosswalk edge
        """)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("importer_id", "system", "bad_effect"),
    [("claims-pricing", "CPT", False), ("drug-claims", "HP_RX_CODE", False), ("drug-claims", "HP_RX_CODE", True)],
)
async def test_restored_reference_dictionary_requires_closed_endpoints_and_destination(importer_id, system, bad_effect):
    """A shape- and count-matching archive still needs native relationship and effect validation."""
    engine = create_async_engine(_database_url())
    try:
        async with AsyncSession(engine) as session:
            await session.begin()
            ownership, manifest, crosswalk = await _restore_dictionary_family(session, importer_id, system)
            tables = await archive.validate_reference_family_stage(session, ownership=ownership, manifest=manifest)
            assert (
                tuple(table.table_name for table in tables)
                == archive.reference_family_receive_spec(importer_id).table_names
            )
            if bad_effect:
                await session.execute(
                    text(f"""UPDATE "{ownership.schema_name}".drug_claims_catalog_effect
                        SET destination_name = 'other_table'""")
                )
                error = "effect closure differs"
            else:
                await session.execute(text(f"UPDATE {crosswalk} SET to_code = 'missing-code'"))
                error = "crosswalk endpoint is missing"
            with pytest.raises(archive.ReferenceFamilyArchiveError, match=error):
                await archive.validate_reference_family_stage(session, ownership=ownership, manifest=manifest)
    finally:
        await engine.dispose()
