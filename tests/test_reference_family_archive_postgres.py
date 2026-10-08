# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import os
import re
from contextlib import AsyncExitStack
from dataclasses import replace
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
    """Prove cancellation cleanup, stale CAS rejection, and scope preservation."""

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
        async with sessions() as session, session.begin():
            incumbent = await archive.capture_reference_family_incumbent(
                session,
                importer_id="places-zcta",
                schema_name=live_schema,
            )
        async with sessions() as session, session.begin():
            await _validate_and_change_incumbent(session, owner, manifest, live_schema)
        async with sessions() as session, session.begin():
            with pytest.raises(archive.ReferenceFamilyArchiveError, match="incumbent changed"):
                await archive.activate_reference_family_stage(
                    session,
                    ownership=owner,
                    manifest=manifest,
                    expected_incumbent=incumbent,
                    authority="manual",
                )
        async with sessions() as session, session.begin():
            assert (
                await session.scalar(text(f'SELECT measure_name FROM "{live_schema}".pricing_places_zcta')) == "changed"
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
        owner = candidate[-1]
        cutover = archive.ReferenceFamilyCutoverAuthority(
            package_id, archive.CONTRACT, owner, owner, "automatic", source_generation_by_field
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
        sealed_owner_oid = candidate[-1]
        cutover = archive.ReferenceFamilyCutoverAuthority(
            package_id,
            archive.CONTRACT,
            sealed_owner_oid,
            sealed_owner_oid,
            "automatic",
            {
                "origin_lineage_id": str(uuid4()),
                "origin_generation": 1,
                "published_at": "2026-09-14T10:00:00Z",
            },
        )
        async with sessions() as session, session.begin():
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
            sealed_owner_oid,
            sealed_owner_oid,
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
            assert authority.relation_oids == tuple(oid for _, oid in ownership.relation_oids)
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
            "NULL::bigint[] AS relation_oids",
            lineage_id,
        )
        authority = result_generation.validate_reference_family_result_generation_authority(authority_row)
        assert authority.importer_id == "places-zcta"
        assert authority.local_lineage_id == str(lineage_id)
        assert authority.serving_generation is None
        assert authority.relation_oids is None
    finally:
        await connection.close()


async def _assert_invalid_validation_cutovers(sessions, candidate, package_id):
    ownership, manifest, incumbent, validation, sealed_owner_oid = candidate
    forged_receipt = validation.as_dict()
    forged_receipt["package_id"] = "b" * 64
    cases = (
        (forged_receipt, sealed_owner_oid, sealed_owner_oid, "digest differs"),
        (validation, sealed_owner_oid, sealed_owner_oid + 1, "owner differs"),
        (validation, sealed_owner_oid + 1, sealed_owner_oid, "authority differs"),
    )
    for validation_receipt, original_owner_oid, current_owner_oid, error_message in cases:
        async with sessions() as session, session.begin():
            with pytest.raises(archive.ReferenceFamilyArchiveError, match=error_message):
                await archive.activate_validated_reference_family_stage(
                    session,
                    ownership=ownership,
                    manifest=manifest,
                    expected_incumbent=incumbent,
                    validation_receipt=validation_receipt,
                    cutover=archive.ReferenceFamilyCutoverAuthority(
                        package_id, archive.CONTRACT, original_owner_oid, current_owner_oid, "manual"
                    ),
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
            cutover=archive.ReferenceFamilyCutoverAuthority(
                package_id, archive.CONTRACT, sealed_owner_oid, sealed_owner_oid, "manual"
            ),
        )
        assert receipt.tables == manifest.tables
        cleared = await result_generation.read_reference_family_result_generation_authority(
            session,
            importer_id=ownership.importer_id,
            schema_name=incumbent.schema_name,
        )
        assert cleared.local_generation == initial_generation.local_generation
        assert cleared.serving_generation is None
        assert cleared.relation_oids is None
        await transaction.rollback()
    async with sessions() as session, session.begin():
        assert (
            await result_generation.read_reference_family_result_generation_authority(
                session,
                importer_id=ownership.importer_id,
                schema_name=incumbent.schema_name,
            )
        ) == initial_generation


async def _replace_incumbent_and_reject(sessions, live_schema, candidate, package_id):
    ownership, manifest, incumbent, validation, sealed_owner_oid = candidate
    async with sessions() as session, session.begin():
        await session.execute(
            text(f'ALTER TABLE "{live_schema}".pricing_places_zcta RENAME TO pricing_places_zcta_stale')
        )
        await session.execute(
            text(
                f'CREATE TABLE "{live_schema}".pricing_places_zcta '
                f'(LIKE "{live_schema}".pricing_places_zcta_stale INCLUDING ALL)'
            )
        )
    async with sessions() as session, session.begin():
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="incumbent changed"):
            await archive.activate_validated_reference_family_stage(
                session,
                ownership=ownership,
                manifest=manifest,
                expected_incumbent=incumbent,
                validation_receipt=validation,
                cutover=archive.ReferenceFamilyCutoverAuthority(
                    package_id, archive.CONTRACT, sealed_owner_oid, sealed_owner_oid, "manual"
                ),
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
            sealed_owner_oid,
            sealed_owner_oid,
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
        assert adopted.relation_oids == tuple(oid for _, oid in ownership.relation_oids)
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
            assert committed.relation_oids == tuple(oid for _, oid in ownership.relation_oids)
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
        await _replace_incumbent_and_reject(sessions, live_schema, candidate, package_id)
    finally:
        async with engine.begin() as connection:
            for schema_name in (stage_schema, live_schema, unrelated_schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


async def _validate_and_change_incumbent(session, owner, manifest, live_schema):
    await archive.validate_reference_family_stage(session, ownership=owner, manifest=manifest)
    await session.execute(text(f'ALTER TABLE "{live_schema}".pricing_places_zcta RENAME TO pricing_places_zcta_stale'))
    await session.execute(
        text(
            f'CREATE TABLE "{live_schema}".pricing_places_zcta '
            f'(LIKE "{live_schema}".pricing_places_zcta_stale INCLUDING ALL)'
        )
    )
    await session.execute(
        text(f'INSERT INTO "{live_schema}".pricing_places_zcta SELECT * FROM "{live_schema}".pricing_places_zcta_stale')
    )
    await session.execute(text(f"UPDATE \"{live_schema}\".pricing_places_zcta SET measure_name = 'changed'"))


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
        assert cleared.relation_oids is None
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
            changed = replace(ownership, schema_oid=ownership.schema_oid + 1)
            with pytest.raises(archive.ReferenceFamilyArchiveError, match="ownership"):
                await archive.cleanup_model_family_stage(session, spec, changed, include_identity=True)
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
