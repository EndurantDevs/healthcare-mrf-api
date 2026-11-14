# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native component proof; synthetic source enrollment is not CMS publication."""

from __future__ import annotations

import asyncio
import json
import os
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateTable

from process import entity_address_snapshot_receipt as receipt
from process import network_cms_registry_address_equivalence as equivalence
from process.entity_address_snapshot_ownership import (
    capture_created_entity_address_archive_stage,
    entity_address_archive_stage_schema,
)
from process.entity_address_snapshot_source import _SOURCE_INDEX_PREDICATES, _clone_entity_address_evidence_sequence
from process.network_cms_registry_address_capture import native_driver
from tests.registry_native_unique_trigger_test_support import verify_native_unique_trigger_predicate


def _engine():
    raw = os.environ.get("REGISTRY_ADDRESS_EQUIVALENCE_TEST_DSN")
    if not raw:
        pytest.skip("REGISTRY_ADDRESS_EQUIVALENCE_TEST_DSN is not set")
    url = make_url(raw)
    if url.host not in {"127.0.0.1", "localhost"} or not re.fullmatch(
        r"hc_address_equivalence_[0-9a-f]{32}", str(url.database)
    ):
        pytest.fail("address equivalence tests require their UUID-owned local database")
    return create_async_engine(url.set(drivername="postgresql+asyncpg"))


async def _source(session, schema):
    await session.execute(text(f'CREATE SCHEMA "{schema}"'))
    metadata = MetaData(schema=schema)
    for model in receipt._models():
        table = model.__table__.to_metadata(metadata, schema=schema)
        statement = str(CreateTable(table).compile(dialect=session.bind.dialect))
        if model.__tablename__ == "entity_address_unified":
            statement = statement.replace("(\n", "(\n discarded_column integer,\n", 1)
        await session.execute(text(statement))
    await session.execute(text(f'ALTER TABLE "{schema}".entity_address_unified DROP COLUMN discarded_column'))
    await session.execute(text(f'CREATE INDEX checksum_copy ON "{schema}".entity_address_unified (checksum)'))
    await session.execute(
        text(
            f'ALTER TABLE "{schema}".entity_address_unified '
            "ADD CONSTRAINT checksum_deferred UNIQUE (checksum) DEFERRABLE INITIALLY DEFERRED"
        )
    )
    for names, predicate in _SOURCE_INDEX_PREDICATES.items():
        for name in names:
            original_predicate = predicate.replace("])::text[]", "])")
            await session.execute(
                text(
                    f'CREATE INDEX "{name}" ON "{schema}".entity_address_unified (checksum) WHERE {original_predicate}'
                )
            )
    await session.execute(
        text(f'ALTER SEQUENCE "{schema}".entity_address_evidence_evidence_id_seq RENAME TO promoted_evidence_id_seq')
    )
    await session.execute(
        text(f"""INSERT INTO "{schema}".entity_address_unified
        (entity_type,entity_id,location_key,checksum,type,base_address_version)
        VALUES ('synthetic','office','office',1,'primary',:version)"""),
        {"version": receipt.entity_address_unified.ALIAS_BASE_ADDRESS_VERSION_PREFIX + "0"},
    )
    await session.execute(
        text(f"""INSERT INTO "{schema}".entity_address_evidence
        (location_key,entity_type,entity_id,source_id,source_run_id,observed_at)
        VALUES ('office','synthetic','office',1,'synthetic-run',TIMESTAMPTZ '2026-01-02 03:04:05+00')""")
    )
    oids = []
    for model in receipt._models():
        oids.append((model.__tablename__, await receipt._relation_oid(session, schema, model.__tablename__)))
    return tuple(sorted(oids))


async def _clone(session, source_schema, dataset_id):
    schema = entity_address_archive_stage_schema(dataset_id)
    await session.execute(text(f'CREATE SCHEMA "{schema}"'))
    for model in receipt._models():
        name = model.__tablename__
        await session.execute(text(f'CREATE TABLE "{schema}"."{name}" (LIKE "{source_schema}"."{name}" INCLUDING ALL)'))
        if name == "entity_address_evidence":
            await _clone_entity_address_evidence_sequence(session, stage_schema=schema)
        await session.execute(text(f'INSERT INTO "{schema}"."{name}" SELECT * FROM "{source_schema}"."{name}"'))
    return await capture_created_entity_address_archive_stage(session, dataset_id=dataset_id)


async def _assert_native_rejections(session, clone_schema, source_capture, owner, source_schema):
    mutations = (
        f'ALTER TABLE "{clone_schema}".entity_address_unified ADD COLUMN unknown_column integer',
        f'ALTER TABLE "{clone_schema}".entity_address_unified ALTER COLUMN entity_id TYPE varchar(64)',
        f'ALTER TABLE "{clone_schema}".entity_address_unified ALTER COLUMN checksum SET DEFAULT 7',
        f'CREATE INDEX unknown_index ON "{clone_schema}".entity_address_unified (entity_id)',
        f'CREATE INDEX unknown_expression ON "{clone_schema}".entity_address_unified (lower(entity_id))',
        f'CREATE INDEX unknown_method ON "{clone_schema}".entity_address_unified USING hash (entity_id)',
        f'ALTER TABLE "{clone_schema}".entity_address_unified ADD CONSTRAINT unknown_check CHECK (checksum >= 0)',
        f'UPDATE "{clone_schema}".entity_address_unified SET checksum=2',
        f'ALTER SEQUENCE "{clone_schema}".entity_address_evidence_evidence_id_seq INCREMENT BY 2',
        f'CREATE SEQUENCE "{clone_schema}".unexpected_owned OWNED BY "{clone_schema}".entity_address_unified.checksum',
        f"ALTER TABLE \"{clone_schema}\".entity_address_evidence ALTER COLUMN evidence_id SET DEFAULT nextval('{source_schema}.promoted_evidence_id_seq'::regclass)",
        f"ALTER TABLE \"{clone_schema}\".entity_address_evidence ALTER COLUMN evidence_id SET DEFAULT nextval('{clone_schema}.entity_address_evidence_evidence_id_seq'::regclass) + 1",
    )
    for mutation in mutations:
        nested = await session.begin_nested()
        try:
            await session.execute(text(mutation))
            with pytest.raises(
                (equivalence.RegistryCMSAddressEquivalenceError, receipt.EntityAddressArchiveReceiptError)
            ):
                await equivalence.validate_registry_cms_address_copy(
                    session, source_capture=source_capture, clone_ownership=owner
                )
        finally:
            await nested.rollback()


@pytest.mark.asyncio
async def test_native_copy_preserves_receipts_and_rejects_drift(monkeypatch):
    """Real catalogs prove numbering/name equivalence; every unrelated mutation fails."""
    engine = _engine()
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    source_schema = "synthetic_address_" + uuid4().hex
    dataset_id = uuid4()
    clone_schema = entity_address_archive_stage_schema(dataset_id)
    try:
        async with sessions() as session, session.begin():
            oids = await _source(session, source_schema)
            source_capture = await equivalence.capture_registry_cms_address_copy_source(
                session, source_schema=source_schema, expected_relation_oids=oids
            )
            original_bytes = json.dumps(source_capture.semantic_receipt.as_dict(), sort_keys=True)
            owner = await _clone(session, source_schema, dataset_id)
            witness = await equivalence.validate_registry_cms_address_copy(
                session, source_capture=source_capture, clone_ownership=owner
            )
            await verify_native_unique_trigger_predicate(await native_driver(session), owner.schema_name)
            assert witness.source.semantic_receipt.schema_sha256 != witness.clone_receipt.schema_sha256
            assert witness.source.semantic_receipt.main_input_sha256 == witness.clone_receipt.main_input_sha256
            assert [table_receipt.row_count for table_receipt in witness.clone_receipt.tables][:2] == [1, 1]
            assert json.dumps(source_capture.semantic_receipt.as_dict(), sort_keys=True) == original_bytes
            await _assert_retained_logical_receipt(session, owner, source_capture)
            assert {name for name, _ in source_capture.relation_oids} == {
                entry.table_name for entry in source_capture.semantic_receipt.tables
            }
            assert all(oid is None for _, oid in source_capture.relation_oids)
            assert "physical" not in json.loads(source_capture.catalog_json)
            assert "schema_oid" not in json.loads(source_capture.catalog_json)
            assert dict(source_capture.ordinals)["entity_address_unified"][0][1:] == (2, 1)
            assert dict(witness.clone_ordinals)["entity_address_unified"][0][1:] == (1, 1)
            assert (
                json.loads(source_capture.sequence_json)["settings"]
                == json.loads(witness.clone_sequence_json)["settings"]
            )
            copied_names = await equivalence.capture_registry_cms_address_copy_source(
                session,
                source_schema=source_schema,
                expected_relation_oids=tuple((name, oid + 1) for name, oid in oids),
            )
            assert copied_names == source_capture
            await _assert_native_rejections(session, clone_schema, source_capture, owner, source_schema)
            repeated = await equivalence.validate_registry_cms_address_copy(
                session, source_capture=source_capture, clone_ownership=owner
            )
            assert repeated.clone_receipt == witness.clone_receipt
            assert repeated.source.logical_sha256 == witness.source.logical_sha256
    finally:
        # Creation and all mutations occur in one transaction; rollback also owns cleanup on failure.
        async with engine.begin() as connection:
            for schema in (clone_schema, source_schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()

    await _assert_read_committed_metadata_transaction()
    await _assert_prepared_name_copy(monkeypatch)
    await _assert_native_clone_grants()


async def _assert_read_committed_metadata_transaction():
    """Closed stage metadata can be checked inside the publisher's write transaction."""
    from process.network_cms_registry_address_capture import address_clone_identity

    engine = _engine()
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    source_schema = "synthetic_address_" + uuid4().hex
    dataset_id = uuid4()
    clone_schema = entity_address_archive_stage_schema(dataset_id)
    try:
        async with sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            await _source(session, source_schema)
            owner = await _clone(session, source_schema, dataset_id)
            expected = await receipt.capture_entity_address_archive_receipt(session, schema_name=clone_schema)
            actual, layout_hash = await address_clone_identity(
                session, owner, "unused_owner", (), verify_content=False, expected_receipt=expected
            )
            assert actual is None and re.fullmatch(r"[0-9a-f]{64}", layout_hash)
            assert await session.scalar(text("SHOW transaction_isolation")) == "read committed"
            assert await session.scalar(text("SHOW transaction_read_only")) == "off"
            with pytest.raises(ValueError, match="address_layout_changed"):
                async with session.begin_nested():
                    await session.execute(
                        text(f'CREATE INDEX changed_layout ON "{clone_schema}".entity_address_unified (entity_id)')
                    )
                    await address_clone_identity(
                        session, owner, "unused_owner", (), verify_content=False, expected_receipt=expected
                    )
            assert await receipt.capture_entity_address_archive_receipt(session, schema_name=clone_schema) == expected
    finally:
        async with engine.begin() as connection:
            for schema in (clone_schema, source_schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


async def _prepared_name_input(engine, sessions, monkeypatch, created_schemas):
    """Use real native stages/catalogs with a synthetic reservation callback."""
    from process import network_registry_cms_prepared_address as prepared_address
    from process import provider_directory_generic_source_policy as policy
    from process.provider_directory_cms_nonprofile_observation import catalog_observation
    from tests.cms_npd_admission_postgres_support import fhir
    from tests.test_network_registry_cms_prepared_pair_postgres import NativePreparationDB, _staged_address

    async with sessions() as session, session.begin():
        assert await session.scalar(text("SELECT to_regnamespace('mrf')")) is None
        await _source(session, "mrf")
    created_schemas.append("mrf")
    address = await _staged_address(SimpleNamespace(engine=engine), monkeypatch)
    address.stage_oids = tuple((logical, stage, None) for logical, stage, _ in address.stage_oids)
    controls = NativePreparationDB(sessions)

    async def measure(directory, schema):
        rows = await directory.db.all(
            "SELECT n.nspname AS schema,c.relname AS relation,c.oid::bigint AS oid,"
            "pg_total_relation_size(c.oid) AS total_bytes,c.relpersistence::text AS persistence "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=:schema AND c.relname=ANY(:names)",
            schema=schema,
            names=[stage for _, stage, _ in address.stage_oids],
        )
        return [SimpleNamespace(**row) for row in rows]

    admission = SimpleNamespace(
        plan=SimpleNamespace(batch_size=1, native_address_targets=tuple(prepared_address._COPY_KEYS)),
        lease=object(),
        measure=measure,
        assert_ready=AsyncMock(),
        register_external_relation=AsyncMock(),
    )
    monkeypatch.setattr(policy, "is_source_storage_required", lambda: False)
    monkeypatch.setattr(prepared_address, "retained_raw_policy", lambda _lease: {})
    return SimpleNamespace(address=address, fhir=catalog_observation(fhir, controls), nonprofile_admission=admission)


async def _assert_prepared_name_copy(monkeypatch):
    """Run the ordinary native copy from name-only metadata and retain exact rollback."""
    from process import network_registry_cms_prepared_address as prepared_address
    from tests.test_network_registry_cms_prepared_pair_postgres import _native_storage_options

    engine = _engine()
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    capture_id, cancelled_id = uuid4(), uuid4()
    schema = entity_address_archive_stage_schema(capture_id)
    cancelled_schema = entity_address_archive_stage_schema(cancelled_id)
    created_schemas = [cancelled_schema, schema]
    try:
        with monkeypatch.context() as patch:
            prepared = await _prepared_name_input(engine, sessions, patch, created_schemas)
            async with prepared.fhir.db.transaction() as session:
                role = await session.scalar(text("SELECT current_user"))
                owner, copied, _, stages = await prepared_address.prepare_retained_registry_address(
                    session, prepared, capture_id=capture_id, owner_role=role, runtime_roles=()
                )
                assert stages == tuple(sorted(prepared.address.stage_oids))
                assert all(oid is None for _, _, oid in stages)
                assert all(table.row_count > 0 for table in copied.tables)
                driver = await native_driver(session)
                for logical, stage, _ in stages:
                    source_relation, target_relation = f'"mrf"."{stage}"', f'"{schema}"."{logical}"'
                    assert await _native_storage_options(driver, source_relation) == await _native_storage_options(
                        driver, target_relation
                    )
                    assert await driver.fetchval(
                        f"SELECT NOT EXISTS((SELECT to_jsonb(s) FROM {source_relation} s EXCEPT ALL "
                        f"SELECT to_jsonb(t) FROM {target_relation} t) UNION ALL "
                        f"(SELECT to_jsonb(t) FROM {target_relation} t EXCEPT ALL SELECT to_jsonb(s) FROM {source_relation} s))"
                    )
                assert owner.schema_name == schema
                assert prepared.nonprofile_admission.register_external_relation.await_count == 7
                await _assert_prepared_name_locks(session, sessions, prepared)
            await _assert_prepared_copy_rollback(prepared, cancelled_id, role, patch)
            async with sessions() as session:
                assert (
                    await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": cancelled_schema}) is None
                )
                assert await receipt.capture_entity_address_archive_receipt(session, schema_name=schema) == copied
    finally:
        async with engine.begin() as connection:
            for name in created_schemas:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
        await engine.dispose()


async def _assert_prepared_name_locks(session, sessions, prepared):
    """Name normalization keeps write fences and refuses actual unsafe layouts."""
    from process import network_registry_cms_prepared_address as prepared_address

    stage = prepared.address.stage_oids[0][1]
    async with sessions() as other, other.begin():
        await other.execute(text("SET LOCAL lock_timeout='100ms'"))
        with pytest.raises(DBAPIError, match="lock timeout"):
            await other.execute(text(f'ALTER TABLE mrf."{stage}" ADD COLUMN forbidden integer'))
    with pytest.raises(ValueError, match="prepared_address_changed"):
        async with session.begin_nested():
            await session.execute(text(f'ALTER TABLE mrf."{stage}" ENABLE ROW LEVEL SECURITY'))
            await prepared_address.lock_prepared_address(session, prepared.address)
    legacy_stages = tuple((logical, stage, 1) for logical, stage, _ in prepared.address.stage_oids)
    original = prepared.address.stage_oids
    try:
        prepared.address.stage_oids = legacy_stages
        assert await prepared_address.lock_prepared_address(session, prepared.address) == tuple(sorted(legacy_stages))
    finally:
        prepared.address.stage_oids = original


async def _assert_prepared_copy_rollback(prepared, capture_id, role, monkeypatch):
    """A cancellation after a genuine COPY batch removes the incomplete snapshot."""
    from process import network_registry_cms_prepared_address as prepared_address

    original = prepared_address._copy_native_batch
    completed_batches = []

    async def cancel_after_copy(*arguments, **options):
        await original(*arguments, **options)
        completed_batches.append(arguments[4])
        raise asyncio.CancelledError

    with monkeypatch.context() as patch:
        patch.setattr(prepared_address, "_copy_native_batch", cancel_after_copy)
        with pytest.raises(asyncio.CancelledError):
            async with prepared.fhir.db.transaction() as session:
                await prepared_address.prepare_retained_registry_address(
                    session, prepared, capture_id=capture_id, owner_role=role, runtime_roles=()
                )
    assert len(completed_batches) == 1


async def _assert_retained_logical_receipt(session, owner, source_capture):
    """Restore gaps/sequence names without rewriting receipts or accepting real drift."""
    from process.network_cms_registry_address_capture import address_clone_identity

    expected = source_capture.semantic_receipt
    capture_options_by_field = dict(expected_receipt=expected, expected_logical_sha256=source_capture.logical_sha256)
    for verify_content in (False, True):
        actual, logical_hash = await address_clone_identity(
            session, owner, "unused_owner", (), verify_content=verify_content, **capture_options_by_field
        )
        assert actual == (expected if verify_content else None)
        assert logical_hash == source_capture.logical_sha256
        for mutation in (
            f'ALTER TABLE "{owner.schema_name}".entity_address_unified ALTER COLUMN entity_id TYPE varchar(64)',
            f'ALTER TABLE "{owner.schema_name}".entity_address_unified ALTER COLUMN checksum SET DEFAULT 7',
            f'CREATE INDEX changed_layout ON "{owner.schema_name}".entity_address_unified (entity_id)',
            f'ALTER SEQUENCE "{owner.schema_name}".entity_address_evidence_evidence_id_seq INCREMENT BY 2',
        ):
            nested = await session.begin_nested()
            try:
                await session.execute(text(mutation))
                with pytest.raises(ValueError, match="layout_changed|column type"):
                    await address_clone_identity(
                        session, owner, "unused_owner", (), verify_content=verify_content, **capture_options_by_field
                    )
            finally:
                await nested.rollback()
    with pytest.raises(ValueError, match="layout_changed"):
        await address_clone_identity(
            session, owner, "unused_owner", (), verify_content=False, expected_receipt=expected
        )
    with pytest.raises(ValueError, match="content_changed"):
        async with session.begin_nested():
            await session.execute(text(f'UPDATE "{owner.schema_name}".entity_address_unified SET checksum=2'))
            await address_clone_identity(session, owner, "unused_owner", (), **capture_options_by_field)
    assert source_capture.semantic_receipt == expected


async def _clone_writer_roles(engine, source_schema, builder, reader):
    """Use isolated roles so default grants never affect another test's creator."""
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE ROLE "{builder}"'))
        await connection.execute(text(f'CREATE ROLE "{reader}"'))
        await connection.execute(text(f'GRANT CREATE ON DATABASE "{engine.url.database}" TO "{builder}"'))
        for target, privileges in (("SCHEMAS", "USAGE, CREATE"), ("TABLES", "ALL"), ("SEQUENCES", "ALL")):
            await connection.execute(
                text(f'ALTER DEFAULT PRIVILEGES FOR ROLE "{builder}" GRANT {privileges} ON {target} TO "{reader}"')
            )
        await connection.execute(text(f'GRANT USAGE ON SCHEMA "{source_schema}" TO "{builder}"'))
        await connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{source_schema}" TO "{builder}"'))


async def _assert_clone_writes_denied(engine, schema, reader, *, readable):
    """Exercise native permissions after commit, with no archive transaction pin."""
    relation = f'"{schema}".entity_address_unified'
    async with engine.connect() as connection, connection.begin():
        await connection.execute(text(f'SET LOCAL ROLE "{reader}"'))
        if readable:
            assert await connection.scalar(text(f"SELECT count(*) FROM {relation}")) > 0
        else:
            with pytest.raises(DBAPIError) as error:
                async with connection.begin_nested():
                    await connection.execute(text(f"SELECT * FROM {relation}"))
            assert error.value.orig.sqlstate == "42501"
        for statement in (
            f"UPDATE {relation} SET checksum=2",
            f"DELETE FROM {relation}",
            f"TRUNCATE {relation}",
            f'CREATE TABLE "{schema}".unexpected (value integer)',
            f"SELECT nextval('{schema}.entity_address_evidence_evidence_id_seq')",
        ):
            with pytest.raises(DBAPIError) as error:
                async with connection.begin_nested():
                    await connection.execute(text(statement))
            assert error.value.orig.sqlstate == "42501"


async def _cleanup_clone_roles(engine, schemas, roles):
    """Remove only this case's named schemas and UUID-scoped roles."""
    async with engine.begin() as connection:
        for schema in schemas:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        for role in roles:
            if await connection.scalar(text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=:role)"), {"role": role}):
                await connection.execute(text(f'DROP OWNED BY "{role}"'))
                await connection.execute(text(f'DROP ROLE "{role}"'))


async def _assert_native_clone_grants():
    """No inherited or column grant can mutate the committed clone before its dump."""
    from process import entity_address_snapshot_source as source
    from process.network_cms_registry_address_capture import freeze_address_clone
    from tests.test_entity_address_snapshot_stage_postgres import _indexed_archive_sessions

    engine = _engine()
    source_schema, dataset_id = "synthetic_address_" + uuid4().hex, uuid4()
    stage_schema = source.entity_address_archive_stage_schema(dataset_id)
    builder, reader = "clone_builder_" + uuid4().hex, "clone_reader_" + uuid4().hex
    clone_engine = create_async_engine(engine.url, connect_args={"server_settings": {"role": builder}})
    try:
        sessions = await _indexed_archive_sessions(engine, source_schema, source.entity_address_archive_relations())
        await _clone_writer_roles(engine, source_schema, builder, reader)
        clones = async_sessionmaker(clone_engine, expire_on_commit=False)
        async with sessions() as pinned, pinned.begin():
            captured = await source.capture_entity_address_archive_source(pinned, schema_name=source_schema)

            async def created(session):
                await session.execute(
                    text(f'GRANT UPDATE (checksum) ON "{stage_schema}".entity_address_unified TO "{reader}"')
                )
                assert await session.scalar(
                    text("SELECT has_table_privilege(:role,:relation,'UPDATE')"),
                    {"role": reader, "relation": f"{stage_schema}.entity_address_unified"},
                )

            async with clones() as session, session.begin():
                await source._clone_entity_address_archive_source(
                    session,
                    source_capture=captured,
                    stage_schema=stage_schema,
                    source_copy=source.native_entity_address_source_copy(max_bytes=16 * 1024**2, timeout=30),
                    copy_deadline=asyncio.get_running_loop().time() + 30,
                    on_precreated=created,
                )
                owner = await capture_created_entity_address_archive_stage(session, dataset_id=dataset_id)
        await _assert_clone_writes_denied(engine, stage_schema, reader, readable=False)
        async with clones() as session, session.begin():
            # Cover a column-only grant introduced by a pre-freeze creation callback.
            await session.execute(
                text(f'GRANT UPDATE (checksum) ON "{stage_schema}".entity_address_unified TO "{reader}"')
            )
            expected, _layout = await freeze_address_clone(session, owner, builder, (reader,))
        await _assert_clone_writes_denied(engine, stage_schema, reader, readable=True)
        async with sessions() as session, session.begin():
            assert await receipt.capture_entity_address_archive_receipt(session, schema_name=stage_schema) == expected
    finally:
        await clone_engine.dispose()
        await _cleanup_clone_roles(engine, (stage_schema, source_schema), (builder, reader))
        await engine.dispose()
