# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native component proof; synthetic source enrollment is not CMS publication."""

from __future__ import annotations

import json
import os
import re
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.engine import make_url
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
    nested = await session.begin_nested()
    try:
        await session.execute(
            text(
                f'CREATE TRIGGER selected_invalid_guard BEFORE UPDATE ON "{source_schema}".entity_address_unified FOR EACH ROW EXECUTE FUNCTION pg_catalog.suppress_redundant_updates_trigger()'
            )
        )
        with pytest.raises(ValueError, match="registry_cms_address_custody_invalid"):
            await equivalence.capture_registry_cms_address_copy_source(
                session, source_schema=source_schema, expected_relation_names=source_capture.relation_names
            )
    finally:
        await nested.rollback()
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
async def test_native_copy_preserves_receipts_and_rejects_drift():
    """Real catalogs prove numbering/name equivalence; every unrelated mutation fails."""
    engine = _engine()
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    source_schema = "synthetic_address_" + uuid4().hex
    dataset_id = uuid4()
    clone_schema = entity_address_archive_stage_schema(dataset_id)
    try:
        async with sessions() as session, session.begin():
            oids = await _source(session, source_schema)
            await session.execute(text(f'CREATE TABLE "{source_schema}".unrelated_control (value integer)'))
            await session.execute(
                text(
                    f'CREATE TRIGGER unrelated_control_guard BEFORE UPDATE ON "{source_schema}".unrelated_control FOR EACH ROW EXECUTE FUNCTION pg_catalog.suppress_redundant_updates_trigger()'
                )
            )
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
            assert len(json.dumps(witness.as_dict()).encode()) < 65536
            assert dict(source_capture.ordinals)["entity_address_unified"][0][1:] == (2, 1)
            assert dict(witness.clone_ordinals)["entity_address_unified"][0][1:] == (1, 1)
            assert "sequence_oid" not in json.loads(source_capture.sequence_json)
            assert source_capture.relation_names == tuple(name for name, _opaque in oids)
            copied = await equivalence.capture_registry_cms_address_copy_source(
                session,
                source_schema=source_schema,
                expected_relation_oids=tuple((name, {"opaque": "copied"}) for name, _token in oids),
            )
            assert copied == source_capture
            with pytest.raises(equivalence.RegistryCMSAddressEquivalenceError, match="named family"):
                await equivalence.capture_registry_cms_address_copy_source(
                    session, source_schema=source_schema, expected_relation_names=source_capture.relation_names[:-1]
                )
            await _assert_native_rejections(session, clone_schema, source_capture, owner, source_schema)
            assert (
                await equivalence.validate_registry_cms_address_copy(
                    session, source_capture=source_capture, clone_ownership=owner
                )
            ).as_dict() == witness.as_dict()
    finally:
        # Creation and all mutations occur in one transaction; rollback also owns cleanup on failure.
        async with engine.begin() as connection:
            for schema in (clone_schema, source_schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


def _legacy_wire(witness):
    """Keep the old semantic commitments while its physical tokens are opaque."""
    document = json.loads(json.dumps(witness.as_dict()))
    document["version"] = "registry_cms_address_copy.v1"
    for side in (document["source"], document["clone"]):
        side["schema_oid"] = {"copied": "opaque namespace"}
        side["relation_oids"] = [[entry["logical_name"], {"copied": "opaque table"}] for entry in side.pop("relations")]
        side["catalog_sha256"] = "e" * 64
        sequence = side["sequence"]
        sequence.pop("table_name")
        sequence.update(sequence_oid={"copied": "opaque sequence"}, table_oid=None)
        sequence["settings"].pop("type")
        sequence["settings"]["type_oid"] = {"copied": "opaque type"}
    return document


async def _assert_old_layout_refusals(session, clone_schema, old, owner):
    """Keep the custom default/index/CHECK commitment after portable slot projection."""
    for mutation in (
        f'ALTER TABLE "{clone_schema}".entity_address_unified ALTER COLUMN checksum SET DEFAULT 123',
        f'CREATE INDEX unexpected_copy_index ON "{clone_schema}".entity_address_unified (entity_id)',
        f'ALTER TABLE "{clone_schema}".entity_address_unified ADD CONSTRAINT unexpected_copy_check CHECK(checksum>0)',
    ):
        nested = await session.begin_nested()
        try:
            await session.execute(text(mutation))
            with pytest.raises(equivalence.RegistryCMSAddressEquivalenceError, match="layout"):
                await equivalence.require_registry_cms_address_copy_witness(session, old, clone_ownership=owner)
        finally:
            await nested.rollback()


@pytest.mark.asyncio
async def test_named_and_old_receipts_survive_copy_and_refuse_layout_drift():
    engine = _engine()
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    source_schema = "synthetic_address_" + uuid4().hex
    dataset_id = uuid4()
    clone_schema = entity_address_archive_stage_schema(dataset_id)
    try:
        async with sessions() as session, session.begin():
            names = tuple(name for name, _old in await _source(session, source_schema))
            source_capture = await equivalence.capture_registry_cms_address_copy_source(
                session, source_schema=source_schema, expected_relation_names=names
            )
            owner = await _clone(session, source_schema, dataset_id)
            witness = await equivalence.validate_registry_cms_address_copy(
                session, source_capture=source_capture, clone_ownership=owner
            )
            old = _legacy_wire(witness)
            old_bytes = equivalence.decode_registry_cms_address_copy_witness(old)
            await session.execute(
                text(
                    f'CREATE TABLE "{clone_schema}".copied_main (discarded integer, LIKE "{clone_schema}".entity_address_unified INCLUDING ALL)'
                )
            )
            await session.execute(text(f'ALTER TABLE "{clone_schema}".copied_main DROP COLUMN discarded'))
            await session.execute(
                text(f'INSERT INTO "{clone_schema}".copied_main SELECT * FROM "{clone_schema}".entity_address_unified')
            )
            await session.execute(text(f'DROP TABLE "{clone_schema}".entity_address_unified'))
            await session.execute(text(f'ALTER TABLE "{clone_schema}".copied_main RENAME TO entity_address_unified'))
            for document in (witness.as_dict(), old):
                await equivalence.require_registry_cms_address_copy_witness(session, document, clone_ownership=owner)
            await _assert_old_layout_refusals(session, clone_schema, old, owner)
            assert equivalence.decode_registry_cms_address_copy_witness(old) == old_bytes
    finally:
        async with engine.begin() as connection:
            for schema in (clone_schema, source_schema):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
