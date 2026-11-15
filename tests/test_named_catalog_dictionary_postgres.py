# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named publications preserve dictionary effect CAS under native publisher grants."""

from __future__ import annotations

import asyncio
import json
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.exc import DBAPIError

from db.models import CodeCrosswalk
from process import code_sets_result_archive as codes
from process import ms_drg_result_generation as drg
from process import reference_family_archive as native
from process import reference_family_dictionary as dictionary
from process import scoped_catalog_binding as binding
from process import scoped_catalog_handoff as handoff
from process import scoped_catalog_named_storage as storage
from process.scoped_catalog_publisher import run_publisher
from tests.test_named_scoped_catalog_postgres import _attempt, _prepare, _resources


async def _prepare_effects(actors, schema):
    """Capture actual key images before the named catalog replacement."""
    incoming = "dictionary_" + uuid4().hex
    previous = "dictionary_" + uuid4().hex
    async with actors.publisher.begin() as session:
        await session.execute(text(f'SET LOCAL ROLE "{actors.roles["owner"]}"'))
        connection = await session.connection()
        metadata = MetaData()
        CodeCrosswalk.__table__.to_metadata(metadata, schema=schema)
        await connection.run_sync(metadata.create_all)
        await session.execute(text(f'GRANT SELECT ON "{schema}".code_crosswalk TO "{actors.roles["builder"]}"'))
        for stage in (incoming, previous):
            await session.execute(text(f'CREATE SCHEMA "{stage}"'))
            metadata = MetaData()
            for model in (*dictionary._DRUG_SCOPED_MODELS, *dictionary._DRUG_EFFECT_MODELS):
                model.__table__.to_metadata(metadata, schema=stage)
            await connection.run_sync(metadata.create_all)
        await session.execute(
            text(
                f'INSERT INTO "{incoming}".drug_claims_code_catalog (code_system,code,source,display_name) '
                "VALUES ('HP_RX_CODE','synthetic','synthetic-drug','Example')"
            )
        )
        await session.execute(
            text(
                f'INSERT INTO "{incoming}".drug_claims_code_crosswalk '
                "(from_system,from_code,to_system,to_code,source) "
                "VALUES ('HP_RX_CODE','synthetic','NDC','123','synthetic-drug')"
            )
        )
        for mirror, effect in zip(dictionary._DRUG_SCOPED_MODELS, dictionary._DRUG_EFFECT_MODELS, strict=True):
            oid = await native._relation_oid(session, schema, mirror.__source_table__)
            await dictionary._capture_dictionary_effect_table(session, incoming, schema, mirror, effect, oid, False)
    return incoming, previous


async def _copy_crosswalk(actors, schema):
    """Copy a real named table while preserving every frozen effect receipt byte."""
    async with actors.publisher.begin() as session:
        await session.execute(text(f'SET LOCAL ROLE "{actors.roles["owner"]}"'))
        await session.execute(
            text(f'CREATE TABLE "{schema}".crosswalk_copy (LIKE "{schema}".code_crosswalk INCLUDING ALL)')
        )
        await session.execute(text(f'INSERT INTO "{schema}".crosswalk_copy SELECT * FROM "{schema}".code_crosswalk'))
        await session.execute(text(f'ALTER TABLE "{schema}".code_crosswalk RENAME TO crosswalk_original'))
        await session.execute(text(f'ALTER TABLE "{schema}".crosswalk_copy RENAME TO code_crosswalk'))
        await session.execute(text(f'GRANT SELECT ON "{schema}".code_crosswalk TO "{actors.roles["builder"]}"'))


async def _effect_receipts(session, schema):
    return tuple(
        [
            await session.scalar(
                text(f'SELECT jsonb_agg(to_jsonb(effect)) FROM "{schema}"."{model.__tablename__}" effect')
            )
            for model in dictionary._DRUG_EFFECT_MODELS
        ]
    )


async def _generation(session, schema, importer):
    if importer == "code-sets":
        return await codes.read_generation(session, schema)
    return await drg.read_current_generation(session, schema)


async def _legacy_receipt(session, schema, importer):
    """Represent copied historical IDs without changing semantic wire fields."""
    if importer == "code-sets":
        await session.execute(text(f'UPDATE "{schema}".code_sets_result_generation SET code_catalog_oid=999'))
    else:
        receipt = (await _generation(session, schema, importer))["receipt"]
        for index, table in enumerate(receipt["tables"]):
            table["relation_oid"] = 999 + index
        await session.execute(
            text(f'UPDATE "{schema}".ms_drg_result_generation SET receipt=CAST(:receipt AS jsonb)'),
            {"receipt": json.dumps(receipt)},
        )


async def _assert_native_readers(actors, schema, importer, expected):
    async with actors.builder.begin() as session:
        if importer == "code-sets":
            oid = await native._relation_oid(session, schema, "code_catalog")
            observed = await binding.require_code_sets_binding(
                session,
                schema,
                expected,
                schema_sha256=codes._schema_digest(await codes._column_signature(session, oid)),
            )
        else:
            observed = await binding.require_ms_drg_binding(session, schema, expected)
        assert observed == expected
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(text(f'DELETE FROM "{schema}".code_catalog'))
    async with actors.publisher.begin() as session:
        rebind = binding.rebind_code_sets_generation if importer == "code-sets" else binding.rebind_ms_drg_generation
        assert await rebind(session, schema, expected) == expected
        assert await _generation(session, schema, importer) == expected


async def _assert_source_refusals(actors, schema, importer):
    mutations = (
        f"""UPDATE "{schema}".code_catalog SET display_name='changed' WHERE code_system<>'OTHER'""",
        f'ALTER TABLE "{schema}".code_catalog ADD COLUMN unexpected TEXT',
        f'DROP TABLE "{schema}".code_relationship',
        f'UPDATE "{schema}".{codes.TABLE if importer == "code-sets" else drg.TABLE} SET origin_generation=NULL',
    )
    async with actors.publisher.begin() as session:
        oid = await native._relation_oid(session, schema, "code_catalog")
        for statement in mutations:
            with pytest.raises(
                RuntimeError, match="content changed|result changed|columns differ|incomplete|authority"
            ):
                async with session.begin_nested():
                    await session.execute(text(statement))
                    await binding.require_current_catalog_rebinding(session, schema, oid)


async def _assert_effect_refusals(actors, schema, incoming, previous):
    options_by_field = dict(incoming_schema=incoming, current_schema=previous, destination_schema=schema)
    async with actors.publisher.begin() as session:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="destination changed"):
            async with session.begin_nested():
                await session.execute(
                    text(
                        f'INSERT INTO "{schema}".code_crosswalk '
                        "(from_system,from_code,to_system,to_code,source) "
                        "VALUES ('HP_RX_CODE','synthetic','NDC','123','conflicting')"
                    )
                )
                await dictionary.apply_reference_dictionary_effects(session, **options_by_field)
        with pytest.raises(native.ReferenceFamilyArchiveError, match="destination changed"):
            async with session.begin_nested():
                await session.execute(
                    text(
                        f'INSERT INTO "{schema}".code_catalog (code_system,code,source) '
                        "VALUES ('HP_RX_CODE','synthetic','conflicting')"
                    )
                )
                await dictionary.apply_reference_dictionary_effects(session, **options_by_field)
    with pytest.raises(asyncio.CancelledError):
        async with actors.publisher.begin() as session:
            await dictionary.apply_reference_dictionary_effects(session, **options_by_field)
            raise asyncio.CancelledError
    async with actors.builder.begin() as session:
        assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".code_crosswalk')) == 0
        assert (
            await session.scalar(text(f"SELECT count(*) FROM \"{schema}\".code_catalog WHERE code_system='HP_RX_CODE'"))
            == 0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
@pytest.mark.parametrize("legacy", [False, True])
async def test_named_catalog_dictionary_continuation_preserves_generation_and_native_acl(monkeypatch, importer, legacy):
    async with _resources(monkeypatch) as (actors, settings):
        schema = settings.schema
        incoming, previous = await _prepare_effects(actors, schema)
        await _copy_crosswalk(actors, schema)
        candidate = (await _prepare(actors, schema, importer))[handoff.METRIC]
        assert (await run_publisher(settings, _attempt(candidate)))["status"] == "succeeded"
        async with actors.publisher.begin() as session:
            assert not await session.scalar(text("SELECT rolsuper FROM pg_roles WHERE rolname=current_user"))
            if legacy:
                await _legacy_receipt(session, schema, importer)
            expected = await _generation(session, schema, importer)
            original = await storage.table_receipts(session, schema, storage.MODELS)
            original_effects = await _effect_receipts(session, incoming)
        await _assert_native_readers(actors, schema, importer, expected)
        await _assert_source_refusals(actors, schema, importer)
        await _assert_effect_refusals(actors, schema, incoming, previous)
        async with actors.publisher.begin() as session:
            await dictionary.apply_reference_dictionary_effects(
                session, incoming_schema=incoming, current_schema=previous, destination_schema=schema
            )
        async with actors.builder.begin() as session:
            assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".code_crosswalk')) == 1
            assert (
                await session.scalar(
                    text(f"SELECT display_name FROM \"{schema}\".code_catalog WHERE code_system='HP_RX_CODE'")
                )
                == "Example"
            )
            assert await _generation(session, schema, importer) == expected
        async with actors.publisher.begin() as session:
            await dictionary.apply_reference_dictionary_effects(
                session, incoming_schema=previous, current_schema=incoming, destination_schema=schema, rollback=True
            )
            assert await storage.table_receipts(session, schema, storage.MODELS) == original
            assert await _effect_receipts(session, incoming) == original_effects
            assert await _generation(session, schema, importer) == expected
            assert await session.scalar(text(f'SELECT count(*) FROM "{schema}".code_crosswalk')) == 0
