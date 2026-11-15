# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained source receipts must still describe the family restored by rollback."""

from copy import deepcopy
from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import code_sets_result_archive as codes
from process import reference_family_archive as native
from process import scoped_catalog_binding as binding
from process import scoped_catalog_handoff as handoff
from process import scoped_catalog_named_generation as generations
from process import scoped_catalog_named_retention as retention
from process import scoped_catalog_named_storage as storage
from process.scoped_catalog_named_cutover import rollback
from process.scoped_catalog_publisher import run_publisher
from process.scoped_catalog_retention import canonical_metadata
from tests.test_named_scoped_catalog_postgres import _attempt, _prepare, _resources


async def _published(actors, settings, importer):
    prepared = await _prepare(actors, settings.schema, importer)
    candidate = prepared[handoff.METRIC]
    await run_publisher(settings, _attempt(candidate))
    async with actors.builder.begin() as session:
        return await handoff.read_catalog_handoff_outcome(session, candidate)


async def _assert_code_binding(session, schema, installed):
    oid = await native._relation_oid(session, schema, "code_catalog")
    observed = await binding.require_code_sets_binding(
        session,
        schema,
        installed,
        schema_sha256=codes._schema_digest(await codes._column_signature(session, oid)),
    )
    assert binding.same_code_sets_origin(observed, installed)
    assert (await codes.scope_receipt(session, schema))[:2] == (observed.row_count, observed.row_sha256)
    return observed


@pytest.mark.asyncio
async def test_named_code_sets_reader_survives_copied_metadata_and_restored_origin(monkeypatch):
    async with _resources(monkeypatch) as (actors, settings):
        await _published(actors, settings, "code-sets")
        async with actors.builder.begin() as session:
            installed = await codes.read_generation(session, settings.schema)
            assert installed.code_catalog_oid is None
            assert await _assert_code_binding(session, settings.schema, installed) == installed
            copied = replace(installed, code_catalog_oid=999)
            assert await _assert_code_binding(session, settings.schema, copied) == installed
        second = await _published(actors, settings, "code-sets")
        async with actors.publisher.begin() as session:
            await rollback(session, second, settings)
        async with actors.builder.begin() as session:
            restored = await _assert_code_binding(session, settings.schema, copied)
            assert restored.local_generation > installed.local_generation
            assert restored.origin_generation == installed.origin_generation
            assert restored.code_catalog_oid is None
            with pytest.raises(DBAPIError):
                async with session.begin_nested():
                    await session.execute(text(f'DELETE FROM "{settings.schema}".code_catalog'))


async def _replace_receipt(session, schema, receipt):
    await session.execute(
        text(
            f"UPDATE {storage.qualified(schema, generations.TABLES['ms-drg'])} SET receipt=CAST(:receipt AS jsonb) WHERE id=1"
        ),
        {"receipt": canonical_metadata(receipt)},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
async def test_retained_ms_drg_legacy_ids_remain_inert_and_unchanged(monkeypatch, importer):
    async with _resources(monkeypatch) as (actors, settings):
        await _published(actors, settings, "ms-drg")
        publication = await _published(actors, settings, importer)
        retained = publication["retained"]
        schema = retained["schema_name"]
        async with actors.publisher.begin() as session:
            original = await session.scalar(
                text(f"SELECT receipt FROM {storage.qualified(schema, generations.TABLES['ms-drg'])} WHERE id=1")
            )
            copied = deepcopy(original)
            for index, table in enumerate(copied["tables"]):
                table["relation_oid"] = 900 + index
            await _replace_receipt(session, schema, copied)
        async with actors.builder.begin() as session:
            await retention.require_retained(session, retained)
            assert (
                await session.scalar(
                    text(f"SELECT receipt FROM {storage.qualified(schema, generations.TABLES['ms-drg'])} WHERE id=1")
                )
                == copied
            )
        async with actors.publisher.begin() as session:
            await rollback(session, publication, settings)
            assert (
                await session.scalar(
                    text(
                        f"SELECT receipt FROM {storage.qualified(settings.schema, generations.TABLES['ms-drg'])} WHERE id=1"
                    )
                )
                == copied
            )
            restored = await generations.read_generations(session, settings.schema, importer=importer)
            await generations.validate_published_content(session, settings.schema, restored)


def _tampered_receipt(original, change):
    changed = deepcopy(original)
    if change == "digest":
        changed["content_sha256"] = "0" * 64
    elif change == "sources":
        changed["tables"][0]["sources"] = ["unrelated-source"]
    elif change == "schema":
        changed["tables"][0]["schema_sha256"] = "0" * 64
    elif change == "count":
        changed["tables"][0]["row_count"] += 1
    else:
        changed["tables"].pop()
    return changed


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
@pytest.mark.parametrize("change", ["digest", "sources", "schema", "count", "missing-table"])
async def test_tampered_retained_ms_drg_receipt_refuses_before_rollback(monkeypatch, importer, change):
    async with _resources(monkeypatch) as (actors, settings):
        await _published(actors, settings, "ms-drg")
        publication = await _published(actors, settings, importer)
        retained = publication["retained"]
        schema = retained["schema_name"]
        async with actors.builder.begin() as session:
            await retention.require_retained(session, retained)
            with pytest.raises(DBAPIError):
                async with session.begin_nested():
                    await _replace_receipt(session, schema, {})
        async with actors.publisher.begin() as session:
            before = await generations.read_generations(session, settings.schema, importer=importer)
            tables = await storage.table_receipts(session, settings.schema, storage.MODELS)
            original = await session.scalar(
                text(f"SELECT receipt FROM {storage.qualified(schema, generations.TABLES['ms-drg'])} WHERE id=1")
            )
            await _replace_receipt(session, schema, _tampered_receipt(original, change))
            with pytest.raises(RuntimeError, match="content or structure differs"):
                await rollback(session, publication, settings)
            assert await generations.read_generations(session, settings.schema, importer=importer) == before
            assert await storage.table_receipts(session, settings.schema, storage.MODELS) == tables
            await _replace_receipt(session, schema, original)
            await rollback(session, publication, settings)
            restored = await generations.read_generations(session, settings.schema, importer=importer)
            await generations.validate_published_content(session, settings.schema, restored)
