# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native scoped cutover preserves unrelated catalog rows and can restore its before-image."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import clinical_reference_result_archive as archive
from process import reference_family_archive as family
from tests.test_reference_family_archive_postgres import _database_url


async def _create_scoped_fixtures(session, live: str, incoming: str, conflicting: str) -> None:
    """Seed one owned source and one foreign source in each shared table."""
    spec = family.ReferenceFamilySpec("clinical-reference", archive.SHARED_MODELS)
    for schema in (live, incoming, conflicting):
        await family._create_model_family(session, spec, schema, create_indexes=False)
    for model in archive.SHARED_MODELS:
        name = model.__tablename__
        keys = tuple(column.name for column in model.__table__.primary_key.columns)
        key_columns = ", ".join(f'"{key}"' for key in keys)
        parameters = ", ".join(f":key_{index}" for index in range(len(keys)))
        for schema, marker, source_name in (
            (live, "foreign", "other_source"),
            (live, "old", archive.CLINICAL_REFERENCE_SOURCES[0]),
            (incoming, "new", archive.CLINICAL_REFERENCE_SOURCES[0]),
            (conflicting, "foreign", archive.CLINICAL_REFERENCE_SOURCES[0]),
        ):
            await session.execute(
                text(f'INSERT INTO "{schema}"."{name}" ({key_columns},source) VALUES ({parameters},:source)'),
                {**{f"key_{index}": marker for index in range(len(keys))}, "source": source_name},
            )


async def _seed_descriptions(session, live: str, incoming: str) -> None:
    for schema, code, display_name, short_description in (
        (live, "old", "old display", "old short"),
        (incoming, "new", "new display", "new short"),
    ):
        await session.execute(
            text(
                f'UPDATE "{schema}".code_catalog '
                "SET display_name=:display_name, short_description=:short_description WHERE code=:code"
            ),
            {"display_name": display_name, "short_description": short_description, "code": code},
        )


async def _reorder_live_descriptions(session, live: str) -> None:
    for old_name, new_name in (
        ("display_name", "description_swap"),
        ("short_description", "display_name"),
        ("description_swap", "short_description"),
    ):
        await session.execute(text(f'ALTER TABLE "{live}".code_catalog RENAME COLUMN {old_name} TO {new_name}'))


async def _assert_scoped_rows(session, live: str, marker: str) -> None:
    descriptions = (
        await session.execute(
            text(f'SELECT display_name,short_description FROM "{live}".code_catalog WHERE code=:code'),
            {"code": marker},
        )
    ).one()
    assert descriptions == (f"{marker} display", f"{marker} short")
    for model in archive.SHARED_MODELS:
        catalog_rows = (
            await session.execute(
                text(
                    f'SELECT "{next(iter(model.__table__.primary_key.columns)).name}",source '
                    f'FROM "{live}"."{model.__tablename__}" ORDER BY 1'
                )
            )
        ).all()
        assert catalog_rows == [("foreign", "other_source"), (marker, archive.CLINICAL_REFERENCE_SOURCES[0])]


@pytest.mark.asyncio
async def test_scoped_merge_collision_and_rollback():
    """Preserve scoped payloads across activation and rollback with reordered columns."""
    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    suffix = uuid4().hex
    live = "clinical_live_" + suffix
    incoming = "clinical_new_" + suffix
    conflicting = "clinical_bad_" + suffix
    prior = "clinical_prior_" + suffix
    try:
        async with sessions.begin() as session:
            await _create_scoped_fixtures(session, live, incoming, conflicting)
            await _seed_descriptions(session, live, incoming)
        with pytest.raises(archive.ClinicalReferenceScopeError, match="collides"):
            async with sessions.begin() as session:
                await archive.replace_shared_sources(session, destination=live, replacement=conflicting)
        async with sessions.begin() as session:
            await archive.capture_shared_before_image(session, destination=live, predecessor=prior)
            before = await archive.shared_effect_receipt(session, schema=prior)
            assert [entry["row_count"] for entry in before["tables"]] == [1] * 4
            await _reorder_live_descriptions(session, live)
            await archive.replace_shared_sources(session, destination=live, replacement=incoming)
        async with sessions.begin() as session:
            await _assert_scoped_rows(session, live, "new")
            await archive.replace_shared_sources(session, destination=live, replacement=prior)
            assert await archive.shared_effect_receipt(session, schema=prior) == before
        async with sessions.begin() as session:
            await _assert_scoped_rows(session, live, "old")
    finally:
        async with engine.begin() as connection:
            for schema in (prior, conflicting, incoming, live):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
