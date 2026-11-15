# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unpublished companion controls do not require catalog tables never installed."""

import pytest
from sqlalchemy import text

from process import scoped_catalog_handoff as handoff
from process import scoped_catalog_named_generation as generations
from process import scoped_catalog_named_storage as storage
from process.scoped_catalog_named_cutover import rollback
from process.scoped_catalog_publisher import run_publisher
from tests.test_named_scoped_catalog_postgres import _attempt, _prepare, _resources


async def _drop_companions(actors, schema, *, complete):
    async with actors.engine.begin() as connection:
        await connection.execute(text(f'DROP TABLE "{schema}".code_synonym'))
        if complete:
            await connection.execute(text(f'DROP TABLE "{schema}".code_relationship'))


@pytest.mark.asyncio
async def test_code_only_catalog_publishes_and_rolls_back_with_both_control_rows(monkeypatch):
    async with _resources(monkeypatch) as (actors, settings):
        await _drop_companions(actors, settings.schema, complete=True)
        prepared = await _prepare(actors, settings.schema, "code-sets")
        candidate = prepared[handoff.METRIC]
        expected = candidate["expected_generations"]
        assert set(expected) == {"code-sets", "ms-drg"}
        assert expected["ms-drg"]["local_generation"] == 0
        async with actors.engine.begin() as connection:
            await connection.execute(
                text(f'UPDATE "{settings.schema}".ms_drg_result_generation SET local_generation=1')
            )
        with pytest.raises(RuntimeError, match="generation vector changed"):
            await run_publisher(settings, _attempt(candidate))
        async with actors.engine.begin() as connection:
            await connection.execute(
                text(f'UPDATE "{settings.schema}".ms_drg_result_generation SET local_generation=0')
            )
        assert (await run_publisher(settings, _attempt(candidate)))["status"] == "succeeded"
        async with actors.publisher.begin() as session:
            receipt = await handoff.read_catalog_handoff_outcome(session, candidate)
            await rollback(session, receipt, settings)
        async with actors.builder.begin() as session:
            observed = await generations.read_generations(session, settings.schema)
            assert observed["ms-drg"] == {**expected["ms-drg"], "local_generation": 1}
            assert observed["code-sets"]["local_generation"] == 2
            assert observed["code-sets"]["origin_generation"] is None
            assert await storage.current_models(session, settings.schema) == storage.MODELS[:1]
            assert await session.scalar(text(f'SELECT count(*) FROM "{settings.schema}".code_catalog')) == 1
            await generations.validate_published_content(session, settings.schema, observed)


@pytest.mark.asyncio
@pytest.mark.parametrize("published", [False, True])
async def test_missing_companion_refuses_partial_or_published_family(monkeypatch, published):
    async with _resources(monkeypatch) as (actors, settings):
        if published:
            prepared = await _prepare(actors, settings.schema, "ms-drg")
            await run_publisher(settings, _attempt(prepared[handoff.METRIC]))
        await _drop_companions(actors, settings.schema, complete=published)
        async with actors.builder.begin() as session:
            observed = await generations.read_generations(session, settings.schema)
            expected_error = (
                "MS-DRG result relation is missing" if published else "shared code-set family is incomplete"
            )
            with pytest.raises(RuntimeError, match=expected_error):
                await generations.validate_published_content(session, settings.schema, observed)
            assert await generations.read_generations(session, settings.schema) == observed
