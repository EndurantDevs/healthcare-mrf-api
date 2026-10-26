# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained-page replay refuses incomplete, drifting and nonprogressing inputs."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_source_recipe_composition as composition

pytestmark = pytest.mark.asyncio


def _page(**changes):
    values_by_name = dict(
        generation_id="edition-one",
        complete_raw_facts=True,
        source_rows=1,
        accepted_source_rows=1,
        unresolved_source_rows=0,
        omitted_source_rows=0,
        membership_rows=1,
        omitted_rows=0,
        unresolved_rows=0,
    )
    values_by_name.update(changes)
    return SimpleNamespace(**values_by_name)


def _empty_page(**changes):
    return _page(**{"source_rows": 0, "accepted_source_rows": 0, "membership_rows": 0, **changes})


def _replay(monkeypatch, pages, *, source_rows=1):
    """Provide reader-contract outputs while keeping the replay guards real."""
    pin = SimpleNamespace(
        retained_capture=SimpleNamespace(origin=SimpleNamespace(resource_counts=(("PractitionerRole", source_rows),)))
    )
    recipe = SimpleNamespace(source_pin=pin)
    connection, approved, retained = object(), object(), object()
    reads_by_recipe = {}
    custody = AsyncMock(return_value=retained)
    reader = AsyncMock(side_effect=[(page, cursor, 1000) for page, cursor in pages])
    monkeypatch.setattr(composition, "_require_recipe_custody", custody)
    monkeypatch.setattr(composition, "_read_recipe_page", reader)
    totals_by_kind = {"membership": 0, "omitted": 0, "raw": 0, "accepted": 0, "omitted_raw": 0, "generations": []}
    stream = composition._verified_retained_pages(
        connection, [recipe], approved, "registry", reads_by_recipe, totals_by_kind
    )
    return SimpleNamespace(
        stream=stream,
        totals_by_kind=totals_by_kind,
        custody=custody,
        reader=reader,
        connection=connection,
        recipe=recipe,
        approved=approved,
        retained=retained,
        reads_by_recipe=reads_by_recipe,
    )


async def test_retained_replay_keeps_custody_cursor_and_three_way_counts(monkeypatch):
    page = _page(
        source_rows=4,
        accepted_source_rows=2,
        unresolved_source_rows=1,
        omitted_source_rows=1,
        membership_rows=2,
        unresolved_rows=1,
        omitted_rows=1,
    )
    cursor = ("PractitionerRole", "record-four")
    replay = _replay(monkeypatch, [(page, cursor), (_empty_page(), None)], source_rows=4)
    assert [entry async for entry in replay.stream] == [(page, None, replay.retained)]
    replay.custody.assert_awaited_once_with(
        replay.connection, replay.recipe, replay.approved, "registry", retained_fhir_reads=replay.reads_by_recipe
    )
    assert [call.args[3] for call in replay.reader.await_args_list] == [None, cursor]
    assert all(call.args[6] is replay.retained for call in replay.reader.await_args_list)
    assert replay.totals_by_kind == {
        "membership": 2,
        "omitted": 2,
        "raw": 4,
        "accepted": 2,
        "omitted_raw": 1,
        "generations": ["edition-one"],
    }


@pytest.mark.parametrize("change", [{"complete_raw_facts": False}, {"accepted_source_rows": 0}])
async def test_retained_replay_refuses_unverifiable_facts_before_yield(monkeypatch, change):
    replay = _replay(monkeypatch, [(_page(**change), ("PractitionerRole", "record-one"))])
    with pytest.raises(composition.RegistrySourceCompositionError, match="accounting_invalid"):
        await anext(replay.stream)
    assert replay.reader.await_count == 1 and replay.totals_by_kind["generations"] == []


async def test_retained_replay_refuses_generation_drift_before_next_page(monkeypatch):
    first = _page()
    replay = _replay(
        monkeypatch,
        [(first, ("PractitionerRole", "record-one")), (_page(generation_id="edition-two"), None)],
        source_rows=2,
    )
    assert await anext(replay.stream) == (first, None, replay.retained)
    with pytest.raises(composition.RegistrySourceCompositionError, match="accounting_invalid"):
        await anext(replay.stream)
    assert replay.totals_by_kind["raw"] == 1 and replay.totals_by_kind["generations"] == []


@pytest.mark.parametrize("page,cursor", [(_empty_page(membership_rows=1), None), (_empty_page(), ("role", "one"))])
async def test_empty_retained_page_cannot_hide_memberships_or_a_continuation(monkeypatch, page, cursor):
    replay = _replay(monkeypatch, [(page, cursor)])
    with pytest.raises(composition.RegistrySourceCompositionError, match="cursor_invalid"):
        await anext(replay.stream)
    assert replay.reader.await_count == 1 and replay.totals_by_kind["generations"] == []


@pytest.mark.parametrize("next_cursor", [None, ("role", "two"), ("role", "one")])
async def test_retained_replay_requires_strict_cursor_progress(monkeypatch, next_cursor):
    first, second = _page(), _page()
    initial_cursor = ("role", "two")
    replay = _replay(monkeypatch, [(first, initial_cursor), (second, next_cursor)], source_rows=2)
    assert await anext(replay.stream) == (first, None, replay.retained)
    assert await anext(replay.stream) == (second, initial_cursor, replay.retained)
    with pytest.raises(composition.RegistrySourceCompositionError, match="cursor_invalid"):
        await anext(replay.stream)
    assert replay.reader.await_count == 2 and replay.totals_by_kind["generations"] == []


async def test_retained_replay_requires_complete_origin_resource_count(monkeypatch):
    replay = _replay(monkeypatch, [(_page(), ("role", "one")), (_empty_page(), None)], source_rows=2)
    await anext(replay.stream)
    with pytest.raises(composition.RegistrySourceCompositionError, match="accounting_invalid"):
        await anext(replay.stream)
    assert replay.reader.await_count == 2 and replay.totals_by_kind["generations"] == []


@pytest.mark.parametrize("cache_kind", ["not-a-dict", "foreign-read"])
async def test_retained_read_cache_refuses_untrusted_values_without_admission(monkeypatch, cache_kind):
    from process import network_fhir_retained_capture as retained

    capture, connection = object(), object()
    cache = [] if cache_kind == "not-a-dict" else {capture: object()}
    pin = AsyncMock()
    monkeypatch.setattr(retained, "pin_retained_fhir_read", pin)
    with pytest.raises(composition.RegistrySourceCompositionError, match="custody_unavailable"):
        await composition._retained_recipe_read(connection, capture, cache)
    pin.assert_not_awaited()


@pytest.mark.parametrize("rows,recipes", [(1, [object()]), (0, []), (0, [SimpleNamespace(source_pin=object())])])
async def test_retained_fast_path_refuses_custom_or_unverified_sources_before_sql(rows, recipes):
    connection = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(composition.RegistrySourceCompositionError, match="scope_invalid"):
        await composition._stage_verified_retained_recipes(
            connection, recipes, SimpleNamespace(total_rows=rows), "raw_stage", "registry", {}, (object(), ())
        )
    connection.execute.assert_not_awaited()
