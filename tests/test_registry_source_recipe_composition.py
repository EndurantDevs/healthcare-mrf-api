# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained-page replay refuses incomplete, drifting and nonprogressing inputs."""

from hashlib import sha256
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
    monkeypatch.setattr(composition, "fhir_physical_source", lambda source: source)
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
    from tests.test_network_fhir_retained_capture import retained as capture_fixture

    capture, connection = capture_fixture(), object()
    key = capture.schema_name, capture.capture_id, capture.capture_sha256
    cache = [] if cache_kind == "not-a-dict" else {key: object()}
    pin = AsyncMock()
    monkeypatch.setattr(retained, "pin_retained_fhir_read", pin)
    with pytest.raises(composition.RegistrySourceCompositionError, match="custody_unavailable"):
        await composition._retained_recipe_read(connection, capture, cache)
    pin.assert_not_awaited()


async def test_retained_read_cache_reuses_complete_validation_by_named_edition(monkeypatch):
    from copy import deepcopy

    from process import network_fhir_retained_capture as retained
    from tests.test_network_fhir_retained_capture import retained as capture_fixture

    capture, connection, cache = capture_fixture(), object(), {}
    selected = retained.RetainedFHIRRead(capture)
    pin = AsyncMock(return_value=selected)
    monkeypatch.setattr(retained, "pin_retained_fhir_read", pin)
    assert await composition._retained_recipe_read(connection, capture, cache) is selected
    assert await composition._retained_recipe_read(connection, deepcopy(capture), cache) is selected
    pin.assert_awaited_once_with(connection, capture)
    assert list(cache) == [(capture.schema_name, capture.capture_id, capture.capture_sha256)]


@pytest.mark.parametrize("available", [False, True])
async def test_plain_fhir_recipe_checks_named_publication_data(available):
    from tests.registry_source_recipe_support import _example

    source = _example().source_pin
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=available))
    if available:
        await composition._require_fhir_custody(connection, source)
    else:
        with pytest.raises(composition.RegistrySourceCompositionError, match="custody_unavailable"):
            await composition._require_fhir_custody(connection, source)
    assert "provider_directory_endpoint_dataset" in connection.fetchval.call_args.args[0]
    assert "pg_roles" not in connection.fetchval.call_args.args[0]


@pytest.mark.parametrize("rows,recipes", [(1, [object()]), (0, []), (0, [SimpleNamespace(source_pin=object())])])
async def test_retained_fast_path_refuses_custom_or_unverified_sources_before_sql(rows, recipes):
    connection = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(composition.RegistrySourceCompositionError, match="scope_invalid"):
        await composition._stage_verified_retained_recipes(
            connection, recipes, SimpleNamespace(total_rows=rows), "raw_stage", "registry", {}, (object(), ())
        )
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "second_page,cursor,error,copies",
    [
        (_page(generation_id="edition-two"), ("role", "two"), "generation_changed", 1),
        (_page(), ("role", "one"), "cursor_invalid", 2),
        (_empty_page(membership_rows=1), None, "accounting_invalid", 2),
        (_page(accepted_source_rows=0), ("role", "two"), "accounting_invalid", 2),
    ],
)
async def test_ordinary_source_staging_refuses_mixed_or_incomplete_pages(
    monkeypatch, second_page, cursor, error, copies
):
    from tests.registry_source_recipe_support import _example

    recipe = _example()
    connection = object()
    read_context = SimpleNamespace(office_authority=None, aca_reads_by_oid=None, retained_fhir_reads={})
    first_cursor = ("role", "one")
    read_page = AsyncMock(side_effect=[(_page(), first_cursor, 1000), (second_page, cursor, 1000)])
    copy_page = AsyncMock()
    monkeypatch.setattr(composition, "_require_recipe_custody", AsyncMock())
    monkeypatch.setattr(composition, "_read_recipe_page", read_page)
    monkeypatch.setattr(composition, "_copy_recipe_page", copy_page)
    with pytest.raises(composition.RegistrySourceCompositionError, match=error):
        await composition._stage_recipe(connection, recipe, object(), "raw_stage", "registry", read_context)
    assert [call.args[3] for call in read_page.await_args_list] == [None, first_cursor]
    assert copy_page.await_count == copies


@pytest.mark.parametrize("failure", ["codec-count", "short-copy", "trailing-bytes"])
async def test_ordinary_source_copy_requires_full_binary_accounting(monkeypatch, failure):
    input_bytes, binary_bytes = b"staged membership", b"binary membership"
    page = _page(input_bytes=input_bytes, input_sha256=sha256(input_bytes).hexdigest())
    monkeypatch.setattr(composition, "_encode", lambda _: (binary_bytes, 0 if failure == "codec-count" else 1))

    async def receive_copy(table, *, source, **options_by_field):
        assert table == "raw_stage" and options_by_field == {
            "schema_name": "pg_temp",
            "columns": composition.COPY_COLUMNS,
            "format": "binary",
        }
        if failure == "trailing-bytes":
            assert source.read(1) == binary_bytes[:1]
        else:
            assert source.read() == binary_bytes
        return "COPY 0" if failure == "short-copy" else "COPY 1"

    connection = SimpleNamespace(copy_to_table=AsyncMock(side_effect=receive_copy))
    with pytest.raises(composition.RegistrySourceCompositionError, match="accounting_invalid"):
        await composition._copy_recipe_page(connection, page, "raw_stage")
    assert connection.copy_to_table.await_count == int(failure != "codec-count")
