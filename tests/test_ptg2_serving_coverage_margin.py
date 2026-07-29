# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import pytest
from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import (
    FakeResult,
    FakeSession,
    strict_v3_tables,
)

_PROVIDER_ID = "00000000000000000000000000000007"


def _tables(**overrides):
    table_overrides_by_name = {
        "price_dictionary_item_count": 64,
        "price_dictionary_block_bytes": 4096,
        "coverage_scope_id": "coverage-scope",
        "plan_id": "plan-a",
        "plan_market_type": "group",
    }
    table_overrides_by_name.update(overrides)
    return strict_v3_tables(**table_overrides_by_name)


def _entry(
    *,
    code_key=11,
    provider_set_key=7,
    provider_count=3,
    price_key=13,
    source_key=0,
):
    return serving.PTG2V3PageRecord(
        code_key=code_key,
        provider_set_key=provider_set_key,
        provider_count=provider_count,
        price_key=price_key,
        source_key=source_key,
    )


def _page(*entries, total_row_count=None):
    return serving.PTG2V3ProviderPage(
        entries=tuple(entries),
        total_row_count=len(entries) if total_row_count is None else total_row_count,
    )


def _reverse_query(*, provider_set_ids=(_PROVIDER_ID,), limit=1):
    return serving._VersionThreeReverseQuery(
        provider_set_ids=provider_set_ids,
        requested_plan="plan-a",
        code_value="",
        code_system=None,
        q_text="",
        code_context=None,
        source_trace_set_hash=None,
        network_names=[],
        limit=limit,
        offset=0,
        apply_window=True,
        plan_market_type="group",
    )


async def _raises_manifest_error(match, awaitable):
    with pytest.raises(serving.PTG2ManifestArtifactError, match=match):
        await awaitable


def _raises_sync_manifest_error(match, callback):
    with pytest.raises(serving.PTG2ManifestArtifactError, match=match):
        callback()


@pytest.mark.asyncio
async def test_provider_set_metadata_skips_then_rejects_duplicate_rows(monkeypatch):
    metadata = serving._ProviderSetGraphMetadata(7, 3)
    parser = Mock(
        side_effect=[None, (_PROVIDER_ID, metadata), (_PROVIDER_ID, metadata)]
    )
    monkeypatch.setattr(serving, "_provider_set_metadata_from_fields", parser)
    await _raises_manifest_error(
        "duplicate identity",
        serving._provider_set_metadata_for_ids(
            FakeSession([FakeResult([{}, {}, {}])]),
            _tables(),
            (_PROVIDER_ID,),
        ),
    )
    assert parser.call_count == 3


@pytest.mark.asyncio
async def test_provider_page_count_and_empty_scope_boundaries(monkeypatch):
    assert (
        await serving._version_three_provider_pages_for_keys(object(), _tables(), None)
        is None
    )
    assert (
        await serving._version_three_provider_pages_for_keys(object(), _tables(), ())
        == {}
    )
    monkeypatch.setattr(
        serving,
        "_version_three_provider_pages_for_keys",
        AsyncMock(return_value={7: _page(_entry(provider_count=9))}),
    )
    assert await serving._version_three_provider_counts_for_keys(
        object(), _tables(), (7,)
    ) == {7: 9}


def test_forward_window_rejects_missing_dense_price_key():
    _raises_sync_manifest_error(
        "missing its dense price key",
        lambda: serving._shared_forward_row_window(
            (SimpleNamespace(provider_set_key=7, price_key=None),),
            {7: _PROVIDER_ID},
            limit=1,
            offset=0,
            descending=False,
        ),
    )


def test_page_price_lookup_requires_authenticated_dictionary_bounds():
    _raises_sync_manifest_error(
        "price metadata is missing",
        lambda: serving._version_three_page_price_lookup_hints(
            _tables(price_dictionary_item_count=None)
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider_ids", "price_ids", "message"),
    (({}, {}, "unknown provider set"), ({7: _PROVIDER_ID}, {}, "unknown price set")),
)
async def test_forward_page_rejects_unknown_dictionary_keys(
    monkeypatch, provider_ids, price_ids, message
):
    monkeypatch.setattr(
        serving, "_provider_set_ids_for_keys", AsyncMock(return_value=provider_ids)
    )
    monkeypatch.setattr(
        serving, "lookup_price_ids_from_db", AsyncMock(return_value=price_ids)
    )
    await _raises_manifest_error(
        message,
        serving._version_three_forward_page_ids(object(), _tables(), (_entry(),)),
    )


def test_provider_page_rejects_empty_projection():
    _raises_sync_manifest_error(
        "projection has no rows",
        lambda: serving._version_three_provider_code_entries({7: _page()}, 11),
    )


@pytest.mark.asyncio
async def test_provider_filtered_page_boundaries():
    common_args_by_name = {
        "session": object(),
        "serving_tables": _tables(),
        "code_metadata": {"code_key": 11},
        "network_names": [],
        "limit": 1,
        "offset": 0,
    }
    assert (
        await serving._version_three_provider_filtered_page_rows(
            **common_args_by_name,
            provider_pages_by_key={7: _page(_entry())},
            descending=True,
        )
        is None
    )
    assert (
        await serving._version_three_provider_filtered_page_rows(
            **common_args_by_name,
            provider_pages_by_key={7: _page(_entry(), total_row_count=2)},
            descending=False,
        )
        is None
    )
    common_args_by_name["code_metadata"] = {"code_key": 12}
    assert (
        await serving._version_three_provider_filtered_page_rows(
            **common_args_by_name,
            provider_pages_by_key={7: _page(_entry())},
            descending=False,
        )
        == []
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metadata", "limit", "descending"),
    (
        ({"code_key": 11}, 1, True),
        ({"code_key": 11}, serving.PTG2_SERVING_BINARY_V3_PAGE_ROWS + 1, False),
        ({}, 1, False),
    ),
)
async def test_forward_page_ineligible_boundaries(metadata, limit, descending):
    assert (
        await serving._version_three_forward_page_rows(
            object(),
            _tables(),
            code_metadata=metadata,
            source_trace_set_hash=None,
            network_names=[],
            limit=limit,
            offset=0,
            descending=descending,
        )
        is None
    )


@pytest.mark.asyncio
async def test_zero_length_prefix_falls_back_without_io(monkeypatch):
    monkeypatch.setattr(
        serving, "_version_three_forward_page_rows", AsyncMock(return_value=None)
    )
    request = serving._SharedCodeRowsRequest(
        code_data={"code_key": 11},
        provider_set_keys=None,
        source_trace_set_hash=None,
        network_names=[],
        limit=0,
    )
    assert (
        await serving._shared_code_prefix_rows(object(), _tables(), request, 11) is None
    )


@pytest.mark.asyncio
async def test_manifest_code_group_propagates_unavailable_rows(monkeypatch):
    monkeypatch.setattr(serving, "_shared_rows_for_scope", AsyncMock(return_value=None))
    assert (
        await serving._read_manifest_code_groups(
            object(),
            _tables(),
            {11: ({"code_key": 11, "rate_count": 1},)},
            object(),
            1,
        )
        is None
    )


@pytest.mark.asyncio
async def test_projected_code_rows_return_page_counts_on_fallback(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_version_three_provider_filtered_page_rows",
        AsyncMock(return_value=None),
    )
    rows, counts = await serving._version_three_projected_code_rows(
        object(),
        _tables(),
        {"code_key": 11},
        {7: _page(_entry(provider_count=9))},
        [],
        1,
        0,
        False,
    )
    assert rows is None
    assert counts == {7: 9}


def test_materialization_skips_dictionary_id_that_disappears(monkeypatch):
    forward_row = SimpleNamespace(provider_set_key=7)
    monkeypatch.setattr(
        serving, "_shared_forward_row_window", Mock(return_value=[forward_row])
    )
    assert (
        serving._materialize_full_shared_rows(
            (forward_row,), {}, {}, None, [], 1, 0, False
        )
        == []
    )


def test_reverse_text_filter_and_missing_code_metadata_boundaries():
    filters: list[str] = []
    query_params_by_name: dict[str, object] = {}
    serving._append_provider_reverse_text_filter(
        filters,
        query_params_by_name,
        "mri",
    )
    assert filters == [serving._PTG2_PROVIDER_REVERSE_TEXT_FILTER_SQL]
    assert query_params_by_name == {"q_like": "%mri%"}
    assert (
        serving._version_three_candidate_rows(
            ({"reported_code": "70553"},), {}, {}, None, []
        )
        == []
    )


@pytest.mark.asyncio
async def test_reverse_exact_code_metadata_fails_closed(monkeypatch):
    monkeypatch.setattr(
        serving, "_manifest_reverse_code_rows", AsyncMock(return_value=None)
    )
    await _raises_manifest_error(
        "code dictionary is unavailable",
        serving._version_three_exact_code_metadata(
            object(), _tables(), _reverse_query()
        ),
    )


def test_reverse_row_window_full_and_unbounded_paths():
    full = serving._VersionThreeRowWindow(
        limit=1, remaining_offset=0, candidates=[{"id": 1}]
    )
    full.add_code_candidates([{"id": 2}])
    assert (full.candidates, full.rows_seen) == ([{"id": 1}], 0)
    unbounded = serving._VersionThreeRowWindow(limit=None, remaining_offset=1)
    unbounded.add_code_candidates([{"id": 1}, {"id": 2}])
    assert (unbounded.candidates, unbounded.rows_seen) == ([{"id": 2}], 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata_rows", (None, ()))
async def test_reverse_candidate_batch_terminal_metadata(monkeypatch, metadata_rows):
    monkeypatch.setattr(
        serving,
        "_manifest_reverse_code_rows",
        AsyncMock(return_value=metadata_rows),
    )
    result = await serving._version_three_candidate_batch(
        object(),
        _tables(),
        _reverse_query(),
        serving._VersionThreeReverseScope({7: _PROVIDER_ID}, (11,)),
        0,
        1,
    )
    assert result is None if metadata_rows is None else result == ([], 0)


@pytest.mark.asyncio
async def test_single_plan_page_rejects_market_mismatch():
    assert not await serving._has_single_plan_page_order(
        object(), _tables(), "plan-a", "individual"
    )


@pytest.mark.asyncio
async def test_page_projection_rejects_missing_provider_dictionary(monkeypatch):
    monkeypatch.setattr(
        serving, "_provider_set_keys_for_ids", AsyncMock(return_value={})
    )
    await _raises_manifest_error(
        "missing a referenced provider set",
        serving._load_version_three_page_projection(
            object(), _tables(), _reverse_query()
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("provider_pages", (None, {}))
async def test_page_projection_unavailable_or_incomplete(monkeypatch, provider_pages):
    monkeypatch.setattr(
        serving,
        "_provider_set_keys_for_ids",
        AsyncMock(return_value={_PROVIDER_ID: 7}),
    )
    monkeypatch.setattr(
        serving,
        "lookup_shared_provider_pages_from_db",
        AsyncMock(return_value=provider_pages),
    )
    call = serving._load_version_three_page_projection(
        object(), _tables(), _reverse_query()
    )
    if provider_pages is None:
        assert await call is None
    else:
        await _raises_manifest_error("missing a referenced provider set", call)


@pytest.mark.asyncio
async def test_page_projection_rejects_unsized_provider_scope(monkeypatch):
    monkeypatch.setattr(
        serving, "has_shared_provider_pages_in_db", AsyncMock(return_value=True)
    )
    assert (
        await serving._version_three_page_projection_scope(
            object(),
            _tables(),
            _reverse_query(provider_set_ids=iter((_PROVIDER_ID,))),
        )
        is None
    )


@pytest.mark.asyncio
async def test_page_window_propagates_unavailable_code_dictionary(monkeypatch):
    monkeypatch.setattr(
        serving, "_manifest_reverse_code_rows", AsyncMock(return_value=None)
    )
    scope = serving._VersionThreePageProjectionScope(
        {7: _PROVIDER_ID}, {7: _page(_entry())}, (_entry(),)
    )
    assert (
        await serving._version_three_page_window(
            object(), _tables(), _reverse_query(), scope
        )
        is None
    )


@pytest.mark.asyncio
async def test_reverse_page_rejects_unknown_price_set(monkeypatch):
    scope = serving._VersionThreePageProjectionScope(
        {7: _PROVIDER_ID}, {7: _page(_entry())}, (_entry(),)
    )
    monkeypatch.setattr(
        serving, "_version_three_page_projection_scope", AsyncMock(return_value=scope)
    )
    monkeypatch.setattr(
        serving,
        "_version_three_page_window",
        AsyncMock(return_value=({11: {"code_key": 11}}, (_entry(),))),
    )
    monkeypatch.setattr(serving, "lookup_price_ids_from_db", AsyncMock(return_value={}))
    await _raises_manifest_error(
        "unknown price set",
        serving._version_three_reverse_page_selection(
            object(), _tables(), _reverse_query()
        ),
    )
