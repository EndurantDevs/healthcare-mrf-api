# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete ranked hydration shares capacity across physical codes and aliases."""

import asyncio
from collections import Counter
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_db_sidecars as sidecars
from api import ptg2_serving as serving
from tests.test_ptg2_graph_runtime_boundaries import _local_distance_tables
from tests.test_ptg2_postgres_binary_v3_cold_paths import _provider_page, _version_three_tables
from tests.test_ptg2_v3_bounded_readers import (
    _fragment,
    _grouped_payload,
    _patch_reference_lookups,
    _shard_block_key,
)


def _code(code_key, *, plan="synthetic-plan", rate_count=100_000):
    return {
        "code_key": code_key,
        "plan_id": plan,
        "plan_market_type": "group",
        "reported_code_system": "RC",
        "reported_code": "0450" if code_key == 7 else "450",
        "rate_count": rate_count,
    }


def _install_reader(monkeypatch, branch, prices_by_code, *, failure=None):
    """Replace storage boundaries while retaining page and packed-prefix readers."""
    pairs = tuple((code, price) for code, prices in prices_by_code.items() for price in prices)
    page = (
        _provider_page(3, pairs) if branch == "page" else _provider_page(3, ((1, 1),), total_row_count=len(pairs) + 1)
    )
    page_lookup = AsyncMock(return_value={3: page})
    monkeypatch.setattr(serving, "lookup_shared_provider_pages_from_db", page_lookup)
    monkeypatch.setattr(serving, "_provider_set_ids_for_keys", AsyncMock(return_value={3: "11" * 16}))

    async def price_ids(_session, keys, **_kwargs):
        if failure is not None and 9 in keys:
            raise failure
        return {key: f"{key:032x}" for key in keys}

    monkeypatch.setattr(serving, "lookup_price_ids_from_db", AsyncMock(side_effect=price_ids))
    _patch_reference_lookups(monkeypatch)
    fragment_by_code = {
        code: _fragment(
            _grouped_payload(2, [(3, [(price, 0) for price in prices])]),
            entry_count=1,
            block_key=_shard_block_key(code, 3),
        )
        for code, prices in prices_by_code.items()
    }
    streamed_codes = []

    async def shards(_session, options, code_keys):
        assert tuple(options.provider_set_keys) == (3,)
        return {code: (_shard_block_key(code, 3),) for code in code_keys}, frozenset({3}), True

    async def stream(_session, **kwargs):
        assert branch == "fallback"
        assert kwargs["require_all"] is True
        for block_key in kwargs["block_keys"]:
            code = block_key >> 31
            streamed_codes.append(code)
            if failure is not None and code == 8:
                raise failure
            yield fragment_by_code[code]

    monkeypatch.setattr(sidecars, "_forward_shard_keys_for_read", shards)
    monkeypatch.setattr(sidecars, "stream_shared_blocks", stream)
    reader = AsyncMock(wraps=serving._shared_rows_for_scope)
    monkeypatch.setattr(serving, "_shared_rows_for_scope", reader)
    budget = serving.ForwardReadBudget(10, 100_000, maximum_row_capacity=11)
    budget.reserve_row_capacity(read_rows=2, result_rows=2)
    limits = replace(serving._v4_geo_rate_forward_limits(_local_distance_tables()), scan_budget=budget)
    return SimpleNamespace(
        limits=limits,
        page_lookup=page_lookup,
        reader=reader,
        streamed_codes=streamed_codes,
        fragment_by_code=fragment_by_code,
    )


async def _read(case, code_rows):
    return await serving._complete_ranked_manifest_code_rows(
        object(),
        _version_three_tables(),
        code_rows=code_rows,
        provider_set_keys=(3,),
        network_names=[],
        limits=case.limits,
    )


def _assert_released(case):
    budget = case.limits.scan_budget
    assert budget.active_read_row_capacity == budget.active_result_row_capacity == 2
    assert budget.peak_read_row_capacity == budget.peak_result_row_capacity == 11


@pytest.mark.asyncio
@pytest.mark.parametrize("branch", ["page", "fallback"])
async def test_ranked_forms_share_allowance_with_real_readers(monkeypatch, branch):
    case = _install_reader(monkeypatch, branch, {7: [8], 8: [9]})

    rows = await _read(case, [_code(7), _code(8)])

    assert [(row["reported_code"], row["price_key"]) for row in rows] == [("0450", 8), ("450", 9)]
    assert [call.kwargs["limit"] for call in case.reader.await_args_list] == [9, 8]
    assert all(call.args[3].scan_budget is case.limits.scan_budget for call in case.reader.await_args_list)
    case.page_lookup.assert_awaited_once()
    _assert_released(case)
    budget = case.limits.scan_budget
    assert budget.fragment_count == (1 if branch == "page" else 3)
    expected_bytes = serving._PTG2_PAGE_SCAN_MAX_RAW_BYTES
    if branch == "fallback":
        assert case.streamed_codes == [7, 8]
        expected_bytes += sum(len(fragment.payload) for fragment in case.fragment_by_code.values())
    else:
        assert case.streamed_codes == []
    assert budget.raw_payload_bytes == expected_bytes


@pytest.mark.asyncio
@pytest.mark.parametrize("branch", ["page", "fallback"])
@pytest.mark.parametrize("second_count", [4, 5])
async def test_ranked_forms_preserve_duplicates_and_aliases_at_shared_boundary(monkeypatch, branch, second_count):
    case = _install_reader(monkeypatch, branch, {7: [8, 8], 8: list(range(9, 9 + second_count))})
    code_rows = [_code(7), _code(7, plan="synthetic-alias"), _code(8)]

    if second_count == 5:
        with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded, match="code_occurrences"):
            await _read(case, code_rows)
    else:
        rows = await _read(case, code_rows)
        assert Counter((row["code_key"], row["plan_id"], row["price_key"]) for row in rows) == Counter(
            {
                (7, "synthetic-plan", 8): 2,
                (7, "synthetic-alias", 8): 2,
                **{(8, "synthetic-plan", price): 1 for price in range(9, 13)},
            }
        )
    assert [call.args[2]["code_key"] for call in case.reader.await_args_list] == [7, 8]
    assert [call.kwargs["limit"] for call in case.reader.await_args_list] == [5, 5]
    _assert_released(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("branch", ["page", "fallback"])
@pytest.mark.parametrize("physical_count", [4, 5])
async def test_ranked_alias_limit_keeps_a_physical_overflow_sentinel(monkeypatch, branch, physical_count):
    case = _install_reader(monkeypatch, branch, {7: [8] * physical_count})
    code_rows = [_code(7), _code(7, plan="synthetic-alias")]

    if physical_count == 5:
        with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded, match="code_occurrences"):
            await _read(case, code_rows)
    else:
        rows = await _read(case, code_rows)
        assert Counter(row["plan_id"] for row in rows) == {"synthetic-plan": 4, "synthetic-alias": 4}
    case.reader.assert_awaited_once()
    assert case.reader.await_args.kwargs["limit"] == 5
    _assert_released(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_count", [None, True, -1, "invalid", 100_001])
async def test_ranked_alias_metadata_refuses_before_storage(monkeypatch, invalid_count):
    case = _install_reader(monkeypatch, "page", {7: [8]})

    with pytest.raises(serving.PTG2ManifestArtifactError, match="rate count"):
        await _read(case, [_code(7), _code(7, plan="synthetic-alias", rate_count=invalid_count)])

    case.page_lookup.assert_not_awaited()
    case.reader.assert_not_awaited()
    budget = case.limits.scan_budget
    assert budget.active_read_row_capacity == budget.active_result_row_capacity == 2
    assert budget.fragment_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("branch", ["page", "fallback"])
@pytest.mark.parametrize("declared_count", [0, 1])
async def test_ranked_reader_rejects_underdeclared_rows(monkeypatch, branch, declared_count):
    case = _install_reader(monkeypatch, branch, {7: [8, 8]})

    with pytest.raises(serving.PTG2ManifestArtifactError, match="declared rate count"):
        await _read(case, [_code(7, rate_count=declared_count)])

    assert case.reader.await_args.kwargs["limit"] == 9
    _assert_released(case)


@pytest.mark.asyncio
@pytest.mark.parametrize("branch", ["page", "fallback"])
@pytest.mark.parametrize("failure_type", [RuntimeError, asyncio.CancelledError])
async def test_ranked_reader_failure_releases_only_its_reservation(monkeypatch, branch, failure_type):
    case = _install_reader(monkeypatch, branch, {7: [8], 8: [9]}, failure=failure_type())

    with pytest.raises(failure_type):
        await _read(case, [_code(7), _code(8)])

    assert case.reader.await_count == 2
    _assert_released(case)


@pytest.mark.asyncio
async def test_ranked_fallback_validates_occurrences_beyond_retained_prefix(monkeypatch):
    case = _install_reader(monkeypatch, "fallback", {7: [8, 8, 1024]})
    code_rows = [_code(7, plan=f"synthetic-plan-{index}") for index in range(5)]

    with pytest.raises(serving.PTG2ManifestArtifactError):
        await _read(case, code_rows)

    assert case.reader.await_args.kwargs["limit"] == 2
    _assert_released(case)


@pytest.mark.asyncio
async def test_unavailable_ranked_code_reader_releases_its_reservation(monkeypatch):
    case = _install_reader(monkeypatch, "page", {7: [8]})
    unavailable = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_shared_rows_for_scope", unavailable)

    assert await _read(case, [_code(7)]) is None

    unavailable.assert_awaited_once()
    assert unavailable.await_args.kwargs["limit"] == 9
    _assert_released(case)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("occurrence_limit", "capacity", "reason"), [(1, 11, "code_occurrences"), (10, 10, "forward_scan")]
)
async def test_ranked_admission_preserves_prior_reservations_when_capacity_is_unavailable(
    monkeypatch, occurrence_limit, capacity, reason
):
    case = _install_reader(monkeypatch, "page", {7: [8]})
    case.limits = replace(case.limits, maximum_code_occurrences=occurrence_limit)
    budget = case.limits.scan_budget
    budget.maximum_row_capacity = capacity

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded, match=reason):
        await _read(case, [_code(7)])

    case.reader.assert_not_awaited()
    assert budget.active_read_row_capacity == budget.active_result_row_capacity == 2
    assert budget.peak_read_row_capacity == budget.peak_result_row_capacity == 2
