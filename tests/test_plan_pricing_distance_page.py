# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cover cross-snapshot distance ties and release-bound continuation."""

import json
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage

from api import plan_pricing_distance_page as distance_page
from api import ptg2_serving as serving
from api.billing_search_cursor import (
    BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS,
    BillingSearchCursorError,
    BillingSearchCursorGenerationExpired,
    BillingSearchCursorKeyring,
)
from api.billing_search_transport_contract import BILLING_SEARCH_TRANSPORT_PATH
from api.endpoint.pagination import PaginationParams
from api.plan_pricing_distance_page import (
    DistancePagePosition,
    RankedSnapshotBatch,
    merge_ranked_distance_page,
    open_distance_position,
    seal_distance_position,
)
from api.provider_specialty_filters import ProviderSpecialtyFilter
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_plan_pricing_state_scan_policy import _scope
from tests.test_plan_release_serving import _network_binding, _release_selection
from tests.test_pricing_api import make_request, pricing_module


def _rate(source, npi, distance, rates, arrangement="ffs"):
    return {
        "source_key": source,
        "snapshot_id": f"snapshot-{source}",
        "npi": npi,
        "distance_miles": distance,
        "negotiation_arrangement": arrangement,
        "reported_code": "99213",
        "prices": [{"negotiated_rate": rate} for rate in rates],
    }


def _batch(source, *items, exhausted=True):
    return RankedSnapshotBatch(source, f"snapshot-{source}", list(items), exhausted)


def test_split_tie_resumes_all_rate_groups_from_all_snapshots():
    nearest = _rate("a", 1, 0.5, [150])
    tied_a = _rate("a", 2, 1.0, [90, 96])
    tied_b = _rate("b", 2, 1.0, [84, 94])
    tied_b_bundle = _rate("b", 2, 1.0, [91], "bundle")
    farther = _rate("a", 3, 3.0, [100])
    null_tail = _rate("b", 4, None, [101])

    first = merge_ranked_distance_page(
        [_batch("b", tied_b_bundle, tied_b), _batch("a", nearest, tied_a, farther, exhausted=False)], 2
    )
    assert first.items == [nearest | {"network": "a"}, tied_b | {"network": "b"}]
    assert (first.offset, first.total_lower_bound, first.total_is_exact) == (0, 5, False)
    assert first.next_position is not None
    assert (first.next_position.rank, first.next_position.group_offset) == ((1.0, 2), 1)

    second = merge_ranked_distance_page(
        [_batch("a", tied_a, farther), _batch("b", tied_b, tied_b_bundle, null_tail)], 2, first.next_position
    )
    assert second.items == [tied_a | {"network": "a"}, tied_b_bundle | {"network": "b"}]
    assert (second.offset, second.total_lower_bound, second.total_is_exact) == (2, 6, True)
    assert second.next_position is not None
    assert second.next_position.group_offset == 3

    last = merge_ranked_distance_page(
        [_batch("b", tied_b_bundle, tied_b, null_tail), _batch("a", tied_a, farther)], 2, second.next_position
    )
    assert last.items == [farther | {"network": "a"}, null_tail | {"network": "b"}]
    assert (last.offset, last.total_lower_bound, last.total_is_exact, last.next_position) == (4, 6, True, None)


def test_merge_refuses_unproven_order_and_foreign_snapshot_rows():
    first = _rate("a", 1, 0.5, [90])
    farther = _rate("b", 3, 3.0, [85])
    with pytest.raises(PTG2ManifestArtifactError, match="prove the next complete page"):
        merge_ranked_distance_page([_batch("a", first, exhausted=False), _batch("b", farther)], 1)
    with pytest.raises(PTG2ManifestArtifactError, match="escaped its snapshot binding"):
        merge_ranked_distance_page([_batch("a", farther)], 1)
    with pytest.raises(PTG2ManifestArtifactError, match="repeated a rate group"):
        merge_ranked_distance_page([_batch("a", first, first)], 1)


def test_resume_refuses_a_missing_or_changed_tie_member():
    tied_a = _rate("a", 2, 1.0, [90])
    tied_b = _rate("b", 2, 1.0, [95])
    first = merge_ranked_distance_page([_batch("a", tied_a), _batch("b", tied_b)], 1)
    assert first.next_position is not None
    for other in (_batch("b"), _batch("b", _rate("b", 2, 1.0, [96]))):
        with pytest.raises(BillingSearchCursorGenerationExpired):
            merge_ranked_distance_page([_batch("a", tied_a), other], 1, first.next_position)


@pytest.mark.parametrize("distance", [1.0 / 3, None])
def test_distance_cursor_requires_current_scope_and_expires(distance):
    keyring = BillingSearchCursorKeyring(active_key_id="test", keys_by_id={"test": bytes(range(32))})
    scope = ("a" * 64, "b" * 64, "c" * 64, "d" * 64)
    position = DistancePagePosition(distance, 123, 2, 50, "e" * 64)
    cursor = seal_distance_position(position, keyring=keyring, trusted_now=1000, scope=scope)
    assert open_distance_position(cursor, keyring=keyring, trusted_now=1001, scope=scope) == position
    for index in range(len(scope)):
        changed = (*scope[:index], "f" * 64, *scope[index + 1 :])
        with pytest.raises(BillingSearchCursorError):
            open_distance_position(cursor, keyring=keyring, trusted_now=1001, scope=changed)
    with pytest.raises(BillingSearchCursorError):
        open_distance_position(
            cursor, keyring=keyring, trusted_now=1001 + BILLING_SEARCH_CURSOR_MAX_TTL_SECONDS, scope=scope
        )
    with pytest.raises(BillingSearchCursorError):
        malformed = seal_distance_position(
            replace(position, group_offset=51), keyring=keyring, trusted_now=1000, scope=scope
        )
        open_distance_position(malformed, keyring=keyring, trusted_now=1001, scope=scope)


def _install_distance_endpoint(monkeypatch, provider_inputs=None):
    provider_inputs = {"generation": 1} if provider_inputs is None else provider_inputs

    @asynccontextmanager
    async def input_snapshot(*_):
        yield provider_inputs

    monkeypatch.setattr(distance_page, "distance_input_snapshot", input_snapshot)
    bindings = tuple(
        _network_binding(index, f"snapshot-{source_key}", source_key) for index, source_key in enumerate("abcd")
    )
    selection = _release_selection(*bindings)
    resolve = AsyncMock(return_value=selection)
    monkeypatch.setattr(pricing_module, "resolve_plan_release_serving", resolve)
    monkeypatch.setattr(pricing_module, "resolve_plan_release_guard_selection", AsyncMock(return_value=selection))
    monkeypatch.setattr(serving, "prepare_plan_release_bindings", AsyncMock(side_effect=lambda _, current, __: current))
    admission = AsyncMock(side_effect=lambda _, current: current.in_network_bindings)
    monkeypatch.setattr(serving, "admitted_plan_release_bindings", admission)
    monkeypatch.setattr(serving, "_uses_local_distance_rate_scope", lambda *_: True)
    keyring = BillingSearchCursorKeyring(active_key_id="test", keys_by_id={"test": bytes(range(32))})
    monkeypatch.setattr(distance_page, "_cursor_keyring", lambda: keyring)
    rows_by_source = {
        binding.source_key: [
            _rate(binding.source_key, 1_000_000_000 + index, index / 1000, [90 + ordinal, 95 + ordinal])
            for index in range(count)
        ]
        for ordinal, (binding, count) in enumerate(zip(bindings, (5100, 49, 37, 0), strict=True))
    }
    batch_reads = []

    async def read_snapshot(_session, binding, _tables, args, candidate_target, after):
        assert args["lat"] == 40 and args["long"] == -80 and args["radius_miles"] == 10
        assert args["primary_only"] == "false" and args["include_evidence"] == "true"
        source_groups = rows_by_source[binding.source_key]
        start = int(after[1]) - 1_000_000_000 if after is not None else 0
        batch_reads.append((binding.source_key, candidate_target, start))
        return _batch(
            binding.source_key,
            *source_groups[start : start + candidate_target],
            exhausted=start + candidate_target >= len(source_groups),
        ), {
            "query": {"code": "99213", "code_system": "CPT"},
            "provenance": {},
        }

    monkeypatch.setattr(serving, "_read_ranked_distance_snapshot", read_snapshot)
    return selection, resolve, admission, rows_by_source, batch_reads


async def _fetch_distance_page(selection, managed, cursor=None, **changed):
    query_by_field = {
        "plan_release_id": selection.plan_release_id,
        "code": "99213",
        "code_system": "CPT",
        "lat": "40",
        "long": "-80",
        "radius_miles": "10",
        "order_by": "distance",
        "order": "asc",
        "include_allowed_amounts": "false",
        "include_providers": "true",
        "primary_only": "false",
        "include_sources": "true",
        "include_evidence": "true",
        "limit": "25",
    }

    options = query_by_field | ({"cursor": cursor} if cursor is not None else {}) | changed
    request = make_request([], None if managed else options)
    request.method = "POST" if managed else "GET"
    request.path = "/api/v1/registry/serving/providers/collection" if managed else BILLING_SEARCH_TRANSPORT_PATH
    with _scope() if managed else nullcontext():
        return await pricing_module.list_providers_by_procedure(request, native_args=options if managed else None)


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
async def test_http_continuation_crosses_original_offset_failure_with_complete_rates(monkeypatch, managed):
    """Traverse every synthetic price, then reject a changed release generation."""
    selection, resolve, admission, rows_by_source, batch_reads = _install_distance_endpoint(monkeypatch)
    observed_groups = []
    cursor = None
    first_cursor = None
    has_crossed_old_boundary = False
    for _ in range(208):
        reply = await _fetch_distance_page(selection, managed, cursor)
        assert reply.status == 200
        response_by_field = json.loads(reply.body)
        page = response_by_field["pagination"]
        assert page["offset"] == len(observed_groups)
        assert response_by_field["plan_release_id"] == selection.plan_release_id
        observed_groups.extend(response_by_field["items"])
        cursor = page["next_cursor"]
        first_cursor = first_cursor or cursor
        if page["offset"] == 4975:
            has_crossed_old_boundary = True
            assert len(response_by_field["items"]) == 25
        if not page["has_more"]:
            assert cursor is None and page["total_is_exact"]
            assert page["total"] == len(observed_groups)
            break
        assert cursor
    oracle = sorted(
        (group for groups in rows_by_source.values() for group in groups),
        key=lambda group: (group["distance_miles"], group["npi"], group["prices"][0]["negotiated_rate"]),
    )
    assert has_crossed_old_boundary
    assert observed_groups == [group | {"network": group["source_key"]} for group in oracle]
    assert resolve.await_count == 208
    assert admission.await_count == 208
    assert len(batch_reads) == 208 * 4
    assert [probe_limit for _, probe_limit, _ in batch_reads[:4]] == [26] * 4
    assert all(probe_limit == 27 for _, probe_limit, _ in batch_reads[4:])
    reads_before = len(batch_reads)
    resolve.return_value = replace(selection, serving_revision_id="changed-serving-revision")
    expired = await _fetch_distance_page(selection, managed, first_cursor)
    assert expired.status == 409
    assert len(batch_reads) == reads_before


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
async def test_http_continuation_expires_before_rank_reads_when_provider_inputs_change(monkeypatch, managed):
    generation_by_field = {"npi": 1, "address": 2, "tiger": "first-leaf"}
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch, generation_by_field)
    first = await _fetch_distance_page(selection, managed)
    assert first.status == 200
    cursor = json.loads(first.body)["pagination"]["next_cursor"]
    reads_before = len(reads)
    generation_by_field["npi"] = 3
    expired = await _fetch_distance_page(selection, managed, cursor)
    assert expired.status == 409
    assert len(reads) == reads_before


@pytest.mark.asyncio
async def test_missing_input_capability_keeps_legacy_first_page_and_expires_existing_cursor(monkeypatch):
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch)

    @asynccontextmanager
    async def unavailable_inputs(*_):
        yield None

    monkeypatch.setattr(distance_page, "distance_input_snapshot", unavailable_inputs)
    bindings = selection.in_network_bindings
    tables_by_snapshot = {binding.snapshot_id: None for binding in bindings}
    pagination = PaginationParams(page=1, limit=25, offset=0, source="offset")
    for cursor in (None, "existing-distance-cursor"):
        request = distance_page.search_ranked_distance_release(
            None,
            selection,
            bindings,
            tables_by_snapshot,
            {distance_page.DISTANCE_CONTINUATION_FIELD: cursor},
            pagination,
        )
        if cursor is None:
            assert await request is None
        else:
            with pytest.raises(BillingSearchCursorGenerationExpired):
                await request
    assert not reads


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
async def test_http_continuation_expires_when_specialty_resolution_changes(monkeypatch, managed):
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch)
    resolver = AsyncMock(return_value=ProviderSpecialtyFilter(taxonomy_codes=("207Q00000X",), primary_only=False))
    monkeypatch.setattr(pricing_module, "resolve_ptg_provider_specialty_filter", resolver)
    first = await _fetch_distance_page(selection, managed, specialty="synthetic-specialty")
    assert first.status == 200
    cursor = json.loads(first.body)["pagination"]["next_cursor"]
    reads_before = len(reads)
    resolver.return_value = replace(resolver.return_value, taxonomy_codes=("207R00000X",))
    expired = await _fetch_distance_page(selection, managed, cursor, specialty="synthetic-specialty")
    assert expired.status == 409
    assert len(reads) == reads_before


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
async def test_http_continuation_expires_when_specialty_resolution_disappears(monkeypatch, managed):
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch)
    resolver = AsyncMock(return_value=ProviderSpecialtyFilter(taxonomy_codes=("207Q00000X",), primary_only=False))
    monkeypatch.setattr(pricing_module, "resolve_ptg_provider_specialty_filter", resolver)
    first = await _fetch_distance_page(selection, managed, specialty="synthetic-specialty")
    cursor = json.loads(first.body)["pagination"]["next_cursor"]
    reads_before = len(reads)
    resolver.return_value = ProviderSpecialtyFilter(unresolved_specialty="synthetic-specialty", primary_only=False)
    expired = await _fetch_distance_page(selection, managed, cursor, specialty="synthetic-specialty")
    assert expired.status == 409
    assert len(reads) == reads_before


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("has_cursor", [False, True])
async def test_unknown_specialty_requires_an_unchanged_cursor_selector(monkeypatch, managed, has_cursor):
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch)
    resolver = AsyncMock(return_value=ProviderSpecialtyFilter(taxonomy_codes=("207Q00000X",), primary_only=False))
    monkeypatch.setattr(pricing_module, "resolve_ptg_provider_specialty_filter", resolver)
    cursor = None
    if has_cursor:
        first = await _fetch_distance_page(selection, managed, specialty="synthetic-specialty")
        cursor = json.loads(first.body)["pagination"]["next_cursor"]
    reads_before = len(reads)
    resolver.return_value = ProviderSpecialtyFilter(unresolved_specialty="unknown-specialty", primary_only=False)
    expected_error = "billing_search_cursor_invalid" if has_cursor else "does not match any known specialty"
    with pytest.raises(InvalidUsage, match=expected_error) as rejected:
        await _fetch_distance_page(selection, managed, cursor, specialty="unknown-specialty")
    assert rejected.value.status_code == 400
    assert len(reads) == reads_before


@pytest.mark.asyncio
async def test_unresolved_specialty_cursor_cannot_change_authorization(monkeypatch):
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch)
    resolver = AsyncMock(return_value=ProviderSpecialtyFilter(taxonomy_codes=("207Q00000X",), primary_only=False))
    monkeypatch.setattr(pricing_module, "resolve_ptg_provider_specialty_filter", resolver)
    first = await _fetch_distance_page(selection, False, specialty="synthetic-specialty")
    cursor = json.loads(first.body)["pagination"]["next_cursor"]
    reads_before = len(reads)
    resolver.return_value = ProviderSpecialtyFilter(unresolved_specialty="synthetic-specialty", primary_only=False)
    with pytest.raises(InvalidUsage, match="billing_search_cursor_invalid") as rejected:
        await _fetch_distance_page(selection, True, cursor, specialty="synthetic-specialty")
    assert rejected.value.status_code == 400
    assert len(reads) == reads_before


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize(
    ("field", "original", "replacement"),
    [
        ("specialty", "synthetic-specialty", "other-specialty"),
        ("taxonomy_codes", "207Q00000X", "207R00000X"),
        ("taxonomy_code", "207Q00000X", "207R00000X"),
    ],
)
async def test_http_continuation_rejects_changed_taxonomy_selector(monkeypatch, managed, field, original, replacement):
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch)
    resolver = AsyncMock(return_value=ProviderSpecialtyFilter(taxonomy_codes=("207Q00000X",), primary_only=False))
    monkeypatch.setattr(pricing_module, "resolve_ptg_provider_specialty_filter", resolver)
    first = await _fetch_distance_page(selection, managed, **{field: original})
    assert first.status == 200
    cursor = json.loads(first.body)["pagination"]["next_cursor"]
    reads_before = len(reads)
    with pytest.raises(InvalidUsage, match="billing_search_cursor_invalid") as rejected:
        await _fetch_distance_page(selection, managed, cursor, **{field: replacement})
    assert rejected.value.status_code == 400
    assert len(reads) == reads_before


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("field", ["taxonomy_codes", "taxonomy_code"])
async def test_explicit_taxonomy_keeps_precedence_when_specialty_resolution_changes(monkeypatch, managed, field):
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch)
    resolver = AsyncMock(return_value=ProviderSpecialtyFilter(taxonomy_codes=("207Q00000X",), primary_only=False))
    monkeypatch.setattr(pricing_module, "resolve_ptg_provider_specialty_filter", resolver)
    request_by_field = {"specialty": "synthetic-specialty", field: "207Q00000X"}
    first = await _fetch_distance_page(selection, managed, **request_by_field)
    assert first.status == 200
    cursor = json.loads(first.body)["pagination"]["next_cursor"]
    reads_before = len(reads)
    resolver.return_value = replace(resolver.return_value, taxonomy_codes=("207R00000X",))
    second = await _fetch_distance_page(selection, managed, cursor, **request_by_field)
    assert second.status == 200
    assert json.loads(second.body)["pagination"]["offset"] == 25
    assert len(reads) > reads_before


def _observe_distance_taxonomy_filters(monkeypatch):
    read_snapshot = serving._read_ranked_distance_snapshot
    taxonomy_parameter_maps = []

    async def read_with_taxonomy(session, binding, tables, args, candidate_target, after):
        parameter_map = {}
        serving._membership_taxonomy_filters(args, parameter_map)
        taxonomy_parameter_maps.append(parameter_map)
        return await read_snapshot(session, binding, tables, args, candidate_target, after)

    monkeypatch.setattr(serving, "_read_ranked_distance_snapshot", read_with_taxonomy)
    monkeypatch.setattr(
        "tests.test_pricing_api.FakeSession.execute", AsyncMock(side_effect=AssertionError("unexpected database read"))
    )
    return taxonomy_parameter_maps


def _install_taxonomy_distance_endpoint(monkeypatch):
    selection, _, _, _, _ = _install_distance_endpoint(monkeypatch)
    taxonomy_by_npi = {1_000_000_000: "207Q00000X", 1_000_000_001: "207R00000X", 1_000_000_002: "207Q00000X"}
    parameter_maps_by_lane = {"cursor": [], "numeric": []}

    def matching_groups(source_key, args, lane):
        parameter_map = {}
        serving._membership_taxonomy_filters(args, parameter_map)
        parameter_maps_by_lane[lane].append(parameter_map)
        taxonomy_codes = {
            value
            for key, value in parameter_map.items()
            if key.startswith("membership_location_specialty_taxonomy_code_")
        }
        return [
            _rate(source_key, npi, index / 10, [90 + index])
            for index, (npi, taxonomy_code) in enumerate(taxonomy_by_npi.items())
            if source_key == "a" and (not taxonomy_codes or taxonomy_code in taxonomy_codes)
        ]

    async def ranked_snapshot(_session, binding, _tables, args, candidate_target, after):
        groups = matching_groups(binding.source_key, args, "cursor")
        if after is not None:
            groups = [group for group in groups if (group["distance_miles"], group["npi"]) >= after]
        return _batch(binding.source_key, *groups[:candidate_target], exhausted=len(groups) <= candidate_target), {
            "query": {"code": "99213", "code_system": "CPT"},
            "provenance": {},
        }

    async def numeric_snapshot(_session, snapshot_id, args, pagination, **_tables):
        groups = matching_groups(snapshot_id.removeprefix("snapshot-"), args, "numeric")
        return {
            "items": groups[: pagination.limit],
            "pagination": {"total": len(groups), "total_is_exact": True},
            "query": {"code": "99213", "code_system": "CPT"},
        }

    monkeypatch.setattr(serving, "_read_ranked_distance_snapshot", ranked_snapshot)
    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", numeric_snapshot)
    monkeypatch.setattr(
        "tests.test_pricing_api.FakeSession.execute", AsyncMock(side_effect=AssertionError("unexpected database read"))
    )
    return selection, parameter_maps_by_lane


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("plural_codes", ["", " \t "])
async def test_numeric_and_cursor_distance_pages_use_the_same_explicit_taxonomy(monkeypatch, managed, plural_codes):
    selection, parameter_maps_by_lane = _install_taxonomy_distance_endpoint(monkeypatch)
    dynamic_entry = AsyncMock(return_value=(("207R00000X",), ("207R00000X",)))
    monkeypatch.setattr("api.provider_specialty_cache_entries.load_dynamic_specialty_entry", dynamic_entry)
    request_by_field = {
        "specialty": "synthetic-specialty",
        "taxonomy_codes": plural_codes,
        "taxonomy_code": " 207q00000x ",
        "limit": "1",
    }
    first = await _fetch_distance_page(selection, managed, **request_by_field)
    assert first.status == 200
    first_payload = json.loads(first.body)
    assert first_payload["items"][0]["npi"] == 1_000_000_000
    cursor = first_payload["pagination"]["next_cursor"]
    assert cursor

    dynamic_entry.return_value = None
    continued = await _fetch_distance_page(selection, managed, cursor, **request_by_field)
    numeric = await _fetch_distance_page(selection, managed, offset="1", **request_by_field)
    for response in (continued, numeric):
        assert response.status == 200
        page_by_field = json.loads(response.body)
        assert [rate_group["npi"] for rate_group in page_by_field["items"]] == [1_000_000_002]
        assert page_by_field["items"][0]["prices"] == [{"negotiated_rate": 92}]
        assert page_by_field["pagination"]["offset"] == 1
        assert page_by_field["pagination"]["has_more"] is False
    for parameter_maps in parameter_maps_by_lane.values():
        assert parameter_maps
        assert all(
            parameter_map["membership_location_specialty_taxonomy_code_0"] == "207Q00000X"
            for parameter_map in parameter_maps
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("plural_codes", [None, " \t "])
async def test_explicit_taxonomy_continues_when_dynamic_alias_disappears(monkeypatch, managed, plural_codes):
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch)
    taxonomy_parameter_maps = _observe_distance_taxonomy_filters(monkeypatch)
    dynamic_entry = AsyncMock(return_value=(("207R00000X",), ("207R00000X",)))
    monkeypatch.setattr("api.provider_specialty_cache_entries.load_dynamic_specialty_entry", dynamic_entry)
    request_by_field = {
        "specialty": "synthetic-specialty",
        "taxonomy_code": " 207q00000x ",
        "taxonomy_codes": plural_codes,
    }
    first = await _fetch_distance_page(selection, managed, **request_by_field)
    assert first.status == 200
    cursor = json.loads(first.body)["pagination"]["next_cursor"]
    reads_before = len(reads)
    dynamic_entry.return_value = None
    second = await _fetch_distance_page(selection, managed, cursor, **request_by_field)
    assert second.status == 200
    assert json.loads(second.body)["pagination"]["offset"] == 25
    assert len(reads) > reads_before
    dynamic_entry.assert_not_awaited()
    assert all(
        parameter_map["membership_location_specialty_taxonomy_code_0"] == "207Q00000X"
        for parameter_map in taxonomy_parameter_maps
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("replacement", [(("207R00000X",), ("207R00000X",)), None])
async def test_blank_taxonomy_codes_cannot_hide_dynamic_alias_changes(monkeypatch, managed, replacement):
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch)
    taxonomy_parameter_maps = _observe_distance_taxonomy_filters(monkeypatch)
    dynamic_entry = AsyncMock(return_value=(("207Q00000X",), ("207Q00000X",)))
    monkeypatch.setattr("api.provider_specialty_cache_entries.load_dynamic_specialty_entry", dynamic_entry)
    request_by_field = {"specialty": "synthetic-specialty", "taxonomy_codes": " \t "}
    first = await _fetch_distance_page(selection, managed, **request_by_field)
    assert first.status == 200
    cursor = json.loads(first.body)["pagination"]["next_cursor"]
    reads_before = len(reads)
    dynamic_entry.return_value = replacement
    expired = await _fetch_distance_page(selection, managed, cursor, **request_by_field)
    assert expired.status == 409
    assert len(reads) == reads_before
    assert dynamic_entry.await_count == 2
    assert all(
        parameter_map["membership_location_specialty_taxonomy_code_0"] == "207Q00000X"
        for parameter_map in taxonomy_parameter_maps
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("replacement", [{"latitude": 41, "longitude": -80}, None, {"latitude": 40}])
async def test_zip_continuation_expires_when_resolved_center_changes(monkeypatch, managed, replacement):
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch)
    lookup = AsyncMock(return_value={"latitude": 40, "longitude": -80})
    monkeypatch.setattr(pricing_module, "_lookup_zip_context", lookup)
    request_by_field = {"lat": None, "long": None, "radius_miles": None, "zip5": "12345", "zip_radius_miles": "10"}
    first = await _fetch_distance_page(selection, managed, **request_by_field)
    assert first.status == 200
    cursor = json.loads(first.body)["pagination"]["next_cursor"]
    reads_before = len(reads)
    lookup.return_value = replacement
    expired = await _fetch_distance_page(selection, managed, cursor, **request_by_field)
    assert expired.status == 409
    assert len(reads) == reads_before


@pytest.mark.asyncio
@pytest.mark.parametrize("include_unverified", [None, "true"])
@pytest.mark.parametrize("network_bound", [False, True])
async def test_displayed_addresses_keep_rank_and_split_tie_continuation(monkeypatch, include_unverified, network_bound):
    from copy import deepcopy

    selection, _, _, groups_by_source, _ = _install_distance_endpoint(monkeypatch)
    for groups in groups_by_source.values():
        for group in groups[:2]:
            group.update(
                plan_id="synthetic-plan",
                address={"first_line": "1 Synthetic Street"},
                address_verification={"displayed_address_present": True, "network_bound_address": network_bound},
            )
    read = serving._read_ranked_distance_snapshot

    async def fresh_rows(*args):
        return deepcopy(await read(*args))

    monkeypatch.setattr(serving, "_read_ranked_distance_snapshot", fresh_rows)
    request_by_field = {
        "limit": "1",
        "include_unverified_addresses": include_unverified,
        "include_sources": "false",
        "include_debug": "true",
    }
    first = await _fetch_distance_page(selection, False, **request_by_field)
    assert first.status == 200
    first_payload = json.loads(first.body)
    second = await _fetch_distance_page(
        selection, False, first_payload["pagination"]["next_cursor"], **request_by_field
    )
    assert second.status == 200
    second_payload = json.loads(second.body)
    for response_by_field in (first_payload, second_payload):
        provider_by_field = response_by_field["items"][0]
        assert provider_by_field["npi"] == 1_000_000_000
        assert provider_by_field["prices"]
        assert "distance_miles" in provider_by_field
        assert "address" in provider_by_field


def _install_mixed_address_distance_endpoint(monkeypatch):
    from copy import deepcopy

    selection, _, _, groups_by_source, ranked_reads = _install_distance_endpoint(monkeypatch)
    groups_by_source.update(
        a=[_rate("a", 1_000_000_000, 0.1, [90, 95])],
        b=[_rate("b", 1_000_000_001, 2.0, [100, 105])],
        c=[],
        d=[],
    )
    for source_key, groups in groups_by_source.items():
        for group in groups:
            group.update(
                plan_id="synthetic-plan",
                address={"first_line": "1 Synthetic Street"},
                address_verification={"displayed_address_present": True, "network_bound_address": source_key == "b"},
            )

    async def legacy_snapshot(_session, snapshot_id, args, pagination, **_tables):
        assert pagination.offset == 0
        provider_groups = deepcopy(groups_by_source[snapshot_id.removeprefix("snapshot-")])
        for provider_group in provider_groups:
            serving._apply_address_display_policy(provider_group, args)
        return {
            "items": provider_groups,
            "pagination": {"total": len(provider_groups), "total_is_exact": True},
            "query": {"code": "99213", "code_system": "CPT"},
        }

    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", legacy_snapshot)
    return selection, ranked_reads


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("include_unverified", [None, "true", "false", "0", "no", "off"])
async def test_numeric_distance_pages_keep_one_order_when_inferred_addresses_are_suppressed(
    monkeypatch, managed, include_unverified
):
    selection, ranked_reads = _install_mixed_address_distance_endpoint(monkeypatch)
    pages = []
    for offset in (0, 1):
        response = await _fetch_distance_page(
            selection, managed, limit="1", offset=str(offset), include_unverified_addresses=include_unverified
        )
        assert response.status == 200
        pages.append(json.loads(response.body))
        pagination = pages[-1]["pagination"]
        assert (pagination["limit"], pagination["offset"], pagination["page"]) == (1, offset, offset + 1)
        assert pagination["has_more"] is (offset == 0)
    is_address_suppressed = include_unverified in {"false", "0", "no", "off"}
    providers = [page["items"][0] for page in pages]
    expected_npis = [1_000_000_001, 1_000_000_000] if is_address_suppressed else [1_000_000_000, 1_000_000_001]
    assert [provider["npi"] for provider in providers] == expected_npis
    for provider in providers:
        is_inferred = provider["npi"] == 1_000_000_000
        assert provider["prices"] == [{"negotiated_rate": rate} for rate in ([90, 95] if is_inferred else [100, 105])]
        assert ("distance_miles" in provider) is (not is_inferred or not is_address_suppressed)
        assert ("address" in provider) is (not is_inferred or not is_address_suppressed)
    if is_address_suppressed:
        assert all("next_cursor" not in page["pagination"] for page in pages)
        assert ranked_reads == []
    else:
        assert pages[0]["pagination"]["next_cursor"]
        assert len(ranked_reads) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
async def test_distance_cursor_cannot_switch_to_suppressed_address_order(monkeypatch, managed):
    selection, ranked_reads = _install_mixed_address_distance_endpoint(monkeypatch)
    first = await _fetch_distance_page(selection, managed, limit="1")
    cursor = json.loads(first.body)["pagination"]["next_cursor"]
    assert cursor
    reads_before = len(ranked_reads)
    with pytest.raises(pricing_module.InvalidUsage, match="same ascending full geographic"):
        await _fetch_distance_page(selection, managed, cursor, limit="1", include_unverified_addresses="false")
    assert len(ranked_reads) == reads_before
