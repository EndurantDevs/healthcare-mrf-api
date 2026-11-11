# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise ranked-page identity, readiness and authenticated cursor refusals."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import orjson
import pytest
from sanic.exceptions import InvalidUsage, ServiceUnavailable

from api import plan_pricing_distance_page as distance_page
from api import ptg2_serving as serving
from api.billing_search_cursor import (
    BillingSearchCursorError,
    BillingSearchCursorGenerationExpired,
    BillingSearchCursorKeyring,
    BillingSearchCursorState,
    seal_billing_search_cursor,
)
from api.billing_search_cursor_keys import BillingSearchCursorKeyringError
from api.endpoint.pagination import PaginationParams
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_plan_pricing_distance_page import _batch, _fetch_distance_page, _install_distance_endpoint, _rate
from tests.test_plan_release_serving import _network_binding, _release_selection


@pytest.mark.parametrize("npi", [None, True, 0, -1, "invalid", "١٢٣"])
def test_ranked_page_rejects_invalid_provider_identity(npi):
    with pytest.raises(PTG2ManifestArtifactError, match="provider identity"):
        distance_page.merge_ranked_distance_page([_batch("a", _rate("a", npi, 1.0, [90]))], 1)


@pytest.mark.parametrize("distance", [True, "1.0", -1.0, float("inf"), float("nan")])
def test_ranked_page_rejects_invalid_provider_distance(distance):
    with pytest.raises(PTG2ManifestArtifactError, match="invalid distance"):
        distance_page.merge_ranked_distance_page([_batch("a", _rate("a", 123, distance, [90]))], 1)


@pytest.mark.parametrize("limit", [0, -1])
def test_ranked_page_requires_positive_limit(limit):
    with pytest.raises(ValueError, match="must be positive"):
        distance_page.merge_ranked_distance_page([_batch("a")], limit)


def test_ranked_page_refuses_an_empty_unexhausted_snapshot():
    with pytest.raises(PTG2ManifestArtifactError, match="no proven distance frontier"):
        distance_page.merge_ranked_distance_page([_batch("a", exhausted=False)], 1)


@pytest.mark.parametrize(
    "sort_key",
    [
        ("distance-v1", "null", 123, 1, 1),
        ("foreign", "null", 123, 1, 1, "e" * 64),
        ("distance-v1", 1, 123, 1, 1, "e" * 64),
        ("distance-v1", "null", "123", 1, 1, "e" * 64),
        ("distance-v1", "null", 123, "1", 1, "e" * 64),
        ("distance-v1", "null", 123, 1, "1", "e" * 64),
        ("distance-v1", "null", 0, 1, 1, "e" * 64),
        ("distance-v1", "null", 123, 0, 1, "e" * 64),
        ("distance-v1", "null", 123, 2, 1, "e" * 64),
        ("distance-v1", "null", 123, 1, 1, 123),
        ("distance-v1", "null", 123, 1, 1, "e" * 63),
        ("distance-v1", "null", 123, 1, 1, "E" * 64),
        ("distance-v1", "invalid", 123, 1, 1, "e" * 64),
        ("distance-v1", "0x1p+999999999999999999", 123, 1, 1, "e" * 64),
        ("distance-v1", "inf", 123, 1, 1, "e" * 64),
        ("distance-v1", "-0x1.0000000000000p+0", 123, 1, 1, "e" * 64),
        ("distance-v1", "1", 123, 1, 1, "e" * 64),
    ],
)
def test_authenticated_cursor_rejects_invalid_distance_position(sort_key):
    keyring = BillingSearchCursorKeyring(active_key_id="test", keys_by_id={"test": bytes(range(32))})
    scope = ("a" * 64, "b" * 64, "c" * 64, "d" * 64)
    cursor = seal_billing_search_cursor(
        BillingSearchCursorState(
            request_fingerprint_sha256=scope[0],
            authorization_context_sha256=scope[1],
            generation_bundle_sha256=scope[2],
            snapshot_set_sha256=scope[3],
            sort_key=sort_key,
            issued_at=1000,
            expires_at=1100,
        ),
        keyring=keyring,
        trusted_now=1000,
    )
    with pytest.raises(BillingSearchCursorError, match="billing_search_cursor_invalid"):
        distance_page.open_distance_position(cursor, keyring=keyring, trusted_now=1001, scope=scope)


@pytest.fixture
def ranked_read(monkeypatch):
    binding = _network_binding(0, "snapshot-a", "a")
    selection = _release_selection(binding)
    keyring = BillingSearchCursorKeyring(active_key_id="test", keys_by_id={"test": bytes(range(32))})

    @asynccontextmanager
    async def inputs(*_args):
        yield {"generation": 1}

    monkeypatch.setattr(distance_page, "distance_input_snapshot", inputs)
    monkeypatch.setattr(distance_page, "_cursor_keyring", lambda: keyring)
    monkeypatch.setattr(distance_page, "current_network_address_scope", lambda: None)
    monkeypatch.setattr(serving, "_uses_local_distance_rate_scope", lambda *_args: True)
    read_snapshot = AsyncMock(return_value=(_batch("a"), {"query": {"code": "99213"}}))
    monkeypatch.setattr(serving, "_read_ranked_distance_snapshot", read_snapshot)
    enrich = AsyncMock(side_effect=lambda _session, response_by_field, _args: response_by_field)
    monkeypatch.setattr(distance_page, "_enrich_ptg2_code_details", enrich)
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unexpected database read")))
    query_by_field = {
        distance_page.DISTANCE_CONTINUATION_FIELD: None,
        "code": "99213",
        "code_system": "CPT",
        "lat": 40,
        "long": -80,
        "radius_miles": 10,
        "include_code_details": False,
    }

    async def search(*, cursor=None, bindings=None, **changed):
        return await distance_page.search_ranked_distance_release(
            session,
            selection,
            (binding,) if bindings is None else bindings,
            {binding.snapshot_id: None},
            {**query_by_field, distance_page.DISTANCE_CONTINUATION_FIELD: cursor, **changed},
            PaginationParams(page=1, limit=1, offset=0, source="offset"),
        )

    return SimpleNamespace(search=search, binding=binding, read_snapshot=read_snapshot, enrich=enrich, session=session)


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor", [None, "existing-distance-cursor"])
async def test_unsupported_ranked_snapshot_keeps_first_page_fallback_only(monkeypatch, ranked_read, cursor):
    monkeypatch.setattr(serving, "_uses_local_distance_rate_scope", lambda *_args: False)
    keyring = Mock(side_effect=AssertionError("unexpected keyring access"))
    monkeypatch.setattr(distance_page, "_cursor_keyring", keyring)
    if cursor is None:
        assert await ranked_read.search() is None
    else:
        with pytest.raises(BillingSearchCursorGenerationExpired):
            await ranked_read.search(cursor=cursor)
    keyring.assert_not_called()
    ranked_read.read_snapshot.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("cursor", [None, "existing-distance-cursor"])
async def test_missing_keyring_keeps_first_page_fallback_only(monkeypatch, ranked_read, cursor):
    monkeypatch.setattr(distance_page, "_cursor_keyring", Mock(side_effect=BillingSearchCursorKeyringError("missing")))
    if cursor is None:
        assert await ranked_read.search() is None
    else:
        with pytest.raises(BillingSearchCursorKeyringError):
            await ranked_read.search(cursor=cursor)
    ranked_read.read_snapshot.assert_not_awaited()


@pytest.mark.asyncio
async def test_duplicate_snapshot_bindings_refuse_before_hydration(ranked_read):
    with pytest.raises(PlanPricingProjectionUnavailable, match="distinct source snapshots"):
        await ranked_read.search(bindings=(ranked_read.binding, ranked_read.binding))
    ranked_read.read_snapshot.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_complete_snapshot_has_exact_zero_and_skips_enrichment(ranked_read):
    response_by_field = await ranked_read.search()
    assert response_by_field["items"] == []
    assert response_by_field["pagination"] == {
        "limit": 1,
        "offset": 0,
        "page": 1,
        "total": 0,
        "total_lower_bound": 0,
        "total_is_exact": True,
        "has_more": False,
        "next_cursor": None,
    }
    ranked_read.read_snapshot.assert_awaited_once()
    ranked_read.enrich.assert_not_awaited()
    ranked_read.session.execute.assert_not_awaited()


def test_network_scope_requires_authenticated_access_digest(monkeypatch):
    monkeypatch.setattr(
        distance_page, "current_network_address_scope", lambda: SimpleNamespace(access_scope_sha256=None)
    )
    binding = _network_binding(0, "snapshot-a", "a")
    with pytest.raises(PlanPricingProjectionUnavailable, match="authenticated network scope"):
        distance_page._distance_scope(_release_selection(binding), (binding,), {}, 1, {"generation": 1})


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
async def test_http_missing_keyring_refuses_continuation_without_rank_reads(monkeypatch, managed):
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch)
    first = await _fetch_distance_page(selection, managed)
    cursor = json.loads(first.body)["pagination"]["next_cursor"]
    reads_before = len(reads)
    monkeypatch.setattr(distance_page, "_cursor_keyring", Mock(side_effect=BillingSearchCursorKeyringError("missing")))
    with pytest.raises(ServiceUnavailable) as refused:
        await _fetch_distance_page(selection, managed, cursor)
    assert refused.value.status_code == 503
    assert len(reads) == reads_before


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
async def test_http_cursor_requires_canonical_release(monkeypatch, managed):
    selection, _, _, _, reads = _install_distance_endpoint(monkeypatch)
    with pytest.raises(InvalidUsage, match="requires a canonical plan release"):
        await _fetch_distance_page(selection, managed, "existing-distance-cursor", plan_release_id="")
    assert reads == []


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
async def test_http_continuation_refuses_retired_release_before_rank_reads(monkeypatch, managed):
    selection, resolve, _, _, reads = _install_distance_endpoint(monkeypatch)
    first = await _fetch_distance_page(selection, managed)
    cursor = json.loads(first.body)["pagination"]["next_cursor"]
    reads_before = len(reads)
    resolve.return_value = None
    refused = await _fetch_distance_page(selection, managed, cursor)
    assert refused.status == 409
    assert len(reads) == reads_before


@pytest.mark.asyncio
async def test_unresolved_internal_scope_cannot_reach_hydration(ranked_read):
    with pytest.raises(BillingSearchCursorError, match="billing_search_cursor_invalid"):
        await ranked_read.search(distance_unresolved_specialty="unknown-specialty")
    ranked_read.read_snapshot.assert_not_awaited()
    ranked_read.session.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "changed",
    [
        {"order": "desc"},
        {"plan_release_id": None},
        {"lat": None},
        {"code": None},
        {"q": "provider"},
        {"npi": 123},
        {"include_unverified_addresses": False},
        {"pos": "11"},
        {"service_code": "11"},
        {"modifier": "25"},
        {"billing_code_modifier": "25"},
        {"rate": 100},
        {"negotiated_rate": 100},
    ],
)
def test_distance_eligibility_preserves_required_query_shape(changed):
    query_by_field = {
        "order": "asc",
        "plan_release_id": "synthetic-release",
        "lat": 40,
        "zip5": None,
        "zip_radius_miles": None,
        "code": "99213",
        "q": None,
        "npi": None,
        "pos": None,
        "service_code": None,
        "modifier": None,
        "billing_code_modifier": None,
        "rate": None,
        "negotiated_rate": None,
    }
    pagination = PaginationParams(page=1, limit=25, offset=0, source="offset")
    assert distance_page.is_distance_continuation_query(
        query_by_field, pagination, include_allowed_amounts=False, include_providers=True
    )
    assert not distance_page.is_distance_continuation_query(
        query_by_field | changed, pagination, include_allowed_amounts=False, include_providers=True
    )


@pytest.mark.parametrize(
    ("offset", "include_allowed", "include_providers"),
    [(1, False, True), (0, True, True), (0, False, False)],
)
def test_distance_eligibility_preserves_paging_and_response_contract(offset, include_allowed, include_providers):
    query_by_field = {
        "order": "asc",
        "plan_release_id": "synthetic-release",
        "lat": None,
        "zip5": "00000",
        "zip_radius_miles": 10,
        "code": "99213",
        "q": None,
        "npi": None,
        "pos": None,
        "service_code": None,
        "modifier": None,
        "billing_code_modifier": None,
        "rate": None,
        "negotiated_rate": None,
    }
    assert not distance_page.is_distance_continuation_query(
        query_by_field,
        PaginationParams(page=1, limit=25, offset=offset, source="offset"),
        include_allowed_amounts=include_allowed,
        include_providers=include_providers,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("split_snapshots", [False, True])
@pytest.mark.parametrize("include_unverified", [None, "true"])
async def test_numeric_distance_pages_keep_hidden_address_rank(monkeypatch, split_snapshots, include_unverified):
    from tests.ptg2_serving_coverage_paydown_support import FakeResult, FakeSession
    from tests.test_ptg2_manifest_search_transitions import (
        _CODE_ROW,
        _NPI,
        _PROVIDER_ROW,
        _PROVIDER_SET_ID,
        _install_ranked_geo_rate_batch,
    )

    query_by_field, tables, _, _, location_read, _ = _install_ranked_geo_rate_batch(monkeypatch, False)
    query_by_field["include_unverified_addresses"] = include_unverified
    providers = [
        {**_PROVIDER_ROW, "address_payload": {}, "distance_miles": 0.1},
        {
            **_PROVIDER_ROW,
            "npi": _NPI + 1,
            "address_payload": {"first_line": "1 Synthetic Street", "city": "Example City", "state": "IL"},
            "distance_miles": 2.0,
        },
    ]

    async def snapshot(_session, snapshot_id, args, pagination, **_tables):
        selected = providers[int(snapshot_id) : int(snapshot_id) + 1] if split_snapshots else providers
        location_read.return_value = ({_PROVIDER_SET_ID}, {_PROVIDER_SET_ID: selected})
        return await serving._search_manifest_serving_table(
            FakeSession([FakeResult([{**_CODE_ROW, "rate_count": 2}])]),
            snapshot_id,
            args,
            pagination,
            tables,
            serving.PTG2_MODE_PRODUCT_SEARCH,
        )

    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", snapshot)
    monkeypatch.setattr(serving, "_network_tables_by_snapshot_id", AsyncMock(return_value={"0": tables, "1": tables}))
    observed_providers = []
    for offset in (0, 1):
        pagination = PaginationParams(page=offset + 1, limit=1, offset=offset, source="offset")
        if split_snapshots:
            response = await serving._search_multi_ptg2_snapshots(
                None, [("first", "0"), ("second", "1")], query_by_field, pagination
            )
        else:
            response = await snapshot(None, "0", query_by_field, pagination)
        observed_providers.extend(response["items"])
    assert [provider_item["npi"] for provider_item in observed_providers] == [_NPI, _NPI + 1]
    assert [provider_item["rate_option_count"] for provider_item in observed_providers] == [2, 2]
    assert "distance_miles" not in observed_providers[0] and "address" not in observed_providers[0]
    assert observed_providers[0]["address_verification"]["displayed_address_present"] is False
    assert observed_providers[1]["distance_miles"] == 2.0


@pytest.mark.asyncio
@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("include_unverified", [None, "true"])
async def test_hidden_address_cursor_and_numeric_pages_agree(monkeypatch, managed, include_unverified):
    selection, _, _, groups_by_source, _ = _install_distance_endpoint(monkeypatch)
    near = _rate("a", 1_000_000_000, 0.1, [90, 95]) | {
        "plan_id": "synthetic-plan",
        "address_verification": {"displayed_address_present": False, "network_bound_address": False},
    }
    far = _rate("b", 1_000_000_001, 2.0, [100, 105]) | {
        "plan_id": "synthetic-plan",
        "address": {"first_line": "1 Synthetic Street"},
        "address_verification": {"displayed_address_present": True, "network_bound_address": True},
    }
    groups_by_source.update(a=[near], b=[far], c=[], d=[])
    ranked_read = serving._read_ranked_distance_snapshot

    async def fresh_ranked_rows(*args):
        batch, metadata = await ranked_read(*args)
        return distance_page.RankedSnapshotBatch(
            batch.source_key, batch.snapshot_id, deepcopy(batch.items), batch.exhausted
        ), metadata

    async def snapshot(_session, snapshot_id, args, _pagination, **_tables):
        rows = deepcopy(groups_by_source[snapshot_id.removeprefix("snapshot-")])
        if not args.get("_ptg2_defer_response_shaping"):
            for row in rows:
                serving._apply_address_display_policy(row, args)
        return {"items": rows, "pagination": {"total": len(rows), "total_is_exact": True}, "query": {}}

    monkeypatch.setattr(serving, "_read_ranked_distance_snapshot", fresh_ranked_rows)
    monkeypatch.setattr(serving, "_search_one_ptg2_snapshot", snapshot)
    pages = []
    for offset in (0, 1):
        response = await _fetch_distance_page(
            selection, managed, limit="1", offset=str(offset), include_unverified_addresses=include_unverified
        )
        assert response.status == 200
        pages.append(json.loads(response.body))
    assert [page["items"][0]["npi"] for page in pages] == [near["npi"], far["npi"]]
    assert "address" not in pages[0]["items"][0] and "distance_miles" not in pages[0]["items"][0]
    continuation = await _fetch_distance_page(
        selection,
        managed,
        pages[0]["pagination"]["next_cursor"],
        limit="1",
        include_unverified_addresses=include_unverified,
    )
    assert continuation.status == 200
    assert json.loads(continuation.body)["items"] == pages[1]["items"]


def _install_numeric_distance_ties(monkeypatch, visibility, tie_field, include_unverified):
    """Keep real source shaping and capture each internal page before union."""
    from tests.ptg2_serving_coverage_paydown_support import FakeResult, FakeSession
    from tests.test_ptg2_manifest_search_transitions import _CODE_ROW, _install_ranked_geo_rate_batch

    args, tables, _, _, _, _ = _install_ranked_geo_rate_batch(monkeypatch, False)
    args.update(
        include_sources=visibility == "sources",
        include_debug=visibility == "debug",
        include_unverified_addresses=include_unverified,
    )
    original_rates = deepcopy(serving._shared_rows_for_scope.return_value)
    artifact_keys = {"0": 9, "1": 1} if tie_field == "artifact" else {"0": 1, "1": 9}
    source_by_snapshot = {"0": "a", "1": "z"}
    provenance_by_key = {key: {"source_key": key} for key in artifact_keys.values()}
    monkeypatch.setattr(serving, "_ptg2_source_provenance_for_rows", AsyncMock(return_value=provenance_by_key))
    internal_pages = []

    async def table(_session, snapshot_id, query, pagination, *, serving_tables):
        rates = [
            {
                **rate_row,
                "source_key": artifact_keys[snapshot_id],
                "billing_code_type_version": ("2" if snapshot_id == "0" else "1") if tie_field == "version" else "1",
            }
            for rate_row in original_rates
        ]
        serving._shared_rows_for_scope.return_value = rates
        serving._merge_manifest_code_variant_rows.return_value = rates
        if tie_field == "location":
            location_reader = serving._ptg2_manifest_location_provider_matches
            provider_set, providers = deepcopy(location_reader.return_value)
            for providers_in_set in providers.values():
                for provider_item in providers_in_set:
                    provider_item["location_hash"] = "z-location" if snapshot_id == "0" else "a-location"
            location_reader.return_value = provider_set, providers
        return await serving._search_manifest_serving_table(
            FakeSession([FakeResult([{**_CODE_ROW, "rate_count": 2}])]),
            snapshot_id,
            {**query, "source_key": source_by_snapshot[snapshot_id]},
            pagination,
            serving_tables,
            serving.PTG2_MODE_PRODUCT_SEARCH,
        )

    async def enrich(_session, payload, _args):
        return payload

    combine = serving._combine_ptg2_network_results

    def observe_internal_pages(responses):
        internal_pages.extend(deepcopy(responses))
        return combine(responses)

    monkeypatch.setattr(serving, "search_ptg2_serving_table", table)
    monkeypatch.setattr(serving, "_enrich_ptg2_code_details", enrich)
    monkeypatch.setattr(serving, "_combine_ptg2_network_results", observe_internal_pages)
    monkeypatch.setattr(serving, "_network_tables_by_snapshot_id", AsyncMock(return_value={"0": tables, "1": tables}))
    return args, [("a", "0"), ("z", "1")], artifact_keys, internal_pages


@pytest.mark.asyncio
@pytest.mark.parametrize("visibility", ["hidden", "sources", "debug"])
@pytest.mark.parametrize(
    ("tie_field", "include_unverified"),
    [("artifact", None), ("artifact", "false"), ("version", None), ("location", None)],
)
async def test_numeric_distance_union_keeps_private_ties_until_final_slice(
    monkeypatch, visibility, tie_field, include_unverified
):
    args, snapshots, artifact_keys, internal_pages = _install_numeric_distance_ties(
        monkeypatch, visibility, tie_field, include_unverified
    )
    pages = []
    for offset in (0, 1):
        response = await serving._search_multi_ptg2_snapshots(
            None, snapshots, args, PaginationParams(page=offset + 1, limit=1, offset=offset, source="offset")
        )
        pages.extend(response["items"])
        assert response["query"]["source_key"] is None
        assert response["query"]["snapshot_id"] is None
    assert [provider_item["network"] for provider_item in pages] == ["z", "a"]
    assert all(provider_item["rate_option_count"] == 2 and len(provider_item["prices"]) == 2 for provider_item in pages)
    assert all(("source_artifact_key" in provider_item) is (visibility != "hidden") for provider_item in pages)
    assert all(not any(key.startswith("_ptg") for key in provider_item) for provider_item in pages)
    complete = await serving._search_multi_ptg2_snapshots(
        None, snapshots, args, PaginationParams(page=1, limit=2, offset=0, source="offset")
    )
    assert orjson.dumps(pages, option=orjson.OPT_SORT_KEYS) == orjson.dumps(
        complete["items"], option=orjson.OPT_SORT_KEYS
    )
    for _, snapshot_id, response in internal_pages:
        for provider_item in response["items"]:
            assert provider_item["source_artifact_key"] == artifact_keys[snapshot_id]
            assert provider_item["billing_code_type_version"]
            assert "_ptg_price_key" in provider_item
            assert "_ptg_provider_rank" not in provider_item
