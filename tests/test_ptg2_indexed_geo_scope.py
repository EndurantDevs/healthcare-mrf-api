# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Require complete protected membership before prefiltering geographic rates."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api import ptg_membership_reader as reader
from tests.test_ptg2_geo_rate_prefix import _production_tables, _rate_row
from tests.test_ptg2_reverse_geo_npi_lookup_postgres import _location_query


def _request(**args):
    return serving._GeoRateSelectionRequest(
        code_rows=[{"code_key": 7, "rate_count": 300}],
        args={"code_system": "CPT", "code": "99213", "zip5": "00001", "lat": 42.0, "long": -83.0} | args,
        network_names=["synthetic network"],
        target_count=3,
        descending=False,
        source_trace_set_hash="ab" * 16,
    )


def _ready(monkeypatch, *, covered=(7,), error=None, absent=False):
    identity_by_key = {"source_key": "source", "snapshot_id": "snapshot", "code_system": "CPT", "code": "99213"}
    resolve = AsyncMock(return_value=(object(), object(), identity_by_key), side_effect=error)
    locate = AsyncMock(
        return_value=None
        if absent
        else (
            SimpleNamespace(schema_name="synthetic_membership"),
            {"counts": {"code_coverage": [{"code_key": key} for key in covered]}},
            1,
            2,
            object(),
        )
    )
    monkeypatch.setattr(reader, "source_identity", resolve)
    monkeypatch.setattr(reader, "locate", locate)

    async def pairs(_session, _tables, npis, _request, _state, _ownership, code_keys):
        assert code_keys == (7,)
        return (
            {"first_count": len(npis), "priced_count": len(npis), "pairs": [[npi, 11] for npi in npis]},
            100,
            100,
            len(npis),
        )

    monkeypatch.setattr(reader, "_membership_pairs", pairs)
    return resolve, locate


@pytest.mark.asyncio
async def test_ready_aggregate_uses_code_prefilter_and_same_complete_rate_reader(monkeypatch):
    resolve, locate = _ready(monkeypatch)
    locations = AsyncMock(return_value=[{"npi": 1000000001, "_ptg_source_exhausted": True}])
    monkeypatch.setattr(serving, "_indexed_geo_location_rows", locations)
    fallback = AsyncMock(side_effect=AssertionError("ready index must not expand raw graph membership"))
    monkeypatch.setattr(serving, "_oversized_geo_local_provider_sets", fallback)
    rates = [_rate_row(10), _rate_row(10) | {"price_key": 12, "source_key": 1}]
    read_rates = AsyncMock(return_value=rates)
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", read_rates)
    tables = _production_tables()
    budget = serving._geo_rate_selection_budget(tables)
    request = _request()
    result = await serving._select_oversized_geo_rate_scope(object(), tables, request, budget)
    assert result == serving._GeoRateSelection(tuple(rates), True)
    assert read_rates.call_args.kwargs["provider_set_keys"] == (11,)
    assert read_rates.call_args.kwargs["code_rows"] is request.code_rows
    assert read_rates.call_args.kwargs["network_names"] is request.network_names
    assert read_rates.call_args.kwargs["source_trace_set_hash"] == request.source_trace_set_hash
    assert budget.reverse_provider_set_keys_by_npi == {1000000001: (11,)}
    resolve.assert_awaited_once()
    locate.assert_awaited_once()
    fallback.assert_not_awaited()


@pytest.mark.asyncio
async def test_attachment_and_geo_state_refresh_for_each_source_and_code(monkeypatch):
    resolve, locate = _ready(monkeypatch)
    monkeypatch.setattr(
        serving,
        "_indexed_geo_location_rows",
        AsyncMock(
            side_effect=[[{"npi": npi, "_ptg_source_exhausted": True}] for npi in (1000000001, 1000000002, 1000000003)]
        ),
    )
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", AsyncMock(return_value=[]))
    tables = _production_tables()
    scopes = (
        (tables, "99213"),
        (serving.replace(tables, source_key="another-source", snapshot_id="another-snapshot"), "99213"),
        (tables, "99214"),
    )
    for index, (source_tables, code) in enumerate(scopes, 1):
        budget = serving._geo_rate_selection_budget(source_tables)
        await serving._select_oversized_geo_rate_scope(object(), source_tables, _request(code=code), budget)
        assert resolve.call_args.args[1:5] == (source_tables.source_key, source_tables.snapshot_id, "CPT", code)
        assert budget.reverse_provider_set_keys_by_npi == {1000000000 + index: (11,)}
    assert resolve.await_count == locate.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing", "coverage", "stale", "wrong_source", "wrong_client", "unverified"])
async def test_attachment_guard_precedes_address_read_and_invalid_never_falls_back(monkeypatch, failure):
    error = None if failure in {"missing", "coverage"} else serving.PTG2ManifestArtifactError(failure)
    _ready(monkeypatch, covered=() if failure == "coverage" else (7,), error=error, absent=failure == "missing")
    address = AsyncMock()
    fallback = AsyncMock(return_value=())
    monkeypatch.setattr(serving, "_indexed_geo_location_rows", address)
    monkeypatch.setattr(serving, "_oversized_geo_local_provider_sets", fallback)
    tables = _production_tables()
    if failure == "missing":
        assert await serving._select_oversized_geo_rate_scope(
            object(), tables, _request(), serving._geo_rate_selection_budget(tables)
        ) == serving._GeoRateSelection((), True)
        fallback.assert_awaited_once()
    else:
        with pytest.raises(serving.PTG2ManifestArtifactError):
            await serving._select_oversized_geo_rate_scope(
                object(), tables, _request(), serving._geo_rate_selection_budget(tables)
            )
        fallback.assert_not_awaited()
    address.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "locations",
    [
        [{"npi": None, "_ptg_source_exhausted": False}],
        [{"npi": 1000000001, "_ptg_source_exhausted": False}],
        [{"npi": 1000000001, "_ptg_source_exhausted": True}, {"npi": 1000000002, "_ptg_source_exhausted": True}],
    ],
)
async def test_incomplete_raw_or_output_prefix_cannot_assert_exhaustion(monkeypatch, locations):
    _ready(monkeypatch)
    monkeypatch.setattr(serving, "_ptg2_manifest_location_match_limit", lambda: 2)
    monkeypatch.setattr(serving, "_indexed_geo_location_rows", AsyncMock(return_value=locations))
    rates = AsyncMock()
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", rates)
    tables = _production_tables()
    with pytest.raises(serving.PTG2LocationScopeError):
        await serving._select_oversized_geo_rate_scope(
            object(), tables, _request(), serving._geo_rate_selection_budget(tables)
        )
    rates.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("knn", [False, True])
async def test_code_predicate_precedes_every_raw_limit_without_replacing_address_filters(monkeypatch, knn):
    context = _location_query("mrf")
    if knn:
        context = serving.replace(
            context,
            knn_order_sql=serving._ptg2_geo_knn_meters_sql("addr.lat", "addr.long"),
            knn_prefilter_sql="addr.npi > 0",
        )
    monkeypatch.setattr(serving, "_membership_location_query", AsyncMock(return_value=context))
    execute = AsyncMock(return_value=[])
    finalize = AsyncMock(return_value=[])
    monkeypatch.setattr(serving, "_execute_membership_location_sql", execute)
    monkeypatch.setattr(serving, "_finalize_location_rows", finalize)
    await serving._indexed_geo_location_rows(
        object(), object(), _request().args, SimpleNamespace(schema_name="synthetic_membership"), (7, 8), limit=10
    )
    updated, sql = execute.call_args.args[1:]
    assert context.filter_sql in updated.filter_sql
    assert context.address_assurance_sql == updated.address_assurance_sql
    assert "first.owner_key=npi_scope.npi_key" in sql and "OFFSET 0)" in sql
    assert updated.parameter_map["membership_geo_code_keys"] == [7, 8]
    if knn:
        for part in sql.split("LIMIT :raw_probe_limit")[:2]:
            assert "membership_geo_code_keys" in part
        assert (
            "membership_geo_code_keys"
            in sql.split("exact_zip_candidates AS MATERIALIZED")[1].split("LIMIT GREATEST")[0]
        )
    assert "membership_geo_code_keys" not in context.parameter_map
