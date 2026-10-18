# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure contracts for exact attributed aggregate reads; native SQL is separate."""

import asyncio
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest
from sqlalchemy import Integer
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.exc import DBAPIError

from api import plan_pricing_projection_aggregate_read as aggregate_read
from tests.test_plan_pricing_projection_v3_read import _pack_row, _record, _Result


def _request():
    return SimpleNamespace(projection_id="a" * 64, code_system="CPT", code="27447")


def _row(zip5="10001", minimum="2", **changes):
    aggregate_by_field = {
        "profile_count": 2,
        "membership_count": 4,
        "rate_value_count": 3,
        "result_byte_count": 256,
        "profiles_valid": True,
        "geo_cell": zip5,
        "provider_count": 3,
        "rate_count": 4,
        "minimum_negotiated_rate": Decimal(minimum),
        "median_lower": Decimal("2"),
        "median_upper": Decimal("6"),
        "maximum_negotiated_rate": Decimal("6"),
    }
    aggregate_by_field.update(changes)
    return aggregate_by_field


def _page(limit=10, offset=0):
    return SimpleNamespace(limit=limit, offset=offset)


def _render(fragments):
    return orjson.loads(orjson.dumps(fragments))


def test_partial_sql_filters_before_probes_and_gates_arrays_before_expansion():
    query = aggregate_read._partial_aggregate_query((7, 1))
    sql = str(query)
    for window, predicate, probe in [
        ("profile_window", "binding_ordinal", "profile_probe_limit"),
        ("member_window", "member.binding_ordinal", "membership_probe_limit"),
    ]:
        start = sql.index(f"{window} AS MATERIALIZED")
        restriction = sql.index(f"AND {predicate} = ANY(CAST(:binding_ordinals AS integer[]))", start)
        assert start < restriction < sql.index("ORDER BY", start) < sql.index(f"LIMIT :{probe}", start)
    assert sql.index("rate_admission AS MATERIALIZED") < sql.index("profile_validation AS MATERIALIZED")
    assert "rate_admission.rate_value_count <= :rate_value_limit" in sql
    assert "provider.geo_cell = ANY(CAST(:geo_cells AS varchar[]))" in sql
    assert "SUM(octet_length(row_to_json(aggregates)::text))" in sql
    assert "LEFT JOIN aggregates ON result_admission.result_byte_count <= :result_byte_limit" in sql
    bound = query.compile().binds["binding_ordinals"]
    assert bound.value == [7, 1]
    assert isinstance(bound.type, ARRAY) and isinstance(bound.type.item_type, Integer)


def test_weighted_sql_counts_sets_once_per_zip_and_distinct_providers():
    sql = str(aggregate_read._partial_aggregate_query((1,)))
    assert "SELECT DISTINCT binding_ordinal, provider_set_key, geo_cell" in sql
    assert "COUNT(DISTINCT npi)" in sql
    frequency_sql = sql.split("rate_frequency AS MATERIALIZED", 1)[1].split("ranked AS MATERIALIZED", 1)[0]
    assert "FROM admitted_profiles profile" in frequency_sql
    assert "SUM(rate.multiplicity)" in frequency_sql and "member_window" not in frequency_sql
    assert "(ranked.total + 1) / 2" in sql and "(ranked.total + 2) / 2" in sql
    assert "generate_subscripts" in sql and "multiplicity IS NULL OR multiplicity <= 0" in sql
    assert "FALSE)), TRUE) AS profiles_valid" in sql
    assert "plan_pricing_aggregate_pack" not in sql


@pytest.mark.parametrize("ordinals", [[1], (True,), (-1,), (2**31,), (1, 1), tuple(range(5001))])
def test_partial_ordinal_array_is_closed_and_bounded(ordinals):
    with pytest.raises(aggregate_read.PlanPricingProjectionUnavailable):
        aggregate_read._partial_aggregate_query(ordinals)


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinals", [(), (1,)])
async def test_empty_scope_skips_pricing_and_retains_prefix_validation(ordinals):
    session = AsyncMock()
    assert await aggregate_read.read_aggregate_pack_page(
        session, _request(), [] if ordinals else ["10001"], {}, _page(), binding_ordinals=ordinals
    ) == ([], 0)
    with pytest.raises(aggregate_read.PlanPricingProjectionUnsupported):
        await aggregate_read.read_aggregate_pack_page(
            session, _request(), [f"{prefix:02d}000" for prefix in range(17)], {}, _page(), binding_ordinals=ordinals
        )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("order,offset,expected_zip", [("asc", 0, "10001"), ("desc", 0, "10002"), ("asc", 1, "10002")])
async def test_partial_uses_one_supplied_session_exact_scalar_format_and_existing_pagination(
    order, offset, expected_zip
):
    session = AsyncMock()
    session.execute.return_value = _Result(
        [_row("10002", "3", median_lower=Decimal("3"), median_upper=Decimal("5")), _row("10001")]
    )
    fragments, total = await aggregate_read.read_aggregate_pack_page(
        session, _request(), ["10002", "10001", "10001"], {"order": order}, _page(1, offset), binding_ordinals=(2, 4)
    )
    session.execute.assert_awaited_once()
    query, parameters = session.execute.await_args.args
    assert query.compile().params["binding_ordinals"] == [2, 4]
    assert parameters["geo_cells"] == ["10001", "10002"]
    assert parameters["profile_probe_limit"] == parameters["membership_probe_limit"] == 65_537
    assert parameters["rate_value_limit"] == 1_048_576
    assert parameters["result_probe_limit"] == 3
    rendered = _render(fragments)
    assert total == 2 and rendered[0]["geo_cell"] == expected_zip
    assert rendered[0]["median_negotiated_rate"] == 4
    assert rendered[0]["provider_count"] == 3 and rendered[0]["rate_count"] == 4


@pytest.mark.asyncio
async def test_tied_prices_use_zip_order_and_offset_empty_keeps_exact_total():
    session = AsyncMock()
    session.execute.return_value = _Result([_row("10002"), _row("10001")])
    fragments, total = await aggregate_read.read_aggregate_pack_page(
        session, _request(), ["10001", "10002"], {"order": "desc"}, _page(1), binding_ordinals=(1,)
    )
    assert _render(fragments)[0]["geo_cell"] == "10001" and total == 2
    fragments, total = await aggregate_read.read_aggregate_pack_page(
        session, _request(), ["10001", "10002"], {}, _page(1, 10), binding_ordinals=(1,)
    )
    assert fragments == [] and total == 2


@pytest.mark.parametrize(
    "field,maximum", [("profile_count", 65_536), ("membership_count", 65_536), ("rate_value_count", 1_048_576)]
)
def test_native_work_budget_sentinel_is_explicit_refusal(field, maximum):
    with pytest.raises(aggregate_read.PlanPricingProjectionUnsupported, match="native work bound"):
        aggregate_read._partial_aggregate_rows(_Result([_row(**{field: maximum + 1})]), frozenset({"10001"}))


@pytest.mark.parametrize(
    "changes",
    [
        {"profiles_valid": False},
        {"profile_count": True},
        {"membership_count": -1},
        {"median_lower": None},
        {"geo_cell": "99999"},
        {"rate_count": 0},
        {"provider_count": True},
        {"median_lower": 2.0},
        {"median_lower": Decimal("6"), "median_upper": Decimal("2")},
    ],
)
def test_partial_malformed_receipt_or_result_fails_closed(changes):
    with pytest.raises(aggregate_read.PlanPricingProjectionUnavailable):
        aggregate_read._partial_aggregate_rows(_Result([_row(**changes)]), frozenset({"10001"}))


def test_missing_inconsistent_duplicate_and_oversized_rows_fail_closed(monkeypatch):
    with pytest.raises(aggregate_read.PlanPricingProjectionUnavailable, match="missing"):
        aggregate_read._partial_aggregate_rows(_Result([]), frozenset({"10001"}))
    with pytest.raises(aggregate_read.PlanPricingProjectionUnavailable, match="inconsistent"):
        aggregate_read._partial_aggregate_rows(
            _Result([_row(), _row("10002", profile_count=3)]), frozenset({"10001", "10002"})
        )
    with pytest.raises(aggregate_read.PlanPricingProjectionUnavailable, match="invalid"):
        aggregate_read._partial_aggregate_rows(_Result([_row(), _row()]), frozenset({"10001", "10002"}))
    with pytest.raises(aggregate_read.PlanPricingProjectionUnsupported, match="row bound"):
        aggregate_read._partial_aggregate_rows(_Result([_row(), _row()]), frozenset({"10001"}))
    monkeypatch.setattr(aggregate_read, "MAX_AGGREGATE_REQUEST_STORED_BYTES", 1)
    with pytest.raises(aggregate_read.PlanPricingProjectionUnsupported, match="byte bound"):
        aggregate_read._partial_aggregate_rows(_Result([_row()]), frozenset({"10001"}))


@pytest.mark.asyncio
async def test_native_error_is_sanitized_and_cancellation_propagates():
    session = AsyncMock()
    session.execute.side_effect = DBAPIError(None, None, ValueError("sensitive database detail"), False)
    with pytest.raises(aggregate_read.PlanPricingProjectionUnavailable) as caught:
        await aggregate_read.read_aggregate_pack_page(
            session, _request(), ["10001"], {}, _page(), binding_ordinals=(1,)
        )
    assert str(caught.value) == "aggregate native read is unavailable"
    cancellation = asyncio.CancelledError()
    session.execute.side_effect = cancellation
    with pytest.raises(asyncio.CancelledError) as caught:
        await aggregate_read.read_aggregate_pack_page(
            session, _request(), ["10001"], {}, _page(), binding_ordinals=(1,)
        )
    assert caught.value is cancellation


@pytest.mark.asyncio
async def test_none_keeps_whole_pack_receipt_decoder_and_query_shape():
    session = AsyncMock()
    session.execute.return_value = _Result([_pack_row(_record("10001"))])
    fragments, total = await aggregate_read.read_aggregate_pack_page(session, _request(), ["10001"], {}, _page())
    query, parameters = session.execute.await_args.args
    assert "plan_pricing_aggregate_pack" in str(query) and "binding_ordinals" not in str(query)
    assert set(parameters) == {"projection_id", "code_system", "code", "zip_prefixes"}
    assert total == 1 and _render(fragments)[0]["provider_count"] == 2


@pytest.mark.parametrize("cells", [[None], ["bad"], ["１０００１"], ["10001"] * 16001])
@pytest.mark.asyncio
async def test_empty_admission_cannot_bypass_geographic_validation(cells):
    session = AsyncMock()
    with pytest.raises(aggregate_read.PlanPricingProjectionUnsupported):
        await aggregate_read.read_aggregate_pack_page(session, _request(), cells, {}, _page(), binding_ordinals=())
    session.execute.assert_not_awaited()


def test_no_match_requires_an_exact_empty_admission_receipt():
    empty_row = _row(None, membership_count=0, rate_value_count=0)
    assert aggregate_read._partial_aggregate_rows(_Result([empty_row]), frozenset({"10001"})) == []
    with pytest.raises(aggregate_read.PlanPricingProjectionUnavailable, match="incomplete"):
        aggregate_read._partial_aggregate_rows(_Result([_row(None)]), frozenset({"10001"}))


@pytest.mark.asyncio
async def test_empty_admission_validates_original_projection_identity():
    session = AsyncMock()
    request = _request()
    request.projection_id = "invalid"
    with pytest.raises(aggregate_read.PlanPricingProjectionUnavailable, match="identity"):
        await aggregate_read.read_aggregate_pack_page(session, request, ["10001"], {}, _page(), binding_ordinals=())
    session.execute.assert_not_awaited()
