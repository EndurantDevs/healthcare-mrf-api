# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused serving coverage for packed factorized pricing projections."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import orjson
import pytest

from api import plan_pricing_projection_read as projection_read
from api import plan_pricing_projection_aggregate_read as aggregate_read
from api.plan_pricing_aggregate_pack import (
    AggregateCodeIdentity,
    AggregatePack,
    AggregatePackKey,
    AggregateZipRecord,
    aggregate_logical_digest,
    aggregate_pack_raw_byte_count,
    encode_aggregate_pack,
)
from api.plan_pricing_projection_contract import (
    PROJECTION_CONTRACT,
    PlanPricingProjectionUnavailable,
    PlanPricingProjectionUnsupported,
)

from .test_plan_pricing_projection import PROJECTION_ID, _selection


class _Result:
    def __init__(self, rows=()):
        self._rows = list(rows)

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self._rows)


class _Session:
    def __init__(self, rows=()):
        self.rows = rows
        self.statements = []

    async def execute(self, statement, params=None):
        self.statements.append((str(statement), dict(params or {})))
        return _Result(self.rows)


def _v3_selection():
    return replace(
        _selection(),
        pricing_projection_contract=PROJECTION_CONTRACT,
    )


def _pack_row(*records: AggregateZipRecord, **updates):
    code_identity = AggregateCodeIdentity("CPT", "27447")
    pack = AggregatePack(
        AggregatePackKey(PROJECTION_ID, code_identity, records[0].zip5[:2]),
        records,
    )
    payload = encode_aggregate_pack(pack)
    row_by_field = {
        "zip_prefix_2": records[0].zip5[:2],
        "entry_count": len(records),
        "raw_byte_count": aggregate_pack_raw_byte_count(payload),
        "stored_byte_count": len(payload),
        "logical_digest": aggregate_logical_digest(code_identity, records),
        "payload_sha256": hashlib.sha256(payload).digest(),
        "payload": payload,
    }
    row_by_field.update(updates)
    return row_by_field


def _record(zip5: str, *, minimum: str = "10") -> AggregateZipRecord:
    return AggregateZipRecord(
        zip5,
        2,
        3,
        Decimal(minimum),
        Decimal("15"),
        Decimal("20"),
    )


@pytest.mark.asyncio
async def test_v3_aggregate_page_validates_and_renders_exact_pack() -> None:
    session = _Session([_pack_row(_record("10001"), _record("10002"))])
    response = await projection_read.search_plan_pricing_projection(
        session,
        _v3_selection(),
        {
            "include_providers": "false",
            "code_system": "CPT",
            "code": "27447",
            "zip5": "10001",
        },
        SimpleNamespace(limit=3, offset=0, page=1),
    )

    rendered = orjson.loads(orjson.dumps(response))
    assert rendered["result_type"] == "rate_aggregates"
    assert rendered["query"]["projection_contract"] == PROJECTION_CONTRACT
    assert rendered["items"] == [
        {
            "geo_cell": "10001",
            "provider_count": 2,
            "rate_count": 3,
            "minimum_negotiated_rate": 10,
            "median_negotiated_rate": 15,
            "maximum_negotiated_rate": 20,
        }
    ]


@pytest.mark.asyncio
async def test_v3_aggregate_page_fails_closed_on_false_raw_receipt() -> None:
    row_by_field = _pack_row(_record("10001"))
    row_by_field["raw_byte_count"] += 1
    with pytest.raises(PlanPricingProjectionUnavailable, match="invalid"):
        await projection_read.search_plan_pricing_projection(
            _Session([row_by_field]),
            _v3_selection(),
            {
                "include_providers": "false",
                "code_system": "CPT",
                "code": "27447",
                "zip5": "10001",
            },
            SimpleNamespace(limit=3, offset=0, page=1),
        )


def test_unknown_attached_projection_contract_fails_closed() -> None:
    with pytest.raises(PlanPricingProjectionUnavailable, match="unsupported"):
        projection_read._validated_projection_request(
            replace(
                _selection(),
                pricing_projection_contract="future-contract",
            ),
            {
                "include_providers": "false",
                "code_system": "CPT",
                "code": "27447",
                "zip5": "10001",
            },
        )


@pytest.mark.asyncio
async def test_packed_reader_bounds_zip_prefixes_before_query() -> None:
    request = projection_read._ProjectionRequest(
        "rate_aggregates",
        PROJECTION_ID,
        PROJECTION_CONTRACT,
        "CPT",
        "27447",
    )
    pagination = SimpleNamespace(limit=3, offset=0, page=1)
    sixteen_prefix_cells = [
        f"{prefix:02d}{suffix:03d}"
        for prefix in range(16)
        for suffix in range(32)
    ]
    session = _Session()
    assert await aggregate_read.read_aggregate_pack_page(
        session,
        request,
        sixteen_prefix_cells,
        {},
        pagination,
    ) == ([], 0)
    assert len(session.statements[0][1]["zip_prefixes"]) == 16

    with pytest.raises(PlanPricingProjectionUnsupported, match="ZIP prefixes"):
        await aggregate_read.read_aggregate_pack_page(
            _Session(),
            request,
            [f"{prefix:02d}000" for prefix in range(17)],
            {},
            pagination,
        )
