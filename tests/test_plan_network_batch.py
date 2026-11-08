# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bulk legacy metadata reads keep canonical scope and request ordering."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage, NotFound

from api.endpoint import plan


def _request(rows, query=""):
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: rows)))
    return SimpleNamespace(args={}, query_string=query, ctx=SimpleNamespace(sa_session=session))


async def test_batch_queries_once_and_keeps_requested_order_and_plan_deduplication():
    request = _request(
        [
            {"checksum_network": checksum, "plan_id": plan_id, "year": 2026}
            for checksum, plan_id in ((1, "plan-a"), (2, "plan-b"), (1, "plan-c"), (1, "plan-a"))
        ]
    )
    result = await plan.get_network_batch_by_checksums(request, "2,bad,1,2,3")
    payload = json.loads(result.body)
    assert [entry["checksum"] for entry in payload] == [2, 1]
    assert payload[1]["plans"] == [{"plan_id": "plan-a", "year": 2026}, {"plan_id": "plan-c", "year": 2026}]
    request.ctx.sa_session.execute.assert_awaited_once()
    statement = request.ctx.sa_session.execute.await_args.args[0]
    assert statement.compile().params == {"checksum_network_1": [2, 1, 3]}


async def test_empty_valid_checksum_batch_does_not_query():
    request = _request([])
    with pytest.raises(NotFound):
        await plan.get_network_batch_by_checksums(request, "bad, ,")
    request.ctx.sa_session.execute.assert_not_awaited()


@pytest.mark.parametrize("query", ["network_generation=1", "network_ids=1&network_ids=2"])
async def test_batch_rejects_invalid_canonical_selector_before_query(query):
    request = _request([], query)
    with pytest.raises(InvalidUsage) as caught:
        await plan.get_network_batch_by_checksums(request, "42")
    assert caught.value.headers["Cache-Control"] == "private, no-store"
    request.ctx.sa_session.execute.assert_not_awaited()
