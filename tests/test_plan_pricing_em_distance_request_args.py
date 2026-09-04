# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""E&M retries preserve Sanic's first-value query parameter semantics."""

from copy import deepcopy
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.request.parameters import RequestParameters

from api.plan_pricing_em_distance import em_distance_retry_option
from tests.test_plan_release_serving import _Session, _binding_row
from tests.test_pricing_api import make_request, pricing_module
from tests.test_pricing_em_release_resolution import _distance_args, _install_search


RETRY_BY_FIELD = {
    "order_by": "distance",
    "order": "asc",
    "include_providers": True,
    "view": "card",
    "offset": 0,
    "start": 0,
    "page": 1,
}


@pytest.mark.parametrize("is_sanic_args", [False, True])
@pytest.mark.parametrize(("field", "values", "is_eligible"), [
    ("code", ["99203", "not-an-em-code"], True),
    ("code", ["not-an-em-code", "99203"], False),
    ("zip_radius_miles", ["25", "26"], True),
    ("zip_radius_miles", ["26", "25"], False),
    ("include_sources", ["false", "true"], True),
    ("include_sources", ["true", "false"], False),
    ("npi", ["", "1003000123"], True),
    ("npi", ["1003000123", ""], False),
])
def test_retry_uses_first_query_value_without_mutation(
    is_sanic_args, field, values, is_eligible
):
    values_by_field = {
        key: [value] for key, value in _distance_args(order_by="total_allowed_amount").items()
    }
    values_by_field[field] = values
    args = RequestParameters(values_by_field) if is_sanic_args else {
        key: value[0] for key, value in values_by_field.items()
    }
    before_by_field = deepcopy(dict(args))

    retry_by_field = em_distance_retry_option(
        args, SimpleNamespace(limit=25, offset=0)
    )

    assert retry_by_field == (RETRY_BY_FIELD if is_eligible else None)
    assert dict(args) == before_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize(("order_by", "status", "query_count"), [
    ("distance", 200, 2),
    ("total_allowed_amount", 422, 1),
])
async def test_native_query_args_use_projection_resolution_and_retry(
    monkeypatch, order_by, status, query_count
):
    args = RequestParameters({
        key: [value] for key, value in _distance_args(order_by=order_by).items()
    })
    session = _Session([_binding_row()])
    search = _install_search(monkeypatch)
    monkeypatch.setattr(
        pricing_module, "is_em_distance_projection_ready", AsyncMock(return_value=True)
    )
    request = make_request([], args=args)
    request.ctx.sa_session = session

    response = await pricing_module.list_providers_by_procedure(request)

    assert response.status == status
    assert len(session.calls) == query_count
    if status == 422:
        assert json.loads(response.body)["fix_it"]["retry_options"] == [RETRY_BY_FIELD]
        search.assert_not_awaited()
    else:
        assert search.await_args.kwargs["release_selection"].plan_release_id == args.get("plan_release_id")
        search.assert_awaited_once()
