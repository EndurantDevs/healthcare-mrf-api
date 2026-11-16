# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep billing office eligibility in the signed query and native address selection."""

from unittest.mock import AsyncMock

import pytest

from api import ptg2_billing_geo_reader as geo_reader
from api.billing_search_request import BillingSearchRequestError, parse_billing_search_request
from tests.ptg2_billing_geo_reader_support import _provider_rate, _tables
from tests.test_billing_search_request import _coordinate_pairs, _MultiParameters, _zip_pairs


@pytest.mark.parametrize("pairs", [_zip_pairs, _coordinate_pairs])
def test_billing_minimum_is_preserved_and_bound_to_cursor_request_identity(pairs):
    baseline = parse_billing_search_request(_MultiParameters(pairs()))
    zero = parse_billing_search_request(_MultiParameters(pairs(min_address_sources="0")))
    minimum = parse_billing_search_request(_MultiParameters(pairs(min_address_sources="3")))
    changed = parse_billing_search_request(_MultiParameters(pairs(min_address_sources="4")))
    assert baseline.min_address_sources == zero.min_address_sources == 0
    assert baseline.geo_args == zero.geo_args
    assert minimum.min_address_sources == 3 and minimum.geo_args["min_address_sources"] == 3
    assert minimum.request_fingerprint_sha256 != baseline.request_fingerprint_sha256
    assert minimum.request_fingerprint_sha256 != changed.request_fingerprint_sha256
    assert geo_reader._validated_geo_args(minimum.geo_args)["min_address_sources"] == 3


@pytest.mark.parametrize("value", ["-1", "2.5", "true", "2147483648", "", " 3"])
def test_billing_minimum_rejects_invalid_values_at_closed_request_boundary(value):
    with pytest.raises(BillingSearchRequestError, match="billing_search_request_invalid"):
        parse_billing_search_request(_MultiParameters(_zip_pairs(min_address_sources=value)))


def test_billing_minimum_rejects_duplicate_query_values():
    with pytest.raises(BillingSearchRequestError):
        parse_billing_search_request(
            _MultiParameters((*_zip_pairs(), ("min_address_sources", "2"), ("min_address_sources", "3")))
        )


@pytest.mark.asyncio
async def test_billing_minimum_reaches_native_office_selection_before_no_match(monkeypatch):
    lookup = AsyncMock(return_value=[])
    monkeypatch.setattr(geo_reader.ptg2_serving, "_membership_location_rows", lookup)
    selected = await geo_reader.load_exact_billing_geo_witnesses(
        object(),
        _tables(),
        provider_rate_witnesses=(_provider_rate(),),
        geo_args={"zip5": "25000", "min_address_sources": 3},
    )
    assert selected.address_projection_available is True and selected.witnesses == ()
    assert lookup.await_args.args[2]["min_address_sources"] == 3
    assert lookup.await_args.kwargs["offset"] == 0
