# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_state_scan as scan
from api.billing_search_cursor import BillingSearchCursorError
from api.endpoint.pagination import PaginationParams
from api.plan_pricing_projection_contract import PlanPricingProjectionUnsupported
from tests.test_plan_pricing_state_scan import _Session, _args, _keyring, _selection


def test_state_scan_rejects_malformed_cursor_position(monkeypatch):
    monkeypatch.setattr(
        scan,
        "open_billing_search_cursor",
        lambda *_args, **_kwargs: SimpleNamespace(sort_key=(1, 2, 3)),
    )

    with pytest.raises(BillingSearchCursorError, match="invalid"):
        scan._open_position(
            "cursor",
            keyring=_keyring(),
            trusted_now=2_000_000_000,
            scope=("a" * 64, "b" * 64, "c" * 64, "d" * 64),
        )


def test_state_scan_rejects_out_of_range_limit():
    with pytest.raises(PlanPricingProjectionUnsupported, match="limit"):
        scan._validate_search_request(
            _selection(),
            _args(),
            PaginationParams(page=1, limit=0, offset=0, source="page"),
        )


def test_state_scan_refuses_nonadvancing_cursor():
    with pytest.raises(scan.PTG2ManifestArtifactError, match="no progress"):
        scan._next_page_cursor(
            (),
            0,
            0,
            1,
            has_more=True,
            keyring=_keyring(),
            trusted_now=2_000_000_000,
            scope=("a" * 64, "b" * 64, "c" * 64, "d" * 64),
        )


@pytest.mark.asyncio
async def test_state_scan_skips_occurrence_query_when_no_provider_is_eligible(
    monkeypatch,
):
    monkeypatch.setattr(scan, "_cursor_keyring", _keyring)
    monkeypatch.setattr(scan, "_eligible_provider_npis", lambda *_args: ())
    hydrate = AsyncMock()
    monkeypatch.setattr(scan, "_hydrate_selected_groups", hydrate)
    session = _Session([1000000001], [])

    response_document = await scan.search_plan_pricing_state_scan(
        session,
        _selection(),
        _args(),
        PaginationParams(page=1, limit=1, offset=0, source="page"),
    )

    assert response_document["items"] == []
    assert len(session.calls) == 1
    hydrate.assert_not_awaited()
