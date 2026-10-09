# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Full-scope composition feeds native price checks and positional pagination."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import billing_search_pagination as pagination
from api import ptg2_billing_search_service as service
from api.billing_search_endpoint_access import validate_billing_search_endpoint_access_state
from api.billing_search_response import shape_billing_search_response
from api.custom_import_billing_query import _new_billing_import_query
from api.plan_release_serving_resolution import PLAN_RELEASE_RESOLUTION_READY, PlanReleaseServingResolution
from api.ptg2_billing_geo_contract import BillingGeoSelection, BillingProviderGeoPriceWitness
from api.ptg2_billing_search_contract import (
    BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS,
    BILLING_SEARCH_RESULT_NO_MATCHING_RATES,
    BillingSearchServingUnavailableError,
)
from tests import test_billing_search_endpoint_access as native_fixtures
from tests.billing_search_page_support import NPI_VALUES
from tests.billing_search_service_support import CURSOR_KEYRING, TRUSTED_NOW, install_binding_readers
from tests.test_billing_search_response import _endpoint_access, _matched_result, _price
from tests.test_custom_import_billing_query import _TARGET, _prepared


class _OrdinalSession:
    """Observe the complete typed scope while controlling imported SQL results."""

    def __init__(self, ordinals):
        self.ordinals = ordinals
        self.candidate_scopes = []

    async def execute(self, statement):
        compiled = statement.compile()
        assert "LIMIT" not in str(compiled)
        self.candidate_scopes.append(compiled.params["billing_candidate_npis"])
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: self.ordinals))


def _context(access, *, require_match=True):
    state = validate_billing_search_endpoint_access_state(access, trusted_now=TRUSTED_NOW)[1]
    return _new_billing_import_query(_prepared(), require_match, _TARGET, state)


def _install_native_scope(monkeypatch, access, candidates):
    """Keep real cursor binding and native hydration around synthetic read results."""

    fixture = _matched_result(access)
    monkeypatch.setattr(
        service.plan_release_serving_resolution,
        "resolve_plan_release_serving_resolution",
        AsyncMock(
            return_value=PlanReleaseServingResolution(PLAN_RELEASE_RESOLUTION_READY, fixture.selection),
        ),
    )
    monkeypatch.setattr(
        service.billing_search_entity_ref_resolution,
        "resolve_billing_search_entity_ref_selector",
        AsyncMock(
            return_value=fixture.selector_resolution,
        ),
    )
    pin = pagination.BillingSearchGenerationPin(
        pagination.billing_search_snapshot_set_sha256(fixture.selection), "8" * 64, 1001, 1002
    )
    monkeypatch.setattr(pagination, "capture_billing_search_generation_pin", AsyncMock(return_value=pin))
    geo_witnesses = tuple(witness for candidate in candidates for witness in candidate.geo_witnesses)
    install_binding_readers(monkeypatch, geo_selection=BillingGeoSelection(True, geo_witnesses))

    async def hydrate(_session, _tables, *, geo_witnesses, **_kwargs):
        return tuple(
            BillingProviderGeoPriceWitness(witness, (_price(),))
            for witness in geo_witnesses
            if witness.address.npi != NPI_VALUES[1]
        )

    monkeypatch.setattr(
        service.ptg2_billing_search_page.ptg2_billing_price_reader, "hydrate_exact_billing_geo_prices", hydrate
    )


@pytest.mark.asyncio
async def test_complete_import_order_and_price_survivors_page_across_fresh_proofs(monkeypatch):
    access = _endpoint_access(limit="2")
    candidates = tuple(_matched_result(access, npi=npi).providers[0].candidate for npi in NPI_VALUES[:4])
    _install_native_scope(monkeypatch, access, candidates)
    session = _OrdinalSession((3, 2, 1, 4))
    first = await service.search_exact_billing_provider_page(
        session,
        access=access,
        cursor_keyring=CURSOR_KEYRING,
        trusted_now=TRUSTED_NOW,
        import_context=_context(access),
    )
    first_payload = shape_billing_search_response(
        access,
        first,
        cursor_keyring=CURSOR_KEYRING,
        trusted_now=TRUSTED_NOW,
        import_scope=first.import_scope,
    )
    monkeypatch.setattr(native_fixtures, "REQUEST_ID", "123e4567-e89b-42d3-a456-426614174001")
    monkeypatch.setattr(native_fixtures, "ISSUED_AT", "2031-01-02T03:03:56Z")
    resumed_access = _endpoint_access(limit="2", cursor=first_payload["pagination"]["next_cursor"])
    second = await service.search_exact_billing_provider_page(
        session,
        access=resumed_access,
        cursor_keyring=CURSOR_KEYRING,
        trusted_now=TRUSTED_NOW,
        import_context=_context(resumed_access),
    )
    assert tuple(provider.candidate.address.npi for provider in first.providers + second.providers) == (
        NPI_VALUES[2],
        NPI_VALUES[0],
        NPI_VALUES[3],
    )
    assert first.has_more and not second.has_more
    assert session.candidate_scopes == [NPI_VALUES[:4], NPI_VALUES[:4]]
    assert first.import_scope == second.import_scope
    assert all(provider_row["rate_occurrences"] for provider_row in first_payload["items"])


@pytest.mark.parametrize("native_empty", (False, True))
@pytest.mark.asyncio
async def test_native_radius_empty_and_import_intersection_empty_are_distinct(monkeypatch, native_empty):
    access = _endpoint_access()
    candidates = () if native_empty else (_matched_result(access).providers[0].candidate,)
    _install_native_scope(monkeypatch, access, candidates)
    session = _OrdinalSession(())
    result = await service.search_exact_billing_provider_page(
        session,
        access=access,
        cursor_keyring=CURSOR_KEYRING,
        trusted_now=TRUSTED_NOW,
        import_context=_context(access),
    )
    assert result.state == (
        BILLING_SEARCH_RESULT_NO_MATCH_IN_RADIUS if native_empty else BILLING_SEARCH_RESULT_NO_MATCHING_RATES
    )
    assert result.import_scope == _context(access).import_scope
    assert len(session.candidate_scopes) == (0 if native_empty else 1)


@pytest.mark.asyncio
async def test_optional_sort_cannot_silently_drop_native_candidates(monkeypatch):
    access = _endpoint_access()
    candidates = tuple(_matched_result(access, npi=npi).providers[0].candidate for npi in NPI_VALUES[:2])
    _install_native_scope(monkeypatch, access, candidates)
    with pytest.raises(BillingSearchServingUnavailableError):
        await service.search_exact_billing_provider_page(
            _OrdinalSession((1,)),
            access=access,
            cursor_keyring=CURSOR_KEYRING,
            trusted_now=TRUSTED_NOW,
            import_context=_context(access, require_match=False),
        )
