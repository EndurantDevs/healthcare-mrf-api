# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configured-import cursor and sealed provider ordering regressions."""

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import billing_search_cursor as cursor
from api import billing_search_pagination as pagination
from api import ptg2_billing_search_page as page
from api.billing_search_import_contract import (
    BillingSearchComposedOrder,
    BillingSearchImportCursorScope,
    _new_billing_search_composed_order,
    validate_billing_search_composed_order,
)
from api.ptg2_billing_search_contract import BillingSearchMatchedProvider, BillingSearchProviderPage
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.billing_search_page_support import NPI_VALUES, candidate, hydrated_price
from tests.test_billing_search_pagination import (
    KEYRING,
    REQUEST_TIME,
    _authorization_context,
    _pin,
    _request,
    _wire_token,
)


def _scope(**overrides):
    values_by_name = {
        "query_fingerprint_sha256": "1" * 64,
        "authorization_scope_sha256": "2" * 64,
        "generation_bundle_sha256": "3" * 64,
        **overrides,
    }
    return BillingSearchImportCursorScope(**values_by_name)


def _binding(request=None, scope=None):
    return pagination.build_billing_search_cursor_binding(
        request or _request(), _authorization_context(), _pin(), trusted_now=REQUEST_TIME, import_scope=scope
    )


@pytest.mark.parametrize("field", tuple(_scope().as_dict()))
@pytest.mark.parametrize("invalid", [None, True, "0" * 64, "a" * 63, "G" * 64])
def test_import_scope_rejects_invalid_coordinates(field, invalid):
    with pytest.raises(PTG2ManifestArtifactError):
        _scope(**{field: invalid})


def test_composed_binding_preserves_native_snapshot_and_legacy_coordinates():
    legacy = _binding()
    composed = _binding(scope=_scope())
    assert legacy.request_fingerprint_sha256 == _request().request_fingerprint_sha256
    assert legacy.import_scope is None
    assert legacy.native_generation_bundle_sha256 is None
    assert composed.snapshot_set_sha256 == legacy.snapshot_set_sha256
    assert composed.native_generation_bundle_sha256 == legacy.generation_bundle_sha256
    assert composed.request_fingerprint_sha256 != legacy.request_fingerprint_sha256
    assert composed.authorization_scope_sha256 != legacy.authorization_scope_sha256
    assert composed.generation_bundle_sha256 != legacy.generation_bundle_sha256


@pytest.mark.parametrize(
    ("field", "failure"),
    [
        ("query_fingerprint_sha256", cursor.BillingSearchCursorError),
        ("authorization_scope_sha256", cursor.BillingSearchCursorError),
        ("generation_bundle_sha256", cursor.BillingSearchCursorGenerationExpired),
    ],
)
def test_composed_cursor_cannot_cross_import_coordinates(field, failure):
    first_binding = _binding(scope=_scope())
    marker = candidate().sort_key
    sealed = pagination.seal_billing_search_page_cursor(marker, keyring=KEYRING, binding=first_binding)
    request = _request(cursor=_wire_token(sealed, first_binding))
    same_binding = _binding(request, _scope())
    assert pagination.open_billing_search_page_cursor(request, keyring=KEYRING, binding=same_binding) == marker
    changed_binding = _binding(request, _scope(**{field: "9" * 64}))
    with pytest.raises(failure):
        pagination.open_billing_search_page_cursor(request, keyring=KEYRING, binding=changed_binding)


def test_composed_binding_rejects_replaced_native_generation():
    with pytest.raises(PTG2ManifestArtifactError):
        replace(_binding(scope=_scope()), native_generation_bundle_sha256="9" * 64)


def _ordered_candidates():
    return tuple(candidate(npi=npi, distance=float(ordinal)) for ordinal, npi in enumerate(NPI_VALUES[:4], start=1))[
        ::-1
    ]


def _order(candidates):
    return _new_billing_search_composed_order(tuple(item.sort_key for item in candidates), _scope())


def test_sealed_order_allows_only_complete_order_or_increasing_subsequence():
    candidates = _ordered_candidates()
    keys = tuple(item.sort_key for item in candidates)
    order = _order(candidates)
    validate_billing_search_composed_order(keys, order, complete=True)
    validate_billing_search_composed_order(keys[::2], order)
    for bad_keys, complete in [(keys[::-1], False), (keys[:1], True), (keys + keys[:1], False)]:
        with pytest.raises(PTG2ManifestArtifactError):
            validate_billing_search_composed_order(bad_keys, order, complete=complete)
    with pytest.raises(PTG2ManifestArtifactError):
        validate_billing_search_composed_order(keys, None)


@pytest.mark.parametrize("mutation", ["keys", "scope", "signature"])
def test_order_rejects_tampered_or_forged_state(mutation):
    candidates = _ordered_candidates()
    order = _order(candidates)
    if mutation == "keys":
        object.__setattr__(order, "candidate_keys", order.candidate_keys[::-1])
    elif mutation == "scope":
        object.__setattr__(order, "import_scope", _scope(generation_bundle_sha256="9" * 64))
    else:
        order = object.__new__(BillingSearchComposedOrder)
        object.__setattr__(order, "candidate_keys", tuple(item.sort_key for item in candidates))
        object.__setattr__(order, "import_scope", _scope())
        object.__setattr__(order, "_signature", b"x" * 32)
    with pytest.raises(PTG2ManifestArtifactError):
        validate_billing_search_composed_order(tuple(item.sort_key for item in candidates), order, complete=True)


def test_provider_page_independently_reproves_composed_order():
    candidates = _ordered_candidates()
    providers = tuple(
        BillingSearchMatchedProvider(item, tuple(hydrated_price(w) for w in item.geo_witnesses)) for item in candidates
    )
    order = _order(candidates)
    valid = BillingSearchProviderPage(providers, False, None, composed_order=order)
    assert valid.providers == providers
    with pytest.raises(PTG2ManifestArtifactError):
        BillingSearchProviderPage(providers[::-1], False, None, composed_order=order)


@pytest.mark.asyncio
async def test_composed_pages_resume_by_identity_and_skip_price_ineligible_candidates(monkeypatch):
    candidates = _ordered_candidates()
    order = _order(candidates)
    excluded = candidates[1].address.npi
    hydrate = AsyncMock(
        side_effect=lambda _session, _tables, *, geo_witnesses, **_kwargs: tuple(
            hydrated_price(witness) for witness in geo_witnesses if witness.address.npi != excluded
        )
    )
    monkeypatch.setattr(page.ptg2_billing_price_reader, "hydrate_exact_billing_geo_prices", hydrate)
    first = await page.hydrate_billing_search_page(
        object(), candidates=candidates, after_sort_key=None, limit=2, price_filter_args={}, composed_order=order
    )
    second = await page.hydrate_billing_search_page(
        object(),
        candidates=candidates,
        after_sort_key=first.next_sort_key,
        limit=2,
        price_filter_args={},
        composed_order=order,
    )
    assert tuple(provider.candidate for provider in first.providers + second.providers) == (
        candidates[0],
        candidates[2],
        candidates[3],
    )
    assert first.has_more and not second.has_more
    assert first.next_sort_key == candidates[2].sort_key
    with pytest.raises(PTG2ManifestArtifactError):
        await page.hydrate_billing_search_page(
            object(),
            candidates=candidates,
            after_sort_key=candidate(binding_ordinal=9).sort_key,
            limit=2,
            price_filter_args={},
            composed_order=order,
        )
