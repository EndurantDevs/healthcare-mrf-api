# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed billing keeps full authority while bounding admitted traversal."""

from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import orjson
import pytest
from sanic.request import RequestParameters

from api import billing_search_network_policy as policy
from api import billing_search_pagination as pagination
from api import provider_billing_network_read as transport
from api import ptg2_billing_search_service as service
from api import ptg2_serving as native
from api.billing_search_cursor import BillingSearchCursorGenerationExpired
from api.network_address_scope import network_address_read_scope
from api.plan_release_serving_resolution import PLAN_RELEASE_RESOLUTION_READY, PlanReleaseServingResolution
from api.ptg2_billing_search_contract import BillingSearchServingUnavailableError
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_catalog_policy import RegistryOrganizationVisibility
from tests.billing_search_entity_ref_support import billing_entity_reference
from tests.billing_search_service_support import CURSOR_KEYRING, TRUSTED_NOW, selection, selector_resolution
from tests.test_billing_search_endpoint_access import _authorize, _keyring, _parameters, _signed_headers
from tests.test_billing_search_pagination import _authorization_context, _pin, _request
from tests.test_ptg2_serving_billing_address_lineage import _location_query


@contextmanager
def _scope(*, exclusions=(), networks=None, legacy_checksums=None):
    candidate = "123e4567-e89b-42d3-a456-426614174000"
    manifest = PinnedNetworkServingManifest(
        7, candidate, "network_candidate_" + candidate.replace("-", ""), 1, {}, 4, "a" * 64, 17
    )
    with network_address_read_scope(
        manifest,
        networks,
        access_scope_sha256="b" * 64,
        excluded_network_ids=exclusions if networks is None else None,
        organization_policy=RegistryOrganizationVisibility(),
        legacy_checksums=legacy_checksums,
    ):
        yield manifest


def test_original_coordinates_and_full_recipe_are_bound():
    release = selection(binding_count=2)
    allowed = release.in_network_bindings[1:]
    assert policy.admitted_coordinates(release, allowed) == ((1, "ptg2:synthetic-1"),)
    with _scope():
        first = policy.network_scope_sha256(release, allowed)
        changed = replace(
            release, bindings=(replace(release.bindings[0], source_key="other-source"), release.bindings[1])
        )
        assert policy.network_scope_sha256(changed, allowed) != first
        assert policy.network_scope_sha256(release, ()) != first
    with _scope(exclusions=(19,)):
        assert policy.network_scope_sha256(release, allowed) != first
    assert release.bindings == selection(binding_count=2).bindings


@pytest.mark.parametrize("positions", [(1, 0), (0, 0)])
def test_subset_cannot_reorder_or_duplicate_original_bindings(positions):
    release = selection(binding_count=2)
    with pytest.raises(BillingSearchServingUnavailableError):
        policy.admitted_coordinates(release, tuple(release.bindings[index] for index in positions))


def test_actual_native_sql_scopes_all_office_access_before_ranking():
    with _scope(exclusions=(19,)) as manifest:
        sql = policy._retained_location_sql(native, _location_query(address_assurance_sql="addr.is_assured"))
    assert "FROM mrf.entity_address_unified" not in sql
    assert "JOIN mrf.entity_address_unified" not in sql
    assert sql.count('"' + manifest.schema_name + '".entity_address_unified') >= 3
    assert "canonical_network_ids" in sql
    assert "_network_excluded_ids" in sql
    assert sql.index(manifest.schema_name) < sql.index("ROW_NUMBER")
    assert sql.index(manifest.schema_name) < sql.index("LIMIT")


def test_native_renderer_fails_closed_if_canonical_anchor_changes(monkeypatch):
    monkeypatch.setattr(native, "_MEMBERSHIP_UNIFIED_ASSURED_LOCATION_SQL", "SELECT {address_table}")
    with _scope(), pytest.raises(BillingSearchServingUnavailableError):
        policy._retained_location_sql(native, _location_query(address_assurance_sql="addr.is_assured"))


@pytest.mark.asyncio
async def test_full_selector_validates_before_admitted_traversal(monkeypatch):
    release = selection(binding_count=2)
    candidate_reader = AsyncMock(return_value=((), False))
    monkeypatch.setattr(service, "_binding_candidates", candidate_reader)
    await service._traverse_release(
        object(),
        selection=release,
        selector_resolution=selector_resolution(release),
        request=object(),
        admitted_bindings=release.bindings[1:],
    )
    assert candidate_reader.await_count == 1
    assert candidate_reader.await_args.kwargs["binding"].binding_ordinal == 1
    candidate_reader.reset_mock()
    partial = selector_resolution(replace(release, bindings=release.bindings[1:]))
    with pytest.raises(BillingSearchServingUnavailableError):
        await service._traverse_release(
            object(),
            selection=release,
            selector_resolution=partial,
            request=object(),
            admitted_bindings=release.bindings[1:],
        )
    candidate_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_admission_never_traverses_denied_source(monkeypatch):
    release = selection(binding_count=2)
    candidate_reader = AsyncMock(side_effect=AssertionError("denied traversal"))
    monkeypatch.setattr(service, "_binding_candidates", candidate_reader)
    result = await service._traverse_release(
        object(),
        selection=release,
        selector_resolution=selector_resolution(release),
        request=object(),
        admitted_bindings=(),
    )
    assert result.candidates == ()
    candidate_reader.assert_not_awaited()


def test_real_aead_cursor_refuses_policy_change_without_changing_native_pin():
    request = _request()
    pin = _pin()
    authority = _authorization_context()
    first = pagination.build_billing_search_cursor_binding(
        request, authority, pin, trusted_now=TRUSTED_NOW, network_scope_sha256="a" * 64
    )
    assert first.native_generation_bundle_sha256 == pin.generation_bundle_sha256
    token = pagination.seal_billing_search_page_cursor(
        (1, 1000000004, 0, "ptg2:synthetic", "0" * 64, "1" * 64, "2" * 64), keyring=CURSOR_KEYRING, binding=first
    )
    from api.billing_search_cursor_authentication import authenticate_billing_search_sealed_page_cursor

    _, public_token = authenticate_billing_search_sealed_page_cursor(
        token,
        keyring=CURSOR_KEYRING,
        trusted_now=first.trusted_now,
        request_fingerprint_sha256=first.request_fingerprint_sha256,
        authorization_context_sha256=first.authorization_scope_sha256,
        generation_bundle_sha256=first.generation_bundle_sha256,
        snapshot_set_sha256=first.snapshot_set_sha256,
    )
    continuation = _request(cursor=public_token)
    second = pagination.build_billing_search_cursor_binding(
        continuation, authority, pin, trusted_now=TRUSTED_NOW, network_scope_sha256="b" * 64
    )
    with pytest.raises(BillingSearchCursorGenerationExpired):
        pagination.open_billing_search_page_cursor(continuation, keyring=CURSOR_KEYRING, binding=second)
    assert pin == _pin()


def _signed_native():
    parameters = _parameters(billing_entity_ref=billing_entity_reference(), code="99213", zip5="25000")
    headers = _signed_headers(tuple(sorted(parameters.items())))
    access = _authorize(parameters, headers)
    values_by_name = {
        **parameters,
        "billing_transport_context_sha256": access.verified_transport.transport_context_sha256,
    }
    return headers, RequestParameters({name: [value] for name, value in values_by_name.items()})


def _install_real_transport(monkeypatch):
    monkeypatch.setattr(transport.billing_http, "_trusted_now", lambda: TRUSTED_NOW)
    monkeypatch.setattr(transport.billing_http, "_transport_keyring", _keyring)
    monkeypatch.setattr(transport.billing_http, "_cursor_keyring", lambda: CURSOR_KEYRING)


@pytest.mark.asyncio
async def test_signed_adapter_calls_actual_service_with_only_admitted_sources(monkeypatch):
    _install_real_transport(monkeypatch)
    release = selection(binding_count=2)
    monkeypatch.setattr(
        service.plan_release_serving_resolution,
        "resolve_plan_release_serving_resolution",
        AsyncMock(return_value=PlanReleaseServingResolution(PLAN_RELEASE_RESOLUTION_READY, release)),
    )
    monkeypatch.setattr(
        service.billing_search_entity_ref_resolution,
        "resolve_billing_search_entity_ref_selector",
        AsyncMock(return_value=selector_resolution(release, states=("no_match", "matched"))),
    )
    monkeypatch.setattr(policy, "admitted_plan_release_bindings", AsyncMock(return_value=release.bindings[1:]))
    pin = pagination.BillingSearchGenerationPin(
        pagination.billing_search_snapshot_set_sha256(release), "c" * 64, 11, 12
    )
    monkeypatch.setattr(pagination, "capture_billing_search_generation_pin", AsyncMock(return_value=pin))
    reader = AsyncMock(return_value=((), False))
    monkeypatch.setattr(service, "_binding_candidates", reader)
    headers, args = _signed_native()
    with _scope():
        response = await transport.read_managed_billing_search(
            SimpleNamespace(headers=headers), session=SimpleNamespace(in_transaction=lambda: True), native_args=args
        )
    assert response.status == 200, response.body
    assert orjson.loads(response.body)["items"] == []
    assert reader.await_count == 1
    assert reader.await_args.kwargs["binding"].binding_ordinal == 1


@pytest.mark.asyncio
async def test_real_invalid_billing_hmac_never_enters_service(monkeypatch):
    _install_real_transport(monkeypatch)
    headers, args = _signed_native()
    from api.billing_search_transport_contract import BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER

    headers[BILLING_SEARCH_TRANSPORT_SIGNATURE_HEADER] = "A" * 43
    reader = AsyncMock(side_effect=AssertionError("unauthorized service"))
    monkeypatch.setattr(transport, "search_exact_billing_provider_page", reader)
    with _scope():
        response = await transport.read_managed_billing_search(
            SimpleNamespace(headers=headers), session=SimpleNamespace(in_transaction=lambda: True), native_args=args
        )
    assert response.status == 404
    reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_selected_network_requires_source_authority_before_payload_sql():
    from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable

    session = SimpleNamespace(
        connection=AsyncMock(side_effect=PlanPricingProjectionUnavailable("source authority unavailable")),
        execute=AsyncMock(side_effect=AssertionError("selected payload SQL")),
    )
    with _scope(networks=(19,)), pytest.raises(PlanPricingProjectionUnavailable):
        await policy.admitted_plan_release_bindings(session, selection())
    session.connection.assert_awaited_once()
    session.execute.assert_not_awaited()


def test_nested_billing_authority_can_be_checked_before_reader(monkeypatch):
    _install_real_transport(monkeypatch)
    headers, args = _signed_native()
    access, trusted_now = transport.authorize_managed_billing_search(SimpleNamespace(headers=headers), native_args=args)
    assert access.request.billing_entity_ref == billing_entity_reference()
    assert trusted_now == TRUSTED_NOW
    args["billing_transport_context_sha256"] = ["f" * 64]
    with pytest.raises(BillingSearchServingUnavailableError):
        transport.authorize_managed_billing_search(SimpleNamespace(headers=headers), native_args=args)


@pytest.mark.asyncio
async def test_exact_retained_lineage_is_one_set_query_on_same_reader():
    from tests.network_read_execution_support import with_read_identity_receipts
    from tests.ptg2_billing_geo_reader_support import NPI_A, _location_row
    from tests.ptg2_serving_coverage_paydown_support import FakeResult, FakeSession

    rows = [{**_location_row(NPI_A), "_geo_evidence_level": "nppes_registry_address", "_geo_evidence_source_id": 1}]
    session = with_read_identity_receipts(FakeSession([FakeResult([])]))
    with _scope(exclusions=(19,)) as manifest:
        await policy._retained_lineage(session, native, rows)
    assert len(session.calls) == 1
    arguments, _ = session.calls[0]
    assert '"' + manifest.schema_name + '".entity_address_unified' in str(arguments[0])
    assert "FROM mrf.entity_address_unified" not in str(arguments[0])
    assert arguments[1]["stored_only"] is True
    assert arguments[1]["strict_stored_identity"] is True
    assert arguments[1]["_network_excluded_ids"] == [19]
    assert rows == []


@pytest.mark.parametrize("next_checksums", [(84,), None])
def test_billing_cursor_rejects_changed_legacy_alias_or_canonical_namespace(next_checksums):
    from api.billing_search_cursor_authentication import authenticate_billing_search_sealed_page_cursor

    release = selection()
    with _scope(networks=(42,), legacy_checksums=(42,)):
        original_digest = policy.network_scope_sha256(release, release.in_network_bindings)
    with _scope(networks=(42,), legacy_checksums=next_checksums):
        changed_digest = policy.network_scope_sha256(release, release.in_network_bindings)
    assert changed_digest != original_digest
    request, authority, pin = _request(), _authorization_context(), _pin()
    first = pagination.build_billing_search_cursor_binding(
        request, authority, pin, trusted_now=TRUSTED_NOW, network_scope_sha256=original_digest
    )
    token = pagination.seal_billing_search_page_cursor(
        (1, 1000000004, 0, "ptg2:synthetic", "0" * 64, "1" * 64, "2" * 64), keyring=CURSOR_KEYRING, binding=first
    )
    _, public_token = authenticate_billing_search_sealed_page_cursor(
        token,
        keyring=CURSOR_KEYRING,
        trusted_now=first.trusted_now,
        request_fingerprint_sha256=first.request_fingerprint_sha256,
        authorization_context_sha256=first.authorization_scope_sha256,
        generation_bundle_sha256=first.generation_bundle_sha256,
        snapshot_set_sha256=first.snapshot_set_sha256,
    )
    continuation = _request(cursor=public_token)
    second = pagination.build_billing_search_cursor_binding(
        continuation, authority, pin, trusted_now=TRUSTED_NOW, network_scope_sha256=changed_digest
    )
    with pytest.raises(BillingSearchCursorGenerationExpired):
        pagination.open_billing_search_page_cursor(continuation, keyring=CURSOR_KEYRING, binding=second)
