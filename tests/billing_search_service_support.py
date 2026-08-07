# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared synthetic setup for exact billing-search service tests."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

from api import ptg2_billing_search_service as service
from api.billing_search_cursor import BillingSearchCursorKeyring
from api.billing_search_pagination import BillingSearchCursorBinding
from api.billing_search_selector_contract import (
    BILLING_SELECTOR_MATCHED,
    BILLING_SELECTOR_NO_MATCH,
    BILLING_SELECTOR_PROJECTION_UNAVAILABLE,
    BillingSearchSelectorBindingScope,
    BillingSearchSelectorNotFoundError,
    BillingSearchSelectorResolution,
    BillingSearchSelectorScope,
)
from api.plan_release_serving import PlanReleaseServingSelection
from api.plan_release_serving_resolution import (
    PLAN_RELEASE_RESOLUTION_READY,
    PlanReleaseServingResolution,
)
from api.ptg2_billing_geo_contract import BillingGeoSelection
from tests.billing_search_page_support import (
    binding,
    code_witness,
)
from tests.billing_search_entity_ref_support import (
    billing_entity_reference,
    resolved_source_scope,
    serving_tables as source_serving_tables,
    source_publication,
)

PLAN_RELEASE_ID = "hprelease_01K123456789ABCDEFGHJKMNPQ"
TRUSTED_NOW = "2031-01-02T03:04:05Z"
_DEFAULT_SOURCE_SCOPE = object()
CURSOR_KEYRING = BillingSearchCursorKeyring(
    active_key_id="cursor-v1",
    keys_by_id={"cursor-v1": b"c" * 32},
)
CURSOR_BINDING = BillingSearchCursorBinding(
    request_fingerprint_sha256="1" * 64,
    authorization_scope_sha256="2" * 64,
    generation_bundle_sha256="3" * 64,
    snapshot_set_sha256="4" * 64,
    trusted_now=1,
)


def request(*, cursor=None, provider_npi=None, limit=25):
    return SimpleNamespace(
        billing_entity_ref=billing_entity_reference(),
        code="99213",
        code_system="CPT",
        cursor=cursor,
        geo_args={"zip5": "25000"},
        limit=limit,
        plan_release_id=PLAN_RELEASE_ID,
        price_filter_args={"modifiers": (), "place_of_service": ()},
        provider_npi=provider_npi,
    )


def access(**request_overrides):
    return SimpleNamespace(
        request=request(**request_overrides),
        authorization_context=object(),
    )


def selection(*, binding_count=1) -> PlanReleaseServingSelection:
    bindings = tuple(
        binding(ordinal, snapshot_id=f"ptg2:synthetic-{ordinal}")
        for ordinal in range(binding_count)
    )
    serving_table_entries = []
    for ordinal, release_binding in enumerate(bindings):
        publication = source_publication(
            content_digest=f"{6 + ordinal:x}" * 64,
        )
        serving_table_entries.append(
            (
                release_binding.snapshot_id,
                source_serving_tables(
                    publication=publication,
                    snapshot_id=release_binding.snapshot_id,
                    shared_snapshot_key=17 + ordinal,
                    plan_id=release_binding.plan_id,
                    plan_market_type=release_binding.plan_market_type,
                    source_key=release_binding.source_key,
                ),
            )
        )
    return PlanReleaseServingSelection(
        serving_revision_id="hpserve_01K123456789ABCDEFGHJKMNPQ",
        plan_release_id=PLAN_RELEASE_ID,
        healthporta_plan_id="hpplan_01K123456789ABCDEFGHJKMNPQ",
        plan_version_id="hpversion_01K123456789ABCDEFGHJKMNPQ",
        release_month="2031-01",
        release_status="published",
        binding_set_digest="5" * 64,
        bindings=bindings,
        _validated_serving_tables=tuple(serving_table_entries),
        _includes_billing_tax_identity_source=True,
    )


def selector_resolution(
    release_selection,
    *,
    states=None,
    source_scopes=None,
) -> BillingSearchSelectorResolution:
    selected_states = states or (BILLING_SELECTOR_MATCHED,) * len(
        release_selection.in_network_bindings
    )
    selected_source_scopes = source_scopes or ()
    bindings = []
    for position, (release_binding, state) in enumerate(
        zip(
            release_selection.in_network_bindings,
            selected_states,
            strict=True,
        )
    ):
        serving = release_selection.serving_tables_for_snapshot(
            release_binding.snapshot_id
        )
        if state == BILLING_SELECTOR_MATCHED:
            source_scope = (
                selected_source_scopes[position]
                if selected_source_scopes
                else resolved_source_scope(
                    publication=serving.provider_tax_identity_source_publication,
                    snapshot_key=serving.shared_snapshot_key,
                )
            )
            bindings.append(
                BillingSearchSelectorBindingScope(
                    binding_ordinal=release_binding.binding_ordinal,
                    snapshot_id=release_binding.snapshot_id,
                    state=state,
                    source_scope=source_scope,
                    billing_entity_ref=billing_entity_reference(),
                )
            )
        else:
            bindings.append(
                BillingSearchSelectorBindingScope(
                    binding_ordinal=release_binding.binding_ordinal,
                    snapshot_id=release_binding.snapshot_id,
                    state=state,
                )
            )
    return BillingSearchSelectorResolution(
        BillingSearchSelectorScope(
            selector_kind="billing_entity_ref",
            bindings=tuple(bindings),
        ),
        "6" * 64,
    )


def install_access(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "validate_billing_search_endpoint_access_state",
        lambda endpoint_access, **_kwargs: (endpoint_access, "a" * 64),
    )


def install_ready_release(
    monkeypatch,
    release_selection,
    *,
    after_sort_key=None,
) -> None:
    monkeypatch.setattr(
        service.plan_release_serving_resolution,
        "resolve_plan_release_serving_resolution",
        AsyncMock(
            return_value=PlanReleaseServingResolution(
                PLAN_RELEASE_RESOLUTION_READY,
                release_selection,
            )
        ),
    )
    monkeypatch.setattr(
        service.billing_search_entity_ref_resolution,
        "resolve_billing_search_entity_ref_selector",
        AsyncMock(return_value=selector_resolution(release_selection)),
    )
    monkeypatch.setattr(
        service.billing_search_pagination,
        "capture_billing_search_generation_pin",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        service.billing_search_pagination,
        "build_billing_search_cursor_binding",
        lambda *_args, **_kwargs: CURSOR_BINDING,
    )
    monkeypatch.setattr(
        service.billing_search_pagination,
        "open_billing_search_page_cursor",
        lambda *_args, **_kwargs: after_sort_key,
    )


def install_binding_readers(
    monkeypatch,
    *,
    source_scope=_DEFAULT_SOURCE_SCOPE,
    code_witnesses=None,
    provider_rates=(object(),),
    geo_selection=None,
) -> None:
    if code_witnesses is None:
        code_witnesses = (code_witness(),)
    if geo_selection is None:
        geo_selection = BillingGeoSelection(True, ())
    if source_scope is None:
        monkeypatch.setattr(
            service.billing_search_entity_ref_resolution,
            "resolve_billing_search_entity_ref_selector",
            AsyncMock(
                side_effect=BillingSearchSelectorNotFoundError(
                    "billing_search_resource_not_found"
                )
            ),
        )
    elif source_scope is not _DEFAULT_SOURCE_SCOPE:
        selected_release = selection()
        monkeypatch.setattr(
            service.billing_search_entity_ref_resolution,
            "resolve_billing_search_entity_ref_selector",
            AsyncMock(
                return_value=selector_resolution(
                    selected_release,
                    source_scopes=(source_scope,),
                )
            ),
        )
    monkeypatch.setattr(
        service.ptg2_billing_code_reader,
        "load_exact_billing_code_witnesses",
        AsyncMock(return_value=code_witnesses),
    )
    monkeypatch.setattr(
        service.ptg2_billing_exact_reader,
        "load_exact_billing_rate_occurrence_witnesses",
        AsyncMock(return_value=(object(),)),
    )
    monkeypatch.setattr(
        service.ptg2_billing_geo_reader,
        "expand_billing_rate_witnesses_to_npis",
        AsyncMock(return_value=provider_rates),
    )
    monkeypatch.setattr(
        service.ptg2_billing_geo_reader,
        "load_exact_billing_geo_witnesses",
        AsyncMock(return_value=geo_selection),
    )
