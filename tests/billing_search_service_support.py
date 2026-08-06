# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared synthetic setup for exact billing-search service tests."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

from api import ptg2_billing_search_service as service
from api.billing_search_cursor import BillingSearchCursorKeyring
from api.plan_release_serving import PlanReleaseServingSelection
from api.plan_release_serving_resolution import (
    PLAN_RELEASE_RESOLUTION_READY,
    PlanReleaseServingResolution,
)
from api.ptg2_billing_geo_contract import BillingGeoSelection
from tests.billing_search_page_support import (
    binding,
    code_witness,
    serving_tables,
)

PLAN_RELEASE_ID = "hprelease_01K123456789ABCDEFGHJKMNPQ"
CURSOR_KEYRING = BillingSearchCursorKeyring(
    active_key_id="cursor-v1",
    keys_by_id={"cursor-v1": b"c" * 32},
)


def request(*, cursor=None, provider_npi=None, limit=25):
    return SimpleNamespace(
        billing_entity_ref="be1_" + "A" * 64,
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
        trusted_now="2031-01-02T03:04:05Z",
    )


def selection(*, binding_count=1) -> PlanReleaseServingSelection:
    bindings = tuple(
        binding(ordinal, snapshot_id=f"ptg2:synthetic-{ordinal}")
        for ordinal in range(binding_count)
    )
    serving_table_entries = tuple(
        (
            release_binding.snapshot_id,
            serving_tables(
                snapshot_id=release_binding.snapshot_id,
                snapshot_key=17 + ordinal,
            ),
        )
        for ordinal, release_binding in enumerate(bindings)
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
        _validated_serving_tables=serving_table_entries,
    )


def install_access(monkeypatch) -> None:
    monkeypatch.setattr(
        service,
        "validate_billing_search_endpoint_access",
        lambda endpoint_access: endpoint_access,
    )


def install_ready_release(
    monkeypatch,
    release_selection,
    *,
    after_sort_key=None,
) -> None:
    monkeypatch.setattr(
        service.plan_release_serving,
        "resolve_plan_release_serving_resolution",
        AsyncMock(
            return_value=PlanReleaseServingResolution(
                PLAN_RELEASE_RESOLUTION_READY,
                release_selection,
            )
        ),
    )
    monkeypatch.setattr(
        service.billing_search_pagination,
        "capture_billing_search_generation_pin",
        AsyncMock(return_value=object()),
    )
    monkeypatch.setattr(
        service.billing_search_pagination,
        "build_billing_search_cursor_binding",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        service.billing_search_pagination,
        "open_billing_search_page_cursor",
        lambda *_args, **_kwargs: after_sort_key,
    )


def install_binding_readers(
    monkeypatch,
    *,
    source_scope=object(),
    code_witnesses=None,
    provider_rates=(object(),),
    geo_selection=None,
) -> None:
    if code_witnesses is None:
        code_witnesses = (code_witness(),)
    if geo_selection is None:
        geo_selection = BillingGeoSelection(True, ())
    monkeypatch.setattr(
        service.ptg2_billing_entity_source_resolution,
        "resolve_billing_entity_ref_source_scope",
        AsyncMock(return_value=source_scope),
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
