# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generation-bound billing-search pagination tests."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import billing_search_cursor as cursor
from api import billing_search_pagination as pagination
from api.billing_search_access_contract import (
    build_billing_search_authorization_context,
)
from api.billing_search_request import parse_billing_search_request
from api.plan_release_serving import (
    PlanReleaseServingSelection,
    PlanReleaseSnapshotBinding,
)
from api.ptg2_types import PTG2ServingTables
from tests.test_billing_search_transport_contract import (
    BILLING_ENTITY_REF,
    PLAN_ENTITLEMENT_SHA256,
    PLAN_RELEASE_ID,
)

SNAPSHOT_ID = "ptg2:203101:synthetic"
REQUEST_TIME = "2031-01-02T03:04:05Z"
NEXT_REQUEST_TIME = "2031-01-02T03:05:05Z"
KEYRING = cursor.BillingSearchCursorKeyring(
    active_key_id="cursor-v1",
    keys_by_id={"cursor-v1": b"c" * 32},
)


def _request(**overrides):
    values_by_name = {
        "billing_entity_ref": BILLING_ENTITY_REF,
        "code": "99213",
        "code_system": "CPT",
        "limit": "25",
        "plan_release_id": PLAN_RELEASE_ID,
        "zip5": "25701",
    }
    values_by_name.update(overrides)
    return parse_billing_search_request(values_by_name)


def _authorization_context(
    *,
    issued_at="2031-01-02T03:03:55Z",
    expires_at="2031-01-02T03:04:55Z",
    trusted_now=REQUEST_TIME,
    **overrides,
):
    claims_by_name = {
        "principal_scope_sha256": "1" * 64,
        "tenant_scope_sha256": "2" * 64,
        "plan_entitlement_sha256": PLAN_ENTITLEMENT_SHA256,
        "audit_scope_sha256": "3" * 64,
        "quota_scope_sha256": "4" * 64,
        "capabilities": ("pricing:billing-search",),
        "issued_at": issued_at,
        "expires_at": expires_at,
    }
    claims_by_name.update(overrides)
    return build_billing_search_authorization_context(
        claims_by_name,
        trusted_now=trusted_now,
    )


def _binding() -> PlanReleaseSnapshotBinding:
    return PlanReleaseSnapshotBinding(
        binding_ordinal=0,
        snapshot_id=SNAPSHOT_ID,
        source_key="synthetic-source",
        plan_id="synthetic-plan",
        plan_market_type="group",
        role="in_network",
        required=True,
    )


def _tables() -> PTG2ServingTables:
    return PTG2ServingTables(
        snapshot_id=SNAPSHOT_ID,
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=1,
        coverage_scope_id="coverage-synthetic",
        plan_id="synthetic-plan",
        plan_market_type="group",
        source_key="synthetic-source",
        source_trace_set_hash="trace-synthetic",
    )


def _selection() -> PlanReleaseServingSelection:
    binding = _binding()
    return PlanReleaseServingSelection(
        serving_revision_id="hpserve_01K123456789ABCDEFGHJKMNPQ",
        plan_release_id=PLAN_RELEASE_ID,
        healthporta_plan_id="hpplan_01K123456789ABCDEFGHJKMNPQ",
        plan_version_id="hpversion_01K123456789ABCDEFGHJKMNPQ",
        release_month="2031-01",
        release_status="published",
        binding_set_digest="5" * 64,
        bindings=(binding,),
        _validated_serving_tables=((SNAPSHOT_ID, _tables()),),
    )


def _pin(*, address_relation_oid=1001):
    snapshot_digest = pagination.billing_search_snapshot_set_sha256(_selection())
    generation_digest = pagination._framed_sha256(
        pagination._GENERATION_BUNDLE_DOMAIN,
        pagination._canonical_json_bytes(
            {
                "address_relation_oid": address_relation_oid,
                "address_selection_contract": (
                    pagination.BILLING_ADDRESS_SELECTION_CONTRACT
                ),
                "snapshot_set_sha256": snapshot_digest,
            }
        ),
    )
    return pagination.BillingSearchGenerationPin(
        snapshot_set_sha256=snapshot_digest,
        generation_bundle_sha256=generation_digest,
        address_relation_oid=address_relation_oid,
    )


def _cursor_binding(request=None, context=None, pin=None, *, trusted_now=REQUEST_TIME):
    return pagination.build_billing_search_cursor_binding(
        request or _request(),
        context or _authorization_context(trusted_now=trusted_now),
        pin or _pin(),
        trusted_now=trusted_now,
    )


def test_cursor_auth_scope_is_stable_across_transport_timestamps(monkeypatch) -> None:
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    first_request = _request()
    first_binding = _cursor_binding(first_request)
    token = pagination.seal_billing_search_page_cursor(
        (0, 1.25, 0, SNAPSHOT_ID, 1396271656, "address-key"),
        keyring=KEYRING,
        binding=first_binding,
    )
    next_context = _authorization_context(
        issued_at="2031-01-02T03:04:55Z",
        expires_at="2031-01-02T03:05:55Z",
        trusted_now=NEXT_REQUEST_TIME,
    )
    next_request = _request(cursor=token)
    next_binding = _cursor_binding(
        next_request,
        next_context,
        trusted_now=NEXT_REQUEST_TIME,
    )

    assert first_binding.authorization_scope_sha256 == (
        next_binding.authorization_scope_sha256
    )
    assert pagination.open_billing_search_page_cursor(
        next_request,
        keyring=KEYRING,
        binding=next_binding,
    ) == (0, 1.25, 0, SNAPSHOT_ID, 1396271656, "address-key")


@pytest.mark.parametrize(
    "context_overrides",
    [
        {"principal_scope_sha256": "6" * 64},
        {"tenant_scope_sha256": "6" * 64},
        {"plan_entitlement_sha256": "6" * 64},
        {"audit_scope_sha256": "6" * 64},
        {"quota_scope_sha256": "6" * 64},
        {
            "capabilities": (
                "pricing:billing-search",
                "pricing:billing-search:provenance",
            )
        },
    ],
)
def test_cursor_rejects_cross_scope_authority(monkeypatch, context_overrides) -> None:
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    token = pagination.seal_billing_search_page_cursor(
        (1,), keyring=KEYRING, binding=_cursor_binding()
    )
    next_request = _request(cursor=token)
    next_context = _authorization_context(**context_overrides)
    next_binding = _cursor_binding(next_request, next_context)

    with pytest.raises(cursor.BillingSearchCursorError):
        pagination.open_billing_search_page_cursor(
            next_request,
            keyring=KEYRING,
            binding=next_binding,
        )


def test_cursor_reports_expired_generation_after_address_swap(monkeypatch) -> None:
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    token = pagination.seal_billing_search_page_cursor(
        (1,),
        keyring=KEYRING,
        binding=_cursor_binding(pin=_pin(address_relation_oid=10)),
    )
    next_request = _request(cursor=token)
    next_binding = _cursor_binding(next_request, pin=_pin(address_relation_oid=11))

    with pytest.raises(cursor.BillingSearchCursorGenerationExpired):
        pagination.open_billing_search_page_cursor(
            next_request,
            keyring=KEYRING,
            binding=next_binding,
        )


@pytest.mark.asyncio
async def test_generation_capture_locks_address_relation_before_reading_oid() -> None:
    session = AsyncMock()
    session.scalar.return_value = 1001

    pin = await pagination.capture_billing_search_generation_pin(
        session,
        _selection(),
    )

    assert pin.address_relation_oid == 1001
    assert len(pin.snapshot_set_sha256) == 64
    assert len(pin.generation_bundle_sha256) == 64
    lock_sql = str(session.execute.await_args.args[0])
    assert lock_sql == 'LOCK TABLE "mrf"."entity_address_unified" IN ACCESS SHARE MODE'
    assert "to_regclass" in str(session.scalar.await_args.args[0])
    assert session.scalar.await_args.args[1] == {
        "relation_name": "mrf.entity_address_unified"
    }


def test_snapshot_digest_changes_with_any_release_binding_coordinate() -> None:
    baseline = pagination.billing_search_snapshot_set_sha256(_selection())
    binding = _binding()
    changed_selection = _selection()
    changed_selection = PlanReleaseServingSelection(
        **{
            **changed_selection.__dict__,
            "binding_set_digest": "6" * 64,
            "bindings": (binding,),
        }
    )

    assert pagination.billing_search_snapshot_set_sha256(changed_selection) != baseline
