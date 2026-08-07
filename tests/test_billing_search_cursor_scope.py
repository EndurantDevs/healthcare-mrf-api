# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stable keyed-selector scope tests for billing-search cursors."""

from __future__ import annotations

import pytest

from api import billing_search_cursor as cursor
from api import billing_search_cursor_scope as cursor_scope
from api import billing_search_pagination as pagination
from api.billing_search_access_contract import (
    build_billing_search_authorization_context,
)
from api.billing_search_post_request import parse_billing_search_post_request

PLAN_RELEASE_ID = "hprelease_01K123456789ABCDEFGHJKMNPQ"
HEALTHPORTA_PLAN_ID = "hpplan_01K123456789ABCDEFGHJKMNPQ"
REQUEST_TIME = "2031-01-02T03:04:05Z"
SORT_KEY = (
    0,
    1.25,
    0,
    "ptg2:203101:synthetic",
    1234567893,
    "00000000-0000-4000-8000-000000000001",
    "ab" * 32,
)


def _request(*, identity_value="123456789", cursor_token=None):
    return parse_billing_search_post_request(
        {
            "healthporta_plan_id": HEALTHPORTA_PLAN_ID,
            "billing_identity": {
                "tax_identity": {"type": "ein", "value": identity_value}
            },
            "procedure": {
                "code_system": "CPT",
                "code": "00000",
                "modifiers": [],
                "place_of_service": [],
            },
            "geo": {"zip5": "00000", "radius_miles": 0},
            "page": {"limit": 25, "cursor": cursor_token},
        }
    )


def _authorization_context():
    return build_billing_search_authorization_context(
        {
            "principal_scope_sha256": "1" * 64,
            "tenant_scope_sha256": "2" * 64,
            "plan_entitlement_sha256": "3" * 64,
            "audit_scope_sha256": "4" * 64,
            "quota_scope_sha256": "5" * 64,
            "capabilities": ("pricing:billing-search",),
            "issued_at": "2031-01-02T03:03:55Z",
            "expires_at": "2031-01-02T03:04:55Z",
        },
        trusted_now=REQUEST_TIME,
    )


def _binding(fingerprint):
    pin = pagination.BillingSearchGenerationPin(
        snapshot_set_sha256="8" * 64,
        generation_bundle_sha256="9" * 64,
        address_relation_oid=1001,
        address_evidence_relation_oid=1002,
    )
    return pagination.build_billing_search_cursor_binding(
        fingerprint,
        _authorization_context(),
        pin,
        trusted_now=REQUEST_TIME,
    )


def _cursor_rotation_case(monkeypatch):
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    first_keyring = cursor.BillingSearchCursorKeyring(
        active_key_id="cursor-v1",
        keys_by_id={"cursor-v1": b"a" * 32},
    )
    rotated_keyring = cursor.BillingSearchCursorKeyring(
        active_key_id="cursor-v2",
        keys_by_id={
            "cursor-v1": b"a" * 32,
            "cursor-v2": b"b" * 32,
        },
    )
    first_request = _request()
    first_chain = cursor_scope.select_billing_search_cursor_chain_keyring(
        None,
        keyring=first_keyring,
    )
    first_fingerprint = cursor_scope.billing_search_stable_request_fingerprint(
        first_request,
        plan_release_id=PLAN_RELEASE_ID,
        chain_keyring=first_chain,
    )
    first_binding = _binding(first_fingerprint)
    sealed_cursor = pagination.seal_billing_search_page_cursor(
        SORT_KEY,
        keyring=first_chain,
        binding=first_binding,
    )
    return first_request, first_fingerprint, sealed_cursor, rotated_keyring


def test_raw_tin_fingerprint_uses_cursor_chain_key_across_rotation(
    monkeypatch,
) -> None:
    """A continuation keeps its retained keyed HMAC scope across rotation."""

    first_request, first_fingerprint, sealed_cursor, rotated_keyring = (
        _cursor_rotation_case(monkeypatch)
    )

    continuation_request = _request(cursor_token=sealed_cursor.token)
    continuation_chain = cursor_scope.select_billing_search_cursor_chain_keyring(
        sealed_cursor.token,
        keyring=rotated_keyring,
    )
    continuation_fingerprint = cursor_scope.billing_search_stable_request_fingerprint(
        continuation_request,
        plan_release_id=PLAN_RELEASE_ID,
        chain_keyring=continuation_chain,
    )
    continuation_binding = _binding(continuation_fingerprint)

    assert continuation_chain.active_key_id == "cursor-v1"
    assert continuation_fingerprint == first_fingerprint
    assert (
        pagination.open_billing_search_page_cursor(
            sealed_cursor.token,
            keyring=continuation_chain,
            binding=continuation_binding,
        )
        == SORT_KEY
    )
    resealed = pagination.seal_billing_search_page_cursor(
        SORT_KEY,
        keyring=continuation_chain,
        binding=continuation_binding,
    )
    assert resealed.token.startswith("bsc1_cursor-v1_")


def test_cursor_chain_scope_rejects_selector_change_and_uses_new_active_key(
    monkeypatch,
) -> None:
    first_request, first_fingerprint, sealed_cursor, rotated_keyring = (
        _cursor_rotation_case(monkeypatch)
    )
    continuation_chain = cursor_scope.select_billing_search_cursor_chain_keyring(
        sealed_cursor.token,
        keyring=rotated_keyring,
    )

    changed_fingerprint = cursor_scope.billing_search_stable_request_fingerprint(
        _request(identity_value="123456788", cursor_token=sealed_cursor.token),
        plan_release_id=PLAN_RELEASE_ID,
        chain_keyring=continuation_chain,
    )
    with pytest.raises(cursor.BillingSearchCursorError):
        pagination.open_billing_search_page_cursor(
            sealed_cursor.token,
            keyring=continuation_chain,
            binding=_binding(changed_fingerprint),
        )

    active_chain = cursor_scope.select_billing_search_cursor_chain_keyring(
        None,
        keyring=rotated_keyring,
    )
    active_fingerprint = cursor_scope.billing_search_stable_request_fingerprint(
        first_request,
        plan_release_id=PLAN_RELEASE_ID,
        chain_keyring=active_chain,
    )
    assert active_chain.active_key_id == "cursor-v2"
    assert active_fingerprint != first_fingerprint


def test_stable_fingerprint_binds_plan_release_lineage() -> None:
    request = _request()
    keyring = cursor.BillingSearchCursorKeyring(
        active_key_id="cursor-v1",
        keys_by_id={"cursor-v1": b"a" * 32},
    )
    chain_keyring = cursor_scope.select_billing_search_cursor_chain_keyring(
        None,
        keyring=keyring,
    )
    baseline = cursor_scope.billing_search_stable_request_fingerprint(
        request,
        plan_release_id=PLAN_RELEASE_ID,
        chain_keyring=chain_keyring,
    )
    changed = cursor_scope.billing_search_stable_request_fingerprint(
        request,
        plan_release_id="hprelease_01K123456789ABCDEFGHJKMNPR",
        chain_keyring=chain_keyring,
    )

    assert changed != baseline
