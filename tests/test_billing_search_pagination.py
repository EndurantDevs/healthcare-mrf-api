# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generation-bound billing-search pagination tests."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import billing_search_cursor as cursor
from api import billing_search_pagination as pagination
from api.billing_search_access_contract import (
    build_billing_search_authorization_context,
)
from api.billing_search_cursor_authentication import (
    authenticate_billing_search_sealed_page_cursor,
)
from api.billing_search_request import parse_billing_search_request
from api.plan_release_serving import (
    PlanReleaseServingSelection,
    PlanReleaseSnapshotBinding,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.billing_search_entity_ref_support import (
    serving_tables as source_serving_tables,
    source_pinned_selection,
    source_publication,
)
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


def _pin(*, address_relation_oid=1001, address_evidence_relation_oid=1002):
    snapshot_digest = pagination.billing_search_snapshot_set_sha256(_selection())
    generation_digest = pagination._framed_sha256(
        pagination._GENERATION_BUNDLE_DOMAIN,
        pagination._canonical_json_bytes(
            {
                "address_relation_oid": address_relation_oid,
                "address_evidence_relation_oid": address_evidence_relation_oid,
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
        address_evidence_relation_oid=address_evidence_relation_oid,
    )


def _cursor_binding(request=None, context=None, pin=None, *, trusted_now=REQUEST_TIME):
    return pagination.build_billing_search_cursor_binding(
        request or _request(),
        context or _authorization_context(trusted_now=trusted_now),
        pin or _pin(),
        trusted_now=trusted_now,
    )


def _wire_token(sealed_cursor, binding):
    _, token = authenticate_billing_search_sealed_page_cursor(
        sealed_cursor,
        keyring=KEYRING,
        trusted_now=binding.trusted_now,
        request_fingerprint_sha256=binding.request_fingerprint_sha256,
        authorization_context_sha256=binding.authorization_scope_sha256,
        generation_bundle_sha256=binding.generation_bundle_sha256,
        snapshot_set_sha256=binding.snapshot_set_sha256,
    )
    return token


def test_cursor_auth_scope_is_stable_across_transport_timestamps(monkeypatch) -> None:
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    first_request = _request()
    first_binding = _cursor_binding(first_request)
    sealed_cursor = pagination.seal_billing_search_page_cursor(
        (0, 1.25, 0, SNAPSHOT_ID, 1396271656, "address-key"),
        keyring=KEYRING,
        binding=first_binding,
    )
    assert type(sealed_cursor) is cursor.BillingSearchSealedPageCursor
    next_context = _authorization_context(
        issued_at="2031-01-02T03:04:55Z",
        expires_at="2031-01-02T03:05:55Z",
        trusted_now=NEXT_REQUEST_TIME,
    )
    next_request = _request(cursor=_wire_token(sealed_cursor, first_binding))
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
    first_binding = _cursor_binding()
    sealed_cursor = pagination.seal_billing_search_page_cursor(
        (1,), keyring=KEYRING, binding=first_binding
    )
    next_request = _request(cursor=_wire_token(sealed_cursor, first_binding))
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
    first_binding = _cursor_binding(pin=_pin(address_relation_oid=10))
    sealed_cursor = pagination.seal_billing_search_page_cursor(
        (1,),
        keyring=KEYRING,
        binding=first_binding,
    )
    next_request = _request(cursor=_wire_token(sealed_cursor, first_binding))
    next_binding = _cursor_binding(next_request, pin=_pin(address_relation_oid=11))

    with pytest.raises(cursor.BillingSearchCursorGenerationExpired):
        pagination.open_billing_search_page_cursor(
            next_request,
            keyring=KEYRING,
            binding=next_binding,
        )


def test_cursor_reports_expired_generation_after_evidence_swap(monkeypatch) -> None:
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    first_binding = _cursor_binding(pin=_pin(address_evidence_relation_oid=10))
    sealed_cursor = pagination.seal_billing_search_page_cursor(
        (1,),
        keyring=KEYRING,
        binding=first_binding,
    )
    next_request = _request(cursor=_wire_token(sealed_cursor, first_binding))
    next_binding = _cursor_binding(
        next_request,
        pin=_pin(address_evidence_relation_oid=11),
    )

    with pytest.raises(cursor.BillingSearchCursorGenerationExpired):
        pagination.open_billing_search_page_cursor(
            next_request,
            keyring=KEYRING,
            binding=next_binding,
        )


@pytest.mark.asyncio
async def test_generation_capture_locks_address_bundle_before_reading_oids() -> None:
    session = AsyncMock()

    async def relation_oids_after_lock(*_args, **_kwargs):
        assert session.execute.await_count == 1
        return 1001, 1002

    session.scalar.side_effect = relation_oids_after_lock

    pin = await pagination.capture_billing_search_generation_pin(
        session,
        _selection(),
    )

    assert pin.address_relation_oid == 1001
    assert pin.address_evidence_relation_oid == 1002
    assert len(pin.snapshot_set_sha256) == 64
    assert len(pin.generation_bundle_sha256) == 64
    lock_sql = str(session.execute.await_args.args[0])
    assert lock_sql == (
        'LOCK TABLE "mrf"."entity_address_evidence", '
        '"mrf"."entity_address_unified" IN ACCESS SHARE MODE'
    )
    assert "to_regclass" in str(session.scalar.await_args.args[0])
    assert session.scalar.await_args.args[1] == {
        "address_relation_name": "mrf.entity_address_unified",
        "evidence_relation_name": "mrf.entity_address_evidence",
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


def test_generation_contract_rejects_bad_digests_oids_and_timestamps() -> None:
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.BillingSearchGenerationPin(
            snapshot_set_sha256="0" * 64,
            generation_bundle_sha256="1" * 64,
            address_relation_oid=1,
            address_evidence_relation_oid=2,
        )
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.BillingSearchGenerationPin(
            snapshot_set_sha256="1" * 64,
            generation_bundle_sha256="2" * 64,
            address_relation_oid=0,
            address_evidence_relation_oid=2,
        )
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.BillingSearchCursorBinding(
            request_fingerprint_sha256="1" * 64,
            authorization_scope_sha256="2" * 64,
            generation_bundle_sha256="3" * 64,
            snapshot_set_sha256="4" * 64,
            trusted_now=-1,
        )

    missing_binding = object.__new__(pagination.BillingSearchCursorBinding)
    with pytest.raises(PTG2ManifestArtifactError):
        missing_binding.__post_init__()

    assert repr(_pin()) == "<billing-search-generation-pin>"
    assert repr(_cursor_binding()) == "<billing-search-cursor-binding>"


def test_generation_metadata_is_optional_but_strictly_bounded() -> None:
    assert pagination._optional_generation_text(None) is None
    with pytest.raises(PTG2ManifestArtifactError):
        pagination._optional_generation_text("\n")


def test_snapshot_digest_requires_complete_v4_release_bindings() -> None:
    missing_tables = replace(_selection(), _validated_serving_tables=())
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.billing_search_snapshot_set_sha256(missing_tables)

    non_v4_tables = replace(_tables(), storage_generation="shared_blocks_v3")
    non_v4_selection = replace(
        _selection(),
        _validated_serving_tables=((SNAPSHOT_ID, non_v4_tables),),
    )
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.billing_search_snapshot_set_sha256(non_v4_selection)

    with pytest.raises(PTG2ManifestArtifactError):
        pagination.billing_search_snapshot_set_sha256(object())


def test_snapshot_digest_pins_source_publication_and_rejects_corruption() -> None:
    source_aware_selection = source_pinned_selection()
    source_aware_digest = pagination.billing_search_snapshot_set_sha256(
        source_aware_selection
    )
    assert len(source_aware_digest) == 64

    mismatched_publication = source_publication(source_count=1)
    corrupt_tables = source_serving_tables(publication=mismatched_publication)
    corrupt_selection = source_pinned_selection(tables=corrupt_tables)
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.billing_search_snapshot_set_sha256(corrupt_selection)


def test_address_relation_names_reject_untrusted_schema(monkeypatch) -> None:
    monkeypatch.setattr(pagination.ptg2_serving, "PTG2_SCHEMA", "bad-schema")
    with pytest.raises(PTG2ManifestArtifactError):
        pagination._quoted_address_relations()


@pytest.mark.asyncio
async def test_generation_capture_rejects_missing_relation_oids() -> None:
    session = AsyncMock()
    session.scalar.return_value = (1001, None)

    with pytest.raises(PTG2ManifestArtifactError):
        await pagination.capture_billing_search_generation_pin(
            session,
            _selection(),
        )


def test_page_cursor_helpers_reject_untyped_generation_inputs() -> None:
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.build_billing_search_cursor_binding(
            _request(),
            _authorization_context(),
            object(),
            trusted_now=REQUEST_TIME,
        )
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.open_billing_search_page_cursor(
            _request(),
            keyring=KEYRING,
            binding=object(),
        )
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.seal_billing_search_page_cursor(
            (1,),
            keyring=KEYRING,
            binding=object(),
        )


def test_page_cursor_open_returns_none_without_client_cursor() -> None:
    assert (
        pagination.open_billing_search_page_cursor(
            _request(),
            keyring=KEYRING,
            binding=_cursor_binding(),
        )
        is None
    )
