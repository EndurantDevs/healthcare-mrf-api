# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generation-bound POST billing-search pagination tests."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from api import billing_search_cursor as cursor
from api import billing_search_pagination as pagination
from api.billing_search_access_contract import (
    build_billing_search_authorization_context,
)
from api.plan_release_serving import (
    PlanReleaseServingSelection,
    PlanReleaseSnapshotBinding,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    tax_identity_source_publication_from_metadata,
)

SNAPSHOT_ID = "ptg2:203101:synthetic"
REQUEST_TIME = "2031-01-02T03:04:05Z"
NEXT_REQUEST_TIME = "2031-01-02T03:05:05Z"
REQUEST_FINGERPRINT = "6" * 64
KEYRING = cursor.BillingSearchCursorKeyring(
    active_key_id="cursor-v1",
    keys_by_id={"cursor-v1": b"c" * 32},
)
SORT_KEY = (
    0,
    1.25,
    0,
    SNAPSHOT_ID,
    1234567893,
    "00000000-0000-4000-8000-000000000001",
    "ab" * 32,
)


def _publication(**overrides):
    metadata_by_field = {
        "contract": "ptg2_provider_group_tax_identity_source_v1",
        "content_contract": "ptg2_provider_group_tax_identity_source_content_v1",
        "binding_contract": "ptg2_tax_identity_rate_source_binding_v1",
        "binding_vector_contract": "ptg2_tax_identity_source_binding_vector_v1",
        "token_policy_id": "ptg-tin-hmac-sha256-v1:test",
        "token_policy_descriptor_sha256": "1" * 64,
        "source_ordinal_map_digest": "2" * 64,
        "source_count": 1,
        "provider_group_occurrence_count": 2,
        "matched_ein_count": 1,
        "missing_count": 1,
        "malformed_count": 0,
        "unsupported_type_count": 0,
        "content_digest": "3" * 64,
        "artifact_byte_count": 143,
        "binding_vector_digest": "4" * 64,
    }
    metadata_by_field.update(overrides)
    return tax_identity_source_publication_from_metadata(metadata_by_field)


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
        "plan_entitlement_sha256": "3" * 64,
        "audit_scope_sha256": "4" * 64,
        "quota_scope_sha256": "5" * 64,
        "capabilities": ("pricing:billing-search",),
        "issued_at": issued_at,
        "expires_at": expires_at,
    }
    claims_by_name.update(overrides)
    return build_billing_search_authorization_context(
        claims_by_name,
        trusted_now=trusted_now,
    )


def _snapshot_binding() -> PlanReleaseSnapshotBinding:
    return PlanReleaseSnapshotBinding(
        binding_ordinal=0,
        snapshot_id=SNAPSHOT_ID,
        source_key="synthetic-source",
        plan_id="synthetic-plan",
        plan_market_type="group",
        role="in_network",
        required=True,
    )


def _tables(*, publication=...) -> PTG2ServingTables:
    if publication is ...:
        publication = _publication()
    return PTG2ServingTables(
        snapshot_id=SNAPSHOT_ID,
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=1,
        code_count=100,
        price_dictionary_item_count=20,
        price_dictionary_block_bytes=1024,
        provider_shard_span=1024,
        atom_key_bits=16,
        price_key_block_span=128,
        atom_key_block_span=128,
        coverage_scope_id="coverage-synthetic",
        plan_id="synthetic-plan",
        plan_market_type="group",
        source_key="synthetic-source",
        source_trace_set_hash="trace-synthetic",
        provider_tax_identity_source_publication=publication,
    )


def _selection(*, tables=None) -> PlanReleaseServingSelection:
    binding = _snapshot_binding()
    return PlanReleaseServingSelection(
        serving_revision_id="hpserve_01K123456789ABCDEFGHJKMNPQ",
        plan_release_id="hprelease_01K123456789ABCDEFGHJKMNPQ",
        healthporta_plan_id="hpplan_01K123456789ABCDEFGHJKMNPQ",
        plan_version_id="hpversion_01K123456789ABCDEFGHJKMNPQ",
        release_month="2031-01",
        release_status="published",
        binding_set_digest="7" * 64,
        bindings=(binding,),
        _validated_serving_tables=((SNAPSHOT_ID, tables or _tables()),),
    )


def _pin(*, snapshot_digest="8" * 64, generation_digest="9" * 64):
    return pagination.BillingSearchGenerationPin(
        snapshot_set_sha256=snapshot_digest,
        generation_bundle_sha256=generation_digest,
        address_relation_oid=1001,
        address_evidence_relation_oid=1002,
    )


def _cursor_binding(
    *,
    fingerprint=REQUEST_FINGERPRINT,
    context=None,
    pin=None,
    trusted_now=REQUEST_TIME,
):
    return pagination.build_billing_search_cursor_binding(
        fingerprint,
        context or _authorization_context(trusted_now=trusted_now),
        pin or _pin(),
        trusted_now=trusted_now,
    )


def test_cursor_binds_explicit_canonical_post_fingerprint(monkeypatch) -> None:
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    first_binding = _cursor_binding()
    sealed_cursor = pagination.seal_billing_search_page_cursor(
        SORT_KEY,
        keyring=KEYRING,
        binding=first_binding,
    )
    next_context = _authorization_context(
        issued_at="2031-01-02T03:04:55Z",
        expires_at="2031-01-02T03:05:55Z",
        trusted_now=NEXT_REQUEST_TIME,
    )
    next_binding = _cursor_binding(
        context=next_context,
        trusted_now=NEXT_REQUEST_TIME,
    )

    assert first_binding.authorization_scope_sha256 == (
        next_binding.authorization_scope_sha256
    )
    assert (
        pagination.open_billing_search_page_cursor(
            sealed_cursor.token,
            keyring=KEYRING,
            binding=next_binding,
        )
        == SORT_KEY
    )
    assert (
        pagination.open_billing_search_page_cursor(
            None,
            keyring=KEYRING,
            binding=next_binding,
        )
        is None
    )


def test_cursor_rejects_changed_post_body_fingerprint(monkeypatch) -> None:
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    sealed_cursor = pagination.seal_billing_search_page_cursor(
        SORT_KEY,
        keyring=KEYRING,
        binding=_cursor_binding(),
    )

    with pytest.raises(cursor.BillingSearchCursorError):
        pagination.open_billing_search_page_cursor(
            sealed_cursor.token,
            keyring=KEYRING,
            binding=_cursor_binding(fingerprint="a" * 64),
        )


@pytest.mark.parametrize(
    "context_overrides",
    [
        {"principal_scope_sha256": "a" * 64},
        {"tenant_scope_sha256": "a" * 64},
        {"plan_entitlement_sha256": "a" * 64},
        {"audit_scope_sha256": "a" * 64},
        {"quota_scope_sha256": "a" * 64},
        {
            "capabilities": (
                "pricing:billing-search",
                "pricing:billing-search:provenance",
            )
        },
    ],
)
def test_cursor_rejects_cross_scope_authority(
    monkeypatch,
    context_overrides,
) -> None:
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    sealed_cursor = pagination.seal_billing_search_page_cursor(
        SORT_KEY,
        keyring=KEYRING,
        binding=_cursor_binding(),
    )
    changed_context = _authorization_context(**context_overrides)

    with pytest.raises(cursor.BillingSearchCursorError):
        pagination.open_billing_search_page_cursor(
            sealed_cursor.token,
            keyring=KEYRING,
            binding=_cursor_binding(context=changed_context),
        )


def test_cursor_reports_expired_generation_after_bundle_change(monkeypatch) -> None:
    monkeypatch.setattr(cursor.secrets, "token_bytes", lambda size: b"n" * size)
    sealed_cursor = pagination.seal_billing_search_page_cursor(
        SORT_KEY,
        keyring=KEYRING,
        binding=_cursor_binding(),
    )

    with pytest.raises(cursor.BillingSearchCursorGenerationExpired):
        pagination.open_billing_search_page_cursor(
            sealed_cursor.token,
            keyring=KEYRING,
            binding=_cursor_binding(
                pin=_pin(generation_digest="a" * 64),
            ),
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
    assert str(session.execute.await_args.args[0]) == (
        'LOCK TABLE "mrf"."entity_address_evidence", '
        '"mrf"."entity_address_unified" IN ACCESS SHARE MODE'
    )
    assert "to_regclass" in str(session.scalar.await_args.args[0])
    assert session.scalar.await_args.args[1] == {
        "address_relation_name": "mrf.entity_address_unified",
        "evidence_relation_name": "mrf.entity_address_evidence",
    }


def test_snapshot_digest_pins_complete_tax_identity_source_publication() -> None:
    baseline = pagination.billing_search_snapshot_set_sha256(_selection())
    changed_publication = _publication(content_digest="a" * 64)
    changed = pagination.billing_search_snapshot_set_sha256(
        _selection(tables=_tables(publication=changed_publication))
    )

    assert changed != baseline


def test_snapshot_digest_fails_closed_without_tax_identity_publication() -> None:
    selection = _selection(tables=_tables(publication=None))

    with pytest.raises(PTG2ManifestArtifactError):
        pagination.billing_search_snapshot_set_sha256(selection)


def test_snapshot_digest_changes_with_release_binding_coordinates() -> None:
    selection = _selection()
    baseline = pagination.billing_search_snapshot_set_sha256(selection)
    changed = replace(selection, binding_set_digest="a" * 64)

    assert pagination.billing_search_snapshot_set_sha256(changed) != baseline


def test_page_sort_key_stably_breaks_equal_distance_ties() -> None:
    first = (
        0,
        1.25,
        0,
        SNAPSHOT_ID,
        1234567802,
        "00000000-0000-4000-8000-000000000001",
        "ab" * 32,
    )
    second = SORT_KEY

    assert pagination.validate_billing_search_page_sort_key(first) == first
    assert pagination.validate_billing_search_page_sort_key(second) == second
    assert first < second
    assert len(first) == 7


@pytest.mark.parametrize(
    "invalid_sort_key",
    [
        (),
        (0, 1.0, 0, SNAPSHOT_ID, 1234567893, "bad", "ab" * 32),
        (0, float("nan"), 0, SNAPSHOT_ID, 1234567893, SORT_KEY[5], "ab" * 32),
        (1, 1.0, 0, SNAPSHOT_ID, 1234567893, SORT_KEY[5], "ab" * 32),
        (0, 1.0, -1, SNAPSHOT_ID, 1234567893, SORT_KEY[5], "ab" * 32),
        (0, 1.0, 0, SNAPSHOT_ID, 1234567890, SORT_KEY[5], "ab" * 32),
        (0, 1.0, 0, SNAPSHOT_ID, 1234567893, SORT_KEY[5], "AB" * 32),
    ],
)
def test_page_sort_key_rejects_unstable_or_noncanonical_values(
    invalid_sort_key,
) -> None:
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.validate_billing_search_page_sort_key(invalid_sort_key)


def test_binding_rejects_noncanonical_fingerprint() -> None:
    for invalid_fingerprint in ("0" * 64, "A" * 64, "short"):
        with pytest.raises(PTG2ManifestArtifactError):
            _cursor_binding(fingerprint=invalid_fingerprint)
