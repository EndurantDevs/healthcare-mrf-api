# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed generation and page-position coverage for billing search."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import billing_search_pagination as pagination
from api import billing_search_post_cursor_preflight as cursor_preflight
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    tax_identity_source_publication_from_metadata,
)

SNAPSHOT_ID = "ptg2:203101:synthetic"
VALID_SORT_KEY = (
    0,
    1.25,
    0,
    SNAPSHOT_ID,
    1234567893,
    "00000000-0000-4000-8000-000000000001",
    "ab" * 32,
)


def _publication():
    return tax_identity_source_publication_from_metadata(
        {
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
    )


def _pin() -> pagination.BillingSearchGenerationPin:
    return pagination.BillingSearchGenerationPin(
        snapshot_set_sha256="8" * 64,
        generation_bundle_sha256="9" * 64,
        address_relation_oid=1001,
        address_evidence_relation_oid=1002,
    )


def _binding() -> pagination.BillingSearchCursorBinding:
    return pagination.BillingSearchCursorBinding(
        request_fingerprint_sha256="6" * 64,
        authorization_scope_sha256="7" * 64,
        generation_bundle_sha256="9" * 64,
        snapshot_set_sha256="8" * 64,
        trusted_now=1_800_000_100,
    )


def test_generation_json_serialization_fails_closed() -> None:
    with pytest.raises(PTG2ManifestArtifactError) as failure:
        pagination._canonical_json_bytes(object())

    assert failure.value.__cause__ is None


def test_generation_scalar_validation_covers_optional_and_invalid_values() -> None:
    assert pagination._generation_integer(None) is None

    for operation in (
        lambda: pagination._generation_text(""),
        lambda: pagination._generation_integer(True),
    ):
        with pytest.raises(PTG2ManifestArtifactError):
            operation()


def test_generation_pin_and_cursor_binding_are_redacted_and_validated() -> None:
    pin = _pin()
    binding = _binding()

    assert repr(pin) == "<billing-search-generation-pin>"
    assert repr(binding) == "<billing-search-cursor-binding>"

    with pytest.raises(PTG2ManifestArtifactError):
        pagination.BillingSearchGenerationPin(
            snapshot_set_sha256="8" * 64,
            generation_bundle_sha256="9" * 64,
            address_relation_oid=0,
            address_evidence_relation_oid=1002,
        )
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.BillingSearchCursorBinding(
            request_fingerprint_sha256="6" * 64,
            authorization_scope_sha256="7" * 64,
            generation_bundle_sha256="9" * 64,
            snapshot_set_sha256="8" * 64,
            trusted_now=True,
        )


def test_source_publication_payload_sanitizes_projection_failure(
    monkeypatch,
) -> None:
    publication = _publication()
    serving_tables = SimpleNamespace(
        provider_tax_identity_source_publication=publication,
        source_count=1,
    )

    def fail_projection(_metadata):
        raise RuntimeError("synthetic-source-detail")

    monkeypatch.setattr(
        pagination,
        "tax_identity_source_publication_from_metadata",
        fail_projection,
    )
    with pytest.raises(PTG2ManifestArtifactError) as failure:
        pagination._source_publication_payload(serving_tables)

    assert failure.value.__cause__ is None
    assert "synthetic-source-detail" not in str(failure.value)


def test_source_publication_payload_rejects_noncanonical_reprojection(
    monkeypatch,
) -> None:
    publication = _publication()
    serving_tables = SimpleNamespace(
        provider_tax_identity_source_publication=publication,
        source_count=1,
    )
    monkeypatch.setattr(
        pagination,
        "tax_identity_source_publication_from_metadata",
        lambda _metadata: replace(publication, content_digest="a" * 64),
    )

    with pytest.raises(PTG2ManifestArtifactError):
        pagination._source_publication_payload(serving_tables)


def test_binding_generation_rejects_missing_and_unready_serving_tables(
    monkeypatch,
) -> None:
    missing_tables = SimpleNamespace(
        network_tables_by_snapshot=lambda: None,
        in_network_bindings=(),
    )
    with pytest.raises(PTG2ManifestArtifactError):
        pagination._binding_generation_payload(missing_tables)

    binding = SimpleNamespace(
        snapshot_id=SNAPSHOT_ID,
        binding_ordinal=0,
        required=True,
    )
    serving_tables = SimpleNamespace(
        snapshot_id=SNAPSHOT_ID,
        uses_v4_graph=True,
    )
    unready_selection = SimpleNamespace(
        network_tables_by_snapshot=lambda: {SNAPSHOT_ID: serving_tables},
        in_network_bindings=(binding,),
    )
    monkeypatch.setattr(
        pagination,
        "is_release_binding_serving_scope_exact",
        lambda _tables, _binding: False,
    )
    with pytest.raises(PTG2ManifestArtifactError):
        pagination._binding_generation_payload(unready_selection)


def test_snapshot_and_schema_validation_reject_wrong_types(
    monkeypatch,
) -> None:
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.billing_search_snapshot_set_sha256(object())

    monkeypatch.setattr(pagination.ptg2_serving, "PTG2_SCHEMA", "bad-schema")
    with pytest.raises(PTG2ManifestArtifactError):
        pagination._quoted_address_relations()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "relation_oids",
    [
        None,
        (1001,),
        (1001, 0),
    ],
)
async def test_address_relation_lock_rejects_incomplete_oid_bundle(
    relation_oids,
) -> None:
    session = SimpleNamespace(
        execute=AsyncMock(),
        scalar=AsyncMock(return_value=relation_oids),
    )

    with pytest.raises(PTG2ManifestArtifactError):
        await pagination._locked_address_relation_oids(session)

    session.execute.assert_awaited_once()
    session.scalar.assert_awaited_once()


def test_cursor_binding_builder_rejects_nonpin_value() -> None:
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.build_billing_search_cursor_binding(
            "6" * 64,
            object(),
            object(),
            trusted_now="2031-01-02T03:04:05Z",
        )


def test_page_sort_key_rejects_malformed_and_noncanonical_uuid() -> None:
    malformed_uuid = (*VALID_SORT_KEY[:5], "bad", VALID_SORT_KEY[6])
    uppercase_uuid = (
        *VALID_SORT_KEY[:5],
        "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA",
        VALID_SORT_KEY[6],
    )

    for candidate in (malformed_uuid, uppercase_uuid):
        with pytest.raises(PTG2ManifestArtifactError):
            pagination.validate_billing_search_page_sort_key(candidate)


def test_page_cursor_open_and_seal_reject_wrong_binding_type() -> None:
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.open_billing_search_page_cursor(
            None,
            keyring=object(),
            binding=object(),
        )
    with pytest.raises(PTG2ManifestArtifactError):
        pagination.seal_billing_search_page_cursor(
            VALID_SORT_KEY,
            keyring=object(),
            binding=object(),
        )


def test_empty_post_page_context_is_redacted() -> None:
    context = cursor_preflight.empty_billing_search_post_page_context()

    assert repr(context) == "<billing-search-post-page-context>"
    assert context.generation_pin is None
    assert context.cursor_binding is None
    assert context.after_sort_key is None
    assert context.chain_keyring is None
