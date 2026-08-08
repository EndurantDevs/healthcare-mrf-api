# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Executable retained-row replay contract tests."""

from __future__ import annotations

from public_evidence.source_record_replay_primitives import (
    CONNECTOR_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
    CONNECTOR_FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID,
    FHIR_ORGANIZATION_RECORD_IDENTITY_CONTRACT_ID,
    FHIR_ORGANIZATION_RECORD_IDENTITY_DESCRIPTOR_SHA256,
    FHIR_ORGANIZATION_REPLAY_REF_PREFIX,
    FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_POLICY_DESCRIPTOR_SHA256,
    connector_token_policy_descriptor_sha256,
)
from process.public_evidence_fhir_organization_replay import (
    replay_fhir_organization_retained_rows,
    verify_fhir_organization_replay_result,
)
from process.tin_npi_connector_security import token_policy_descriptor_sha256
from process.tin_npi_connector_support import (
    FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID,
    TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
)
from tests.public_evidence_fhir_organization_replay_support import (
    replay_fixture,
)
from tests.tin_npi_connector_unit_support import TOKEN_POLICY_ID


def _replay(fixture):
    return replay_fhir_organization_retained_rows(
        release=fixture.release,
        inventory=fixture.inventory,
        source_vector=fixture.source_vector,
        retained_rows=fixture.retained_rows,
        token_projectors=fixture.token_projectors,
        record_identity_token_policy_id=fixture.record_policy_id,
    )


def _verify(fixture, candidate):
    return verify_fhir_organization_replay_result(
        candidate,
        release=fixture.release,
        inventory=fixture.inventory,
        source_vector=fixture.source_vector,
        retained_rows=fixture.retained_rows,
        token_projectors=fixture.token_projectors,
        record_identity_token_policy_id=fixture.record_policy_id,
    )


def test_retained_row_replay_verifies_matched_and_terminal_rows(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)

    replay_result = _replay(fixture)

    assert _verify(fixture, replay_result) == replay_result
    assert replay_result.release == fixture.release
    assert replay_result.inventory == fixture.inventory
    assert replay_result.member_count == 2
    assert replay_result.member_root_sha256 == fixture.inventory.member_root_sha256
    assert replay_result.record_kind == "fhir_organization"
    assert (
        replay_result.record_identity_contract_id
        == FHIR_ORGANIZATION_RECORD_IDENTITY_CONTRACT_ID
    )
    assert replay_result.record_identity_descriptor_sha256 == (
        FHIR_ORGANIZATION_RECORD_IDENTITY_DESCRIPTOR_SHA256
    )
    assert replay_result.scan_contract_id == (
        CONNECTOR_FHIR_ORGANIZATION_SCAN_CONTRACT_ID
    )
    assert replay_result.replay_ref.startswith(FHIR_ORGANIZATION_REPLAY_REF_PREFIX)
    assert len(replay_result.source_vector_sha256) == 64
    assert len(replay_result.dataset_fence_sha256) == 64
    assert len(replay_result.source_record_vector_sha256) == 64
    assert len(replay_result.scan_proof_sha256) == 64
    assert replay_result.replay_ref == (
        "perp1_5bl4ypr2og808M3YdfUJmlGqws1943jn7DvNGUjr_oE"
    )
    assert replay_result.contract_sha256 == (
        "c28eba9458f6383a5ce7742c4d3bfb139e8805ef428c992188e8aa3616875e41"
    )
    assert replay_result.dataset_fence_sha256 == (
        "843988b5d525977b45e6b946fbd9fb737cd9cb037fea98189ddcea20898d403e"
    )
    assert replay_result.source_record_vector_sha256 == (
        "8114e547067532eb9582e55c9b3d71f830f09b9f43574335161d6f2232456440"
    )
    assert replay_result.scan_proof_sha256 == (
        "ec5661e82d0fa88e17ebb0acf75526745d73c5074218655b3ec61020f460d977"
    )


def test_replay_result_states_only_the_checked_claims(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)

    replay_result = _replay(fixture)
    authority = replay_result.authority_state

    assert authority.lifecycle_state == (
        "verified_provided_fhir_organization_retained_row_vector_only"
    )
    assert authority.retained_payload_hashes_recomputed is True
    assert authority.record_identity_hmacs_rederived is True
    assert authority.provided_row_count_matched_dataset_fence is True
    assert authority.provided_row_identity_digest_matched_dataset_fence is True
    assert authority.provided_source_record_vector_matched_inventory is True
    assert authority.canonical_member_ordering_reconstructed is True
    assert authority.duplicate_source_record_refs_rejected is True
    assert authority.declared_inventory_root_recomputed is True
    assert authority.source_bytes_authenticated is False
    assert authority.source_authenticity_claimed is False
    assert authority.whole_source_complete is False
    assert authority.release_content_binding_verified is False
    assert authority.durable_relation_replay_verified is False
    assert authority.payload_derivation_verified is False
    assert authority.adapter_execution_authority == "none"
    assert authority.database_io_enabled is False
    assert authority.serving_authority == "none"
    assert authority.current_pointer_authority == "none"
    assert authority.publication_enabled is False
    assert authority.replacement_enabled is False
    assert authority.deletion_enabled is False
    assert authority.retirement_enabled is False
    assert authority.supersession_enabled is False
    assert fixture.release.whole_source_complete is False
    assert fixture.release.publication_enabled is False
    assert fixture.inventory.authority_state.complete_inventory_scan_verified is False
    assert fixture.inventory.authority_state.publication_enabled is False


def test_replay_is_deterministic_and_binds_the_selected_record_policy(tmp_path) -> None:
    second_policy_id = TOKEN_POLICY_ID.removesuffix("a") + "b"
    fixture = replay_fixture(
        tmp_path,
        policy_ids=(TOKEN_POLICY_ID, second_policy_id),
        record_policy_id=second_policy_id,
    )

    first = _replay(fixture)
    second = _replay(fixture)

    assert first == second
    assert first.token_policy_id == second_policy_id
    assert first.token_policy_descriptor_sha256 == token_policy_descriptor_sha256(
        second_policy_id
    )
    assert first.token_policy_descriptor_sha256 == (
        connector_token_policy_descriptor_sha256(second_policy_id)
    )


def test_public_bridge_ids_freeze_existing_connector_contracts() -> None:
    assert CONNECTOR_FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID == (
        FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID
    )
    assert CONNECTOR_FHIR_ORGANIZATION_SCAN_CONTRACT_ID == (
        TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID
    )
    assert FHIR_ORGANIZATION_RECORD_IDENTITY_CONTRACT_ID.endswith("_v1")
    assert len(FHIR_ORGANIZATION_RECORD_IDENTITY_DESCRIPTOR_SHA256) == 64
    assert len(FHIR_ORGANIZATION_RETAINED_ROW_REPLAY_POLICY_DESCRIPTOR_SHA256) == 64
