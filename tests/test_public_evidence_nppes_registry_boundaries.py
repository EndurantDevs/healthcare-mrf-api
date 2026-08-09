# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Boundary and lifecycle proof for the bounded NPPES registry contract."""

from __future__ import annotations

from dataclasses import replace

import pytest

from public_evidence.nppes_registry_candidate_encoder import (
    NppesRegistryCandidateEncoder,
)
from public_evidence.nppes_registry_merkle import (
    NppesEvidenceRootAccumulator,
    derive_nppes_tree_node,
)
from public_evidence.nppes_registry_primitives import (
    NPPES_REGISTRY_IDENTITY_CONTRACT,
    NppesRegistryReplayError,
    build_nppes_archive_identity,
    compile_nppes_registry_header,
    nppes_manifest_sha256,
    nppes_header_sha256,
    scan_nppes_registry_row,
    validate_nppes_archive_identity,
    validate_nppes_header,
)
from public_evidence.nppes_registry_projection import (
    NppesRegistryPersistenceProjector,
)
from public_evidence.nppes_registry_replay_contract import (
    build_nppes_manifest_from_rows,
)
from tests.public_evidence_nppes_registry_support import (
    HEADER,
    active_type_1_row,
    active_type_2_row,
    archive_identity,
)


def _identity_values(**overrides):
    identity = archive_identity()
    identity_values_by_name = {
        "source_url": identity.source_url,
        "archive_name": identity.archive_name,
        "primary_member_name": identity.primary_member_name,
        "artifact_sha256": identity.artifact_sha256,
        "artifact_byte_count": identity.artifact_byte_count,
        "rights_proof_sha256": identity.rights_proof_sha256,
        "record_identity_contract_id": identity.record_identity_contract_id,
        **overrides,
    }
    return identity_values_by_name


@pytest.mark.parametrize(
    "overrides",
    (
        {"source_url": object()},
        {"primary_member_name": object()},
        {"primary_member_name": "not-a-primary-member.csv"},
        {"primary_member_name": "npidata_pfile_20261301-20260712.csv"},
        {"primary_member_name": "npidata_pfile_20260713-20260712.csv"},
        {"record_identity_contract_id": "forged-contract"},
    ),
)
def test_archive_identity_rejects_type_date_and_contract_boundaries(overrides):
    with pytest.raises(NppesRegistryReplayError):
        build_nppes_archive_identity(**_identity_values(**overrides))


def test_archive_identity_validator_rebuilds_and_rejects_forgery():
    identity = archive_identity()
    with pytest.raises(NppesRegistryReplayError):
        validate_nppes_archive_identity(object())
    with pytest.raises(NppesRegistryReplayError):
        validate_nppes_archive_identity(
            replace(identity, snapshot_at="2026-07-13T00:00:00Z")
        )


@pytest.mark.parametrize(
    "header",
    (
        object(),
        (),
        (1, *HEADER[1:]),
        ("", *HEADER[1:]),
        ("\ud800", *HEADER[1:]),
        ("x" * 1025, *HEADER[1:]),
        (*HEADER, HEADER[0]),
    ),
)
def test_header_validation_rejects_container_field_and_encoding_boundaries(header):
    with pytest.raises(NppesRegistryReplayError):
        validate_nppes_header(header)
    with pytest.raises(NppesRegistryReplayError):
        compile_nppes_registry_header(header)


def test_compiled_header_fast_path_is_exact_and_value_safe():
    compiled = compile_nppes_registry_header(HEADER)
    assert repr(compiled) == "<nppes-registry-header>"
    assert nppes_header_sha256(compiled) == compiled.sha256


@pytest.mark.parametrize(
    ("row_values", "ordinal"),
    (
        ((1, *active_type_1_row()[1:]), 1),
        (("\ud800", *active_type_1_row()[1:]), 1),
        (("x" * 1025, *active_type_1_row()[1:]), 1),
        (active_type_1_row(), True),
        (("bad", *active_type_1_row()[1:]), 1),
        (("0000000000", *active_type_1_row()[1:]), 1),
        (("1003000100", "1", "05/23/2005", "05/01/2026", "", "06/15/2026"), 1),
    ),
)
def test_row_scan_rejects_value_npi_and_ordinal_boundaries(row_values, ordinal):
    with pytest.raises(NppesRegistryReplayError):
        scan_nppes_registry_row(archive_identity(), HEADER, row_values, ordinal)


def test_candidate_encoder_enforces_single_complete_lifecycle():
    rows = (active_type_1_row(), active_type_2_row())
    manifest = build_nppes_manifest_from_rows(archive_identity(), HEADER, rows)
    with pytest.raises(NppesRegistryReplayError):
        NppesRegistryCandidateEncoder(manifest, HEADER + ("wrong",))

    encoder = NppesRegistryCandidateEncoder(manifest, HEADER)
    assert repr(encoder.add(active_type_1_row())) == "<nppes-registry-persistence-rows>"
    with pytest.raises(NppesRegistryReplayError):
        encoder.finish()

    encoder = NppesRegistryCandidateEncoder(manifest, HEADER)
    for row_values in rows:
        encoder.add(row_values)
    assert encoder.finish() == manifest
    with pytest.raises(NppesRegistryReplayError):
        encoder.add(active_type_1_row())
    with pytest.raises(NppesRegistryReplayError):
        encoder.finish()


def test_projector_enforces_single_complete_lifecycle():
    rows = (active_type_1_row(),)
    manifest = build_nppes_manifest_from_rows(archive_identity(), HEADER, rows)
    projector = NppesRegistryPersistenceProjector(manifest, HEADER)
    projection = projector.add(rows[0])
    assert projection.candidate is not None
    assert repr(projection) == "<nppes-registry-persistence-projection>"
    assert projector.finish() == manifest
    with pytest.raises(NppesRegistryReplayError):
        projector.add(rows[0])
    with pytest.raises(NppesRegistryReplayError):
        projector.finish()

    invalid_projector = NppesRegistryPersistenceProjector(manifest, HEADER)
    with pytest.raises(NppesRegistryReplayError):
        invalid_projector.add(("bad", *rows[0][1:]))


def test_merkle_accumulator_rejects_invalid_empty_and_reused_state():
    with pytest.raises(NppesRegistryReplayError):
        derive_nppes_tree_node("invalid", "00" * 32)
    empty = NppesEvidenceRootAccumulator()
    with pytest.raises(NppesRegistryReplayError):
        empty.finish()
    accumulator = NppesEvidenceRootAccumulator()
    accumulator.add("00" * 32)
    assert accumulator.finish() == "00" * 32
    with pytest.raises(NppesRegistryReplayError):
        accumulator.add("11" * 32)
    with pytest.raises(NppesRegistryReplayError):
        accumulator.finish()


def test_manifest_digest_normalizes_nonserializable_payload() -> None:
    with pytest.raises(NppesRegistryReplayError):
        nppes_manifest_sha256(object())
