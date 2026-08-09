# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contract proof for NPPES source-member and admission rows."""

from __future__ import annotations

from dataclasses import replace

import pytest

from public_evidence.nppes_registry_primitives import NppesRegistryReplayError
from public_evidence.nppes_registry_candidate_encoder import (
    NppesRegistryPersistenceRows,
    validate_nppes_registry_persistence_rows,
)
from public_evidence.nppes_registry_replay_contract import (
    build_nppes_manifest_from_rows,
)
from public_evidence.nppes_registry_storage_contract import (
    NppesRegistryArchiveObservation,
    NppesRegistryMemberEncoder,
    _build_member_row,
    build_nppes_registry_admission_row,
)
from tests.public_evidence_nppes_registry_support import (
    HEADER,
    active_type_1_row,
    archive_identity,
    sparse_deactivated_row,
)


def _manifest_and_rows():
    source_rows = (active_type_1_row(), sparse_deactivated_row())
    manifest = build_nppes_manifest_from_rows(archive_identity(), HEADER, source_rows)
    encoder = NppesRegistryMemberEncoder(
        manifest,
        HEADER,
        _archive_observation(),
    )
    paired_rows = tuple(encoder.encode(row_values) for row_values in source_rows)
    assert encoder.finish() == manifest
    return manifest, paired_rows


def _archive_observation() -> NppesRegistryArchiveObservation:
    return NppesRegistryArchiveObservation(
        listing_sha256="c3" * 32,
        zip_member_count=7,
        zip_member_census_sha256="d4" * 32,
    )


def test_member_rows_bind_every_source_occurrence_and_disposition() -> None:
    _manifest, paired_rows = _manifest_and_rows()
    projected_rows, projected = paired_rows[0]
    _excluded_rows, excluded = paired_rows[1]
    assert projected.source_row_ordinal == 1
    assert projected.projection_state == "projected_v1"
    assert projected.evidence_ref == projected_rows.common_row.evidence_ref
    assert projected.exclusion_reason is None
    assert excluded.source_row_ordinal == 2
    assert excluded.projection_state == "excluded_v1"
    assert excluded.evidence_ref is None
    assert excluded.exclusion_reason == "entity_type_not_disclosed"
    assert len({projected.row_sha256, excluded.row_sha256}) == 2
    assert "100300" not in repr(projected)


def test_admission_row_binds_manifest_acquisition_and_exact_census() -> None:
    manifest, _encoded_rows = _manifest_and_rows()
    admission = build_nppes_registry_admission_row(
        manifest,
        _archive_observation(),
    )
    assert repr(_archive_observation()) == "<nppes-registry-archive-observation>"
    assert repr(admission) == "<nppes-registry-admission-row>"
    assert admission.source_release_ref == manifest.release.source_release_ref
    assert admission.source_record_count == 2
    assert admission.projected_record_count == 1
    assert admission.excluded_record_count == 1
    assert admission.entity_type_not_disclosed_count == 1
    assert admission.effective_start_not_disclosed_count == 0
    assert admission.admission_state == "verified_complete_disabled"
    assert admission.serving_authority == "none"
    assert admission.publication_enabled is False
    assert admission == build_nppes_registry_admission_row(
        manifest,
        _archive_observation(),
    )
    later_listing = replace(
        _archive_observation(),
        listing_sha256="e5" * 32,
    )
    assert build_nppes_registry_admission_row(manifest, later_listing) == admission


@pytest.mark.parametrize(
    "observation",
    (
        object(),
        NppesRegistryArchiveObservation("bad", 7, "d4" * 32),
        NppesRegistryArchiveObservation("c3" * 32, 0, "d4" * 32),
        NppesRegistryArchiveObservation("c3" * 32, 7, "bad"),
    ),
)
def test_admission_rejects_invalid_acquisition_evidence(observation) -> None:
    manifest, _encoded_rows = _manifest_and_rows()
    with pytest.raises(NppesRegistryReplayError):
        build_nppes_registry_admission_row(manifest, observation)


def test_member_rejects_detached_or_mismatched_replay_rows() -> None:
    manifest, paired_rows = _manifest_and_rows()
    encoded_rows, _member = paired_rows[0]
    member_encoder = NppesRegistryMemberEncoder(
        manifest,
        HEADER,
        _archive_observation(),
    )
    with pytest.raises(NppesRegistryReplayError):
        member_encoder.encode(object())
    forged_observation = replace(
        encoded_rows.observation,
        payload_sha256="00" * 32,
    )
    with pytest.raises(TypeError):
        replace(encoded_rows, observation=forged_observation)
    forged = object.__new__(NppesRegistryPersistenceRows)
    for field_name in (
        "source_record",
        "common_row",
        "source_link_row",
        "typed_row",
        "exclusion_reason",
    ):
        object.__setattr__(forged, field_name, getattr(encoded_rows, field_name))
    object.__setattr__(forged, "observation", forged_observation)
    object.__setattr__(forged, "_seal", object())
    with pytest.raises(NppesRegistryReplayError):
        validate_nppes_registry_persistence_rows(forged)

    sealed_forgery = object.__new__(NppesRegistryPersistenceRows)
    forged_source_record = encoded_rows.source_record._replace(
        payload_sha256="00" * 32,
    )
    for field_name in (
        "observation",
        "common_row",
        "source_link_row",
        "typed_row",
        "exclusion_reason",
        "_seal",
    ):
        object.__setattr__(
            sealed_forgery,
            field_name,
            getattr(encoded_rows, field_name),
        )
    object.__setattr__(sealed_forgery, "source_record", forged_source_record)
    with pytest.raises(NppesRegistryReplayError):
        _build_member_row(manifest, _member.admission_ref, sealed_forgery)


def test_member_encoder_rejects_invalid_observation_and_reuse() -> None:
    manifest = build_nppes_manifest_from_rows(
        archive_identity(),
        HEADER,
        (active_type_1_row(),),
    )
    with pytest.raises(NppesRegistryReplayError):
        NppesRegistryMemberEncoder(manifest, HEADER, object())

    encoder = NppesRegistryMemberEncoder(manifest, HEADER, _archive_observation())
    encoder.encode(active_type_1_row())
    assert encoder.finish() == manifest
    with pytest.raises(NppesRegistryReplayError):
        encoder.encode(active_type_1_row())
    with pytest.raises(NppesRegistryReplayError):
        encoder.finish()
