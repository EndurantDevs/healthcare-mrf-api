# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Behavior proof for exact NPPES registry replay and v1 projection."""

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
from public_evidence.nppes_registry_metrics import nppes_manifest_metrics
from public_evidence.nppes_registry_primitives import (
    NPPES_REGISTRY_MANIFEST_CONTRACT,
    NppesRegistryReplayError,
    build_nppes_archive_identity,
    nppes_header_sha256,
    scan_nppes_registry_row,
    validate_nppes_archive_identity,
)
from public_evidence.nppes_registry_projection import (
    NppesRegistryPersistenceProjector,
)
from public_evidence.nppes_registry_replay_contract import (
    NppesRegistryArchiveScanner,
    build_nppes_manifest_from_rows,
    validate_nppes_registry_manifest,
)
from public_evidence.record_persistence_candidate_primitives import (
    NpiEnumerationRow,
)
from tests.public_evidence_nppes_registry_support import (
    HEADER,
    active_type_1_row,
    active_type_2_row,
    archive_identity,
    known_type_deactivated_row,
    reactivated_type_1_row,
    sparse_deactivated_row,
)


def _recursive_root(leaves: tuple[str, ...]) -> str:
    if len(leaves) == 1:
        return leaves[0]
    split = 1 << ((len(leaves) - 1).bit_length() - 1)
    if split == len(leaves):
        split //= 2
    return derive_nppes_tree_node(
        _recursive_root(leaves[:split]),
        _recursive_root(leaves[split:]),
    )


def test_archive_identity_and_header_are_exact_and_deterministic() -> None:
    identity = archive_identity()
    assert validate_nppes_archive_identity(identity) == identity
    assert identity.snapshot_at == "2026-07-12T00:00:00Z"
    assert nppes_header_sha256(HEADER) == nppes_header_sha256(list(HEADER))
    assert "NPPES" not in repr(identity)


def test_row_projection_maps_active_reactivated_and_deactivated_states() -> None:
    identity = archive_identity()
    active = scan_nppes_registry_row(identity, HEADER, active_type_1_row(), 1)
    reactivated = scan_nppes_registry_row(
        identity, HEADER, reactivated_type_1_row(), 2
    )
    deactivated = scan_nppes_registry_row(
        identity, HEADER, known_type_deactivated_row(), 3
    )
    sparse = scan_nppes_registry_row(
        identity, HEADER, sparse_deactivated_row(), 4
    )
    assert (
        active.npi_entity_type,
        active.enumeration_state,
        active.effective_start_at,
        active.effective_end_at,
    ) == (
        "individual_type_1",
        "active",
        "2005-05-23T00:00:00Z",
        identity.snapshot_at,
    )
    assert reactivated.enumeration_state == "active"
    assert reactivated.effective_start_at == "2026-06-15T00:00:00Z"
    assert deactivated.enumeration_state == "deactivated"
    assert deactivated.effective_start_at == "2026-06-20T00:00:00Z"
    assert sparse.npi_entity_type is None
    assert sparse.exclusion_reason == "entity_type_not_disclosed"
    assert "100300" not in repr(sparse)


def test_manifest_binds_every_source_row_and_exact_exclusion_census() -> None:
    rows = (
        active_type_1_row(),
        active_type_2_row(),
        sparse_deactivated_row(),
    )
    manifest = build_nppes_manifest_from_rows(archive_identity(), HEADER, rows)
    assert validate_nppes_registry_manifest(manifest) == manifest
    assert manifest.contract == NPPES_REGISTRY_MANIFEST_CONTRACT
    assert manifest.source_record_count == 3
    assert manifest.projected_record_count == 2
    assert manifest.excluded_record_count == 1
    assert manifest.exclusion_counts == (("entity_type_not_disclosed", 1),)
    assert manifest.release.completeness_attestation.expected_record_count == 3
    assert manifest.release.completeness_attestation.observed_record_count == 3
    assert manifest.release.whole_source_complete is False
    assert manifest.release.publication_enabled is False
    assert manifest.release.serving_authority == "none"
    assert manifest.minimum_effective_start_at == "2005-05-23T00:00:00Z"
    assert repr(manifest) == "<nppes-registry-archive-manifest>"


def test_projected_and_excluded_rows_build_exact_persistence_shapes() -> None:
    identity = archive_identity()
    rows = (active_type_2_row(), sparse_deactivated_row())
    manifest = build_nppes_manifest_from_rows(identity, HEADER, rows)
    projector = NppesRegistryPersistenceProjector(manifest, HEADER)
    projected = projector.add(active_type_2_row())
    excluded = projector.add(sparse_deactivated_row())
    assert projector.finish().manifest_sha256 == manifest.manifest_sha256
    assert projected.source_record.record_kind == "nppes_registry_record"
    assert projected.candidate is not None
    assert len(projected.candidate.source_link_rows) == 1
    assert type(projected.candidate.typed_row) is NpiEnumerationRow
    assert projected.candidate.typed_row.npi_entity_type == "organization_type_2"
    assert excluded.candidate is None
    assert excluded.exclusion_reason == "entity_type_not_disclosed"


def test_fast_candidate_encoder_is_exactly_equal_to_public_oracle() -> None:
    rows = (
        active_type_1_row(),
        active_type_2_row(),
        reactivated_type_1_row(),
        known_type_deactivated_row(),
        sparse_deactivated_row(),
    )
    manifest = build_nppes_manifest_from_rows(archive_identity(), HEADER, rows)
    oracle = NppesRegistryPersistenceProjector(manifest, HEADER)
    encoder = NppesRegistryCandidateEncoder(manifest, HEADER)
    for row_values in rows:
        expected = oracle.add(row_values)
        actual = encoder.add(row_values)
        assert actual.source_record == expected.source_record
        if expected.candidate is None:
            assert actual.common_row is None
            assert actual.source_link_row is None
            assert actual.typed_row is None
        else:
            assert actual.common_row == expected.candidate.common_row
            assert actual.source_link_row == expected.candidate.source_link_rows[0]
            assert actual.typed_row == expected.candidate.typed_row
    assert encoder.finish() == oracle.finish() == manifest


def test_projection_requires_the_complete_exact_archive_replay() -> None:
    rows = (active_type_1_row(), active_type_2_row())
    manifest = build_nppes_manifest_from_rows(archive_identity(), HEADER, rows)
    projector = NppesRegistryPersistenceProjector(manifest, HEADER)
    projector.add(active_type_1_row())
    with pytest.raises(NppesRegistryReplayError):
        projector.finish()

    projector = NppesRegistryPersistenceProjector(manifest, HEADER)
    projector.add(active_type_2_row())
    projector.add(active_type_1_row())
    with pytest.raises(NppesRegistryReplayError):
        projector.finish()

    wrong_header = HEADER + ("Extra",)
    with pytest.raises(NppesRegistryReplayError):
        NppesRegistryPersistenceProjector(manifest, wrong_header)


def test_projection_cannot_cross_archive_boundaries() -> None:
    rows = (active_type_1_row(),)
    first = build_nppes_manifest_from_rows(archive_identity(), HEADER, rows)
    second = build_nppes_manifest_from_rows(
        archive_identity(artifact_sha256="c3" * 32), HEADER, rows
    )
    projector = NppesRegistryPersistenceProjector(second, HEADER)
    projector.add(active_type_1_row())
    assert projector.finish().manifest_sha256 == second.manifest_sha256
    assert first.manifest_sha256 != second.manifest_sha256


@pytest.mark.parametrize("leaf_count", (1, 2, 3, 5, 6, 8))
def test_streaming_root_matches_recursive_rfc6962_shape(leaf_count: int) -> None:
    leaves = tuple(f"{ordinal:064x}" for ordinal in range(1, leaf_count + 1))
    accumulator = NppesEvidenceRootAccumulator()
    for leaf in leaves:
        accumulator.add(leaf)
    assert accumulator.finish() == _recursive_root(leaves)


def test_row_and_order_changes_change_the_archive_root() -> None:
    identity = archive_identity()
    first = build_nppes_manifest_from_rows(
        identity, HEADER, (active_type_1_row(), active_type_2_row())
    )
    reversed_manifest = build_nppes_manifest_from_rows(
        identity, HEADER, (active_type_2_row(), active_type_1_row())
    )
    mutated_values = list(active_type_2_row())
    mutated_values[3] = "07/03/2026"
    mutated = build_nppes_manifest_from_rows(
        identity, HEADER, (active_type_1_row(), tuple(mutated_values))
    )
    assert len(
        {
            first.evidence_root_sha256,
            reversed_manifest.evidence_root_sha256,
            mutated.evidence_root_sha256,
        }
    ) == 3


def test_metrics_are_bounded_and_release_scoped() -> None:
    manifest = build_nppes_manifest_from_rows(
        archive_identity(), HEADER, (active_type_1_row(), sparse_deactivated_row())
    )
    metrics = nppes_manifest_metrics(manifest)
    assert metrics["source_record_count"] == 2
    assert metrics["projected_record_count"] == 1
    assert metrics["excluded_record_count"] == 1
    assert metrics["exclusion_counts"] == {"entity_type_not_disclosed": 1}
    assert "npi" not in metrics


def test_manifest_validation_rejects_digest_and_count_tampering() -> None:
    manifest = build_nppes_manifest_from_rows(
        archive_identity(), HEADER, (active_type_1_row(), sparse_deactivated_row())
    )
    for forged in (
        replace(manifest, manifest_sha256="00" * 32),
        replace(manifest, source_record_count=3),
        replace(manifest, evidence_root_sha256="11" * 32),
        replace(manifest, minimum_effective_start_at="2027-01-01T00:00:00Z"),
        replace(manifest, minimum_effective_start_at="not-a-date"),
        replace(manifest, exclusion_counts=[]),
        replace(manifest, exclusion_counts=(("invalid", 1),)),
        replace(manifest, exclusion_counts=(("entity_type_not_disclosed", 2),)),
    ):
        with pytest.raises(NppesRegistryReplayError):
            validate_nppes_registry_manifest(forged)
    with pytest.raises(NppesRegistryReplayError):
        validate_nppes_registry_manifest(object())


def test_manifest_rejects_unsorted_two_reason_census_and_non_tuple_rows() -> None:
    effective_missing_fields = list(active_type_2_row())
    effective_missing_fields[2] = ""
    rows = (
        active_type_1_row(),
        tuple(effective_missing_fields),
        sparse_deactivated_row(),
    )
    manifest = build_nppes_manifest_from_rows(archive_identity(), HEADER, rows)
    assert manifest.exclusion_counts == (
        ("effective_start_not_disclosed", 1),
        ("entity_type_not_disclosed", 1),
    )
    with pytest.raises(NppesRegistryReplayError):
        validate_nppes_registry_manifest(
            replace(manifest, exclusion_counts=tuple(reversed(manifest.exclusion_counts)))
        )
    with pytest.raises(NppesRegistryReplayError):
        build_nppes_manifest_from_rows(archive_identity(), HEADER, list(rows))


def test_empty_archive_scanner_fails_closed() -> None:
    scanner = NppesRegistryArchiveScanner(archive_identity(), HEADER)
    with pytest.raises(NppesRegistryReplayError):
        scanner.finish()


@pytest.mark.parametrize(
    "overrides",
    (
        {"source_url": "https://example.test/nppes/archive.zip"},
        {"archive_name": "../unsafe.zip"},
        {"primary_member_name": "nested/npidata_pfile_20260101-20260802.csv"},
        {"artifact_sha256": "not-a-digest"},
        {"artifact_byte_count": 0},
    ),
)
def test_archive_identity_rejects_unsafe_or_inexact_inputs(overrides) -> None:
    identity_values_by_name = {
        "source_url": (
            "https://download.cms.gov/nppes/"
            "NPPES_Data_Dissemination_July_2026_V2.zip"
        ),
        "archive_name": "NPPES_Data_Dissemination_July_2026_V2.zip",
        "primary_member_name": "npidata_pfile_20050523-20260712.csv",
        "artifact_sha256": "a1" * 32,
        "artifact_byte_count": 100,
        "rights_proof_sha256": "b2" * 32,
    }
    identity_values_by_name.update(overrides)
    with pytest.raises(NppesRegistryReplayError):
        build_nppes_archive_identity(**identity_values_by_name)


@pytest.mark.parametrize(
    "mutator",
    (
        lambda row: row.__setitem__(0, "1003000101"),
        lambda row: row.__setitem__(1, "3"),
        lambda row: row.__setitem__(2, "2026-01-01"),
        lambda row: row.__setitem__(2, "09/01/2026"),
    ),
)
def test_row_scan_rejects_invalid_npi_type_date_or_future_enumeration(
    mutator,
) -> None:
    row_values = list(active_type_1_row())
    mutator(row_values)
    with pytest.raises(NppesRegistryReplayError):
        scan_nppes_registry_row(archive_identity(), HEADER, tuple(row_values), 1)


@pytest.mark.parametrize(
    "row_values",
    (
        ("1003000100", "1", "07/02/2026", "07/02/2026", "07/01/2026", ""),
        (
            "1003000100",
            "1",
            "07/03/2026",
            "07/03/2026",
            "07/01/2026",
            "07/02/2026",
        ),
    ),
)
def test_row_projection_rejects_impossible_event_chronology(
    row_values: tuple[str, ...],
) -> None:
    with pytest.raises(NppesRegistryReplayError):
        scan_nppes_registry_row(archive_identity(), HEADER, row_values, 1)


def test_scanner_does_not_advance_after_chronology_rejection() -> None:
    scanner = NppesRegistryArchiveScanner(archive_identity(), HEADER)
    impossible = (
        "1003000100",
        "1",
        "07/02/2026",
        "07/02/2026",
        "07/01/2026",
        "",
    )
    with pytest.raises(NppesRegistryReplayError):
        scanner.add(impossible)
    assert scanner.count == 0
    scanner.add(active_type_1_row())
    assert scanner.finish().source_record_count == 1


def test_scan_rejects_duplicate_npi_and_reuse_after_finish() -> None:
    scanner = NppesRegistryArchiveScanner(archive_identity(), HEADER)
    scanner.add(active_type_1_row())
    with pytest.raises(NppesRegistryReplayError):
        scanner.add(active_type_1_row())
    manifest = scanner.finish()
    assert manifest.source_record_count == 1
    with pytest.raises(NppesRegistryReplayError):
        scanner.add(active_type_2_row())
    with pytest.raises(NppesRegistryReplayError):
        scanner.finish()


def test_public_failures_do_not_retain_private_exception_context(monkeypatch) -> None:
    def explode(_value):
        raise RuntimeError("PRIVATE-NPPES-MARKER")

    monkeypatch.setattr(
        "public_evidence.nppes_registry_primitives.compile_nppes_registry_header",
        explode,
    )
    with pytest.raises(NppesRegistryReplayError) as caught:
        scan_nppes_registry_row(
            archive_identity(), HEADER, active_type_1_row(), 1
        )
    assert str(caught.value) == "nppes_registry_replay_invalid"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
