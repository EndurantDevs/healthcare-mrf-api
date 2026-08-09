# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Boundary proof for deterministic NPPES chain receipts."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from process.control_cancel import ImportCancelledError
from process.nppes_public_evidence_archive import NppesPublicEvidenceArchiveError
from process.nppes_public_evidence_chain import (
    _archive_snapshot,
    _finished_chain_receipt,
    _listing_candidates,
    _validated_archive_receipt,
    _validated_chain_archives,
    validate_nppes_public_evidence_chain_receipt,
)
from process.nppes_public_evidence_prepared_chain import (
    _listing_file_identity,
    build_prepared_nppes_release_chain,
)
from process.nppes_public_evidence_import import (
    NppesEvidenceRuntimeConfig,
    import_nppes_public_evidence_chain,
)
from tests.nppes_public_evidence_process_support import prepared_release_chain
from tests.public_evidence_nppes_registry_support import active_type_1_row
from tests.test_nppes_public_evidence_import import (
    MONTHLY,
    WEEKLY_1,
    _config,
    _valid_receipt,
)


def _rejects_archive(archive) -> None:
    receipt = _valid_receipt()
    with pytest.raises(NppesPublicEvidenceArchiveError):
        validate_nppes_public_evidence_chain_receipt(
            replace(receipt, archives=(archive, *receipt.archives[1:])),
        )


def test_chain_receipts_have_value_safe_reprs() -> None:
    receipt = _valid_receipt()
    assert repr(receipt) == "<nppes-public-evidence-chain-receipt>"
    assert repr(receipt.archives[0]) == "<nppes-public-evidence-archive-receipt>"


def test_finished_chain_rejects_an_empty_archive_vector() -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        _finished_chain_receipt("ca" * 32, 1, (MONTHLY,), ())


@pytest.mark.parametrize(
    "archive_mutator",
    (
        lambda archive: replace(archive, archive_name=object()),
        lambda archive: replace(archive, archive_name="invalid.zip"),
        lambda archive: replace(archive, snapshot_at=object()),
        lambda archive: replace(archive, snapshot_at="invalid"),
        lambda archive: replace(archive, snapshot_at="2026-7-12T00:00:00Z"),
    ),
)
def test_archive_snapshot_rejects_noncanonical_identity(archive_mutator) -> None:
    _rejects_archive(archive_mutator(_valid_receipt().archives[0]))


def test_archive_snapshot_rejects_wrong_weekly_period_end() -> None:
    weekly = replace(
        _valid_receipt().archives[1],
        snapshot_at="2026-07-20T00:00:00Z",
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        _archive_snapshot(weekly)


def test_archive_receipt_validator_rejects_wrong_outer_type() -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        _validated_archive_receipt(object())


@pytest.mark.parametrize(
    "candidate_names",
    (
        (),
        [],
        (1,),
        ("invalid.zip",),
    ),
)
def test_listing_candidates_reject_wrong_shape_or_name(candidate_names) -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError):
        _listing_candidates(candidate_names)


def test_validated_chain_rejects_empty_or_duplicate_owner_vectors() -> None:
    receipt = _valid_receipt()
    candidates = _listing_candidates(receipt.listing_candidate_names)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        _validated_chain_archives((), candidates)

    duplicate_owner = replace(
        receipt.archives[1],
        admission_ref=receipt.archives[0].admission_ref,
    )
    with pytest.raises(NppesPublicEvidenceArchiveError):
        _validated_chain_archives(
            (receipt.archives[0], duplicate_owner, receipt.archives[2]),
            candidates,
        )


def test_chain_validator_rejects_an_untyped_archive_child() -> None:
    receipt = _valid_receipt()
    with pytest.raises(NppesPublicEvidenceArchiveError):
        validate_nppes_public_evidence_chain_receipt(
            replace(receipt, archives=(object(), *receipt.archives[1:])),
        )


def test_weekly_fixture_name_still_matches_contract() -> None:
    assert _listing_candidates((MONTHLY, WEEKLY_1))[1].archive_name == WEEKLY_1


def test_prepared_chain_repr_and_listing_identity_boundaries(tmp_path) -> None:
    prepared_chain = prepared_release_chain(
        tmp_path,
        ((MONTHLY, "20260712", (active_type_1_row(),)),),
    )
    assert repr(prepared_chain) == "<prepared-nppes-release-chain>"

    directory = tmp_path / "listing-directory"
    directory.mkdir()
    with pytest.raises(NppesPublicEvidenceArchiveError):
        _listing_file_identity(directory)

    symlink = tmp_path / "listing-link"
    symlink.symlink_to(prepared_chain.listing.path)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        _listing_file_identity(symlink)

    with pytest.raises(NppesPublicEvidenceArchiveError):
        build_prepared_nppes_release_chain(prepared_chain.listing, ())


@pytest.mark.asyncio
async def test_chain_import_rejects_disabled_mode_and_preserves_cancel(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prepared_chain = prepared_release_chain(
        tmp_path,
        ((MONTHLY, "20260712", (active_type_1_row(),)),),
    )
    expected_counts = ((MONTHLY, 1),)
    with pytest.raises(NppesPublicEvidenceArchiveError):
        await import_nppes_public_evidence_chain(
            prepared_chain,
            NppesEvidenceRuntimeConfig("off", None),
            expected_source_record_counts=expected_counts,
        )

    monkeypatch.setattr(
        "process.nppes_public_evidence_import._admitted_archive_receipts",
        AsyncMock(side_effect=ImportCancelledError("cancelled")),
    )
    with pytest.raises(ImportCancelledError):
        await import_nppes_public_evidence_chain(
            prepared_chain,
            _config(),
            expected_source_record_counts=expected_counts,
        )
