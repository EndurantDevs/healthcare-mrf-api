# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable storage-row proof for complete NPPES listing-chain receipts."""

from __future__ import annotations

from datetime import datetime

import pytest

from process.nppes_public_evidence_archive import NppesPublicEvidenceArchiveError
from process.nppes_public_evidence_chain_rows import (
    CHAIN_ADMISSION_COLUMNS,
    CHAIN_ARCHIVE_COLUMNS,
    build_nppes_chain_storage_rows,
    chain_admission_values,
    chain_archive_values,
)
from tests.test_nppes_public_evidence_import import _valid_receipt


def _mapped(columns, values) -> dict[str, object]:
    return dict(zip(columns, values, strict=True))


def test_chain_rows_bind_listing_vector_and_every_selected_release() -> None:
    receipt = _valid_receipt()
    admission, archives = build_nppes_chain_storage_rows(receipt)
    assert admission.chain_ref == receipt.chain_ref
    assert admission.listing_candidate_names == receipt.listing_candidate_names
    assert admission.archive_count == len(receipt.archives)
    assert admission.source_record_count == receipt.source_record_count
    assert admission.admission_state == "verified_complete_disabled"
    assert admission.serving_authority == "none"
    assert admission.publication_enabled is False
    assert tuple(row.archive_ordinal for row in archives) == (0, 1, 2)
    assert tuple(row.admission_ref for row in archives) == tuple(
        archive.admission_ref for archive in receipt.archives
    )
    assert len({admission.row_sha256, *(row.row_sha256 for row in archives)}) == 4
    assert "NPPES" not in repr(admission)
    assert "NPPES" not in repr(archives[0])


def test_chain_row_codecs_convert_digests_arrays_and_timestamps() -> None:
    admission, archives = build_nppes_chain_storage_rows(_valid_receipt())
    admission_values = _mapped(
        CHAIN_ADMISSION_COLUMNS,
        chain_admission_values(admission),
    )
    archive_values = _mapped(
        CHAIN_ARCHIVE_COLUMNS,
        chain_archive_values(archives[0]),
    )
    assert type(admission_values["listing_sha256"]) is bytes
    assert type(admission_values["contract_sha256"]) is bytes
    assert type(admission_values["listing_candidate_names"]) is list
    assert type(archive_values["snapshot_at"]) is datetime
    assert type(archive_values["artifact_sha256"]) is bytes
    assert type(archive_values["row_sha256"]) is bytes


def test_chain_row_builder_normalizes_wrong_receipt_type() -> None:
    with pytest.raises(NppesPublicEvidenceArchiveError) as caught:
        build_nppes_chain_storage_rows(object())
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
