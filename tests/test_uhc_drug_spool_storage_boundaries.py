# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Storage and metadata boundaries for retained UHC formulary spools."""

from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
import sqlite3

import pytest

from process.formulary_fhir.repository_shared import json_text
import process.formulary_fhir.uhc_drug_normalization as normalization
import process.formulary_fhir.uhc_drug_parser as parser
import process.formulary_fhir.uhc_drug_spool_contract as spool_contract
import process.formulary_fhir.uhc_drug_spool_reader as spool_reader
import process.formulary_fhir.uhc_drug_spool_storage as spool_storage
from tests.test_uhc_drug_spool_boundaries import _Connection
from tests.uhc_drug_parser_test_support import artifact_set


def test_spool_metadata_rejects_missing_and_duplicate_artifact_proofs(
    monkeypatch,
) -> None:
    artifacts, _bodies = artifact_set()
    evidence = parser.UHCDrugSpoolEvidence(
        source_id=artifacts.source_id,
        source_file_set_sha256=artifacts.source_file_set_sha256,
        artifact_set_sha256=artifacts.artifact_set_sha256,
        spool_content_sha256="c" * 64,
        file_count=48,
        raw_record_count=48,
        raw_plan_entry_count=48,
        plan_count=2,
        medication_membership_count=2,
        duplicate_count=0,
        superseded_count=0,
        max_last_updated_at=dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
    )
    with pytest.raises(RuntimeError, match="metadata"):
        spool_reader._verified_spool_metadata_records(
            _Connection([]),
            evidence,
            artifacts,
        )

    proof = spool_contract.artifact_proof_rows(artifacts)[0]
    duplicate_proofs = [proof, proof]
    metadata_row = (
        json_text(spool_contract.spool_evidence_payload(evidence)),
        json_text(duplicate_proofs),
    )
    monkeypatch.setattr(
        spool_reader,
        "artifact_proof_rows",
        lambda _artifacts: tuple(duplicate_proofs),
    )
    with pytest.raises(RuntimeError, match="artifact proof"):
        spool_reader._verified_spool_metadata_records(
            _Connection([metadata_row]),
            evidence,
            artifacts,
        )


def test_membership_proof_cancels_and_binding_requires_pinned_spool(
    monkeypatch,
) -> None:
    semantic_json = json_text({"contract": normalization.SPOOL_CONTRACT})
    database_row = (
        "alias",
        "cs",
        "HIOS",
        "PLAN",
        2026,
        "1234567",
        "drug",
        "tier",
        0,
        0,
        0,
        "2026-08-10T00:00:00+00:00",
        semantic_json,
        "[]",
    )
    monkeypatch.setattr(
        spool_reader,
        "validated_spool_provenance",
        lambda *_arguments, **_keywords: (),
    )
    with pytest.raises(RuntimeError, match="cancelled"):
        spool_reader._verified_membership_proof(
            _Connection([database_row] * 1_024),
            ("{}", "[]"),
            {},
            lambda: (_ for _ in ()).throw(RuntimeError("cancelled")),
        )
    with pytest.raises(ValueError, match="unavailable"):
        spool_reader.verify_and_bind_uhc_drug_spool(
            object(),
            object(),
            object(),
        )


def test_descriptor_open_rejects_relative_unsafe_parent_and_unsafe_leaf(
    tmp_path,
) -> None:
    with pytest.raises(ValueError, match="unavailable"):
        spool_storage._open_spool_descriptor(Path("relative.sqlite"))

    unsafe_parent = tmp_path / "unsafe-parent"
    unsafe_parent.mkdir(mode=0o755)
    os.chmod(unsafe_parent, 0o755)
    unsafe_parent_file = unsafe_parent / "spool.sqlite"
    unsafe_parent_file.write_bytes(b"spool")
    os.chmod(unsafe_parent_file, 0o600)
    with pytest.raises(ValueError, match="unavailable"):
        spool_storage._open_spool_descriptor(unsafe_parent_file)

    private_parent = tmp_path / "private-parent"
    private_parent.mkdir(mode=0o700)
    os.chmod(private_parent, 0o700)
    unsafe_leaf = private_parent / "spool.sqlite"
    unsafe_leaf.write_bytes(b"spool")
    os.chmod(unsafe_leaf, 0o644)
    with pytest.raises(ValueError, match="unavailable"):
        spool_storage._open_spool_descriptor(unsafe_leaf)


def test_pinned_and_open_spool_reject_invalid_descriptor_path_and_sqlite(
    monkeypatch,
    tmp_path,
) -> None:
    invalid_pin = spool_storage.PinnedUHCDrugSpool(-1, 1, 1, 1, 1, 1)
    with pytest.raises(ValueError, match="unavailable"):
        spool_storage._duplicate_pinned_descriptor(invalid_pin)
    with pytest.raises(ValueError, match="unavailable"):
        with spool_storage.pin_uhc_drug_spool(object()):
            pytest.fail("invalid spool unexpectedly opened")
    with pytest.raises(ValueError, match="unavailable"):
        with spool_storage.open_uhc_drug_spool(object()):
            pytest.fail("invalid spool unexpectedly opened")

    private_file = tmp_path / "private.sqlite"
    private_file.write_bytes(b"not sqlite")
    os.chmod(private_file, 0o600)
    monkeypatch.setattr(
        spool_storage.sqlite3,
        "connect",
        lambda *_arguments, **_keywords: (_ for _ in ()).throw(
            sqlite3.Error("synthetic sqlite failure")
        ),
    )
    with pytest.raises(ValueError, match="unavailable"):
        with spool_storage.open_uhc_drug_spool(private_file):
            pytest.fail("invalid SQLite spool unexpectedly opened")
