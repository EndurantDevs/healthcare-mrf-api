# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from pathlib import Path

import pytest

from process import uhc_provider_file_catalog_artifacts as catalog_artifacts
from process.formulary_fhir.uhc_source_artifacts import (
    prepare_uhc_source_artifact_registration,
)
from process.ptg_parts.artifacts import PTG2ArtifactStore
from process.uhc_provider_file_catalog_contract import UHCFileCatalogError
from tests.uhc_provider_file_catalog_test_data import raw_catalog_snapshot


SOURCE_ID = "uhc-official-formulary-mrf"


def _retained_proof(monkeypatch, tmp_path: Path) -> dict:
    artifact_root = tmp_path / "durable-catalog"
    monkeypatch.setattr(
        catalog_artifacts,
        "catalog_artifact_root",
        lambda: artifact_root,
    )
    artifact_store = PTG2ArtifactStore(artifact_root)
    proof_documents = []
    for document in raw_catalog_snapshot().documents:
        artifact_path = artifact_store.artifact_path(
            document.raw_sha256,
            kind=catalog_artifacts.CATALOG_ARTIFACT_KIND,
            suffix=".json",
        )
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_bytes(document.raw_bytes)
        proof_documents.append(
            {
                "family": document.family,
                "url": document.url,
                "response_url": document.response_url,
                "raw_sha256": document.raw_sha256,
                "byte_count": len(document.raw_bytes),
                "storage_uri": artifact_store.storage_uri(artifact_path),
            }
        )
    return {
        "raw_set_sha256": (
            catalog_artifacts.raw_set_sha256_from_documents(proof_documents)
        ),
        "documents": proof_documents,
    }


def test_retained_listing_proof_projects_exact_48_file_registration(
    monkeypatch,
    tmp_path: Path,
) -> None:
    raw_proof = _retained_proof(monkeypatch, tmp_path)

    registration = prepare_uhc_source_artifact_registration(
        SOURCE_ID,
        raw_proof,
    )

    assert registration.source_observation_sha256 == raw_proof["raw_set_sha256"]
    assert len(registration.identities) == 48
    assert {
        identity.source_file_set_sha256
        for identity in registration.identities
    } == {registration.catalog.acquisition_contract_sha256}
    assert {
        (identity.family, identity.file_name)
        for identity in registration.identities
    } == {
        (catalog_file.family, catalog_file.file_name)
        for catalog_file in registration.catalog.files
    }


def test_retained_listing_proof_rejects_mutated_source_bytes(
    monkeypatch,
    tmp_path: Path,
) -> None:
    raw_proof = _retained_proof(monkeypatch, tmp_path)
    first_document_uri = raw_proof["documents"][0]["storage_uri"]
    Path(first_document_uri.removeprefix("file://")).write_bytes(b"changed")

    with pytest.raises(UHCFileCatalogError, match="artifact is corrupt"):
        prepare_uhc_source_artifact_registration(SOURCE_ID, raw_proof)
