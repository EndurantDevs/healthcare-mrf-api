# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import Mock

import pytest

from process import uhc_provider_file_catalog_artifacts as artifacts
from process import uhc_provider_file_catalog_types as catalog_types
from process.ptg_parts.artifacts import PTG2ArtifactStore
from tests.uhc_provider_file_catalog_test_data import raw_catalog_snapshot


def _retained_proof(monkeypatch, tmp_path: Path) -> dict:
    root = tmp_path / "durable-catalog"
    monkeypatch.setattr(artifacts, "catalog_artifact_root", lambda: root)
    store = PTG2ArtifactStore(root)
    documents = []
    for document in raw_catalog_snapshot().documents:
        path = store.artifact_path(
            document.raw_sha256,
            kind=artifacts.CATALOG_ARTIFACT_KIND,
            suffix=".json",
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(document.raw_bytes)
        documents.append(
            {
                "family": document.family,
                "url": document.url,
                "response_url": document.response_url,
                "raw_sha256": document.raw_sha256,
                "byte_count": len(document.raw_bytes),
                "storage_uri": store.storage_uri(path),
            }
        )
    return {
        "raw_set_sha256": artifacts.raw_set_sha256_from_documents(documents),
        "documents": documents,
    }


def test_retained_proof_rehashes_both_family_bound_artifacts(monkeypatch, tmp_path):
    proof = _retained_proof(monkeypatch, tmp_path)

    assert artifacts.validate_retained_raw_proof(proof) == proof


def test_retained_proof_rejects_hash_or_storage_escape(monkeypatch, tmp_path):
    proof = _retained_proof(monkeypatch, tmp_path)
    proof["documents"][0]["storage_uri"] = (tmp_path / "outside.json").as_uri()

    with pytest.raises(catalog_types.UHCFileCatalogError, match="escapes"):
        artifacts.validate_retained_raw_proof(proof)


def test_retained_proof_requires_exact_canonical_file_uri(monkeypatch, tmp_path):
    proof = _retained_proof(monkeypatch, tmp_path)
    store = PTG2ArtifactStore(tmp_path / "durable-catalog")
    expected_uri = proof["documents"][0]["storage_uri"]
    expected_path = store.path_from_uri(expected_uri)
    noncanonical_path = (
        expected_path.parent
        / ".."
        / expected_path.parent.name
        / expected_path.name
    )
    invalid_storage_uris = (
        expected_uri.replace("file://", "file://evil", 1),
        f"{expected_uri}?version=1",
        f"{expected_uri}#fragment",
        noncanonical_path.as_uri(),
    )

    for invalid_storage_uri in invalid_storage_uris:
        proof["documents"][0]["storage_uri"] = invalid_storage_uri
        with pytest.raises(catalog_types.UHCFileCatalogError, match="storage URI"):
            artifacts.validate_retained_raw_proof(proof)
    proof["documents"][0]["storage_uri"] = expected_uri
    assert artifacts.validate_retained_raw_proof(proof) == proof


def test_catalog_artifact_root_is_required_and_cannot_be_temporary(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", raising=False)
    with pytest.raises(catalog_types.UHCFileCatalogError, match="requires"):
        artifacts.catalog_artifact_root()
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT",
        str(Path(artifacts.tempfile.gettempdir()) / "catalog"),
    )
    with pytest.raises(catalog_types.UHCFileCatalogError, match="temporary"):
        artifacts.catalog_artifact_root()


def test_catalog_artifact_root_accepts_explicit_durable_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(tmp_path))
    monkeypatch.setattr(artifacts.tempfile, "gettempdir", lambda: "/unrelated-temp")

    assert artifacts.catalog_artifact_root() == tmp_path / "uhc-file-catalog"


def test_catalog_artifact_root_converts_resolution_and_permission_errors(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(tmp_path))
    monkeypatch.setattr(artifacts.tempfile, "gettempdir", lambda: "/unrelated-temp")
    original_resolve = artifacts.Path.resolve

    def fail_resolution(_path):
        raise OSError("storage resolution denied")

    monkeypatch.setattr(artifacts.Path, "resolve", fail_resolution)
    with pytest.raises(catalog_types.UHCFileCatalogError, match="storage is unavailable"):
        artifacts.catalog_artifact_root()
    monkeypatch.setattr(artifacts.Path, "resolve", original_resolve)

    original_mkdir = artifacts.Path.mkdir

    def fail_catalog_mkdir(path, *args, **kwargs):
        if path.name == "uhc-file-catalog":
            raise PermissionError("storage creation denied")
        return original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(artifacts.Path, "mkdir", fail_catalog_mkdir)
    with pytest.raises(catalog_types.UHCFileCatalogError, match="storage is unavailable"):
        artifacts.catalog_artifact_root()


def test_snapshot_retention_is_content_addressed_and_idempotent(monkeypatch, tmp_path):
    catalog_root = tmp_path / "durable-catalog"
    monkeypatch.setattr(artifacts, "catalog_artifact_root", lambda: catalog_root)
    snapshot = raw_catalog_snapshot()

    first_proof = artifacts.retain_catalog_snapshot(snapshot)
    second_proof = artifacts.retain_catalog_snapshot(snapshot)

    assert artifacts.validate_retained_raw_proof(first_proof)["raw_set_sha256"] == (
        snapshot.raw_set_sha256
    )
    assert second_proof == first_proof
    assert len((catalog_root / "manifest.jsonl").read_text().splitlines()) == 4


def test_snapshot_retention_fsyncs_each_exact_rename_parent(monkeypatch, tmp_path):
    catalog_root = tmp_path / "durable-catalog"
    monkeypatch.setattr(artifacts, "catalog_artifact_root", lambda: catalog_root)
    directory_sync = Mock()
    monkeypatch.setattr(artifacts, "_fsync_directory", directory_sync)

    proof = artifacts.retain_catalog_snapshot(raw_catalog_snapshot())

    store = PTG2ArtifactStore(catalog_root)
    expected_parents = [
        store.path_from_uri(document["storage_uri"]).parent
        for document in proof["documents"]
    ]
    assert [call.args[0] for call in directory_sync.call_args_list] == expected_parents


def test_snapshot_retention_converts_fsync_and_manifest_storage_errors(
    monkeypatch,
    tmp_path,
):
    catalog_root = tmp_path / "durable-catalog"
    monkeypatch.setattr(artifacts, "catalog_artifact_root", lambda: catalog_root)

    def fail_fsync(_descriptor):
        raise OSError("fsync denied")

    monkeypatch.setattr(artifacts.os, "fsync", fail_fsync)
    with pytest.raises(catalog_types.UHCFileCatalogError, match="storage is unavailable"):
        artifacts.retain_catalog_snapshot(raw_catalog_snapshot())

    monkeypatch.undo()
    monkeypatch.setattr(artifacts, "catalog_artifact_root", lambda: catalog_root)
    monkeypatch.setattr(
        PTG2ArtifactStore,
        "record_manifest",
        Mock(side_effect=OSError("manifest denied")),
    )
    with pytest.raises(catalog_types.UHCFileCatalogError, match="storage is unavailable"):
        artifacts.retain_catalog_snapshot(raw_catalog_snapshot())


def test_retained_catalog_proof_decodes_exact_rehashed_semantics(monkeypatch, tmp_path):
    proof = _retained_proof(monkeypatch, tmp_path)

    normalized_proof, retained_catalog = artifacts.validate_retained_catalog_proof(
        proof
    )

    assert normalized_proof == proof
    assert len(retained_catalog.files) == 102
    assert retained_catalog.catalog_set_sha256 == catalog_types.catalog_set_sha256(
        retained_catalog.files,
        retained_catalog.collection_summary,
    )


def test_retained_catalog_proof_rejects_hash_valid_non_json(monkeypatch, tmp_path):
    proof = _retained_proof(monkeypatch, tmp_path)
    store = PTG2ArtifactStore(tmp_path / "durable-catalog")
    invalid_bytes = b"not-json"
    invalid_digest = hashlib.sha256(invalid_bytes).hexdigest()
    invalid_path = store.artifact_path(
        invalid_digest,
        kind=artifacts.CATALOG_ARTIFACT_KIND,
        suffix=".json",
    )
    invalid_path.parent.mkdir(parents=True, exist_ok=True)
    invalid_path.write_bytes(invalid_bytes)
    proof["documents"][0].update(
        {
            "raw_sha256": invalid_digest,
            "byte_count": len(invalid_bytes),
            "storage_uri": store.storage_uri(invalid_path),
        }
    )
    proof["raw_set_sha256"] = artifacts.raw_set_sha256_from_documents(
        proof["documents"]
    )
    with pytest.raises(catalog_types.UHCFileCatalogError, match="not exact JSON"):
        artifacts.validate_retained_catalog_proof(proof)


def test_retained_catalog_proof_converts_deep_json_recursion(monkeypatch, tmp_path):
    proof = _retained_proof(monkeypatch, tmp_path)
    store = PTG2ArtifactStore(tmp_path / "durable-catalog")
    invalid_bytes = (b"[" * 2_000) + (b"]" * 2_000)
    invalid_digest = hashlib.sha256(invalid_bytes).hexdigest()
    invalid_path = store.artifact_path(
        invalid_digest,
        kind=artifacts.CATALOG_ARTIFACT_KIND,
        suffix=".json",
    )
    invalid_path.parent.mkdir(parents=True, exist_ok=True)
    invalid_path.write_bytes(invalid_bytes)
    proof["documents"][0].update(
        {
            "raw_sha256": invalid_digest,
            "byte_count": len(invalid_bytes),
            "storage_uri": store.storage_uri(invalid_path),
        }
    )
    proof["raw_set_sha256"] = artifacts.raw_set_sha256_from_documents(
        proof["documents"]
    )
    monkeypatch.setattr(
        artifacts.json,
        "loads",
        lambda _raw_bytes: (_ for _ in ()).throw(RecursionError("too deep")),
    )

    with pytest.raises(catalog_types.UHCFileCatalogError, match="not exact JSON"):
        artifacts.validate_retained_catalog_proof(proof)


def test_snapshot_retention_rejects_corrupt_reuse_and_changed_set(monkeypatch, tmp_path):
    catalog_root = tmp_path / "durable-catalog"
    monkeypatch.setattr(artifacts, "catalog_artifact_root", lambda: catalog_root)
    snapshot = raw_catalog_snapshot()
    proof = artifacts.retain_catalog_snapshot(snapshot)
    store = PTG2ArtifactStore(catalog_root)
    retained_path = store.path_from_uri(proof["documents"][0]["storage_uri"])

    with pytest.raises(catalog_types.UHCFileCatalogError, match="set proof changed"):
        artifacts.retain_catalog_snapshot(
            raw_catalog_snapshot(raw_set_sha256="f" * 64)
        )
    retained_path.write_bytes(b"corrupt")
    with pytest.raises(catalog_types.UHCFileCatalogError, match="corrupt"):
        artifacts.retain_catalog_snapshot(snapshot)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda proof: None, "proof is invalid"),
        (lambda proof: {**proof, "raw_set_sha256": "bad"}, "proof is invalid"),
        (
            lambda proof: {**proof, "documents": [None, proof["documents"][1]]},
            "document is invalid",
        ),
        (
            lambda proof: {
                **proof,
                "documents": [proof["documents"][0], proof["documents"][0]],
            },
            "families are invalid",
        ),
        (
            lambda proof: {
                **proof,
                "documents": [
                    {**proof["documents"][0], "url": catalog_types.CATALOG_URLS["ifp"]},
                    proof["documents"][1],
                ],
            },
            "URL does not match",
        ),
        (
            lambda proof: {
                **proof,
                "documents": [
                    {**proof["documents"][0], "byte_count": True},
                    proof["documents"][1],
                ],
            },
            "identity is invalid",
        ),
        (
            lambda proof: {
                **proof,
                "documents": [
                    {**proof["documents"][0], "storage_uri": "https://example.com/a"},
                    proof["documents"][1],
                ],
            },
            "storage URI is invalid",
        ),
        (lambda proof: {**proof, "raw_set_sha256": "f" * 64}, "set proof changed"),
    ],
)
def test_retained_proof_rejects_invalid_boundaries(
    monkeypatch,
    tmp_path,
    mutation,
    message,
):
    proof = _retained_proof(monkeypatch, tmp_path)
    invalid_proof = mutation(proof)

    with pytest.raises(catalog_types.UHCFileCatalogError, match=message):
        artifacts.validate_retained_raw_proof(invalid_proof)


def test_retained_proof_rejects_missing_or_corrupt_artifact(monkeypatch, tmp_path):
    proof = _retained_proof(monkeypatch, tmp_path)
    store = PTG2ArtifactStore(tmp_path / "durable-catalog")
    retained_path = store.path_from_uri(proof["documents"][0]["storage_uri"])
    retained_path.unlink()

    with pytest.raises(catalog_types.UHCFileCatalogError, match="unavailable"):
        artifacts.validate_retained_raw_proof(proof)

    retained_path.write_bytes(b"wrong")
    with pytest.raises(catalog_types.UHCFileCatalogError, match="corrupt"):
        artifacts.validate_retained_raw_proof(proof)


def test_retained_proof_converts_store_initialization_error(monkeypatch, tmp_path):
    proof = _retained_proof(monkeypatch, tmp_path)
    monkeypatch.setattr(
        artifacts,
        "PTG2ArtifactStore",
        Mock(side_effect=OSError("artifact directories unavailable")),
    )

    with pytest.raises(catalog_types.UHCFileCatalogError, match="storage is unavailable"):
        artifacts.validate_retained_raw_proof(proof)
