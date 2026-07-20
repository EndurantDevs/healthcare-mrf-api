# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
from unittest.mock import Mock

import pytest

from process import uhc_provider_file_catalog as catalog_module
from process import uhc_provider_file_catalog_artifacts as artifacts
from process import uhc_provider_file_catalog_store as catalog_store
from process import uhc_provider_file_catalog_types as catalog_types
from process.ptg_parts.artifacts import PTG2ArtifactStore
from tests.uhc_provider_file_catalog_test_data import (
    live_catalog_payloads,
    raw_catalog_snapshot,
)


def _retained_proof(monkeypatch, tmp_path, payloads_by_family):
    artifact_root = tmp_path / "durable-catalog"
    monkeypatch.setattr(artifacts, "catalog_artifact_root", lambda: artifact_root)
    proof = artifacts.retain_catalog_snapshot(
        raw_catalog_snapshot(payloads_by_family=payloads_by_family)
    )
    return artifact_root, proof


def _changed_payloads():
    payloads_by_family = live_catalog_payloads()
    payloads_by_family["cs"]["providers"][0]["size"] += 1
    return payloads_by_family


class _CatalogDatabase:
    def __init__(self, catalog, proof):
        observed_at = dt.datetime.now(dt.UTC)
        self.observation_fields = {
            "catalog_set_sha256": catalog.catalog_set_sha256,
            "schema_version": 1,
            "families_json": sorted(catalog_types.CATALOG_URLS),
            "collection_summary_json": catalog_store._source_collection_summary(
                catalog
            ),
            **catalog_store._catalog_counts(catalog),
            "raw_set_sha256": proof["raw_set_sha256"],
            "raw_documents_json": proof["documents"],
            "observation_first_observed_at": observed_at,
            "observation_last_observed_at": observed_at,
        }
        self.file_records = catalog_store._file_parameters(catalog)

    async def first(self, _statement, **_parameters):
        return self.observation_fields

    async def all(self, _statement, **_parameters):
        return self.file_records


@pytest.mark.asyncio
async def test_record_rejects_valid_raw_semantics_mismatch_before_transaction(
    monkeypatch,
    tmp_path,
):
    catalog = catalog_types.observed_catalog_from_payloads(live_catalog_payloads())
    _artifact_root, mismatched_proof = _retained_proof(
        monkeypatch,
        tmp_path,
        _changed_payloads(),
    )
    transaction = Mock(side_effect=AssertionError("transaction must not start"))
    monkeypatch.setattr(catalog_store.db, "transaction", transaction)

    with pytest.raises(catalog_types.UHCFileCatalogError, match="does not match"):
        await catalog_store.record_catalog_observation(catalog, mismatched_proof)

    transaction.assert_not_called()


@pytest.mark.asyncio
async def test_record_rejects_tampered_raw_bytes_before_transaction(monkeypatch, tmp_path):
    catalog = catalog_types.observed_catalog_from_payloads(live_catalog_payloads())
    artifact_root, proof = _retained_proof(
        monkeypatch,
        tmp_path,
        live_catalog_payloads(),
    )
    store = PTG2ArtifactStore(artifact_root)
    retained_path = store.path_from_uri(proof["documents"][0]["storage_uri"])
    retained_path.write_bytes(b"tampered")
    transaction = Mock(side_effect=AssertionError("transaction must not start"))
    monkeypatch.setattr(catalog_store.db, "transaction", transaction)

    with pytest.raises(catalog_types.UHCFileCatalogError, match="corrupt"):
        await catalog_store.record_catalog_observation(catalog, proof)

    transaction.assert_not_called()


@pytest.mark.asyncio
async def test_read_fails_when_raw_semantics_do_not_match_linked_catalog(
    monkeypatch,
    tmp_path,
):
    catalog = catalog_types.observed_catalog_from_payloads(live_catalog_payloads())
    _artifact_root, mismatched_proof = _retained_proof(
        monkeypatch,
        tmp_path,
        _changed_payloads(),
    )
    monkeypatch.setattr(
        catalog_module,
        "db",
        _CatalogDatabase(catalog, mismatched_proof),
    )

    with pytest.raises(catalog_types.UHCFileCatalogError, match="persisted semantics"):
        await catalog_module.uhc_provider_file_catalog()


@pytest.mark.asyncio
async def test_read_fails_when_linked_raw_artifact_is_tampered(monkeypatch, tmp_path):
    catalog = catalog_types.observed_catalog_from_payloads(live_catalog_payloads())
    artifact_root, proof = _retained_proof(
        monkeypatch,
        tmp_path,
        live_catalog_payloads(),
    )
    store = PTG2ArtifactStore(artifact_root)
    retained_path = store.path_from_uri(proof["documents"][0]["storage_uri"])
    retained_path.write_bytes(b"tampered")
    monkeypatch.setattr(catalog_module, "db", _CatalogDatabase(catalog, proof))

    with pytest.raises(catalog_types.UHCFileCatalogError, match="corrupt"):
        await catalog_module.uhc_provider_file_catalog()
