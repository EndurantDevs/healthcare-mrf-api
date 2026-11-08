# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact source acquisition lineage, bounded reads and immutable candidate projection_map."""

from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql

from api import provider_directory_cms_candidate_catalog as candidate
from api import provider_directory_control_catalog as catalog
from api.provider_directory_cms_generation import RESOURCE_FILES
from process.provider_directory_admission_seal import ADMISSION_GENERIC_PROOF_SUMMARY_KEY
from process.provider_directory_profile_selection_contract import PROFILE_SELECTION_LINEAGE_AUTHORITY

CATALOG = {"items": [{"source_ids": ["cms-npd"], "runnable": True, "profile_enabled": True}]}
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _dataset():
    """Create an eight-member admitted synthetic parent with compact proof counts_by_type."""
    counts_by_type = {kind: 1 for kind in RESOURCE_FILES.values()}
    release_map = {
        "source_id": "cms-npd",
        "manifest_sha256": "b" * 64,
        "vector_sha256": "c" * 64,
        "generated_at": "2026-01-01",
        "files": {
            name: {"sha256": "d" * 64, "compressed_bytes": 1, "original_bytes": 1, "row_count": 1, "distinct_count": 1}
            for name in RESOURCE_FILES
        },
    }
    return {
        "dataset_id": "dataset",
        "endpoint_id": "endpoint",
        "acquisition_root_run_id": "owner",
        "import_run_id": "owner",
        "dataset_hash": "a" * 64,
        "status": "validated",
        "is_current": False,
        "validated_at": NOW,
        "published_at": None,
        "superseded_at": None,
        "resource_count": 8,
        "publication_metadata": {
            "source_ids": ["cms-npd"],
            "source_release": release_map,
            ADMISSION_GENERIC_PROOF_SUMMARY_KEY: {
                "resource_counts": counts_by_type,
                "resource_hashes": {kind: "e" * 64 for kind in counts_by_type},
                "dataset_hash": "a" * 64,
                "resource_count": 8,
            },
        },
    }


def _pair(dataset):
    return {
        **{key: dataset[key] for key in candidate._PIN_FIELDS - {"source_id"}},
        "source_id": "cms-npd",
        "publication_status": dataset["status"],
        "is_current": dataset["is_current"],
        "lineage_authority": PROFILE_SELECTION_LINEAGE_AUTHORITY,
    }


def _run(run_id, dataset, *, parent=None, status="succeeded"):
    return {
        "run_id": run_id,
        "retry_of_run_id": parent,
        "importer": "provider-directory-fhir",
        "engine": "healthcare-mrf-api",
        "node_id": "test-node",
        "status": status,
        "finished_at": NOW,
        "params": {"source_ids": ["cms-npd"], "import_resources": True},
        "candidate": {
            "version": 1,
            "status": "ready",
            "desired_cms_dataset": _pair(dataset),
            "expected_cms_incumbent": None,
            "release_id": "c" * 64,
            "proof_version": 2,
        },
    }


class _Session:
    """Serve selected scalar rows while retaining every SQL statement for boundedness assertions."""

    def __init__(self, dataset, runs):
        self.datasets = [dataset]
        self.runs = runs
        self.statements = []
        self.covered = True

    async def execute(self, statement):
        compiled = statement.compile(dialect=postgresql.dialect())
        self.statements.append(str(compiled))
        params = compiled.params
        rows = []
        if "status_1" in params:
            rows = [row for row in self.datasets if row["status"] == params["status_1"]]
        elif "run_id_1" in params:
            rows = [row for row in self.runs if row["run_id"] == params["run_id_1"]]
        elif "retry_of_run_id_1" in params:
            rows = [row for row in self.runs if row["retry_of_run_id"] == params["retry_of_run_id_1"]]
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))

    async def scalar(self, statement, params):
        self.statements.append(str(statement))
        return self.covered


def _install(monkeypatch, session):
    @asynccontextmanager
    async def transaction():
        yield session

    monkeypatch.setattr(candidate.db, "transaction", transaction)


@pytest.mark.asyncio
async def test_failed_immutable_owner_uses_exact_succeeded_child(monkeypatch):
    dataset = _dataset()
    session = _Session(dataset, [_run("owner", dataset, status="failed"), _run("leaf", dataset, parent="owner")])
    _install(monkeypatch, session)
    before = deepcopy(dataset)
    result = await candidate.cms_serving_candidate(CATALOG)
    assert result["acquisition_run_id"] == "leaf"
    assert result["desired_cms_dataset"]["acquisition_root_run_id"] == "owner"
    assert dataset == before
    assert len(session.statements) == 8
    retry_statements = [sql for sql in session.statements if "retry_of_run_id =" in sql]
    assert len(retry_statements) == 2
    assert all("importer =" in sql and "LIMIT" in sql for sql in retry_statements)
    assert not any("provider_directory_dataset_resource" in sql for sql in session.statements)


@pytest.mark.asyncio
async def test_retry_child_may_be_dataset_creation_anchor(monkeypatch):
    dataset = _dataset()
    session = _Session(dataset, [_run("initial", dataset, status="failed"), _run("owner", dataset, parent="initial")])
    _install(monkeypatch, session)
    assert (await candidate.cms_serving_candidate(CATALOG))["acquisition_run_id"] == "owner"


@pytest.mark.asyncio
async def test_unrelated_manual_same_byte_success_cannot_replace_failed_owner(monkeypatch):
    dataset = _dataset()
    session = _Session(dataset, [_run("owner", dataset, status="failed"), _run("manual", dataset)])
    _install(monkeypatch, session)
    with pytest.raises(ValueError, match="owner_missing"):
        await candidate.cms_serving_candidate(CATALOG)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["cycle", "ambiguous", "owner", "depth", "active", "node", "probe"])
async def test_invalid_retry_lineage_fails_closed(monkeypatch, fault):
    dataset = _dataset()
    owner, leaf = _run("owner", dataset, status="failed"), _run("leaf", dataset, parent="owner")
    session = _Session(dataset, [owner, leaf])
    _install(monkeypatch, session)
    match fault:
        case "cycle":
            owner["retry_of_run_id"] = "leaf"
        case "ambiguous":
            session.runs.append(_run("other", dataset, parent="owner"))
        case "owner":
            dataset["import_run_id"] = "manual"
        case "depth":
            monkeypatch.setattr(candidate, "_MAX_LINEAGE", 1)
        case "active":
            leaf.update(status="running", finished_at=None)
        case "node":
            leaf["node_id"] = "other-node"
        case "probe":
            leaf["params"]["probe"] = True
    with pytest.raises(ValueError):
        await candidate.cms_serving_candidate(CATALOG)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["extra", "hash", "release", "version", "missing_metric", "coverage", "ambiguous"])
async def test_candidate_proof_mismatch_is_not_projected(monkeypatch, fault):
    dataset = _dataset()
    run = _run("owner", dataset)
    session = _Session(dataset, [run])
    _install(monkeypatch, session)
    match fault:
        case "extra":
            run["candidate"]["extra"] = True
        case "hash":
            run["candidate"]["desired_cms_dataset"]["dataset_hash"] = "f" * 64
        case "release":
            run["candidate"]["release_id"] = "f" * 64
        case "version":
            run["candidate"]["version"] = True
        case "missing_metric":
            run["candidate"] = None
        case "coverage":
            session.covered = False
        case "ambiguous":
            session.datasets.append(deepcopy(dataset))
    with pytest.raises(ValueError):
        await candidate.cms_serving_candidate(CATALOG)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_receipt", [False, True])
async def test_current_progression_retains_original_lineage_and_requires_common_receipt(monkeypatch, has_receipt):
    dataset = _dataset()
    run = _run("owner", dataset)
    dataset.update(status="published", is_current=True, published_at=NOW)
    session = _Session(dataset, [run, _run("unrelated", dataset)])
    _install(monkeypatch, session)
    pin_map = {key: _pair(dataset)[key] for key in candidate._PIN_FIELDS}
    receipt_map = {"payload": {"cms": {**pin_map, "release_id": "c" * 64, "proof_version": 2}}}
    # Accepted native authority remains usable when independent build inputs drift.
    receipt_map["payload"].update(alias_generation=1, desired_datasets=[{"source_id": "other", "dataset_id": "prior"}])
    monkeypatch.setattr(candidate, "read_serving_receipt", AsyncMock(return_value=receipt_map if has_receipt else None))
    if not has_receipt:
        with pytest.raises(ValueError, match="serving_receipt_missing"):
            await candidate.cms_serving_candidate(CATALOG)
        return
    result = await candidate.cms_serving_candidate(CATALOG)
    assert result["acquisition_run_id"] == "owner"
    assert result["desired_cms_dataset"] == result["expected_cms_incumbent"] == _pair(dataset)
    assert run["candidate"]["desired_cms_dataset"]["is_current"] is False


@pytest.mark.asyncio
async def test_metric_incumbent_is_re_resolved_from_current_snapshot(monkeypatch):
    dataset = _dataset()
    run = _run("owner", dataset)
    session = _Session(dataset, [run])
    incumbent_map = {
        **deepcopy(dataset),
        "dataset_id": "incumbent",
        "acquisition_root_run_id": "prior",
        "status": "published",
        "is_current": True,
        "published_at": NOW,
    }
    session.datasets.append(incumbent_map)
    _install(monkeypatch, session)
    result = await candidate.cms_serving_candidate(CATALOG)
    assert result["expected_cms_incumbent"] == _pair(incumbent_map)
    assert run["candidate"]["expected_cms_incumbent"] is None


@pytest.mark.asyncio
async def test_optional_candidate_timeout_preserves_other_catalog_fields(monkeypatch):
    import asyncio

    async def blocked(_catalog):
        await asyncio.Event().wait()

    monkeypatch.setattr(catalog, "provider_directory_source_catalog", lambda: CATALOG)
    monkeypatch.setattr(catalog, "current_profile_selection_request", AsyncMock(return_value=None))
    monkeypatch.setattr(catalog, "enrich_provider_directory_source_catalog", AsyncMock(return_value=CATALOG))
    monkeypatch.setattr(catalog, "cms_serving_candidate", blocked)
    monkeypatch.setattr(catalog, "_CMS_CANDIDATE_TIMEOUT_SECONDS", 0.01)
    assert await catalog.provider_directory_control_catalog() == CATALOG


@pytest.mark.asyncio
@pytest.mark.parametrize("failed", [False, True])
async def test_control_catalog_adds_only_verified_optional_candidate(monkeypatch, failed):
    projection_map = {"acquisition_run_id": "leaf"}
    monkeypatch.setattr(catalog, "provider_directory_source_catalog", lambda: CATALOG)
    monkeypatch.setattr(catalog, "current_profile_selection_request", AsyncMock(return_value=None))
    monkeypatch.setattr(catalog, "enrich_provider_directory_source_catalog", AsyncMock(return_value=CATALOG))
    monkeypatch.setattr(
        catalog,
        "cms_serving_candidate",
        AsyncMock(return_value=projection_map, side_effect=ValueError("invalid") if failed else None),
    )
    result = await catalog.provider_directory_control_catalog()
    assert result.get("cms_serving_candidate") == (None if failed else projection_map)
