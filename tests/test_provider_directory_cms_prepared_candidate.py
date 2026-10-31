# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Composition boundaries preserve admission data and the serving contract."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_prepared_candidate as candidate
from tests.test_provider_directory_cms_source_handoff import _identity


def _header():
    header = dict.fromkeys(candidate._HEADER_COLUMNS)
    identity = _identity()
    header.update({key: identity[key] for key in ("dataset_id", "endpoint_id", "acquisition_root_run_id")})
    header.update(
        dataset_hash="a" * 64,
        status="validated",
        is_current=False,
        resource_count=16,
        publication_metadata_json={**identity["metadata"], "admitted_proof": {"preserved": True}},
        publication_metadata_summary_json={"source_ids": ["cms-npd"]},
        content_proof_resource_types=identity["metadata"]["selected_resources"],
        validated_at=datetime(2026, 1, 1),
    )
    return header


def test_complete_header_normalizes_json_without_discarding_admitted_fields():
    original = _header()
    encoded_by_field = {
        key: json.dumps(value) if key in candidate._JSON_COLUMNS and value is not None else value
        for key, value in original.items()
    }
    assert candidate._header(encoded_by_field) == original
    with pytest.raises(ValueError, match="header_layout_changed"):
        candidate._header({key: value for key, value in encoded_by_field.items() if key != "import_run_id"})


@pytest.mark.parametrize("change", ["summary", "resources", "published", "superseded"])
async def test_frozen_header_refuses_malformed_or_non_candidate_state_before_allocation(monkeypatch, change):
    header = _header()
    if change == "summary":
        header["publication_metadata_summary_json"] = None
    elif change == "resources":
        header["content_proof_resource_types"] = ["Organization"]
    else:
        header[change + "_at"] = datetime(2026, 1, 2)
    handoff = SimpleNamespace(
        document={
            "identity": _identity(),
            "contract": candidate.storage.CMS_ISOLATED_CONTRACT,
            "tables": {candidate.storage.HEADER: {"name": "frozen_header"}},
        },
        document_sha256="a" * 64,
    )
    workspace = SimpleNamespace(
        verify_current=AsyncMock(),
        binding={"indexed": True, "handoff_sha256": handoff.document_sha256},
        identity=handoff.document["identity"],
        catalog_schema="catalog",
        policy=SimpleNamespace(schema="private"),
        connection=SimpleNamespace(execute=AsyncMock()),
    )
    monkeypatch.setattr(candidate, "_frozen_coordinates", AsyncMock())
    monkeypatch.setattr(candidate.completion, "_frozen_handoff_header", AsyncMock(return_value=header))
    monkeypatch.setattr(candidate.coverage, "_schema_name", lambda: '"catalog"')
    with pytest.raises(ValueError, match="frozen_header_changed"):
        await candidate._inputs(workspace, handoff)
    workspace.connection.execute.assert_not_called()


async def test_existing_header_is_refused_before_any_write():
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=True), execute=AsyncMock())
    workspace = SimpleNamespace(connection=connection, catalog_schema="catalog")
    with pytest.raises(ValueError, match="canonical_header_exists"):
        await candidate._allocate_header(
            workspace.connection, workspace.catalog_schema, '"private"."frozen"', _header()
        )
    connection.execute.assert_not_called()


async def test_descriptor_stays_exact_existing_contract_and_rejects_ambiguous_incumbent():
    header = _header()
    incumbent_by_field = {
        **header,
        "dataset_id": "incumbent",
        "acquisition_root_run_id": "old-root",
        "is_current": True,
        "status": "published",
        "published_at": datetime(2026, 1, 1),
    }
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[incumbent_by_field]))
    workspace = SimpleNamespace(connection=connection, catalog_schema="catalog")
    coverage_proof_by_field = {"release_id": "b" * 64, "proof_version": 2}
    descriptor = await candidate._descriptor(workspace, header, coverage_proof_by_field)
    assert set(descriptor) == {
        "version",
        "status",
        "desired_cms_dataset",
        "expected_cms_incumbent",
        "release_id",
        "proof_version",
    }
    assert descriptor["expected_cms_incumbent"]["dataset_id"] == "incumbent"
    connection.fetch.return_value = [incumbent_by_field, {**incumbent_by_field, "dataset_id": "another"}]
    with pytest.raises(RuntimeError, match="incumbent_ambiguous"):
        await candidate._descriptor(workspace, header, coverage_proof_by_field)


async def test_finish_detaches_allocation_before_awaiting_frozen_inputs(monkeypatch):
    from process.provider_directory_cms_prepared_allocation import CONTRACT

    binding_by_field = {"preparation_id": "original"}
    allocation_by_field = {
        "contract": CONTRACT,
        "workspace": deepcopy(binding_by_field),
        "transaction": "100",
        "header_xmin": "101",
        "inventory": [],
    }
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=False), execute=AsyncMock())
    workspace = SimpleNamespace(binding=binding_by_field, identity=_identity(), connection=connection)

    async def change_caller_receipt(*args):
        allocation_by_field["workspace"]["preparation_id"] = "changed-during-await"
        return '"private"."frozen"', _header()

    @asynccontextmanager
    async def savepoint(_):
        yield

    monkeypatch.setattr(candidate, "_inputs", change_caller_receipt)
    monkeypatch.setattr(candidate, "_savepoint", savepoint)
    with pytest.raises(ValueError, match="allocation_not_committed"):
        await candidate.finish_prepared_cms_candidate(workspace, None, allocation=allocation_by_field)
    assert allocation_by_field["workspace"] != binding_by_field
    connection.fetchval.assert_awaited_once()
    connection.execute.assert_not_called()
