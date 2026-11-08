# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen identity streaming is bounded and rolls back incomplete families."""

import hashlib
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import cms_npd_source
from process import provider_directory_cms_prepared_identity_stream as stream
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_cms_prepared_workspace import CMSPreparedWorkspace
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff
from tests.test_provider_directory_cms_source_handoff import _identity


def frozen_identity_rows(document_by_field):
    """Keep the exact two raw resources and hashes for each fixed family."""
    rows_by_family = {}
    for family in stream._FAMILIES:
        resources = [{"resourceType": family, "id": key} for key in ("a", "b")]
        rows_by_family[family] = [
            dict(
                resource_id=resource_by_field["id"],
                source_id="cms-npd",
                release_id=document_by_field["identity"]["metadata"]["source_release"]["vector_sha256"],
                raw_payload_json=storage._encoded(resource_by_field),
                raw_payload_sha256=hashlib.sha256(storage._encoded(resource_by_field).encode()).hexdigest(),
            )
            for resource_by_field in resources
        ]
    return rows_by_family


@pytest.fixture
def streaming(monkeypatch):
    """Provide a bounded frozen reader and caller-owned rollback events."""
    document_by_field = {
        "identity": _identity(),
        "tables": {storage.WITNESS: {"name": "frozen_witness", "oid": 17}},
    }
    handoff = FHIRSourceCandidateHandoff(storage._encoded(document_by_field))
    workspace = object.__new__(CMSPreparedWorkspace)
    workspace.policy = SimpleNamespace(schema="private_source")
    workspace._binding = {"identity": document_by_field["identity"], "handoff_sha256": handoff.document_sha256}
    workspace._receipts = {}
    workspace._indexed = False
    workspace._inputs = {"frozen_inventory": {"relations": [{"relation_oid": 17, "relation_name": "frozen_witness"}]}}
    workspace.verify_current = AsyncMock()
    events, calls = [], []
    rows_by_family = frozen_identity_rows(document_by_field)
    release = document_by_field["identity"]["metadata"]["source_release"]

    async def fetch(query, dataset, family, after, limit, byte_limit):
        assert 'FROM "private_source"."frozen_witness"' in query
        assert dataset == document_by_field["identity"]["dataset_id"]
        assert limit == 1_000 and byte_limit == 24 * 1024 * 1024
        calls.append((family, after))
        return [row for row in rows_by_family[family] if row["resource_id"] > after][:1]

    workspace.connection = SimpleNamespace(fetch=fetch)

    @asynccontextmanager
    async def savepoint(connection):
        assert connection is workspace.connection
        events.append("entered")
        try:
            yield
        except BaseException:
            events.append("rolled_back")
            raise
        else:
            events.append("released")

    staged = AsyncMock()
    monkeypatch.setattr(stream, "_savepoint", savepoint)
    monkeypatch.setattr(stream, "_stage_batch", staged)
    monkeypatch.setattr(stream, "_frozen_coordinates", AsyncMock())
    return SimpleNamespace(
        workspace=workspace,
        handoff=handoff,
        rows=rows_by_family,
        calls=calls,
        events=events,
        staged=staged,
        release=release,
    )


@pytest.mark.asyncio
async def test_stream_has_exact_family_order_and_keyset_pages(streaming):
    state = streaming
    before = state.handoff.document_json
    result = await stream.stage_prepared_identities(state.workspace, state.handoff)
    assert result == {
        "handoff_sha256": state.handoff.document_sha256,
        "resource_counts": dict.fromkeys(stream._FAMILIES, 2),
    }
    assert state.calls == [(family, after) for family in stream._FAMILIES for after in ("", "a", "b")]
    assert [call.args[1] for call in state.staged.await_args_list] == [
        family for family in stream._FAMILIES for _ in range(2)
    ]
    assert state.events == ["entered", "released"] and state.handoff.document_json == before


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["source", "release", "hash", "type", "id"])
async def test_frozen_row_drift_refuses_before_private_batch(streaming, change):
    state = streaming
    row = state.rows["Organization"][0]
    if change in {"source", "release", "hash"}:
        row[{"source": "source_id", "release": "release_id", "hash": "raw_payload_sha256"}[change]] = "changed"
    else:
        resource_by_field = {
            "resourceType": "Location" if change == "type" else "Organization",
            "id": "other" if change == "id" else "a",
        }
        row["raw_payload_json"] = storage._encoded(resource_by_field)
    with pytest.raises(ValueError, match="raw_.*changed"):
        await stream.stage_prepared_identities(state.workspace, state.handoff)
    state.staged.assert_not_awaited()
    assert state.events == ["entered", "rolled_back"]


@pytest.mark.asyncio
async def test_missing_family_rows_rollback_prior_private_batches(streaming):
    state = streaming
    state.rows["InsurancePlan"] = []
    with pytest.raises(ValueError, match="family_count_changed"):
        await stream.stage_prepared_identities(state.workspace, state.handoff)
    assert state.staged.await_count == 4
    assert state.events == ["entered", "rolled_back"]


@pytest.mark.asyncio
async def test_replaced_handoff_or_frozen_oid_refuses_before_sql(streaming):
    state = streaming
    document = deepcopy(state.handoff.document)
    document["tables"][storage.WITNESS]["oid"] = 18
    handoff = FHIRSourceCandidateHandoff(storage._encoded(document))
    with pytest.raises(ValueError, match="handoff_changed"):
        await stream.stage_prepared_identities(state.workspace, handoff)
    state.workspace._binding["handoff_sha256"] = handoff.document_sha256
    with pytest.raises(ValueError, match="witness_changed"):
        await stream.stage_prepared_identities(state.workspace, handoff)
    assert state.calls == [] and state.events == []


@pytest.mark.asyncio
async def test_batch_router_uses_only_family_specific_prepared_writers(monkeypatch):
    entity, resource, network = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(stream.identity, "stage_prepared_entity_batch", entity)
    monkeypatch.setattr(stream.identity, "stage_prepared_resource_identity_batch", resource)
    monkeypatch.setattr(stream.network, "record_prepared_network_batch", network)
    workspace, rows = object(), [{"id": "one"}]
    for family in stream._FAMILIES:
        await stream._stage_batch(workspace, family, rows)
    assert entity.await_count == resource.await_count == network.await_count == 2
    assert [call.kwargs["resource_type"] for call in network.await_args_list] == ["Organization", "InsurancePlan"]
    assert [call.kwargs["resource_type"] for call in resource.await_args_list] == ["InsurancePlan", "PractitionerRole"]
