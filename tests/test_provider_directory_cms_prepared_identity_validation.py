# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared identity checks retain exact scope and remain read-only."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import provider_directory_cms_prepared_identity_validation as prepared
from process import provider_directory_generic_source_policy as source
from process.provider_directory_resource_identity import source_resource_uuid
from tests.test_provider_directory_cms_prepared_relationships import case, graph


@pytest.fixture
def validation(graph):
    graph.workspace._indexed = True
    graph.identities = []

    async def fetch(query, *after):
        root = prepared.resources.RESOURCE_IDENTITY_ROOT
        name = graph.workspace.receipts[root]["name"]
        if 'FROM "private_source"."' + name + '" candidate' not in query:
            return []
        return [
            {**row, "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc), "prepared_row_bytes": 200}
            for row in graph.identities
            if tuple(row[key] for key in ("source_id", "resource_type", "resource_id")) > tuple(after)
        ]

    graph.connection.fetch = AsyncMock(side_effect=fetch)
    return graph


async def test_verifies_five_maps_without_context_or_write_with_detached_receipt(validation):
    state = validation
    ambient = (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get())
    before = deepcopy(state.workspace.receipts)
    report = await prepared.assert_prepared_cms_identities(state.workspace, state.handoff)
    assert report["state"] == "verified" and report["contract"] == prepared.CONTRACT
    assert set(report["row_counts"]) == {
        *prepared.entities.CMS_IDENTITY_TABLES,
        prepared.resources.RESOURCE_IDENTITY_ROOT,
    }
    assert set(report["row_counts"].values()) == {0}
    assert report["handoff_sha256"] == state.handoff.document_sha256
    assert report["preparation_id"] == state.workspace.binding["preparation_id"]
    assert report["admission_sha256"] == state.workspace.binding["admission_sha256"]
    assert report["content_contract"] == prepared.CONTENT_CONTRACT and len(report["content_sha256"]) == 64
    assert report == await prepared.assert_prepared_cms_identities(state.workspace, state.handoff)
    first = next(iter(report["tables"]))
    report["tables"][first]["name"] = "changed"
    assert state.workspace.receipts == before
    assert all(
        call.args[0].lstrip().startswith(("SELECT", "WITH")) for call in state.connection.fetchval.await_args_list
    )
    assert all(call.args[0].startswith("WITH page AS MATERIALIZED") for call in state.connection.fetch.await_args_list)
    state.workspace.copy_rows.assert_not_awaited()
    state.workspace.session.execute.assert_not_awaited()
    assert (source._FACTORY.get(), source._PUBLISHER.get(), source._OWNER.get()) == ambient


@pytest.mark.parametrize("damage", ["workspace", "handoff", "indexes", "frozen"])
async def test_scope_failure_precedes_payload_sql(validation, monkeypatch, damage):
    state = validation
    if damage == "workspace":
        state.workspace.verify_current = AsyncMock(side_effect=ValueError("workspace_refused"))
    elif damage == "handoff":
        state.workspace._binding["handoff_sha256"] = "other"
    elif damage == "indexes":
        state.workspace._indexed = False
    else:
        monkeypatch.setattr(prepared.relationships.context, "_observe", AsyncMock(return_value={}))
    with pytest.raises(
        ValueError, match="workspace_refused|handoff_changed|indexes_required|frozen_coordinates_changed"
    ):
        await prepared.assert_prepared_cms_identities(state.workspace, state.handoff)
    state.connection.fetchval.assert_not_awaited()
    state.workspace.copy_rows.assert_not_awaited()


async def test_full_frozen_coordinates_are_checked_before_map_queries(validation):
    validation.connection.fetchval.side_effect = [True]
    with pytest.raises(ValueError, match="frozen_source_coordinates_changed"):
        await prepared.assert_prepared_cms_identities(validation.workspace, validation.handoff)
    query, dataset, release, families = validation.connection.fetchval.await_args.args
    assert "dataset_id<>$1" in query and "source_id<>'cms-npd'" in query and "release_id<>$2" in query
    assert dataset == validation.workspace.identity["dataset_id"]
    assert release == validation.workspace.identity["metadata"]["source_release"]["vector_sha256"]
    assert set(families) == {family for _, family in prepared.cms_npd_source.RESOURCE_FILES}


async def test_bidirectional_frozen_twins_are_required(validation):
    validation.bad_twins = True
    with pytest.raises(ValueError, match="frozen_witness_mismatch"):
        await prepared.assert_prepared_cms_identities(validation.workspace, validation.handoff)
    validation.workspace.copy_rows.assert_not_awaited()


@pytest.mark.parametrize("family", ["InsurancePlan", "PractitionerRole"])
async def test_uuid5_encoder_is_reused_without_context_binding(validation, family):
    identity_by_field = dict(source_id="cms-npd", resource_type=family, resource_id="one")
    validation.identities = [{**identity_by_field, "entity_id": source_resource_uuid(**identity_by_field)}]
    assert (await prepared.assert_prepared_cms_identities(validation.workspace, validation.handoff))[
        "state"
    ] == "verified"
    validation.identities[0]["entity_id"] = uuid4()
    with pytest.raises(prepared.entities.GenericCandidateError, match="identity_changed"):
        await prepared.assert_prepared_cms_identities(validation.workspace, validation.handoff)


def test_content_encoding_normalizes_native_json_uuid_and_timestamp():
    identity = uuid4()
    timestamp = datetime(2026, 1, 1, tzinfo=timezone.utc)
    columns = ("organization_id", "created_at", "payload_json", "network_refs")
    original_by_field = dict(
        organization_id=identity, created_at=timestamp, payload_json='{"b":2,"a":1}', network_refs='["x"]'
    )
    equivalent_by_field = {
        **original_by_field,
        "created_at": timestamp.astimezone(timezone(timedelta(hours=2))),
        "payload_json": {"a": 1, "b": 2},
        "network_refs": ["x"],
    }
    assert prepared._typed_row(columns, original_by_field) == prepared._typed_row(columns, equivalent_by_field)
    assert prepared._typed_row(columns, original_by_field)[:2] == [str(identity), "2026-01-01T00:00:00.000000+00:00"]
    with pytest.raises(ValueError, match="timestamp_timezone_required"):
        prepared._typed_row(("created_at",), {"created_at": timestamp.replace(tzinfo=None)})


@pytest.mark.parametrize("damage", ["row_bytes", "page_bytes", "row_count"])
async def test_commitment_native_page_bounds_are_enforced(validation, damage):
    root = prepared.resources.RESOURCE_IDENTITY_ROOT
    identity_by_field = dict(
        source_id="cms-npd",
        resource_type="InsurancePlan",
        resource_id="one",
        entity_id=source_resource_uuid("cms-npd", "InsurancePlan", "one"),
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        prepared_row_bytes=200,
    )
    if damage == "row_bytes":
        rows = [{**identity_by_field, "prepared_row_bytes": prepared._ROW_BYTES + 1}]
    elif damage == "page_bytes":
        rows = [{**identity_by_field, "prepared_row_bytes": prepared._PAGE_BYTES}, identity_by_field]
    else:
        rows = [identity_by_field] * (prepared.COPY_MAX_ROWS + 1)
    validation.connection.fetch = AsyncMock(return_value=rows)
    with pytest.raises(
        ValueError, match="commitment_row_unbounded|commitment_page_unbounded|commitment_rows_unbounded"
    ):
        await prepared._content_commitment(validation.workspace, {root: '"private_source"."map"'})


@pytest.mark.parametrize("answer", [True, None])
async def test_resource_timestamp_and_reverse_uuid_collision_refuse_ambiguous_results(answer):
    connection = AsyncMock()
    connection.fetchval.return_value = answer
    with pytest.raises(prepared.entities.CMSIdentityReprepareRequired, match="resource_replay_conflict"):
        await prepared._resource_replay_coordinates(connection, '"private"."resource_map"', "catalog")
    query = connection.fetchval.await_args.args[0]
    assert "candidate.created_at IS DISTINCT FROM incumbent.created_at" in query
    assert "USING(source_id,resource_type,entity_id)" in query
    assert "candidate.resource_id<>incumbent.resource_id" in query
    assert "HAVING count(*)>1" in query


@pytest.mark.parametrize("name", ["verify_identity_source_facts", "verify_canonical_identity_authority"])
async def test_existing_entity_validation_failure_propagates_without_claim(validation, monkeypatch, name):
    monkeypatch.setattr(prepared.entities, name, AsyncMock(side_effect=ValueError("existing_validation_refused")))
    with pytest.raises(ValueError, match="existing_validation_refused"):
        await prepared.assert_prepared_cms_identities(validation.workspace, validation.handoff)
    validation.workspace.copy_rows.assert_not_awaited()


def test_no_finisher_or_source_binding_api_is_exported():
    assert not hasattr(prepared, "finalize_frozen_cms_source_candidate")
    assert not hasattr(prepared, "bind_generic_source_publisher")
