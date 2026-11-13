# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained registered FHIR inputs can move to schema-name storage in place."""

import asyncio
import hashlib
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_anthem as anthem
from process import provider_directory_generic_source_completion as completion
from process import provider_directory_generic_source_storage as storage
from process import provider_directory_source_storage_binding as binding
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_process_provider_directory_fhir_unit import importer
from tests.test_provider_directory_generic_source_completion import _prepared_detached
from tests.test_provider_directory_generic_source_storage import harness
from tests.test_provider_directory_generic_source_transition import (
    DATASET,
    ROOT,
    _retain_original_root,
    _transition,
    case,
)


def _anthem_source_ids():
    """Resolve the normal registered source without introducing a second identity."""
    return [importer._stable_source_id(anthem.seed_rows()[0])]


@pytest.fixture
def named_case(case):
    """Retain seven ordinary checkpoints and their nonempty legacy intake heaps."""
    _retain_original_root(case, "failed")
    candidate = case.header["candidate_storage_json"]
    metadata = candidate["identity"]["metadata"]
    metadata.update(
        source_ids=_anthem_source_ids(),
        selected_resources=list(anthem.RESOURCES),
        expected_resources=list(anthem.RESOURCES),
    )
    case.header.update(status="failed", publication_metadata_json=deepcopy(metadata))
    case.runs[0]["source_ids"] = _anthem_source_ids()
    counts_by_resource = {resource: index + 1 for index, resource in enumerate(anthem.RESOURCES)}
    template = case.checkpoints[0]
    case.checkpoints[:] = [
        {
            **template,
            "resource_type": resource,
            "rows_processed": count,
            "canonical_api_base": anthem.API_BASE,
            "start_url_hash": hashlib.sha256(anthem.start_url(resource, 500).encode()).hexdigest(),
            "source_ids": _anthem_source_ids(),
            "source_scope_hash": "1" * 64,
        }
        for resource, count in counts_by_resource.items()
    ]
    role = next(checkpoint for checkpoint in case.checkpoints if checkpoint["resource_type"] == "PractitionerRole")
    role.update(
        state="active",
        completed_at=None,
        next_url=anthem.start_url("PractitionerRole", 500) + "&_offset=2",
    )
    case.input_rows[:] = [
        {"dataset_id": DATASET, "resource_type": resource, "total": count, "active": count}
        for resource, count in counts_by_resource.items()
    ]
    candidate.update(ordinal=10, resets={})
    case.old, case.new = (
        {key: document[key] for key in ("catalog_schema", "storage_schema", "producer")}
        for document in (case.old, case.new)
    )
    case.counts = counts_by_resource
    return case


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_physical_drift", [False, True])
async def test_nonempty_named_successor_preserves_original_root_and_all_checkpoints(named_case, legacy_physical_drift):
    if legacy_physical_drift:
        for key in ("database_oid", "catalog_schema_oid", "storage_schema_oid", "owner_oid", "namespace_owner_oid"):
            named_case.header["candidate_storage_json"]["storage"][key] += 1000
        for table in named_case.header["candidate_storage_json"]["tables"].values():
            table.update(oid=999, owner=888, layout_sha256="f" * 64)
    before = deepcopy(named_case.header["candidate_storage_json"])
    checkpoints, inputs = deepcopy(named_case.checkpoints), deepcopy(named_case.input_rows)
    result = await _transition(named_case, named_case.counts, stopped_run_id=ROOT)
    actual = named_case.header["candidate_storage_json"]
    assert result == {
        "dataset_id": DATASET,
        "rows": sum(named_case.counts.values()),
        "lineage_run_ids": [ROOT],
        "already_applied": False,
    }
    assert actual == {
        **before,
        "contract": storage.NAMED_ISOLATED_CONTRACT,
        "storage": named_case.new,
        "tables": {kind: {"name": table["name"]} for kind, table in before["tables"].items()},
    }
    assert named_case.checkpoints == checkpoints and named_case.input_rows == inputs
    assert named_case.header["status"] == "failed"
    assert storage._json(named_case.writes[0][1][2]) == before
    assert (await _transition(named_case, named_case.counts, stopped_run_id=ROOT))["already_applied"] is True
    assert len(named_case.writes) == 1
    named_case.native.assert_not_awaited()
    named_case.tables.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift", ["missing", "count", "base", "source", "scope", "cursor", "producer", "table", "input", "witness"]
)
async def test_named_successor_refuses_changed_retained_evidence_before_writing(named_case, drift):
    match drift:
        case "missing":
            named_case.checkpoints.pop()
        case "count":
            named_case.checkpoints[0]["rows_processed"] += 1
        case "base":
            named_case.checkpoints[0]["canonical_api_base"] = "https://other.example/fhir"
        case "source":
            named_case.checkpoints[0]["source_ids"] = ["other-source"]
        case "scope":
            named_case.checkpoints[0]["source_scope_hash"] = "2" * 64
        case "cursor":
            named_case.checkpoints[1]["next_url"] = "https://other.example/fhir/PractitionerRole?cursor=next"
        case "producer":
            named_case.old["producer"] = ["d" * 40, "sha256:" + "e" * 64]
        case "table":
            named_case.header["candidate_storage_json"]["tables"]["witness_input"]["name"] = "pdgs_" + "b" * 32 + "_wi"
        case "input":
            named_case.input_rows[0]["active"] -= 1
        case "witness":
            original = named_case.connection.fetchval.side_effect
            named_case.connection.fetchval.side_effect = lambda query, *args: (
                False if query.startswith("SELECT NOT EXISTS") else original(query, *args)
            )
    before = deepcopy(named_case.header)
    with pytest.raises(GenericCandidateError):
        await _transition(named_case, named_case.counts, stopped_run_id=ROOT)
    assert named_case.header == before and named_case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_ids,resources", [(["unknown-source"], anthem.RESOURCES), (_anthem_source_ids(), ("Organization",))]
)
async def test_named_successor_requires_the_registered_complete_resource_scope(named_case, source_ids, resources):
    metadata = named_case.header["candidate_storage_json"]["identity"]["metadata"]
    metadata.update(source_ids=source_ids, selected_resources=list(resources), expected_resources=list(resources))
    named_case.header["publication_metadata_json"] = deepcopy(metadata)
    named_case.runs[0]["source_ids"] = source_ids
    with pytest.raises(GenericCandidateError):
        await _transition(named_case, named_case.counts, stopped_run_id=ROOT)
    assert named_case.writes == []


@pytest.mark.asyncio
async def test_named_successor_compare_failure_keeps_the_legacy_preimage(named_case):
    before = deepcopy(named_case.header)
    named_case.connection.execute = AsyncMock(return_value="UPDATE 0")
    with pytest.raises(GenericCandidateError, match="transition_header_compare_failed"):
        await _transition(named_case, named_case.counts, stopped_run_id=ROOT)
    assert named_case.header == before


def test_named_source_selector_uses_registered_source_identity_and_resource_contract():
    from process.vsp_directory_contract import RESOURCE_TYPES, SOURCE_ID

    assert storage.named_source_resources(_anthem_source_ids()) == anthem.RESOURCES
    assert storage.named_source_resources(tuple(_anthem_source_ids())) == anthem.RESOURCES
    assert storage.named_source_resources([SOURCE_ID]) == RESOURCE_TYPES
    for source_ids in (None, "unknown", [], ["unknown"], [*_anthem_source_ids(), "unknown"]):
        assert storage.named_source_resources(source_ids) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [TimeoutError, asyncio.CancelledError])
async def test_named_successor_interrupted_compare_preserves_the_legacy_candidate(named_case, failure):
    before = deepcopy(named_case.header)
    named_case.connection.execute = AsyncMock(side_effect=failure)
    with pytest.raises(failure):
        await _transition(named_case, named_case.counts, stopped_run_id=ROOT)
    assert named_case.header == before


@asynccontextmanager
async def _named_prepared(harness, monkeypatch):
    """Bind the existing completion harness to a genuine name-only publisher scope."""
    from process import provider_directory_generic_source_policy as policy

    async with _prepared_detached(harness, monkeypatch) as (original, document):
        session = policy.source_copy_session()
        original_binding = original.factory.storage_binding
        named = binding.NamedFHIRSourceStorage(original_binding.catalog_schema, original_binding.storage_schema)
        publisher = replace(original, storage_binding=named)
        original.factory.storage_binding = named
        publisher_token = policy._PUBLISHER.set(publisher)
        owner_token = policy._OWNER.set((publisher, session))
        metadata = document["identity"]["metadata"]
        metadata.update(
            source_ids=_anthem_source_ids(),
            selected_resources=list(anthem.RESOURCES),
            expected_resources=list(anthem.RESOURCES),
        )
        document.update(
            contract=storage.NAMED_ISOLATED_CONTRACT,
            storage=storage.storage_document(publisher),
            tables={kind: {"name": table["name"]} for kind, table in document["tables"].items()},
        )
        harness.header["publication_metadata_json"] = deepcopy(metadata)
        monkeypatch.setattr(storage, "_capture", AsyncMock(side_effect=AssertionError("physical capture")))
        try:
            yield publisher, document
        finally:
            policy._OWNER.reset(owner_token)
            policy._PUBLISHER.reset(publisher_token)
            original.factory.storage_binding = original_binding


@pytest.mark.asyncio
async def test_named_binding_preserves_ordinary_singleton_resource_scope(harness, monkeypatch):
    async with _named_prepared(harness, monkeypatch) as (_, document):
        document["identity"]["metadata"].update(
            source_ids=["configured-example-source"],
            selected_resources=["Organization", "Location"],
            expected_resources=["Organization", "Location"],
        )
        storage._validate_isolated_binding_storage(document, document["storage"])


@pytest.mark.asyncio
async def test_named_completion_preserves_data_checks_and_replays_the_same_handoff(harness, monkeypatch):
    async with _named_prepared(harness, monkeypatch) as (publisher, document):
        handoff = await completion.prepare_generic_source_handoff(publisher, document)
        assert handoff.document == harness.header["candidate_storage_json"]
        assert handoff.document["storage"] == storage.storage_document(publisher)
        assert handoff.document["phase"] == "ready_for_freeze"
        assert all(set(table) == {"name"} for table in handoff.document["tables"].values())
        assert set(handoff.document["tables"]) == set(storage.RELATIONS + storage.HANDOFF_CONTROL_ROOTS)
        queries = [call.args[0] for call in harness.connection.fetchval.await_args_list]
        assert any("acquired_resource_sha256" in query for query in queries)
        assert any("reference" in query or "source_payload_hash" in query for query in queries)
        assert sum("EXCEPT" in query for query in queries) == 2
        harness.connection.execute.reset_mock()
        replay = await completion.prepare_generic_source_handoff(publisher, handoff.document)
        assert replay.document_sha256 == handoff.document_sha256
        harness.connection.execute.assert_not_awaited()
        storage._capture.assert_not_awaited()
        harness.header["resource_count"] += 1
        with pytest.raises(GenericCandidateError, match="admission_changed"):
            await completion.prepare_generic_source_handoff(publisher, handoff.document)
