# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete-key semantics, exact raw replay and bounded asynchronous output."""

import hashlib
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_generic_source_completion as completion
from process import provider_directory_timestamp_recovery as timestamp_recovery
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_role_merge import MERGED_ROLE_SOURCES_ENV
from tests.test_provider_directory_generic_candidate import _merge, _resource
from tests.test_provider_directory_generic_source_storage import _isolated_harness, harness
from tests.test_provider_directory_generic_source_transition import _timestamp_recovery_checkpoint


@asynccontextmanager
async def _prepared_detached(harness, monkeypatch, *, named=False):
    from process import provider_directory_generic_source_policy as policy

    async with _isolated_harness(harness, monkeypatch, named=named) as publisher:
        binding = harness.header["candidate_storage_json"]
        binding["phase"] = "prepared"
        for root in completion.storage.RELATIONS:
            name = "pdgs_" + "a" * 32 + "_" + completion.storage._SUFFIXES[root]
            binding["tables"][root] = {"schema": publisher.policy.schema, "name": name}
        harness.header.update(
            status="validated",
            is_current=False,
            dataset_hash="a" * 64,
            resource_count=2,
            previous_dataset_id="previous",
            completion_proof_required_version=None,
            completion_proof_sha256=None,
            completion_proof_json=None,
            content_proof_admission_version=1,
            content_proof_admission_kind="generic",
            content_proof_admission_sha256="c" * 64,
        )
        scalar = harness.connection.fetchval.side_effect

        def snapshot_scalar(sql, *args):
            if sql.startswith("SELECT candidate_storage_json"):
                return scalar(sql, *args)
            if sql.startswith("SELECT EXISTS"):
                return False
            assert sql == "SELECT to_regclass($1)::oid::bigint"
            return None

        harness.connection.fetchval = AsyncMock(side_effect=snapshot_scalar)
        harness.connection.fetch = AsyncMock(
            side_effect=lambda sql, *args: (
                [{"attname": "dataset_id"}, {"attname": "candidate_storage_json"}]
                if sql.startswith("SELECT attname")
                else []
            )
        )
        execute = harness.connection.execute.side_effect

        async def snapshot_execute(sql, *args):
            if sql.startswith("INSERT INTO") or sql.startswith("UPDATE") and '_h"' in sql:
                harness.sql.append(sql)
                return "INSERT 0 1" if sql.startswith("INSERT") else "UPDATE 1"
            return await execute(sql, *args)

        harness.connection.execute.side_effect = snapshot_execute
        monkeypatch.setattr(completion.storage, "_capture", AsyncMock(wraps=completion.storage._capture))
        token = policy._OWNER.set((publisher, SimpleNamespace(in_transaction=lambda: True)))
        try:
            yield publisher, binding
        finally:
            policy._OWNER.reset(token)


@pytest.mark.asyncio
async def test_named_handoff_closes_intake_and_replays_without_physical_table_identity(harness, monkeypatch):
    async with _prepared_detached(harness, monkeypatch, named=True) as (publisher, binding):
        handoff = await completion.attach_generic_source_candidate(publisher, binding)
        document = handoff.document
        assert document["phase"] == "ready_for_freeze"
        assert set(document["tables"]) == set(completion.storage.RELATIONS + completion.storage.HANDOFF_CONTROL_ROOTS)
        assert all(set(table) == {"schema", "name"} for table in document["tables"].values())
        assert sum(statement.startswith("DROP TABLE") for statement in harness.sql) == 2
        assert not any("ATTACH" in statement or "GRANT" in statement for statement in harness.sql)
        previous_statements = list(harness.sql)
        replay = await completion.prepare_generic_source_handoff(publisher, document)
        assert replay.document_sha256 == handoff.document_sha256
        assert harness.sql[: len(previous_statements)] == previous_statements
        assert all(statement.startswith("SELECT 1 FROM ") for statement in harness.sql[len(previous_statements) :])
        completion.storage._capture.assert_not_awaited()
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        execute=AsyncMock(),
        fetchval=AsyncMock(return_value=False),
    )
    header = AsyncMock(return_value=harness.header)
    monkeypatch.setattr(completion, "_frozen_handoff_header", header)
    stored = SimpleNamespace(
        dataset_hash=harness.header["dataset_hash"],
        resource_count=2,
        resource_hashes={"Practitioner": "b" * 64},
        resource_counts={"Practitioner": 2},
        metadata={"proof_sha256": "c" * 64},
    )
    content = AsyncMock(return_value=stored)
    monkeypatch.setattr(completion, "_frozen_content_proof", content)
    for check in ("_relationships", "_every_row", "_frozen_projection_exact"):
        monkeypatch.setattr(completion, check, AsyncMock())

    validation = await completion.validate_frozen_source_handoff(
        connection, harness.directory, handoff, protected_owner_role="source_owner"
    )
    assert validation["dataset_hash"] == stored.dataset_hash
    assert validation["resource_counts"] == stored.resource_counts
    assert connection.execute.await_args.args[0].endswith(" IN ACCESS SHARE MODE NOWAIT")
    assert connection.fetchval.await_count == len(document["tables"])
    assert all(call.args[1:] == ("dataset",) for call in connection.fetchval.await_args_list)
    content.reset_mock()
    connection.fetchval.return_value = True
    with pytest.raises(GenericCandidateError, match="dataset_changed"):
        await completion.validate_frozen_source_handoff(
            connection, harness.directory, handoff, protected_owner_role="source_owner"
        )
    content.assert_not_awaited()


@pytest.mark.asyncio
async def test_protected_handoff_shape_reuses_checks_without_issuing_source_authority(harness, monkeypatch):
    from copy import deepcopy

    from process import provider_directory_generic_source_policy as policy

    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        handoff = await completion.prepare_generic_source_handoff(publisher, binding)
    document = handoff.document
    expected_storage = deepcopy(document["storage"])
    assert policy._PUBLISHER.get() is None
    completion.storage.validate_handoff_document(document, expected_storage)
    with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
        completion.storage._validate_binding(document, document["identity"], phases=("ready_for_freeze",))
    for field, changed in (("phase", "published"), ("auxiliary", {}), ("admission", {}), ("tables", {})):
        invalid_by_field = {**document, field: changed}
        with pytest.raises(GenericCandidateError):
            completion.storage.validate_handoff_document(invalid_by_field, expected_storage)
    with pytest.raises(GenericCandidateError, match="storage_binding_changed"):
        completion.storage.validate_handoff_document(document, {**expected_storage, "catalog_schema": "other_schema"})
    assert policy._PUBLISHER.get() is None


def _legacy_frozen_handoff(saved, content_proof_sha256):
    """Preserve a complete historical raw handoff while exposing its logical projection."""
    saved["contract"] = completion.storage.ISOLATED_CONTRACT
    saved["storage"].update(database_oid=900, owner_oid=901)
    saved["admission"]["content_proof_admission_sha256"] = content_proof_sha256
    saved["relationship_sha256"] = "e" * 64
    for table in saved["tables"].values():
        table.pop("schema")
        table.update(oid=902, owner="historical_owner", layout_sha256="f" * 64)
    document = completion.FHIRSourceCandidateHandoff(completion.storage._encoded(saved)).document
    assert document["contract"] == "generic-source-detached-v1"
    return document


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        None,
        "legacy",
        "physical",
        "contract",
        "schema",
        "table",
        "handoff_data",
        "admission_data",
        "header_data",
        "metadata",
        "status",
        "current",
        "scope",
        "completion",
        "rows",
    ],
)
async def test_frozen_header_compares_complete_logical_handoff_and_preserves_legacy_document(
    harness, monkeypatch, change
):
    from copy import deepcopy

    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        handoff = await completion.prepare_generic_source_handoff(publisher, binding)
    document = handoff.document
    saved = deepcopy(document)
    if change is not None:
        document = _legacy_frozen_handoff(saved, harness.header["content_proof_admission_sha256"])
    header = deepcopy(harness.header)
    header["dataset_id"] = document["identity"]["dataset_id"]
    match change:
        case "physical":
            saved["storage"]["owner_oid"] = 990
            for table in saved["tables"].values():
                table["oid"] = 991
        case "contract":
            saved["contract"] = completion.storage.NAMED_ISOLATED_CONTRACT
        case "schema":
            saved["storage"]["storage_schema"] = "other_schema"
        case "table":
            saved["tables"][completion.storage.RESOURCE]["name"] = "other_resource"
        case "handoff_data":
            saved["admission"]["dataset_hash"] = "0" * 64
        case "admission_data":
            saved["admission"]["content_proof_admission_sha256"] = "0" * 64
        case "header_data":
            header["resource_count"] += 1
        case "metadata":
            metadata = dict(completion.storage._json(header["publication_metadata_json"]))
            metadata["source_ids"] = ["other_source"]
            header["publication_metadata_json"] = completion.storage._encoded(metadata)
        case "status":
            header["status"] = "published"
        case "current":
            header["is_current"] = True
        case "scope":
            header["endpoint_id"] = "other_endpoint"
        case "completion":
            header["completion_proof_json"] = {"changed": True}
    header["candidate_storage_json"] = completion.storage._encoded(saved)
    before = deepcopy(header)
    read_headers = AsyncMock(return_value=[] if change == "rows" else [header])
    monkeypatch.setattr(completion, "_FrozenSourceRows", lambda _connection: SimpleNamespace(all=read_headers))
    policy = SimpleNamespace(schema=document["storage"]["storage_schema"])
    if change in {None, "legacy", "physical"}:
        assert await completion._frozen_handoff_header(object(), policy, document) == header
    else:
        reason = "completion_proof_unexpected" if change == "completion" else "header_snapshot_changed"
        with pytest.raises(GenericCandidateError, match=reason):
            await completion._frozen_handoff_header(object(), policy, document)
    read_headers.assert_awaited_once_with(
        "SELECT * FROM "
        + completion.storage_relation(policy.schema, document["tables"][completion.storage.HEADER]["name"])
    )
    assert header == before


@pytest.mark.asyncio
async def test_frozen_header_digest_markers_use_detached_header(monkeypatch):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=False))
    document_by_field = {
        "identity": {"dataset_id": "dataset"},
        "tables": {
            completion.storage.HEADER: {"name": "closed_header"},
            completion.storage.RESOURCE: {"name": "closed_resource"},
        },
    }
    await completion._validate_acquired_digest_markers(
        connection, SimpleNamespace(schema="snapshots"), document_by_field
    )
    query, dataset = connection.fetchval.await_args.args
    assert '"snapshots"."closed_header"' in query
    assert '"snapshots"."closed_resource"' in query
    assert completion.storage.HEADER not in query
    assert dataset == "dataset"


@pytest.mark.asyncio
@pytest.mark.parametrize("completion_version", [None, 3])
async def test_detached_handoff_preserves_complete_checks_without_live_ddl_and_replays_exactly(
    harness, monkeypatch, completion_version
):
    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        if completion_version is not None:
            from tests.provider_directory_fhir_subset_completion_support import build_proof_pair

            proof, digest, _ = build_proof_pair()
            harness.header.update(
                completion_proof_required_version=completion_version,
                completion_proof_sha256=digest,
                completion_proof_json=proof,
                dataset_hash=proof["dataset"]["hash"],
                resource_count=proof["dataset"]["count"],
            )
        handoff = await completion.attach_generic_source_candidate(publisher, binding)
        assert handoff.document == harness.header["candidate_storage_json"]
        assert handoff.document["phase"] == "ready_for_freeze"
        assert all(
            set(table) == {"schema", "name"} and table["schema"] == publisher.storage_binding.storage_schema
            for table in handoff.document["tables"].values()
        )
        assert set(handoff.document["tables"]) == set(
            completion.storage.RELATIONS + completion.storage.HANDOFF_CONTROL_ROOTS
        )
        assert handoff.document["admission"] == {
            key: harness.header[key] for key in completion.storage.HANDOFF_HEADER_FIELDS
        }
        assert sum("ADD CHECK" in statement for statement in harness.sql) == len(completion.storage.RELATIONS) + 2
        assert sum(statement.startswith("DROP TABLE") for statement in harness.sql) == 2
        assert not any(statement.startswith("GRANT SELECT") for statement in harness.sql)
        aggregate_queries = [call.args[0] for call in harness.connection.fetchval.await_args_list]
        assert any("acquired_resource_sha256" in query for query in aggregate_queries)
        assert any("reference" in query or "source_payload_hash" in query for query in aggregate_queries)
        assert sum("EXCEPT" in query for query in aggregate_queries) == 2
        assert sum(statement.startswith("INSERT INTO") for statement in harness.sql) == 2
        assert not any(
            '"catalog_schema".' in statement
            or "ATTACH" in statement
            or "RENAME" in statement
            or "LOCK TABLE" in statement
            for statement in harness.sql
        )
        assert all('"source_schema"."pdgs_' in statement or statement.startswith("UPDATE") for statement in harness.sql)
        harness.connection.execute.reset_mock()
        replay = await completion.prepare_generic_source_handoff(publisher, handoff.document)
        assert replay.document_sha256 == handoff.document_sha256
        assert harness.connection.execute.await_count == len(handoff.document["tables"])
        assert all(call.args[0].startswith("SELECT 1 FROM ") for call in harness.connection.execute.await_args_list)
        with pytest.raises(FrozenInstanceError):
            replay.document_json = "{}"
        changed = replay.document
        changed["tables"].clear()
        assert replay.document["tables"] == handoff.document["tables"]


def _tamper_completion_header(header, changed):
    if changed == "shape":
        proof = header["completion_proof_json"]
        proof["dataset"]["count"] += 1
        header["completion_proof_sha256"] = hashlib.sha256(completion.storage._encoded(proof).encode()).hexdigest()
        return
    updates_by_case = {
        "digest": {"completion_proof_sha256": "f" * 64},
        "dataset_hash": {"dataset_hash": "f" * 64},
        "count": {"resource_count": header["resource_count"] + 1},
        "unexpected": {"completion_proof_required_version": None, "completion_proof_sha256": None},
        "missing": {"completion_proof_json": None},
    }
    header.update(updates_by_case[changed])


@pytest.mark.asyncio
@pytest.mark.parametrize("replay", [False, True])
@pytest.mark.parametrize("changed", ["digest", "shape", "dataset_hash", "count", "unexpected", "missing"])
async def test_detached_completion_checks_reject_tampering_before_mutation(harness, monkeypatch, changed, replay):
    from tests.provider_directory_fhir_subset_completion_support import build_proof_pair

    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        proof, digest, _ = build_proof_pair()
        harness.header.update(
            completion_proof_required_version=3,
            completion_proof_sha256=digest,
            completion_proof_json=proof,
            dataset_hash=proof["dataset"]["hash"],
            resource_count=proof["dataset"]["count"],
        )
        if replay:
            binding = (await completion.prepare_generic_source_handoff(publisher, binding)).document
        _tamper_completion_header(harness.header, changed)
        harness.connection.execute.reset_mock()
        with pytest.raises(GenericCandidateError, match="completion_"):
            await completion.prepare_generic_source_handoff(publisher, binding)
        harness.connection.execute.assert_not_awaited()


def _add_recovery_completion(header, binding):
    """Store a real compact receipt while canonical rows remain distinct from observations."""
    identity = binding["identity"]
    header["dataset_id"] = identity["dataset_id"]
    checkpoint = _timestamp_recovery_checkpoint(
        {
            "canonical_api_base": "https://example.test/fhir",
            "dataset_id": identity["dataset_id"],
            "acquisition_root_run_id": identity["acquisition_root_run_id"],
            "source_ids": identity["metadata"]["source_ids"],
            "resource_type": "Organization",
            "start_url_hash": hashlib.sha256(b"https://example.test/fhir/Organization?_count=20").hexdigest(),
        }
    )
    diagnostic_by_field = {
        timestamp_recovery.KEY: timestamp_recovery.completion_receipt(checkpoint["completeness_json"]),
        "fetch_mode": timestamp_recovery.FETCH_MODE,
        "complete": True,
        "error": None,
        "bounded": False,
        "next_url_remaining": False,
        "pages_fetched": checkpoint["pages_processed"],
        "rows_fetched": checkpoint["rows_processed"],
    }
    header["publication_metadata_json"] = {
        **deepcopy(header["publication_metadata_json"]),
        "resource_diagnostics": {"Organization": diagnostic_by_field},
    }
    return diagnostic_by_field


async def _frozen_recovery_header(harness, monkeypatch, handoff):
    header = deepcopy(harness.header)
    read_headers = AsyncMock(return_value=[header])
    monkeypatch.setattr(completion, "_FrozenSourceRows", lambda _connection: SimpleNamespace(all=read_headers))
    policy = SimpleNamespace(schema=handoff.document["storage"]["storage_schema"])
    return await completion._frozen_handoff_header(object(), policy, handoff.document)


def _tamper_recovery_completion(diagnostic, drift):
    receipt = diagnostic[timestamp_recovery.KEY]
    if drift == "version":
        diagnostic["descending_timestamp_recovery_v2"] = diagnostic.pop(timestamp_recovery.KEY)
        return
    if drift == "incomplete":
        receipt["complete"] = False
        return
    if drift == "missing":
        diagnostic.pop(timestamp_recovery.KEY)
        return
    if drift == "unknown_mode_without_receipt":
        diagnostic.pop(timestamp_recovery.KEY)
        diagnostic["fetch_mode"] = "descending_timestamp_recovery_v2"
        return
    if drift == "unselected_resource":
        receipt["identity"]["resource_type"] = "Location"
        return
    if drift in receipt["identity"]:
        receipt["identity"][drift] = "changed"
        return
    if drift == "count":
        diagnostic["rows_fetched"] += 1
        return
    receipt["terminal_request_url"] = "https://example.test/fhir/Organization?_page_token=raw"


@pytest.mark.asyncio
async def test_recovery_completion_survives_handoff_replay_and_frozen_publication(harness, monkeypatch):
    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        diagnostic = _add_recovery_completion(harness.header, binding)
        assert diagnostic["rows_fetched"] > harness.header["resource_count"]
        assert "_page_token" not in json.dumps(diagnostic)
        handoff = await completion.prepare_generic_source_handoff(publisher, binding)
        header_query = next(
            call.args[0]
            for call in harness.connection.fetchrow.await_args_list
            if "publication_metadata_json " in call.args[0] and "FOR UPDATE" in call.args[0]
        )
        assert "dataset_id" in header_query.removeprefix("SELECT ").partition(" FROM ")[0].split(",")
        replay = await completion.prepare_generic_source_handoff(publisher, handoff.document)
        assert replay.document_sha256 == handoff.document_sha256
    assert await _frozen_recovery_header(harness, monkeypatch, handoff) == harness.header


@pytest.mark.asyncio
@pytest.mark.parametrize("frozen", [False, True])
@pytest.mark.parametrize(
    "drift",
    [
        "version",
        "incomplete",
        "missing",
        "unknown_mode_without_receipt",
        "unselected_resource",
        "dataset_id",
        "acquisition_root_run_id",
        "source_id",
        "resource_type",
        "count",
        "raw_url",
    ],
)
async def test_recovery_completion_refuses_bad_receipts_before_handoff_or_publication(
    harness, monkeypatch, drift, frozen
):
    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        diagnostic = _add_recovery_completion(harness.header, binding)
        handoff = await completion.prepare_generic_source_handoff(publisher, binding) if frozen else None
        _tamper_recovery_completion(diagnostic, drift)
        if drift == "unselected_resource":
            harness.header["publication_metadata_json"]["resource_diagnostics"] = {"Location": diagnostic}
        harness.connection.execute.reset_mock()
        with pytest.raises(GenericCandidateError, match="completion_recovery_invalid"):
            if frozen:
                await _frozen_recovery_header(harness, monkeypatch, handoff)
            else:
                await completion.prepare_generic_source_handoff(publisher, binding)
        harness.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_validation_summary_retains_handoff_without_claiming_publication(harness, monkeypatch):
    from importlib import import_module

    from process import provider_directory_generic_source_adapters as adapters
    from process import provider_directory_generic_source_policy as policy

    directory = import_module("process.provider_directory_fhir")
    candidate = SimpleNamespace(dataset_id="dataset", verification_role=None)
    proof = SimpleNamespace(dataset_hash="a" * 64, resource_count=2)
    state = ("previous", proof, {}, directory.ENDPOINT_DATASET_VALIDATED, [])
    monkeypatch.setattr(directory, "_persist_endpoint_dataset_validation", AsyncMock(return_value=state))

    @asynccontextmanager
    async def transaction(*args, **kwargs):
        yield harness.connection

    @asynccontextmanager
    async def completed(*args):
        yield publisher, binding

    monkeypatch.setattr(policy, "source_transaction", transaction)
    monkeypatch.setattr(directory, "db", SimpleNamespace(acquire=transaction))
    monkeypatch.setattr(adapters, "completed_source_scope", completed)
    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        summary = await directory._validate_endpoint_dataset_candidate(candidate, {})
        document = harness.header["candidate_storage_json"]
        assert summary["source_candidate_handoff"] == {
            "document": document,
            "sha256": hashlib.sha256(completion.storage._encoded(document).encode()).hexdigest(),
        }
        assert summary["published"] is False and document["phase"] == "ready_for_freeze"
        monkeypatch.setattr(completion, "attach_generic_source_candidate", AsyncMock(return_value=None))
        legacy = await directory._validate_endpoint_dataset_candidate(candidate, {})
        assert legacy == {key: field_value for key, field_value in summary.items() if key != "source_candidate_handoff"}
        monkeypatch.setattr(completion, "attach_generic_source_candidate", AsyncMock(return_value=document))
        with pytest.raises(RuntimeError, match="source_handoff_invalid"):
            await directory._validate_endpoint_dataset_candidate(candidate, {})


@pytest.mark.asyncio
@pytest.mark.parametrize("reject_commit", [False, True])
async def test_run_handoff_requires_committed_native_recheck(harness, monkeypatch, reject_commit):
    from process import provider_directory_generic_source_policy as policy

    @asynccontextmanager
    async def transaction(*args, **kwargs):
        yield
        if reject_commit:
            raise RuntimeError("commit rejected")

    monkeypatch.setattr(policy, "source_transaction", transaction)
    harness.directory.db = object()
    handoffs_by_dataset = {}
    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        monkeypatch.setattr(completion, "source_owner_transaction", completion.storage.source_owner_transaction)
        prepared = await completion.prepare_generic_source_handoff(publisher, binding)
        harness.connection.execute.reset_mock()
        if reject_commit:
            with pytest.raises(RuntimeError, match="commit rejected"):
                await completion.record_candidate_handoff(harness.directory, harness.candidate, handoffs_by_dataset)
            assert handoffs_by_dataset == {}
            return
        await completion.record_candidate_handoff(harness.directory, harness.candidate, handoffs_by_dataset)
        assert handoffs_by_dataset == {
            harness.candidate.dataset_id: {
                "document": prepared.document,
                "sha256": prepared.document_sha256,
                "producer": ["a" * 40, "sha256:" + "b" * 64],
            }
        }
        await completion.record_candidate_handoff(harness.directory, harness.candidate, handoffs_by_dataset)
        handoffs_by_dataset[harness.candidate.dataset_id]["sha256"] = "f" * 64
        with pytest.raises(GenericCandidateError, match="handoff_changed"):
            await completion.record_candidate_handoff(harness.directory, harness.candidate, handoffs_by_dataset)
        assert all(call.args[0].startswith("SELECT 1 FROM ") for call in harness.connection.execute.await_args_list)


@pytest.mark.asyncio
async def test_run_handoff_refuses_unfinished_candidate(harness, monkeypatch):
    from process import provider_directory_generic_source_policy as policy

    @asynccontextmanager
    async def transaction(*args, **kwargs):
        yield

    monkeypatch.setattr(policy, "source_transaction", transaction)
    harness.directory.db = object()
    handoffs_by_dataset = {}
    async with _prepared_detached(harness, monkeypatch):
        monkeypatch.setattr(completion, "source_owner_transaction", completion.storage.source_owner_transaction)
        with pytest.raises(GenericCandidateError, match="binding_changed"):
            await completion.record_candidate_handoff(harness.directory, harness.candidate, handoffs_by_dataset)
        assert handoffs_by_dataset == {}
        harness.connection.execute.assert_not_awaited()
    await completion.record_candidate_handoff(harness.directory, harness.candidate, handoffs_by_dataset)
    assert handoffs_by_dataset == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", ["dataset_hash", "completion_proof_sha256", "previous_dataset_id", "resource_count"]
)
async def test_detached_handoff_replay_refuses_changed_content_or_predecessor(harness, monkeypatch, changed):
    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        result = await completion.prepare_generic_source_handoff(publisher, binding)
        harness.header[changed] = 3 if changed == "resource_count" else "f" * 64
        harness.connection.execute.reset_mock()
        with pytest.raises(GenericCandidateError, match="admission_changed"):
            await completion.prepare_generic_source_handoff(publisher, result.document)
        assert all(call.args[0].startswith("SELECT 1 FROM ") for call in harness.connection.execute.await_args_list)


@pytest.mark.asyncio
async def test_detached_handoff_replay_refuses_unavailable_named_relation_before_mutation(harness, monkeypatch):
    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        result = await completion.prepare_generic_source_handoff(publisher, binding)
        failure = RuntimeError("named relation unavailable")
        harness.connection.execute.reset_mock()
        harness.connection.execute.side_effect = failure
        with pytest.raises(RuntimeError) as caught:
            await completion.prepare_generic_source_handoff(publisher, result.document)
        assert caught.value is failure
        harness.connection.execute.assert_awaited_once()
        assert harness.connection.execute.await_args.args[0].startswith('SELECT 1 FROM "source_schema".')


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["endpoint_id", "acquisition_root_run_id", "publication_metadata_json"])
async def test_detached_handoff_rechecks_header_acquisition_before_indexes(harness, monkeypatch, field):
    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        harness.header[field] = {} if field == "publication_metadata_json" else "changed"
        with pytest.raises(GenericCandidateError, match="acquisition_changed"):
            await completion.prepare_generic_source_handoff(publisher, binding)
        harness.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_detached_relationship_failure_preserves_prepared_binding_and_cannot_attach(harness, monkeypatch):
    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        before = completion.storage._encoded(binding)
        monkeypatch.setattr(completion, "_relationships", AsyncMock(side_effect=GenericCandidateError("invalid edge")))
        with pytest.raises(GenericCandidateError, match="invalid edge"):
            await completion.prepare_generic_source_handoff(publisher, binding)
        assert completion.storage._encoded(harness.header["candidate_storage_json"]) == before
        assert not any(
            "ADD CHECK" in statement or "DROP TABLE" in statement or statement.startswith("UPDATE")
            for statement in harness.sql
        )


@pytest.mark.asyncio
async def test_isolated_contract_cannot_call_the_legacy_attachment_helper():
    connection = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(GenericCandidateError, match="isolated_attach_forbidden"):
        await completion._attach_final_tables(connection, object(), {"contract": completion.storage.ISOLATED_CONTRACT})
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("version,digest", [(None, "a" * 64), (3, None), (1, "a" * 64), (True, "a" * 64)])
def test_detached_handoff_keeps_the_existing_closed_completion_contract(version, digest):
    admission_by_field = dict(
        dataset_hash="a" * 64,
        resource_count=1,
        previous_dataset_id=None,
        completion_proof_required_version=version,
        completion_proof_sha256=digest,
        content_proof_admission_sha256="b" * 64,
    )
    with pytest.raises(GenericCandidateError, match="admission_changed"):
        completion.storage.validate_handoff_header(admission_by_field)


async def _stream(rows):
    for row in rows:
        yield row


@pytest.mark.parametrize(
    ("stale_hash", "acquired", "error"),
    [
        (False, None, None),
        (True, None, "payload_hash_changed"),
        (False, "f" * 64, "semantic_resource_subset_invalid"),
        (True, "f" * 64, "payload_hash_changed"),
    ],
)
def test_semantic_observation_hashes_once_and_keeps_refusal_order(monkeypatch, stale_hash, acquired, error):
    from importlib import import_module

    directory = import_module("process.provider_directory_fhir")
    row_by_field = {
        **_resource("one"),
        "resource_type": "Location",
        "payload_json": {"resource_id": "one", "active": True},
    }
    contract = directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT
    row_by_field["payload_hash"] = directory.resource_payload_sha256_for_contract(
        row_by_field["payload_json"], contract, resource_type=row_by_field["resource_type"]
    )
    if stale_hash:
        row_by_field["payload_hash"] = "0" * 64
    row_by_field["acquired_resource_sha256"] = acquired
    hash_payload = Mock(wraps=directory.resource_payload_sha256_for_contract)
    monkeypatch.setattr(directory, "resource_payload_sha256_for_contract", hash_payload)
    prepare, _merge_rows = completion._resource_callbacks(directory, SimpleNamespace(resource_hash_contract=contract))
    if error:
        error_type = GenericCandidateError if stale_hash else RuntimeError
        with pytest.raises(error_type, match=error):
            prepare(row_by_field)
    else:
        assert prepare(row_by_field) == row_by_field
    hash_payload.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["resource_input", "witness_input"])
async def test_independent_validation_reads_the_same_active_scope_without_sorting(key):
    cursor = Mock(side_effect=lambda *args, **kwargs: _stream([{"resource_id": "one"}]))
    connection, policy = SimpleNamespace(cursor=cursor), SimpleNamespace(schema="source")
    binding_by_field = {"tables": {key: {"name": key}}, "resets": {"*": 2, "Location": 7}}
    args = (connection, policy, binding_by_field, key, ("resource_id",))
    assert [row async for row in completion._active_rows(*args)] == [
        row async for row in completion._active_rows(*args, ordered=False)
    ]
    ordered, unordered = cursor.call_args_list
    assert ordered.args[0].split("ORDER BY")[0] == unordered.args[0]
    assert "ORDER BY" in ordered.args[0] and "ORDER BY" not in unordered.args[0]
    assert ordered.args[1:] == unordered.args[1:] == (2, completion.storage._encoded(binding_by_field["resets"]))
    assert ordered.kwargs == unordered.kwargs == {"prefetch": 4096}


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [completion._RECHECK_ROWS, completion._RECHECK_ROWS + 1, 10**10])
async def test_reduced_heap_rechecks_validate_every_returned_row_with_bounded_sampling(count):
    rows = [{"resource_id": "first"}, {"resource_id": "second"}]
    connection = SimpleNamespace(cursor=Mock(return_value=_stream(rows)))
    validate = Mock()
    await completion._recheck_reduced_rows(
        connection, SimpleNamespace(schema="source_schema"), "candidate_resource", ("resource_id",), validate, count
    )
    assert [call.args[0] for call in validate.call_args_list] == rows
    query = connection.cursor.call_args.args[0]
    assert ("TABLESAMPLE SYSTEM" in query) is (count > completion._RECHECK_ROWS)
    if count > completion._RECHECK_ROWS:
        assert f"ORDER BY random() LIMIT {completion._RECHECK_ROWS}" in query


@pytest.mark.asyncio
async def test_reduced_heap_sample_rejects_invalid_rows():
    rows = [{"resource_id": "first"}, {"resource_id": "invalid"}, {"resource_id": "last"}]
    connection = SimpleNamespace(cursor=Mock(return_value=_stream(rows)))
    failure = ValueError("invalid resource")
    validate = Mock(side_effect=[None, failure])
    with pytest.raises(ValueError) as caught:
        await completion._recheck_reduced_rows(
            connection,
            SimpleNamespace(schema="source_schema"),
            "candidate_resource",
            ("resource_id",),
            validate,
            completion._RECHECK_ROWS + 1,
        )
    assert caught.value is failure
    assert [call.args[0] for call in validate.call_args_list] == rows[:2]


@pytest.mark.asyncio
@pytest.mark.parametrize("chunk_rows", [256, 4])
async def test_complete_semantic_key_can_cross_many_cursor_and_copy_pages(monkeypatch, chunk_rows):
    monkeypatch.setattr(completion, "_KEY_CHUNK_ROWS", chunk_rows)
    rows = [_resource("first", values=(str(index),)) for index in range(31)] + [_resource("second")]
    complete = completion._reduced_rows(_stream(rows), dataset_id="dataset", prepare=dict, merge=_merge)
    result_rows = [row async for row in complete]
    assert result_rows[0]["payload_json"]["values"] == sorted(str(index) for index in range(31))
    assert result_rows[1] == _resource("second")
    copied_records = []

    async def copy(connection, policy, name, columns, records):
        copied_records.extend(records)
        return len(records)

    monkeypatch.setattr(completion.storage, "_copy", AsyncMock(side_effect=copy))
    monkeypatch.setattr(completion, "COPY_MAX_ROWS", 1)
    assert (
        await completion._copy_rows(object(), object(), "heap", ("resource_id", "payload_json"), _stream(result_rows))
        == 2
    )
    assert [row[0] for row in copied_records] == ["first", "second"]


@pytest.mark.asyncio
async def test_each_observation_and_each_merged_result_is_validated_once():
    prepared_values = []

    def prepare(resource_row):
        prepared_values.append(resource_row["payload_json"]["values"])
        return dict(resource_row)

    rows = [_resource("first", values=(str(index),)) for index in range(3)] + [_resource("second")]
    complete = completion._reduced_rows(_stream(rows), dataset_id="dataset", prepare=prepare, merge=_merge)
    assert len([row async for row in complete]) == 2
    assert prepared_values == [["0"], ["1"], ["2"], ["0", "1", "2"], _resource("second")["payload_json"]["values"]]


@pytest.mark.asyncio
async def test_real_organization_v4_union_matches_the_existing_canonical_contract():
    from importlib import import_module

    directory = import_module("process.provider_directory_fhir")
    from process.provider_directory_organization_hash import canonical_organization_payload

    candidate = SimpleNamespace(
        resource_hash_contract=directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of="2026-08-10",
    )
    resource_rows = []
    for name in ("Community Clinic", "Regional Clinic", "Community Clinic"):
        canonical_payload = canonical_organization_payload(
            {"resource_id": "organization", "active": True, "name": name, "fhir_meta": None}
        )
        resource_rows.append(
            dict(
                dataset_id="dataset",
                resource_type="Organization",
                resource_id="organization",
                payload_json=canonical_payload,
                payload_hash=directory.resource_payload_sha256_for_contract(
                    canonical_payload, candidate.resource_hash_contract, resource_type="Organization"
                ),
                acquired_resource_sha256=None,
            )
        )
    prepare, merge = completion._resource_callbacks(directory, candidate)
    actual_rows = [
        resource_row
        async for resource_row in completion._reduced_rows(
            _stream(resource_rows), dataset_id="dataset", prepare=prepare, merge=merge
        )
    ]
    expected = directory._merged_semantic_dataset_payload(
        "Organization",
        resource_rows[0]["payload_json"],
        resource_rows[1]["payload_json"],
        candidate.resource_hash_contract,
    )
    assert len(actual_rows) == 1 and actual_rows[0]["payload_json"] == expected
    assert actual_rows[0]["payload_hash"] == directory.resource_payload_sha256_for_contract(
        expected, candidate.resource_hash_contract, resource_type="Organization"
    )


def _replayed_optum_observation(directory, resource_type, code, *, source_base, outcome):
    from process.provider_directory_organization_hash import canonical_organization_payload
    from process.provider_directory_resource_hash import canonical_practitioner_payload

    resource_id = "example"
    tags = [
        {"system": "http://optum.com/fhir/wm/system/2021/07/resourceid", "code": code},
        {"system": "http://example.test/classification", "code": "one"},
    ]
    payload_by_field = {
        "resource_id": resource_id,
        "resource_url": f"{source_base}/{resource_type}/{resource_id}",
        "fhir_fetch_url": f"{source_base}/{resource_type}" if code == "first" else source_base,
        "active": True,
        "fhir_meta": {"tag": tags, "lastUpdated": "2026-08-11" if code == "first" else "2026-08-10"},
    }
    if code == "second" and outcome == "changed-content":
        payload_by_field["active"] = False
    if code == "second" and outcome == "changed-classification":
        tags[1]["code"] = "two"
    if code == "second" and outcome == "ambiguous-tag":
        tags.append(dict(tags[0]))
    if code == "second" and outcome == "forged-full-url":
        payload_by_field["fhir_fetch_url"] = "https://example.test/fhir"
    if resource_type == "Practitioner":
        payload_by_field = canonical_practitioner_payload({**payload_by_field, "names": [{"family": "Example"}]})
    if resource_type == "Organization":
        payload_by_field = canonical_organization_payload(payload_by_field)
    return {
        **_resource(resource_id),
        "resource_type": resource_type,
        "payload_json": payload_by_field,
        "payload_hash": directory.resource_payload_sha256_for_contract(
            payload_by_field, directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT, resource_type=resource_type
        ),
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resource_type,outcome",
    [
        *[
            (family, "replay")
            for family in ("Location", "Organization", "OrganizationAffiliation", "Practitioner", "PractitionerRole")
        ],
        *[
            ("Location", conflict)
            for conflict in (
                "other-endpoint",
                "changed-content",
                "changed-classification",
                "ambiguous-tag",
                "forged-full-url",
            )
        ],
    ],
)
async def test_replayed_optum_resource_id_tag_preserves_semantic_content(resource_type, outcome):
    from importlib import import_module

    directory = import_module("process.provider_directory_fhir")
    candidate = SimpleNamespace(
        resource_hash_contract=directory.SEMANTIC_CONTENT_V4_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of="2026-08-10",
    )
    source_base = "https://example.test/fhir" if outcome == "other-endpoint" else directory.UHC_PROVIDER_DIRECTORY_BASE
    resource_rows = [
        _replayed_optum_observation(directory, resource_type, code, source_base=source_base, outcome=outcome)
        for code in ("first", "second")
    ]
    first, second = resource_rows
    assert first["payload_hash"] != second["payload_hash"]
    prepare, merge = completion._resource_callbacks(directory, candidate)
    reduced_rows = completion._reduced_rows(_stream(resource_rows), dataset_id="dataset", prepare=prepare, merge=merge)
    if outcome != "replay":
        with pytest.raises(ValueError, match="provider_directory_.*payload_conflict"):
            await anext(reduced_rows)
        return
    assert [resource_row async for resource_row in reduced_rows] == [second]
    assert first["payload_json"]["fhir_meta"]["tag"][0]["code"] == "first"


@pytest.mark.parametrize("outcome", ["valid", "native-conflict", "repeat-only"])
async def test_merged_role_completion_checks_input_and_reduced_output_before_indexing(monkeypatch, outcome):
    connection = SimpleNamespace(
        fetchval=AsyncMock(side_effect=[outcome == "native-conflict", outcome == "repeat-only"]),
    )
    binding_by_field = {
        "identity": {"metadata": {"source_ids": ["source-a"]}},
        "tables": {"resource_input": {"name": "input_rows"}, "witness_input": {"name": "input_witness"}},
        "resets": {},
    }
    tables_by_kind = {
        completion.storage.RESOURCE: {"name": "resource_output"},
        completion.storage.WITNESS: {"name": "witness_output"},
    }
    monkeypatch.setenv(MERGED_ROLE_SOURCES_ENV, "source-a")
    monkeypatch.setattr(completion.storage, "refresh_statistics", AsyncMock())
    monkeypatch.setattr(completion, "_observed_divergence", AsyncMock(return_value=False))
    reduce = AsyncMock()
    indexes = AsyncMock()
    monkeypatch.setattr(completion, "_reduce_payloads_python", reduce)
    monkeypatch.setattr(completion, "_indexes", indexes)
    args = (
        connection,
        SimpleNamespace(schema="source"),
        binding_by_field,
        SimpleNamespace(source_release=None),
        None,
        tables_by_kind,
    )
    if outcome == "valid":
        await completion._complete_payloads(*args)
        assert indexes.await_count == 2
    else:
        error = (
            "provider_directory_resource_payload_conflict"
            if outcome == "native-conflict"
            else "provider_directory_merged_role_observation_missing"
        )
        with pytest.raises(ValueError, match=error):
            await completion._complete_payloads(*args)
        indexes.assert_not_awaited()
    assert reduce.await_count == (outcome != "native-conflict")
    assert '"source"."input_rows"' in connection.fetchval.await_args_list[0].args[0]
    if outcome != "native-conflict":
        assert '"source"."resource_output"' in connection.fetchval.await_args_list[1].args[0]


def _witness(resource_id="example"):
    raw_by_field = {"resourceType": "Organization", "id": resource_id}
    return dict(
        dataset_id="dataset",
        source_id="cms-npd",
        release_id="a" * 64,
        resource_type="Organization",
        resource_id=resource_id,
        raw_payload_sha256=hashlib.sha256(completion.storage._encoded(raw_by_field).encode()).hexdigest(),
        normalized_payload_hash="b" * 64,
        raw_payload_json=raw_by_field,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", [False, True, None])
async def test_closed_candidate_acquired_digest_markers_use_one_set_check(mismatch):
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=mismatch))
    binding_by_field = {
        "identity": {"dataset_id": "dataset"},
        "tables": {completion.storage.RESOURCE: {"name": "private_resource"}},
    }
    if mismatch is False:
        await completion._validate_acquired_digest_markers(
            connection, SimpleNamespace(schema="source_schema"), binding_by_field
        )
    else:
        with pytest.raises(GenericCandidateError, match="acquired_digest_marker_changed"):
            await completion._validate_acquired_digest_markers(
                connection, SimpleNamespace(schema="source_schema"), binding_by_field
            )
    connection.fetchval.assert_awaited_once()
    statement, dataset_id = connection.fetchval.await_args.args
    assert dataset_id == "dataset"
    assert '"source_schema"."private_resource"' in statement
    assert "completion_proof_required_version=3" in statement
    assert "IS DISTINCT FROM (r.acquired_resource_sha256 IS NOT NULL)" in statement


async def _reduced_witness_rows(rows):
    return [value async for value in completion._reduced_witnesses(_stream(rows), "dataset", "a" * 64)]


@pytest.mark.asyncio
async def test_witnesses_of_several_resources_reduce_in_order_and_refuse_disorder():
    first, second = _witness("first"), _witness("second")
    assert await _reduced_witness_rows([first, first, second]) == [first, second]
    with pytest.raises(GenericCandidateError, match="witness_order_changed"):
        await _reduced_witness_rows([second, first])
    assert await _reduced_witness_rows([]) == []
    # A reduction without rows copies nothing and never touches the connection.
    no_rows = completion._reduced_rows(_stream([]), dataset_id="dataset", prepare=dict, merge=_merge)
    assert await completion._copy_rows(object(), object(), "heap", completion.RESOURCE_COLUMNS, no_rows) == 0


@pytest.mark.asyncio
async def test_raw_witness_replay_requires_actual_hash_and_exact_complete_observation():
    row = _witness()
    assert [value async for value in completion._reduced_witnesses(_stream([row, row]), "dataset", "a" * 64)] == [row]
    with pytest.raises(GenericCandidateError, match="raw_hash_changed"):
        [
            value
            async for value in completion._reduced_witnesses(
                _stream([row | {"raw_payload_sha256": "c" * 64}]), "dataset", "a" * 64
            )
        ]
    with pytest.raises(GenericCandidateError, match="witness_replay_conflict"):
        [
            value
            async for value in completion._reduced_witnesses(
                _stream([row, row | {"normalized_payload_hash": "c" * 64}]), "dataset", "a" * 64
            )
        ]


@pytest.mark.asyncio
async def test_attach_refuses_unvalidated_header_before_indexes_or_attach(monkeypatch):
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"status": "acquiring", "is_current": False}),
        execute=AsyncMock(),
    )
    publisher = SimpleNamespace(
        connection=connection,
        policy=SimpleNamespace(schema="source_schema", owner_role="owner", publisher_role="publisher"),
    )
    with pytest.raises(GenericCandidateError, match="validated_admission_required"):
        await completion.attach_generic_source_candidate(
            publisher,
            {"contract": completion.storage.CONTRACT, "phase": "prepared", "identity": {"dataset_id": "dataset"}},
        )
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_index_or_antijoin_cannot_reach_attachment(monkeypatch):
    calls = []

    async def execute(query, *args):
        calls.append(query)
        if "ADD PRIMARY KEY" in query:
            raise RuntimeError("duplicate key")

    connection = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    with pytest.raises(RuntimeError, match="duplicate key"):
        await completion._indexes(
            connection, SimpleNamespace(schema="source_schema"), completion.storage.RESOURCE, "heap"
        )
    assert len(calls) == 1 and "ATTACH" not in calls[0]


@pytest.mark.asyncio
async def test_global_publication_conflict_occurs_after_checks_and_before_any_attach(monkeypatch):
    from process import provider_directory_generic_source_auxiliary as auxiliary

    binding_by_field = {
        "contract": completion.storage.CONTRACT,
        "phase": "prepared",
        "identity": {"dataset_id": "dataset", "metadata": {}},
        "relationship_sha256": None,
        "tables": {
            root: {"schema": "source_schema", "name": "heap_" + str(index)}
            for index, root in enumerate(completion.storage.RELATIONS)
        },
    }
    header_by_field = {
        "status": "validated",
        "is_current": False,
        "content_proof_admission_version": 1,
        "content_proof_admission_kind": "generic",
        "dataset_hash": "a" * 64,
        "content_proof_admission_sha256": "b" * 64,
        "candidate_storage_json": binding_by_field,
    }
    publisher = SimpleNamespace(
        connection=SimpleNamespace(
            fetchrow=AsyncMock(return_value=header_by_field),
            execute=AsyncMock(),
        ),
        policy=SimpleNamespace(schema="source_schema", owner_role="owner", publisher_role="publisher"),
    )
    events = []
    monkeypatch.setattr(
        completion.storage, "_verify_tables", AsyncMock(side_effect=lambda *args: events.append("native"))
    )
    monkeypatch.setattr(completion, "_indexes", AsyncMock(side_effect=lambda *args: events.append("index")))
    monkeypatch.setattr(
        completion, "_relationships", AsyncMock(side_effect=lambda *args: events.append("relationships"))
    )
    publish = AsyncMock(side_effect=RuntimeError("whole candidate reprepare"))
    monkeypatch.setattr(auxiliary, "publish_identity_candidate", publish)
    attach = AsyncMock()
    monkeypatch.setattr(completion, "_attach_final_tables", attach)
    with pytest.raises(RuntimeError, match="whole candidate reprepare"):
        await completion.attach_generic_source_candidate(publisher, binding_by_field)
    assert events == ["native", "index", "index", "index", "index", "relationships"]
    publish.assert_awaited_once_with(publisher, binding_by_field)
    attach.assert_not_awaited()
    publisher.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_origin", [False, True])
async def test_derived_edges_keep_explicit_unresolved_targets_but_require_their_source(monkeypatch, missing_origin):
    """Unresolved declared references remain diagnostic evidence, as before."""
    binding_by_field = {
        "identity": {"dataset_id": "dataset"},
        "tables": {
            root: {"name": "candidate_" + str(index)} for index, root in enumerate(completion.storage.RELATIONS)
        },
    }
    monkeypatch.setattr(completion, "_witness_relationships", AsyncMock(return_value="a" * 64))

    async def missing(query, *arguments):
        if len(arguments) == 2 and arguments[1] in {"InsurancePlan", "OrganizationAffiliation", "Organization"}:
            return missing_origin or arguments[1] == "Organization"
        return False

    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=missing))
    if missing_origin:
        with pytest.raises(GenericCandidateError, match="derived_relationship_changed"):
            await completion._relationships(connection, SimpleNamespace(schema="source_schema"), binding_by_field)
    else:
        await completion._relationships(connection, SimpleNamespace(schema="source_schema"), binding_by_field)
        checked_origins = [call.args[2] for call in connection.fetchval.await_args_list[1:]]
        assert checked_origins == ["InsurancePlan", "InsurancePlan", "OrganizationAffiliation"]


@pytest.mark.asyncio
async def test_recheck_samples_large_heaps_in_random_order():
    """Small heaps are read whole; large ones take a shuffled TABLESAMPLE bounded by the recheck size."""
    rows = [{"resource_type": "Location", "resource_id": f"site-{index}"} for index in range(3)]
    statements = []

    async def cursor(query, prefetch):
        statements.append(query)
        for row in rows:
            yield row

    connection, policy, seen = SimpleNamespace(cursor=cursor), SimpleNamespace(schema="mrf"), []
    columns = ("resource_type", "resource_id")
    await completion._recheck_reduced_rows(connection, policy, "pdgs_x_r", columns, seen.append, 25_000)
    await completion._recheck_reduced_rows(connection, policy, "pdgs_x_r", columns, seen.append, 3)
    assert seen == rows + rows
    assert "TABLESAMPLE SYSTEM" in statements[0] and "ORDER BY random()" in statements[0]
    assert f"LIMIT {completion._RECHECK_ROWS}" in statements[0] and "TABLESAMPLE" not in statements[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("witness_mode", ["present", "empty", "orphan"])
async def test_python_reduction_keeps_release_bound_witnesses_and_refuses_orphans(witness_mode):
    resources = [_resource("same"), _resource("same", values=("latest",))]
    witnesses = [] if witness_mode == "empty" else [_witness(), _witness()]

    def cursor(query, *arguments, prefetch):
        return _stream(witnesses if '"input_witness"' in query else resources)

    connection = SimpleNamespace(
        cursor=cursor,
        execute=AsyncMock(),
        copy_records_to_table=AsyncMock(side_effect=lambda _name, **options: f"COPY {len(options['records'])}"),
    )
    directory = SimpleNamespace(
        is_semantic_resource_hash_contract=lambda _contract: False,
        resource_payload_sha256_for_contract=lambda payload, _contract, **_options: hashlib.sha256(
            json.dumps(payload, sort_keys=True).encode()
        ).hexdigest(),
    )
    binding_by_field = {
        "tables": {"resource_input": {"name": "input_rows"}, "witness_input": {"name": "input_witness"}},
        "resets": {},
    }
    arguments = (
        connection,
        SimpleNamespace(schema="candidate"),
        binding_by_field,
        SimpleNamespace(dataset_id="dataset", resource_hash_contract="synthetic"),
        directory,
        {completion.storage.RESOURCE: {"name": "resources"}, completion.storage.WITNESS: {"name": "witnesses"}},
        "a" * 64 if witness_mode == "present" else None,
    )
    if witness_mode == "orphan":
        with pytest.raises(GenericCandidateError, match="unexpected_witness"):
            await completion._reduce_payloads_python(*arguments)
    else:
        await completion._reduce_payloads_python(*arguments)
    copied = connection.copy_records_to_table.await_args_list
    assert copied[0].args == ("resources",)
    assert json.loads(copied[0].kwargs["records"][0][completion.RESOURCE_COLUMNS.index("payload_json")]) == {
        "values": ["latest"]
    }
    if witness_mode == "present":
        assert len(copied) == 2 and copied[1].args == ("witnesses",)
        assert len(copied[1].kwargs["records"]) == 1
        assert copied[1].kwargs["records"][0][completion.WITNESS_COLUMNS.index("release_id")] == "a" * 64
    else:
        assert len(copied) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("in_transaction", [False, True])
async def test_named_source_rows_preserve_query_parameters_and_require_atomic_read(monkeypatch, in_transaction):
    from process import network_cms_registry_address_capture as capture

    rows = [{"dataset_id": "dataset", "resource_count": 2}]
    session = SimpleNamespace(execute=AsyncMock(return_value=rows))
    monkeypatch.setattr(capture, "NativeReceiptSession", lambda _connection: session)
    reader = completion._FrozenSourceRows(SimpleNamespace(is_in_transaction=lambda: in_transaction))
    query = "SELECT * FROM candidate.header WHERE dataset_id=:dataset_id"
    if in_transaction:
        assert await reader.all(query, dataset_id="dataset") is rows
        statement, parameters = session.execute.await_args.args
        assert str(statement) == query and parameters == {"dataset_id": "dataset"}
    else:
        with pytest.raises(GenericCandidateError, match="transaction_required"):
            await reader.all(query, dataset_id="dataset")
        session.execute.assert_not_awaited()
