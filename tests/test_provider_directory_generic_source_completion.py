# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete-key semantics, exact raw replay and bounded asynchronous output."""

import hashlib
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_generic_source_completion as completion
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_generic_candidate import _merge, _resource
from tests.test_provider_directory_generic_source_storage import _isolated_harness, harness


@asynccontextmanager
async def _prepared_detached(harness, monkeypatch):
    from process import provider_directory_generic_source_policy as policy

    async with _isolated_harness(harness, monkeypatch) as publisher:
        binding = harness.header["candidate_storage_json"]
        binding["phase"] = "prepared"
        for index, root in enumerate(completion.storage.RELATIONS):
            binding["tables"][root] = dict(
                name="pdgs_" + "a" * 32 + "_" + completion.storage._SUFFIXES[root],
                oid=30 + index,
                owner=100,
                layout_sha256="e" * 64,
            )
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
        harness.connection.fetchval = AsyncMock(return_value=False)
        harness.connection.fetch = AsyncMock(
            side_effect=lambda sql, *args: (
                [{"attname": "dataset_id"}, {"attname": "candidate_storage_json"}]
                if sql.startswith("SELECT attname")
                else []
            )
        )
        execute = harness.connection.execute.side_effect
        capture = completion.storage._capture.side_effect

        async def snapshot_execute(sql, *args):
            if sql.startswith("INSERT INTO") or sql.startswith("UPDATE") and '_h"' in sql:
                harness.sql.append(sql)
                return "INSERT 0 1" if sql.startswith("INSERT") else "UPDATE 1"
            return await execute(sql, *args)

        async def snapshot_capture(connection, policy, names, **kwargs):
            captured_by_name = await capture(connection, policy, names, **kwargs)
            for index, suffix in enumerate(("h", "p")):
                name = "pdgs_" + "a" * 32 + "_" + suffix
                if name in names:
                    captured_by_name[name] = dict(name=name, oid=90 + index, owner=100, layout_sha256="f" * 64)
            return {name: captured_by_name[name] for name in names}

        harness.connection.execute.side_effect = snapshot_execute
        monkeypatch.setattr(completion.storage, "_capture", AsyncMock(side_effect=snapshot_capture))
        token = policy._OWNER.set((publisher, object()))
        try:
            yield publisher, binding
        finally:
            policy._OWNER.reset(token)


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
        completion.storage.validate_handoff_document(document, {**expected_storage, "owner_oid": 999})
    assert policy._PUBLISHER.get() is None


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
            table["owner"] == publisher.storage_binding.owner_oid for table in handoff.document["tables"].values()
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
        harness.connection.execute.assert_not_awaited()
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
            }
        }
        await completion.record_candidate_handoff(harness.directory, harness.candidate, handoffs_by_dataset)
        handoffs_by_dataset[harness.candidate.dataset_id]["sha256"] = "f" * 64
        with pytest.raises(GenericCandidateError, match="handoff_changed"):
            await completion.record_candidate_handoff(harness.directory, harness.candidate, handoffs_by_dataset)
        harness.connection.execute.assert_not_awaited()


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
        harness.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_detached_handoff_replay_refuses_replaced_heap_before_mutation(harness, monkeypatch):
    async with _prepared_detached(harness, monkeypatch) as (publisher, binding):
        result = await completion.prepare_generic_source_handoff(publisher, binding)
        actual_by_name = {
            proof["name"]: {**proof, "oid": proof["oid"] + 1} for proof in result.document["tables"].values()
        }
        monkeypatch.setattr(completion.storage, "_capture", AsyncMock(return_value=actual_by_name))
        harness.connection.execute.reset_mock()
        with pytest.raises(GenericCandidateError, match="storage_binding_changed"):
            await completion.prepare_generic_source_handoff(publisher, result.document)
        harness.connection.execute.assert_not_awaited()


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
async def test_complete_semantic_key_can_cross_many_cursor_and_copy_pages(monkeypatch):
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


def _witness():
    raw_by_field = {"resourceType": "Organization", "id": "example"}
    return dict(
        dataset_id="dataset",
        source_id="cms-npd",
        release_id="a" * 64,
        resource_type="Organization",
        resource_id="example",
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
        fetchrow=AsyncMock(
            side_effect=[
                {"session": "publisher", "current": "owner"},
                {"status": "acquiring", "is_current": False},
            ]
        ),
        execute=AsyncMock(),
    )
    publisher = SimpleNamespace(
        connection=connection,
        policy=SimpleNamespace(schema="source_schema", owner_role="owner", publisher_role="publisher"),
    )
    with pytest.raises(GenericCandidateError, match="validated_admission_required"):
        await completion.attach_generic_source_candidate(
            publisher, {"phase": "prepared", "identity": {"dataset_id": "dataset"}}
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
        "phase": "prepared",
        "identity": {"dataset_id": "dataset", "metadata": {}},
        "relationship_sha256": None,
        "tables": {root: {"name": "heap_" + str(index)} for index, root in enumerate(completion.storage.RELATIONS)},
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
            fetchrow=AsyncMock(side_effect=[{"session": "publisher", "current": "owner"}, header_by_field]),
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
