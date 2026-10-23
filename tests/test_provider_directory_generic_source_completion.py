# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete-key semantics, exact raw replay and bounded asynchronous output."""

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_generic_source_completion as completion
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_generic_candidate import _merge, _resource


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
