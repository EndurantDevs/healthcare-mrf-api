# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The cutover keeps exact inputs, short lock limits and existing header transitions."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_rooted_graph_publication_finalize as finalize
from process.provider_directory_generic_candidate import GenericCandidateError


@pytest.fixture
def cutover():
    connection = SimpleNamespace(execute=AsyncMock(), fetch=AsyncMock(), fetchval=AsyncMock())
    handoff_by_field = {
        "identity": {"dataset_id": "candidate", "root_dataset_id": "predecessor", "endpoint_id": "endpoint"},
        "operation": {"publication_acquisition_id": "operation"},
        "tables": {root: {"name": "candidate_" + str(index)} for index, root in enumerate(finalize.ROOTS)},
        "dataset_hash": "a" * 64,
        "preparation_id": "preparation",
        "counts": {"Practitioner": 2, "Location": 1},
    }
    transaction = SimpleNamespace(connection=connection)
    runtime = SimpleNamespace(
        catalog_schema="catalog", candidate_schema="candidate", current_transaction=lambda database: transaction
    )
    preparations = SimpleNamespace(
        connection=connection, fhir_source=(None, SimpleNamespace(document=handoff_by_field, document_sha256="b" * 64))
    )
    return SimpleNamespace(
        connection=connection,
        document=handoff_by_field,
        runtime=runtime,
        preparations=preparations,
        database=SimpleNamespace(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["INSERT 0 1", "INSERT 0 0", "INSERT 0 2"])
async def test_header_copy_requires_one_row_and_uses_current_transaction_timestamp(cutover, changed):
    cutover.connection.fetch.return_value = [
        {"attname": name} for name in ("dataset_id", "created_at", "metadata_json")
    ]
    cutover.connection.execute.return_value = changed
    if changed == "INSERT 0 1":
        await finalize._insert_frozen_header(cutover.connection, cutover.runtime, cutover.document, finalize.PARENT)
    else:
        with pytest.raises(GenericCandidateError, match="header_changed"):
            await finalize._insert_frozen_header(cutover.connection, cutover.runtime, cutover.document, finalize.PARENT)
    statement, dataset_id = cutover.connection.execute.call_args.args
    assert "transaction_timestamp()" in statement and 's."created_at"' not in statement
    assert 's."metadata_json"' in statement and "WHERE dataset_id=$1" in statement and dataset_id == "candidate"


@pytest.mark.asyncio
async def test_header_switch_validates_both_before_supersession_and_publishes_both(cutover, monkeypatch):
    calls = []

    async def status(statement, **values):
        calls.append((statement, values))
        return 1

    async def supersede(database, previous):
        assert database is cutover.database and previous == "previous"
        assert len(calls) == 2 and all("status='validated'" in statement for statement, _ in calls)
        calls.append(("supersede", {}))

    cutover.database.status = status
    monkeypatch.setattr(finalize, "current_rooted_graph_runtime", lambda database: cutover.runtime)
    monkeypatch.setattr(finalize, "supersede_exact_current_dataset", supersede)
    await finalize._switch_frozen_headers(cutover.database, cutover.document, "previous")
    assert len(calls) == 5 and calls[2][0] == "supersede"
    assert all("status='published',is_current=true" in statement for statement, _ in calls[3:])
    assert all(values["dataset_id"] == "candidate" for _, values in calls[:2] + calls[3:])
    assert all(values["dataset_hash"] == "a" * 64 for _, values in calls[:2])


@pytest.mark.asyncio
@pytest.mark.parametrize("failed_step", range(4))
async def test_header_switch_stops_on_any_changed_transition(cutover, monkeypatch, failed_step):
    cutover.database.status = AsyncMock(side_effect=[1] * failed_step + [0])
    supersede = AsyncMock()
    monkeypatch.setattr(finalize, "current_rooted_graph_runtime", lambda database: cutover.runtime)
    monkeypatch.setattr(finalize, "supersede_exact_current_dataset", supersede)
    with pytest.raises(GenericCandidateError, match="header_changed"):
        await finalize._switch_frozen_headers(cutover.database, cutover.document, "previous")
    assert cutover.database.status.await_count == failed_step + 1
    assert supersede.await_count == (1 if failed_step >= 2 else 0)


def _publication_seams(cutover, monkeypatch):
    previous = SimpleNamespace(dataset_id="predecessor")
    proof = SimpleNamespace(current=previous)
    monkeypatch.setattr(finalize, "current_rooted_graph_runtime", lambda database: cutover.runtime)
    monkeypatch.setattr(finalize, "current_root_proof", lambda database: proof)
    seams = SimpleNamespace(
        previous=previous,
        fence=AsyncMock(return_value=previous),
        insert=AsyncMock(),
        attach=AsyncMock(),
        switch=AsyncMock(),
    )
    monkeypatch.setattr(finalize, "require_current_root_fence", seams.fence)
    monkeypatch.setattr(finalize, "_insert_frozen_header", seams.insert)
    monkeypatch.setattr(finalize, "_attach_frozen_group", seams.attach)
    monkeypatch.setattr(finalize, "_switch_frozen_headers", seams.switch)
    monkeypatch.setattr(finalize, "capture_root_input_fence", AsyncMock(return_value='{"post":"generation"}'))
    return seams


@pytest.mark.asyncio
async def test_cutover_refuses_busy_publication_before_touching_headers(cutover, monkeypatch):
    seams = _publication_seams(cutover, monkeypatch)
    cutover.connection.fetchval.return_value = False
    with pytest.raises(GenericCandidateError, match="publication_busy"):
        await finalize.publish_prepared_rooted_graph_dataset(
            cutover.preparations, "claim", {"state": "validated"}, database=cutover.database
        )
    seams.insert.assert_not_awaited()
    seams.attach.assert_not_awaited()
    seams.switch.assert_not_awaited()
    assert "pg_try_advisory_xact_lock" in cutover.connection.fetchval.call_args.args[0]


@pytest.mark.asyncio
async def test_cutover_receipt_binds_exact_handoff_and_post_switch_fence(cutover, monkeypatch):
    seams = _publication_seams(cutover, monkeypatch)
    completed = datetime(2026, 1, 1, tzinfo=UTC)
    cutover.connection.fetchval.side_effect = [True, completed]
    receipt = await finalize.publish_prepared_rooted_graph_dataset(
        cutover.preparations, "claim", {"state": "validated"}, database=cutover.database
    )
    assert receipt["dataset_id"] == "candidate" and receipt["endpoint_id"] == "endpoint"
    assert receipt["publication_acquisition_id"] == "operation" and receipt["preparation_id"] == "preparation"
    assert receipt["dataset_hash"] == "a" * 64 and receipt["source_handoff_sha256"] == "b" * 64
    assert receipt["resource_count"] == 3 and receipt["completed_at"] == completed.isoformat()
    assert receipt["rooted_input_fence"] == {"post": "generation"} and receipt["cutover_elapsed_seconds"] >= 0
    assert [call.args[-1] for call in seams.insert.await_args_list] == [finalize.PARENT, finalize.HEADER]
    seams.fence.assert_awaited_once()
    seams.switch.assert_awaited_once_with(cutover.database, cutover.document, seams.previous)
    statements = [call.args[0] for call in cutover.connection.execute.await_args_list]
    assert statements[:2] == ["SET LOCAL lock_timeout='1s'", "SET LOCAL statement_timeout='3s'"]
    assert statements[-1].startswith("SELECT set_config(")


@pytest.mark.asyncio
async def test_cutover_requires_same_transaction_and_exact_predecessor_before_locks(cutover, monkeypatch):
    _publication_seams(cutover, monkeypatch)
    cutover.document["identity"]["root_dataset_id"] = "other"
    with pytest.raises(GenericCandidateError, match="predecessor_changed"):
        await finalize.publish_prepared_rooted_graph_dataset(
            cutover.preparations, "claim", {"state": "validated"}, database=cutover.database
        )
    cutover.document["identity"]["root_dataset_id"] = "predecessor"
    cutover.preparations.connection = object()
    with pytest.raises(GenericCandidateError, match="transaction_changed"):
        await finalize.publish_prepared_rooted_graph_dataset(
            cutover.preparations, "claim", {"state": "validated"}, database=cutover.database
        )
    cutover.connection.execute.assert_not_awaited()
