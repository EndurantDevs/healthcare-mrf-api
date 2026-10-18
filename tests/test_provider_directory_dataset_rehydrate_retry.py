# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retry admission and complete-member checkpoint journal regressions."""

from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_dataset_rehydrate_journal as protocol
from process import provider_directory_dataset_rehydrate_journal_store as store
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError, RehydrationRequest
from process.ptg_parts import ptg_source_attempt_actions as actions
from tests.test_provider_directory_dataset_rehydrate_journal import _case, _sealed


def _journal(rows, *, predecessor=None, members=("Location",)):
    request = RehydrationRequest("source", "dataset", "root", "child", members, retry_of_run_id="repair")
    scope = SimpleNamespace(
        source_id="source",
        dataset_id="dataset",
        acquisition_root_run_id="root",
        endpoint_id="endpoint",
        dataset_hash="a" * 64,
        resource_types=("Location", "Organization"),
    )
    expected_resource_counts = tuple((resource, 2 if resource == "Location" else 0) for resource in members)
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[
                SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: deepcopy(rows))),
                SimpleNamespace(scalar_one=lambda: "17"),
            ]
        )
    )
    runtime = SimpleNamespace(database=object(), schema="mrf")
    return store.CheckpointOutputJournal(runtime, request, scope, expected_resource_counts, predecessor), session


def _row(resource, journal, *, owner="repair"):
    return dict(
        resource_type=resource,
        endpoint_id="endpoint",
        dataset_hash="a" * 64,
        expected_input_count=2 if resource == "Location" else 0,
        owner_run_id=owner,
        evidence_json={"output_snapshots": journal, "kept": resource},
        witness='{"native":"witness"}',
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", [None, "task_parent", "stored_parent", "importer", "status", "parent_active", "self", "bound_source"]
)
async def test_retry_predecessor_comes_from_admitted_outer_rows_before_source_transaction(monkeypatch, damage):
    events = []
    session = object()

    @asynccontextmanager
    async def transaction():
        events.append("ordinary_control")
        yield session

    database = SimpleNamespace(
        transaction=transaction, _transaction_binding=lambda: object() if damage == "bound_source" else None
    )
    runtime = SimpleNamespace(database=database, schema="mrf")
    request = RehydrationRequest("source", "dataset", "root", "child", retry_of_run_id="repair")
    child_by_field = dict(
        run_id="child", importer="provider-directory-fhir", status="running", retry_of_run_id="repair"
    )
    parent_by_field = dict(run_id="repair", importer="provider-directory-fhir", status="failed")
    if damage == "task_parent":
        request = replace(request, retry_of_run_id="foreign")
    if damage == "stored_parent":
        child_by_field["retry_of_run_id"] = None
    if damage == "importer":
        child_by_field["importer"] = "foreign"
    if damage == "status":
        child_by_field["status"] = "canceled"
    if damage == "parent_active":
        parent_by_field["status"] = "running"
    if damage == "self":
        request = replace(request, retry_of_run_id="child")
    loader = AsyncMock(side_effect=[child_by_field, parent_by_field])
    monkeypatch.setattr(actions, "_load_outer_run", loader)
    if damage:
        with pytest.raises(DatasetRehydrationError, match="retry_lineage_changed"):
            await store.verify_retry_predecessor(runtime, request)
    else:
        assert await store.verify_retry_predecessor(runtime, request) == "repair"
        assert events == ["ordinary_control"]
        assert [call.kwargs["run_id"] for call in loader.await_args_list] == ["child", "repair"]
        assert all(call.args[0] is session and call.kwargs["lock_row"] is False for call in loader.await_args_list)
    if damage in {"self", "bound_source"}:
        loader.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "unverified", "foreign_owner", "source_pin", "members", "targets"])
async def test_retry_child_resumes_committed_page_with_original_operation_and_stage_ids(monkeypatch, damage):
    initial_journal, stage, _, _ = _case()
    copied = protocol.record_stage(initial_journal, stage, cursor=["source", "last"], rows=2)
    checkpoint_row = _row("Location", copied, owner="foreign" if damage == "foreign_owner" else "repair")
    journal, session = _journal([checkpoint_row], predecessor=None if damage == "unverified" else "repair")
    child_operation_by_field = {**copied["active"]["operation"], "run_id": "child"}
    output_targets = tuple(copied["active"]["targets"])
    if damage == "source_pin":
        child_operation_by_field = deepcopy(child_operation_by_field)
        child_operation_by_field["source_pins"][0]["dataset_id"] = "foreign"
    elif damage == "members":
        child_operation_by_field["resource_types"] = ["Organization"]
    elif damage == "targets":
        output_targets = ("provider_directory_organization",)

    @asynccontextmanager
    async def transaction(selected):
        assert selected is journal
        yield session, await store._lock_members(journal, session)

    monkeypatch.setattr(store, "checkpoint_output_transaction", transaction)
    if damage:
        with pytest.raises(DatasetRehydrationError, match="retry_owner_changed|active_conflict"):
            await store.preparation_operation(journal, child_operation_by_field, output_targets)
    else:
        stable = await store.preparation_operation(journal, child_operation_by_field, output_targets)
        assert stable == copied["active"]["operation"] and stable["run_id"] == "repair"
        resumed = protocol.begin_preparation(copied, stable, output_targets, ["Location"])
        assert resumed == copied and resumed["active"]["stages"][stage.target]["rows"] == 2
        continued = protocol.record_stage(resumed, stage, cursor=["source", "next"], rows=3)
        store._preserve_history(copied, continued)
        assert continued["active"]["operation_digest"] == stage.operation_digest
        assert continued["active"]["stages"][stage.target]["stage"] == stage.to_payload()
        assert checkpoint_row["evidence_json"]["output_snapshots"] == copied


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "active_member", "scope", "count"])
async def test_default_all_resource_group_keeps_completed_subset_history_and_new_member(monkeypatch, damage):
    sealed, retained = _sealed()
    published = protocol.publish_generation(sealed, retained.operation_digest, (retained,), {"Location": {"input": 2}})
    empty = protocol.output_journal({})
    checkpoint_rows = [
        _row("Location", sealed if damage == "active_member" else published),
        _row("Organization", empty, owner="child"),
    ]
    if damage == "scope":
        checkpoint_rows[1]["dataset_hash"] = "b" * 64
    elif damage == "count":
        checkpoint_rows[1]["expected_input_count"] = 1
    journal, session = _journal(checkpoint_rows, members=("Location", "Organization"))
    if damage:
        with pytest.raises(DatasetRehydrationError, match="member_receipts_disagree|checkpoint_scope_changed"):
            await store._lock_members(journal, session)
    else:
        snapshot = await store._lock_members(journal, session)
        assert snapshot.journal == published and snapshot.journal["active"] is None
        operation_by_field = {
            **sealed["active"]["operation"],
            "run_id": "child",
            "resource_types": ["Location", "Organization"],
        }
        active = protocol.begin_preparation(
            snapshot.journal,
            operation_by_field,
            ("provider_directory_location", "provider_directory_organization"),
            journal.members,
        )
        store._preserve_history(snapshot.journal, active)
        assert active["generations"] == published["generations"]
        assert active["active"]["members"] == ["Location", "Organization"]
        assert snapshot.rows[0]["evidence_json"]["output_snapshots"] == published
        assert snapshot.rows[1]["evidence_json"]["output_snapshots"] == empty
