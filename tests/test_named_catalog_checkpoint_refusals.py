# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Malformed catalog checkpoints cannot advance resume or replace validation failures."""

import copy
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_store as catalog
from process import registry_fhir_catalog_sync as worker
from tests.test_registry_fhir_catalog_worker_transaction import PreparationConnection, preparation_case


@pytest.fixture
def checkpoint():
    copy_target, snapshot, config = preparation_case()
    preparation_by_field = {
        "target": dict(vars(copy_target)),
        "previous_metadata_generation": "a" * 64,
        "accounting": {"copied_rows": 7},
    }
    snapshot["custody_json"] = {"preparation": preparation_by_field}
    snapshot["accounting_json"] = {}
    return copy_target, snapshot, config


def test_retained_json_checkpoint_keeps_its_names_previous_generation_and_accounting(checkpoint):
    copy_target, snapshot, config = checkpoint
    encoded = json.dumps(snapshot["custody_json"], ensure_ascii=False, indent=2)
    snapshot["custody_json"] = encoded
    original_snapshot = copy.deepcopy(snapshot)
    preparation_by_field = catalog.catalog_snapshot_preparation(snapshot)
    handoff = catalog.named_catalog_preparation(snapshot, config.grant.control_schema)
    assert preparation_by_field["target"] == vars(copy_target)
    assert preparation_by_field["previous_metadata_generation"] == "a" * 64
    assert preparation_by_field["accounting"] == {"copied_rows": 7}
    assert handoff["storage_schema"] == copy_target.schema_name
    assert handoff["snapshot"]["target"] == vars(copy_target)
    assert snapshot == original_snapshot


@pytest.mark.parametrize(
    "damage", ["outer_extra", "preparation", "extra", "target", "schema", "previous", "accounting", "bounds"]
)
def test_malformed_retained_checkpoint_refuses_before_resume(checkpoint, damage):
    _target, snapshot, config = checkpoint
    preparation_by_field = snapshot["custody_json"]["preparation"]
    match damage:
        case "outer_extra":
            snapshot["custody_json"]["extra"] = True
        case "preparation":
            snapshot["custody_json"]["preparation"] = []
        case "extra":
            preparation_by_field["extra"] = True
        case "target":
            preparation_by_field["target"].pop("candidate_id")
        case "schema":
            snapshot["schema_name"] = "another_candidate"
        case "previous":
            preparation_by_field["previous_metadata_generation"] = "not-a-generation"
        case "accounting":
            preparation_by_field["accounting"] = []
        case _:
            preparation_by_field["accounting"]["detail"] = "x" * (catalog.DEFAULT_BYTES + 1)
    original_snapshot = copy.deepcopy(snapshot)
    with pytest.raises(ValueError, match="registry_fhir_catalog_store_"):
        catalog.named_catalog_preparation(snapshot, config.grant.control_schema)
    assert snapshot == original_snapshot


def test_nested_operator_configuration_projects_the_existing_role_profile(checkpoint):
    _target, _snapshot, config = checkpoint
    configured = SimpleNamespace(preparation_roles=config.preparation_roles)
    assert catalog.named_catalog_preparation_roles(configured) == config.preparation_roles


@pytest.mark.parametrize("damage", ["unsorted", "duplicate", "owner_overlap", "publisher_missing", "extra"])
def test_operator_role_profile_refuses_ambiguous_or_overlapping_configuration(checkpoint, damage):
    _target, _snapshot, config = checkpoint
    roles = copy.deepcopy(config.preparation_roles)
    if damage == "unsorted":
        roles["application_roles"].reverse()
    elif damage == "duplicate":
        roles["application_roles"].append(roles["application_roles"][-1])
    elif damage == "owner_overlap":
        roles["application_roles"] = sorted({*roles["application_roles"], roles["namespace_owner_role"]})
    elif damage == "publisher_missing":
        roles["application_roles"].remove(roles["publisher_role"])
    else:
        roles["extra"] = True
    with pytest.raises(ValueError, match="preparation_roles_invalid"):
        catalog.named_catalog_preparation_roles(roles)


@pytest.mark.parametrize("same_previous", [False, True])
def test_resume_uses_the_original_target_and_refuses_a_changed_predecessor(checkpoint, same_previous):
    copy_target, retained, _config = checkpoint
    proposed_by_field = {**retained, "metadata_generation": "b" * 64}
    staged, accounting = {}, {}
    previous_by_field = {"metadata_generation": "a" * 64 if same_previous else "c" * 64}
    if same_previous:
        assert (
            worker._reused_catalog_preparation_target(
                retained, proposed_by_field, previous_by_field, staged, accounting
            )
            == copy_target
        )
        assert staged == {"checkpointed": True}
        assert accounting["copied_rows"] == 7 and accounting["resumed_snapshot"] is True
        assert accounting["resumed_metadata_generation"] == retained["metadata_generation"]
        assert accounting["resumed_snapshot_as_of"] == retained["as_of"].isoformat()
    else:
        with pytest.raises(ValueError, match="preparation_head_changed"):
            worker._reused_catalog_preparation_target(
                retained, proposed_by_field, previous_by_field, staged, accounting
            )
        assert staged == {}
        assert "resumed_snapshot" not in accounting


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_failure", ["no_claim", "lost_cas", "cancel_failed"])
async def test_refused_validation_stays_primary_when_candidate_abandonment_cannot_complete(
    monkeypatch, checkpoint, cleanup_failure
):
    copy_target, snapshot, config = checkpoint
    connection = PreparationConnection()
    connection.execute.return_value = "DELETE 0"

    @asynccontextmanager
    async def acquire():
        yield connection

    @asynccontextmanager
    async def workspace(_connection):
        yield object()

    @asynccontextmanager
    async def reads(*_connections):
        yield

    refusal = ValueError("catalog validation failed")
    claim = object()
    named = SimpleNamespace(yield_named_claim=AsyncMock(), request_cancel=AsyncMock(), abandon=AsyncMock())
    if cleanup_failure == "cancel_failed":
        named.request_cancel.side_effect = RuntimeError("cancel unavailable")
    preparation_by_field = {"state": "frozen", "preparation_id": copy_target.candidate_id, "admission_sha256": "a" * 64}
    admit = AsyncMock(
        side_effect=[
            (named, preparation_by_field, claim),
            (named, preparation_by_field, None if cleanup_failure == "no_claim" else claim),
        ]
    )
    monkeypatch.setattr(worker, "admit_catalog_preparation", admit)
    monkeypatch.setattr(worker, "fhir_catalog_reference_workspace", workspace)
    monkeypatch.setattr(worker, "fhir_catalog_registry_reads", reads)
    monkeypatch.setattr(worker, "validate_fhir_catalog_snapshot_data", AsyncMock(side_effect=refusal))
    original_snapshot = copy.deepcopy(snapshot)
    with pytest.raises(ValueError) as failure:
        await worker._seal_catalog_snapshot(config, copy_target, snapshot, SimpleNamespace(acquire=acquire))
    assert failure.value is refusal
    assert snapshot == original_snapshot
    assert len(refusal.__notes__) == 1 and "was not abandoned" in refusal.__notes__[0]
    assert named.abandon.await_count == int(cleanup_failure == "lost_cas")
    assert connection.execute.await_count == int(cleanup_failure == "lost_cas")
    if cleanup_failure == "lost_cas":
        query, generation, schema = connection.execute.await_args.args
        assert "published_at IS NULL" in query
        assert (generation, schema) == (snapshot["metadata_generation"], copy_target.schema_name)
