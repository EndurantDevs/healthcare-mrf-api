# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host regressions for source custody, indexed candidates and exact publication fences."""

import asyncio
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from process import source_profile_result_archive as archive
from tests.source_profile_archive_support import retained_run
from tests.test_source_profile_result_archive import (
    _versioned_manifest,
    native_capture,
    native_handoff,
)
from tests.test_source_profile_result_archive import (
    configured_native_role_names as configured_native_role_names,
)

IMPORTER = "massachusetts-borim-profile"
pytestmark = pytest.mark.asyncio

WITNESS_PREPARATION_STEPS = [
    "bundle",
    "envelope",
    "heap",
    "copy",
    "index",
    "metrics",
    "describe",
    "remove",
    "publication",
    "seal",
    "validate",
    "sets",
    "attach",
]


def recorded_step(events, event, response=None, should_fail=False):
    """Record a boundary before returning its response or raising a refusal."""

    async def call(*args, **kwargs):
        events.append(event)
        if should_fail:
            raise RuntimeError("synthetic boundary failure")
        return response

    return AsyncMock(side_effect=call)


def adoption_pins(manifest, receipt_dict, is_created_here=False):
    """Build the exact pin authority shared by rollback and maintenance checks."""
    return [
        {
            "source_key": manifest["source_key"],
            "run_id": manifest["run_id"],
            "purpose": "adoption",
            "authority_json": {
                "validation": receipt_dict,
                "root_run_id": manifest["run_id"],
                "run_ids": manifest["run_ids"],
                "created_here": is_created_here,
            },
        }
    ]


def publication_attachment(manifest, ownership):
    """Attach only model-derived children to their declared qualified names."""
    return {
        "contract": archive.ATTACHMENT_CONTRACT,
        "created_run_ids": manifest["run_ids"],
        "reused_run_ids": [],
        "parents": [[name, archive._reference("mrf", name)] for name in archive.TABLES],
        "children": [
            [name, child, archive._reference(ownership.schema_name, child)]
            for name, child in zip(archive.TABLES, archive.PUBLICATION_TABLES, strict=True)
        ],
        "ancestor_relations": [],
    }


def maintenance_receipt(handoff, manifest, ownership, parents, include_publication):
    """Seal maintenance inputs, including publication parents when applicable."""
    receipt_dict = {
        "contract": archive.NATIVE_PUBLICATION_CONTRACT,
        "handoff": handoff,
        "result": manifest,
        "pin_id": str(uuid4()),
        "sealed_owner_oid": 41,
        "ownership": archive.ownership_dict(ownership),
        "destination_schema": "mrf",
        "source_result": {"published": True},
        **(
            {
                "publication": {
                    "contract": archive.ATTACHMENT_CONTRACT,
                    "created_run_ids": manifest["run_ids"],
                    "reused_run_ids": [],
                    "ancestor_relations": [],
                    "parents": parents,
                    "children": [
                        [name, child, archive._reference(ownership.schema_name, child)]
                        for name, child in zip(archive.TABLES, archive.PUBLICATION_TABLES, strict=True)
                    ],
                }
            }
            if include_publication
            else {}
        ),
    }
    receipt_dict["validation_sha256"] = archive._digest(receipt_dict)
    return receipt_dict


def rollback_receipt(prepared, pin_id, origin, publication_dict):
    """Use the installed or native receipt format for the retained origin."""
    if origin == "installed":
        return archive._validation(prepared, "e" * 64, 41, "mrf", "b" * 32, pin_id, publication=publication_dict)
    receipt_dict = {
        "contract": archive.NATIVE_CAPTURE_CONTRACT if origin == "captured" else archive.NATIVE_PUBLICATION_CONTRACT,
        "pin_id": str(pin_id),
        "result": prepared.manifest,
        "ownership": archive.ownership_dict(prepared.ownership),
        "destination_schema": "mrf",
        "sealed_owner_oid": 41,
        **({"publication": publication_dict} if origin == "published" else {}),
    }
    receipt_dict["validation_sha256"] = archive._digest(receipt_dict)
    return receipt_dict


def maintenance_control_run(handoff, receipt_dict):
    """Bind finalizing control state to the same handoff and publication receipt."""
    control_run_dict, _source = managed_rows(handoff)
    control_run_dict.update(
        run_id=handoff["run_id"],
        status="finalizing",
        phase_detail=archive.NATIVE_MAINTENANCE_PHASE,
        metrics={
            "published": True,
            "source_profile_handoff": handoff,
            "source_profile_native_publication": receipt_dict,
        },
    )
    return control_run_dict


def corrupt_managed_run(run_dict, boundary, fault):
    """Change one custody field without altering unrelated run identity."""
    if fault == "attempt":
        field = "progress" if boundary == "control" else "source_manifest"
        identity = "attempt_id" if boundary == "control" else "control_run_id"
        run_dict[field][identity] = "another_attempt" if boundary == "control" else "another_control"
        return
    replacements_by_fault = {
        "status": ("status", "completed"),
        "finished": ("finished_at", datetime(2026, 1, 2)),
        "error": ("error", {"message": "synthetic failure"}),
        "scope": ("importer" if boundary == "control" else "source_key", "unsupported"),
    }
    field, replacement = replacements_by_fault[fault]
    run_dict[field] = replacement


def mapped_result(selected_run):
    return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: selected_run, one=lambda: selected_run))


def managed_rows(handoff):
    control_run_dict = {
        "importer": handoff["importer_id"],
        "engine": "healthcare-mrf-api",
        "status": "running",
        "progress": {key: handoff[key] for key in ("attempt_id", "attempt_started_at")},
        "finished_at": None,
        "error": None,
        "node_id": handoff["node_id"],
        "params": {},
    }
    source_run_dict = retained_run(handoff["importer_id"], handoff["source_run_id"])
    source_run_dict.update(status="running", finished_at=None)
    source_run_dict["source_manifest"]["control_run_id"] = handoff["run_id"]
    return control_run_dict, source_run_dict


@pytest.mark.parametrize("boundary", ["control", "source"])
@pytest.mark.parametrize("fault", [None, "missing", "status", "attempt", "finished", "error", "scope"])
async def test_locked_managed_rows_refuse_other_attempts_and_terminal_sources(boundary, fault):
    handoff = native_handoff(IMPORTER)
    control_run_dict, source_run_dict = managed_rows(handoff)
    selected_run = control_run_dict if boundary == "control" else source_run_dict
    if fault == "missing":
        selected_run = None
    elif fault is not None:
        corrupt_managed_run(selected_run, boundary, fault)
    session = SimpleNamespace(execute=AsyncMock(return_value=mapped_result(selected_run)))
    callback = archive._native_control_run if boundary == "control" else archive._native_source_run
    arguments_dict = (session, handoff, ("running",)) if boundary == "control" else (session, handoff)
    if fault is None:
        assert await callback(*arguments_dict) is selected_run
    else:
        with pytest.raises(archive.SourceProfileArchiveError, match="attempt differs|producer differs"):
            await callback(*arguments_dict)
    assert "FOR UPDATE" in str(session.execute.await_args.args[0])


@pytest.mark.parametrize("changed", [False, True])
async def test_handoff_records_actual_location_source_digest_and_pointer_before_cas(monkeypatch, changed):
    handoff = native_handoff(IMPORTER)
    control_run_dict, source_run_dict = managed_rows(handoff)
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=[mapped_result(control_run_dict), mapped_result(source_run_dict)]),
        scalar=AsyncMock(return_value=None if changed else handoff["run_id"]),
    )
    monkeypatch.setattr(archive, "_source_lock", AsyncMock())
    pointer_dict = {"current_run_id": "b" * 32, "previous_run_id": None, "published_at": "reporting only"}
    monkeypatch.setattr(archive, "_pointer", AsyncMock(return_value=pointer_dict))
    monkeypatch.setattr(archive.native, "_relation_oid", AsyncMock(return_value=11))
    context_dict = {
        "context": {
            "control_run_id": handoff["run_id"],
            "_control_attempt_id": handoff["attempt_id"],
            "_control_attempt_started_at": handoff["attempt_started_at"],
        }
    }
    arguments_dict = dict(
        importer_id=IMPORTER, schema="mrf", source_run_id=handoff["source_run_id"], metrics={"source_records": 1}
    )
    if changed:
        with pytest.raises(archive.SourceProfileArchiveError, match="attempt changed"):
            await archive.record_native_handoff(session, context_dict, **arguments_dict)
    else:
        recorded = await archive.record_native_handoff(session, context_dict, **arguments_dict)
        assert archive.validate_native_handoff(recorded) == recorded
        assert recorded["expected"] == archive.native_pointer_identity(pointer_dict)
        assert recorded["source_manifest_sha256"] == archive._digest(source_run_dict["source_manifest"])
        assert recorded["source_contract_sha256"] == archive._native_source_contract(
            recorded, {}, source_run_dict["source_manifest"]
        )
        assert "database_oid" not in recorded and "import_run_oid" not in recorded
        assert json.loads(session.scalar.await_args.args[1]["handoff"]) == recorded
    query = str(session.scalar.await_args.args[0])
    assert "status='running'" in query and "source_profile_handoff' IS NULL" in query


@pytest.mark.parametrize("importer", [IMPORTER, archive.PROJECTION_IMPORTER])
@pytest.mark.parametrize("fault", [None, "copy", "index", "owner", "rows"])
async def test_native_capture_indexes_validates_and_seals_complete_model_family(monkeypatch, importer, fault):
    handoff = native_handoff(importer)
    dataset_id = uuid4()
    ownership = archive.StageOwnership(importer, dataset_id, tuple(sorted(archive.source_spec(importer).table_names)))
    manifest = _versioned_manifest(archive.CONTRACT, importer)
    events = []

    def checkpoint(event, response=None, should_fail=False):
        return recorded_step(events, event, response, should_fail)

    monkeypatch.setattr(archive, "_lineage", AsyncMock(return_value=[{"run_id": handoff["source_run_id"]}]))
    monkeypatch.setattr(archive.native, "_create_model_family", checkpoint("heap"))
    monkeypatch.setattr(archive, "capture_ownership", AsyncMock(return_value=ownership))
    copier = checkpoint("copy", should_fail=fault == "copy")
    monkeypatch.setattr(archive.native, "_copy_model_run_scope", copier)
    monkeypatch.setattr(archive, "complete_restore", checkpoint("index", should_fail=fault == "index"))
    monkeypatch.setattr(archive.native, "seal_model_family_storage", checkpoint("seal", should_fail=fault == "owner"))
    monkeypatch.setattr(archive, "describe_result", checkpoint("describe", manifest))
    monkeypatch.setattr(archive, "validate_stage", checkpoint("validate"))
    compare = checkpoint("compare", fault != "rows")
    monkeypatch.setattr(archive, "_are_source_model_rows_equal", compare)
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=None if fault == "owner" else '"snapshot_owner"'), execute=AsyncMock()
    )
    source_copy = archive.native.ReferenceFamilySourceCopy(AsyncMock(), 1000, 30)
    if fault is None:
        completion_result = await archive._capture_native_source(session, handoff, dataset_id, source_copy, 41)
        assert completion_result == archive.PreparedResult(manifest, ownership)
        assert events == ["heap", "copy", "index", "seal", "describe", "validate"] + ["compare"] * len(
            archive.source_spec(importer).model_types
        )
    else:
        with pytest.raises(RuntimeError, match="boundary failure|owner differs|content differs"):
            await archive._capture_native_source(session, handoff, dataset_id, source_copy, 41)
        if fault in {"copy", "index"}:
            assert "seal" not in events and "validate" not in events
    assert archive.native._create_model_family.await_args.kwargs == {"create_indexes": False}
    call = copier.await_args
    assert call.kwargs["source_copy"] is source_copy and call.kwargs["target_schema"] == ownership.schema_name
    assert call.kwargs["run_scope"][1] == [handoff["source_run_id"]]
    if importer == archive.PROJECTION_IMPORTER:
        assert call.args[1].model_types[-1].__tablename__ == handoff["projection"]["table_name"]
        assert call.kwargs["run_scope"][0][-1] == "generation_id"


@pytest.mark.parametrize("fault", [None, "owner", "copy", "index", "metrics", "sets"])
async def test_witnessed_source_keeps_one_budget_and_completes_before_attachment(monkeypatch, fault):
    from process import new_york_profile_store as producer

    handoff = native_handoff("new-york-nypp-profile")
    handoff["metrics"]["bundle"] = {"reference": "synthetic"}
    ownership = archive.StageOwnership(handoff["importer_id"], uuid4(), tuple(sorted(())))
    manifest = _versioned_manifest(archive.CONTRACT, handoff["importer_id"])
    publication_dict, source_result = {"created_run_ids": [manifest["run_id"]]}, {"published": True}
    source_copy = archive.native.ReferenceFamilySourceCopy(AsyncMock(), 1000, 30)
    events = []

    def step(event, response=None):
        return recorded_step(events, event, response, should_fail=event == fault)

    monkeypatch.setattr(archive, "_native_source_run", AsyncMock(return_value={"run_id": manifest["run_id"]}))
    monkeypatch.setattr(producer, "read_witness_bundle", step("bundle"))
    monkeypatch.setattr(archive, "_require_local_source_envelope", step("envelope"))
    monkeypatch.setattr(archive, "precreate_restore", step("heap", ownership))
    load = step("copy", 400)
    monkeypatch.setattr(archive, "_load_witnessed_source", load)
    monkeypatch.setattr(archive, "complete_restore", step("index"))
    monkeypatch.setattr(archive, "_complete_witnessed_source", step("metrics", source_result))
    monkeypatch.setattr(archive, "describe_result", step("describe", manifest))
    monkeypatch.setattr(archive, "_remove_local_source_envelope", step("remove"))
    publication_copy = step("publication", (publication_dict, 200))
    monkeypatch.setattr(archive, "_prepare_publication", publication_copy)
    monkeypatch.setattr(
        archive.native, "seal_model_family_storage", recorded_step(events, "seal", should_fail=fault == "owner")
    )
    monkeypatch.setattr(archive, "validate_stage", step("validate"))
    monkeypatch.setattr(archive, "_validate_publication_sets", step("sets"))
    attach = step("attach")
    monkeypatch.setattr(archive, "_attach_publication", attach)
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=None if fault == "owner" else '"snapshot_owner"'), execute=AsyncMock()
    )
    if fault is None:
        completion_result = await archive._prepare_witnessed_source(
            session, handoff, ownership.dataset_id, source_copy, 41
        )
        assert completion_result == (archive.PreparedResult(manifest, ownership), source_result, publication_dict)
        assert events == WITNESS_PREPARATION_STEPS
        call = publication_copy.await_args
        assert call.kwargs["source_copy"].copy_rows is source_copy.copy_rows
        assert call.kwargs["source_copy"].max_bytes == 400
        assert call.kwargs["deadline"] == load.await_args.args[4]
        queries = [str(call.args[0]) for call in session.execute.await_args_list]
        assert queries[:4] == [f'ANALYZE "{ownership.schema_name}"."{name}"' for name in archive.TABLES]
    else:
        with pytest.raises(RuntimeError, match="boundary failure|owner differs"):
            await archive._prepare_witnessed_source(session, handoff, ownership.dataset_id, source_copy, 41)
        attach.assert_not_awaited()


@pytest.mark.parametrize("fault", [None, "seal", "owner"])
@pytest.mark.parametrize("previous", [None, "b" * 32])
async def test_projection_completion_seals_exact_heap_before_recording_policy(monkeypatch, fault, previous):
    from process import florida_projection_named_source as projection

    handoff = native_handoff(archive.PROJECTION_IMPORTER)
    handoff["expected"]["current_run_id"] = previous
    seal = handoff["projection"]
    monkeypatch.setattr(archive.native, "_lock_family", AsyncMock())
    check = AsyncMock(side_effect=RuntimeError("synthetic projection changed") if fault == "seal" else None)
    freeze = AsyncMock(side_effect=PermissionError("synthetic owner refusal") if fault == "owner" else None)
    monkeypatch.setattr(projection, "named_projection_seal", check)
    monkeypatch.setattr(projection, "freeze_named_projection", freeze)
    candidate, predecessor = {"run_id": handoff["source_run_id"]}, {"run_id": previous}
    monkeypatch.setattr(archive, "_run", AsyncMock(side_effect=[candidate, predecessor] if previous else [candidate]))
    policy = Mock(return_value={"previous_run_id": previous})
    monkeypatch.setattr(projection.legacy, "native_publication_policy", policy)
    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=seal["row_count"]))
    if fault:
        with pytest.raises((RuntimeError, PermissionError), match="projection changed|owner refusal"):
            await archive._finish_native_projection(session, handoff)
        session.execute.assert_not_awaited()
        policy.assert_not_called()
    else:
        result_by_field, progress = await archive._finish_native_projection(session, handoff)
        freeze.assert_awaited_once_with(session, "mrf", seal["table_name"])
        policy.assert_called_once_with(candidate, predecessor if previous else None)
        writes = [json.loads(call.args[1]["metrics"]) for call in session.execute.await_args_list]
        assert "previous_run_id" not in writes[0]["publication"]
        assert writes[1] == result_by_field and result_by_field["publication"]["previous_run_id"] == previous
        assert result_by_field["published_providers"] == progress["done"] == progress["total"] == seal["row_count"]
        assert "status='completed'" in str(session.execute.await_args_list[0].args[0])


@pytest.mark.parametrize("fault", [None, "attempt", "source", "legacy_database", "legacy_relation"])
async def test_reauthenticated_handoff_rejects_changed_frozen_inputs(monkeypatch, fault):
    handoff = native_handoff(IMPORTER)
    control_run_dict, source_run_dict = managed_rows(handoff)
    control_run_dict.update(
        status="finalizing", phase_detail=archive.NATIVE_HANDOFF_PHASE, metrics={"source_profile_handoff": handoff}
    )
    handoff["source_manifest_sha256"] = archive._digest(source_run_dict["source_manifest"])
    handoff["source_contract_sha256"] = archive._native_source_contract(
        handoff, control_run_dict["params"], source_run_dict["source_manifest"]
    )
    handoff["handoff_sha256"] = archive._digest(
        {key: metric_value for key, metric_value in handoff.items() if key != "handoff_sha256"}
    )
    if fault == "attempt":
        control_run_dict["node_id"] = "another_node"
    if fault == "source":
        source_run_dict["source_manifest"]["cohort_sha256"] = "e" * 64
    session = SimpleNamespace(
        execute=AsyncMock(side_effect=[mapped_result(control_run_dict), mapped_result(source_run_dict)]),
        scalar=AsyncMock(return_value=12 if fault == "legacy_database" else 10),
    )
    monkeypatch.setattr(archive, "_source_lock", AsyncMock())
    monkeypatch.setattr(
        archive.native, "_relation_oid", AsyncMock(return_value=12 if fault == "legacy_relation" else 11)
    )
    if fault in {None, "legacy_database", "legacy_relation"}:
        assert await archive.require_native_handoff(session, handoff) is handoff
    else:
        with pytest.raises(archive.SourceProfileArchiveError, match="binding differs"):
            await archive.require_native_handoff(session, handoff)


@pytest.mark.parametrize("fault", [None, "attempt", "legacy_location", "source", "current", "previous"])
async def test_abandonment_authenticates_never_published_attempt_before_cleanup(monkeypatch, fault):
    handoff = native_handoff(IMPORTER)
    control_run_dict, source_run_dict = managed_rows(handoff)
    control_run_dict.update(status="canceling", metrics={"source_profile_handoff": handoff})
    handoff["source_contract_sha256"] = archive._native_source_contract(
        handoff, control_run_dict["params"], source_run_dict["source_manifest"]
    )
    if fault == "attempt":
        control_run_dict["progress"]["attempt_started_at"] = "changed"
    if fault == "source":
        source_run_dict["status"] = "completed"
    pointer_dict = {"current_run_id": "b" * 32, "previous_run_id": None}
    if fault in {"current", "previous"}:
        pointer_dict[f"{fault}_run_id"] = handoff["source_run_id"]
    session = SimpleNamespace(
        execute=AsyncMock(return_value=mapped_result(control_run_dict)),
        scalar=AsyncMock(return_value=99 if fault == "legacy_location" else 10),
    )
    monkeypatch.setattr(archive, "_source_lock", AsyncMock())
    monkeypatch.setattr(archive, "_run", AsyncMock(return_value=source_run_dict))
    monkeypatch.setattr(archive, "_pointer", AsyncMock(return_value=pointer_dict))
    monkeypatch.setattr(archive.native, "_relation_oid", AsyncMock(return_value=11))
    if fault in {None, "legacy_location"}:
        assert await archive._require_native_abandonment(session, handoff) is control_run_dict
    else:
        with pytest.raises(archive.SourceProfileArchiveError, match="abandonment"):
            await archive._require_native_abandonment(session, handoff)
    assert session.execute.await_count == 1


@pytest.mark.parametrize("importer", [IMPORTER, archive.PROJECTION_IMPORTER])
@pytest.mark.parametrize("status", ["canceling", "failed"])
async def test_abandonment_writes_exact_terminal_receipt_without_touching_publication(monkeypatch, importer, status):
    handoff = native_handoff(importer)
    monkeypatch.setattr(archive.native, "protected_publisher_owner", AsyncMock(return_value=41))
    monkeypatch.setattr(archive, "_require_native_abandonment", AsyncMock(return_value={"status": status}))
    drop = AsyncMock()
    monkeypatch.setattr(archive, "_drop_native_projection_candidate", drop)
    session = SimpleNamespace(execute=AsyncMock())
    receipt_dict = await archive.abandon_native_handoff(session, handoff)
    assert receipt_dict == {
        "contract": "source-profile-native-abandonment.v1",
        "handoff_sha256": handoff["handoff_sha256"],
    }
    queries = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(queries) == (3 if status == "canceling" else 2)
    assert "status='failed'" in queries[0] and "source_profile_native_abandonment" in queries[-1]
    assert all("provider_profile_source_publication" not in query and "DELETE" not in query for query in queries)
    assert json.loads(session.execute.await_args.args[1]["receipt"]) == receipt_dict
    if importer == archive.PROJECTION_IMPORTER:
        drop.assert_awaited_once_with(session, "mrf", handoff["projection"], source_run_id=handoff["source_run_id"])
    else:
        drop.assert_not_awaited()


@pytest.mark.parametrize("fault", [None, "missing", "transaction", "closed", "reused", "receipt"])
async def test_retained_activation_consumes_one_continuation_on_original_transaction(monkeypatch, fault):
    manifest = _versioned_manifest(archive.LEGACY_CONTRACT)
    ownership = archive.StageOwnership(IMPORTER, uuid4(), tuple(sorted(archive.TABLES)))
    prepared = archive.PreparedResult(manifest, ownership)
    validation_dict = archive._validation(prepared, "e" * 64, 41, "mrf", "b" * 32, uuid4())
    if fault == "receipt":
        validation_dict["result_sha256"] = "0" * 64
    adopt, publish_pointer = AsyncMock(), AsyncMock()
    monkeypatch.setattr(archive, "_adopt_validated_result", adopt)
    monkeypatch.setattr(archive, "_publish_result_pointer", publish_pointer)
    transaction_by_field = {"current": object()}
    session = SimpleNamespace(
        in_transaction=lambda: fault != "closed", get_transaction=lambda: transaction_by_field["current"]
    )

    async def continuation(observed, publish):
        assert observed is session and adopt.await_count == 1
        if fault == "transaction":
            transaction_by_field["current"] = object()
        if fault != "missing":
            await publish()
        if fault == "reused":
            await publish()

    if fault:
        with pytest.raises(
            archive.SourceProfileArchiveError,
            match="validation differs|validation digest differs|transaction changed|not completed",
        ):
            await archive.activate_retained_result(
                session, prepared=prepared, validation=validation_dict, publication_continuation=continuation
            )
        assert publish_pointer.await_count == (1 if fault == "reused" else 0)
        if fault == "receipt":
            adopt.assert_not_awaited()
    else:
        completion_result = await archive.activate_retained_result(
            session, prepared=prepared, validation=validation_dict, publication_continuation=continuation
        )
        assert (
            completion_result["current_run_id"] == manifest["run_id"]
            and completion_result["previous_run_id"] == "b" * 32
        )
        publish_pointer.assert_awaited_once_with(session, prepared, validation_dict, retained_serving=True)


@pytest.mark.parametrize(
    "contract", [archive.NATIVE_CAPTURE_CONTRACT, archive.NATIVE_PUBLICATION_CONTRACT, archive.VALIDATION_CONTRACT]
)
@pytest.mark.parametrize("changed", [False, True])
async def test_projection_rollback_uses_exact_origin_oid_not_reconstructed_heap(monkeypatch, contract, changed):
    from process import florida_projection_named_source as projection

    manifest = _versioned_manifest(archive.CONTRACT, archive.PROJECTION_IMPORTER)
    ownership = archive.StageOwnership(
        archive.PROJECTION_IMPORTER,
        uuid4(),
        tuple(sorted(archive.source_spec(archive.PROJECTION_IMPORTER).table_names)),
    )
    receipt_by_field = {"contract": contract, "result": manifest, "ownership": archive.ownership_dict(ownership)}
    pointer_by_field = {"current_run_id": "b" * 32, "previous_run_id": manifest["run_id"]}
    validate = AsyncMock(side_effect=archive.SourceProfileArchiveError("retained content differs") if changed else None)
    restore = AsyncMock()
    monkeypatch.setattr(archive, "validate_stage", validate)
    monkeypatch.setattr(projection, "rollback_named_source_projection", restore)
    session = object()
    if changed:
        with pytest.raises(archive.SourceProfileArchiveError, match="content differs"):
            await archive._restore_retained_projection(session, "mrf", receipt_by_field, pointer_by_field, 301)
        restore.assert_not_awaited()
    else:
        await archive._restore_retained_projection(session, "mrf", receipt_by_field, pointer_by_field, 301)
        restore.assert_awaited_once_with(
            session, archive.PreparedResult(manifest, ownership), "mrf", pointer_by_field, None
        )


@pytest.mark.parametrize("importer", [IMPORTER, archive.PROJECTION_IMPORTER])
@pytest.mark.parametrize("changed", [False, True])
async def test_incumbent_capture_seals_only_the_still_current_pointer(monkeypatch, importer, changed):
    from process import florida_projection_named_source as projection

    capture = native_capture(importer)
    serving_by_field = {"schema": "mrf", "table_name": projection.PROJECTION}
    receipt_by_field = {"capture": capture, "serving": serving_by_field}
    pointer = deepcopy(capture["expected"])
    if changed:
        pointer["current_run_id"] = "b" * 32
    monkeypatch.setattr(archive, "_pointer", AsyncMock(return_value=pointer))
    freeze, record = AsyncMock(), AsyncMock()
    monkeypatch.setattr(projection, "freeze_existing_source_projection", freeze)
    monkeypatch.setattr(archive, "_record_native_custody", record)
    session = object()
    if changed:
        with pytest.raises(archive.SourceProfileArchiveError, match="predecessor changed"):
            await archive._seal_captured_incumbent(session, receipt_by_field)
        freeze.assert_not_awaited()
        record.assert_not_awaited()
    else:
        await archive._seal_captured_incumbent(session, receipt_by_field)
        assert freeze.await_count == (importer == archive.PROJECTION_IMPORTER)
        record.assert_awaited_once_with(session, capture, receipt_by_field)


@pytest.mark.parametrize("fault", [None, "authority", "database"])
async def test_maintenance_authority_translates_only_catalog_refusal(monkeypatch, fault):
    error = (
        ValueError("synthetic authority refusal")
        if fault == "authority"
        else RuntimeError("synthetic database failure")
    )
    require = AsyncMock(side_effect=error if fault else None)
    monkeypatch.setattr(archive.pins, "require_worker_authority", require)
    session = object()
    if fault:
        expected = archive.SourceProfileArchiveError if fault == "authority" else RuntimeError
        with pytest.raises(expected, match=str(error)) as caught:
            await archive._require_native_maintenance_authority(session, "mrf", 41)
        if fault == "database":
            assert caught.value is error
    else:
        await archive._require_native_maintenance_authority(session, "mrf", 41)
    require.assert_awaited_once_with(session, "mrf", 41)


@pytest.mark.parametrize("fault", [None, "manifest", "metrics", "database", "owner", "parents", "ancestors"])
async def test_maintenance_source_binds_content_location_and_every_physical_family(monkeypatch, fault):
    handoff = native_handoff(IMPORTER)
    source_run_dict = retained_run(IMPORTER, handoff["source_run_id"])
    handoff["source_manifest_sha256"] = archive._digest(source_run_dict["source_manifest"])
    handoff["source_contract_sha256"] = archive._native_source_contract(handoff, {}, source_run_dict["source_manifest"])
    parents = [[name, archive._reference("mrf", name)] for name in archive.TABLES]
    receipt_dict = {
        "handoff": handoff,
        "sealed_owner_oid": 41,
        "source_result": {**source_run_dict["metrics"], "run_id": source_run_dict["run_id"]},
        "parents": parents,
        "ancestor_relations": [[source_run_dict["run_id"], parents]],
        "result": {"run_ids": [source_run_dict["run_id"]]},
    }
    if fault == "manifest":
        source_run_dict["source_manifest"]["cohort_sha256"] = "e" * 64
    elif fault == "metrics":
        source_run_dict["metrics"] = {"published": True, "unretained": 1}
    elif fault == "parents":
        receipt_dict["parents"] = [[name, archive._reference("other", name)] for name, _ in parents]
    elif fault == "ancestors":
        receipt_dict["ancestor_relations"] = []
    monkeypatch.setattr(archive, "_run", AsyncMock(return_value=source_run_dict))
    monkeypatch.setattr(archive, "_ancestor_relations", AsyncMock(return_value=parents))
    relation = AsyncMock(side_effect=lambda session, schema, name: 11 if name == "import_run" else dict(parents)[name])
    monkeypatch.setattr(archive.native, "_relation_oid", relation)
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[99 if fault == "legacy_database" else 10, 42 if fault == "owner" else 41, 41])
    )
    if fault in {"manifest", "metrics", "parents", "ancestors"}:
        with pytest.raises(archive.SourceProfileArchiveError, match="producer differs|physical source differs"):
            await archive._require_native_maintenance_source(session, {"params": {}}, receipt_dict)
    else:
        await archive._require_native_maintenance_source(session, {"params": {}}, receipt_dict)
        relation.assert_not_awaited()


@pytest.mark.parametrize("incumbent", [None, "b" * 32])
@pytest.mark.parametrize("fault", [None, "pin", "volume"])
async def test_witness_completion_checks_retained_volume_before_stamping_rows(monkeypatch, incumbent, fault):
    from process import new_york_profile_store as producer

    handoff = native_handoff("new-york-nypp-profile")
    handoff["metrics"]["bundle"] = {"reference": "synthetic"}
    handoff["expected"]["current_run_id"] = incumbent
    run_dict = {"run_id": handoff["source_run_id"]}
    reference_dict, counts_dict = (
        {"reference": "synthetic"},
        {"acquired_profiles": 1, "retained_facts": 1, "matched_public_providers": 1},
    )
    monkeypatch.setattr(producer, "read_witness_bundle", AsyncMock(return_value=({"artifact_id": "c" * 64}, {})))
    monkeypatch.setattr(producer, "native_witness_counts", AsyncMock(return_value=counts_dict))
    monkeypatch.setattr(producer, "bundle_reference", Mock(return_value=reference_dict))
    completion = Mock(return_value={key: metric_value for key, metric_value in counts_dict.items()})
    monkeypatch.setattr(type(producer.store), "_completion_metrics", completion)
    previous_counts_dict = {
        "retained_source_records": 10 if fault == "volume" else 1,
        "retained_facts": 1,
        "matched_public_providers": 1,
    }
    monkeypatch.setattr(
        type(producer.store), "_retained_counts_by_run", AsyncMock(return_value={incumbent: previous_counts_dict})
    )
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[fault != "pin", datetime(2026, 1, 2)] if incumbent else [datetime(2026, 1, 2)]),
        execute=AsyncMock(),
    )
    if incumbent and fault:
        with pytest.raises(RuntimeError, match="custody is unavailable|publication_volume_drop"):
            await archive._complete_witnessed_source(session, handoff, "candidate", run_dict)
        session.execute.assert_not_awaited()
        assert session.scalar.await_count == 1
    else:
        completion_result = await archive._complete_witnessed_source(session, handoff, "candidate", run_dict)
        assert completion_result == {
            "acquired_profiles": 1,
            "retained_facts": 1,
            "matched_public_providers": 1,
            "published": True,
            "run_id": run_dict["run_id"],
            "previous_run_id": incumbent,
        }
        assert completion.call_args.args[2]["bundle_reference"] == reference_dict
        stamped = json.loads(session.scalar.await_args.args[1]["metrics"])
        assert stamped["published"] is True and "run_id" not in stamped
        assert session.execute.await_args.args[1] == {"run": run_dict["run_id"], "finished": datetime(2026, 1, 2)}


@pytest.mark.parametrize("bounded", [False, True])
async def test_ordinary_completion_borrows_caller_session_only_after_publication_seal(monkeypatch, bounded):
    handoff = native_handoff(IMPORTER)
    source_run_dict = retained_run(IMPORTER)
    source_run_dict["source_manifest"]["max_providers"] = 1 if bounded else None
    monkeypatch.setattr(archive, "_native_source_run", AsyncMock(return_value=source_run_dict))
    monkeypatch.setattr(archive.native, "_relation_oid", AsyncMock(return_value=301))
    monkeypatch.setattr(archive.native, "protected_publisher_owner", AsyncMock(return_value=41))
    events = []
    completion = SimpleNamespace(
        _finish_source=AsyncMock(side_effect=lambda *args: events.append("finish") or {"published": True}),
        _terminal_progress=Mock(return_value={"pct": 100}),
    )
    monkeypatch.setattr(archive, "_native_completion", lambda importer: completion)
    session = object()

    @asynccontextmanager
    async def borrow(observed):
        assert observed is session and events == []
        events.append("borrow")
        yield observed

    monkeypatch.setattr(archive.models.db, "bind_existing_session", borrow)
    if bounded:
        with pytest.raises(archive.SourceProfileArchiveError, match="bounded publication differs"):
            await archive._finish_native_source(session, handoff)
        completion._finish_source.assert_not_awaited()
        assert events == []
    else:
        assert await archive._finish_native_source(session, handoff) == ({"published": True}, {"pct": 100})
        assert events == ["borrow", "finish"]
        completion._finish_source.assert_awaited_once_with(source_run_dict, handoff["metrics"])


@pytest.mark.parametrize("importer", [IMPORTER, archive.PROJECTION_IMPORTER])
@pytest.mark.parametrize("with_publication", [False, True])
async def test_serving_cutover_finishes_attempt_after_only_its_applicable_pointer_change(
    monkeypatch, importer, with_publication
):
    from process import florida_projection_named_source as projection

    handoff = native_handoff(importer)
    receipt_dict = {"handoff": handoff, "sealed_owner_oid": 41}
    if with_publication:
        receipt_dict["publication"] = {"created_run_ids": [handoff["source_run_id"]]}
    events = []
    publish_projection = AsyncMock(side_effect=lambda *args: events.append("projection"))
    publish_pointer = AsyncMock(side_effect=lambda *args, **kwargs: events.append("pointer"))
    finish = AsyncMock(side_effect=lambda *args: events.append("finish"))
    monkeypatch.setattr(projection, "publish_named_source_projection", publish_projection)
    monkeypatch.setattr(archive, "_publish_result_pointer", publish_pointer)
    monkeypatch.setattr(archive, "_finish_native_attempt", finish)
    session, prepared = object(), object()
    assert await archive._publish_native_serving(session, prepared, receipt_dict) is receipt_dict
    if importer == archive.PROJECTION_IMPORTER:
        assert events == ["projection", "finish"]
        publish_projection.assert_awaited_once_with(
            session, "mrf", handoff["projection"], handoff["expected"], handoff["source_run_id"]
        )
        publish_pointer.assert_not_awaited()
    else:
        assert events == (["pointer", "finish"] if with_publication else ["finish"])
        if with_publication:
            assert publish_pointer.await_args.kwargs == {"retained_serving": True}
        publish_projection.assert_not_awaited()


async def test_native_custody_records_sorted_complete_graph_and_actual_created_subset(monkeypatch):
    producer = native_handoff(IMPORTER)
    receipt_dict = {
        "pin_id": str(uuid4()),
        "result": {"run_ids": ["b" * 32, "a" * 32]},
        "publication": {"created_run_ids": ["a" * 32]},
    }
    record = AsyncMock()
    monkeypatch.setattr(archive.pins, "record_pin", record)
    session = object()
    await archive._record_native_custody(session, producer, receipt_dict)
    calls = record.await_args_list
    assert [call.kwargs["run_id"] for call in calls] == ["a" * 32, "b" * 32]
    assert [call.kwargs["authority"]["created_here"] for call in calls] == [True, False]
    assert all(
        call.kwargs["authority"]["validation"] is receipt_dict
        and call.kwargs["authority"]["run_ids"] is receipt_dict["result"]["run_ids"]
        for call in calls
    )
    assert all(
        call.kwargs["purpose"] == "adoption" and call.kwargs["source_key"] == archive.SOURCES[IMPORTER][0]
        for call in calls
    )


@pytest.mark.parametrize("origin", ["installed", "captured", "published"])
@pytest.mark.parametrize("fault", [None, "pointer", "receipt", "storage"])
async def test_retained_rollback_requires_origin_seal_and_exact_storage_before_pointer(monkeypatch, origin, fault):
    """Refuse changed origins or storage before restoring the publication pointer."""
    manifest = _versioned_manifest(archive.CONTRACT)
    dataset_id, pin_id = uuid4(), uuid4()
    names = archive.source_spec(IMPORTER, publication=True).table_names
    ownership = archive.StageOwnership(IMPORTER, dataset_id, tuple(sorted(names)))
    prepared = archive.PreparedResult(manifest, ownership)
    parents = [[name, archive._reference("mrf", name)] for name in archive.TABLES]
    publication_dict = publication_attachment(manifest, ownership)
    receipt_dict = rollback_receipt(prepared, pin_id, origin, publication_dict)
    pin_records = adoption_pins(manifest, receipt_dict)
    if fault == "receipt":
        receipt_dict["validation_sha256"] = "f" * 64
    pointer_dict = {
        "current_run_id": "c" * 32 if fault == "pointer" else "b" * 32,
        "previous_run_id": manifest["run_id"],
    }
    monkeypatch.setattr(archive.native, "protected_publisher_owner", AsyncMock(return_value=41))
    monkeypatch.setattr(archive, "_source_lock", AsyncMock())
    monkeypatch.setattr(archive, "_pointer", AsyncMock(return_value=pointer_dict))
    monkeypatch.setattr(archive, "require_pin_guards", AsyncMock())
    monkeypatch.setattr(archive, "_pin_group", AsyncMock(return_value=pin_records))
    verify = AsyncMock(side_effect=archive.SourceProfileArchiveError("storage changed") if fault == "storage" else None)
    monkeypatch.setattr(archive, "verify_ownership", verify)
    monkeypatch.setattr(archive, "_require_stage_topology", AsyncMock())
    monkeypatch.setattr(
        archive.native, "_relation_oid", AsyncMock(side_effect=lambda session, schema, name: dict(parents)[name])
    )
    monkeypatch.setattr(archive, "_require_publication_parents", AsyncMock())
    restore = AsyncMock()
    monkeypatch.setattr(archive, "_restore_pointer", restore)
    session = object()
    arguments_dict = dict(
        schema="mrf",
        expected_current_run_id="b" * 32,
        pin_id=pin_id,
        manifest=manifest,
        package_id="e" * 64 if origin == "installed" else None,
        native_receipt=None if origin == "installed" else receipt_dict,
    )
    if fault:
        with pytest.raises(
            archive.SourceProfileArchiveError,
            match="predecessor changed|seal differs|authority changed|storage changed|validation digest differs",
        ):
            await archive.rollback_retained_result(session, **arguments_dict)
        restore.assert_not_awaited()
    else:
        completion_result = await archive.rollback_retained_result(session, **arguments_dict)
        assert completion_result == {
            "current_run_id": manifest["run_id"],
            "previous_run_id": "b" * 32,
            "pin_id": str(pin_id),
        }
        verify.assert_awaited_once_with(session, ownership)
        restore.assert_awaited_once_with(session, "mrf", IMPORTER, "b" * 32, manifest["run_id"])


@pytest.mark.parametrize("importer", [IMPORTER, archive.PROJECTION_IMPORTER])
@pytest.mark.parametrize("with_publication", [False, True])
@pytest.mark.parametrize("fault", [None, "pin", "stage", "serving"])
async def test_maintenance_checks_sealed_storage_and_projection_oid_after_pin_authentication(
    monkeypatch, importer, with_publication, fault
):
    """Authenticate pins before checking storage and the retained serving heap."""
    from process import florida_projection_named_source as projection

    handoff = native_handoff(importer)
    manifest = _versioned_manifest(archive.CONTRACT, importer)
    ownership = archive.StageOwnership(importer, uuid4(), tuple(sorted(archive.source_spec(importer).table_names)))
    parents = [[name, archive._reference("mrf", name)] for name in archive.TABLES]
    receipt_dict = maintenance_receipt(handoff, manifest, ownership, parents, with_publication)
    pin_records = adoption_pins(manifest, receipt_dict, is_created_here="invalid" if fault == "pin" else False)
    control_run_dict = maintenance_control_run(handoff, receipt_dict)
    mapped = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: pin_records))
    session = SimpleNamespace(
        in_transaction=lambda: True, execute=AsyncMock(return_value=mapped), scalar=AsyncMock(return_value=41)
    )
    monkeypatch.setattr(archive, "_source_lock", AsyncMock())
    monkeypatch.setattr(archive, "_require_native_maintenance_source", AsyncMock())
    monkeypatch.setattr(archive, "_require_native_maintenance_authority", AsyncMock())
    verify = AsyncMock(side_effect=RuntimeError("synthetic stage refusal") if fault == "stage" else None)
    monkeypatch.setattr(archive, "verify_ownership", verify)
    monkeypatch.setattr(archive, "_require_publication_parents", AsyncMock())
    monkeypatch.setattr(archive, "_require_stage_topology", AsyncMock())
    guards = AsyncMock()
    monkeypatch.setattr(archive, "require_pin_guards", guards)
    monkeypatch.setattr(
        archive.native, "_relation_oid", AsyncMock(side_effect=lambda session, schema, name: dict(parents)[name])
    )
    monkeypatch.setattr(archive, "_pointer", AsyncMock(return_value={"current_run_id": manifest["run_id"]}))
    retained = AsyncMock(
        side_effect=archive.SourceProfileArchiveError("serving names differ") if fault == "serving" else None
    )
    monkeypatch.setattr(projection, "named_source_projection", retained)
    should_fail = fault in {"pin", "stage"} or fault == "serving" and importer == archive.PROJECTION_IMPORTER
    if should_fail:
        with pytest.raises(RuntimeError, match="authority changed|stage refusal|serving names differ"):
            await archive.require_native_maintenance(session, control_run_dict)
        if fault == "pin":
            verify.assert_not_awaited()
        if fault in {"pin", "stage"}:
            guards.assert_not_awaited()
    else:
        assert await archive.require_native_maintenance(session, control_run_dict) is receipt_dict
        verify.assert_awaited_once_with(session, ownership)
        if importer == archive.PROJECTION_IMPORTER:
            retained.assert_awaited_once_with(session, "mrf", manifest["run_id"])
        else:
            retained.assert_not_awaited()


@pytest.mark.parametrize("fault", [None, "unsupported_fact", "completion"])
async def test_witnessed_integrity_uses_complete_set_and_compact_acquisition_metrics(monkeypatch, fault):
    from process import new_york_profile_store as producer

    run = retained_run("new-york-nypp-profile")
    run["source_manifest"]["bundle_contract"] = producer.WITNESS_CONTRACT
    run["metrics"] = {"published": True, "retained_facts": 1}
    if fault == "completion":
        run["metrics"]["retained_facts"] = 2
    bundle_dict = {"acquisition": {"acquisition_complete": True, "nysed_support": {"synthetic": "not portable"}}}
    reference_dict = {"reference": "synthetic"}
    monkeypatch.setattr(archive, "_run", AsyncMock(return_value=run))
    monkeypatch.setattr(producer, "read_witness_bundle", AsyncMock(return_value=({}, bundle_dict)))
    counts_dict = {"retained_facts": 1}
    monkeypatch.setattr(producer, "native_witness_counts", AsyncMock(return_value=counts_dict))
    monkeypatch.setattr(producer, "bundle_reference", Mock(return_value=reference_dict))
    completion = Mock(return_value={"retained_facts": 1})
    monkeypatch.setattr(type(producer.store), "_completion_metrics", completion)
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[False, fault == "unsupported_fact"]))
    if fault:
        with pytest.raises(archive.SourceProfileArchiveError, match="fact type differs|completion metrics differ"):
            await archive._integrity(session, "candidate", "new-york-nypp-profile", run["run_id"])
    else:
        await archive._integrity(session, "candidate", "new-york-nypp-profile", run["run_id"])
        assert completion.call_args.args == (
            run,
            {"acquisition_complete": True, "bundle": reference_dict},
            {"retained_facts": 1, "bundle_reference": reference_dict},
        )
    assert "LEFT JOIN" in str(session.scalar.await_args_list[0].args[0])
    if fault == "unsupported_fact":
        completion.assert_not_called()


@pytest.mark.parametrize("compact", [False, True])
async def test_witness_handoff_requires_real_bundle_reference_and_compact_metrics(compact):
    from process import new_york_profile_store as producer

    handoff = native_handoff("new-york-nypp-profile")
    run_id = handoff["source_run_id"]
    handoff["metrics"]["bundle"] = producer.bundle_reference(
        {
            "artifact_id": producer._hash([run_id, producer.SOURCE_KEY]),
            "run_id": run_id,
            "content_sha256": "e" * 64,
            "content_bytes": 100,
        }
    )
    if not compact:
        handoff["metrics"]["nysed_support"] = {}
    handoff["handoff_sha256"] = archive._digest(
        {key: metric_value for key, metric_value in handoff.items() if key != "handoff_sha256"}
    )
    if compact:
        assert archive.validate_native_handoff(handoff) is handoff
    else:
        with pytest.raises(archive.SourceProfileArchiveError, match="metrics are not compact"):
            archive.validate_native_handoff(handoff)


@pytest.mark.parametrize("outcome", ["present", "absent", "interrupted", "canceled_read", "failed_read"])
async def test_uncertain_commit_reconciliation_waits_for_fresh_locked_read(monkeypatch, outcome):
    handoff = native_handoff(IMPORTER)
    started, released = asyncio.Event(), asyncio.Event()

    async def execute(statement, *args):
        if str(statement).startswith("SET LOCAL"):
            return None
        assert "FOR SHARE" in str(statement)
        started.set()
        await released.wait()
        if outcome == "canceled_read":
            raise asyncio.CancelledError
        if outcome == "failed_read":
            raise RuntimeError("synthetic read failure")
        return mapped_result(None if outcome == "absent" else {"metrics": {"source_profile_handoff": handoff}})

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute))

    @asynccontextmanager
    async def transaction():
        yield session

    task = asyncio.create_task(archive.reconcile_native_handoff(SimpleNamespace(transaction=transaction), handoff))
    async with asyncio.timeout(2):
        await started.wait()
        if outcome == "interrupted":
            task.cancel()
        released.set()
        if outcome in {"canceled_read", "failed_read"}:
            with pytest.raises(asyncio.CancelledError if outcome == "canceled_read" else RuntimeError):
                await task
        else:
            assert await task is (outcome != "absent")
    assert session.execute.await_count == 2


@pytest.mark.parametrize("required", [False, True])
async def test_ordinary_pointer_writer_requires_protected_authority_after_custody(monkeypatch, required):
    monkeypatch.setattr(archive, "is_native_handoff_required", AsyncMock(return_value=required))
    owner = Mock()
    monkeypatch.setattr(archive.native, "model_family_roles", owner)
    await archive.require_ordinary_publication_authority(object(), "mrf")
    assert owner.call_count == required


@pytest.mark.parametrize("boundary", ["run", "integrity"])
@pytest.mark.parametrize("refused", [False, True])
async def test_projection_validation_preserves_closed_error_boundary(monkeypatch, boundary, refused):
    from process import florida_projection_archive as projection

    error = ValueError("synthetic projection mismatch")
    if boundary == "run":
        validator = Mock(side_effect=error if refused else None)
        monkeypatch.setattr(projection, "validate_native_run", validator)
        invoke = lambda: archive._validate_run(archive.PROJECTION_IMPORTER, {})
        if refused:
            with pytest.raises(
                archive.SourceProfileArchiveError, match="projection publication scope differs"
            ) as caught:
                invoke()
            assert caught.value.__cause__ is error
        else:
            assert invoke() == {}
    else:
        validator = AsyncMock(side_effect=error if refused else None)
        monkeypatch.setattr(projection, "validate_native_result", validator)
        if refused:
            with pytest.raises(
                archive.SourceProfileArchiveError, match="projection evidence closure differs"
            ) as caught:
                await archive._integrity(object(), "mrf", archive.PROJECTION_IMPORTER, "a" * 32)
            assert caught.value.__cause__ is error
        else:
            await archive._integrity(object(), "mrf", archive.PROJECTION_IMPORTER, "a" * 32)


@pytest.mark.parametrize("importer", [IMPORTER, archive.PROJECTION_IMPORTER])
async def test_original_producer_and_capture_branches_share_same_publication_continuation(monkeypatch, importer):
    handoff = native_handoff(importer)
    dataset_id = uuid4()
    ownership = archive.StageOwnership(importer, dataset_id, tuple(sorted(())))
    prepared = archive.PreparedResult(_versioned_manifest(archive.CONTRACT, importer), ownership)
    copy = archive.native.ReferenceFamilySourceCopy(AsyncMock(), 1000, 30)
    for name in ("_source_lock", "require_pin_guards", "_record_native_custody"):
        monkeypatch.setattr(archive, name, AsyncMock())
    monkeypatch.setattr(archive.native, "protected_publisher_owner", AsyncMock(return_value=41))
    monkeypatch.setattr(archive.native, "_lock_family", AsyncMock())
    monkeypatch.setattr(archive, "require_native_handoff", AsyncMock(return_value=handoff))
    monkeypatch.setattr(archive, "_pointer", AsyncMock(return_value=handoff["expected"]))
    finish = AsyncMock(return_value=({"published": True}, {"pct": 100}))
    capture_source = AsyncMock(return_value=prepared)
    monkeypatch.setattr(archive, "_finish_native_source", finish)
    monkeypatch.setattr(archive, "_capture_native_source", capture_source)
    monkeypatch.setattr(
        archive, "_native_receipt_witnesses", AsyncMock(return_value={"publisher_transaction_id": "70"})
    )
    publish = AsyncMock(side_effect=lambda session, prepared, receipt_dict: receipt_dict)
    monkeypatch.setattr(archive, "_publish_native_serving", publish)
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value="70"))

    async def continuation(observed, receipt_dict, cutover):
        assert observed is session and receipt_dict["source_result"] == {"published": True}
        return await cutover()

    receipt_dict = await archive.complete_native_handoff(
        session, handoff, dataset_id=dataset_id, source_copy=copy, publication_continuation=continuation
    )
    finish.assert_awaited_once_with(session, handoff)
    capture_source.assert_awaited_once_with(session, handoff, dataset_id, copy, "synthetic_snapshot_owner")
    assert receipt_dict["validation_sha256"] == archive._digest(
        {key: metric_value for key, metric_value in receipt_dict.items() if key != "validation_sha256"}
    )
    capture = native_capture(importer)
    from process import florida_projection_named_source as projection

    monkeypatch.setattr(
        projection,
        "named_source_projection",
        AsyncMock(return_value={"schema": "mrf", "table_name": projection.PROJECTION}),
    )
    monkeypatch.setattr(archive, "_pointer", AsyncMock(return_value=capture["expected"]))
    monkeypatch.setattr(archive, "_require_captured_source", AsyncMock(return_value={"source_manifest": {}}))
    monkeypatch.setattr(archive, "_seal_captured_incumbent", AsyncMock())

    async def capture_continuation(observed, receipt_dict, seal):
        await seal()
        return receipt_dict

    receipt_dict = await archive.capture_native_incumbent(
        session, capture, dataset_id=dataset_id, source_copy=copy, publication_continuation=capture_continuation
    )
    assert receipt_dict["serving"] == (
        {"schema": "mrf", "table_name": "provider_profile_projection"}
        if importer == archive.PROJECTION_IMPORTER
        else None
    )


async def test_projection_producer_completion_uses_projection_boundary_after_pointer_seal(monkeypatch):
    handoff = native_handoff(archive.PROJECTION_IMPORTER)
    monkeypatch.setattr(archive, "_native_source_run", AsyncMock(return_value={}))
    events = []
    finish = AsyncMock(side_effect=lambda *args: events.append("projection") or ({"published": True}, {"pct": 100}))
    monkeypatch.setattr(archive, "_finish_native_projection", finish)
    session = object()
    assert await archive._finish_native_source(session, handoff) == ({"published": True}, {"pct": 100})
    assert events == ["projection"]
    finish.assert_awaited_once_with(session, handoff)


@pytest.mark.parametrize("importer", [IMPORTER, archive.PROJECTION_IMPORTER])
@pytest.mark.parametrize("retained", [False, True])
async def test_result_pointer_uses_only_source_scoped_sql_or_exact_projection_cutover(monkeypatch, importer, retained):
    from process import florida_projection_named_source as projection

    prepared = SimpleNamespace(manifest=_versioned_manifest(archive.CONTRACT, importer))
    cutover_by_field = {"schema": "mrf", "table_name": "synthetic_candidate"}
    expected_by_field = {"current_run_id": "b" * 32, "previous_run_id": None}
    validation_by_field = {
        "destination_schema": "mrf",
        "expected_current_run_id": "b" * 32,
        "projection": {"cutover": cutover_by_field, "expected": expected_by_field},
    }
    order, publish = AsyncMock(), AsyncMock()
    monkeypatch.setattr(projection.legacy, "require_native_publication_order", order)
    monkeypatch.setattr(projection, "publish_named_source_projection", publish)
    session = SimpleNamespace(execute=AsyncMock())
    await archive._publish_result_pointer(session, prepared, validation_by_field, retained_serving=retained)
    if importer == archive.PROJECTION_IMPORTER:
        order.assert_awaited_once_with(session, prepared, "mrf", expected_by_field)
        publish.assert_awaited_once_with(
            session, "mrf", cutover_by_field, expected_by_field, prepared.manifest["run_id"]
        )
        session.execute.assert_not_awaited()
    else:
        order.assert_not_awaited()
        publish.assert_not_awaited()
        query, parameters = session.execute.await_args.args
        assert 'INSERT INTO "mrf"."provider_profile_source_publication"' in str(
            query
        ) and "ON CONFLICT (source_key)" in str(query)
        assert parameters == {
            "source_key": prepared.manifest["source_key"],
            "run_id": prepared.manifest["run_id"],
            "previous": "b" * 32,
        }


async def test_activation_wrapper_forwards_same_fence_and_explicit_copy_request(monkeypatch):
    prepare = AsyncMock(return_value={"validation": "synthetic"})
    activate = AsyncMock(return_value={"current_run_id": "a" * 32})
    monkeypatch.setattr(archive, "prepare_activation", prepare)
    monkeypatch.setattr(archive, "activate_validated_result", activate)
    session = object()
    arguments_dict = dict(
        prepared=object(),
        destination_schema="mrf",
        expected_current_run_id="b" * 32,
        package_id="e" * 64,
        sealed_owner_oid=41,
        pin_id=uuid4(),
    )
    request_dict = {"source_copy": archive.native.ReferenceFamilySourceCopy(AsyncMock(), 1000, 30)}
    assert await archive.activate_result(session, **arguments_dict, publication_request=request_dict) == {
        "current_run_id": "a" * 32
    }
    prepare.assert_awaited_once_with(
        session,
        **{key: value for key, value in arguments_dict.items() if key != "sealed_owner_oid"},
        publication_request=request_dict,
    )
    activate.assert_awaited_once_with(session, **arguments_dict, validation={"validation": "synthetic"})


async def test_retained_projection_rollback_authenticates_capture_and_publishes_original_oid(monkeypatch):
    from process import florida_projection_named_source as projection

    importer = archive.PROJECTION_IMPORTER
    manifest = _versioned_manifest(archive.CONTRACT, importer)
    ownership = archive.StageOwnership(importer, uuid4(), tuple(sorted(archive.source_spec(importer).table_names)))
    pin_id = uuid4()
    receipt_dict = {
        "contract": archive.NATIVE_CAPTURE_CONTRACT,
        "pin_id": str(pin_id),
        "result": manifest,
        "sealed_owner_oid": 41,
        "ownership": archive.ownership_dict(ownership),
        "serving": {"relation_oid": 301},
    }
    receipt_dict["validation_sha256"] = archive._digest(receipt_dict)
    pin_records = adoption_pins(manifest, receipt_dict)
    pointer_dict = {"current_run_id": "b" * 32, "previous_run_id": manifest["run_id"], "previous_relation_oid": 301}
    monkeypatch.setattr(archive.native, "protected_publisher_owner", AsyncMock(return_value=41))
    for name in ("_source_lock", "require_pin_guards", "verify_ownership", "_require_stage_topology"):
        monkeypatch.setattr(archive, name, AsyncMock())
    monkeypatch.setattr(archive, "_pointer", AsyncMock(return_value=pointer_dict))
    monkeypatch.setattr(archive, "_pin_group", AsyncMock(return_value=pin_records))
    publish = AsyncMock()
    monkeypatch.setattr(projection, "rollback_named_source_projection", publish)
    monkeypatch.setattr(archive, "validate_stage", AsyncMock())
    session = object()
    completion_result = await archive.rollback_retained_result(
        session,
        schema="mrf",
        expected_current_run_id="b" * 32,
        pin_id=pin_id,
        manifest=manifest,
        package_id=None,
        native_receipt=receipt_dict,
        serving_relation_oid=301,
    )
    assert completion_result["current_run_id"] == manifest["run_id"]
    publish.assert_awaited_once_with(session, archive.PreparedResult(manifest, ownership), "mrf", pointer_dict, None)
