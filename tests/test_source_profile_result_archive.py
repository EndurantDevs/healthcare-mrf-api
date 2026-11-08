# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only completed self-contained assertion graphs cross the archive boundary."""

import asyncio
import hashlib
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from process import source_profile_result_archive as archive
from tests.source_profile_archive_support import retained_run


def maintenance_run(importer):
    handoff = native_handoff(importer)
    receipt_by_field = {
        "handoff": handoff,
        "source_result": {"source_records": 1},
        "terminal_progress": {"phase": "published", "message": "succeeded"},
    }
    return {
        "run_id": handoff["run_id"],
        "importer": importer,
        "node_id": handoff["node_id"],
        "engine": "healthcare-mrf-api",
        "status": "finalizing",
        "error": None,
        "finished_at": None,
        "phase_detail": archive.NATIVE_MAINTENANCE_PHASE,
        "progress": {key: handoff[key] for key in ("attempt_id", "attempt_started_at")},
        "metrics": {
            "source_records": 1,
            "source_profile_handoff": handoff,
            "source_profile_native_publication": receipt_by_field,
        },
    }


def maintenance_database(run, *, uncertain_commit=False):
    """Model the control transaction only; native tests supply the actual custody proof."""
    updates = []

    @asynccontextmanager
    async def transaction():
        yield SimpleNamespace(execute=AsyncMock())
        if uncertain_commit and len(updates) == 1:
            updates.append("uncertain commit")
            raise RuntimeError("commit outcome unknown")

    async def first(statement):
        if str(statement).startswith("UPDATE"):
            parameters_by_name = statement.compile().params
            assert run["status"] == "finalizing"
            updates.append(parameters_by_name)
            for key in ("status", "phase_detail", "metrics", "progress"):
                run[key] = parameters_by_name[key]
            run["finished_at"] = datetime(2026, 1, 2)
        return SimpleNamespace(_mapping=run, **run)

    return SimpleNamespace(transaction=transaction, first=first), updates


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", tuple(archive.SOURCES))
@pytest.mark.parametrize("uncertain_commit", [False, True])
async def test_actual_finish_dispatch_completes_and_replays_only_after_maintenance(
    monkeypatch, importer, uncertain_commit
):
    import process
    from api import control_workers
    from process import provider_profile_source_completion as completion
    from process.ext import utils

    run = maintenance_run(importer)
    database, updates = maintenance_database(run, uncertain_commit=uncertain_commit)
    receipt = run["metrics"]["source_profile_native_publication"]

    async def maintain(_receipt):
        assert _receipt == receipt and run["status"] == "finalizing" and updates == []
        return {"status": "completed"}

    maintenance = AsyncMock(side_effect=maintain)
    monkeypatch.setenv("HLTHPRT_CONTROL_RUN_ID", run["run_id"])
    monkeypatch.setattr(completion, "db", database)
    monkeypatch.setattr(utils, "db_startup", AsyncMock())
    monkeypatch.setattr(archive, "require_native_maintenance", AsyncMock(return_value=receipt))
    monkeypatch.setattr(completion, "_native_source_maintenance", maintenance)
    spec = control_workers._resolve_specs({"importer": importer, "status": "finalizing"})[0]
    assert spec.role == "finish" and spec.worker_class == "process.SourceProfile_finish"
    worker = getattr(process, spec.worker_class.split(".")[1])
    maintenance_by_field = await worker.on_startup({})
    assert run["status"] == "succeeded" and maintenance_by_field["source_records"] == 1
    assert maintenance_by_field["source_profile_maintenance"] == {
        "handoff_sha256": receipt["handoff"]["handoff_sha256"],
        "outcome": {"status": "completed"},
    }
    assert await worker.on_startup({}) == maintenance_by_field
    maintenance.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["unpublished", "canceled", "wrong_run", "maintenance_cancel"])
async def test_actual_finish_dispatch_never_terminalizes_unfinished_work(monkeypatch, boundary):
    import process
    from process import provider_profile_source_completion as completion
    from process.ext import utils

    run = maintenance_run(archive.PROJECTION_IMPORTER)
    database, updates = maintenance_database(run)
    receipt = run["metrics"]["source_profile_native_publication"]
    proof = AsyncMock(return_value=receipt)
    if boundary in {"unpublished", "canceled"}:
        proof.side_effect = archive.SourceProfileArchiveError("publication is unavailable")
    cleanup = AsyncMock(side_effect=asyncio.CancelledError if boundary == "maintenance_cancel" else None)
    monkeypatch.setenv("HLTHPRT_CONTROL_RUN_ID", run["run_id"])
    monkeypatch.setattr(completion, "db", database)
    monkeypatch.setattr(utils, "db_startup", AsyncMock())
    monkeypatch.setattr(archive, "require_native_maintenance", proof)
    monkeypatch.setattr(completion, "_native_source_maintenance", cleanup)
    expected = asyncio.CancelledError if boundary == "maintenance_cancel" else archive.SourceProfileArchiveError
    with pytest.raises(expected):
        if boundary == "wrong_run":
            await process.SourceProfile_finish.functions[0]({}, "another_run")
        else:
            await process.SourceProfile_finish.on_startup({})
    assert updates == [] and run["status"] == "finalizing"
    assert cleanup.await_count == (boundary == "maintenance_cancel")


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", tuple(archive.SOURCES))
@pytest.mark.parametrize("failed", [False, True])
async def test_finish_dispatch_uses_each_existing_local_retention_policy(monkeypatch, tmp_path, importer, failed):
    import process
    from process import provider_profile_source_completion as completion
    from process.ext import utils

    run = maintenance_run(importer)
    run["source_manifest"] = {"retention": {"failed_run_days": 3}}
    database, updates = maintenance_database(run)
    receipt = run["metrics"]["source_profile_native_publication"]
    outcome_by_field = {"status": "failed" if failed else "completed"}
    florida_retention = AsyncMock(return_value={"retention": outcome_by_field})
    ordinary_retention = AsyncMock(
        return_value=outcome_by_field, side_effect=OSError("synthetic maintenance failure") if failed else None
    )
    worker_module = SimpleNamespace(_artifact_root=lambda: tmp_path, _apply_post_success_retention=florida_retention)
    monkeypatch.setenv("HLTHPRT_CONTROL_RUN_ID", run["run_id"])
    monkeypatch.setattr(completion, "db", database)
    monkeypatch.setattr(utils, "db_startup", AsyncMock())
    monkeypatch.setattr(completion, "import_module", lambda _name: worker_module)
    monkeypatch.setattr(archive, "require_native_maintenance", AsyncMock(return_value=receipt))
    monkeypatch.setattr(
        archive,
        "_native_completion",
        lambda _importer: SimpleNamespace(store=SimpleNamespace(retain_source_history=ordinary_retention)),
    )
    maintenance_by_field = await process.SourceProfile_finish.on_startup({})
    assert run["status"] == "succeeded" and len(updates) == 1
    assert maintenance_by_field["source_profile_maintenance"]["outcome"]["status"] == outcome_by_field["status"]
    assert maintenance_by_field["source_records"] == receipt["source_result"]["source_records"] == 1
    if importer == archive.PROJECTION_IMPORTER:
        florida_retention.assert_awaited_once_with(
            run_id=receipt["handoff"]["source_run_id"], metrics={}, artifact_root=tmp_path, failed_retention_days=3
        )
        ordinary_retention.assert_not_awaited()
    else:
        ordinary_retention.assert_awaited_once_with(tmp_path)
        florida_retention.assert_not_awaited()


@pytest.mark.asyncio
async def test_published_pending_cancel_authenticates_without_undoing_publication(monkeypatch):
    from api import control_imports

    run = maintenance_run(archive.PROJECTION_IMPORTER)

    @asynccontextmanager
    async def transaction():
        yield "locked"

    database = SimpleNamespace(
        transaction=transaction, execute=AsyncMock(), first=AsyncMock(return_value=SimpleNamespace(_mapping=run))
    )
    proof = AsyncMock()
    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(archive, "require_native_maintenance", proof)
    normalized = await control_imports._request_source_profile_cancel(run["run_id"])
    assert normalized["status"] == "finalizing" and normalized["phase_detail"] == archive.NATIVE_MAINTENANCE_PHASE
    proof.assert_awaited_once_with("locked", run)
    database.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["attempt", "missing_pin", "changed_pin", "authority"])
async def test_native_maintenance_refuses_control_or_pin_substitution(monkeypatch, changed):
    manifest = _versioned_manifest(archive.CONTRACT)
    run = maintenance_run(manifest["importer_id"])
    receipt = run["metrics"]["source_profile_native_publication"]
    receipt.update(
        contract=archive.NATIVE_PUBLICATION_CONTRACT, result=manifest, pin_id=str(uuid4()), sealed_owner_oid=41
    )
    receipt["validation_sha256"] = archive._digest(receipt)
    if changed == "attempt":
        run["progress"]["attempt_id"] = "another_attempt"
    pin_rows = (
        []
        if changed == "missing_pin"
        else [
            {
                "run_id": manifest["run_id"],
                "source_key": manifest["source_key"],
                "purpose": "adoption",
                "authority_json": {
                    "root_run_id": manifest["run_id"],
                    "run_ids": manifest["run_ids"],
                    "created_here": False,
                    "validation": {"different": "receipt"},
                },
            }
        ]
    )
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: pin_rows))),
    )
    monkeypatch.setattr(archive, "_source_lock", AsyncMock())
    monkeypatch.setattr(archive, "_require_native_maintenance_source", AsyncMock())
    authority = AsyncMock(side_effect=ValueError("pin policy differs") if changed == "authority" else None)
    monkeypatch.setattr(archive.pins, "require_worker_authority", authority)
    with pytest.raises(
        archive.SourceProfileArchiveError, match="maintenance attempt|adoption authority|pin policy differs"
    ):
        await archive.require_native_maintenance(session, run)
    if changed in {"attempt", "authority"}:
        session.execute.assert_not_awaited()
    else:
        query = str(session.execute.await_args.args[0])
        assert "LIMIT 65" in query and "FOR UPDATE" not in query
    assert authority.await_count == (changed != "attempt")


@pytest.mark.parametrize(
    "phase,status,finished,accepted",
    [
        (archive.NATIVE_MAINTENANCE_PHASE, "finalizing", None, True),
        (archive.NATIVE_HANDOFF_PHASE, "finalizing", None, False),
        ("arbitrary", "finalizing", None, False),
        (archive.NATIVE_MAINTENANCE_PHASE, "running", None, False),
        (archive.NATIVE_MAINTENANCE_PHASE, "canceling", None, False),
        ("published", "succeeded", datetime(2026, 1, 2), True),
    ],
)
def test_native_pending_phase_is_not_arbitrary_finalizing(phase, status, finished, accepted):
    assert (
        archive.is_native_published_attempt(
            {"phase_detail": phase, "status": status, "finished_at": finished, "error": None}
        )
        is accepted
    )


def native_handoff(importer):
    """A synthetic closed managed attempt, never a substitute for native source proof."""
    fields_by_name = {
        "contract": archive.NATIVE_HANDOFF_CONTRACT,
        "importer_id": importer,
        "run_id": "synthetic_control",
        "source_run_id": "a" * 32,
        "attempt_id": "synthetic_attempt",
        "attempt_started_at": "2026-01-02T00:00:00+00:00",
        "schema_name": "mrf",
        "node_id": "synthetic_node",
        "database_oid": 10,
        "import_run_oid": 11,
        "source_contract_sha256": "c" * 64,
        "source_manifest_sha256": "d" * 64,
        "expected": {"current_run_id": None, "previous_run_id": None},
        "metrics": {"source_records": 1},
        "projection": None,
    }
    if importer == archive.PROJECTION_IMPORTER:
        fields_by_name["projection"] = {
            "relation_oid": 12,
            "owner_oid": 13,
            "table_name": "provider_profile_projection_" + "a" * 16,
            "row_count": 1,
        }
        fields_by_name["expected"].update(current_relation_oid=14, previous_relation_oid=None)
    fields_by_name["handoff_sha256"] = archive._digest(fields_by_name)
    return fields_by_name


@pytest.mark.parametrize("importer", tuple(archive.SOURCES))
def test_native_handoff_binds_every_producer_and_rejects_unsealed_changes(importer):
    handoff = native_handoff(importer)
    assert archive.validate_native_handoff(handoff) == handoff
    for field, replacement in (
        ("attempt_id", "other"),
        ("database_oid", 0),
        ("import_run_oid", True),
        ("source_run_id", "wrong"),
        ("source_contract_sha256", "e" * 64),
        ("peer", "extra"),
    ):
        with pytest.raises(archive.SourceProfileArchiveError):
            archive.validate_native_handoff({**handoff, field: replacement})


def native_capture(importer):
    """First-use selection names existing source data, not a historical control attempt."""
    expected_by_field = native_handoff(importer)["expected"]
    expected_by_field["current_run_id"] = "a" * 32
    return {
        "contract": archive.NATIVE_CAPTURE_CONTRACT,
        "importer_id": importer,
        "schema_name": "mrf",
        "source_run_id": "a" * 32,
        "expected": expected_by_field,
        "database_oid": 10,
        "source_manifest_sha256": "d" * 64,
    }


@pytest.mark.parametrize("importer", tuple(archive.SOURCES))
def test_first_use_capture_is_closed_and_requires_actual_predecessor(importer):
    capture = native_capture(importer)
    assert archive.validate_native_capture(capture) == capture
    for field, replacement in (
        ("run_id", "invented_attempt"),
        ("database_oid", True),
        ("source_run_id", "b" * 32),
        ("source_manifest_sha256", "bad"),
        ("expected", {"current_run_id": "a" * 32}),
    ):
        with pytest.raises(archive.SourceProfileArchiveError):
            archive.validate_native_capture({**capture, field: replacement})


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["success", "missing", "changed_transaction", "reused"])
async def test_first_use_capture_requires_exact_single_transaction_continuation(monkeypatch, boundary):
    importer = next(iter(archive.SOURCES))
    capture = native_capture(importer)
    run = retained_run(importer)
    capture["source_manifest_sha256"] = archive._digest(run["source_manifest"])
    session = SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock(side_effect=[10, "70", "71" if boundary == "changed_transaction" else "70"]),
    )
    prepared = SimpleNamespace(manifest={"run_ids": [capture["source_run_id"]]}, ownership=object())
    for name in ("_source_lock", "require_pin_guards"):
        monkeypatch.setattr(archive, name, AsyncMock())
    for name, field_value in (("protected_publisher_owner", 22), ("_lock_family", None), ("_relation_oid", 30)):
        monkeypatch.setattr(archive.native, name, AsyncMock(return_value=field_value))
    for name, field_value in (
        ("_pointer", capture["expected"]),
        ("_run", run),
        ("_capture_native_source", prepared),
        ("_ancestor_relations", []),
    ):
        monkeypatch.setattr(archive, name, AsyncMock(return_value=field_value))
    monkeypatch.setattr(archive, "ownership_dict", lambda ownership: {"synthetic": True})
    seal = AsyncMock()
    monkeypatch.setattr(archive, "_seal_captured_incumbent", seal)

    async def continuation(actual_session, receipt, publish):
        """Exercise the exact callback contract, without pretending to prove PostgreSQL custody."""
        assert actual_session is session
        if boundary != "missing":
            await publish()
        if boundary == "reused":
            await publish()
        return receipt

    pending = archive.capture_native_incumbent(
        session,
        capture,
        dataset_id=uuid4(),
        source_copy=archive.native.ReferenceFamilySourceCopy(archive.native.native_copy_projection, 1024, 300),
        publication_continuation=continuation,
    )
    if boundary == "success":
        receipt = await pending
        assert receipt["capture"] == capture and "handoff" not in receipt
        assert receipt["validation_sha256"] == archive._digest(
            {key: field_value for key, field_value in receipt.items() if key != "validation_sha256"}
        )
    else:
        with pytest.raises(archive.SourceProfileArchiveError, match="transaction changed|not published"):
            await pending
    assert seal.await_count == (boundary in {"success", "reused"})


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["committed", "rolled_back", "unknown", "before_handoff"])
async def test_native_handoff_uncertain_commit_never_releases_committed_custody(monkeypatch, outcome):
    monkeypatch.setattr(archive.pins, "load_role_policy", Mock())
    handoff = native_handoff(archive.PROJECTION_IMPORTER)
    context_by_name = {"context": {}}

    @asynccontextmanager
    async def transaction():
        yield object()
        raise RuntimeError("commit reply lost")

    record_handoff = AsyncMock(
        return_value=handoff, side_effect=RuntimeError("record failed") if outcome == "before_handoff" else None
    )
    reconcile = AsyncMock(
        return_value=outcome == "committed",
        side_effect=RuntimeError("readback failed") if outcome == "unknown" else None,
    )
    monkeypatch.setattr(archive, "record_native_handoff", record_handoff)
    monkeypatch.setattr(archive, "reconcile_native_handoff", reconcile)
    pending = archive.handoff_native_publication(
        SimpleNamespace(transaction=transaction), context_by_name, metrics={"source_records": 1}
    )
    if outcome == "committed":
        handoff_result = await pending
        assert handoff_result == {"source_records": 1, "source_profile_handoff": handoff}
        assert context_by_name["context"]["control_run_handoff_committed"] is True
    else:
        with pytest.raises(RuntimeError):
            await pending
        assert "control_run_handoff_committed" not in context_by_name["context"]
    assert context_by_name["context"].get("source_profile_commit_unknown", False) == (outcome == "unknown")
    assert reconcile.await_count == (outcome != "before_handoff")


@pytest.mark.asyncio
async def test_native_handoff_refuses_missing_policy_before_any_transaction(monkeypatch):
    monkeypatch.delenv(archive.pins.ROLE_POLICY_ENVIRONMENT, raising=False)
    database = SimpleNamespace(transaction=Mock())
    with pytest.raises(ValueError, match="role policy is unavailable"):
        await archive.handoff_native_publication(database, {}, metrics={})
    database.transaction.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", tuple(name for name in archive.SOURCES if name != archive.PROJECTION_IMPORTER))
async def test_all_five_ordinary_completions_use_the_real_shared_handoff(monkeypatch, importer):
    from process import provider_profile_source_completion as completion

    actual = archive._native_completion(importer)
    assert actual.importer == importer and actual.store.policy.source_key == archive.SOURCES[importer][0]

    @asynccontextmanager
    async def transaction():
        yield object()

    handoff = native_handoff(importer)
    job_context_by_field = {
        "context": {
            "control_run_id": handoff["run_id"],
            "_control_attempt_id": handoff["attempt_id"],
            "_control_attempt_started_at": handoff["attempt_started_at"],
        }
    }
    run = retained_run(importer)
    run["source_manifest"]["control_run_id"] = handoff["run_id"]
    dispatch = AsyncMock(return_value={"source_profile_handoff": handoff})
    monkeypatch.setattr(completion, "db", SimpleNamespace(transaction=transaction))
    monkeypatch.setattr(completion, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(archive, "is_native_handoff_required", AsyncMock(return_value=True))
    monkeypatch.setattr(archive, "handoff_native_publication", dispatch)
    handoff_result = await actual.complete_run(
        job_context_by_field, {"run_id": handoff["run_id"]}, run, {"requested_licenses": 1}
    )
    assert handoff_result == dispatch.return_value
    assert dispatch.call_args.kwargs == {
        "importer_id": importer,
        "schema": "mrf",
        "source_run_id": run["run_id"],
        "metrics": {"requested_licenses": 1},
    }


async def test_new_york_witness_requires_publisher_even_before_first_protected_generation(monkeypatch):
    from process import new_york_profile_store as producer
    from process import provider_profile_source_completion as completion

    @asynccontextmanager
    async def transaction():
        yield object()

    run = retained_run(producer.IMPORTER)
    run["source_manifest"]["bundle_contract"] = producer.WITNESS_CONTRACT
    context_by_field = {"context": {"_control_attempt_id": "attempt", "_control_attempt_started_at": "started"}}
    dispatch = AsyncMock(return_value={"handoff": "recorded"})
    monkeypatch.setattr(completion, "db", SimpleNamespace(transaction=transaction))
    monkeypatch.setattr(completion, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(archive, "is_native_handoff_required", AsyncMock(return_value=False))
    monkeypatch.setattr(archive, "handoff_native_publication", dispatch)
    assert (
        await producer.completion.complete_run(
            context_by_field, {"run_id": run["source_manifest"]["control_run_id"]}, run, {"bundle": "bounded reference"}
        )
        == dispatch.return_value
    )
    assert dispatch.await_args.kwargs["importer_id"] == producer.IMPORTER


@pytest.mark.parametrize("is_witnessed", [False, True])
@pytest.mark.parametrize("contract", [archive.LEGACY_CONTRACT, archive.CONTRACT])
async def test_witness_cannot_enter_legacy_adoption_without_changing_sealed_v1(monkeypatch, is_witnessed, contract):
    from process import new_york_profile_store as producer

    run = retained_run(producer.IMPORTER)
    if is_witnessed:
        run["source_manifest"]["bundle_contract"] = producer.WITNESS_CONTRACT
    monkeypatch.setattr(archive, "_lineage", AsyncMock(return_value=[run]))
    monkeypatch.setattr(archive, "_integrity", AsyncMock())
    monkeypatch.setattr(archive, "_table_receipts", AsyncMock(return_value=[]))
    pending = archive.describe_result(
        object(), importer_id=producer.IMPORTER, schema="isolated_candidate", run_id=run["run_id"], contract=contract
    )
    if is_witnessed and contract == archive.LEGACY_CONTRACT:
        with pytest.raises(archive.SourceProfileArchiveError, match="witnessed result requires native v2"):
            await pending
        archive._integrity.assert_not_awaited()
        archive._table_receipts.assert_not_awaited()
    else:
        assert (await pending)["contract"] == contract
        archive._integrity.assert_awaited_once()


@pytest.mark.parametrize("failure", [None, "payload", "parent", "pin", "delete"])
async def test_local_envelope_replacement_requires_exact_unpublished_parent_custody(monkeypatch, failure):
    handoff = native_handoff("new-york-nypp-profile")
    witnesses = [failure == "payload", False, failure != "parent", True, failure == "pin"]
    removed = [] if failure == "delete" else [handoff["source_run_id"]]
    result = SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: removed))
    session = SimpleNamespace(scalar=AsyncMock(side_effect=witnesses), execute=AsyncMock(return_value=result))
    source = AsyncMock()
    monkeypatch.setattr(archive, "_native_source_run", source)
    if failure:
        with pytest.raises(archive.SourceProfileArchiveError):
            await archive._remove_local_source_envelope(session, handoff)
    else:
        await archive._remove_local_source_envelope(session, handoff)
        assert [str(call.args[0]).split('"mrf".')[1].split()[0] for call in session.execute.await_args_list] == [
            '"provider_profile_artifact"',
            '"provider_profile_import_run"',
        ]
    assert all("DELETE FROM ONLY" in str(call.args[0]) for call in session.execute.await_args_list)
    source.assert_awaited_once_with(session, handoff)
    if failure in {"payload", "parent", "pin"}:
        session.execute.assert_not_awaited()


@pytest.mark.parametrize("failure", [None, "overflow", "cancel"])
async def test_witness_copy_keeps_one_byte_and_deadline_budget_without_insert(monkeypatch, failure):
    handoff = native_handoff("new-york-nypp-profile")
    source_copy = archive.native.ReferenceFamilySourceCopy(AsyncMock(), 1000, 300)
    ownership = SimpleNamespace(schema_name="isolated_candidate")
    envelope_copy = AsyncMock(return_value=800)
    payload_copy = AsyncMock(side_effect=[600, 400])
    if failure == "overflow":
        payload_copy.side_effect = archive.native.ReferenceFamilyArchiveError("byte cap exceeded")
    elif failure == "cancel":
        payload_copy.side_effect = asyncio.CancelledError
    monkeypatch.setattr(archive.native, "_copy_model_run_scope", envelope_copy)
    monkeypatch.setattr(archive.native, "_copy_source_projection", payload_copy)
    session = object()
    pending = archive._load_witnessed_source(session, handoff, ownership, source_copy, 123.0)
    if failure:
        with pytest.raises(
            asyncio.CancelledError if failure == "cancel" else archive.native.ReferenceFamilyArchiveError
        ):
            await pending
    else:
        assert await pending == 400
        assert [call.args[-2] for call in payload_copy.await_args_list] == [800, 600]
    assert envelope_copy.await_args.kwargs["deadline"] == 123.0
    assert all(call.args[-1] == 123.0 for call in payload_copy.await_args_list)
    assert all("json_populate_record" in call.args[2] for call in payload_copy.await_args_list)


@pytest.mark.parametrize("failure", [None, "validation", "cancel", "continuation"])
async def test_witness_native_publication_never_falls_back_to_ordinary_payload_completion(monkeypatch, failure):
    """Only verified publication completes the exact no-error control attempt; failed work never finishes."""
    importer = "new-york-nypp-profile"
    handoff = native_handoff(importer)
    handoff["metrics"]["bundle"] = {"synthetic": "bounded reference"}

    async def scalar(statement, *_parameters):
        return handoff["run_id"] if str(statement).startswith("UPDATE") else "70"

    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(side_effect=scalar))
    prepared = SimpleNamespace(manifest={"run_ids": [handoff["source_run_id"]]}, ownership=object())
    publication_by_field = {"created_run_ids": [handoff["source_run_id"]]}
    result_by_field = {"requested_licenses": 1, "published": True}
    witness = AsyncMock(return_value=(prepared, result_by_field, publication_by_field))
    if failure in {"validation", "cancel"}:
        witness.side_effect = asyncio.CancelledError if failure == "cancel" else ValueError("candidate invalid")
    for name in ("_source_lock", "require_pin_guards", "_record_native_custody", "_publish_result_pointer"):
        monkeypatch.setattr(archive, name, AsyncMock())
    monkeypatch.setattr(archive, "_finish_native_attempt", AsyncMock(wraps=archive._finish_native_attempt))
    for name, field_value in (("protected_publisher_owner", 22), ("_lock_family", None)):
        monkeypatch.setattr(archive.native, name, AsyncMock(return_value=field_value))
    monkeypatch.setattr(archive, "require_native_handoff", AsyncMock(return_value=handoff))
    monkeypatch.setattr(archive, "_pointer", AsyncMock(return_value=handoff["expected"]))
    monkeypatch.setattr(archive, "_native_receipt_witnesses", AsyncMock(return_value={}))
    monkeypatch.setattr(archive, "ownership_dict", lambda ownership: {})
    monkeypatch.setattr(archive, "_prepare_witnessed_source", witness)
    old_completion = AsyncMock(side_effect=AssertionError("ordinary indexed writer reached"))
    monkeypatch.setattr(archive, "_finish_native_source", old_completion)

    async def continuation(actual_session, receipt, cutover):
        assert actual_session is session
        if failure == "continuation":
            raise RuntimeError("register failed")
        await cutover()
        return receipt

    pending = archive.complete_native_handoff(
        session,
        handoff,
        dataset_id=uuid4(),
        source_copy=archive.native.ReferenceFamilySourceCopy(AsyncMock(), 1024, 300),
        publication_continuation=continuation,
    )
    if failure:
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else (ValueError, RuntimeError)):
            await pending
        archive._finish_native_attempt.assert_not_awaited()
    else:
        receipt = await pending
        assert receipt["publication"] == publication_by_field and receipt["destination_schema"] == "mrf"
        archive._publish_result_pointer.assert_awaited_once()
        archive._finish_native_attempt.assert_awaited_once()
        statement, parameters = session.scalar.await_args.args
        assert "AND (error IS NULL OR error::jsonb='null'::jsonb) " in str(statement)
        assert parameters["handoff_phase"] == archive.NATIVE_HANDOFF_PHASE
        assert parameters["attempt_id"] == handoff["attempt_id"]
    old_completion.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", tuple(archive.SOURCES))
@pytest.mark.parametrize("change", [None, "node_id", "engine", "attempt_id"])
async def test_source_cancel_preserves_exact_publisher_handoff(monkeypatch, importer, change):
    """Cancellation changes only control intent, and refuses a different producer or attempt."""
    from api import control_imports

    handoff = native_handoff(importer)
    control_by_field = {
        "run_id": handoff["run_id"],
        "importer": importer,
        "node_id": handoff["node_id"],
        "engine": "healthcare-mrf-api",
        "status": "finalizing",
        "metrics": {"source_profile_handoff": handoff},
        "progress": {key: handoff[key] for key in ("attempt_id", "attempt_started_at")},
    }
    if change == "attempt_id":
        control_by_field["progress"][change] = "changed"
    elif change is not None:
        control_by_field[change] = "changed"

    @asynccontextmanager
    async def transaction():
        yield

    database = SimpleNamespace(
        transaction=transaction,
        first=AsyncMock(return_value=SimpleNamespace(_mapping=control_by_field)),
        execute=AsyncMock(),
    )
    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(
        control_imports, "get_import_run", AsyncMock(return_value={**control_by_field, "status": "canceling"})
    )
    monkeypatch.setattr(control_imports, "_write_run_live_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda _run: None)
    if change is not None:
        with pytest.raises(RuntimeError, match="cancellation attempt changed"):
            await control_imports._request_source_profile_cancel(handoff["run_id"])
        database.execute.assert_not_awaited()
    else:
        canceled = await control_imports._request_source_profile_cancel(handoff["run_id"])
        assert canceled["metrics"] == {"source_profile_handoff": handoff}
        assert canceled["status"] == "canceling"
        parameters = database.execute.call_args.args[0].compile().params
        assert set(parameters) == {"status", "phase_detail", "heartbeat_at", "run_id_1"}
        assert "FOR UPDATE" in str(database.first.call_args.args[0])


@pytest.mark.asyncio
async def test_worker_cancel_losing_handoff_race_uses_publisher_cancellation(monkeypatch):
    """The ordinary cancel CAS cannot overwrite a concurrently committed handoff or its receipt."""
    from api import control_imports

    handoff = native_handoff(archive.PROJECTION_IMPORTER)
    current_by_field = {
        "run_id": handoff["run_id"],
        "importer": handoff["importer_id"],
        "status": "running",
        "metrics": {},
    }
    persisted_by_field = {**current_by_field, "status": "finalizing", "metrics": {"source_profile_handoff": handoff}}
    database = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(rowcount=0)))
    protected_cancel = AsyncMock(return_value={**persisted_by_field, "status": "canceling"})
    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(return_value=persisted_by_field))
    monkeypatch.setattr(control_imports, "_request_source_profile_cancel", protected_cancel)
    canceled = await control_imports._persist_cancel_request(
        handoff["run_id"],
        current_run=current_by_field,
        requested_at=datetime(2026, 1, 2),
        cancel_state_by_name={"progress": {}, "canceled_now": False, "status": "canceling", "phase_detail": "cancel"},
        run_metrics_by_name={"cancel_signal": {}},
    )
    assert canceled == protected_cancel.return_value
    protected_cancel.assert_awaited_once_with(handoff["run_id"])
    statement = database.execute.call_args.args[0]
    assert "NOT IN" in str(statement) and "source_profile_handoff" in statement.compile().params.values()
    assert not control_imports._has_source_profile_handoff({**persisted_by_field, "importer": "ptg"})


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["unchanged", "replaced_after_lock", "absent", "moved"])
async def test_canceled_projection_cleanup_rechecks_exact_oid_after_lock(monkeypatch, state):
    """A Runtime-owned candidate cannot redirect Publisher cleanup between lookup and lock."""
    seal = native_handoff(archive.PROJECTION_IMPORTER)["projection"]
    has_candidate = state in {"unchanged", "replaced_after_lock"}
    observed_oids = (
        [seal["relation_oid"], seal["relation_oid"] + (state == "replaced_after_lock")] if has_candidate else [None]
    )
    monkeypatch.setattr(archive.native, "_relation_oid", AsyncMock(side_effect=observed_oids))
    locked = AsyncMock()
    monkeypatch.setattr(archive.native, "_lock_family", locked)
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=seal["owner_oid"] if has_candidate else state == "moved"), execute=AsyncMock()
    )
    pending = archive._drop_native_projection_candidate(session, "mrf", seal, 99)
    if state in {"replaced_after_lock", "moved"}:
        with pytest.raises(archive.SourceProfileArchiveError, match="ownership changed|candidate moved"):
            await pending
    else:
        await pending
    assert locked.await_count == has_candidate
    assert session.execute.await_count == (state == "unchanged")
    if state == "unchanged":
        assert str(session.execute.call_args.args[0]) == f'DROP TABLE "mrf"."{seal["table_name"]}" RESTRICT'


def test_historical_pin_guard_ddl_is_unchanged():
    statements = "\n".join(archive.pins.pin_guard_statements("fixture"))
    assert (
        hashlib.sha256(statements.encode()).hexdigest()
        == "d8c97782387576c62ac6e45a391a7c1d057f2bb8592531171ba8ec9ee0f389f4"
    )
    current = "\n".join(archive.pins.statement_pin_guard_statements("fixture"))
    assert "FOR EACH ROW" not in current
    assert current.count("FOR EACH STATEMENT") == 19
    assert "OLD TABLE AS profile_guard_old NEW TABLE AS profile_guard_new" in current


@pytest.mark.parametrize("importer", [name for name in archive.SOURCES if name != archive.PROJECTION_IMPORTER])
def test_completed_source_result_has_no_live_reference_dependency(importer):
    run = retained_run(importer)
    assert archive._validate_run(importer, run) == {}
    assert archive.source_spec(importer).dependencies == ()
    for field, replacement in (
        ("status", "running"),
        ("source_key", "other-source"),
        ("metrics", {"published": False}),
        ("finished_at", None),
    ):
        with pytest.raises(archive.SourceProfileArchiveError):
            archive._validate_run(importer, {**run, field: replacement})
    run["source_manifest"]["max_providers"] = 1
    with pytest.raises(archive.SourceProfileArchiveError):
        archive._validate_run(importer, run)


def test_projection_control_and_local_authority_are_not_portable():
    assert archive.source_spec("florida-mqa-profile").table_names == (*archive.TABLES, "provider_profile_projection")
    assert archive.TABLES == (
        "provider_profile_import_run",
        "provider_profile_artifact",
        "provider_profile_source_record",
        "provider_profile_fact",
    )
    with pytest.raises(archive.SourceProfileArchiveError):
        archive.result_dependencies({"npi": "a" * 64})
    with pytest.raises(archive.SourceProfileArchiveError):
        archive.stage_schema("peer-selected")


@pytest.mark.parametrize("importer", [name for name in archive.SOURCES if name != archive.PROJECTION_IMPORTER])
@pytest.mark.parametrize("fault", ["agency", "categories", "missing_registry"])
def test_unservable_publication_scope_is_rejected(importer, fault):
    run = retained_run(importer)
    manifest = run["source_manifest"]
    if fault == "agency":
        manifest["source"]["agency"] = "Incorrect agency"
    elif fault == "categories":
        manifest["categories"] = ["unsupported"]
    else:
        del manifest["source"]["registry_generation"]
    with pytest.raises(archive.SourceProfileArchiveError):
        archive._validate_run(importer, run)


@pytest.mark.parametrize("importer", ["tennessee-tdh-profile", "rhode-island-doh-profile", "new-york-nypp-profile"])
@pytest.mark.parametrize("field", ["coverage_scope", "registry_generation"])
def test_registry_bound_publication_scope_is_preserved(importer, field):
    run = retained_run(importer)
    run["source_manifest"]["source"][field] = "different"
    with pytest.raises(archive.SourceProfileArchiveError, match="serving scope differs"):
        archive._validate_run(importer, run)


def _versioned_manifest(contract, importer="massachusetts-borim-profile"):
    return {
        "contract": contract,
        "importer_id": importer,
        "source_key": archive.SOURCES[importer][0],
        "run_id": "a" * 32,
        "run_ids": ["a" * 32],
        "source_completed_at": "2026-01-02T00:00:00+00:00",
        "source_manifest_sha256": "b" * 64,
        "dependencies": {},
        "tables": [
            {
                "table_name": name,
                "row_count": 1,
                "schema_sha256": "c" * 64,
                **({"content_sha256": "d" * 64} if contract == archive.LEGACY_CONTRACT else {}),
            }
            for name in archive.source_spec(importer).table_names
        ],
    }


@pytest.mark.parametrize(
    "importer,contract",
    [
        (importer, contract)
        for importer in archive.SOURCES
        for contract in (archive.CONTRACT, archive.LEGACY_CONTRACT)
        if importer != archive.PROJECTION_IMPORTER or contract == archive.CONTRACT
    ],
)
async def test_source_export_copies_every_supported_contract_before_index_and_validation(
    monkeypatch, importer, contract
):
    manifest = _versioned_manifest(contract, importer)
    ownership = SimpleNamespace(schema_name="isolated_source")
    for name, field_value in (
        ("_source_lock", None),
        ("require_pin_guards", None),
        ("_pointer", {"current_run_id": manifest["run_id"]}),
        ("_lineage", [{"run_id": manifest["run_id"]}]),
        ("precreate_restore", ownership),
    ):
        monkeypatch.setattr(archive, name, AsyncMock(return_value=field_value))
    monkeypatch.setattr(archive.native, "_lock_family", AsyncMock())
    monkeypatch.setattr(archive.pins, "record_pin", AsyncMock())
    events = []
    copy_rows = AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("copy") or 10)
    for name, event, field_value in (
        ("complete_restore", "index", None),
        ("describe_result", "describe", manifest),
        ("_require_equal_scope", "compare", None),
    ):

        async def checkpoint(*_args, _event=event, _result=field_value, **_kwargs):
            events.append(_event)
            return _result

        monkeypatch.setattr(archive, name, AsyncMock(side_effect=checkpoint))
    session = SimpleNamespace(execute=AsyncMock())
    source_copy = archive.native.ReferenceFamilySourceCopy(copy_rows, 1000, 300)
    prepared = await archive.prepare_source(
        session,
        importer_id=importer,
        schema="source",
        run_id=manifest["run_id"],
        dataset_id=uuid4(),
        contract=contract,
        source_copy=source_copy,
    )
    assert prepared.manifest == manifest and prepared.ownership is ownership
    spec = archive.source_spec(importer)
    assert events == ["copy"] * len(spec.model_types) + ["index", "describe", "compare"]
    for index, (call, model) in enumerate(zip(copy_rows.await_args_list, spec.model_types, strict=True)):
        assert call.args[0] is session
        assert f'FROM "source"."{model.__tablename__}"' in call.args[1]
        column = "generation_id" if model is archive.models.ProviderProfileProjection else "run_id"
        assert f"WHERE \"{column}\"=ANY(ARRAY['{manifest['run_id']}']::text[])" in call.args[1]
        assert call.kwargs["columns"] == tuple(field.name for field in model.__table__.columns)
        assert call.kwargs["schema_name"] == ownership.schema_name
        assert call.kwargs["table_name"] == model.__tablename__
        assert call.kwargs["max_bytes"] == 1000 - 10 * index
        assert 0 < call.kwargs["timeout"] <= 300
    assert all(str(call.args[0]).startswith("SET LOCAL") for call in session.execute.await_args_list)
    archive.pins.record_pin.assert_awaited_once()


@pytest.mark.parametrize("importer", tuple(archive.SOURCES))
async def test_publication_copies_only_novel_model_runs_with_one_remaining_budget(importer):
    prepared = SimpleNamespace(
        manifest=_versioned_manifest(archive.CONTRACT, importer),
        ownership=SimpleNamespace(
            schema_name="isolated_source", relation_oids=tuple(zip(archive.PUBLICATION_TABLES, range(1, 5)))
        ),
    )
    copy_rows = AsyncMock(return_value=10)
    session = SimpleNamespace(scalar=AsyncMock(return_value=True), execute=AsyncMock())
    source_copy = archive.native.ReferenceFamilySourceCopy(copy_rows, 1000, 300)
    novel_run_ids = ["b" * 32]
    assert (
        await archive._load_publication_models(
            session, prepared, novel_run_ids, source_copy, asyncio.get_running_loop().time() + source_copy.timeout
        )
        == 960
    )
    assert session.scalar.await_count == 2 * len(archive.TABLES)
    session.execute.assert_not_awaited()
    for index, call in enumerate(copy_rows.await_args_list):
        assert f'FROM "isolated_source"."{archive.TABLES[index]}"' in call.args[1]
        assert f"ARRAY['{novel_run_ids[0]}']" in call.args[1] and prepared.manifest["run_id"] not in call.args[1]
        assert call.kwargs["table_name"] == archive.PUBLICATION_TABLES[index]
        assert call.kwargs["max_bytes"] == 1000 - 10 * index
        assert 0 < call.kwargs["timeout"] <= 300
    assert copy_rows.await_count == len(archive.TABLES)


@pytest.mark.parametrize("phase", ["export", "publication"])
@pytest.mark.parametrize("fault", ["missing", "invalid", "overflow", "cancel"])
async def test_isolated_copy_failure_never_inserts_or_finishes_indexes(monkeypatch, phase, fault):
    importer = "massachusetts-borim-profile"
    ownership = SimpleNamespace(
        schema_name="isolated_source", relation_oids=tuple(zip(archive.PUBLICATION_TABLES, range(1, 5)))
    )
    prepared = SimpleNamespace(manifest=_versioned_manifest(archive.CONTRACT), ownership=ownership)
    copy_rows = AsyncMock(return_value=1001, side_effect=asyncio.CancelledError if fault == "cancel" else None)
    source_copy = archive.native.ReferenceFamilySourceCopy(copy_rows, 1000, 300)
    if fault in {"missing", "invalid"}:
        source_copy = None if fault == "missing" else SimpleNamespace(copy_rows=copy_rows, max_bytes=1000, timeout=300)
    session = SimpleNamespace(scalar=AsyncMock(return_value=True), execute=AsyncMock())
    indexes = AsyncMock()
    monkeypatch.setattr(archive, "complete_restore", indexes)
    expected = (
        asyncio.CancelledError
        if fault == "cancel"
        else (
            archive.SourceProfileArchiveError
            if fault in {"missing", "invalid"}
            else archive.native.ReferenceFamilyArchiveError
        )
    )
    with pytest.raises(expected):
        if phase == "export":
            await archive._load_source_models(
                session, archive.source_spec(importer), "source", ownership, ["a" * 32], source_copy
            )
        else:
            await archive._load_publication_models(
                session, prepared, ["a" * 32], source_copy, asyncio.get_running_loop().time() + 300
            )
    indexes.assert_not_awaited()
    session.execute.assert_not_awaited()
    assert copy_rows.await_count == (fault in {"overflow", "cancel"})


@pytest.mark.parametrize("importer", tuple(archive.SOURCES))
@pytest.mark.parametrize("fault", [None, "missing", "invalid", "extra", "wrong_fence"])
def test_publication_request_requires_exact_shared_copy_capability(importer, fault):
    source_copy = archive.native.ReferenceFamilySourceCopy(AsyncMock(), 1000, 300)
    is_projection = importer == archive.PROJECTION_IMPORTER
    valid_request_by_field = {"source_copy": source_copy, **({"expected": {}} if is_projection else {})}
    request_by_fault = {
        None: valid_request_by_field,
        "missing": None,
        "invalid": {**valid_request_by_field, "source_copy": object()},
        "extra": {**valid_request_by_field, "peer": "untrusted"},
        "wrong_fence": {"source_copy": source_copy, **({} if is_projection else {"expected": {}})},
    }
    request_by_field = request_by_fault[fault]
    manifest = _versioned_manifest(archive.CONTRACT, importer)
    if fault is not None:
        with pytest.raises(archive.SourceProfileArchiveError, match="COPY capability"):
            archive._publication_copy_request(manifest, request_by_field)
    else:
        assert archive._publication_copy_request(manifest, request_by_field) == (
            request_by_field if importer == archive.PROJECTION_IMPORTER else None
        )


@pytest.mark.parametrize("importer", tuple(name for name in archive.SOURCES if name != archive.PROJECTION_IMPORTER))
def test_legacy_adoption_does_not_invent_isolated_publication_storage(importer):
    manifest = _versioned_manifest(archive.LEGACY_CONTRACT, importer)
    assert archive._publication_copy_request(manifest, None) is None
    with pytest.raises(archive.SourceProfileArchiveError, match="cannot carry a publication request"):
        archive._publication_copy_request(
            manifest, {"source_copy": archive.native.ReferenceFamilySourceCopy(AsyncMock(), 1000, 300)}
        )


@pytest.mark.parametrize("importer", tuple(archive.SOURCES))
async def test_adoption_preserves_one_copy_deadline_and_projection_remainder(monkeypatch, importer):
    from process import florida_projection_archive as projection

    source_copy = archive.native.ReferenceFamilySourceCopy(AsyncMock(), 1000, 300)
    request_by_field = {
        "source_copy": source_copy,
        **({"expected": {}} if importer == archive.PROJECTION_IMPORTER else {}),
    }
    publication = AsyncMock(return_value=({"synthetic": "publication"}, 400))
    cutover = AsyncMock(return_value={"synthetic": "projection"})
    monkeypatch.setattr(archive, "_prepare_publication", publication)
    monkeypatch.setattr(projection, "prepare_native_cutover", cutover)
    prepared = SimpleNamespace(manifest=_versioned_manifest(archive.CONTRACT, importer))
    session, pin_id = object(), uuid4()
    before = asyncio.get_running_loop().time()
    result = await archive._prepare_adoption_storage(session, prepared, "destination", 41, pin_id, request_by_field)
    call = publication.await_args
    assert call.args == (session, prepared, "destination") and call.kwargs["source_copy"] is source_copy
    assert before < call.kwargs["deadline"] <= asyncio.get_running_loop().time() + source_copy.timeout
    assert result[0] == {"synthetic": "publication"}
    if importer == archive.PROJECTION_IMPORTER:
        assert result[1] == {"synthetic": "projection"}
        assert cutover.await_args.kwargs["source_copy"].copy_rows is source_copy.copy_rows
        assert cutover.await_args.kwargs["source_copy"].max_bytes == 400
        assert cutover.await_args.kwargs["deadline"] == call.kwargs["deadline"]
        assert cutover.await_args.kwargs["expected"] is request_by_field["expected"]
    else:
        assert result[1] is None
        cutover.assert_not_awaited()


@pytest.mark.parametrize("contract", (archive.CONTRACT, archive.LEGACY_CONTRACT))
def test_versioned_receipts_preserve_exact_original_meaning(contract):
    manifest = _versioned_manifest(contract)
    assert archive.validate_manifest(manifest) == manifest
    assert archive._validation_contract(manifest) == (
        archive.VALIDATION_CONTRACT if contract == archive.CONTRACT else archive.LEGACY_VALIDATION_CONTRACT
    )
    manifest["contract"] = archive.LEGACY_CONTRACT if contract == archive.CONTRACT else archive.CONTRACT
    with pytest.raises(archive.SourceProfileArchiveError, match="table receipt"):
        archive.validate_manifest(manifest)


def test_set_validated_receipt_rejects_legacy_hash_and_unknown_fields():
    for field in ("content_sha256", "approved_by_peer", "payload_sha256"):
        manifest = _versioned_manifest(archive.CONTRACT)
        manifest["tables"][0][field] = "d" * 64
        with pytest.raises(archive.SourceProfileArchiveError, match="table receipt"):
            archive.validate_manifest(manifest)
