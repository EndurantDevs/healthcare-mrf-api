# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source admission, retained-output orchestration, and best-effort cleanup."""

import asyncio
import hashlib
import importlib
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_dataset_rehydrate_pin as source_pin
from process import provider_directory_dataset_rehydrate_prepare as preparation
from process import provider_directory_dataset_rehydrate_publish as publication
from process import provider_directory_dataset_rehydrate_scope as scopes
from process import provider_directory_generic_source_policy as policy
from process.provider_directory_dataset_rehydrate_types import _payload_hash

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_unbound_startup_defers_admitted_layout_preflight(monkeypatch):
    """The registered worker can start before a per-job Source capability exists."""
    monkeypatch.setattr(fhir, "ensure_database", AsyncMock())
    monkeypatch.setattr(fhir.db, "status", AsyncMock(side_effect=AssertionError("startup SQL")))
    assert policy._FACTORY.get() is policy._PUBLISHER.get() is None
    startup_by_field = {}
    await fhir.startup(startup_by_field)
    assert startup_by_field["context"]["run"] == 0
    assert "provider_directory_tables_ready" not in startup_by_field["context"]
    fhir.db.status.assert_not_awaited()
    with pytest.raises(ValueError, match="publisher_connection_required"):
        await fhir._ensure_provider_directory_tables()
    fhir.db.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_ordinary_command_defers_preflight_until_job(monkeypatch):
    """The ordinary command shares startup but grants no ambient Source authority."""
    monkeypatch.setattr(fhir, "ensure_database", AsyncMock())

    async def process(context, task):
        assert "provider_directory_tables_ready" not in context["context"]
        assert policy._FACTORY.get() is policy._PUBLISHER.get() is None
        with pytest.raises(ValueError, match="publisher_connection_required"):
            await fhir._ensure_provider_directory_tables()
        return {"admission_required": True}

    monkeypatch.setattr(fhir, "process_data", process)
    assert await fhir.run_provider_directory_fhir_command() == {"admission_required": True}


@pytest.mark.asyncio
async def test_ordinary_profile_layout_reads_never_admit_acquisition(monkeypatch):
    from tests.test_provider_directory_profile_selection_attestation import _execution

    context = {"context": {}}
    monkeypatch.setattr(fhir.db, "all", AsyncMock(return_value=[]))
    monkeypatch.setattr(fhir.db, "status", AsyncMock(side_effect=AssertionError("runtime DDL")))
    assert not policy.is_source_storage_required()
    await fhir._prepare_provider_directory_job_tables(context, _execution())
    assert "provider_directory_tables_ready" not in context["context"]
    assert fhir.db.all.await_count == len(fhir.SOURCE_MODELS) + len(fhir.CANONICAL_RESOURCE_MODELS)
    assert all(call.args[0].startswith("SELECT ") and call.args[0].endswith(" LIMIT 0")
               for call in fhir.db.all.await_args_list)
    with pytest.raises(ValueError, match="publisher_connection_required"):
        await fhir._prepare_provider_directory_job_tables(context, None)
    fhir.db.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_ordinary_profile_missing_layout_refuses_without_ddl(monkeypatch):
    from tests.test_provider_directory_profile_selection_attestation import _execution

    context = {"context": {}}
    monkeypatch.setattr(fhir.db, "all", AsyncMock(side_effect=RuntimeError("missing column")))
    monkeypatch.setattr(fhir.db, "status", AsyncMock(side_effect=AssertionError("runtime DDL")))
    with pytest.raises(RuntimeError, match="missing column"):
        await fhir._prepare_provider_directory_job_tables(context, _execution())
    assert "provider_directory_tables_ready" not in context["context"]
    fhir.db.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_real_profile_entry_reaches_existing_selection_guard_without_source_binding(monkeypatch):
    from process import provider_directory_cms_replay as replay
    from process import provider_directory_profile_selection as selection
    from tests.test_provider_directory_profile_selection_attestation import _attestation

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    task = {
        **selection._GLOBAL_PROFILE_PARAMS,
        "provider_directory_profile_generation": 11,
        "provider_directory_profile_selection_attestation": _attestation().payload,
        "provider_directory_profile_capacity_attestation": {},
    }
    context = {"control_run_id": "run_" + "e" * 32, "context": {}}
    monkeypatch.setattr(fhir, "ensure_database", AsyncMock())
    monkeypatch.setattr(fhir, "_raise_if_resource_import_cancelled", AsyncMock())
    monkeypatch.setattr(fhir.db, "all", AsyncMock(return_value=[]))
    monkeypatch.setattr(fhir.db, "status", AsyncMock(side_effect=AssertionError("unexpected write")))
    monkeypatch.setattr(replay, "replay_committed_cms_profile", AsyncMock(return_value=None))
    monkeypatch.setattr(fhir, "_provider_directory_profile_selection_catalog", lambda: {})
    selection_guard = AsyncMock(side_effect=RuntimeError("selection guard reached"))
    monkeypatch.setattr(fhir, "assert_registered_profile_selection_current", selection_guard)
    assert not policy.is_source_storage_required()
    with pytest.raises(RuntimeError, match="selection guard reached"):
        await fhir.process_provider_directory_fhir_data(context, task)
    selection_guard.assert_awaited_once()
    fhir.db.status.assert_not_awaited()
    assert "provider_directory_tables_ready" not in context["context"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_phase", ["enter", "query", "exit"])
async def test_retry_cleanup_catches_source_transaction_failures(monkeypatch, failure_phase):
    """Query abort and role-reset/transaction-exit failures remain best effort."""
    events = []

    @asynccontextmanager
    async def transaction(_database):
        events.append("enter")
        if failure_phase == "enter":
            raise TimeoutError("lock timeout")
        try:
            yield
        finally:
            events.append("exit")
            if failure_phase == "exit":
                raise TimeoutError("role reset after aborted savepoint")

    monkeypatch.setattr(policy, "source_transaction", transaction)
    monkeypatch.setattr(fhir.db, "status", AsyncMock())
    monkeypatch.setattr(
        fhir,
        "_locked_finalized_retry_cleanup_dataset_ids",
        AsyncMock(
            side_effect=TimeoutError("statement timeout") if failure_phase == "query" else None,
            return_value=["dataset-a"],
        ),
    )
    monkeypatch.setattr(fhir, "_delete_endpoint_dataset_retry_state", AsyncMock())
    assert await fhir._clear_promoted_endpoint_dataset_retry_state([" dataset-a "]) == []
    assert events == (["enter"] if failure_phase == "enter" else ["enter", "exit"])


@pytest.mark.asyncio
async def test_retry_cleanup_preserves_cancellation(monkeypatch):
    """Cancellation leaves the Source transaction before propagating unchanged."""
    events = []

    @asynccontextmanager
    async def transaction(_database):
        try:
            yield
        finally:
            events.append("closed")

    monkeypatch.setattr(policy, "source_transaction", transaction)
    monkeypatch.setattr(fhir.db, "status", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await fhir._clear_promoted_endpoint_dataset_retry_state(["dataset-a"])
    assert events == ["closed"]


def _request():
    return fhir.RehydrationRequest("source-a", "dataset-a", "root-a", "repair-a", ("Organization",), 1)


def _retained_scope():
    payload = {"resource_id": "organization-a", "name": "Example Organization"}
    retained_by_field = dict(
        resource_type="Organization",
        resource_id="organization-a",
        payload_hash=_payload_hash(payload),
        payload_json=payload,
        acquired_resource_sha256=None,
    )
    digest = hashlib.sha256()
    scopes._append_digest_rows(digest, [retained_by_field], 0)
    scope = fhir.DatasetScope(
        "source-a",
        "dataset-a",
        "root-a",
        "endpoint-a",
        "https://example.test/fhir",
        digest.hexdigest(),
        1,
        ("Organization",),
        fhir.LEGACY_RESOURCE_HASH_CONTRACT,
        None,
        "b" * 64,
        "2026-01-01T00:00:00Z",
    )
    return scope, retained_by_field


class _PinnedReader:
    def __init__(self, row):
        self.row = row
        self.calls = []

    async def all(self, sql, **parameters):
        self.calls.append((sql, parameters))
        if "SELECT resource_type, resource_id, payload_hash" in sql:
            return [self.row]
        assert 'FROM "snapshot"."source_child"' in sql
        assert parameters["batch_size"] == 1
        return [self.row] if parameters["cursor"] is None else []


def _pinned(row):
    scope, _ = _retained_scope()
    return SimpleNamespace(
        scope=scope, reader=_PinnedReader(row), source_relation=lambda directory: '"snapshot"."source_child"'
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "payload", "digest"])
async def test_real_retained_payload_validation_is_read_only(monkeypatch, damage):
    """Actual batch validation catches corrupt payloads before any output preparation."""
    scope, retained_by_field = _retained_scope()
    if damage == "payload":
        retained_by_field = {
            **retained_by_field,
            "payload_json": {**retained_by_field["payload_json"], "name": "Changed"},
        }
    if damage == "digest":
        scope = replace(scope, dataset_hash="c" * 64)
    pinned = _pinned(retained_by_field)
    pinned.scope = scope
    runtime = fhir._dataset_rehydration_runtime({}, {}, _request())
    monkeypatch.setattr(fhir, "_raise_if_resource_import_cancelled", AsyncMock())
    monkeypatch.setattr(fhir, "_upsert_resource_rows", AsyncMock(side_effect=AssertionError("published SOURCE write")))
    if damage:
        with pytest.raises(RuntimeError, match="payload_rejected|dataset_hash_mismatch"):
            await fhir._validate_rehydration_input(runtime, _request(), pinned, ("Organization",))
    else:
        assert await fhir._validate_rehydration_input(runtime, _request(), pinned, ("Organization",)) == (
            scope.dataset_hash,
            (("Organization", 1),),
        )
    fhir._upsert_resource_rows.assert_not_awaited()
    with pytest.raises(RuntimeError, match="prepared_outputs_required"):
        await runtime.upsert_batch(fhir.ProviderDirectoryOrganization, [retained_by_field["payload_json"]], scope)


def _prepared_runner(monkeypatch, *, reused, failed):
    """Bind trust-boundary doubles while preserving the actual entrypoint and validator."""
    scope, retained_by_field = _retained_scope()
    pinned = _pinned(retained_by_field)
    events = []
    prepared = object()

    @asynccontextmanager
    async def guard(runtime, request):
        events.append("guard")
        try:
            yield
        finally:
            events.append("guard_closed")

    @asynccontextmanager
    async def pin(runtime, request, *, directory):
        assert directory is fhir
        events.append("pin")
        try:
            yield pinned
        finally:
            events.append("pin_closed")

    @asynccontextmanager
    async def prepare(directory, original, operation, *, checkpoint_journal):
        assert original is pinned
        assert operation["source_pins"] == [preparation._source_pin(scope)]
        assert checkpoint_journal.expected_by_resource == (("Organization", 1),)
        events.append("prepare")
        try:
            yield prepared
        finally:
            events.append("prepare_closed")

    async def publish(directory, original, operation, actual, **options):
        assert directory is fhir and original is pinned and actual is prepared
        assert not options["artifact_bundle"].stages
        events.append("publish")
        if failed:
            raise RuntimeError("final proof rejected")

    _bind_prepared_runner(monkeypatch, guard, pin, prepare, publish, reused)
    return scope, events


def _bind_prepared_runner(monkeypatch, guard, pin, prepare, publish, reused):
    """Keep protocol dependencies explicit for success, replay, and rollback cases."""
    monkeypatch.setattr(scopes, "_dataset_guard", guard)
    monkeypatch.setattr(source_pin, "pinned_current_source", pin)
    monkeypatch.setattr(preparation, "prepared_compatibility_outputs", prepare)
    monkeypatch.setattr(publication, "publish_prepared_rehydration", publish)
    monkeypatch.setattr(fhir, "_raise_if_resource_import_cancelled", AsyncMock())
    monkeypatch.setattr(
        fhir,
        "_resolve_provider_directory_artifact_datasets",
        AsyncMock(return_value=fhir.ProviderDirectoryArtifactDatasetFence(())),
    )
    monkeypatch.setattr(fhir, "_is_rehydration_output_replay_valid", AsyncMock(return_value=reused))
    monkeypatch.setattr(
        fhir,
        "_rehydration_output_summaries",
        AsyncMock(
            return_value={
                "Organization": {
                    "state": "complete",
                    "input": 1,
                    "mapped": 1,
                    "rejected": 0,
                    "reused_complete_checkpoint": reused,
                }
            }
        ),
    )
    monkeypatch.setattr(fhir, "_upsert_resource_rows", AsyncMock(side_effect=AssertionError("SOURCE write")))
    from process import provider_directory_dataset_rehydrate_journal_store as journals

    monkeypatch.setattr(
        journals, "preparation_operation", AsyncMock(side_effect=lambda journal, operation, targets: operation)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("reused,failed", [(False, False), (True, False), (False, True)])
async def test_retained_runner_uses_pinned_prepare_publish_and_closes(monkeypatch, reused, failed):
    """The active callback uses isolated output stages, with no SOURCE append path."""
    scope, events = _prepared_runner(monkeypatch, reused=reused, failed=failed)
    execution_by_field = {"context": {}}
    task_by_field = dict(
        rehydrate_dataset_id="dataset-a",
        rehydrate_acquisition_root_run_id="root-a",
        rehydrate_resources="Organization",
        rehydrate_batch_size=1,
    )
    if failed:
        with pytest.raises(RuntimeError, match="final proof rejected"):
            await fhir._run_provider_directory_dataset_rehydrate(
                execution_by_field, task_by_field, "repair-a", ["source-a"]
            )
        assert "audit" not in execution_by_field["context"]
        fhir._rehydration_output_summaries.assert_not_awaited()
    else:
        execution_summary = await fhir._run_provider_directory_dataset_rehydrate(
            execution_by_field, task_by_field, "repair-a", ["source-a"]
        )
        assert execution_summary["status"] == "complete" and execution_summary["input_count"] == 1
        assert (
            execution_summary["dataset_hash_verified_before"]
            == execution_summary["dataset_hash_verified_after"]
            == scope.dataset_hash
        )
        assert execution_by_field["context"]["audit"] is execution_summary
    assert events[-2:] == ["pin_closed", "guard_closed"]
    assert ("prepare" in events) is not reused
    assert ("publish" in events) is not reused
    fhir._upsert_resource_rows.assert_not_awaited()


def _published_outputs(*, run_id="earlier-repair", oid_offset=0):
    """Build structurally authentic receipts with the existing checkpoint protocol."""
    from process import provider_directory_dataset_rehydrate_journal as protocol
    from process.provider_directory_dataset_rehydrate_pages import _key_columns
    from process.provider_directory_output_snapshot_catalog import output_models
    from process.provider_directory_output_snapshot_custody import PreparedOutputReceipt, _operation_digest
    from process.provider_directory_output_snapshot_retention import RetainedOutputReceipt, retained_name
    from process.provider_directory_output_snapshot_stage import OutputStage
    from tests.test_provider_directory_output_snapshot_retention import relation

    scope, _ = _retained_scope()
    operation_by_field = dict(
        kind="rehydrate",
        run_id=run_id,
        producer="provider-directory-fhir",
        source_pins=[preparation._source_pin(scope)],
        resource_types=["Organization"],
        source_fence_digest="f" * 64,
    )
    output_targets = preparation._targets(fhir, ("Organization",))
    digest = _operation_digest(operation_by_field, output_targets)
    journal_by_field = protocol.begin_preparation(
        protocol.output_journal({}), operation_by_field, output_targets, ["Organization"]
    )
    retained_receipts = []
    for index, target_name in enumerate(output_targets):
        incumbent = relation(target_name, target_name, 101 + index + oid_offset)
        successor = relation(target_name, "isolated_" + str(index), 201 + index + oid_offset)
        stage = OutputStage(incumbent.schema, target_name, successor.relation, successor.oid, digest, incumbent)
        receipt = PreparedOutputReceipt(1, digest, incumbent, successor)
        journal_by_field = protocol.record_stage(
            journal_by_field,
            stage,
            cursor=["key"] * len(_key_columns(output_models()[target_name])),
            rows=1,
            receipt=receipt,
        )
        retained_receipts.append(
            RetainedOutputReceipt(
                1,
                digest,
                incumbent.schema,
                target_name,
                retained_name(incumbent.oid),
                replace(incumbent, relation=retained_name(incumbent.oid)),
                replace(successor, relation=target_name),
            )
        )
    sealed = protocol.seal_preparation(journal_by_field)
    published = protocol.publish_generation(sealed, digest, tuple(retained_receipts), {"Organization": {"input": 1}})
    return operation_by_field, published, tuple(retained_receipts)


@pytest.mark.asyncio
@pytest.mark.parametrize("native_damage", [None, "index", "acl"])
async def test_completed_repair_replay_checks_native_receipts_without_mutating(monkeypatch, native_damage):
    """Stored JSON alone cannot authorize replay after native identity/ACL changes."""
    from process import provider_directory_output_snapshot_custody as custody

    intent, history, receipts = _published_outputs()
    pinned = _pinned(_retained_scope()[1])
    journal = SimpleNamespace(members=("Organization",), runtime=SimpleNamespace(schema="source_schema"))
    monkeypatch.setattr(fhir, "_rehydration_completed_generation", AsyncMock(return_value=history))
    capability = SimpleNamespace(connection=SimpleNamespace(execute=AsyncMock()))

    @asynccontextmanager
    async def transaction(directory, *, operation, targets):
        assert directory is fhir and operation["source_pins"] == intent["source_pins"]
        assert targets == tuple(receipt.target for receipt in receipts)
        yield capability

    actual_by_name = {proof.relation: proof for receipt in receipts for proof in (receipt.incumbent, receipt.successor)}
    if native_damage == "index":
        first = receipts[0].successor
        actual_by_name[first.relation] = replace(first, indexes=())
    monkeypatch.setattr(custody, "output_snapshot_transaction", transaction)
    monkeypatch.setattr(
        custody,
        "_capture",
        AsyncMock(
            return_value=actual_by_name,
            side_effect=RuntimeError("custody_changed") if native_damage == "acl" else None,
        ),
    )
    current_intent_by_field = {**intent, "run_id": "new-control-run"}
    if native_damage:
        with pytest.raises(RuntimeError, match="native_receipt_changed|custody_changed"):
            await fhir._is_rehydration_output_replay_valid(journal, pinned, current_intent_by_field)
    else:
        assert await fhir._is_rehydration_output_replay_valid(journal, pinned, current_intent_by_field) is True
    if native_damage == "acl":
        capability.connection.execute.assert_not_awaited()
        return
    statement = capability.connection.execute.await_args.args[0]
    assert statement.startswith("LOCK TABLE ") and statement.endswith(" IN ACCESS SHARE MODE NOWAIT")
    assert "ALTER" not in statement and "INSERT" not in statement


@pytest.mark.asyncio
async def test_completed_checkpoint_read_requires_agreement_and_real_counts(monkeypatch):
    """Checkpoint evidence remains bound to exact source, expected count, and output proof."""
    from process import provider_directory_dataset_rehydrate_store as store
    from process.provider_directory_dataset_rehydrate_types import RehydrationCheckpoint, ResourceProof

    _, history, _ = _published_outputs()
    pinned = _pinned(_retained_scope()[1])
    runtime = fhir._dataset_rehydration_runtime({}, {}, _request())
    journal = SimpleNamespace(runtime=runtime, request=_request(), expected_by_resource=(("Organization", 1),))
    checkpoint = RehydrationCheckpoint("complete", "organization-a", 1, 1, 1, 0, {"output_snapshots": history})
    monkeypatch.setattr(store, "_load_checkpoint", AsyncMock(return_value=checkpoint))
    monkeypatch.setattr(
        store, "_load_pinned_resource_proof", AsyncMock(return_value=ResourceProof(1, 1, 0, 1, 0, 1, 0))
    )

    @asynccontextmanager
    async def transaction(database):
        assert database is fhir.db
        yield

    monkeypatch.setattr(policy, "source_transaction", transaction)
    assert await fhir._rehydration_completed_generation(journal, pinned) == history
    store._load_pinned_resource_proof.return_value = ResourceProof(1, 0, 0, 1, 0, 1, 0)
    assert await fhir._rehydration_completed_generation(journal, pinned) is None


@pytest.mark.asyncio
async def test_completed_replay_skips_older_native_generations(monkeypatch):
    """Multiple retained repairs select the installed cut rather than the first history item."""
    from process import provider_directory_output_snapshot_custody as custody

    old_intent, old_history, _ = _published_outputs()
    current_intent, current_history, receipts = _published_outputs(run_id="later-repair", oid_offset=1000)
    history_by_field = {
        **current_history,
        "generations": {**old_history["generations"], **current_history["generations"]},
    }
    journal = SimpleNamespace(members=("Organization",), runtime=SimpleNamespace(schema="source_schema"))
    monkeypatch.setattr(fhir, "_rehydration_completed_generation", AsyncMock(return_value=history_by_field))
    capability = SimpleNamespace(connection=SimpleNamespace(execute=AsyncMock()))

    @asynccontextmanager
    async def transaction(directory, *, operation, targets):
        assert operation in (old_intent, current_intent)
        yield capability

    native_by_name = {proof.relation: proof for receipt in receipts for proof in (receipt.incumbent, receipt.successor)}
    monkeypatch.setattr(custody, "output_snapshot_transaction", transaction)
    monkeypatch.setattr(custody, "_capture", AsyncMock(return_value=native_by_name))
    assert await fhir._is_rehydration_output_replay_valid(journal, _pinned(_retained_scope()[1]), current_intent)
    capability.connection.execute.assert_awaited_once()
    assert custody._capture.await_count == 2


@pytest.mark.asyncio
async def test_active_checkpoint_preparation_is_not_skipped_as_completed(monkeypatch):
    """An active resumable attempt must pass normal preparation intent checks."""
    _, history, _ = _published_outputs()
    monkeypatch.setattr(fhir, "_rehydration_completed_generation", AsyncMock(return_value={**history, "active": {}}))
    assert await fhir._is_rehydration_output_replay_valid(None, None, None) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_field", ["source_fence_digest", "producer", "resource_types"])
async def test_completed_repair_replay_requires_same_operation_except_run_id(monkeypatch, changed_field):
    """Installed native identity cannot authorize replay for a different admitted intent."""
    from process import provider_directory_output_snapshot_custody as custody

    intent, history, receipts = _published_outputs()
    journal = SimpleNamespace(members=("Organization",), runtime=SimpleNamespace(schema="source_schema"))
    monkeypatch.setattr(fhir, "_rehydration_completed_generation", AsyncMock(return_value=history))
    capability = SimpleNamespace(connection=SimpleNamespace(execute=AsyncMock()))
    current_operation_by_field = {**intent, "run_id": "new-control-run"}
    current_operation_by_field[changed_field] = {
        "source_fence_digest": "e" * 64,
        "producer": "different-producer",
        "resource_types": ["Organization", "Location"],
    }[changed_field]
    assert current_operation_by_field["source_pins"] == intent["source_pins"]

    @asynccontextmanager
    async def transaction(directory, *, operation, targets):
        assert directory is fhir and operation is current_operation_by_field
        assert targets == tuple(receipt.target for receipt in receipts)
        yield capability

    installed_by_target = {receipt.target: receipt.target for receipt in receipts}
    actual_by_name = {proof.relation: proof for receipt in receipts for proof in (receipt.incumbent, receipt.successor)}
    monkeypatch.setattr(custody, "output_snapshot_transaction", transaction)
    monkeypatch.setattr(custody, "_capture", AsyncMock(return_value=actual_by_name))
    assert not await fhir._is_rehydration_output_replay_valid(
        journal, _pinned(_retained_scope()[1]), current_operation_by_field
    )
    custody._capture.assert_awaited_once_with(capability, installed_by_target)
    capability.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_issued_command_runs_actual_startup_and_shutdown_with_factory(monkeypatch):
    """The operation runner covers the real command lifecycle without granting a session."""
    events = []
    original_startup, original_shutdown = fhir.startup, fhir.shutdown
    factory = SimpleNamespace(transaction=lambda: None)

    async def startup(context):
        assert policy._FACTORY.get() is factory and policy._PUBLISHER.get() is None
        events.append("startup")
        await original_startup(context)

    async def process(context, task):
        assert policy._FACTORY.get() is factory and policy._PUBLISHER.get() is None
        assert context["context"]["run"] == 0
        events.append("job")
        return {"admitted": True}

    async def shutdown(context):
        assert policy._FACTORY.get() is factory and policy._PUBLISHER.get() is None
        events.append("shutdown")
        await original_shutdown(context)

    monkeypatch.setattr(fhir, "ensure_database", AsyncMock())
    monkeypatch.setattr(fhir, "startup", startup)
    monkeypatch.setattr(fhir, "process_data", process)
    monkeypatch.setattr(fhir, "shutdown", shutdown)
    async with policy.bind_generic_source_transaction_factory(factory):
        assert await fhir.run_provider_directory_fhir_command() == {"admitted": True}
    assert events == ["startup", "job", "shutdown"]
    assert policy._FACTORY.get() is policy._PUBLISHER.get() is None


def test_click_issued_runner_preserves_existing_option_normalization(monkeypatch):
    """The launcher receives the existing command and normalized public options."""
    from click.testing import CliRunner

    import process as commands

    events = []
    factory = SimpleNamespace(transaction=lambda: None)

    async def command(**fields):
        assert policy._FACTORY.get() is factory and policy._PUBLISHER.get() is None
        assert fields["source_ids"] == ["source-a"]
        assert fields["rehydrate_resources"] == ["Organization"]
        assert fields["probe"] is False and fields["test_mode"] is True
        events.append("command")

    async def runner(original, fields):
        assert original is command and policy._FACTORY.get() is None
        async with policy.bind_generic_source_transaction_factory(factory):
            await original(**fields)
        events.append("closed")

    monkeypatch.setattr(commands, "initiate_provider_directory_fhir", command)
    command_outcome = CliRunner().invoke(
        commands.provider_directory_fhir,
        ["--test", "--source-id", "source-a", "--no-probe", "--rehydrate-resource", "Organization"],
        obj={"provider_directory_operation_runner": runner},
    )
    assert command_outcome.exit_code == 0, command_outcome.output
    assert events == ["command", "closed"]
    assert policy._FACTORY.get() is policy._PUBLISHER.get() is None


@pytest.mark.asyncio
async def test_registered_worker_callbacks_use_actual_fhir_lifecycle_with_issued_factory(monkeypatch):
    import process as commands

    settings = commands.ProviderDirectoryFHIR
    assert settings.on_startup is fhir.startup and settings.on_shutdown is fhir.shutdown
    assert settings.functions[0] is fhir.process_data
    factory = SimpleNamespace(transaction=lambda: None)
    context_by_field = {}
    monkeypatch.setattr(fhir, "ensure_database", AsyncMock())
    async with policy.bind_generic_source_transaction_factory(factory):
        await settings.on_startup(context_by_field)
        assert policy._FACTORY.get() is factory and policy._PUBLISHER.get() is None
        assert context_by_field["context"]["run"] == 0
        await settings.on_shutdown(context_by_field)
        assert "finished_at" in context_by_field["context"] and policy._FACTORY.get() is factory
    assert policy._FACTORY.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "name,argument_count",
    [
        ("_verify_pinned_rehydration_digest", 2),
        ("_validate_rehydration_input", 4),
        ("_rehydration_completed_generation", 2),
        ("_is_rehydration_output_replay_valid", 3),
        ("_rehydrate_prepared_dataset", 2),
        ("_rehydration_output_summaries", 3),
    ],
)
async def test_rehydration_delegates_keep_directory(monkeypatch, name, argument_count):
    """Existing entrypoints keep their arguments and explicit directory callback owner."""
    from process import provider_directory_dataset_rehydrate as rehydrate

    arguments = tuple(object() for _ in range(argument_count))
    expected = object()
    helper = AsyncMock(return_value=expected)
    monkeypatch.setattr(rehydrate, name, helper)
    assert await getattr(fhir, name)(*arguments) is expected
    helper.assert_awaited_once_with(fhir, *arguments)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "binding", "proof"])
async def test_rehydration_summaries_keep_pinned_scope(monkeypatch, damage):
    """Summary proof checks stay inside the original Source transaction after delegation."""
    from process import provider_directory_dataset_rehydrate_store as store
    from process.provider_directory_dataset_rehydrate_types import RehydrationCheckpoint, ResourceProof

    scope, retained_by_field = _retained_scope()
    pinned = _pinned(retained_by_field)
    binding = object()
    pinned.binding = lambda directory: binding if directory is fhir else None
    runtime = fhir._dataset_rehydration_runtime({}, {}, _request())
    journal = SimpleNamespace(runtime=runtime, request=_request(), expected_by_resource=(("Organization", 1),))
    checkpoint = RehydrationCheckpoint("complete", "organization-a", 1, 1, 1, 0, {})
    proof = ResourceProof(1, 0 if damage == "proof" else 1, 0, 1, 0, 1, 0)
    events = []

    @asynccontextmanager
    async def transaction(database):
        assert database is fhir.db
        events.append("enter")
        try:
            yield
        finally:
            events.append("exit")

    monkeypatch.setattr(policy, "source_transaction", transaction)
    monkeypatch.setattr(
        source_pin,
        "_capture_source",
        AsyncMock(return_value=(scope, None, object() if damage == "binding" else binding)),
    )
    monkeypatch.setattr(store, "_load_checkpoint", AsyncMock(return_value=checkpoint))
    monkeypatch.setattr(store, "_load_pinned_resource_proof", AsyncMock(return_value=proof))
    if damage:
        with pytest.raises(RuntimeError, match="source_binding_changed|proof_incomplete"):
            await fhir._rehydration_output_summaries(journal, pinned, True)
    else:
        summaries_by_resource = await fhir._rehydration_output_summaries(journal, pinned, True)
        assert summaries_by_resource["Organization"]["reused_complete_checkpoint"] is True
        context, actual_pin, directory = store._load_pinned_resource_proof.await_args.args
        assert context.model is fhir.RESOURCE_MODELS_BY_TYPE["Organization"]
        assert actual_pin is pinned and directory is fhir
    assert events == ["enter", "exit"]
