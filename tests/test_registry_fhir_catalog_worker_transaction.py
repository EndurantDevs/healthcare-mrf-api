# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Worker enqueue transaction wiring only; no native capture authority is simulated."""

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest

from process import registry_fhir_catalog_publication as publication
from process import registry_fhir_catalog_sync as worker
from process.registry_fhir_catalog_publication import FHIRCatalogPublicationRuntime
from process.registry_publication_consumer import RegistryPublicationConsumerConfig


@pytest.mark.parametrize("completed", [False, True])
@pytest.mark.parametrize("approved_revision", [None, 7])
async def test_worker_enqueue_owns_repeatable_read_before_metadata_authorization(
    monkeypatch, completed, approved_revision
):
    class Loader:
        active = False
        isolation = None

        @asynccontextmanager
        async def transaction(self, *, isolation=None):
            self.active, self.isolation = True, isolation
            try:
                yield
            finally:
                self.active = False

    loader = Loader()
    identity = uuid4()
    snapshot_by_field = {"metadata_generation": "a" * 64, "network_generation": None, "metadata_approved_revision": 0}

    async def authorize(**arguments):
        assert arguments["connection"] is loader and loader.active
        assert loader.isolation == "repeatable_read"
        return {"authorized": True}

    runtime = FHIRCatalogPublicationRuntime(AsyncMock(side_effect=authorize), AsyncMock(), AsyncMock())
    consumer = RegistryPublicationConsumerConfig(
        "control_example", "lifecycle_example", "execution_example", "api_example", "queue_example", "owner_example"
    )
    config = SimpleNamespace(
        grant=SimpleNamespace(sha256="b" * 64, control_schema="control_example"), consumer=consumer
    )

    async def enqueue(connection, command, *, authorize_metadata, control_schema):
        assert connection is loader and control_schema == "control_example"
        assert command["expected_metadata_generation"] is None
        assert command["expected_approved_revision"] == (0 if approved_revision is None else approved_revision)
        await authorize_metadata(connection=connection, command=command, snapshot=snapshot_by_field)
        return {"request_id": str(identity), "state": "completed" if completed else "pending"}

    async def consume(**arguments):
        assert not loader.active
        actual = arguments["config"]
        assert actual.only_request_id == identity and actual.metadata_only is True
        assert actual.metadata_runtime is runtime and arguments["authorize"] is runtime.authorize_metadata
        return {"state": "published", "request_id": str(identity)}

    monkeypatch.setattr(worker, "enqueue_fhir_catalog_publication", AsyncMock(side_effect=enqueue))
    publish = AsyncMock(side_effect=consume)
    monkeypatch.setattr(worker, "consume_registry_publication_once", publish)
    cas_by_field = {"expected_approved_revision": approved_revision}
    operation_result = await worker._publish_catalog_snapshot(
        config, loader, snapshot_by_field, None, runtime, object(), object(), **cas_by_field
    )
    assert operation_result == {"state": "completed" if completed else "published", "request_id": str(identity)}
    assert publish.await_count == int(not completed)
    runtime.authorize_metadata.assert_awaited_once()


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_control_worker_keeps_partial_report_on_original_failure_or_cancellation(monkeypatch, failure):
    from process import registry_cms_source_worker as watcher
    from process import registry_fhir_catalog_reporting as reporting
    from process import registry_fhir_catalog_worker as control
    from tests.test_registry_fhir_catalog_reporting import disposition

    config = control.RegistryFHIRCatalogSyncConfig(None, None, None, None, None, None, None, "example", ())
    error = failure("private error details")

    async def reconcile(_config):
        report = reporting.current_fhir_catalog_report()
        report.add_sources((disposition(),), generations=("a" * 64,))
        report.source_started("a" * 64)
        raise error

    async def wait_for_cancel(*_arguments):
        await asyncio.Future()

    monkeypatch.setattr(control, "parse_fhir_catalog_sync_config", lambda _encoded: config)
    monkeypatch.setattr(worker, "reconcile_fhir_catalog", reconcile)
    monkeypatch.setattr(watcher, "watch_registry_control_cancel", wait_for_cancel)
    context_by_field = {"context": {"audit": {"existing": "retained"}}}
    with pytest.raises(failure) as caught:
        await control.control_start(context_by_field, {"run_id": "synthetic-run"})
    assert caught.value is error
    audit = context_by_field["context"]["audit"]
    assert audit["existing"] == "retained" and len(audit["fhir_catalog_sync"]["sources"]) == 1
    assert audit["fhir_catalog_sync"]["run_state"] == ("canceled" if failure is asyncio.CancelledError else "failed")
    assert "private error" not in str(audit)
    assert reporting.current_fhir_catalog_report() is None


@pytest.mark.parametrize("state", ["completed", "unchanged"])
async def test_worker_result_uses_existing_control_metrics_without_sealed_accounting_changes(monkeypatch, state):
    from process import control_lifecycle as lifecycle
    from process import registry_fhir_catalog_reporting as reporting
    from process import registry_fhir_catalog_worker as control
    from tests.test_registry_fhir_catalog_reporting import disposition

    config = control.RegistryFHIRCatalogSyncConfig(None, None, None, None, None, None, None, "example", ())
    events = []
    monkeypatch.setattr(reporting, "current_live_progress_context", lambda: {"run_id": "synthetic-run"})
    monkeypatch.setattr(reporting, "enqueue_live_progress", lambda **payload: events.append(payload))

    async def reconcile(_config):
        assert len(events) == 1 and events[0]["phase"] == "discovery"
        assert events[0]["denominator_state"] == "unknown"
        report = reporting.current_fhir_catalog_report()
        report.add_sources((disposition(),), generations=("a" * 64,))
        report.inventory_ready({"a" * 64: object()}, None)
        return {"state": state, "accepted_sources": 1, "unavailable_sources": 0}

    monkeypatch.setattr(worker, "reconcile_fhir_catalog", reconcile)
    context_by_field = {"audit": {"existing": "retained"}}
    with reporting.fhir_catalog_run_reporting(context_by_field):
        result = await control.run_fhir_catalog_sync(config)
    metrics = lifecycle._terminal_metrics_from_result(result, context=context_by_field)
    assert metrics["audit"]["fhir_catalog_sync"] is context_by_field["audit"]["fhir_catalog_sync"]
    assert metrics["audit"]["fhir_catalog_sync"]["run_state"] == state
    assert metrics["audit"]["existing"] == "retained"
    assert metrics["accepted_sources"] == 1 and metrics["unavailable_sources"] == 0
    assert "accounting_json" not in metrics and "custody_json" not in metrics


def service_request():
    from tests.test_registry_fhir_catalog_grant import command, snapshot, standing_grant

    grant = standing_grant()
    selected = snapshot(grant)
    command_by_field = command(grant, selected)
    actor_by_field, key, metadata = publication._metadata(command_by_field)
    record_by_field = {
        **metadata,
        "session_token_sha256": publication.NO_USER_SESSION,
        "idempotency_key": key,
        "actor_json": json.dumps(actor_by_field),
        "command_json": json.dumps(command_by_field),
        "state": "queued",
        "result_json": None,
    }
    return selected, command_by_field, record_by_field


@pytest.mark.parametrize("split_revision", [False, True])
def test_service_request_validates_legacy_and_exact_split_revision_keys(split_revision):
    _selected, command_by_field, record = service_request()
    if split_revision:
        command_by_field = {
            **command_by_field,
            "expected_approved_revision": command_by_field["expected_approved_revision"] + 1,
        }
    actor, key, metadata = publication._metadata(command_by_field, split_revision=split_revision)
    record.update(
        metadata, idempotency_key=key, actor_json=json.dumps(actor), command_json=json.dumps(command_by_field)
    )
    assert publication._validated_request(record) == command_by_field
    assert key.endswith(":approved:" + str(command_by_field["expected_approved_revision"])) is split_revision


@pytest.mark.parametrize("suffix", [":approved:0", ":approved:07", ":approved:7:extra", ":changed"])
def test_service_request_refuses_noncanonical_or_wrong_revision_key(suffix):
    _selected, command, record = service_request()
    command["expected_approved_revision"] = 7
    actor, key, metadata = publication._metadata(command, split_revision=True)
    record.update(metadata, idempotency_key=key, actor_json=json.dumps(actor), command_json=json.dumps(command))
    record["idempotency_key"] = "fhir-catalog:" + command["metadata_generation"] + suffix
    with pytest.raises(ValueError, match="request_invalid"):
        publication._validated_request(record)


def test_split_revision_retries_keep_generation_but_bind_each_exact_approved_head():
    _selected, command, record = service_request()
    first_by_field = {**command, "expected_approved_revision": 7}
    second_by_field = {**command, "expected_approved_revision": 8}
    actor, key, metadata = publication._metadata(first_by_field, split_revision=True)
    _, next_key, next_metadata = publication._metadata(second_by_field, split_revision=True)
    assert key != next_key and metadata["request_sha256"] != next_metadata["request_sha256"]
    assert publication._metadata(first_by_field, split_revision=True) == (actor, key, metadata)
    record.update(
        metadata, idempotency_key=next_key, actor_json=json.dumps(actor), command_json=json.dumps(second_by_field)
    )
    with pytest.raises(ValueError, match="request_invalid"):
        publication._validated_request(record)


@pytest.mark.asyncio
@pytest.mark.parametrize("raced", [False, True])
async def test_authenticated_enqueue_reuses_an_existing_or_concurrently_inserted_request(monkeypatch, raced):
    selected, command_by_field, _record = service_request()
    existing_receipt_by_field = {"request_id": str(uuid4()), "state": "pending"}
    connection = Mock(is_in_transaction=Mock(return_value=True), fetchrow=AsyncMock(side_effect=[selected, None]))
    lookup = AsyncMock(side_effect=[None, existing_receipt_by_field] if raced else [existing_receipt_by_field])
    monkeypatch.setattr(publication, "existing_publication_request", lookup)
    monkeypatch.setattr(publication, "validate_fhir_catalog_snapshot_record", AsyncMock())
    authorize = AsyncMock(
        return_value={
            "authorized": True,
            "operation": publication.OPERATION,
            "command_sha256": hashlib.sha256(publication.publication_json(command_by_field).encode()).hexdigest(),
            "standing_grant_sha256": command_by_field["standing_grant_sha256"],
        }
    )
    assert (
        await publication.enqueue_fhir_catalog_publication(
            connection, command_by_field, authorize_metadata=authorize, control_schema="catalog_control"
        )
        is existing_receipt_by_field
    )
    authorize.assert_awaited_once()
    assert lookup.await_count == 1 + int(raced)
    assert connection.fetchrow.await_count == 1 + int(raced)


@pytest.mark.asyncio
async def test_existing_queue_receipt_does_not_bypass_current_command_authorization(monkeypatch):
    selected, command_by_field, _record = service_request()
    connection = Mock(is_in_transaction=Mock(return_value=True), fetchrow=AsyncMock(return_value=selected))
    lookup = AsyncMock(return_value={"state": "completed"})
    monkeypatch.setattr(publication, "existing_publication_request", lookup)
    monkeypatch.setattr(publication, "validate_fhir_catalog_snapshot_record", AsyncMock())
    with pytest.raises(ValueError, match="authority_invalid"):
        await publication.enqueue_fhir_catalog_publication(
            connection,
            command_by_field,
            authorize_metadata=AsyncMock(return_value={"authorized": True}),
            control_schema="catalog_control",
        )
    lookup.assert_not_awaited()
    assert connection.fetchrow.await_count == 1


@pytest.mark.parametrize("state", ["queued", "running", "rejected", "completed"])
def test_service_request_status_validates_each_durable_terminal_shape(state):
    _selected, command_by_field, record_by_field = service_request()
    record_by_field["state"] = state
    if state == "rejected":
        record_by_field["result_json"] = {"error": {"code": "registry_publication_rejected"}}
    if state == "completed":
        record_by_field["result_json"] = {
            "operation": publication.OPERATION,
            "metadata_generation": command_by_field["metadata_generation"],
            "generation": "9",
            "memberships_reused": True,
            "addresses_reused": True,
            "canonical_entities_deleted": 0,
            "canonical_entities_archived": 0,
        }
    assert publication.validated_fhir_catalog_request_status(record_by_field) == command_by_field
    if state == "completed":
        record_by_field["result_json"]["canonical_entities_deleted"] = False
        with pytest.raises(ValueError, match="result_invalid"):
            publication.validated_fhir_catalog_request_status(record_by_field)


@pytest.mark.parametrize("callback", ["authorize_metadata", "verify_sources", "source_fence"])
def test_publication_runtime_requires_every_callback_before_any_queue_work(callback):
    callbacks_by_name = {name: AsyncMock() for name in ("authorize_metadata", "verify_sources", "source_fence")}
    callbacks_by_name[callback] = None
    with pytest.raises(ValueError, match="registry_fhir_catalog_runtime_invalid"):
        publication.FHIRCatalogPublicationRuntime(**callbacks_by_name)


@pytest.mark.parametrize(
    "field,value",
    [
        ("operation", "other_operation"),
        ("metadata_generation", "a" * 63),
        ("standing_grant_sha256", "A" * 64),
        ("expected_metadata_generation", "invalid"),
        ("expected_serving_generation", True),
        ("expected_approved_revision", -1),
        ("unexpected", True),
    ],
)
def test_closed_publication_command_rejects_substituted_pins_and_extra_authority(field, value):
    _selected, command_by_field, _record = service_request()
    command_by_field[field] = value
    with pytest.raises(ValueError, match="registry_fhir_catalog_command_invalid"):
        publication.validated_fhir_catalog_command(command_by_field)


@pytest.mark.asyncio
async def test_metadata_enqueue_requires_outer_transaction_before_authorization():
    _selected, command_by_field, _record = service_request()
    connection = Mock(is_in_transaction=Mock(return_value=False), fetchrow=AsyncMock())
    authorize = AsyncMock()
    with pytest.raises(ValueError, match="registry_fhir_catalog_transaction_required"):
        await publication.enqueue_fhir_catalog_publication(
            connection, command_by_field, authorize_metadata=authorize, control_schema="catalog_control"
        )
    authorize.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_metadata_authority_must_be_a_callback_not_a_precomputed_receipt():
    selected, command_by_field, _record = service_request()
    with pytest.raises(ValueError, match="registry_fhir_catalog_authority_required"):
        await publication._authority({"authorized": True}, command_by_field, selected, object())


@pytest.mark.parametrize(
    "field,value",
    [
        ("session_token_sha256", "a" * 64),
        ("idempotency_key", "another-request"),
        ("actor_json", {"kind": "human"}),
        ("actor_key", "another-actor"),
        ("request_sha256", "f" * 64),
    ],
)
def test_durable_metadata_request_cannot_rebind_actor_or_idempotency_identity(field, value):
    _selected, _command, record_by_field = service_request()
    record_by_field[field] = value
    with pytest.raises(ValueError, match="registry_fhir_catalog_request_invalid"):
        publication.validated_fhir_catalog_request_status(record_by_field)


@pytest.mark.parametrize("state", ["completed", "rejected", "queued", "unknown"])
def test_metadata_status_rejects_results_that_do_not_match_durable_state(state):
    _selected, command_by_field, record_by_field = service_request()
    record_by_field["state"] = state
    if state == "queued":
        record_by_field["result_json"] = publication._result(command_by_field)
    elif state == "rejected":
        record_by_field["result_json"] = {"error": {"code": "unrecognized_rejection"}}
    with pytest.raises(ValueError, match="registry_fhir_catalog_result_invalid"):
        publication.validated_fhir_catalog_request_status(record_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "network_generation", "metadata_approved_revision", "standing_grant_sha256"])
async def test_enqueue_refuses_missing_or_changed_closed_snapshot_before_authorizing(monkeypatch, changed):
    selected, command_by_field, _record = service_request()
    if changed is None:
        selected = None
    else:
        selected[changed] = "changed"
    connection = Mock(is_in_transaction=Mock(return_value=True), fetchrow=AsyncMock(return_value=selected))
    verify = AsyncMock()
    authorize = AsyncMock()
    monkeypatch.setattr(publication, "validate_fhir_catalog_snapshot_record", verify)
    with pytest.raises(publication.RegistryApprovalConflict, match="registry_fhir_catalog_snapshot_changed"):
        await publication.enqueue_fhir_catalog_publication(
            connection, command_by_field, authorize_metadata=authorize, control_schema="catalog_control"
        )
    authorize.assert_not_awaited()
    verify.assert_not_awaited()


@pytest.mark.asyncio
async def test_same_serving_generation_cannot_hide_a_changed_manifest_fingerprint(monkeypatch):
    selected, command_by_field, _record = service_request()
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            return_value={"generation_id": command_by_field["expected_serving_generation"], "metadata_generation": None}
        )
    )
    monkeypatch.setattr(publication, "lock_publication_controls", AsyncMock(return_value=(6, None)))
    monkeypatch.setattr(
        publication,
        "resolve_network_serving_manifest",
        AsyncMock(return_value=SimpleNamespace(manifest_sha256="f" * 64)),
    )
    reuse = AsyncMock()
    monkeypatch.setattr(publication, "require_fhir_catalog_membership_reuse", reuse)
    with pytest.raises(publication.RegistryApprovalConflict, match="registry_fhir_catalog_source_changed"):
        await publication._compare_head(connection, '"catalog_control"', command_by_field, selected)
    reuse.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("outer_transaction", [False, True])
async def test_completion_requires_fresh_source_fence_outside_any_prior_transaction(outer_transaction):
    connection = Mock(is_in_transaction=Mock(return_value=outer_transaction), transaction=Mock())
    fence = AsyncMock() if outer_transaction else None
    with pytest.raises(ValueError, match="registry_fhir_catalog_source_fence_required"):
        await publication.complete_queued_fhir_catalog_publication(
            connection,
            uuid4(),
            uuid4(),
            authorize_metadata=AsyncMock(),
            verify_sources=AsyncMock(),
            source_fence=fence,
            control_schema="catalog_control",
        )
    connection.transaction.assert_not_called()


@pytest.mark.parametrize("encoded", [None, "", " " * (1048576 + 1)])
def test_server_configuration_refuses_missing_or_oversized_document_before_decoding(encoded):
    from process import registry_fhir_catalog_worker as control

    with pytest.raises(ValueError, match="registry_fhir_catalog_sync_configuration_invalid"):
        control.parse_fhir_catalog_sync_config(encoded)


@pytest.mark.parametrize(
    "field,value",
    [
        ("reader_roles", []),
        ("reader_roles", ["reader_b", "reader_a"]),
        ("reader_roles", ["reader_a", "reader_a"]),
        ("operation_seconds", True),
        ("operation_seconds", 59),
        ("operation_seconds", 86401),
    ],
)
def test_server_configuration_keeps_canonical_reader_set_and_bounded_deadline(field, value):
    from process import registry_fhir_catalog_worker as control
    from tests.test_registry_fhir_catalog_worker_startup import _configuration

    configured = _configuration(source_reader=True)
    configured[field] = value
    with pytest.raises(ValueError, match="registry_fhir_catalog_sync_configuration_invalid"):
        control.parse_fhir_catalog_sync_config(json.dumps(configured))


@pytest.mark.asyncio
@pytest.mark.parametrize("run_id", ["", " padded", "padded ", "x" * 129])
async def test_control_request_refuses_noncanonical_run_identity_before_configuration(monkeypatch, run_id):
    from process import registry_fhir_catalog_worker as control

    parse_config = Mock(side_effect=AssertionError("invalid request must not read credentials or configuration"))
    monkeypatch.setattr(control, "parse_fhir_catalog_sync_config", parse_config)
    with pytest.raises(ValueError, match="registry_fhir_catalog_sync_parameters_invalid"):
        await control.control_start({}, {"run_id": run_id})
    parse_config.assert_not_called()


def preparation_case():
    from process.registry_fhir_catalog_custody import FHIRCatalogTarget
    from process.registry_fhir_catalog_store import named_catalog_preparation_roles
    from tests.test_registry_fhir_catalog_grant import snapshot, standing_grant

    grant = standing_grant()
    candidate = uuid4()
    target = FHIRCatalogTarget(*(str(uuid4()) for _ in range(3)), str(candidate), "fhir_catalog_" + candidate.hex)
    selected = snapshot(grant, schema_name=target.schema_name)
    selected["schema_name"] = target.schema_name
    roles = named_catalog_preparation_roles(
        {
            "namespace_owner_role": "catalog_frozen",
            "publisher_role": "catalog_validator",
            "application_roles": ["catalog_execution", "catalog_loader", "catalog_reader", "catalog_validator"],
        }
    )
    return target, selected, SimpleNamespace(grant=grant, preparation_roles=roles)


class PreparationConnection:
    def __init__(self):
        self.execute = AsyncMock()
        self.active = False

    def is_in_transaction(self):
        return self.active

    @asynccontextmanager
    async def transaction(self, **_options):
        self.active = True
        try:
            yield
        finally:
            self.active = False


@pytest.mark.parametrize("initial", [{}, {"custody_json": None}])
def test_fresh_catalog_snapshot_has_no_preparation_checkpoint(initial):
    from process.registry_fhir_catalog_store import catalog_snapshot_preparation

    assert catalog_snapshot_preparation(initial) is None


async def test_catalog_checkpoint_preserves_exact_candidate_and_bounded_resume_data():
    from process import registry_fhir_catalog_store as store

    target, selected, config = preparation_case()
    connection = PreparationConnection()
    accounting_by_phase = {"copied_rows": 7}
    async with connection.transaction():
        await store.checkpoint_fhir_catalog_snapshot(
            connection, target, selected, None, accounting_by_phase, control_schema=config.grant.control_schema
        )
    accounting_by_phase["transient_status"] = "waiting"
    handoff = store.named_catalog_preparation(selected, config.grant.control_schema)
    assert selected["accounting_json"] == {}
    assert handoff["candidate_id"] == target.candidate_id
    assert store.catalog_snapshot_preparation(selected)["accounting"] == {"copied_rows": 7}
    assert "accounting" not in handoff["snapshot"]
    assert {table["schema"] for table in handoff["tables"]} == {target.schema_name}
    assert len(handoff["tables"]) == 5
    connection.fetchrow = AsyncMock(return_value=selected)
    assert (
        await store.load_named_catalog_preparation(
            connection, target.candidate_id, control_schema=config.grant.control_schema
        )
        == handoff
    )
    query, name = connection.fetchrow.await_args.args
    assert "WHERE schema_name=$1" in query and name == target.schema_name
    assert len(json.dumps(handoff).encode()) < 262144


async def test_catalog_admission_reuses_saved_run_scope_and_publication_fence(monkeypatch):
    from pathlib import Path

    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[2] / "source"))
    import directory_catalog_runtime

    from process import registry_fhir_catalog_store as store

    copy_target, selected, config = preparation_case()
    connection = PreparationConnection()
    async with connection.transaction():
        await store.checkpoint_fhir_catalog_snapshot(
            connection, copy_target, selected, None, {}, control_schema=config.grant.control_schema
        )
    named_store = SimpleNamespace(admit_named=AsyncMock(return_value=({"state": "handed_off"}, None)))
    constructor = Mock(return_value=named_store)
    monkeypatch.setattr(directory_catalog_runtime, "NamedPreparationStore", constructor)
    for _ in range(2):
        await store.admit_catalog_preparation(
            connection, selected, control_schema=config.grant.control_schema, roles=config.preparation_roles
        )
    first, second = named_store.admit_named.await_args_list
    assert first == second
    request, handoff = first.args
    assert request.run_id == request.generation_id == request.publication_fence == copy_target.candidate_id
    assert request.node_id == config.grant.control_schema
    assert request.dataset_key == selected["metadata_generation"]
    assert request.idempotency_key == "catalog:" + selected["metadata_generation"]
    assert request.request_sha256 == hashlib.sha256(worker.canonical_json(handoff).encode()).hexdigest()
    assert first.kwargs == {"attempt_id": copy_target.candidate_id}


@pytest.mark.parametrize("frozen", [False, True])
@pytest.mark.parametrize("validation_failure", [None, asyncio.CancelledError])
async def test_catalog_waits_for_freeze_then_records_only_completed_data_validation(
    monkeypatch, frozen, validation_failure
):
    """Wait for freeze and persist only successful data validation, preserving cancellation."""
    from process import registry_fhir_catalog_store as store

    copy_target, selected, config = preparation_case()
    connection = PreparationConnection()
    await _checkpoint_catalog_candidate(connection, copy_target, selected, config)

    @asynccontextmanager
    async def acquire():
        yield connection

    @asynccontextmanager
    async def workspace(_connection):
        yield object()

    @asynccontextmanager
    async def reads(*_connections):
        yield

    claim = object() if frozen else None
    preparation_by_field = {"state": "frozen" if frozen else "handed_off"}
    named_store = SimpleNamespace(validate_named=AsyncMock(), yield_named_claim=AsyncMock())
    admit = AsyncMock(return_value=(named_store, preparation_by_field, claim))
    receipt_by_field = {
        "metadata_generation": selected["metadata_generation"],
        "inputs": {},
        "tables": {"registry_fhir_catalog_source": {"rows": 2, "sha256": "b" * 64}},
        "content_sha256": "c" * 64,
    }
    validate = AsyncMock(return_value=receipt_by_field, side_effect=validation_failure)
    monkeypatch.setattr(worker, "admit_catalog_preparation", admit)
    monkeypatch.setattr(worker, "fhir_catalog_reference_workspace", workspace)
    monkeypatch.setattr(worker, "fhir_catalog_registry_reads", reads)
    monkeypatch.setattr(worker, "build_fhir_catalog_observation_verifier", Mock(return_value=AsyncMock()))
    monkeypatch.setattr(worker, "validate_fhir_catalog_snapshot_data", validate)
    if frozen and validation_failure:
        with pytest.raises(validation_failure):
            await worker._seal_catalog_snapshot(config, copy_target, selected, SimpleNamespace(acquire=acquire))
        named_store.validate_named.assert_not_awaited()
        connection.execute.assert_not_awaited()
        assert admit.await_count == 1
    else:
        state = await worker._seal_catalog_snapshot(config, copy_target, selected, SimpleNamespace(acquire=acquire))
        assert state == ("validated" if frozen else "handed_off")
        assert validate.await_count == int(frozen)
        assert named_store.validate_named.await_count == int(frozen)
        assert connection.execute.await_count == int(frozen)
        assert admit.await_count == (2 if frozen else 1)
        if frozen:
            assert (
                named_store.validate_named.await_args.args[1]["data"]["content_sha256"]
                == receipt_by_field["content_sha256"]
            )
            assert store.catalog_snapshot_preparation(selected)["target"]["candidate_id"] == copy_target.candidate_id


@pytest.mark.parametrize("failure", [None, RuntimeError])
async def test_catalog_preparation_publication_shares_the_pointer_transaction(monkeypatch, failure):
    """Commit the preparation and pointer together, or roll both back on publication failure."""
    copy_target, selected, config = preparation_case()
    connection = PreparationConnection()
    await _checkpoint_catalog_candidate(connection, copy_target, selected, config)
    _snapshot, command, request_record = service_request()
    command["metadata_generation"] = selected["metadata_generation"]
    events = []

    @asynccontextmanager
    async def locked(*_arguments):
        events.append("begin")
        try:
            yield request_record
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    async def complete(_claim, result):
        assert len(connection.execute.await_args_list) == 2
        assert result["data"]["metadata_generation"] == selected["metadata_generation"]
        events.append("preparation")
        if failure:
            raise failure("publication failed")

    named_store = SimpleNamespace(publish_named=AsyncMock(side_effect=complete))
    monkeypatch.setattr(publication, "locked_publication_request_transaction", locked)
    monkeypatch.setattr(publication, "_validated_request", Mock(return_value=command))
    monkeypatch.setattr(publication, "_snapshot", AsyncMock(return_value=selected))
    monkeypatch.setattr(publication, "_authority", AsyncMock())
    monkeypatch.setattr(publication, "_compare_head", AsyncMock())
    monkeypatch.setattr(
        publication,
        "admit_catalog_preparation",
        AsyncMock(
            return_value=(named_store, {"state": "validated", "publication_fence": copy_target.candidate_id}, object())
        ),
    )
    finish = AsyncMock()
    monkeypatch.setattr(publication, "finish_registry_publication_request", finish)
    publication_options_by_name = dict(
        authorize_metadata=AsyncMock(),
        verify_sources=AsyncMock(return_value=True),
        control_schema=config.grant.control_schema,
        preparation_roles=config.preparation_roles,
    )
    if failure:
        with pytest.raises(failure):
            await publication._complete_fenced(connection, uuid4(), uuid4(), **publication_options_by_name)
        finish.assert_not_awaited()
        assert events == ["begin", "preparation", "rollback"]
    else:
        await publication._complete_fenced(connection, uuid4(), uuid4(), **publication_options_by_name)
        finish.assert_awaited_once()
        assert events == ["begin", "preparation", "commit"]


async def _checkpoint_catalog_candidate(connection, copy_target, selected, config):
    """Complete and clear the candidate-loading setup before publication assertions."""
    from process import registry_fhir_catalog_store as store

    async with connection.transaction():
        await store.checkpoint_fhir_catalog_snapshot(
            connection, copy_target, selected, None, {}, control_schema=config.grant.control_schema
        )
    connection.execute.reset_mock()


async def test_catalog_administrative_setup_separates_candidate_ddl_and_validator_control_grants():
    from dataclasses import replace

    from process.registry_fhir_catalog_permissions import install_fhir_catalog_preparation_permissions

    connection = PreparationConnection()
    connection.fetchval = AsyncMock(return_value='"example_database"')
    runtime = FHIRCatalogPublicationRuntime(AsyncMock(), AsyncMock(), AsyncMock())
    consumer = RegistryPublicationConsumerConfig(
        "catalog_control",
        "catalog_lifecycle",
        "catalog_execution",
        "catalog_reader",
        "catalog_queue",
        "catalog_frozen",
        loader_roles=("catalog_loader",),
        reader_roles=("catalog_reader",),
    )
    consumer = replace(consumer, metadata_runtime=runtime, metadata_only=True)
    async with connection.transaction():
        await install_fhir_catalog_preparation_permissions(
            connection, consumer, loader_role="catalog_loader", sealer_role="catalog_validator"
        )
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert 'REVOKE "catalog_loader" FROM "catalog_validator"' in statements
    assert 'GRANT CREATE ON DATABASE "example_database" TO "catalog_loader"' in statements
    assert 'REVOKE CREATE ON DATABASE "example_database" FROM "catalog_validator"' in statements
    assert any(
        "GRANT UPDATE(custody_json,accounting_json)" in sql and 'TO "catalog_validator"' in sql for sql in statements
    )
    control_grants = [
        sql
        for sql in statements
        if sql.startswith("GRANT SELECT,INSERT(") and " ON hp_snapshot_retention.preparation TO " in sql
    ]
    assert len(control_grants) == 1
    assert 'TO "catalog_execution","catalog_validator"' in control_grants[0]
    assert "frozen_inventory" not in control_grants[0] and "frozen_sha256" not in control_grants[0]
    assert "INSERT(preparation_id," in control_grants[0] and "UPDATE(state," in control_grants[0]
    assert any(
        sql.startswith("REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON hp_snapshot_retention.preparation")
        for sql in statements
    )
    assert any(
        sql.startswith("REVOKE INSERT(") and "frozen_inventory,frozen_sha256" in sql and "REFERENCES(" in sql
        for sql in statements
    )
    assert any('REVOKE "catalog_frozen" FROM ' in sql for sql in statements)


@pytest.mark.parametrize("checkpointed", [False, True])
async def test_catalog_cancellation_retains_a_durable_handoff_and_cleans_only_unregistered_candidate(
    monkeypatch, checkpointed
):
    target, _selected, config = preparation_case()
    config.loader_role = "catalog_loader"
    connection = PreparationConnection()
    canceled = asyncio.CancelledError()

    async def prepare(_config, _loader, _source, _sealer, staged, **_options):
        staged.update(target=target, checkpointed=checkpointed)
        raise canceled

    discard = AsyncMock()
    monkeypatch.setattr(worker, "_prepare_fhir_catalog_snapshot", prepare)
    monkeypatch.setattr(worker, "discard_fhir_catalog_candidate", discard)
    with pytest.raises(asyncio.CancelledError) as caught:
        await worker.prepare_fhir_catalog_snapshot(config, connection, None, None)
    assert caught.value is canceled
    assert discard.await_count == int(not checkpointed)


def test_operator_role_profile_uses_names_without_worker_credentials():
    from process.registry_fhir_catalog_store import named_catalog_preparation_roles
    from process.registry_fhir_catalog_worker import parse_fhir_catalog_sync_config
    from tests.test_registry_fhir_catalog_worker_startup import _configuration

    configured = _configuration(source_reader=True)
    parsed = parse_fhir_catalog_sync_config(json.dumps(configured))
    roles = named_catalog_preparation_roles(parsed)
    roles["application_roles"] = sorted({*roles["application_roles"], "source_loader", "source_runtime"})
    configured["preparation_roles"] = roles
    assert parse_fhir_catalog_sync_config(json.dumps(configured)).preparation_roles == roles
    assert named_catalog_preparation_roles(roles) == roles
    configured["preparation_roles"] = {**roles, "publisher_role": "another_publisher"}
    with pytest.raises(ValueError, match="configuration_invalid"):
        parse_fhir_catalog_sync_config(json.dumps(configured))


@pytest.mark.parametrize("changed", [None, "mapping_sha256", "source_vector_json", "previous_generation"])
async def test_catalog_pending_resume_preserves_original_date_and_compares_build_inputs(changed):
    from datetime import date

    from process import registry_fhir_catalog_store as store

    copy_target, selected, config = preparation_case()
    connection = PreparationConnection()
    async with connection.transaction():
        await store.checkpoint_fhir_catalog_snapshot(
            connection, copy_target, selected, None, {"written": 1}, control_schema=config.grant.control_schema
        )
    proposed_by_field = {**selected, "as_of": date(2026, 1, 2)}
    if changed == "mapping_sha256":
        proposed_by_field[changed] = "f" * 64
    elif changed == "source_vector_json":
        proposed_by_field[changed] = [{"source_id": "alias-a", "source_generation": "f" * 64, "status": "current"}]
    proposed_by_field["metadata_generation"] = worker.metadata_generation(proposed_by_field)
    connection.fetchrow = AsyncMock(side_effect=[None, selected])
    retained_snapshot = await worker._reusable_catalog_snapshot(
        connection,
        config.grant,
        proposed_by_field["metadata_generation"],
        expected_snapshot=proposed_by_field,
        previous_generation="e" * 64 if changed == "previous_generation" else None,
    )
    assert retained_snapshot == (selected if changed is None else None)
    assert selected["as_of"] == date(2026, 1, 1)
    assert connection.fetchrow.await_count == 2
    query, policy = connection.fetchrow.await_args.args
    assert "LIMIT 1" in query and "published_at IS NULL" in query and policy == config.grant.sha256
