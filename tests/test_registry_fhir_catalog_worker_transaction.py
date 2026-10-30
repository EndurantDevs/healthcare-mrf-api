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
async def test_worker_enqueue_owns_repeatable_read_before_metadata_authorization(monkeypatch, completed):
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
    operation_result = await worker._publish_catalog_snapshot(
        config, loader, snapshot_by_field, None, runtime, object(), object()
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

    async def reconcile(_config):
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


@pytest.mark.asyncio
@pytest.mark.parametrize("raced", [False, True])
async def test_authenticated_enqueue_reuses_an_existing_or_concurrently_inserted_request(monkeypatch, raced):
    selected, command_by_field, _record = service_request()
    existing_receipt_by_field = {"request_id": str(uuid4()), "state": "pending"}
    connection = Mock(is_in_transaction=Mock(return_value=True), fetchrow=AsyncMock(side_effect=[selected, None]))
    lookup = AsyncMock(side_effect=[None, existing_receipt_by_field] if raced else [existing_receipt_by_field])
    monkeypatch.setattr(publication, "existing_publication_request", lookup)
    monkeypatch.setattr(publication, "verify_fhir_catalog_snapshot", AsyncMock())
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
    monkeypatch.setattr(publication, "verify_fhir_catalog_snapshot", AsyncMock())
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
    monkeypatch.setattr(publication, "verify_fhir_catalog_snapshot", verify)
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
