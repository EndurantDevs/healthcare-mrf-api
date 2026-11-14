# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable named preparation, complete data validation and exact queue resume."""

import asyncio
import hashlib
import json
import sys
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_membership_publication as publication
from process import network_membership_validation as validation
from process import registry_publication_consumer as consumer
from process import registry_publication_worker as worker
from process.network_membership_copy import MembershipCopyTarget
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_publication_queue import release_registry_publication_request

TARGET = MembershipCopyTarget(*(str(UUID(int=value)) for value in range(1, 5)), "network_candidate_" + UUID(int=4).hex)
ROLES = {
    "namespace_owner_role": "frozen_owner",
    "publisher_role": "publisher",
    "application_roles": ["publisher", "reader"],
}
COUNTS = {
    field: 0
    for field in (
        "raw_rows",
        "batch_rows",
        "batch_count",
        "membership_rows",
        "distinct_memberships",
        "projected_locations",
        "orphan_bindings",
        "unknown_network_rows",
    )
}


def candidate():
    candidate_by_field = {
        field: getattr(TARGET, field)
        for field in (
            "dataset_id",
            "schema_id",
            "producer_id",
            "candidate_id",
            "schema_name",
        )
    }
    return candidate_by_field | {
        "state": "ready",
        "source_generations": {"edition": "saved-edition"},
        "validation_json": COUNTS,
    }


def completed_validation():
    return {
        "candidate_id": TARGET.candidate_id,
        "schema_name": TARGET.schema_name,
        "complete": True,
        "source_generations": {"edition": "saved-edition"},
        "counts": COUNTS,
        "semantic_validation": None,
    }


def test_manifest_digest_uses_canonical_projection_counts():
    scope_by_field = {
        "candidate_id": TARGET.candidate_id,
        "schema_revision": 1,
        "source_generations": {"unified_address": "address-edition"},
        "approved_custom_revision": 0,
    }
    canonical_by_field = {"membership_rows": 1, "distinct_memberships": 1, "projected_locations": 1}
    serving_by_field = {"index_profile": "serving"}
    tables_by_field = {"schema_name": TARGET.schema_name, "tables": ["entity_address_unified"]}
    digest = publication._manifest_digest(1, scope_by_field, canonical_by_field, serving_by_field, tables_by_field)
    assert len(digest) == 64
    changed_counts_by_field = {**canonical_by_field, "membership_rows": 2}
    assert digest != publication._manifest_digest(
        1, scope_by_field, changed_counts_by_field, serving_by_field, tables_by_field
    )


def _composition_connection():
    from tests.test_registry_ptg_address_carry import _artifact

    composition = _artifact()
    candidate_by_field = candidate() | {
        "schema_revision": 1,
        "expected_head": 0,
        "approved_custom_revision": composition.approved_revision,
        "expected_rows": 0,
        "accepted_rows": 0,
        "source_generations": {"unified_address": composition.generation_sha256},
        "validation_json": COUNTS | {"address_source": asdict(composition.address_source)},
    }
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=candidate_by_field),
        fetchval=AsyncMock(return_value=json.dumps(composition.as_dict())),
        fetch=AsyncMock(
            side_effect=[
                [
                    {"relname": name}
                    for name in ("entity_address_unified", "network_membership", "provider_location_binding")
                ],
                [{"relname": name} for name in ("entity_address_unified", "provider_location_binding")],
            ]
        ),
    )
    return connection, composition


@pytest.mark.parametrize("approval_delta", [0, 1])
async def test_ready_handoff_includes_recorded_address_composition_in_atomic_freeze(approval_delta):
    connection, composition = _composition_connection()
    connection.fetchrow.return_value["approved_custom_revision"] += approval_delta
    document = await publication.load_named_network_preparation(
        connection, TARGET.candidate_id, control_schema=composition.control_schema
    )
    assert document["tables"][-2:] == [
        {"schema": composition.address_source.schema_name, "name": name}
        for name in ("entity_address_unified", "provider_location_binding")
    ]
    assert document["snapshot"]["address_composition"] == composition.as_dict()
    assert document["snapshot"]["approved_custom_revision"] == composition.approved_revision + approval_delta
    assert document["snapshot"]["address_composition"]["approved_revision"] == composition.approved_revision
    assert {table["schema"] for table in document["tables"]} == {
        TARGET.schema_name,
        composition.address_source.schema_name,
    }


@pytest.mark.parametrize("changed", ["metadata", "control", "generation", "approval", "extra", "missing"])
async def test_ready_handoff_refuses_unbound_or_incomplete_address_composition(changed):
    connection, composition = _composition_connection()
    control_schema = composition.control_schema
    if changed == "metadata":
        connection.fetchval.return_value = "{}"
    elif changed == "control":
        control_schema = "other_control"
    elif changed == "generation":
        connection.fetchrow.return_value["source_generations"]["unified_address"] = "f" * 64
    elif changed == "approval":
        connection.fetchrow.return_value["approved_custom_revision"] -= 1
    else:
        tables = ["entity_address_unified", "provider_location_binding", "unexpected"]
        tables = tables[:1] if changed == "missing" else tables
        connection.fetch.side_effect = [
            [{"relname": name} for name in ("network_membership", "provider_location_binding")],
            [{"relname": name} for name in tables],
        ]
    with pytest.raises(publication.NetworkPublicationError):
        await publication.load_named_network_preparation(connection, TARGET.candidate_id, control_schema=control_schema)


async def test_ready_handoff_keeps_other_retained_named_inputs_outside_the_generation_freeze():
    connection, composition = _composition_connection()
    connection.fetchrow.return_value["validation_json"]["address_source"]["schema_name"] = "retained_source"
    document = await publication.load_named_network_preparation(
        connection, TARGET.candidate_id, control_schema=composition.control_schema
    )
    assert {table["schema"] for table in document["tables"]} == {TARGET.schema_name}
    assert "address_composition" not in document["snapshot"]
    connection.fetchval.assert_not_awaited()
    connection.fetch.assert_awaited_once()


class Connection:
    def __init__(self):
        self.active = False
        self.phases = []
        self.fetchrow = AsyncMock(return_value=candidate())
        self.execute = AsyncMock(return_value="UPDATE 1")

    def is_in_transaction(self):
        return self.active

    @asynccontextmanager
    async def transaction(self, **options):
        assert not self.active
        self.active = True
        self.phases.append(options)
        try:
            yield
        finally:
            self.active = False


async def test_named_admission_uses_the_existing_store_and_portable_request(monkeypatch):
    handoff_by_field = {"candidate_id": TARGET.candidate_id, "snapshot": {"edition": "saved-é"}}
    monkeypatch.setattr(publication, "load_named_network_preparation", AsyncMock(return_value=handoff_by_field))
    store = SimpleNamespace(admit_named=AsyncMock(return_value=({"state": "handed_off"}, None)))
    factory = lambda connection, roles, *, kind: store
    monkeypatch.setitem(
        sys.modules,
        "directory_catalog_runtime",
        SimpleNamespace(
            NamedPreparationStore=factory,
            NativePreparationAdmission=lambda **values: SimpleNamespace(**values),
        ),
    )
    connection = Connection()
    async with connection.transaction():
        actual, preparation_by_field, claim = await publication.admit_network_preparation(
            connection,
            TARGET.candidate_id,
            control_schema="registry",
            roles=ROLES,
        )
    assert actual is store and preparation_by_field["state"] == "handed_off" and claim is None
    request = store.admit_named.await_args.args[0]
    assert request.run_id == request.generation_id == request.preparation_id == TARGET.candidate_id
    assert request.importer_id == "network" and request.node_id == "registry"
    assert (
        request.request_sha256
        == hashlib.sha256(
            json.dumps(
                handoff_by_field,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
        ).hexdigest()
    )
    assert store.admit_named.await_args.kwargs == {"attempt_id": TARGET.candidate_id}


async def test_handoff_waits_before_any_data_scan_or_control_write(monkeypatch):
    monkeypatch.setattr(
        publication,
        "admit_network_preparation",
        AsyncMock(
            return_value=(object(), {"state": "handed_off"}, None),
        ),
    )
    scan = AsyncMock()
    monkeypatch.setattr(validation, "validate_network_candidate_final", scan)
    connection = Connection()
    with pytest.raises(publication.NetworkPreparationPending) as pending:
        await publication.validate_named_network_preparation(connection, TARGET, control_schema="registry", roles=ROLES)
    assert pending.value.candidate_id == TARGET.candidate_id
    scan.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()
    connection.execute.assert_not_awaited()
    assert connection.phases == [{}] and not connection.active


@pytest.mark.parametrize("failure", [None, ValueError("invalid source rows"), asyncio.CancelledError()])
async def test_final_data_scan_precedes_short_claim_and_report_update(monkeypatch, failure):
    store = SimpleNamespace(yield_named_claim=AsyncMock(), validate_named=AsyncMock())
    claim = object()
    admission = AsyncMock(return_value=(store, {"state": "frozen"}, claim))
    monkeypatch.setattr(publication, "admit_network_preparation", admission)
    proof = completed_validation()
    scan = AsyncMock(return_value=proof, side_effect=failure)
    monkeypatch.setattr(validation, "validate_network_candidate_final", scan)
    connection = Connection()
    if failure is None:
        validation_result = await publication.validate_named_network_preparation(
            connection,
            TARGET,
            control_schema="registry",
            roles=ROLES,
        )
        assert validation_result == {"state": "validated", "candidate_id": TARGET.candidate_id}
        assert connection.phases == [{}, {"isolation": "repeatable_read"}, {}]
        assert admission.await_count == store.yield_named_claim.await_count == 2
        store.validate_named.assert_awaited_once_with(
            claim,
            {
                "kind": "network",
                "candidate_id": TARGET.candidate_id,
                "data": publication._network_validation_reference(proof),
            },
        )
        query = connection.execute.await_args.args[0]
        assert "validation_json=jsonb_set" in query and "state='ready'" in query
        assert "serving_control" not in query
    else:
        with pytest.raises(type(failure)):
            await publication.validate_named_network_preparation(
                connection,
                TARGET,
                control_schema="registry",
                roles=ROLES,
            )
        assert admission.await_count == store.yield_named_claim.await_count == 1
        store.validate_named.assert_not_awaited()
        connection.execute.assert_not_awaited()
    assert not connection.active


@pytest.mark.parametrize("changed", [False, True])
async def test_validated_resume_checks_saved_result_without_repeating_data_work(monkeypatch, changed):
    proof = completed_validation()
    stored = proof | ({"complete": False} if changed else {})
    store = SimpleNamespace(yield_named_claim=AsyncMock())
    monkeypatch.setattr(
        publication,
        "admit_network_preparation",
        AsyncMock(
            return_value=(
                store,
                {
                    "state": "validated",
                    "validation": {"evidence": {"data": publication._network_validation_reference(stored)}},
                },
                object(),
            )
        ),
    )
    scan = AsyncMock()
    monkeypatch.setattr(validation, "validate_network_candidate_final", scan)
    connection = Connection()
    connection.fetchrow.return_value["validation_json"] = COUNTS | {"final_validation": proof}
    if changed:
        with pytest.raises(publication.NetworkPublicationError, match="validation changed"):
            await publication.validate_named_network_preparation(
                connection, TARGET, control_schema="registry", roles=ROLES
            )
    else:
        assert (
            await publication.validate_named_network_preparation(
                connection,
                TARGET,
                control_schema="registry",
                roles=ROLES,
            )
        )["state"] == "validated"
    scan.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("changed", [False, True])
async def test_queue_release_requires_the_original_live_lease(changed):
    request_id, lease_id = UUID(int=7), UUID(int=8)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(
            return_value=None if changed else request_id,
        ),
    )
    if changed:
        with pytest.raises(RegistryApprovalConflict, match="lease_conflict"):
            await release_registry_publication_request(connection, request_id, lease_id, control_schema="registry")
    else:
        await release_registry_publication_request(connection, request_id, lease_id, control_schema="registry")
    query, *parameters = connection.fetchval.await_args.args
    assert "state='queued'" in query and "lease_expires_at>clock_timestamp()" in query
    assert "command_json=" not in query and "session_token_sha256=" not in query
    assert parameters == [request_id, lease_id]


async def test_normal_consumer_returns_durable_pending_candidate_without_rejection(monkeypatch):
    request_id, lease_id = UUID(int=7), UUID(int=8)
    request_by_field = {"request_id": request_id, "lease_id": lease_id, "command_json": {}}
    monkeypatch.setattr(consumer, "_claim", AsyncMock(return_value=request_by_field))
    monkeypatch.setattr(
        consumer,
        "_run_with_heartbeat",
        AsyncMock(
            side_effect=publication.NetworkPreparationPending(TARGET.candidate_id),
        ),
    )
    reject = AsyncMock()
    monkeypatch.setattr(consumer, "_reject", reject)
    release = AsyncMock()
    monkeypatch.setattr(consumer, "release_registry_publication_request", release)
    connection = Connection()

    @asynccontextmanager
    async def acquire():
        yield connection

    lifecycle = SimpleNamespace(acquire=acquire)
    config = consumer.RegistryPublicationConsumerConfig(
        "registry",
        "lifecycle",
        "publisher",
        "api",
        "queue_owner",
        "builder",
    )
    pending_result = await consumer.consume_registry_publication_once(
        lifecycle_pool=lifecycle,
        execution_pool=object(),
        authorize=AsyncMock(),
        config=config,
    )
    release.assert_awaited_once_with(connection, request_id, lease_id, control_schema="registry")
    reject.assert_not_awaited()
    assert worker._summary(pending_result) == {
        "request_id": str(request_id),
        "state": "waiting_admin_freeze",
        "candidate_id": TARGET.candidate_id,
    }


@pytest.mark.parametrize("omit_group", [False, True])
def test_worker_preparation_profile_covers_configured_application_groups(omit_group):
    configured_by_field = {
        "lifecycle_dsn": "postgresql://lifecycle:{synthetic-password}@db.example.test/example",
        "execution_dsn": "postgresql://publisher:{synthetic-password}@db.example.test/example",
        "app_origin": "https://authority.example.test",
        "service_token": "synthetic-service-token",
        "consumer": {
            "control_schema": "registry",
            "lifecycle_role": "lifecycle",
            "execution_role": "publisher",
            "api_role": "api",
            "queue_owner_role": "queue_group",
            "physical_owner_role": "builder_group",
            "loader_roles": ["builder"],
            "reader_roles": ["reader"],
        },
        "preparation_roles": {
            "namespace_owner_role": "frozen_owner",
            "publisher_role": "publisher",
            "application_roles": ["lifecycle", "publisher", "api", "queue_group", "builder", "reader"],
        },
    }
    if omit_group:
        with pytest.raises(ValueError, match="config_invalid"):
            worker.parse_registry_publication_worker_config(json.dumps(configured_by_field))
    else:
        configured_by_field["preparation_roles"]["application_roles"].append("builder_group")
        parsed = worker.parse_registry_publication_worker_config(json.dumps(configured_by_field))
        assert parsed.preparation_roles["application_roles"] == sorted(
            configured_by_field["preparation_roles"]["application_roles"]
        )
        configured_by_field["preparation_roles"]["application_roles"].append("later")
        assert "later" not in parsed.preparation_roles["application_roles"]


async def test_final_publication_reauthorizes_after_validation_before_control_locks(monkeypatch):
    from process import registry_publication_execution as execution

    connection = Connection()
    request_by_field = {"request_sha256": "a" * 64, "command_json": {"expected_serving_generation": 0}}
    monkeypatch.setattr(execution, "_request", AsyncMock(return_value=request_by_field))
    events = []

    async def prepare(*arguments, **options):
        events.append("prepare")

    async def seal(target):
        assert target == TARGET and not connection.active
        events.append("validated")

    async def authorize(*arguments):
        assert not connection.active
        events.append("authorized")
        return 7

    async def controls(*arguments):
        assert connection.active
        events.append("controls")

    monkeypatch.setattr(execution, "prepare_registry_candidate_for_context", prepare)
    monkeypatch.setattr(execution, "_fresh_authority", authorize)
    monkeypatch.setattr(execution, "_lock_publication_controls", controls)
    monkeypatch.setattr(execution, "_locked_candidate", AsyncMock())
    monkeypatch.setattr(execution, "_approval", AsyncMock(return_value={"approved_revision": 1}))
    monkeypatch.setattr(execution, "_require_candidate_approval", lambda *arguments: None)
    finish = AsyncMock(return_value={"published": True})
    monkeypatch.setattr(execution, "_finish_approved_publication", finish)
    context = consumer.RegistryPublicationConsumerConfig(
        "registry",
        "lifecycle",
        "publisher",
        "api",
        "queue_owner",
        "builder",
        preparation_roles=ROLES,
    )
    publication_result = await execution.complete_queued_registry_publication(
        connection,
        UUID(int=7),
        UUID(int=8),
        TARGET,
        object(),
        authorize=object(),
        writer_context=context,
        seal_candidate=seal,
    )
    assert publication_result == {"published": True}
    assert events == ["prepare", "authorized", "validated", "authorized", "controls"]
    budget = connection.execute.await_args.args[0]
    assert "least(nullif(setting::bigint,0),budget)" in budget
    assert "('statement_timeout',3000)" in budget and "('lock_timeout',250)" in budget
    assert finish.await_args.args[-1] is context
    assert finish.await_args.kwargs == {}
    assert not connection.active
