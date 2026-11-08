# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Installed local rollback joins real authority interfaces without replacing payload IDs."""

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_snapshot_rollback_local_data as local
from process.ptg_parts.source_snapshot_rollback_types import PTG2SourceSnapshotRollbackConflict
from tests import ptg_source_snapshot_rollback_unit_support as fixtures
from tests.test_ptg2_physical_binding import _binding

TARGET = "snapshot-archive-aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
CURRENT = "snapshot-archive-bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
OWNER = "snapshot-sync:" + CURRENT.removeprefix("snapshot-archive-")
CONTROL_SCHEMA = "package_control_test"


def installed(snapshot_id, predecessor=None):
    binding = replace(_binding(), snapshot_id=snapshot_id)
    candidate_by_field = {
        **fixtures.target_snapshot(),
        "snapshot_id": snapshot_id,
        "snapshot_key": binding.destination_layout_key,
        "previous_snapshot_id": predecessor,
        "attested_source_key": fixtures.SOURCE_KEY,
    }
    authority_by_field = {"package_id": "a" * 64, "generation_id": "generation-a"}
    evidence_by_field = {"activation_evidence": {"source_key": fixtures.SOURCE_KEY}}
    return authority_by_field, evidence_by_field, binding, candidate_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "snapshot", "layout", "source", "attestation"])
async def test_installed_state_keeps_genuine_binding_and_refuses_crossed_identity(monkeypatch, drift):
    authority_by_field, evidence_by_field, binding, candidate_by_field = installed(TARGET)
    if drift == "snapshot":
        binding = replace(binding, snapshot_id=CURRENT)
    if drift == "layout":
        candidate_by_field["snapshot_key"] += 1
    if drift == "source":
        evidence_by_field["activation_evidence"]["source_key"] = "foreign"
    if drift == "attestation":
        candidate_by_field["attested_source_key"] = "foreign"
    resolve = AsyncMock(return_value=(authority_by_field, evidence_by_field, binding, candidate_by_field))
    producer = AsyncMock(return_value=("node-example", "cluster-example"))
    monkeypatch.setattr(local, "local_data_physical_read_state", resolve)
    monkeypatch.setattr(local, "_producer", producer)
    session = object()
    if drift:
        with pytest.raises(PTG2SourceSnapshotRollbackConflict, match="binding differs"):
            await local._installed_state(session, TARGET, fixtures.SOURCE_KEY, control_schema_name=CONTROL_SCHEMA)
        producer.assert_not_awaited()
    else:
        actual = await local._installed_state(session, TARGET, fixtures.SOURCE_KEY, control_schema_name=CONTROL_SCHEMA)
        assert actual[1] is binding and actual[2] is candidate_by_field
        assert binding.payload_snapshot_id != TARGET
        assert binding.payload_snapshot_key != candidate_by_field["snapshot_key"]
    resolve.assert_awaited_once_with(session, TARGET, is_prepared=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("is_readonly", [False, True])
@pytest.mark.parametrize("drift", [None, "digest", "source", "producer", "contract", "origin"])
async def test_producer_requires_exact_installed_package_digest(monkeypatch, drift, is_readonly):
    monkeypatch.setenv("HLTHPRT_SOURCE_ATTEMPT_SCHEMA", "unrelated_attempt_schema")
    manifest_by_field = {
        "producer_node_id": "node-example",
        "producer_cluster_id": "cluster-example",
        "importer_id": "ptg",
        "dataset_key": "ptg." + fixtures.SOURCE_KEY,
        "contract_version": "ptg_result.postgres.v2",
    }
    if drift == "source":
        manifest_by_field["dataset_key"] = "ptg.foreign"
    if drift == "producer":
        manifest_by_field["producer_node_id"] = ""
    if drift == "contract":
        manifest_by_field["contract_version"] = "ptg_result.postgres.v1"
    digest = hashlib.sha256(
        json.dumps(manifest_by_field, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()
    authority_by_field = {"package_id": digest, "manifest_sha256": digest}
    package_by_field = {
        "manifest": manifest_by_field,
        "manifest_sha256": "b" * 64 if drift == "digest" else digest,
        "origin_node_id": "foreign-node" if drift == "origin" else "node-example",
    }
    query = AsyncMock(return_value=package_by_field)
    monkeypatch.setattr(local.store, "_one", query)
    if drift:
        with pytest.raises(PTG2SourceSnapshotRollbackConflict, match="producer differs"):
            await local._producer(
                object(),
                authority_by_field,
                fixtures.SOURCE_KEY,
                control_schema_name=CONTROL_SCHEMA,
                is_readonly=is_readonly,
            )
    else:
        assert await local._producer(
            object(),
            authority_by_field,
            fixtures.SOURCE_KEY,
            control_schema_name=CONTROL_SCHEMA,
            is_readonly=is_readonly,
        ) == (
            "node-example",
            "cluster-example",
        )
    assert f'FROM "{CONTROL_SCHEMA}".snapshot_sync_package ' in query.await_args.args[1]
    assert ("FOR SHARE NOWAIT" in query.await_args.args[1]) is not is_readonly
    assert query.await_args.args[2] == {"package_id": digest}


@pytest.mark.asyncio
@pytest.mark.parametrize("schema_name", [None, "", "unsafe-name", "1unsafe", "bad.schema", "x" * 64, "Uppercase"])
async def test_producer_refuses_missing_or_unsafe_supplied_schema_before_query(monkeypatch, schema_name):
    query = AsyncMock()
    monkeypatch.setattr(local.store, "_one", query)
    with pytest.raises(ValueError, match="package control schema"):
        await local._producer(object(), {"package_id": "a" * 64}, fixtures.SOURCE_KEY, control_schema_name=schema_name)
    query.assert_not_awaited()


class Session:
    def __init__(self):
        self.events = []

    def in_transaction(self):
        return True

    @asynccontextmanager
    async def begin_nested(self):
        self.events.append("savepoint")
        try:
            yield
        except BaseException:
            self.events.append("rollback")
            raise
        else:
            self.events.append("release")


def _inspection_case(monkeypatch, drift):
    candidate_by_field = fixtures.target_snapshot()
    expected = installed(CURRENT, fixtures.TARGET_SNAPSHOT)
    expected_state = (expected[0], expected[2], expected[3], ("node-example", "cluster-example"))
    pointer_by_field = {"snapshot_id": CURRENT, "previous_snapshot_id": fixtures.TARGET_SNAPSHOT}
    pin_by_field = fixtures.pin(owner_id=OWNER)
    attestation_by_field = fixtures.activated_attestation()
    match drift:
        case "pointer":
            pointer_by_field["snapshot_id"] = "superseded"
        case "installed-target":
            expected_state[2]["previous_snapshot_id"] = TARGET
        case "source":
            candidate_by_field["manifest"] = fixtures.serving_manifest(source_key="foreign")
        case "layout":
            candidate_by_field["layout_state"] = "open"
        case "pin":
            pin_by_field["owner_id"] = "foreign"
        case "attestation":
            attestation_by_field["activated_at"] = None
    values_by_method = {
        "_one": pointer_by_field,
        "_load_target_snapshot": candidate_by_field,
        "_load_rollback_pin": pin_by_field,
        "load_target_snapshot_scope": fixtures.snapshot_scope(),
        "load_target_attestation": attestation_by_field,
        "_load_target_plan_scopes": fixtures.context().target_plan_scope_records,
    }
    for method, fixture_value in values_by_method.items():
        monkeypatch.setattr(local.store, method, AsyncMock(return_value=fixture_value))
    installed_read = AsyncMock(return_value=expected_state)
    monkeypatch.setattr(local, "_installed_state", installed_read)
    mutations = AsyncMock()
    monkeypatch.setattr(local.store, "apply_rollback", mutations)
    monkeypatch.setattr(local, "_authoritative_global_projection", mutations)
    return installed_read, mutations


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "pointer", "installed-target", "source", "layout", "pin", "attestation"])
async def test_native_predecessor_inspection_has_real_identity_and_never_mutates(monkeypatch, drift):
    installed_read, mutations = _inspection_case(monkeypatch, drift)
    session = Session()
    if drift:
        with pytest.raises((ValueError, PTG2SourceSnapshotRollbackConflict)):
            await local.inspect_local_data_predecessor(
                session,
                source_key=fixtures.SOURCE_KEY,
                expected_current_snapshot_id=CURRENT,
                control_schema_name=CONTROL_SCHEMA,
            )
    else:
        receipt = await local.inspect_local_data_predecessor(
            session,
            source_key=fixtures.SOURCE_KEY,
            expected_current_snapshot_id=CURRENT,
            control_schema_name=CONTROL_SCHEMA,
        )
        assert receipt["snapshot_id"] == fixtures.TARGET_SNAPSHOT and receipt["snapshot_key"] == 17
        assert receipt["rollback_owner_id"] == OWNER and receipt["plan_scope_count"] == 1
        assert receipt["mapping_sha256"] == (b"m" * 32).hex()
        assert "generation_id" not in receipt and "installation_id" not in receipt
        assert local.store._load_target_snapshot.await_args.kwargs == {"is_readonly": True}
        assert local.store._load_rollback_pin.await_args.kwargs["is_readonly"] is True
    installed_read.assert_awaited_once_with(
        session, CURRENT, fixtures.SOURCE_KEY, control_schema_name=CONTROL_SCHEMA, is_readonly=True
    )
    mutations.assert_not_awaited()
    assert session.events == []


def context(retry=False):
    scope_by_field = {**fixtures.snapshot_scope(), "snapshot_id": TARGET}
    attestation_by_field = {
        **fixtures.activated_attestation(),
        "snapshot_id": TARGET,
        "snapshot_key": _binding().destination_layout_key,
    }
    forward_pointer_by_field = local.state._plan_pointer_entry(
        plan_id="plan-1",
        plan_market_type="group",
        source_key=fixtures.SOURCE_KEY,
        snapshot_id=TARGET if retry else CURRENT,
        previous_snapshot_id=CURRENT if retry else TARGET,
        import_month=fixtures.IMPORT_MONTH,
        updated_at=datetime.min,
    )
    return fixtures.context(
        source_pointer_by_field={
            "snapshot_id": TARGET if retry else CURRENT,
            "previous_snapshot_id": CURRENT if retry else TARGET,
        },
        target_snapshot_by_field=installed(TARGET)[3],
        expected_snapshot_by_field=installed(CURRENT, TARGET)[3],
        rollback_pin_by_field={**fixtures.pin(owner_id=OWNER), "snapshot_id": TARGET},
        target_snapshot_scope_by_field=scope_by_field,
        target_attestation_by_field=attestation_by_field,
        source_plan_pointer_records=(forward_pointer_by_field,),
        global_pointer_by_field={},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "pin", "scope", "attestation"])
async def test_context_checks_foreign_pin_and_resolver_scope_before_writes(monkeypatch, drift):
    selected = context()
    pin_by_field = dict(selected.rollback_pin_by_field)
    scope_by_field = dict(selected.target_snapshot_scope_by_field)
    audit_by_field = dict(selected.target_attestation_by_field)
    if drift == "pin":
        pin_by_field["owner_id"] = "foreign"
    if drift == "scope":
        scope_by_field["coverage_scope_id"] = b"short"
    if drift == "attestation":
        audit_by_field["activated_at"] = None
    return_values_by_method = {
        "load_target_snapshot_scope": scope_by_field,
        "load_target_attestation": audit_by_field,
        "_load_target_plan_scopes": selected.target_plan_scope_records,
        "_load_source_pointer": selected.source_pointer_by_field,
        "_load_rollback_pin": pin_by_field,
        "_load_source_plan_pointers": selected.source_plan_pointer_records,
        "_load_allowed_pointer": {},
    }
    for method_name, return_value in return_values_by_method.items():
        monkeypatch.setattr(local.store, method_name, AsyncMock(return_value=return_value))
    target_state = (
        installed(TARGET)[0],
        installed(TARGET)[2],
        installed(TARGET)[3],
        ("node-example", "cluster-example"),
    )
    expected_state = (
        installed(CURRENT, TARGET)[0],
        installed(CURRENT, TARGET)[2],
        installed(CURRENT, TARGET)[3],
        target_state[3],
    )
    if drift:
        with pytest.raises((ValueError, PTG2SourceSnapshotRollbackConflict)):
            await local._context(object(), '"mrf"', fixtures.SOURCE_KEY, target_state, expected_state, OWNER)
    else:
        assert (
            await local._context(object(), '"mrf"', fixtures.SOURCE_KEY, target_state, expected_state, OWNER)
            == selected
        )
    assert local.store._load_rollback_pin.await_args.kwargs == {
        "owner_type": "ptg_v4_rollback",
        "owner_id": OWNER,
        "snapshot_id": TARGET,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["forward", "retry", "foreign-plan", "missing-plan", "default-failure"])
async def test_pointer_vector_and_default_share_the_supplied_session(monkeypatch, mode):
    selected = context(retry=mode == "retry")
    plan_scope_records = selected.target_plan_scope_records
    if mode == "foreign-plan":
        plan_scope_records = ({"plan_id": "foreign-plan", "plan_market_type": "group"},)
    if mode == "missing-plan":
        plan_scope_records = ()
    monkeypatch.setattr(local, "_context", AsyncMock(return_value=selected))
    monkeypatch.setattr(local.store, "_load_target_plan_scopes", AsyncMock(return_value=plan_scope_records))
    timestamp = datetime.now(timezone.utc)
    monkeypatch.setattr(local.store, "database_utc_timestamp", AsyncMock(return_value=timestamp))
    write = AsyncMock()
    default = AsyncMock(side_effect=RuntimeError("default refused") if mode == "default-failure" else None)
    monkeypatch.setattr(local.store, "apply_rollback", write)
    monkeypatch.setattr(local, "_authoritative_global_projection", default)
    target_state = (installed(TARGET)[0], installed(TARGET)[2], installed(TARGET)[3])
    expected_state = (installed(CURRENT, TARGET)[0], installed(CURRENT, TARGET)[2], installed(CURRENT, TARGET)[3])
    session = object()
    if mode in {"foreign-plan", "missing-plan", "default-failure"}:
        with pytest.raises((PTG2SourceSnapshotRollbackConflict, RuntimeError)):
            await local._restore_pointers(session, "mrf", fixtures.SOURCE_KEY, target_state, expected_state, OWNER)
    else:
        decision = await local._restore_pointers(
            session, "mrf", fixtures.SOURCE_KEY, target_state, expected_state, OWNER
        )
        assert decision.is_already_rolled_back == (mode == "retry")
    assert write.await_count == (1 if mode in {"forward", "default-failure"} else 0)
    assert default.await_count == (0 if mode in {"foreign-plan", "missing-plan"} else 1)
    if write.await_count:
        assert write.await_args.args == (session,)
        assert write.await_args.kwargs["snapshot_id"] == TARGET
        assert write.await_args.kwargs["snapshot_id"] != _binding().payload_snapshot_id
    if default.await_count:
        default.assert_awaited_once_with(session, schema='"mrf"')


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, "publisher", "custody", "predecessor", "default"])
async def test_entrypoint_derives_pin_owner_and_rolls_back_failed_completion(monkeypatch, refusal):
    target_state = (
        installed(TARGET)[0],
        installed(TARGET)[2],
        installed(TARGET)[3],
        ("node-example", "cluster-example"),
    )
    expected_state = (
        installed(CURRENT, TARGET)[0],
        installed(CURRENT, TARGET)[2],
        installed(CURRENT, TARGET)[3],
        target_state[3],
    )
    if refusal == "predecessor":
        expected_state[2]["previous_snapshot_id"] = "foreign-snapshot"
    publisher = AsyncMock(side_effect=RuntimeError("publisher refused") if refusal == "publisher" else None)
    resolver = AsyncMock(
        side_effect=RuntimeError("custody refused") if refusal == "custody" else None,
        return_value=expected_state,
    )
    restore = AsyncMock(
        side_effect=RuntimeError("default refused") if refusal == "default" else None,
        return_value=fixtures.decision(fixtures.context()),
    )
    monkeypatch.setattr(local.physical, "require_local_binding_publisher", publisher)
    monkeypatch.setattr(local, "acquire_ptg2_source_lifecycle_lock", AsyncMock())
    monkeypatch.setattr(local, "_installed_state", resolver)
    monkeypatch.setattr(local, "_target_state", AsyncMock(return_value=target_state))
    monkeypatch.setattr(local, "_restore_pointers", restore)
    session = Session()
    restore_arguments_by_field = {
        "source_key": fixtures.SOURCE_KEY,
        "snapshot_id": TARGET,
        "expected_current_snapshot_id": CURRENT,
        "control_schema_name": CONTROL_SCHEMA,
    }
    if refusal:
        with pytest.raises((RuntimeError, PTG2SourceSnapshotRollbackConflict)):
            await local.rollback_installed_local_data_in_transaction(session, **restore_arguments_by_field)
        assert session.events == ["savepoint", "rollback"]
    else:
        receipt = await local.rollback_installed_local_data_in_transaction(session, **restore_arguments_by_field)
        assert session.events == ["savepoint", "release"]
        assert (
            receipt["snapshot_id"] == TARGET and receipt["payload_snapshot_id"] == target_state[1].payload_snapshot_id
        )
        assert receipt["rollback_owner_id"] == OWNER
        assert restore.await_args.args[-1] == OWNER
        assert restore.await_args.args[0] is session
    assert restore.await_count == (1 if refusal in {None, "default"} else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("snapshot_id", ["foreign", TARGET.upper(), CURRENT])
async def test_invalid_or_equal_destinations_refuse_before_transaction_or_authority(snapshot_id):
    with pytest.raises((ValueError, PTG2SourceSnapshotRollbackConflict)):
        await local.rollback_installed_local_data_in_transaction(
            SimpleNamespace(in_transaction=lambda: False),
            source_key=fixtures.SOURCE_KEY,
            snapshot_id=snapshot_id,
            expected_current_snapshot_id=CURRENT,
            control_schema_name=CONTROL_SCHEMA,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "source", "layout", "publication"])
async def test_ordinary_predecessor_reuses_strict_locked_binding_boundary(monkeypatch, drift):
    ordinary_by_field = fixtures.target_snapshot()
    if drift == "source":
        ordinary_by_field["manifest"]["serving_index"]["source_key"] = "foreign"
    if drift == "layout":
        ordinary_by_field["layout_state"] = "loading"
    if drift == "publication":
        ordinary_by_field["published_at"] = None
    query = AsyncMock(return_value=ordinary_by_field)
    monkeypatch.setattr(local.store, "_load_target_snapshot", query)
    session = object()
    if drift:
        with pytest.raises(ValueError):
            await local._target_state(
                session, fixtures.TARGET_SNAPSHOT, fixtures.SOURCE_KEY, control_schema_name=CONTROL_SCHEMA
            )
    else:
        resolved = await local._target_state(
            session, fixtures.TARGET_SNAPSHOT, fixtures.SOURCE_KEY, control_schema_name=CONTROL_SCHEMA
        )
        assert resolved == (None, None, ordinary_by_field, None)
    query.assert_awaited_once_with(session, '"mrf"', fixtures.TARGET_SNAPSHOT)


@pytest.mark.asyncio
async def test_different_individually_authenticated_producers_are_not_invented_as_one(monkeypatch):
    target_state = (installed(TARGET)[0], installed(TARGET)[2], installed(TARGET)[3], ("node-a", "cluster-a"))
    expected_state = (
        installed(CURRENT, TARGET)[0],
        installed(CURRENT, TARGET)[2],
        installed(CURRENT, TARGET)[3],
        ("node-b", "cluster-b"),
    )
    monkeypatch.setattr(local.physical, "require_local_binding_publisher", AsyncMock())
    monkeypatch.setattr(local, "acquire_ptg2_source_lifecycle_lock", AsyncMock())
    monkeypatch.setattr(local, "_target_state", AsyncMock(return_value=target_state))
    monkeypatch.setattr(local, "_installed_state", AsyncMock(return_value=expected_state))
    monkeypatch.setattr(local, "_restore_pointers", AsyncMock(return_value=fixtures.decision(fixtures.context())))
    receipt = await local.rollback_installed_local_data_in_transaction(
        Session(),
        source_key=fixtures.SOURCE_KEY,
        snapshot_id=TARGET,
        expected_current_snapshot_id=CURRENT,
        control_schema_name=CONTROL_SCHEMA,
    )
    assert receipt["target_producer"] == target_state[3] and receipt["current_producer"] == expected_state[3]


@pytest.mark.asyncio
async def test_first_received_successor_can_restore_genuine_ordinary_predecessor(monkeypatch):
    ordinary_by_field = fixtures.target_snapshot()
    expected_state = (
        installed(CURRENT, fixtures.TARGET_SNAPSHOT)[0],
        installed(CURRENT, fixtures.TARGET_SNAPSHOT)[2],
        installed(CURRENT, fixtures.TARGET_SNAPSHOT)[3],
        ("node-example", "cluster-example"),
    )
    monkeypatch.setattr(local.physical, "require_local_binding_publisher", AsyncMock())
    monkeypatch.setattr(local, "acquire_ptg2_source_lifecycle_lock", AsyncMock())
    monkeypatch.setattr(local.store, "_load_target_snapshot", AsyncMock(return_value=ordinary_by_field))
    monkeypatch.setattr(local, "_installed_state", AsyncMock(return_value=expected_state))
    writer = AsyncMock(return_value=fixtures.decision(fixtures.context()))
    monkeypatch.setattr(local, "_restore_pointers", writer)
    session = Session()
    receipt = await local.rollback_installed_local_data_in_transaction(
        session,
        source_key=fixtures.SOURCE_KEY,
        snapshot_id=fixtures.TARGET_SNAPSHOT,
        expected_current_snapshot_id=CURRENT,
        control_schema_name=CONTROL_SCHEMA,
    )
    assert receipt["origin_kind"] == "ordinary" and receipt["snapshot_key"] == ordinary_by_field["snapshot_key"]
    assert receipt["target_producer"] is None and "payload_snapshot_id" not in receipt
    assert writer.await_args.args[0] is session and writer.await_args.args[-1] == OWNER
    assert session.events == ["savepoint", "release"]


@pytest.mark.asyncio
@pytest.mark.parametrize("predecessor", [None, TARGET])
async def test_allowed_pointer_reverse_uses_authenticated_header_not_global_expected_loader(monkeypatch, predecessor):
    from dataclasses import replace as dataclass_replace

    target_state = (installed(TARGET)[0], installed(TARGET)[2], installed(TARGET)[3])
    expected_candidate_by_field = installed(CURRENT, TARGET)[3]
    expected_candidate_by_field["manifest"]["allowed_amount_index"] = fixtures.allowed_index(
        previous_snapshot_id=predecessor
    )
    expected_state = (installed(CURRENT, TARGET)[0], installed(CURRENT, TARGET)[2], expected_candidate_by_field)
    selected = dataclass_replace(
        context(),
        expected_snapshot_by_field=expected_candidate_by_field,
        allowed_pointer_by_field={
            "snapshot_id": CURRENT,
            "previous_snapshot_id": predecessor,
            "previous_snapshot_import_month": fixtures.IMPORT_MONTH,
        },
    )
    monkeypatch.setattr(local, "_context", AsyncMock(return_value=selected))
    monkeypatch.setattr(
        local.store, "_load_target_plan_scopes", AsyncMock(return_value=selected.target_plan_scope_records)
    )
    global_loader = AsyncMock(side_effect=AssertionError("GLOBAL_DATA loader forbidden for installed successor"))
    monkeypatch.setattr(local.store, "_load_expected_snapshot", global_loader)
    monkeypatch.setattr(local.store, "_load_target_snapshot", global_loader)
    monkeypatch.setattr(local.store, "database_utc_timestamp", AsyncMock(return_value=datetime.min))
    writer = AsyncMock()
    monkeypatch.setattr(local.store, "apply_rollback", writer)
    monkeypatch.setattr(local, "_authoritative_global_projection", AsyncMock())
    session = object()
    decision = await local._restore_pointers(session, "mrf", fixtures.SOURCE_KEY, target_state, expected_state, OWNER)
    assert decision.allowed_action == ("delete" if predecessor is None else "reverse")
    assert writer.await_args.kwargs["decision"] is decision
    global_loader.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["inspect", "rollback"])
@pytest.mark.parametrize("schema_name", [None, "", "unsafe-name", "1unsafe", "bad.schema", "x" * 64, "Uppercase"])
async def test_public_entrypoint_refuses_invalid_schema_before_authority_or_transaction(
    monkeypatch, entrypoint, schema_name
):
    query = AsyncMock()
    publisher = AsyncMock()
    monkeypatch.setattr(local, "local_data_physical_read_state", query)
    monkeypatch.setattr(local.physical, "require_local_binding_publisher", publisher)
    session = Session()
    arguments_by_name = {
        "source_key": fixtures.SOURCE_KEY,
        "expected_current_snapshot_id": CURRENT,
        "control_schema_name": schema_name,
    }
    function = local.inspect_local_data_predecessor
    if entrypoint == "rollback":
        function = local.rollback_installed_local_data_in_transaction
        arguments_by_name["snapshot_id"] = TARGET
    with pytest.raises(ValueError, match="package control schema"):
        await function(session, **arguments_by_name)
    query.assert_not_awaited()
    publisher.assert_not_awaited()
    assert session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["inspect", "rollback"])
async def test_public_entrypoint_requires_explicit_schema_keyword(entrypoint):
    session = Session()
    arguments_by_name = {"source_key": fixtures.SOURCE_KEY, "expected_current_snapshot_id": CURRENT}
    function = local.inspect_local_data_predecessor
    if entrypoint == "rollback":
        function = local.rollback_installed_local_data_in_transaction
        arguments_by_name["snapshot_id"] = TARGET
    with pytest.raises(TypeError, match="control_schema_name"):
        await function(session, **arguments_by_name)
    assert session.events == []
