# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact output receipts cannot substitute authority, sessions, or native history."""

import asyncio
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_output_snapshot_custody as custody
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_output_snapshot_catalog import OUTPUT_ROOTS, encoded, output_models


def operation(kind="contact_backfill"):
    operation_by_field = dict(
        kind=kind,
        run_id="operation-a",
        producer="source-v1",
        source_pins=[],
        resource_types=[],
        source_fence_digest="a" * 64,
    )
    if kind == "rollback":
        operation_by_field["rollback_of"] = "b" * 64
    return operation_by_field


def relation(name, oid, *, descriptor=None, readable=True):
    return custody.NativeOutputRelation(
        "source_schema",
        15,
        OUTPUT_ROOTS[0],
        name,
        oid,
        17,
        (encoded({"name": "key", "type_oid": 25}),),
        (encoded({"type": "p", "oid": oid + 1}),),
        (encoded({"oid": oid + 2, "name": name + "_pk"}),),
        encoded({"kind": "r"}),
        encoded(
            {"table": [[17, 17, "SELECT", False]] + ([[18, 17, "SELECT", False]] if readable else []), "columns": []}
        ),
        encoded(descriptor) if descriptor is not None else None,
    )


def test_native_receipts_have_strict_canonical_payload_roundtrip():
    old = relation(OUTPUT_ROOTS[0], 101)
    new = relation("isolated_a", 201, descriptor={"phase": "sealed"}, readable=False)
    prepared = custody.PreparedOutputReceipt(1, "b" * 64, old, new)
    assert custody.PreparedOutputReceipt.from_payload(prepared.to_payload()) == prepared
    assert custody.NativeOutputRelation.from_payload(old.to_payload()) == old
    with pytest.raises(FrozenInstanceError):
        old.oid = 999
    payload = prepared.to_payload()
    payload["successor"]["indexes"][0]["oid"] = 777
    assert custody.PreparedOutputReceipt.from_payload(payload) != prepared


@pytest.mark.parametrize("change", ["extra", "bool_oid", "bad_target", "mutable_shape", "bad_protocol", "bad_digest"])
def test_untrusted_payload_cannot_relax_receipt_shape(change):
    payload = custody.PreparedOutputReceipt(
        1, "b" * 64, relation(OUTPUT_ROOTS[0], 101), relation("isolated_a", 201)
    ).to_payload()
    match change:
        case "extra":
            payload["skip_checks"] = True
        case "bool_oid":
            payload["incumbent"]["oid"] = True
        case "bad_target":
            payload["successor"]["target"] = "unrelated_table"
        case "mutable_shape":
            payload["incumbent"]["columns"] = {}
        case "bad_protocol":
            payload["protocol"] = True
        case _:
            payload["operation_digest"] = "z" * 64
    with pytest.raises(RuntimeError, match="receipt_invalid"):
        custody.PreparedOutputReceipt.from_payload(payload)


def test_fixed_model_contract_has_ten_exact_native_shapes():
    assert tuple(output_models()) == OUTPUT_ROOTS
    assert sum(len(model.__table__.columns) for model in output_models().values()) == 266
    assert sum(1 + len(getattr(model, "__my_additional_indexes__", ())) for model in output_models().values()) == 44


@pytest.mark.parametrize(
    "change", ["extra", "unknown_kind", "empty_run", "bad_fence", "bad_rollback", "source_without_pin"]
)
def test_invalid_operation_refuses_before_native_io(change):
    value = operation()
    match change:
        case "extra":
            value["skip_checks"] = True
        case "unknown_kind":
            value["kind"] = "arbitrary_sql"
        case "empty_run":
            value["run_id"] = ""
        case "bad_fence":
            value["source_fence_digest"] = "unsafe"
        case "bad_rollback":
            value = operation("rollback")
            value["rollback_of"] = ""
        case _:
            value["kind"] = "source_projection"
    with pytest.raises(RuntimeError, match="operation_invalid|source_identity_invalid"):
        custody._operation_digest(value, (OUTPUT_ROOTS[0],))


@pytest.fixture
def capability(monkeypatch):
    def bind():
        policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
        driver = SimpleNamespace(is_in_transaction=lambda: True, execute=AsyncMock())
        session = object()
        publisher = SimpleNamespace(connection=driver, policy=policy)
        cap = custody._OutputCapability(
            session,
            driver,
            policy,
            "b" * 64,
            (OUTPUT_ROOTS[0],),
            asyncio.current_task(),
            91,
            "71",
            publisher,
            None,
            custody.encoded(operation()),
        )
        monkeypatch.setattr(custody, "_OUTPUT", SimpleNamespace(get=lambda: cap))
        monkeypatch.setattr(custody, "bound_publisher", lambda: publisher)
        monkeypatch.setattr(custody, "source_copy_session", lambda: session)
        monkeypatch.setattr(custody, "_copy_driver", AsyncMock(return_value=driver))
        monkeypatch.setattr(
            custody,
            "_actor",
            AsyncMock(
                return_value=dict(session=policy.publisher_role, current=policy.owner_role, backend=91, xid="71")
            ),
        )
        return cap

    return bind


@pytest.mark.asyncio
async def test_capability_is_not_transferable_to_an_inherited_child_task(capability):
    capability = capability()
    await custody._require_capability(capability)
    with pytest.raises(RuntimeError, match="capability_required"):
        await asyncio.create_task(custody._require_capability(capability))


@pytest.mark.asyncio
async def test_admitted_intent_is_an_immutable_canonical_copy(capability):
    cap = capability()
    altered = cap.operation
    altered["source_pins"].append({"untrusted": True})
    altered["run_id"] = "changed"
    assert cap.operation == operation()
    with pytest.raises(FrozenInstanceError):
        cap.operation_json = custody.encoded(altered)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["context", "session", "driver", "role", "backend", "xid"])
async def test_original_physical_transaction_substitution_refuses(capability, monkeypatch, change):
    capability = capability()
    if change == "context":
        monkeypatch.setattr(custody, "_OUTPUT", SimpleNamespace(get=lambda: None))
    elif change == "session":
        monkeypatch.setattr(custody, "source_copy_session", lambda: object())
    elif change == "driver":
        monkeypatch.setattr(custody, "_copy_driver", AsyncMock(return_value=object()))
    else:
        actor_by_field = dict(
            session=capability.policy.publisher_role, current=capability.policy.owner_role, backend=91, xid="71"
        )
        actor_by_field[{"role": "current", "backend": "backend", "xid": "xid"}[change]] = "changed"
        monkeypatch.setattr(custody, "_actor", AsyncMock(return_value=actor_by_field))
    with pytest.raises(RuntimeError, match="capability_required|original_.*changed"):
        await custody._require_capability(capability)


@pytest.mark.asyncio
async def test_forward_recheck_rejects_native_oid_or_index_substitution(capability, monkeypatch):
    capability = capability()
    old = relation(OUTPUT_ROOTS[0], 101)
    new = relation(
        "isolated_a",
        201,
        readable=False,
        descriptor=dict(
            protocol=1,
            operation_digest=capability.operation_digest,
            target=OUTPUT_ROOTS[0],
            phase="sealed",
            incumbent_oid=old.oid,
            incumbent_digest=custody._digest(old.to_payload()),
        ),
    )
    receipt = custody.PreparedOutputReceipt(1, capability.operation_digest, old, new)
    capture = AsyncMock(side_effect=[{old.relation: old}, {new.relation: replace(new, indexes=old.indexes)}])
    monkeypatch.setattr(custody, "_capture", capture)
    with pytest.raises(RuntimeError, match="native_receipt_changed"):
        await custody.recheck_output_cut(capability, (receipt,))
    assert capture.await_count == 2
    capability.connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_reader_grant_is_the_only_allowed_postpublication_delta(capability, monkeypatch):
    capability = capability()
    old = relation(OUTPUT_ROOTS[0], 101)
    new = relation(
        "isolated_a",
        201,
        readable=False,
        descriptor=dict(
            protocol=1,
            operation_digest=capability.operation_digest,
            target=OUTPUT_ROOTS[0],
            phase="sealed",
            incumbent_oid=old.oid,
            incumbent_digest=custody._digest(old.to_payload()),
        ),
    )
    receipt = custody.PreparedOutputReceipt(1, capability.operation_digest, old, new)
    monkeypatch.setattr(custody, "storage_roles", AsyncMock(return_value=(17, [18])))
    published = await custody._published_successor(capability, receipt)
    assert published.relation == OUTPUT_ROOTS[0]
    assert published.indexes == new.indexes and published.oid == new.oid
    monkeypatch.setattr(
        custody,
        "_capture",
        AsyncMock(
            side_effect=[{OUTPUT_ROOTS[0]: replace(new, relation=OUTPUT_ROOTS[0])}, {OUTPUT_ROOTS[0]: published}]
        ),
    )
    await custody.publish_output_read_access(capability, receipt)
    assert capability.connection.execute.await_args.args[0] == (
        'GRANT SELECT ON TABLE "source_schema"."provider_directory_canonical_resource" TO "source_reader"'
    )


@pytest.mark.asyncio
async def test_restore_preserves_unsealed_legacy_predecessor_and_exact_acl(capability, monkeypatch):
    capability = capability()
    rollback = replace(capability, reference="c" * 64)
    monkeypatch.setattr(custody, "_OUTPUT", SimpleNamespace(get=lambda: rollback))
    old, new = relation("retained_a", 101), relation(OUTPUT_ROOTS[0], 201, descriptor={"phase": "sealed"})
    receipt = custody.PreparedOutputReceipt(1, "c" * 64, old, new)
    monkeypatch.setattr(custody, "_capture", AsyncMock(return_value={old.relation: old, new.relation: new}))
    await custody.recheck_output_restore(rollback, (receipt,), {OUTPUT_ROOTS[0]: "retained_a"})
    restored, failed = replace(old, relation=OUTPUT_ROOTS[0]), replace(new, relation="failed_a")
    monkeypatch.setattr(
        custody, "_capture", AsyncMock(return_value={restored.relation: restored, failed.relation: failed})
    )
    assert await custody.capture_restored_output(rollback, receipt, "failed_a") == (failed, restored)
    assert restored.descriptor is None and restored.indexes == old.indexes
    with pytest.raises(RuntimeError, match="restore_receipt_identity_changed"):
        await custody.recheck_output_restore(
            rollback, (replace(receipt, operation_digest="d" * 64),), {OUTPUT_ROOTS[0]: "retained_a"}
        )
