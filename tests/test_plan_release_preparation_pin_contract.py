# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed retention identity and outcome vocabulary at the application boundary."""

import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import plan_release_preparation_pin_retirement as history
from process import plan_release_preparation_pins as pins
from process.plan_pricing_control import pricing_control_digest
from process.plan_release_preparation_pin_contract import (
    PlanReleaseHistoryAuthority,
    PlanReleasePublication,
    PreparationPinAdmission,
    PreparationPinAuthority,
    PreparationPinHeap,
    PreparationPinReceipt,
    PreparationPinScope,
    digest,
    preparation_pin_receipt_from_payload,
    preparation_pin_scope_from_payload,
)

REVISION = "hpserve_" + "1" * 26
OTHER_REVISION = "hpserve_" + "2" * 26


def scope():
    return PreparationPinScope(
        UUID(int=1), "a" * 64, "b" * 64, "c" * 64, "d" * 64, (("e" * 64, REVISION),), ("synthetic_source",)
    ).validate()


def binding_input_inventory(identity, plan):
    """Build the managed aggregate seal independently of a physical publication."""
    cohort_by_field = {
        "revisions": list(identity.revision_ids),
        "plans": [plan],
        "source_keys": list(identity.source_keys),
    }
    inputs_by_key = {
        pricing_control_digest(cohort_by_field): {
            "identity": cohort_by_field,
            "admission_inputs": True,
            "scope_digest": identity.scope_digest,
            "outcome_receipt_digest": identity.outcome_receipt_digest,
        }
    }
    return {
        "binding_inputs": inputs_by_key,
        "binding_input_seal": {
            "scope_digest": identity.scope_digest,
            "outcome_receipt_digest": identity.outcome_receipt_digest,
            "cohorts_digest": pricing_control_digest(inputs_by_key),
            "revision_count": len(identity.revision_ids),
        },
    }


def heap():
    return PreparationPinHeap("prepared", "pin_cohort", 1, 2, 3, "a" * 64, "b" * 64, 1, "c" * 64).validate()


def admission(*, status="ready"):
    return PreparationPinAdmission(
        "e" * 64, REVISION, "a" * 64, "b" * 64, "synthetic-admission", status, 1, "synthetic-run", 0, False
    )


def witness(*, state=None):
    identity = scope()
    return PreparationPinAuthority(
        identity, "sealed", (state or admission(),), None, binding_heap=heap(), prepared_pin_heap=heap()
    ).validate(identity)


def test_native_receipt_json_roundtrip_preserves_captured_identity():
    identity = scope()
    receipt = PreparationPinReceipt(identity, heap(), 4, 5, identity.revision_ids).validate()
    assert preparation_pin_scope_from_payload(json.loads(json.dumps(identity.payload()))) == identity
    assert preparation_pin_receipt_from_payload(json.loads(json.dumps(receipt.payload()))) == receipt


@pytest.mark.parametrize(
    "changes",
    [
        {"source_keys": ("SYNTHETIC_SOURCE",)},
        {"source_keys": (" ",)},
        {"source_keys": ("a" * 49,)},
        {"scope_digest": "A" * 64},
        {"preparation_id": "not-a-uuid"},
        {"publication_revisions": (("e" * 64, REVISION), ("e" * 64, OTHER_REVISION))},
        {"publication_revisions": (("e" * 64, REVISION), ("f" * 64, REVISION))},
    ],
)
def test_scope_rejects_noncanonical_reused_or_partial_identity(changes):
    with pytest.raises(ValueError):
        replace(scope(), **changes).validate()


@pytest.mark.parametrize("status", ["canceled", "cancelled", "dead_letter", "timeout", "unknown"])
def test_nonjournal_outcomes_cannot_authorize_retirement(status):
    with pytest.raises(ValueError, match="admission_state_invalid"):
        replace(admission(), status=status).validate()


@pytest.mark.parametrize("status", ["reserved", "dispatching", "accepted"])
def test_pending_or_ambiguous_outcomes_remain_retained(status):
    with pytest.raises(ValueError, match="admissions_unresolved"):
        witness(state=admission(status=status)).require_settled()


def test_unavailable_is_retained_and_failure_requires_no_work_or_retry():
    assert witness(state=admission(status="unavailable")).require_settled() == (REVISION,)
    assert witness(state=admission(status="failed")).require_settled() == ()
    for changes in ({"remaining_work": 1}, {"retryable": True}):
        with pytest.raises(ValueError, match="admissions_unresolved"):
            witness(state=replace(admission(status="failed"), **changes)).require_settled()


def test_protected_images_and_receipt_inventory_are_mandatory():
    identity = scope()
    with pytest.raises(ValueError, match="protected_heap_inventory_required"):
        replace(witness(), prepared_pin_heap=None).validate(identity)
    receipt = PreparationPinReceipt(identity, heap(), 4, 5, identity.revision_ids)
    with pytest.raises(ValueError, match="receipt_inventory_conflict"):
        replace(witness(), registered_receipt=receipt, detached_receipts=(receipt,)).validate(identity)
    with pytest.raises(ValueError, match="receipt_inventory_conflict"):
        replace(witness(), detached_receipts=(receipt,), cleaned_receipts=(receipt,)).validate(identity)


def publication():
    return PlanReleasePublication(
        REVISION,
        "hprelease_" + "1" * 26,
        "hpplan_" + "2" * 26,
        None,
        "2026-10",
        1,
        "c" * 64,
        {"contract": "synthetic-release", "authority": {"source": "synthetic_source"}},
    ).validate()


def test_unneeded_pricing_is_truthful_and_remains_retained():
    state = replace(admission(status="not_required"), dispatch_count=0, engine_run_id=None).validate()
    assert witness(state=state).require_settled() == (REVISION,)


@pytest.mark.parametrize(
    "changes", [{"dispatch_count": 1}, {"engine_run_id": "real-run"}, {"remaining_work": 1}, {"retryable": True}]
)
def test_unneeded_pricing_cannot_hide_dispatched_or_pending_work(changes):
    state = replace(admission(status="not_required"), dispatch_count=0, engine_run_id=None)
    with pytest.raises(ValueError, match="unneeded_admission_has_work"):
        replace(state, **changes).validate()


@pytest.mark.parametrize(
    "changes",
    [
        {"serving_revision_id": "not-a-revision"},
        {"plan_release_id": "hprelease_bad"},
        {"healthporta_plan_id": "hpplan_bad"},
        {"plan_version_id": "hpversion_bad"},
        {"release_month": "2026-13"},
        {"expected_binding_count": True},
        {"expected_binding_count": 0},
        {"expected_binding_count": 2_147_483_648},
        {"binding_set_digest": "A" * 64},
        {"source_manifest": []},
        {"source_manifest": {"value": float("nan")}},
        {"source_manifest": {"value": (1, 2)}},
        {"source_manifest": {1: "noncanonical-key"}},
    ],
)
def test_publication_rejects_invalid_native_header_or_manifest(changes):
    with pytest.raises(ValueError):
        replace(publication(), **changes).validate()


def test_publication_payload_preserves_and_copies_exact_manifest():
    header = publication()
    payload = header.payload()
    assert payload["source_manifest"] == header.source_manifest
    payload["source_manifest"]["authority"]["source"] = "other_source"
    assert header.source_manifest["authority"]["source"] == "synthetic_source"


def publication_case(monkeypatch):
    header = publication()
    inventory = binding_input_inventory(scope(), header.healthporta_plan_id)
    identity = replace(scope(), binding_inputs_digest=pricing_control_digest(inventory["binding_input_seal"]))
    binding = replace(heap(), name="binding_cohort", oid=7)
    inputs = pins.PreparationPinInputs(binding, heap())
    receipt = PreparationPinReceipt(identity, heap(), 4, 5, (REVISION,)).validate()
    before = replace(
        witness(),
        scope=identity,
        registered_receipt=receipt,
        binding_heap=binding,
        publication_digest=digest(header.payload()),
    )
    after = replace(before, preparation_status="published", writers_closed=True)
    events = []

    @asynccontextmanager
    async def transaction():
        events.append("savepoint")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("release")

    connection = SimpleNamespace(transaction=transaction, is_in_transaction=lambda: True)
    for name in (
        "require_preparation_pins",
        "_publish_native_binding",
        "_require_published_header",
        "_require_published_inventory",
        "_publisher",
    ):
        monkeypatch.setattr(pins, name, AsyncMock())
    monkeypatch.setattr(
        pins, "_original_publisher_identity", AsyncMock(return_value=(2, 3, "4", "publisher", "publisher"))
    )
    return SimpleNamespace(
        header=header,
        inputs=inputs,
        receipt=receipt,
        connection=connection,
        events=events,
        before=before,
        after=after,
        authority=AsyncMock(side_effect=(before, after)),
    )


async def publish_case(case):
    await pins.publish_prepared_plan_release(
        case.connection,
        "storage",
        case.header,
        inputs=case.inputs,
        receipt=case.receipt,
        expected_current=None,
        owner_role="storage_owner",
        publisher_role="publisher",
        reader_roles=("reader",),
        authority=case.authority,
    )


@pytest.mark.asyncio
async def test_publication_authorizes_exact_header_before_and_after_native_cut(monkeypatch):
    case = publication_case(monkeypatch)
    assert case.header.binding_set_digest != case.receipt.scope.binding_inputs_digest
    assert case.before.publication_digest == digest(case.header.payload())
    assert case.before.publication_digest != case.receipt.scope.binding_inputs_digest
    await publish_case(case)
    assert case.events == ["savepoint", "release"]
    pins._publish_native_binding.assert_awaited_once()
    pins._require_published_inventory.assert_awaited_once()
    pins._publisher.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["header", "inventory", "state", "count"])
async def test_publication_refuses_unbound_header_or_custody_before_writes(monkeypatch, damage):
    case = publication_case(monkeypatch)
    changes_by_damage = {
        "header": {"publication_digest": "f" * 64},
        "inventory": {"registered_receipt": None},
        "state": {"preparation_status": "published"},
        "count": {"binding_heap": replace(case.inputs.binding_heap, row_count=2)},
    }
    case.authority.side_effect = (replace(case.before, **changes_by_damage[damage]),)
    with pytest.raises(ValueError):
        await publish_case(case)
    pins._publish_native_binding.assert_not_awaited()
    assert case.events == ["savepoint", "rollback"]
    pins._publisher.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["before", "after"])
@pytest.mark.parametrize("damage", ["physical", "scope"])
async def test_publication_refuses_changed_commitment_in_either_digest_domain(monkeypatch, phase, damage):
    case = publication_case(monkeypatch)
    observed = case.before if phase == "before" else case.after
    if damage == "physical":
        changed_header = replace(case.header, binding_set_digest=digest({"binding": "changed"}))
        changed = replace(observed, publication_digest=digest(changed_header.payload()))
        error = "publication_authority_changed"
    else:
        seal = binding_input_inventory(case.receipt.scope, case.header.healthporta_plan_id)["binding_input_seal"]
        changed_scope = replace(
            case.receipt.scope, binding_inputs_digest=pricing_control_digest({**seal, "revision_count": 2})
        )
        changed = replace(observed, scope=changed_scope)
        error = "authority_scope_changed"
    case.authority.side_effect = (changed,) if phase == "before" else (case.before, changed)
    with pytest.raises(ValueError, match=error):
        await publish_case(case)
    assert pins._publish_native_binding.await_count == (phase == "after")
    assert case.events == ["savepoint", "rollback"]
    pins._publisher.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["before", "after"])
async def test_publication_rejects_changed_physical_transaction_and_restores_role(monkeypatch, phase):
    case = publication_case(monkeypatch)
    original = (2, 3, "4", "publisher", "publisher")
    changed = (2, 3, "5", "publisher", "publisher")
    pins._original_publisher_identity.side_effect = [original] + ([original] if phase == "after" else []) + [changed]
    with pytest.raises(ValueError, match="original_transaction_changed"):
        await publish_case(case)
    assert case.events == ["savepoint", "rollback"]
    assert pins._publish_native_binding.await_count == (phase == "after")
    pins._publisher.assert_awaited_once()


def history_cut():
    header_by_field = {
        **publication().payload(),
        "is_current": True,
        "release_status": "published",
        "serving_status": "published",
    }
    return history.PlanReleaseHistoryCut(
        "storage",
        publication(),
        header_by_field,
        replace(heap(), name="binding", oid=7),
        heap(),
        (REVISION, OTHER_REVISION),
        True,
        4,
        5,
        6,
        (2, 3, "4", "publisher", "publisher"),
    )


@pytest.mark.parametrize("operation", ["repair", "withdraw", "detach", "reactivate"])
def test_history_intent_preserves_exact_publication_identity(operation):
    cut = history_cut()
    history._require_history_intent(cut, operation, cut.publication)
    with pytest.raises(ValueError, match="history_publication_changed"):
        history._require_history_intent(
            cut, operation, replace(cut.publication, plan_release_id="hprelease_" + "3" * 26)
        )
    changed = replace(cut.publication, source_manifest={"projection": "verified"})
    if operation == "repair":
        history._require_history_intent(cut, operation, changed)
    else:
        with pytest.raises(ValueError, match="history_publication_changed"):
            history._require_history_intent(cut, operation, changed)


def test_history_repair_requires_current_published_header():
    cut = history_cut()
    cut.header["is_current"] = False
    with pytest.raises(ValueError, match="history_repair_not_current"):
        history._require_history_intent(cut, "repair", cut.publication)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["cut", "publication", "operation", "pointer", "cohort", "pin"])
async def test_history_authority_binds_the_whole_child_and_exact_pointer(damage):
    cut = history_cut()
    expected = (REVISION, "c" * 64)
    witness = PlanReleaseHistoryAuthority(
        digest(cut.payload()), digest(cut.publication.payload()), "detach", expected, cut.pin_revision_ids
    )
    changes_by_damage = {
        "cut": {"cut_digest": "f" * 64},
        "publication": {"publication_digest": "f" * 64},
        "operation": {"operation": "withdraw"},
        "pointer": {"expected_current": None},
        "cohort": {"eligible_revision_ids": (REVISION,)},
        "pin": {"detached_pin_heap": heap()},
    }
    authority = AsyncMock(return_value=replace(witness, **changes_by_damage[damage]))
    with pytest.raises(ValueError, match="history_authority_changed"):
        await history._authorize_history(None, cut, authority, ("detach", cut.publication, expected), phase="before")


@pytest.mark.asyncio
async def test_history_detachment_requires_persisted_actual_receipt():
    cut = history_cut()
    expected = (REVISION, "c" * 64)
    witness = PlanReleaseHistoryAuthority(
        digest(cut.payload()), digest(cut.publication.payload()), "detach", expected, cut.pin_revision_ids
    )
    authority = AsyncMock(return_value=witness)
    with pytest.raises(ValueError, match="history_authority_changed"):
        await history._authorize_history(None, cut, authority, ("detach", cut.publication, expected), phase="after")
    authority.return_value = replace(witness, detached_pin_heap=cut.pin_heap)
    assert (
        await history._authorize_history(None, cut, authority, ("detach", cut.publication, expected), phase="after")
        == authority.return_value
    )
