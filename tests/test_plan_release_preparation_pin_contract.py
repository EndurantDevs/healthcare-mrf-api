# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed retention identity and outcome vocabulary at the application boundary."""

import json
from dataclasses import replace
from uuid import UUID

import pytest

from process.plan_release_preparation_pin_contract import (
    PreparationPinAdmission,
    PreparationPinAuthority,
    PreparationPinHeap,
    PreparationPinReceipt,
    PreparationPinScope,
    preparation_pin_receipt_from_payload,
    preparation_pin_scope_from_payload,
)

REVISION = "hpserve_" + "1" * 26
OTHER_REVISION = "hpserve_" + "2" * 26


def scope():
    return PreparationPinScope(
        UUID(int=1), "a" * 64, "b" * 64, "c" * 64, "d" * 64, (("e" * 64, REVISION),), ("synthetic_source",)
    ).validate()


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
