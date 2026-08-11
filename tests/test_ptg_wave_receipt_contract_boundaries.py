"""Closed-boundary coverage for V12 receipt and terminal contracts."""

from __future__ import annotations

import copy
import datetime as dt
from types import SimpleNamespace

import pytest

from process import ptg_wave_ordinary_terminal_contract as terminal_contract
from process import ptg_wave_receipt_contract as receipt_contract
from process import ptg_wave_v12_pristine_abandonment as pristine_contract
from process.ptg_wave_materialized_preclaim_supersession_contract import (
    PTGWaveMaterializedPreclaimConflict,
)
from process.ptg_wave_ordinary_terminal_contract import (
    ORDINARY_TERMINAL_REQUEST_SCHEMA,
    PTGWaveOrdinaryTerminalConflict,
)
from process.ptg_wave_receipt_contract import (
    ABANDONMENT_REQUEST_SCHEMA,
    PTGWaveReceiptContractError,
    admission_receipt_mapping,
    linkage_receipt_payload,
    ordinary_cutover_id,
    validate_abandonment_request,
    validate_receipt_admission,
)
from tests.ptg_wave_v12_pristine_abandonment_support import (
    boundary,
    proof,
    request,
)


def _valid_admission() -> dict[str, object]:
    return proof()[1]


@pytest.mark.parametrize(
    "admission_change",
    (
        ("missing", None),
        ("receipt_key_id", "invalid key"),
        ("wave_digest", "not-a-digest"),
        ("entitlement_coverage_count", -1),
        ("physical_coordinate_count", 0),
        ("physical_coordinate_count", 3),
    ),
)
def test_receipt_admission_rejects_closed_invalid_shapes(admission_change):
    admission_by_field = _valid_admission()
    field_name, replacement = admission_change
    if field_name == "missing":
        admission_by_field.pop("wave_id")
    else:
        admission_by_field[field_name] = replacement

    with pytest.raises(PTGWaveReceiptContractError):
        validate_receipt_admission(admission_by_field)


def test_receipt_admission_requires_an_object():
    with pytest.raises(PTGWaveReceiptContractError, match="must be an object"):
        validate_receipt_admission(None)


def test_receipt_admission_rejects_normalized_key_alias(monkeypatch):
    admission_by_field = _valid_admission()
    monkeypatch.setattr(
        receipt_contract,
        "require_receipt_key_id",
        lambda *_args: "another-valid-key",
    )

    with pytest.raises(PTGWaveReceiptContractError, match="key is invalid"):
        validate_receipt_admission(admission_by_field)


def test_receipt_admission_rejects_normalized_public_key_alias(monkeypatch):
    admission_by_field = _valid_admission()
    monkeypatch.setattr(
        receipt_contract,
        "require_receipt_public_material",
        lambda *_args: ("8" + "0" * 510 + "1", 65537),
    )

    with pytest.raises(PTGWaveReceiptContractError, match="public key is invalid"):
        validate_receipt_admission(admission_by_field)


def test_cutover_identity_rejects_a_domain_collision(monkeypatch):
    operation_id = "a" * 64
    fake_digest = SimpleNamespace(hexdigest=lambda: operation_id)
    monkeypatch.setattr(receipt_contract.hashlib, "sha256", lambda *_args: fake_digest)

    with pytest.raises(PTGWaveReceiptContractError, match="cutover ID is invalid"):
        ordinary_cutover_id(operation_id)


def test_linkage_payload_rejects_cutover_and_field_drift(monkeypatch):
    admission_by_field = _valid_admission()
    common_keywords_by_field = {
        "outcomes_digest": "a" * 64,
        "mapping_digest": "b" * 64,
        "linkage_ack_digest": "c" * 64,
    }
    with pytest.raises(PTGWaveReceiptContractError, match="cutover identity"):
        linkage_receipt_payload(
            admission_by_field,
            cutover_id="d" * 64,
            **common_keywords_by_field,
        )

    monkeypatch.setattr(receipt_contract, "LINKAGE_PAYLOAD_FIELDS", frozenset())
    with pytest.raises(AssertionError, match="field set changed"):
        linkage_receipt_payload(
            admission_by_field,
            cutover_id=ordinary_cutover_id(admission_by_field["wave_id"]),
            **common_keywords_by_field,
        )


def test_abandonment_request_rejects_shape_schema_and_binding():
    wave, _intents, _runs, admission_by_field = boundary()
    request_by_field = request(admission_by_field)
    invalid_requests = (
        None,
        {**request_by_field, "extra": True},
        {**request_by_field, "schema": "unsupported"},
        {**request_by_field, "cutover_id": "f" * 64},
    )
    for invalid_request in invalid_requests:
        with pytest.raises(PTGWaveReceiptContractError):
            validate_abandonment_request(
                invalid_request,
                wave=wave,
                admission=admission_by_field,
            )


def test_persisted_admission_rejects_incomplete_and_changed_intents():
    wave, intents, _runs, _admission_by_field = boundary()
    with pytest.raises(PTGWaveReceiptContractError, match="incomplete"):
        admission_receipt_mapping(wave, intents[:-1])

    changed_intent = copy.copy(intents[0])
    changed_intent.job_id = "changed-job"
    with pytest.raises(PTGWaveReceiptContractError, match="intent changed"):
        admission_receipt_mapping(wave, (changed_intent, *intents[1:]))


def _terminal_request() -> dict[str, object]:
    return {
        "schema": ORDINARY_TERMINAL_REQUEST_SCHEMA,
        "key_id": "receipt-active",
        "operation_id": "a" * 64,
        "member_ordinal": 1,
        "source_file_import_id": "source-import",
        "run_id": "ordinary-run",
    }


def test_terminal_request_rejects_schema_and_operation_drift():
    request_by_field = _terminal_request()
    invalid_requests = (
        None,
        {**request_by_field, "schema": "unsupported"},
        {**request_by_field, "extra": True},
    )
    for invalid_request in invalid_requests:
        with pytest.raises(PTGWaveOrdinaryTerminalConflict):
            terminal_contract.validate_ordinary_terminal_request(invalid_request)

    with pytest.raises(PTGWaveOrdinaryTerminalConflict, match="another operation"):
        terminal_contract.validate_ordinary_terminal_request(
            request_by_field,
            operation_id="b" * 64,
        )


@pytest.mark.parametrize(
    "validator,arguments",
    (
        (terminal_contract._object, (None, "object")),
        (terminal_contract._digest, ("bad", "digest")),
        (terminal_contract._key_id, ("invalid key",)),
        (terminal_contract._ordinal, (-1,)),
        (terminal_contract._count, (-1, "count")),
        (terminal_contract._text, (" padded ", "text", 64)),
        (terminal_contract._month, ("2026-13",)),
        (terminal_contract._string_list, (["b", "a"], "items")),
        (terminal_contract._market_types, (["individual"],)),
    ),
)
def test_terminal_scalar_validators_fail_closed(validator, arguments):
    with pytest.raises(PTGWaveOrdinaryTerminalConflict):
        validator(*arguments)


def test_terminal_month_normalizes_date_and_month_start():
    assert terminal_contract._month(dt.date(2026, 8, 1)) == "2026-08"
    assert terminal_contract._month("2026-08-01") == "2026-08"


@pytest.mark.parametrize(
    "proof_change",
    (
        ("fields", None),
        ("schema_version", "unsupported"),
        ("cutover_id", "f" * 64),
        ("operation_binding", "f" * 64),
        ("cutover_binding", "f" * 64),
        ("admission", None),
        ("admission_binding", None),
    ),
)
def test_pristine_proof_rejects_closed_identity_drift(proof_change):
    proof_by_field, admission_by_field = proof()
    change_name, replacement = proof_change
    validation_keywords_by_field = {}
    if change_name == "fields":
        proof_by_field.pop("redis")
    elif change_name == "operation_binding":
        validation_keywords_by_field["operation_id"] = replacement
    elif change_name == "cutover_binding":
        validation_keywords_by_field["cutover_id"] = replacement
    elif change_name == "admission_binding":
        changed_admission_by_field = dict(admission_by_field)
        changed_admission_by_field["wave_digest"] = "f" * 64
        validation_keywords_by_field["admission"] = changed_admission_by_field
    else:
        proof_by_field[change_name] = replacement

    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        pristine_contract.validate_v12_pristine_abandonment_proof(
            proof_by_field,
            **validation_keywords_by_field,
        )


def test_pristine_database_proof_rejects_shape_counts_and_state():
    proof_by_field, admission_by_field = proof()
    database_by_field = proof_by_field["database"]
    invalid_pairs = (
        (None, admission_by_field["intent_count"]),
        (database_by_field, 0),
        ({**database_by_field, "state": "prepared"}, admission_by_field["intent_count"]),
    )
    for invalid_database, intent_count in invalid_pairs:
        with pytest.raises(PTGWaveMaterializedPreclaimConflict):
            pristine_contract._validate_database(invalid_database, intent_count)


def test_pristine_external_proofs_reject_shape_values_and_identity():
    proof_by_field, _admission_by_field = proof()
    kubernetes_by_field = proof_by_field["kubernetes"]
    redis_by_field = proof_by_field["redis"]
    invalid_kubernetes = (
        None,
        {**kubernetes_by_field, "failed": 11},
        {**kubernetes_by_field, "job_uid": ""},
    )
    for invalid_proof in invalid_kubernetes:
        with pytest.raises(PTGWaveMaterializedPreclaimConflict):
            pristine_contract._validate_kubernetes(invalid_proof)

    invalid_redis = (None, {**redis_by_field, "release_present": True})
    for invalid_proof in invalid_redis:
        with pytest.raises(PTGWaveMaterializedPreclaimConflict):
            pristine_contract._validate_redis(invalid_proof)


@pytest.mark.parametrize("invalid_digest", (None, "g" * 64, "a" * 63))
def test_pristine_digest_requires_exact_lowercase_hex(invalid_digest):
    with pytest.raises(PTGWaveMaterializedPreclaimConflict):
        pristine_contract._require_digest(invalid_digest, "proof digest")


def test_pristine_receipt_projection_guards_its_closed_fields(monkeypatch):
    proof_by_field, _admission_by_field = proof()
    monkeypatch.setattr(pristine_contract, "ABANDONMENT_PAYLOAD_FIELDS", frozenset())

    with pytest.raises(AssertionError, match="payload fields changed"):
        pristine_contract.abandonment_receipt_payload(proof_by_field)
