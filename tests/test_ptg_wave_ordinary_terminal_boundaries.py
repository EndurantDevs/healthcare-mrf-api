"""Fail-closed boundaries for ordinary terminal receipt derivation."""

from __future__ import annotations

import copy
import datetime as dt
from types import SimpleNamespace

import pytest

from process import ptg_wave_ordinary_terminal_payload as terminal_payload
from process import ptg_wave_ordinary_terminal_receipt as terminal_receipt
from process import ptg_wave_ordinary_terminal_validation as terminal_validation
from process import ptg_wave_receipt_process_authority as process_authority
from process.ptg_wave_ordinary_terminal_receipt import (
    ORDINARY_TERMINAL_RECEIPT_SCHEMA,
    PTGWaveOrdinaryTerminalConflict,
    issue_ordinary_terminal_receipt,
    ordinary_terminal_receipt_payload,
)
from process.ptg_wave_receipt_authority import PTGWaveReceiptAuthorityError
from process.ptg_wave_quarantine_basis import (
    V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS,
)
from tests.ptg_wave_ordinary_terminal_receipt_support import (
    ISSUED_AT,
    OPERATION_ID,
    QueuedTerminalSession,
    TerminalTransaction,
    keyring,
    ordinary_result,
    v13_ordinary_result,
)
from tests.ptg_blank_terminal_support import blank_ordinary_result


def _set_nested(mapping, *path_and_value):
    *path, replacement = path_and_value
    target = mapping
    for field_name in path[:-1]:
        target = target[field_name]
    target[path[-1]] = replacement


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            lambda state: setattr(
                state["quarantine"], "recovery_evidence", None
            ),
            "abandonment proof is invalid",
        ),
        (
            lambda state: _set_nested(
                state["quarantine"].abandonment_receipt,
                "schema",
                "unsupported",
            ),
            "abandonment receipt is invalid",
        ),
        (
            lambda state: setattr(state["intent"], "params", None),
            "member params are invalid",
        ),
        (
            lambda state: state["intent"].params.pop(
                "direct_rate_file_intent"
            ),
            "direct input is invalid|not a frozen singleton",
        ),
        (
            lambda state: setattr(
                state["intent"],
                "source_file_import_id",
                "other-source",
            ),
            "coordinate conflicts",
        ),
        (
            lambda state: setattr(state["run"], "snapshot_id", "other"),
            "snapshot identity conflicts",
        ),
        (
            lambda state: _set_nested(
                state["run"].metrics, "status", "failed"
            ),
            "ordinary run does not match",
        ),
        (
            lambda state: state.update(engine_run=None),
            "terminal result is unavailable",
        ),
        (
            lambda state: state.update(engine_snapshot=None),
            "terminal result is unavailable",
        ),
        (
            lambda state: setattr(
                state["engine_snapshot"], "snapshot_id", "other"
            ),
            "durable PTG result conflicts",
        ),
        (
            lambda state: setattr(
                state["engine_snapshot"], "status", "published"
            ),
            "durable PTG result conflicts",
        ),
    ),
)
def test_terminal_payload_rejects_durable_state_boundary_matrix(
    monkeypatch,
    mutation,
    message,
):
    terminal_state = ordinary_result(monkeypatch)
    mutation(terminal_state)

    with pytest.raises(PTGWaveOrdinaryTerminalConflict, match=message):
        ordinary_terminal_receipt_payload(**terminal_state)


@pytest.mark.parametrize("field_name", ("reason", "recovery_basis"))
def test_terminal_payload_rejects_cross_family_abandonment_drift(
    monkeypatch,
    field_name,
):
    terminal_state = v13_ordinary_result(monkeypatch)
    setattr(
        terminal_state["quarantine"],
        field_name,
        V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS,
    )

    with pytest.raises(
        PTGWaveOrdinaryTerminalConflict,
        match="signed V12 abandonment or V13 abandonment",
    ):
        ordinary_terminal_receipt_payload(**terminal_state)


@pytest.mark.parametrize(
    ("field_constant", "message"),
    (
        ("COORDINATE_FIELDS", "coordinate fields changed"),
        ("SCOPE_FIELDS", "scope fields changed"),
        ("TERMINAL_RESULT_FIELDS", "result fields changed"),
        ("ORDINARY_TERMINAL_PAYLOAD_FIELDS", "V12 abandonment"),
    ),
)
def test_terminal_payload_rejects_internal_field_set_drift(
    monkeypatch,
    field_constant,
    message,
):
    terminal_state = ordinary_result(monkeypatch)
    monkeypatch.setattr(terminal_payload, field_constant, frozenset())

    expected_error = (
        PTGWaveOrdinaryTerminalConflict
        if field_constant == "ORDINARY_TERMINAL_PAYLOAD_FIELDS"
        else AssertionError
    )
    with pytest.raises(expected_error, match=message):
        ordinary_terminal_receipt_payload(**terminal_state)


def test_terminal_payload_rejects_admission_and_recovery_digest_drift(
    monkeypatch,
):
    terminal_state = ordinary_result(monkeypatch)
    terminal_state["wave"].receipt_public_exponent = 3
    with pytest.raises(
        PTGWaveOrdinaryTerminalConflict,
        match="conflicts with V12 admission",
    ):
        ordinary_terminal_receipt_payload(**terminal_state)

    terminal_state = ordinary_result(monkeypatch)
    terminal_state["quarantine"].recovery_evidence_sha256 = "f" * 64
    with pytest.raises(
        PTGWaveOrdinaryTerminalConflict,
        match="conflicts with V12 abandonment",
    ):
        ordinary_terminal_receipt_payload(**terminal_state)


def test_outer_result_identity_rejects_the_historical_member_run(monkeypatch):
    terminal_state = ordinary_result(monkeypatch)
    terminal_state["run"].run_id = terminal_state["intent"].run_id

    with pytest.raises(
        PTGWaveOrdinaryTerminalConflict,
        match="terminal run identity is invalid",
    ):
        terminal_validation._outer_result_identities(
            terminal_state["run"],
            request={
                **terminal_state["request"],
                "run_id": terminal_state["intent"].run_id,
            },
            intent=terminal_state["intent"],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("queued_rows", "message"),
    (
        ((None,), "operation is unavailable"),
        ((SimpleNamespace(wave_id=OPERATION_ID), None), "member is unavailable"),
    ),
)
async def test_terminal_snapshot_requires_operation_and_member(
    monkeypatch,
    queued_rows,
    message,
):
    terminal_state = ordinary_result(monkeypatch)
    session = QueuedTerminalSession(queued_rows)

    with pytest.raises(PTGWaveOrdinaryTerminalConflict, match=message):
        await terminal_receipt._load_terminal_snapshot(
            session,
            terminal_state["request"],
        )


@pytest.mark.asyncio
async def test_terminal_snapshot_requires_the_outer_run(monkeypatch):
    terminal_state = ordinary_result(monkeypatch)
    session = QueuedTerminalSession((None,))

    with pytest.raises(
        PTGWaveOrdinaryTerminalConflict,
        match="terminal run is unavailable",
    ):
        await terminal_receipt._load_outer_run(
            session,
            terminal_state["request"],
            terminal_state["intent"],
        )


def test_database_sqlstate_traverses_wrappers_once_and_handles_cycles():
    outer_error = RuntimeError("outer")
    inner_error = RuntimeError("inner")
    inner_error.pgcode = "55P03"
    outer_error.orig = inner_error
    inner_error.__cause__ = outer_error

    assert terminal_receipt._database_sqlstate(outer_error) == "55P03"
    assert terminal_receipt._database_sqlstate(RuntimeError("plain")) == ""
    cyclic_error = RuntimeError("cycle")
    cyclic_error.__cause__ = cyclic_error
    assert terminal_receipt._database_sqlstate(cyclic_error) == ""


def test_blank_terminal_loading_boundaries(monkeypatch):
    terminal_state = blank_ordinary_result(monkeypatch)
    run = terminal_state["run"]
    run.params = []
    assert terminal_receipt._run_with_blank_metrics(
        run,
        terminal_state["engine_run"],
        terminal_state["engine_snapshot"],
    ) is run

    run.params = terminal_state["intent"].params
    assert terminal_receipt._run_with_blank_metrics(run, None, None) is run

    run.status = "running"
    with pytest.raises(PTGWaveOrdinaryTerminalConflict):
        terminal_receipt._outer_engine_import_run_id(
            run,
            request=terminal_state["request"],
            intent=terminal_state["intent"],
        )

    run.run_id = terminal_state["intent"].run_id
    with pytest.raises(PTGWaveOrdinaryTerminalConflict):
        terminal_receipt._outer_engine_import_run_id(
            run,
            request=terminal_state["request"],
            intent=terminal_state["intent"],
        )


@pytest.mark.asyncio
async def test_issue_preserves_nonretryable_database_errors(monkeypatch):
    terminal_state = ordinary_result(monkeypatch)
    database_error = RuntimeError("integrity failure")
    database_error.sqlstate = "23505"

    class FailingSession:
        async def execute(self, _statement, _params=None):
            raise database_error

    monkeypatch.setattr(
        terminal_receipt.db,
        "transaction",
        lambda: TerminalTransaction(FailingSession()),
    )

    with pytest.raises(RuntimeError, match="integrity failure") as failure:
        await issue_ordinary_terminal_receipt(
            OPERATION_ID,
            terminal_state["request"],
            receipt_keyring=keyring(monkeypatch),
        )
    assert failure.value is database_error


def _stored_receipt(monkeypatch):
    terminal_state = ordinary_result(monkeypatch)
    receipt_keyring = keyring(monkeypatch)
    receipt_payload = ordinary_terminal_receipt_payload(**terminal_state)
    receipt_by_field = receipt_keyring.sign_receipt(
        schema=ORDINARY_TERMINAL_RECEIPT_SCHEMA,
        key_id=terminal_state["request"]["key_id"],
        issued_at=ISSUED_AT,
        receipt_payload=receipt_payload,
    )
    stored_receipt = SimpleNamespace(
        wave_id=terminal_state["request"]["operation_id"],
        member_ordinal=terminal_state["request"]["member_ordinal"],
        source_file_import_id=terminal_state["request"][
            "source_file_import_id"
        ],
        run_id=terminal_state["request"]["run_id"],
        receipt_key_id=terminal_state["request"]["key_id"],
        receipt=receipt_by_field,
        payload_digest=receipt_by_field["payload_digest"],
        issued_at=dt.datetime.strptime(
            ISSUED_AT, "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(tzinfo=dt.UTC),
    )
    return terminal_state, receipt_keyring, receipt_payload, stored_receipt


def test_stored_terminal_receipt_rejects_identity_and_payload_drift(monkeypatch):
    terminal_state, receipt_keyring, receipt_payload, stored_receipt = (
        _stored_receipt(monkeypatch)
    )
    stored_receipt.wave_id = "f" * 64
    with pytest.raises(
        PTGWaveOrdinaryTerminalConflict,
        match="receipt identity is invalid",
    ):
        terminal_receipt._validate_existing_receipt(
            stored_receipt,
            request=terminal_state["request"],
            expected_payload=receipt_payload,
            keyring=receipt_keyring,
        )

    stored_receipt.wave_id = terminal_state["request"]["operation_id"]
    changed_payload_by_field = {**receipt_payload, "snapshot_id": "other"}
    with pytest.raises(
        PTGWaveOrdinaryTerminalConflict,
        match="immutable binding",
    ):
        terminal_receipt._validate_existing_receipt(
            stored_receipt,
            request=terminal_state["request"],
            expected_payload=changed_payload_by_field,
            keyring=receipt_keyring,
        )


def test_abandonment_signature_rejects_cryptographic_drift(monkeypatch):
    terminal_state = ordinary_result(monkeypatch)
    terminal_state["quarantine"].abandonment_receipt = copy.deepcopy(
        terminal_state["quarantine"].abandonment_receipt
    )
    terminal_state["quarantine"].abandonment_receipt["signature"] = "0" * 512

    with pytest.raises(
        PTGWaveOrdinaryTerminalConflict,
        match="abandonment signature is invalid",
    ):
        terminal_receipt._verify_abandonment_signature(
            terminal_state,
            terminal_state["request"],
            keyring(monkeypatch),
        )


def test_process_authority_rejects_invalid_roles_and_worker_counts():
    with pytest.raises(PTGWaveReceiptAuthorityError, match="reader or signer"):
        process_authority.receipt_authority_role(
            {process_authority.RECEIPT_AUTHORITY_ROLE_ENV: "writer"}
        )

    for worker_count in (object(), "many", 0, 2):
        with pytest.raises(
            PTGWaveReceiptAuthorityError,
            match="exactly one API worker",
        ):
            process_authority.require_receipt_authority_worker_count(
                worker_count
            )


def test_process_authority_rejects_reader_keys_and_absent_signer(monkeypatch):
    active_key_name = "HLTHPRT_PTG_WAVE_RECEIPT_ACTIVE_KEY_ID"
    with pytest.raises(PTGWaveReceiptAuthorityError, match="forbidden"):
        process_authority.load_process_receipt_keyring(
            {
                process_authority.RECEIPT_AUTHORITY_ROLE_ENV: "reader",
                active_key_name: "receipt-active",
            }
        )

    with pytest.raises(PTGWaveReceiptAuthorityError, match="is absent"):
        process_authority.load_process_receipt_keyring(
            {
                process_authority.RECEIPT_AUTHORITY_ROLE_ENV: "signer",
                process_authority.API_WORKERS_ENV: "1",
            }
        )

    monkeypatch.setattr(
        process_authority,
        "load_configured_receipt_keyring",
        lambda: None,
    )
    with pytest.raises(PTGWaveReceiptAuthorityError, match="is absent"):
        process_authority.load_process_receipt_keyring(
            {
                process_authority.RECEIPT_AUTHORITY_ROLE_ENV: "signer",
                process_authority.API_WORKERS_ENV: "1",
                active_key_name: "receipt-active",
            }
        )
