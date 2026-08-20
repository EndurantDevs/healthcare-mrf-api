"""Per-member ordinary terminal receipt contract tests."""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import SanicException
from sqlalchemy.dialects import postgresql

from api import control_wave_routes as routes
from api.control_import_waves import (
    _new_wave_record,
    _prepare_wave_intents,
    sign_cohort_attestation,
    validate_import_wave_payload,
)
from process.ptg_singleton_direct_control import (
    DIRECT_RATE_FILE_INTENT_CONTRACT,
    singleton_direct_intent_sha256,
    singleton_direct_source_key,
)
from process.ptg_singleton_direct_resource import PTG_SMALL_RESOURCE_CONTRACT
from process.ptg_wave_ordinary_terminal_receipt import (
    COORDINATE_DIGEST_DOMAIN,
    ENGINE_OPTIONS_DIGEST_DOMAIN,
    ENGINE_REPORT_DIGEST_DOMAIN,
    ORDINARY_TERMINAL_PAYLOAD_FIELDS,
    ORDINARY_TERMINAL_REQUEST_SCHEMA,
    PTGWaveOrdinaryTerminalConflict,
    PTGWaveOrdinaryTerminalRetryable,
    RUN_METRICS_DIGEST_DOMAIN,
    RUN_PARAMS_DIGEST_DOMAIN,
    SCOPE_DIGEST_DOMAIN,
    SNAPSHOT_MANIFEST_DIGEST_DOMAIN,
    TERMINAL_RESULT_DIGEST_DOMAIN,
    issue_ordinary_terminal_receipt,
    ordinary_terminal_receipt_payload,
    validate_ordinary_terminal_request,
)
from process.ptg_wave_quarantine_basis import (
    V12_PRISTINE_MATERIALIZED_CUTOVER_BASIS,
)
from process.ptg_wave_receipt_authority import (
    ABANDONMENT_RECEIPT_SCHEMA,
    ACTIVE_KEY_ID_ENV,
    ACTIVE_PRIVATE_KEY_FILE_ENV,
    ORDINARY_TERMINAL_RECEIPT_SCHEMA,
    PTGWaveReceiptKeyring,
)
from process.ptg_wave_receipt_contract import (
    ABANDONMENT_PROOF_SCHEMA,
    admission_receipt_mapping,
    ordinary_cutover_id,
)
from process.ptg_wave_state import canonical_json, sha256_digest
from process.ptg_wave_v12_pristine_abandonment import (
    abandonment_receipt_payload,
)
from tests.ptg_wave_supersession_fixtures import recovery_proofs
from tests.test_control_import_waves import (
    _KEY,
    _unsigned,
)
from tests.ptg_wave_receipt_test_keys import (
    EPHEMERAL_RECEIPT_PRIVATE_KEY,
    EPHEMERAL_RECEIPT_PUBLIC_MODULUS,
)


from tests.ptg_wave_ordinary_terminal_receipt_support import (
    CONTENT_VERSION,
    ENGINE_IMPORT_RUN_ID,
    IMPORT_MONTH,
    ISSUED_AT,
    NODE_ID,
    OPERATION_ID,
    ORDINARY_IMPORT_ID,
    ORDINARY_PLAN_IDS,
    ORDINARY_RUN_ID,
    PLAN_IDS,
    PLAN_MARKET_TYPES,
    QueuedTerminalSession as _QueuedTerminalSession,
    ScalarResult as _ScalarResult,
    SNAPSHOT_ID,
    SOURCE_FILE_ID,
    TerminalTransaction as _TerminalTransaction,
    direct_v6_boundary as _direct_v6_boundary,
    keyring as _keyring,
    ordinary_result as _ordinary_result,
    v13_ordinary_result as _v13_ordinary_result,
)
from tests.ptg_blank_terminal_support import (
    blank_ordinary_result as _blank_ordinary_result,
)


def test_builds_per_member_terminal_payload_from_later_ordinary_run(monkeypatch):
    state = _ordinary_result(monkeypatch)

    payload = ordinary_terminal_receipt_payload(**state)

    assert set(payload) == ORDINARY_TERMINAL_PAYLOAD_FIELDS
    assert payload["operation_id"] == OPERATION_ID
    assert payload["member_ordinal"] == 0
    assert payload["source_file_import_id"] == ORDINARY_IMPORT_ID
    assert payload["run_id"] == ORDINARY_RUN_ID
    assert payload["run_id"] != state["intent"].run_id
    assert payload["coordinate"] == {
        "source_file_id": SOURCE_FILE_ID,
        "content_version": CONTENT_VERSION,
        "import_month": IMPORT_MONTH,
        "historical_source_file_import_id": "candidate-neutral-v12",
        "direct_input_digest": state["intent"].params[
            "direct_rate_file_intent_sha256"
        ],
    }
    assert payload["scope"]["plan_ids"] == ORDINARY_PLAN_IDS
    assert payload["scope"]["admission_plan_ids"] == PLAN_IDS
    assert payload["terminal_result"]["engine_result_status"] == "validated"
    assert payload["terminal_result"]["finished_at"] == (
        "2026-08-10T13:14:15.123456Z"
    )
    assert len(payload["terminal_result_digest"]) == 64


def test_builds_authenticated_blank_terminal_payload(monkeypatch):
    state = _blank_ordinary_result(monkeypatch)

    payload = ordinary_terminal_receipt_payload(**state)

    assert set(payload) == ORDINARY_TERMINAL_PAYLOAD_FIELDS
    assert payload["terminal_result"]["status"] == "blank"
    assert payload["terminal_result"]["engine_result_status"] == "failed"
    assert payload["snapshot_id"] == SNAPSHOT_ID

    state["engine_snapshot"].manifest["allowed_amount_lane"][
        "successful_files"
    ][0]["summary"]["allowed_amount_payments"] = 1
    with pytest.raises(
        PTGWaveOrdinaryTerminalConflict,
        match="durable PTG result",
    ):
        ordinary_terminal_receipt_payload(**state)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            lambda state: setattr(
                state["run"], "run_id", state["intent"].run_id
            ),
            "run identity|frozen V12 member",
        ),
        (
            lambda state: state["run"].params.update(source_key="ptg_wrong"),
            "frozen V12 member",
        ),
        (
            lambda state: state["run"].params.update(plan_ids=["wrong"]),
            "durable PTG result",
        ),
        (
            lambda state: state["run"].params.update(
                ordinary_cutover_operation_id="8" * 64,
                ordinary_cutover_id=ordinary_cutover_id("8" * 64),
            ),
            "frozen V12 member",
        ),
        (
            lambda state: setattr(state["run"], "status", "running"),
            "frozen V12 member",
        ),
        (
            lambda state: setattr(state["engine_run"], "status", "failed"),
            "durable PTG result",
        ),
        (
            lambda state: state.update(quarantine=None),
            "signed V12 abandonment",
        ),
    ),
)
def test_rejects_wave_run_scope_result_and_abandonment_drift(
    monkeypatch,
    mutation,
    message,
):
    state = _ordinary_result(monkeypatch)
    mutation(state)

    with pytest.raises(PTGWaveOrdinaryTerminalConflict, match=message):
        ordinary_terminal_receipt_payload(**state)


def test_request_and_digest_domains_are_frozen(monkeypatch):
    state = _ordinary_result(monkeypatch)
    request = state["request"]
    assert validate_ordinary_terminal_request(
        request, operation_id=OPERATION_ID
    ) == request
    for domain in (
        COORDINATE_DIGEST_DOMAIN,
        SCOPE_DIGEST_DOMAIN,
        TERMINAL_RESULT_DIGEST_DOMAIN,
        RUN_PARAMS_DIGEST_DOMAIN,
        RUN_METRICS_DIGEST_DOMAIN,
        ENGINE_OPTIONS_DIGEST_DOMAIN,
        ENGINE_REPORT_DIGEST_DOMAIN,
        SNAPSHOT_MANIFEST_DIGEST_DOMAIN,
    ):
        assert domain.startswith("healthporta.ptg-wave-ordinary-terminal-")
        assert domain.endswith(".v1")
    assert ORDINARY_TERMINAL_RECEIPT_SCHEMA == (
        "healthporta.ptg-wave-ordinary-terminal-receipt.v1"
    )

    invalid = copy.deepcopy(request)
    invalid["extra"] = True
    with pytest.raises(PTGWaveOrdinaryTerminalConflict, match="fields"):
        validate_ordinary_terminal_request(invalid)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state_factory",
    (_ordinary_result, _v13_ordinary_result, _blank_ordinary_result),
)
async def test_issue_reads_one_member_and_replays_exact_receipt(
    monkeypatch,
    state_factory,
):
    """Prove issuance reads one member and replays the stored envelope."""
    from process import ptg_wave_ordinary_terminal_receipt as terminal_module

    state = state_factory(monkeypatch)
    first_session = _QueuedTerminalSession(
        [
            None,
            state["wave"],
            state["intent"],
            state["quarantine"],
            state["run"],
            state["engine_run"],
            state["engine_snapshot"],
        ]
    )
    transactions = [_TerminalTransaction(first_session)]
    monkeypatch.setattr(
        terminal_module.db,
        "transaction",
        lambda: transactions.pop(0),
    )
    release_pin = AsyncMock(return_value=1)
    monkeypatch.setattr(
        terminal_module,
        "_release_terminal_snapshot_pin",
        release_pin,
    )
    keyring = _keyring(monkeypatch)
    receipt, created = await issue_ordinary_terminal_receipt(
        OPERATION_ID,
        state["request"],
        receipt_keyring=keyring,
        receipt_issued_at=ISSUED_AT,
    )
    existing = _assert_first_terminal_receipt_write(
        created,
        first_session,
        release_pin,
    )
    await _assert_terminal_receipt_replay(
        state,
        keyring,
        transactions,
        release_pin,
        existing,
        receipt,
    )


def _assert_first_terminal_receipt_write(created, session, release_pin):
    """Assert one-member reads and the first durable receipt write."""
    assert created is True
    assert session.flush_count == 1
    assert len(session.added) == 1
    assert release_pin.await_count == 1
    assert release_pin.await_args.kwargs["payload"]["snapshot_id"] == SNAPSHOT_ID
    statement_sql_texts = [
        str(statement.compile(dialect=postgresql.dialect()))
        for statement, params in session.statements
        if params is None
    ]
    member_queries = [
        sql for sql in statement_sql_texts if "ptg_import_wave_intent" in sql
    ]
    assert len(member_queries) == 1
    assert "ptg_import_wave_intent.wave_id =" in member_queries[0]
    assert "ptg_import_wave_intent.ordinal =" in member_queries[0]
    assert all("count(" not in sql.lower() for sql in statement_sql_texts)
    return session.added[0]


async def _assert_terminal_receipt_replay(
    state,
    keyring,
    transactions,
    release_pin,
    existing,
    receipt,
) -> None:
    """Replay the persisted envelope without another write."""
    replay_session = _QueuedTerminalSession(
        [
            existing,
            state["wave"],
            state["intent"],
            state["quarantine"],
            state["run"],
            state["engine_run"],
            state["engine_snapshot"],
        ]
    )
    transactions.append(_TerminalTransaction(replay_session))
    replay, replay_created = await issue_ordinary_terminal_receipt(
        OPERATION_ID,
        state["request"],
        receipt_keyring=keyring,
        receipt_issued_at="2026-08-10T13:00:00.000000Z",
    )
    assert replay_created is False
    assert replay == receipt
    assert replay_session.flush_count == 0
    assert release_pin.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("sqlstate", ("55P03", "57014"))
async def test_issue_bounds_database_wait_and_returns_retryable(
    monkeypatch,
    sqlstate,
):
    from process import ptg_wave_ordinary_terminal_receipt as terminal_module

    state = _ordinary_result(monkeypatch)

    class DatabaseWaitExpired(Exception):
        pass

    failure = DatabaseWaitExpired("bounded wait expired")
    failure.sqlstate = sqlstate

    class TimeoutSession:
        def __init__(self):
            self.statements = []

        async def execute(self, statement, params=None):
            self.statements.append((str(statement), params))
            if len(self.statements) == 2:
                raise failure
            return _ScalarResult(None)

    session = TimeoutSession()
    monkeypatch.setattr(
        terminal_module.db,
        "transaction",
        lambda: _TerminalTransaction(session),
    )

    with pytest.raises(
        PTGWaveOrdinaryTerminalRetryable,
        match="database wait expired; retry",
    ):
        await issue_ordinary_terminal_receipt(
            OPERATION_ID,
            state["request"],
            receipt_keyring=_keyring(monkeypatch),
        )

    first_sql, first_params = session.statements[0]
    assert "set_config('lock_timeout'" in first_sql
    assert "set_config('statement_timeout'" in first_sql
    assert first_params == {
        "lock_timeout": terminal_module.ORDINARY_TERMINAL_LOCK_TIMEOUT,
        "statement_timeout": (
            terminal_module.ORDINARY_TERMINAL_STATEMENT_TIMEOUT
        ),
    }


def test_identical_coordinate_cross_operation_run_swap_is_rejected(monkeypatch):
    state = _ordinary_result(monkeypatch)
    coordinate_before_by_field = {
        "source_key": state["run"].params["source_key"],
        "selector": state["run"].params["in_network_url"],
        "input_digest": state["run"].params[
            "ordinary_cutover_direct_input_digest"
        ],
    }
    other_operation_id = "8" * 64
    state["run"].params.update(
        ordinary_cutover_operation_id=other_operation_id,
        ordinary_cutover_id=ordinary_cutover_id(other_operation_id),
    )
    assert coordinate_before_by_field == {
        "source_key": state["run"].params["source_key"],
        "selector": state["run"].params["in_network_url"],
        "input_digest": state["run"].params[
            "ordinary_cutover_direct_input_digest"
        ],
    }
    with pytest.raises(
        PTGWaveOrdinaryTerminalConflict,
        match="frozen V12 member",
    ):
        ordinary_terminal_receipt_payload(**state)


@pytest.mark.asyncio
async def test_route_returns_first_write_and_exact_replay_status(monkeypatch):
    state = _ordinary_result(monkeypatch)
    response_receipt_by_field = {
        "schema": ORDINARY_TERMINAL_RECEIPT_SCHEMA,
        "key_id": state["request"]["key_id"],
        "issued_at": ISSUED_AT,
        "payload": ordinary_terminal_receipt_payload(**state),
        "payload_digest": "1" * 64,
        "signature": "2" * 512,
    }
    service = AsyncMock(side_effect=((response_receipt_by_field, True), (response_receipt_by_field, False)))
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(
        routes,
        "issue_ordinary_terminal_receipt",
        service,
    )
    process_keyring = object()
    request = SimpleNamespace(
        json=state["request"],
        app=SimpleNamespace(
            ctx=SimpleNamespace(ptg_wave_receipt_keyring=process_keyring)
        ),
    )

    first = await routes.control_issue_ordinary_terminal_receipt(
        request,
        OPERATION_ID,
    )
    replay = await routes.control_issue_ordinary_terminal_receipt(
        request,
        OPERATION_ID,
    )

    assert first.status == 201
    assert replay.status == 200
    assert json.loads(first.body) == response_receipt_by_field
    assert json.loads(replay.body) == response_receipt_by_field
    assert service.await_args_list[0].kwargs == {
        "receipt_keyring": process_keyring,
    }


@pytest.mark.asyncio
async def test_route_marks_bounded_database_wait_retryable(monkeypatch):
    state = _ordinary_result(monkeypatch)
    monkeypatch.setattr(routes, "require_control_auth", lambda _request: None)
    monkeypatch.setattr(
        routes,
        "issue_ordinary_terminal_receipt",
        AsyncMock(
            side_effect=PTGWaveOrdinaryTerminalRetryable(
                "ordinary terminal receipt database wait expired; retry"
            )
        ),
    )
    request = SimpleNamespace(
        json=state["request"],
        app=SimpleNamespace(
            ctx=SimpleNamespace(ptg_wave_receipt_keyring=object())
        ),
    )

    with pytest.raises(SanicException) as failure:
        await routes.control_issue_ordinary_terminal_receipt(
            request,
            OPERATION_ID,
        )

    assert failure.value.status_code == 503
    assert failure.value.headers == {"Retry-After": "1"}
