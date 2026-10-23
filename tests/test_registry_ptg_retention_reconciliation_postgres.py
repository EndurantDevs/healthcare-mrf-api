# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native service interruption, exact reconciliation and committed review replay."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest

from process import registry_ptg_office_approval as office_approval
from process import registry_ptg_published_plan_scope as published_approval
from process import registry_ptg_source_retention as retention
from process.registry_company_approval_fence import require_registry_company_approval_fence
from process.registry_ptg_published_plan_contract import INTENT_FIELDS
from process.registry_ptg_scope_engine import RegistryPTGScopeDeadlineExpired
from tests.test_registry_ptg_office_coordinator_postgres import (
    coordinator_db as coordinator_db,
)
from tests.test_registry_ptg_office_coordinator_postgres import (
    custom_db as custom_db,
)
from tests.test_registry_ptg_office_coordinator_postgres import (
    office_db as office_db,
)
from tests.test_registry_ptg_office_coordinator_postgres import (
    office_publisher as office_publisher,
)
from tests.test_registry_ptg_office_coordinator_postgres import (
    serving_schema as serving_schema,
)
from tests.test_registry_ptg_scope_engine_postgres import _approve_published_association, _seed_approved_company

pytestmark = pytest.mark.asyncio
_REVIEW_TABLES = (
    "registry_ptg_published_plan_scope",
    "registry_ptg_office_approval",
    "registry_ptg_office_hold",
)


async def _stored_state(case):
    connection = case.native.custom.connection
    state_by_table = {}
    for table in _REVIEW_TABLES:
        state_by_table[table] = await connection.fetch(
            f'SELECT to_jsonb(stored)::text FROM "{case.service.store.control_schema}"."{table}" stored '
            "ORDER BY to_jsonb(stored)::text"
        )
    state_by_table["pins"] = await connection.fetch(
        f'SELECT to_jsonb(pin)::text FROM "{case.service.schema_name}".ptg2_snapshot_pin pin '
        "ORDER BY owner_type,owner_id,snapshot_id"
    )
    return state_by_table


async def _assert_retained_pin(case):
    pin = case.authority["pin"]
    stored = await case.native.custom.connection.fetch(
        f'SELECT snapshot_id,reason FROM "{case.service.schema_name}".ptg2_snapshot_pin '
        "WHERE owner_type=$1 AND owner_id=$2 ORDER BY snapshot_id",
        pin["owner_type"],
        pin["owner_id"],
    )
    assert [tuple(row) for row in stored] == [(pin["snapshot_id"], pin["reason"])]


async def _published_envelope(native, service):
    command = deepcopy(native.request["source_command"])
    document = deepcopy(native.fixture.source.document)
    document["command"]["legal_company_id"] = str(uuid4())
    await _seed_approved_company(native.custom.engine, service.store.control_schema, document)
    network_id, revision = await _approve_published_association(
        SimpleNamespace(engine=native.custom.engine, control=service.store.control_schema, document=document)
    )
    intent_by_field = {name: command[name] for name in INTENT_FIELDS}
    intent_by_field.update(
        scope_id=str(uuid4()),
        statement_id=str(uuid4()),
        idempotency_key=uuid4().hex,
        legal_company_id=document["command"]["legal_company_id"],
        network_id=network_id,
        approved_revision=revision,
    )
    actor_by_field = {name: native.request[name] for name in ("actor", "session_token_sha256")}
    preview = await service.preview({**actor_by_field, "command": intent_by_field, "ownership": command["ownership"]})
    return {**actor_by_field, **preview}


@pytest.fixture(params=[False, True], ids=["published", "office"])
async def retained_review(coordinator_db, request, monkeypatch):
    native, office = coordinator_db, request.param
    service = native.service.preparation.scope_service
    connection, schema = native.custom.connection, service.store.control_schema
    approver, retained_reader = service.store.approval_role, native.custom.roles["reader"]
    is_membership_added = False
    try:
        if office:
            prepared = await native.service.prepare(native.request)
            for table in _REVIEW_TABLES[1:]:
                await connection.execute(f'GRANT INSERT ON "{schema}"."{table}" TO "{approver}"')
            await connection.execute(
                f'GRANT "{retained_reader}" TO "{approver}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE'
            )
            is_membership_added = True
            envelope_by_field = {
                name: native.request[name] for name in ("actor", "session_token_sha256", "read_limits")
            }
            envelope_by_field["command"] = prepared["preparation"]["command"]
        else:
            envelope_by_field = await _published_envelope(native, service)
        case = SimpleNamespace(
            native=native,
            service=service,
            envelope=envelope_by_field,
            office=office,
            authority=None,
            approve=service.approve_offices if office else service.approve,
            approval_module=office_approval if office else published_approval,
            retain_name="retain_office_review_source" if office else "retain_published_review_source",
        )
        actual_retain = getattr(retention, case.retain_name)

        async def observe_committed_source(*args, **kwargs):
            case.authority = await actual_retain(*args, **kwargs)
            await _assert_retained_pin(case)
            return case.authority

        monkeypatch.setattr(retention, case.retain_name, observe_committed_source)
        yield case
    finally:
        if is_membership_added:
            await connection.execute(f'REVOKE "{retained_reader}" FROM "{approver}"')
        if office:
            for table in _REVIEW_TABLES[1:]:
                await connection.execute(f'REVOKE INSERT ON "{schema}"."{table}" FROM "{approver}"')


async def test_native_interruption_after_publisher_commit_releases_only_unapproved_operation(
    retained_review, monkeypatch
):
    case = retained_review
    original_state = await _stored_state(case)
    actual_retain = getattr(retention, case.retain_name)
    interruption = asyncio.CancelledError()

    async def interrupt_after_commit(*args, **kwargs):
        await actual_retain(*args, **kwargs)
        committed = await _stored_state(case)
        assert len(committed["pins"]) == len(original_state["pins"]) + 1
        assert all(committed[table] == original_state[table] for table in _REVIEW_TABLES)
        raise interruption

    monkeypatch.setattr(retention, case.retain_name, interrupt_after_commit)
    with pytest.raises(asyncio.CancelledError) as caught:
        await case.approve(deepcopy(case.envelope))
    assert caught.value is interruption
    assert await _stored_state(case) == original_state
    assert await retention.reconcile_unapproved_review_source(case.service, case.authority) == "already_released"
    assert await _stored_state(case) == original_state


async def test_native_lost_approver_commit_ack_preserves_original_review_hold_and_pin(retained_review, monkeypatch):
    case = retained_review
    original_state = await _stored_state(case)
    actual_transaction = case.approval_module.registry_company_approval_transaction
    committed_states = []

    @asynccontextmanager
    async def lose_committed_ack(sessions, **kwargs):
        async with actual_transaction(sessions, **kwargs) as session:
            yield session
        if not committed_states:
            committed_states.append(await _stored_state(case))
            raise TimeoutError("synthetic committed reply loss")

    monkeypatch.setattr(case.approval_module, "registry_company_approval_transaction", lose_committed_ack)
    with pytest.raises(RegistryPTGScopeDeadlineExpired) as caught:
        await case.approve(deepcopy(case.envelope))
    assert caught.value.outcome_unknown is True
    committed = committed_states[0]
    approval_table = _REVIEW_TABLES[1] if case.office else _REVIEW_TABLES[0]
    assert len(committed[approval_table]) == len(original_state[approval_table]) + 1
    assert len(committed["pins"]) == len(original_state["pins"]) + 1
    assert len(committed[_REVIEW_TABLES[2]]) == len(original_state[_REVIEW_TABLES[2]]) + int(case.office)
    assert await _stored_state(case) == committed
    assert await retention.reconcile_unapproved_review_source(case.service, case.authority) == "retained"
    first = await case.approve(deepcopy(case.envelope))
    assert await case.approve(deepcopy(case.envelope)) == first
    assert await _stored_state(case) == committed
    await _assert_retained_pin(case)


async def test_native_reconciliation_cannot_pass_open_approval_company_fence(retained_review, monkeypatch):
    case = retained_review
    original_state = await _stored_state(case)
    actual_transaction = case.approval_module.registry_company_approval_transaction
    inserted, finish = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def pause_before_native_commit(sessions, **kwargs):
        async with actual_transaction(sessions, **kwargs) as session:
            yield session
            await require_registry_company_approval_fence(session, case.service.store.control_schema)
            inserted.set()
            await finish.wait()

    monkeypatch.setattr(case.approval_module, "registry_company_approval_transaction", pause_before_native_commit)
    worker = asyncio.create_task(case.approve(deepcopy(case.envelope)))
    try:
        await asyncio.wait_for(inserted.wait(), timeout=7)
        uncommitted = await _stored_state(case)
        assert all(uncommitted[table] == original_state[table] for table in _REVIEW_TABLES)
        assert len(uncommitted["pins"]) == len(original_state["pins"]) + 1
        with pytest.raises(ValueError, match="registry_ptg_scope_company_busy"):
            await retention.reconcile_unapproved_review_source(case.service, case.authority)
        assert await _stored_state(case) == uncommitted
        await _assert_retained_pin(case)
        finish.set()
        await asyncio.wait_for(worker, timeout=7)
        committed = await _stored_state(case)
        assert committed != uncommitted
        assert await retention.reconcile_unapproved_review_source(case.service, case.authority) == "retained"
        assert await _stored_state(case) == committed
    finally:
        finish.set()
        if not worker.done():
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
