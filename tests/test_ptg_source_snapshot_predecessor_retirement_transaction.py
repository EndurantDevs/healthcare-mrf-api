# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import source_snapshot_predecessor_retirement as retirement
from process.ptg_parts import source_snapshot_predecessor_retirement_state as state
from process.ptg_parts.source_snapshot_predecessor_retirement_types import (
    PTG2PredecessorRetirementConflict,
)
from tests.test_ptg_source_snapshot_predecessor_retirement import (
    ACTOR,
    IDEMPOTENCY_KEY,
    REASON,
    _context,
    _coordinates,
    _snapshot,
    _source_pointer,
)


class _Transaction:
    def __init__(self):
        self.session = object()
        self.entered = 0
        self.exited = 0

    async def __aenter__(self):
        self.entered += 1
        return self.session

    async def __aexit__(self, _exc_type, _exc, _traceback):
        self.exited += 1
        return False


def _request():
    return retirement.normalized_predecessor_retirement_request(
        **_coordinates(),
        actor=ACTOR,
        reason=REASON,
        idempotency_key=IDEMPOTENCY_KEY,
    )


def _audit_by_field(request):
    return {
        **request.audit_coordinates(),
        "request_digest": request.request_digest,
        "retired_at": datetime.datetime(
            2026,
            7,
            27,
            tzinfo=datetime.UTC,
        ),
        "cleared_source_pointer_count": 1,
        "cleared_plan_pointer_count": 1,
        "cleared_global_pointer_count": 1,
        "deleted_rollback_pin_count": 1,
    }


def _install_ordered_operation_fakes(
    monkeypatch,
    transaction,
    request,
    decision,
    events,
):
    async def acquire_lock(session):
        assert session is transaction.session
        events.append("locked")

    async def load_audit(session, **_kwargs):
        assert session is transaction.session
        events.append("audit_loaded")
        return {}

    async def load_context(session, **_kwargs):
        assert session is transaction.session
        events.append("context_loaded")
        return _context()

    async def apply_changes(session, **kwargs):
        assert session is transaction.session
        assert kwargs["request"] == request
        assert kwargs["decision"] == decision
        events.append("mutated")

    async def validate_layout(session, **_kwargs):
        assert session is transaction.session
        events.append("removal_validated")

    async def postcheck(session, **_kwargs):
        assert session is transaction.session
        events.append("postchecked")

    async def insert_audit(session, **kwargs):
        assert session is transaction.session
        assert kwargs["request"] == request
        events.append("audited")
        return _audit_by_field(request)

    _patch_ordered_operation_fakes(
        monkeypatch,
        transaction,
        decision,
        acquire_lock=acquire_lock,
        load_audit=load_audit,
        load_context=load_context,
        validate_layout=validate_layout,
        apply_changes=apply_changes,
        postcheck=postcheck,
        insert_audit=insert_audit,
    )


def _patch_ordered_operation_fakes(
    monkeypatch,
    transaction,
    decision,
    **operation_by_name,
):
    monkeypatch.setattr(retirement.db, "transaction", lambda: transaction)
    monkeypatch.setattr(
        retirement,
        "acquire_ptg2_lifecycle_lock",
        operation_by_name["acquire_lock"],
    )
    monkeypatch.setattr(
        retirement,
        "load_retirement_audit",
        operation_by_name["load_audit"],
    )
    monkeypatch.setattr(
        retirement,
        "load_retirement_context",
        operation_by_name["load_context"],
    )
    monkeypatch.setattr(
        retirement,
        "predecessor_retirement_decision",
        lambda *_args, **_kwargs: decision,
    )
    monkeypatch.setattr(
        retirement,
        "validate_retirement_shared_layout",
        operation_by_name["validate_layout"],
    )
    monkeypatch.setattr(
        retirement,
        "apply_predecessor_retirement",
        operation_by_name["apply_changes"],
    )
    monkeypatch.setattr(
        retirement,
        "postcheck_predecessor_retirement",
        operation_by_name["postcheck"],
    )
    monkeypatch.setattr(
        retirement,
        "insert_retirement_audit",
        operation_by_name["insert_audit"],
    )


@pytest.mark.asyncio
async def test_retirement_locks_validates_mutates_and_audits_atomically(
    monkeypatch,
):
    transaction = _Transaction()
    events: list[str] = []
    decision = state.predecessor_retirement_decision(
        _context(),
        **_coordinates(),
    )
    request = _request()
    _install_ordered_operation_fakes(
        monkeypatch,
        transaction,
        request,
        decision,
        events,
    )

    report = await retirement.retire_ptg2_source_predecessor(
        **_coordinates(),
        actor=ACTOR,
        reason=REASON,
        idempotency_key=IDEMPOTENCY_KEY,
    )

    assert report["status"] == "retired"
    assert report["idempotent"] is False
    assert events == [
        "locked",
        "audit_loaded",
        "context_loaded",
        "removal_validated",
        "mutated",
        "postchecked",
        "audited",
    ]
    assert transaction.entered == transaction.exited == 1


@pytest.mark.asyncio
async def test_exact_replay_is_write_free_and_key_reuse_conflicts(monkeypatch):
    transaction = _Transaction()
    request = _request()
    audit_by_field = _audit_by_field(request)
    load_context = AsyncMock()
    apply_changes = AsyncMock()
    monkeypatch.setattr(retirement.db, "transaction", lambda: transaction)
    monkeypatch.setattr(
        retirement,
        "acquire_ptg2_lifecycle_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        retirement,
        "load_retirement_audit",
        AsyncMock(return_value=audit_by_field),
    )
    monkeypatch.setattr(retirement, "load_retirement_context", load_context)
    monkeypatch.setattr(retirement, "apply_predecessor_retirement", apply_changes)

    replay = await retirement.retire_ptg2_source_predecessor(
        **_coordinates(),
        actor=ACTOR,
        reason=REASON,
        idempotency_key=IDEMPOTENCY_KEY,
    )

    assert replay["status"] == "already_retired"
    assert replay["idempotent"] is True
    load_context.assert_not_awaited()
    apply_changes.assert_not_awaited()

    audit_by_field["request_digest"] = "0" * 64
    with pytest.raises(
        PTG2PredecessorRetirementConflict,
        match="idempotency key",
    ):
        await retirement.retire_ptg2_source_predecessor(
            **_coordinates(),
            actor=ACTOR,
            reason=REASON,
            idempotency_key=IDEMPOTENCY_KEY,
        )
    audit_by_field["request_digest"] = request.request_digest
    with pytest.raises(
        PTG2PredecessorRetirementConflict,
        match="idempotency key",
    ):
        await retirement.retire_ptg2_source_predecessor(
            **{
                **_coordinates(),
                "rollback_pin_mode": "absent",
                "rollback_owner_id": None,
            },
            actor=ACTOR,
            reason=REASON,
            idempotency_key=IDEMPOTENCY_KEY,
        )


@pytest.mark.asyncio
async def test_stale_conflict_performs_no_mutation_or_audit(monkeypatch):
    transaction = _Transaction()
    stale_context = _context(
        source_pointer_records=(
            _source_pointer(previous_snapshot_id="different-predecessor"),
        )
    )
    apply_changes = AsyncMock()
    insert_audit = AsyncMock()
    monkeypatch.setattr(retirement.db, "transaction", lambda: transaction)
    monkeypatch.setattr(
        retirement,
        "acquire_ptg2_lifecycle_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        retirement,
        "load_retirement_audit",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        retirement,
        "load_retirement_context",
        AsyncMock(return_value=stale_context),
    )
    monkeypatch.setattr(
        retirement,
        "apply_predecessor_retirement",
        apply_changes,
    )
    monkeypatch.setattr(
        retirement,
        "insert_retirement_audit",
        insert_audit,
    )

    with pytest.raises(
        PTG2PredecessorRetirementConflict,
        match="source pointer",
    ):
        await retirement.retire_ptg2_source_predecessor(
            **_coordinates(),
            actor=ACTOR,
            reason=REASON,
            idempotency_key=IDEMPOTENCY_KEY,
        )

    apply_changes.assert_not_awaited()
    insert_audit.assert_not_awaited()


@pytest.mark.parametrize(
    ("runtime_schema", "legacy_schema", "expected_schema"),
    [
        (None, "legacy_tenant", "legacy_tenant"),
        ("runtime_tenant", None, "runtime_tenant"),
        ("shared_tenant", "shared_tenant", "shared_tenant"),
    ],
)
@pytest.mark.asyncio
async def test_retirement_forwards_shared_schema_alias_resolution(
    monkeypatch,
    runtime_schema,
    legacy_schema,
    expected_schema,
):
    if runtime_schema is None:
        monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    else:
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", runtime_schema)
    if legacy_schema is None:
        monkeypatch.delenv("DB_SCHEMA", raising=False)
    else:
        monkeypatch.setenv("DB_SCHEMA", legacy_schema)
    transaction = _Transaction()
    execute_retirement = AsyncMock(return_value={"status": "retired"})
    monkeypatch.setattr(retirement.db, "transaction", lambda: transaction)
    monkeypatch.setattr(
        retirement,
        "_execute_predecessor_retirement",
        execute_retirement,
    )

    await retirement.retire_ptg2_source_predecessor(
        **_coordinates(),
        actor=ACTOR,
        reason=REASON,
        idempotency_key=IDEMPOTENCY_KEY,
    )

    assert execute_retirement.await_args.kwargs["schema_name"] == expected_schema


@pytest.mark.asyncio
async def test_schema_conflict_fails_before_database_transaction(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_tenant")
    monkeypatch.setenv("DB_SCHEMA", "legacy_tenant")
    transaction_factory = Mock(return_value=_Transaction())
    monkeypatch.setattr(retirement.db, "transaction", transaction_factory)

    with pytest.raises(RuntimeError, match="must identify the same schema"):
        await retirement.retire_ptg2_source_predecessor(
            **_coordinates(),
            actor=ACTOR,
            reason=REASON,
            idempotency_key=IDEMPOTENCY_KEY,
        )

    transaction_factory.assert_not_called()


def _serving_manifest(**overrides):
    serving_index_by_field = {
        "source_key": "synthetic-source",
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "shared_snapshot_key": 17,
    }
    serving_index_by_field.update(overrides)
    return {"serving_index": serving_index_by_field}


def _install_removal_contract_fakes(
    monkeypatch,
    transaction,
    context,
    layout_error,
):
    apply_changes = AsyncMock()
    insert_audit = AsyncMock()
    monkeypatch.setattr(retirement.db, "transaction", lambda: transaction)
    monkeypatch.setattr(
        retirement,
        "acquire_ptg2_lifecycle_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(
        retirement,
        "load_retirement_audit",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        retirement,
        "load_retirement_context",
        AsyncMock(return_value=context),
    )
    monkeypatch.setattr(
        retirement,
        "validate_retirement_shared_layout",
        AsyncMock(side_effect=layout_error),
    )
    monkeypatch.setattr(
        retirement,
        "apply_predecessor_retirement",
        apply_changes,
    )
    monkeypatch.setattr(
        retirement,
        "insert_retirement_audit",
        insert_audit,
    )
    return apply_changes, insert_audit


@pytest.mark.parametrize(
    ("predecessor_manifest", "layout_error"),
    [
        (_serving_manifest(arch_version="unsupported"), None),
        (_serving_manifest(storage_generation="unsupported"), None),
        (_serving_manifest(shared_snapshot_key=None), None),
        (_serving_manifest(shared_snapshot_key="017"), None),
        (
            _serving_manifest(storage_generation="shared_blocks_v4"),
            ValueError(
                "PTG V4 snapshot binding is missing its complete packed map root"
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_removal_contract_conflict_is_write_free(
    monkeypatch,
    predecessor_manifest,
    layout_error,
):
    context = _context(
        snapshot_records=(
            _snapshot("snapshot-current", "snapshot-previous"),
            {
                **_snapshot("snapshot-previous", None),
                "manifest": predecessor_manifest,
            },
        )
    )
    transaction = _Transaction()
    apply_changes, insert_audit = _install_removal_contract_fakes(
        monkeypatch,
        transaction,
        context,
        layout_error,
    )

    with pytest.raises(
        PTG2PredecessorRetirementConflict,
        match="removal contract",
    ):
        await retirement.retire_ptg2_source_predecessor(
            **_coordinates(),
            actor=ACTOR,
            reason=REASON,
            idempotency_key=IDEMPOTENCY_KEY,
        )

    apply_changes.assert_not_awaited()
    insert_audit.assert_not_awaited()
