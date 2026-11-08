# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Atomic owner COPY contracts with synthetic transaction and native-driver fakes."""

import asyncio
import json
import time
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import import family_block_store as store
from process.custom_import.continuation_runtime import BuildContinuationContext
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import lease_token_sha256
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_validate import CandidateScope, ClosedCandidateIdentity
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_source import _request


def _layout():
    data = json.loads((Path(__file__).parent / "fixtures/custom_import/v1_valid.json").read_text())
    return FamilyBlockLayout(CustomImportDefinition.from_mapping(data), {"rates": 7})


def _proof(layout, batches):
    proof = store.ShardOutputProof(layout, 0, b"p" * 32, 3)
    for batch in batches:
        for role, rows in batch.items():
            proof.add(role, rows)
    return proof.receipt()


@pytest.mark.parametrize("continued", (False, True))
def test_block_native_authority_keeps_producer_token_separate_from_current_lease(continued):
    current_token = b"current-lease-token" * 2
    old_token = b"original-producer-token" * 2
    original = lease_token_sha256(old_token)
    request = SimpleNamespace(
        execution_id=11,
        fence=7,
        lease_token=current_token,
        continuation=BuildContinuationContext(41, 51, original) if continued else None,
    )
    assert store._authority(request) == (
        ("bigint", 11),
        ("bigint", 7),
        ("bytea", original if continued else lease_token_sha256(current_token)),
    )
    assert request.lease_token == current_token


def _continued_candidate_boundary(monkeypatch, family, descriptor, original, locked):
    observed_results = iter((family, 71, False, descriptor))

    async def owner(_session):
        return None

    async def execute(_session, _statement, _parameters=None):
        if isinstance(_statement, str) and _statement.startswith("LOCK TABLE"):
            assert locked and _statement.endswith("ROW EXCLUSIVE MODE NOWAIT")
            return None
        if getattr(_statement, "is_select", False):
            model = _statement.column_descriptions[0]["expr"]
            if model is store.CustomImportSnapshotFamily or model is store.CustomImportFamilyBlockLayout:
                assert _statement.get_execution_options().get("populate_existing") is True
                assert (_statement._for_update_arg is not None) is locked
        value = next(observed_results)
        return SimpleNamespace(scalar_one=lambda: value, scalar_one_or_none=lambda: value)

    async def control(_session):
        return dialect().identifier_preparer, "synthetic"

    async def call(_session, name, arguments):
        assert locked
        assert name == "lock_custom_import_writable_snapshot"
        assert arguments[-1] == ("bytea", original)
        return SimpleNamespace(scalar_one=lambda: 61)

    async def binding(*_args, **_kwargs):
        return "checked-binding"

    monkeypatch.setattr(store, "_owner", owner)
    monkeypatch.setattr(store, "_execute", execute)
    monkeypatch.setattr(store, "_control_schema", control)
    monkeypatch.setattr(store, "_call", call)
    monkeypatch.setattr(store, "resolve_family_block_binding", binding)


@pytest.mark.parametrize("changed_producer", (False, True))
@pytest.mark.parametrize("locked", (False, True))
def test_continued_candidate_compares_original_producer_and_refuses_rebinding(monkeypatch, changed_producer, locked):
    layout = _layout()
    original = lease_token_sha256(b"original-producer-token" * 2)
    request = SimpleNamespace(
        dataset_id=13,
        definition_revision_id=14,
        schema_revision_id=15,
        execution_id=11,
        fence=7,
        lease_token=b"current-lease-token" * 2,
        definition=layout.definition,
        continuation=BuildContinuationContext(41, 51, original),
    )
    family = SimpleNamespace(
        family_id=61,
        frozen_at=None,
        generation_id=None,
        capture_bundle_id=71,
        dataset_id=13,
        definition_revision_id=14,
        schema_revision_id=15,
        execution_id=11,
        producing_fence=7,
        producing_token_sha256=b"x" * 32 if changed_producer else original,
    )
    descriptor = SimpleNamespace(phase="loading", layout_sha256=layout.digest)

    _continued_candidate_boundary(monkeypatch, family, descriptor, original, locked)
    operation = store._candidate(None, request, 61, layout, {"loading"}, locked=locked)
    if changed_producer:
        with pytest.raises(FamilyBlockUnavailable, match="producer"):
            asyncio.run(operation)
    else:
        assert asyncio.run(operation) == (descriptor, "checked-binding")


def test_shard_digest_binds_ids_order_scalars_role_across_chunks():
    layout = _layout()
    rows = [(9, 1, b"a" * 32, "synthetic-a"), (12, 1, b"b" * 32, "synthetic-b")]
    together_items = [{"key_dictionary": rows}]
    divided_items = [{"key_dictionary": rows[:1]}, {"key_dictionary": rows[1:]}]
    assert _proof(layout, together_items) == _proof(layout, divided_items)
    assert _proof(layout, together_items) != _proof(layout, [{"key_dictionary": rows[::-1]}])
    assert _proof(layout, together_items) != _proof(layout, [{"key_dictionary": [(10, *rows[0][1:]), rows[1]]}])

    def scalar(value):
        proof = store.ShardOutputProof(layout, 0, b"p" * 32, 1)
        proof.add("query_child", [(value, datetime(2026, 1, 1, tzinfo=UTC))])
        return proof.receipt()["result_sha256"]

    assert scalar(Decimal("1.00")) != scalar(Decimal("1.0"))
    assert scalar(1) != scalar(True)


class _Result:
    def __init__(self, row=None):
        self.row = row

    def mappings(self):
        return self

    def one_or_none(self):
        return self.row

    def one(self):
        assert self.row is not None
        return self.row

    def scalar_one_or_none(self):
        return self.row


class _Transaction:
    def __init__(self, *, old=None, copy_failure=False, expired=False, phase="loading", cancelled=False):
        self.old, self.copy_failure, self.expired = old, copy_failure, expired
        self.phase, self.cancelled = phase, cancelled
        self.pending, self.committed, self.events = [], [], []
        self.info, self.closed = {}, False
        self.driver_connection = self
        self.sync_connection = SimpleNamespace(get_execution_options=lambda: {})

    @asynccontextmanager
    async def __call__(self):
        try:
            yield self
        finally:
            self.closed = True

    @asynccontextmanager
    async def begin(self):
        self.events.append("begin")
        try:
            yield
        except BaseException:
            self.pending.clear()
            self.events.append("rollback")
            raise
        else:
            self.committed.extend(self.pending)
            self.events.append("commit")

    async def connection(self):
        return self

    async def get_raw_connection(self):
        return self

    async def copy_records_to_table(self, name, *, schema_name, columns, records):
        self.events.append("copy")
        if self.copy_failure and self.events.count("copy") == 2:
            raise RuntimeError("synthetic copy failure")
        self.pending.append((name, tuple(columns), tuple(records)))


def _transition_binding(layout):
    return SimpleNamespace(
        family_id=11,
        layout=layout,
        tables_by_role=layout.tables(11),
        owner_oid=91,
        oids_by_role={role: 1000 + index for index, role in enumerate(layout.tables(11))},
    )


def _install_transition_authority(monkeypatch, transaction):
    async def initial(session, request):
        transaction.events.append("initial-window")
        session.info[store._WINDOW] = store._PageWindow(time.monotonic() + 60, 1000)

    async def owner(session):
        transaction.events.append("owner")

    async def observe(_session, _request):
        transaction.events.append("observe-attempt")
        context = getattr(_request, "continuation", None)
        return None if context is None else context.build_id

    async def locked(_session, _request, build_id=None):
        context = getattr(_request, "continuation", None)
        assert build_id == (None if context is None else context.build_id)
        transaction.build_id = build_id
        transaction.events.append("authority-start")
        session_window = transaction.info[store._WINDOW]
        assert _request.operation_deadline == session_window.deadline
        transaction.info[store._WINDOW] = store._PageWindow(session_window.deadline + 60, 2000)

    async def fresh(session, request):
        transaction.events.append("fresh-authority-before-commit")
        if transaction.cancelled:
            raise asyncio.CancelledError("synthetic task cancellation")
        if transaction.expired:
            raise RuntimeError("synthetic expired authorization")

    async def call(session, name, arguments):
        assert name == "lock_custom_import_snapshot_attempt"
        transaction.events.append("native-attempt")

    monkeypatch.setattr(store, "_initial_page_window", initial)
    monkeypatch.setattr(store, "_owner", owner)
    monkeypatch.setattr(store, "_observe_attempt", observe)
    monkeypatch.setattr(store, "_lock_page", locked)
    monkeypatch.setattr(store, "_fresh_commit_authority", fresh)
    monkeypatch.setattr(store, "_call", call)


def _install_transition_statements(monkeypatch, transaction, layout):
    descriptor = transaction.descriptor

    async def candidate(session, request, family_id, supplied_layout, phases, *, locked=True):
        assert transaction.phase in phases and family_id == 11 and supplied_layout == layout
        transaction.events.append("resolve-locked" if locked else "resolve-observation")
        return descriptor, transaction.binding

    async def barrier(session, tables, *, loading, nowait=False):
        assert len(tables) == 14 and not nowait
        transaction.events.append("barrier-copy" if loading else "barrier-transition")

    async def execute(session, statement, parameters=None):
        if getattr(statement, "is_select", False):
            transaction.events.append("read-receipt")
            return _Result(transaction.old)
        if getattr(statement, "is_insert", False):
            transaction.events.append("claim-receipt")
            assert "ON CONFLICT (shard_id) DO NOTHING RETURNING" in str(statement)
            if transaction.old is not None:
                return _Result()
            transaction.pending.append(("receipt", statement.compile().params))
            return _Result(0)
        if getattr(statement, "is_update", False):
            transaction.events.append("transition-update")
            transaction.pending.append(("transition", statement.compile().params))
        else:
            transaction.events.append("ddl")
            transaction.pending.append(("ddl", str(statement)))
        return _Result()

    async def prepare(session):
        transaction.events.append("statement-budget")

    monkeypatch.setattr(store, "_relation_barrier", barrier)
    monkeypatch.setattr(store, "_candidate", candidate)
    monkeypatch.setattr(store, "_execute", execute)
    monkeypatch.setattr(store, "_prepare_statement", prepare)


def _install(monkeypatch, transaction, layout):
    descriptor = SimpleNamespace(**{column.name: None for column in store.CustomImportFamilyBlockLayout.__table__.c})
    descriptor.family_id, descriptor.phase, descriptor.layout_sha256 = 11, transaction.phase, layout.digest
    transaction.descriptor, transaction.binding = descriptor, _transition_binding(layout)

    _install_transition_authority(monkeypatch, transaction)
    _install_transition_statements(monkeypatch, transaction, layout)


def _run(transaction, layout, batches, receipt):
    request = _request(definition=layout.definition)
    return asyncio.run(
        store.copy_shard(transaction, request, 11, layout, batches, receipt, maximum_rows=2, maximum_bytes=1000)
    )


def test_one_owner_transaction_copies_bounded_chunks_and_receipt_atomically(monkeypatch):
    layout = _layout()
    batches = [
        {"key_dictionary": [(9, 1, b"a" * 32, "synthetic-a")]},
        {"key_dictionary": [(12, 1, b"b" * 32, "synthetic-b")]},
    ]
    expected = _proof(layout, batches)
    transaction = _Transaction()
    _install(monkeypatch, transaction, layout)
    assert _run(transaction, layout, iter(batches), expected) == expected
    assert transaction.events.count("authority-start") == 1
    assert transaction.events.count("resolve-locked") == 1
    assert transaction.events.count("resolve-observation") == 1
    assert transaction.events.count("copy") == 2
    assert transaction.events.index("barrier-copy") < transaction.events.index("copy")
    assert transaction.events.index("barrier-copy") < transaction.events.index("resolve-observation")
    assert transaction.events.index("copy") < transaction.events.index("authority-start")
    assert transaction.events[-2:] == ["fresh-authority-before-commit", "commit"]
    assert len(transaction.committed) == 3 and transaction.committed[0][0] == "receipt"
    assert transaction.closed and transaction.info[store._WINDOW].statement_timeout_ms == 1000


def test_exact_retry_never_consumes_spool_or_recopies_rows(monkeypatch):
    layout = _layout()
    expected = _proof(layout, [])
    transaction = _Transaction(old={**expected, "completed_at": datetime.now(UTC)})
    _install(monkeypatch, transaction, layout)

    def untouched():
        pytest.fail("exact retry must not read the shard spool")
        yield

    assert _run(transaction, layout, untouched(), expected) == expected
    assert "copy" not in transaction.events and not transaction.committed
    assert transaction.events.index("claim-receipt") < transaction.events.index("read-receipt")
    assert transaction.events[-1] == "commit"


@pytest.mark.parametrize("expired", (False, True))
def test_continued_retry_binds_retained_build_and_still_checks_current_expiry(monkeypatch, expired):
    layout = _layout()
    expected = _proof(layout, [])
    request = _request(definition=layout.definition, continuation=BuildContinuationContext(41, 51, b"p" * 32))
    transaction = _Transaction(old={**expected, "completed_at": datetime.now(UTC)}, expired=expired)
    _install(monkeypatch, transaction, layout)

    def untouched():
        pytest.fail("continued exact retry must not consume the spool")
        yield

    operation = store.copy_shard(
        transaction, request, 11, layout, untouched(), expected, maximum_rows=2, maximum_bytes=1000
    )
    if expired:
        with pytest.raises(RuntimeError, match="expired"):
            asyncio.run(operation)
        assert transaction.events[-1] == "rollback"
    else:
        assert asyncio.run(operation) == expected
        assert transaction.events[-1] == "commit"
    assert transaction.build_id == 51
    assert "fresh-authority-before-commit" in transaction.events


@pytest.mark.parametrize("changed", ("partition_sha256", "result_sha256", "source_record_count", "role_counts"))
def test_replay_refuses_any_changed_input_output_or_count(monkeypatch, changed):
    layout = _layout()
    expected = _proof(layout, [])
    old_by_key = dict(expected)
    old_by_key[changed] = (
        {**expected[changed], "family": 1}
        if changed == "role_counts"
        else (1 if changed == "source_record_count" else b"x" * 32)
    )
    transaction = _Transaction(old=old_by_key)
    _install(monkeypatch, transaction, layout)
    with pytest.raises(FamilyBlockUnavailable, match="replay"):
        _run(transaction, layout, [], expected)
    assert not transaction.committed and transaction.events[-1] == "rollback"


@pytest.mark.parametrize("failure", ("copy", "result", "expiry"))
def test_any_failure_rolls_back_all_chunks_and_the_receipt(monkeypatch, failure):
    layout = _layout()
    batches = [
        {"key_dictionary": [(9, 1, b"a" * 32, "synthetic-a")]},
        {"key_dictionary": [(12, 1, b"b" * 32, "synthetic-b")]},
    ]
    expected = _proof(layout, batches)
    if failure == "result":
        expected["result_sha256"] = b"x" * 32
    transaction = _Transaction(copy_failure=failure == "copy", expired=failure == "expiry")
    _install(monkeypatch, transaction, layout)
    with pytest.raises((RuntimeError, FamilyBlockUnavailable)):
        _run(transaction, layout, batches, expected)
    assert not transaction.pending and not transaction.committed
    assert transaction.events[-1] == "rollback"


def test_shard_receipt_is_an_owner_only_typed_control_role():
    layout = _layout()
    table = layout.tables(11)["shard_receipt"]
    assert list(table.primary_key.columns.keys()) == ["shard_id"]
    assert not table.foreign_keys
    assert set(table.c.keys()) == {
        "shard_id",
        "partition_sha256",
        "result_sha256",
        "source_record_count",
        "role_counts",
        "completed_at",
    }
    assert store.READ_CAPABILITIES["shard_receipt"] == ()
    with pytest.raises(ValueError, match="closed-role"):
        store.check_copy_batch(layout, 11, {"shard_receipt": []}, maximum_rows=1, maximum_bytes=1)


@pytest.mark.parametrize("failure", ("crash_empty", "missing", "wrong_range", "source_count", "role_count"))
def test_durability_witness_refuses_crash_loss_or_accounting_drift(failure):
    layout = _layout()
    role_counts = _proof(layout, [])["role_counts"]
    role_counts["family"] = 2
    expected = store._checked_census(2, 17, role_counts)
    family_by_column = {
        "shards": 2,
        "first_shard": 0,
        "last_shard": 1,
        "source_record_count": 17,
        **{f"role_{i}": value for i, value in enumerate(role_counts.values())},
    }
    store._compare_shard_census(family_by_column, expected)
    match failure:
        case "crash_empty":
            family_by_column.update(shards=0, first_shard=None, last_shard=None, source_record_count=0)
        case "missing":
            family_by_column["shards"] = 1
        case "wrong_range":
            family_by_column.update(first_shard=1, last_shard=2)
        case "source_count":
            family_by_column["source_record_count"] = 16
        case _:
            family_by_column["role_0"] += 1
    with pytest.raises(FamilyBlockUnavailable):
        store._compare_shard_census(family_by_column, expected)


def test_empty_source_still_requires_one_completed_shard_witness():
    role_counts = _proof(_layout(), [])["role_counts"]
    expected = store._checked_census(1, 0, role_counts)
    family_by_column = {
        "shards": 1,
        "first_shard": 0,
        "last_shard": 0,
        "source_record_count": 0,
        **{f"role_{i}": value for i, value in enumerate(role_counts.values())},
    }
    store._compare_shard_census(family_by_column, expected)
    with pytest.raises(ValueError):
        store._checked_census(0, 0, role_counts)


def test_empty_shard_claim_is_committed_only_after_full_authority(monkeypatch):
    layout, transaction = _layout(), _Transaction()
    _install(monkeypatch, transaction, layout)
    expected = _proof(layout, [])
    assert _run(transaction, layout, [], expected) == expected
    assert len(transaction.committed) == 1 and transaction.committed[0][0] == "receipt"
    assert "copy" not in transaction.events
    assert transaction.events.index("claim-receipt") < transaction.events.index("authority-start")
    assert transaction.events[-2:] == ["fresh-authority-before-commit", "commit"]


def test_task_cancellation_drains_transaction_rollback_and_session_close(monkeypatch):
    layout, transaction = _layout(), _Transaction(cancelled=True)
    _install(monkeypatch, transaction, layout)
    batches = [{"key_dictionary": [(9, 1, b"a" * 32, "synthetic-a")]}]
    with pytest.raises(asyncio.CancelledError):
        _run(transaction, layout, batches, _proof(layout, batches))
    assert not transaction.pending and not transaction.committed and transaction.closed
    assert transaction.events[-1] == "rollback"


def test_changed_descriptor_after_bulk_work_cannot_commit(monkeypatch):
    layout, transaction = _layout(), _Transaction()
    _install(monkeypatch, transaction, layout)
    candidate = store._candidate

    async def changed(*args, **kwargs):
        result = await candidate(*args, **kwargs)
        if kwargs.get("locked", True):
            transaction.descriptor.validation_sha256 = b"x" * 32
        return result

    monkeypatch.setattr(store, "_candidate", changed)
    with pytest.raises(FamilyBlockUnavailable, match="descriptor changed"):
        _run(transaction, layout, [], _proof(layout, []))
    assert not transaction.pending and not transaction.committed
    assert transaction.events[-1] == "rollback"


def test_changed_authority_while_waiting_for_barrier_precedes_any_copy(monkeypatch):
    layout, transaction = _layout(), _Transaction()
    _install(monkeypatch, transaction, layout)
    observer = SimpleNamespace(observed=0)

    async def changed(session, request):
        observer.observed += 1
        if observer.observed == 2:
            raise store.LeaseAuthorityLost("synthetic changed epoch")
        return None

    monkeypatch.setattr(store, "_observe_attempt", changed)
    with pytest.raises(store.LeaseAuthorityLost, match="changed epoch"):
        _run(transaction, layout, [], _proof(layout, []))
    assert "barrier-copy" in transaction.events
    assert not {"copy", "claim-receipt", "authority-start"} & set(transaction.events)
    assert transaction.events[-1] == "rollback" and transaction.closed


def test_create_candidate_keeps_the_existing_page_transaction_boundary(monkeypatch):
    layout, seen = _layout(), []

    @asynccontextmanager
    async def page(factory, request, build_id):
        seen.append(build_id)
        raise RuntimeError("synthetic original page boundary")
        yield

    monkeypatch.setattr(store, "_page_session", page)
    request = _request(definition=layout.definition, continuation=BuildContinuationContext(41, 51, b"p" * 32))
    with pytest.raises(RuntimeError, match="original page boundary"):
        asyncio.run(store.create_candidate(None, request, layout))
    assert seen == [51]


@pytest.mark.parametrize("loading", (False, True))
@pytest.mark.parametrize("nowait", (False, True))
def test_native_barrier_covers_every_role_in_fixed_order(monkeypatch, loading, nowait):
    layout, statements = _layout(), []

    async def control(session):
        return dialect().identifier_preparer, "synthetic"

    async def execute(session, statement):
        statements.append(statement)

    monkeypatch.setattr(store, "_control_schema", control)
    monkeypatch.setattr(store, "_execute", execute)
    tables = layout.tables(11)
    asyncio.run(store._relation_barrier(None, tables, loading=loading, nowait=nowait))
    expected = ",".join(dialect().identifier_preparer.format_table(table) for table in tables.values())
    mode = "ROW EXCLUSIVE" if loading else "SHARE ROW EXCLUSIVE"
    assert statements == [f"LOCK TABLE {expected} IN {mode} MODE" + (" NOWAIT" if nowait else "")]
    assert len(tables) == 14


def test_control_first_candidate_refuses_contention_before_any_leaf_lookup(monkeypatch):
    layout, events = _layout(), []
    request = _request(definition=layout.definition)

    async def owner(session):
        events.append("owner")

    async def control(session):
        return dialect().identifier_preparer, "synthetic"

    async def execute(session, statement, parameters=None):
        assert isinstance(statement, str) and statement.startswith("LOCK TABLE ")
        assert statement.endswith("ROW EXCLUSIVE MODE NOWAIT")
        events.append("nowait-refusal")
        raise RuntimeError("synthetic leaf lock unavailable")

    monkeypatch.setattr(store, "_owner", owner)
    monkeypatch.setattr(store, "_control_schema", control)
    monkeypatch.setattr(store, "_execute", execute)
    monkeypatch.setattr(store, "_call", AsyncMock(side_effect=AssertionError("must not reach native resolver")))
    monkeypatch.setattr(
        store, "resolve_family_block_binding", AsyncMock(side_effect=AssertionError("must not read leaves"))
    )
    with pytest.raises(RuntimeError, match="leaf lock unavailable"):
        asyncio.run(store._candidate(None, request, 11, layout, {"loading"}))
    assert events == ["owner", "nowait-refusal"]


@pytest.mark.parametrize("operation", ("close_candidate", "prepare_indexes", "accept_validation", "make_logged"))
@pytest.mark.parametrize("expired", (False, True))
def test_transitions_and_long_ddl_take_leaf_barrier_before_authority(monkeypatch, operation, expired):
    layout = _layout()
    phase = {"close_candidate": "loading", "make_logged": "verified"}.get(operation, "closed")
    transaction = _Transaction(phase=phase, expired=expired)
    _install(monkeypatch, transaction, layout)
    counts = _checked_empty_counts(layout)
    for name, count in counts.items():
        setattr(transaction.descriptor, name, count)

    async def verified(*args, **kwargs):
        transaction.events.append("full-index-or-census-check")

    async def has_indexes(*args, **kwargs):
        transaction.events.append("missing-indexes")
        return False

    async def control(session):
        return dialect().identifier_preparer, "synthetic"

    async def logged(*args, **kwargs):
        assert kwargs["persistence"] == "logged"
        transaction.events.append("logged-custody")
        return transaction.binding

    async def identity(*args):
        transaction.events.append("original-identity")

    async def log_roles(session, binding):
        await store._execute(session, "ALTER TABLE synthetic_role SET LOGGED")

    monkeypatch.setattr(store, "index_statements", lambda *_: ("CREATE INDEX synthetic_a", "CREATE INDEX synthetic_b"))
    monkeypatch.setattr(store, "has_family_block_indexes", has_indexes)
    monkeypatch.setattr(store, "validate_family_block_indexes", verified)
    monkeypatch.setattr(store, "_verify_shard_census", verified)
    monkeypatch.setattr(store, "_control_schema", control)
    monkeypatch.setattr(store, "resolve_family_block_binding", logged)
    monkeypatch.setattr(store, "_validation_identity", identity)
    monkeypatch.setattr(store, "_log_unlogged_roles", log_roles)
    pending = _transition_call(transaction, layout, operation, counts)
    if expired:
        with pytest.raises(RuntimeError, match="expired"):
            asyncio.run(pending)
        assert not transaction.pending and not transaction.committed
        assert transaction.events[-1] == "rollback"
    else:
        asyncio.run(pending)
        assert transaction.events[-1] == "commit"
    barrier = transaction.events.index("barrier-transition")
    authority = transaction.events.index("authority-start")
    assert barrier < authority
    if operation in ("prepare_indexes", "make_logged"):
        assert barrier < transaction.events.index("ddl") < authority
    if operation != "prepare_indexes":
        assert authority < transaction.events.index("transition-update")
    if phase != "loading":
        assert authority < transaction.events.index("native-attempt") < transaction.events.index("resolve-locked")
    assert transaction.closed


def _transition_call(transaction, layout, operation, counts):
    request = _request(definition=layout.definition)
    if operation not in ("accept_validation", "make_logged"):
        return getattr(store, operation)(transaction, request, 11, layout)
    scope = CandidateScope(
        11,
        12,
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        request.execution_id,
        17,
        request.fence,
        store.producer_token_sha256(request),
        request.expected_base_generation_id,
    )
    identity = ClosedCandidateIdentity(
        scope,
        datetime(2026, 1, 1, tzinfo=UTC),
        layout.digest,
        91,
        tuple(sorted(transaction.binding.oids_by_role.items())),
    )
    transaction.descriptor.validation_sha256 = b"v" * 32 if operation == "make_logged" else None
    return getattr(store, operation)(transaction, request, identity, layout, validation_sha256=b"v" * 32, **counts)


def _checked_empty_counts(layout):
    return store._checked_census(1, 0, _proof(layout, [])["role_counts"])


@pytest.mark.parametrize(
    "damage", (None, "missing", "scope", "capture", "retired", "canceling", "token", "fence", "expired")
)
def test_initial_observation_checks_current_authority_without_row_locks(monkeypatch, damage):
    request = _request(definition=_layout().definition)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    scope_by_key = {
        name: getattr(request, name) for name in ("dataset_id", "definition_revision_id", "schema_revision_id")
    }
    execution = SimpleNamespace(
        **scope_by_key, execution_id=request.execution_id, state="running", capture_bundle_id=19
    )
    capture = SimpleNamespace(**scope_by_key, capture_state="sealed")
    lease = SimpleNamespace(
        execution_id=request.execution_id,
        fence=request.fence,
        token_sha256=lease_token_sha256(request.lease_token),
        expires_at=now + timedelta(seconds=60),
        continuation_id=None,
    )
    match damage:
        case "scope":
            execution.schema_revision_id += 1
        case "capture":
            capture.capture_state = "pending"
        case "canceling":
            execution.state = "canceling"
        case "token":
            lease.token_sha256 = b"x" * 32
        case "fence":
            lease.fence += 1
        case "expired":
            lease.expires_at = now

    async def execute(session, statement):
        assert statement._for_update_arg is None
        assert statement.get_execution_options()["populate_existing"] is True
        return _Result(None if damage == "missing" else (execution, lease, capture, None, damage == "retired"))

    monkeypatch.setattr(store, "_execute", execute)
    session = SimpleNamespace(no_autoflush=nullcontext(), scalar=AsyncMock(return_value=now))
    pending = store._observe_attempt(session, request)
    if damage is None:
        assert asyncio.run(pending) is None
    else:
        with pytest.raises((FamilyBlockUnavailable, CandidateRunnerError)):
            asyncio.run(pending)


@pytest.mark.parametrize("damage", (None, "processing_deadline", "authorization_deadline", "canceling"))
def test_final_authority_uses_a_fresh_clock_and_original_deadlines(monkeypatch, damage):
    request = _request(definition=_layout().definition)
    now = datetime(2026, 1, 1, tzinfo=UTC)
    match damage:
        case "processing_deadline":
            request = replace(request, build_deadline_at=now)
        case "authorization_deadline":
            request = replace(request, authorization_expires_at=now)
    execution = SimpleNamespace(state="canceling" if damage == "canceling" else "running")
    lease = SimpleNamespace(
        fence=request.fence,
        token_sha256=lease_token_sha256(request.lease_token),
        expires_at=now + timedelta(seconds=60),
        continuation_id=None,
    )
    session = SimpleNamespace(no_autoflush=nullcontext(), scalar=AsyncMock(return_value=now))
    events = []

    async def execution_lock(*_):
        events.append("execution")
        return execution

    async def lease_lock(*_):
        events.append("lease")
        return lease

    monkeypatch.setattr(store, "_flush_page", AsyncMock())
    monkeypatch.setattr(store, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(store, "lock_execution", execution_lock)
    monkeypatch.setattr(store, "lock_lease", lease_lock)
    pending = store._fresh_commit_authority(session, request)
    if damage is None:
        asyncio.run(pending)
    else:
        with pytest.raises(CandidateRunnerError):
            asyncio.run(pending)
    assert events == ["execution", "lease"]
    if damage != "canceling":
        session.scalar.assert_awaited_once()
