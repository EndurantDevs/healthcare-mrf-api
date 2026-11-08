# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Finite native allocation windows retain every original batch and commit proof."""

import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql

from process.custom_import import family_shard_allocate as allocation
from process.custom_import import family_shard_identity as identity
from process.custom_import import family_shard_ids as ids
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.runner_codec import digest_text
from tests.test_custom_import_family_shard_allocate import _allocate, _close_recovery, _recovery_case
from tests.test_custom_import_family_shard_identity import _session
from tests.test_custom_import_snowflake_bundle import _definition


@pytest.mark.parametrize("loss", (None, "after", "unknown", "before"))
async def test_atomic_window_preserves_all_original_batches_after_each_commit_outcome(tmp_path, monkeypatch, loss):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch, windowed=True)
    boundary.failure = loss
    observed_result = None
    try:
        if loss in ("before", "unknown"):
            with pytest.raises((RuntimeError, FamilyBlockUnavailable)):
                await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
            assert len(boundary.allocations) == 1
            assert len(tuple((journal.root / "vectors").iterdir())) == 4
            assert boundary.committed["owner_checkpoint_revision"] == (None if loss == "before" else 4)
            original_vectors_by_name = {
                vector_path.name: vector_path.read_bytes() for vector_path in (journal.root / "vectors").iterdir()
            }
            boundary.failure = None
        observed_result = await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        family_rows = tuple(allocation.read_source_allocations(observed_result))
        assert [family_row[1] for family_row in family_rows] == (
            [125, 142, 159, 176] if loss == "before" else [57, 74, 91, 108]
        )
        assert len(boundary.allocations) == (2 if loss == "before" else 1)
        assert boundary.committed["owner_checkpoint_revision"] == 4
        snapshot = await allocation._checkpoint(None, case[1], 17, None, journal)
        family_records = tuple(family_record for _head, family_record in reversed(tuple(snapshot.records())))
        assert [family_record.ordinal for family_record in family_records] == [0, 1, 2, 3]
        assert all(family_record.kind == "source_allocation" for family_record in family_records)
        if loss in ("before", "unknown"):
            assert all(
                (journal.root / "vectors" / name).read_bytes() == body
                for name, body in original_vectors_by_name.items()
            )
        if loss is None:
            # Initial read, one window, one confirmation, final read, explicit test read.
            assert boundary.count("authority") == 5
    finally:
        _close_recovery(case, plan, journal, observed_result)


async def test_tampered_middle_of_committed_window_never_reallocates(tmp_path, monkeypatch):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch, windowed=True)
    boundary.failure = "unknown"
    try:
        with pytest.raises(FamilyBlockUnavailable, match="unknown"):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        vectors = sorted((journal.root / "vectors").iterdir())
        vectors[1].chmod(0o600)
        vectors[1].write_bytes(vectors[1].read_bytes() + b"changed")
        vectors[1].chmod(0o400)
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert len(boundary.allocations) == 1
        assert boundary.committed["owner_checkpoint_revision"] == 4
    finally:
        _close_recovery(case, plan, journal)


@pytest.mark.parametrize("when", ("entry", "commit"))
async def test_unproven_or_changed_sequence_binding_never_commits_a_window(tmp_path, monkeypatch, when):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch, windowed=True)
    calls = []

    async def changed(_session, kinds):
        calls.append(True)
        if when == "entry":
            raise FamilyBlockUnavailable("unproven sequence default")
        return {"source_root": len(calls), **{kind: 20 + i for i, kind in enumerate(kinds)}}

    monkeypatch.setattr(allocation, "source_sequence_bindings", changed)
    try:
        with pytest.raises(FamilyBlockUnavailable, match="sequence"):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert boundary.committed["owner_checkpoint_revision"] is None
        assert len(boundary.allocations) == (0 if when == "entry" else 1)
        assert len(tuple((journal.root / "vectors").iterdir())) == (0 if when == "entry" else 4)
    finally:
        _close_recovery(case, plan, journal)


def test_window_groups_whole_original_batches_under_existing_memory_and_array_bounds():
    record = SimpleNamespace(typed_parent_key=("same-key", b"x" * 32))
    pack = SimpleNamespace(records=(record,) * 256, landing=SimpleNamespace(byte_count=1024))
    batches = [(pack,)] * 70
    limits = allocation.SourceAllocationLimits(
        SourceFactLimits(8 * 1024**3, 64 * 1024**2, 64 * 1024**2), 256, 8 * 1024**2
    )
    checks = []
    windows = tuple(allocation._allocation_windows(iter(batches), limits, check=lambda: checks.append(True)))
    assert sum(len(window) for window in windows) == 70 and len(windows) == 2
    assert all(batch is batches[0] for window in windows for batch in window)
    assert all(
        sum(1024 + 512 * len(pack.records) for batch in window for pack in batch) <= 8 * 1024**2 for window in windows
    )
    assert len(checks) == 70
    too_small = allocation.SourceAllocationLimits(SourceFactLimits(1024**2, 1024**2, 4096), 256, 8 * 1024**2)
    with pytest.raises(FamilyBlockResourceLimit, match="memory budget"):
        tuple(allocation._allocation_windows(iter(batches), too_small, check=lambda: None))


async def test_root_insert_attempts_keep_original_batch_groups_and_separate_winner_read(monkeypatch):
    key = "exact lexical key", digest_text("root-key", "exact lexical key")
    session, events = _session(monkeypatch, [{"ordinality": i, "root_record_id": 91, "valid": True} for i in (1, 2, 3)])
    assert await identity.intern_root_records(
        session, _definition(), 17, (key,) * 3, maximum_rows=3, maximum_bytes=4096, batch_ordinals=(0, 0, 1)
    ) == (91, 91, 91)
    statements = [event for event in events if type(event) is tuple]
    assert len(statements) == 2
    assert "GROUP BY root_key,root_hash,batch_ordinal ORDER BY min(ordinality)" in statements[0][0]
    assert "ON CONFLICT" in statements[0][0] and "DO NOTHING" in statements[0][0]
    assert "LEFT JOIN" in statements[1][0] and "ORDER BY i.ordinality" in statements[1][0]
    assert statements[0][1]["batch_ordinals"] == [0, 0, 1]


@pytest.mark.parametrize("damage", ("alias", "invalid", "missing", "extra", "boolean"))
async def test_actual_root_sequence_defaults_must_be_proven_and_disjoint(monkeypatch, damage):
    sequence_rows = [{"kind": "source_root", "oid": 10, "valid": True}, {"kind": "pack", "oid": 20, "valid": True}]
    if damage == "alias":
        sequence_rows[0]["oid"] = 20
    elif damage == "invalid":
        sequence_rows[0]["valid"] = False
    elif damage == "missing":
        sequence_rows.pop()
    elif damage == "extra":
        sequence_rows.append(sequence_rows[-1])
    else:
        sequence_rows[0]["oid"] = True
    queries = []

    async def schema(_session):
        return postgresql.dialect().identifier_preparer, "synthetic_scope"

    async def prepare(_session):
        return None

    class Session:
        async def execute(self, statement, parameters):
            queries.append(str(statement))
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: sequence_rows))

    monkeypatch.setattr(ids, "_control_schema", schema)
    monkeypatch.setattr(ids, "_prepare_statement", prepare)
    with pytest.raises(FamilyBlockUnavailable, match="sequence defaults"):
        await ids.source_sequence_bindings(Session(), ("pack",))
    assert len(queries) == 1 and "nextval(CASE" not in queries[0]
    for requirement in (
        "pg_get_serial_sequence",
        "pg_get_expr",
        "seqtypid='bigint'",
        "seqincrement=1",
        "NOT q.seqcycle",
        "CURRENT_USER",
    ):
        assert requirement in queries[0]


@pytest.mark.parametrize("changed", (None, "build", "phase", "head", "cancel", "expiry"))
async def test_bulk_window_checks_expected_build_and_descriptor_before_final_head_update(monkeypatch, changed):
    from process.custom_import import family_block_store as store
    from tests.test_custom_import_build_source import _request
    from tests.test_custom_import_family_block_store import _install, _layout, _Transaction

    layout = _layout()
    transaction = _Transaction(cancelled=changed == "cancel", expired=changed == "expiry")
    _install(monkeypatch, transaction, layout)

    async def observe(_session, _request):
        return 23

    async def locked(_session, _request, build_id):
        assert build_id == 23
        return SimpleNamespace(
            build_id=24 if changed == "build" else 23, phase="admission" if changed == "phase" else "source"
        )

    monkeypatch.setattr(store, "_observe_attempt", observe)
    monkeypatch.setattr(store, "_lock_page", locked)
    transition = store.SourceAllocationTransition(23, {})

    async def operation():
        async with store._block_session(
            transaction,
            _request(definition=layout.definition),
            11,
            layout,
            "loading",
            loading=True,
            transition=transition,
        ) as (_session, descriptor, _binding):
            # The provisional chain is not assigned to this native descriptor.
            assert descriptor.owner_checkpoint_revision is None
            if changed == "head":
                descriptor.owner_checkpoint_revision = 99
            transition.values.update(owner_checkpoint_revision=4, owner_checkpoint_sha256=b"x" * 32)

    if changed:
        expected = (
            asyncio.CancelledError
            if changed == "cancel"
            else RuntimeError
            if changed == "expiry"
            else FamilyBlockUnavailable
        )
        with pytest.raises(expected):
            await operation()
        assert "rollback" in transaction.events and transaction.committed == []
    else:
        await operation()
        assert transaction.events.index("resolve-locked") < transaction.events.index("transition-update")
        assert transaction.events.index("transition-update") < transaction.events.index("fresh-authority-before-commit")
        assert transaction.committed[0][1]["owner_checkpoint_revision"] == 4


async def test_impossible_original_pack_refuses_before_any_native_work(tmp_path, monkeypatch):
    case, plan, boundary, journal = await _recovery_case(tmp_path, monkeypatch, windowed=True)
    from dataclasses import replace

    from tests.test_custom_import_family_shard_allocate import _journal

    journal.close_terminal()
    journal = _journal(tmp_path, facts=replace(journal.work.limits, memory_bytes=4))
    native_calls = []

    async def forbidden(*args, **kwargs):
        native_calls.append(True)
        raise AssertionError("impossible allocation reached native authority")

    monkeypatch.setattr(allocation, "_checkpoint", forbidden)
    monkeypatch.setattr(allocation, "source_sequence_bindings", forbidden)
    try:
        with pytest.raises(FamilyBlockResourceLimit, match="window memory"):
            await _allocate(tmp_path, case[1], plan, maximum=2, journal=journal)
        assert not native_calls and not boundary.allocations
        assert not tuple(journal.checkpoints.iterdir())
        assert not tuple((journal.root / "vectors").iterdir())
        assert not tuple((journal.root / "outputs").iterdir())
    finally:
        _close_recovery(case, plan, journal)
