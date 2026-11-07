# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded SOURCE preparation reuse, original custody and borrowed replay EOF."""

import os
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest

from process.custom_import import family_shard_source_values as source_values
from process.custom_import.family_block_records import record_parts
from process.custom_import.family_shard_plan import PlanWork, _cpu, _Scratch
from process.custom_import.runner_types import CancellationRequested
from tests.test_custom_import_family_shard_source_plan import _child, _root
from tests.test_custom_import_family_shard_source_values import _LIMITS, _case, _children


def _prepared(tmp_path, native, *, check=None):
    return source_values.prepared_source_family_inputs(
        native.context,
        native.admission,
        native.plan,
        work=PlanWork(tmp_path / "prepared-values", _LIMITS, lambda _path: None, check),
    )


def _snapshot(prepared):
    snapshots = []
    with closing(prepared.families()) as families:
        for plan, family in families:
            children = _children(family)
            definition = prepared.context.request.definition
            snapshots.append(
                (
                    plan.typed_key,
                    family.family_revision_id,
                    family.root_record_id,
                    family.entity_binding_id,
                    family.expected_family_sha256,
                    record_parts(definition, None, family.root, retained=False)[0],
                    tuple(record_parts(definition, "details", child, retained=False)[0] for child in children),
                )
            )
    return snapshots


@pytest.mark.asyncio
async def test_replays_stage_full_source_once_and_preserve_original_bytes_and_budgets(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, base=True) as native:
        originals, calls = source_values._all_occurrences, []

        def occurrences(*arguments):
            calls.append(True)
            yield from originals(*arguments)

        monkeypatch.setattr(source_values, "_all_occurrences", occurrences)
        with _prepared(tmp_path, native) as prepared:
            assert type(prepared) is source_values.PreparedSourceFamilies
            staged_bytes = prepared.work.used
            original_paths = set(prepared.work.paths)
            snapshots = [_snapshot(prepared) for _pass in range(3)]
            assert snapshots[0] == snapshots[1] == snapshots[2] and len(snapshots[0]) == 1
            assert calls == [True]
            assert prepared.work.used == staged_bytes and set(prepared.work.paths) == original_paths
            assert prepared.work.peak_bytes <= _LIMITS.max_spool_bytes
            assert all(path.stat().st_mode & 0o777 == 0o400 for path in original_paths)
        assert not (tmp_path / "prepared-values").exists()
        assert Path(native.admission.root).exists() and Path(native.plan.root).exists()


@pytest.mark.asyncio
async def test_replay_allocates_fresh_objects_and_invalidates_previous_child_factories(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            originals = []
            for _pass in range(2):
                with closing(prepared.families()) as families:
                    _plan, family = next(families)
                    originals.append(family)
                    assert len(_children(family)) == 2
                    assert next(families, None) is None
                with pytest.raises(ValueError, match="no longer active"):
                    _children(family)
            assert originals[0] is not originals[1] and originals[0].root is not originals[1].root


@pytest.mark.asyncio
async def test_unstarted_and_partial_replays_close_without_sharing_an_active_family(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            replay = prepared.families()
            with pytest.raises(ValueError, match="active replay"):
                prepared.families()
            replay.close()
            replay = prepared.families()
            _plan, family = next(replay)
            child_reader = family.children_by_collection["details"]()
            next(child_reader)
            replay.close()
            with pytest.raises(ValueError, match="no longer active"):
                next(child_reader)
            assert len(_snapshot(prepared)) == 1


@pytest.mark.asyncio
async def test_owner_close_drains_live_readers_and_cannot_be_replayed(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            replay = prepared.families()
            _plan, family = next(replay)
            reader = family.children_by_collection["details"]()
            next(reader)
            prepared.close()
            prepared.close()
            with pytest.raises(ValueError, match="no longer active"):
                next(reader)
            with pytest.raises(ValueError, match="closed"):
                prepared.families()
            with pytest.raises(ValueError, match="closed"):
                prepared.check_scope(native.context, native.admission, native.plan)
        assert not (tmp_path / "prepared-values").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["source", "allocation", "admission", "plan"])
async def test_original_in_place_bytes_with_restored_mtime_are_refused_by_ctime(tmp_path, monkeypatch, kind):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            path = dict(zip(("source", "allocation", "admission", "plan"), (entry[0] for entry in prepared.inputs)))[
                kind
            ]
            observed, payload = path.stat(), path.read_bytes()
            path.chmod(0o600)
            path.write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))
            path.chmod(0o400)
            os.utime(path, ns=(observed.st_atime_ns, observed.st_mtime_ns))
            assert path.stat().st_ino == observed.st_ino and path.stat().st_size == observed.st_size
            assert path.stat().st_mtime_ns == observed.st_mtime_ns
            assert path.stat().st_ctime_ns != observed.st_ctime_ns
            with pytest.raises(ValueError, match="original scope or custody"):
                prepared.families()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["selected", "occurrences"])
@pytest.mark.parametrize("damage", ["mode", "replace", "symlink"])
async def test_staged_file_substitution_is_refused_without_touching_siblings(tmp_path, monkeypatch, kind, damage):
    async with _case(tmp_path, monkeypatch) as native:
        sibling = tmp_path / "preserve-sibling"
        sibling.write_text("keep")
        with _prepared(tmp_path, native) as prepared:
            path = getattr(prepared, kind)
            if damage == "mode":
                path.chmod(0o600)
            elif damage == "replace":
                payload = path.read_bytes()
                original = path.rename(tmp_path / "original-staged-file")
                path.write_bytes(payload)
                path.chmod(0o400)
                assert path.stat().st_ino != original.stat().st_ino
            else:
                path.unlink()
                path.symlink_to(sibling)
            with pytest.raises(ValueError, match="custody"):
                prepared.families()
        assert sibling.read_text() == "keep" and not (tmp_path / "prepared-values").exists()


@pytest.mark.asyncio
async def test_staged_digest_is_checked_at_actual_eof_independently_of_stat_guards(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            path, original_stamp = prepared.occurrences, source_values._file_stamp
            stamp = original_stamp(path)
            payload = path.read_bytes()
            assert b"1.23" in payload
            path.chmod(0o600)
            path.write_bytes(payload.replace(b"1.23", b"9.23", 1))
            path.chmod(0o400)
            monkeypatch.setattr(
                source_values, "_file_stamp", lambda target: stamp if target == path else original_stamp(target)
            )
            with pytest.raises(ValueError, match="complete EOF bytes"):
                tuple(prepared._rows(path))


@pytest.mark.asyncio
async def test_scope_changes_after_last_family_still_refuse_successful_eof(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            with closing(prepared.families()) as families:
                next(families)
                path = Path(native.admission.root) / "admission.mpk"
                path.chmod(0o600)
                path.chmod(0o400)
                with pytest.raises(ValueError, match="original scope or custody"):
                    next(families)


@pytest.mark.asyncio
async def test_preparation_checks_original_custody_after_full_input_reads(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        original = source_values._all_occurrences

        def changed(*arguments):
            yield from original(*arguments)
            path = Path(native.plan.root) / "family-plan.mpk"
            path.chmod(0o600)
            path.chmod(0o400)

        monkeypatch.setattr(source_values, "_all_occurrences", changed)
        with pytest.raises(ValueError, match="original scope or custody"):
            with _prepared(tmp_path, native):
                pytest.fail("changed originals must not produce prepared families")
        assert not (tmp_path / "prepared-values").exists()


@pytest.mark.asyncio
async def test_changed_declared_counts_or_operation_deadline_do_not_rebind_preparation(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            counts = tuple((name, count + 1) for name, count in native.plan.counts)
            with pytest.raises(ValueError, match="original scope or custody"):
                prepared.check_scope(native.context, native.admission, replace(native.plan, counts=counts))
            request = replace(native.context.request, operation_deadline=999999999999.0)
            with pytest.raises(ValueError, match="original scope or custody"):
                prepared.check_scope(replace(native.context, request=request), native.admission, native.plan)


@pytest.mark.asyncio
async def test_empty_replays_still_consume_both_staged_files_to_eof(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, {"root_source": [[]], "detail_source": [[]]}) as native:
        with _prepared(tmp_path, native) as prepared:
            assert prepared.occurrences.stat().st_size == prepared.selected.stat().st_size == 0
            for _pass in range(2):
                with closing(prepared.families()) as families:
                    assert list(families) == []


@pytest.mark.asyncio
async def test_replay_cancel_cleans_owned_work_and_preserves_originals(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        requests = []

        def check():
            if requests:
                raise CancellationRequested("prepared SOURCE cancelled")

        with pytest.raises(CancellationRequested):
            with _prepared(tmp_path, native, check=check) as prepared:
                with closing(prepared.families()) as families:
                    _plan, family = next(families)
                    requests.append(True)
                    _children(family)
        assert not (tmp_path / "prepared-values").exists()
        assert Path(native.admission.root).exists() and Path(native.plan.root).exists()


@pytest.mark.asyncio
async def test_prepared_deadline_is_captured_once_for_all_replays(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        captures, checks, expired = [], [], []

        def deadline(request):
            captures.append(request)
            return 123.0

        def require(current):
            checks.append(current)
            if expired:
                raise TimeoutError("original prepared deadline expired")

        monkeypatch.setattr(source_values, "_read_deadline", deadline)
        monkeypatch.setattr(source_values, "_require_deadline", require)
        with _prepared(tmp_path, native) as prepared:
            assert len(_snapshot(prepared)) == len(_snapshot(prepared)) == 1
            expired.append(True)
            with pytest.raises(TimeoutError, match="original prepared deadline"):
                prepared.families()
        assert captures == [native.context.request] and checks and set(checks) == {123.0}


@pytest.mark.asyncio
async def test_requester_stop_interrupts_large_family_before_yield_and_drains(tmp_path, monkeypatch):
    rows_by_stream = {
        "root_source": [[_root()]],
        "detail_source": [[_child(key=f"child-{index:04}") for index in range(128)]],
    }
    async with _case(tmp_path, monkeypatch, rows_by_stream) as native:
        leaf = _Scratch(PlanWork(tmp_path / "requester", _LIMITS, lambda _path: None))
        original, processed = source_values._SourceFamily._accepted_child_order, []

        def accepted(*arguments):
            processed.append(True)
            if len(processed) == 32:
                leaf.stop.set()
            return original(*arguments)

        try:
            with _prepared(tmp_path, native) as prepared:
                staged_paths, staged_bytes = set(prepared.work.paths), prepared.work.used
                monkeypatch.setattr(source_values._SourceFamily, "_accepted_child_order", accepted)
                with closing(prepared.families(check=leaf.current)) as families, pytest.raises(CancellationRequested):
                    await _cpu(leaf, next, families)
                assert len(processed) == 32
                assert prepared._active is None and prepared._replay_check is None
                assert set(prepared.work.paths) == staged_paths and prepared.work.used == staged_bytes
                monkeypatch.setattr(source_values._SourceFamily, "_accepted_child_order", original)
                with closing(prepared.families()) as families:
                    _plan, family = next(families)
                    assert len(_children(family)) == 128 and next(families, None) is None
        finally:
            leaf.close()
        assert not (tmp_path / "prepared-values").exists()


@pytest.mark.asyncio
async def test_requester_check_resets_after_unstarted_close_and_initial_refusal(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            calls = []
            replay = prepared.families(check=lambda: calls.append(True))
            replay.close()
            observed = len(calls)
            assert _snapshot(prepared) and len(calls) == observed

            def stopped():
                raise CancellationRequested("requester already stopped")

            with pytest.raises(CancellationRequested):
                prepared.families(check=stopped)
            assert prepared._active is None and prepared._replay_check is None
            with pytest.raises(ValueError, match="cancellation check"):
                prepared.families(check=True)
            assert _snapshot(prepared)


@pytest.mark.asyncio
async def test_observations_do_not_construct_logical_families(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:

            def forbidden(*_arguments, **_keywords):
                raise AssertionError("original observation must precede family/scalar construction")

            monkeypatch.setattr(source_values, "_SourceFamily", forbidden)
            monkeypatch.setattr(source_values, "_record", forbidden)
            with closing(prepared.observations()) as observations:
                counts = [sum(1 for _record in records) for _plan, records in observations]
            assert counts == [3] and prepared._active is None


@pytest.mark.asyncio
async def test_observation_lifetime_and_exclusive_replay(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            replay = prepared.observations()
            _plan, records = next(replay)
            next(records)
            with pytest.raises(ValueError, match="active replay"):
                prepared.families()
            with pytest.raises(ValueError, match="active replay"):
                prepared.observations()
            replay.close()
            with pytest.raises(ValueError, match="no longer active"):
                next(records)
            assert _snapshot(prepared)
            replay = prepared.observations()
            _plan, records = next(replay)
            prepared.close()
            with pytest.raises(ValueError, match="no longer active"):
                next(records)


@pytest.mark.asyncio
async def test_observation_advance_requires_complete_family_eof(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            with closing(prepared.observations()) as observations:
                _plan, occurrences = next(observations)
                next(occurrences)
                with pytest.raises(ValueError, match="complete family EOF"):
                    next(observations)
            assert prepared._active is None and prepared._replay_check is None
            assert _snapshot(prepared)
