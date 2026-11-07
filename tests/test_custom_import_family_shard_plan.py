# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Base-aware exact union over actual Parquet SOURCE/admission owner artifacts."""

import asyncio
import json
import pickle
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path

import pytest

from process.custom_import import family_shard_admission as admission
from process.custom_import import family_shard_plan as planner
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import CandidateRejected
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.runner_codec import digest_text
from tests.test_custom_import_family_shard_admission import _build_audit, _inputs
from tests.test_custom_import_family_shard_source_plan import _child, _root
from tests.test_custom_import_family_shard_worker import _projected_definition

_LIMITS = SourceFactLimits(64 * 1024**2, 1024**2, 512)


def _definition(mode):
    document = json.loads(_projected_definition().canonical)
    document["refresh_mode"] = mode
    return CustomImportDefinition.from_mapping(document)


def _assignments(descriptor):
    assignments = [
        (header["typed_key"], header["family_ids"])
        for _shard, header in admission.read_admission_families(descriptor)
        if header["family_ids"] is not None
    ]
    return sorted(assignments, key=lambda entry: (entry[0][1], entry[0][0]))


def _roots(descriptor):
    return [
        occurrence
        for occurrence in admission.read_admission_occurrences(descriptor)
        if occurrence.record.collection_slot == 0 and occurrence.record.typed_parent_key is not None
    ]


def _base(typed_key, *, root=500, index=1, snapshot=31):
    return planner.BaseFamilyHeader(
        typed_key, root, 1000 + index, 2000 + index, 3000 + index, bytes([index]) * 32, index, 77, snapshot
    )


def _opaque_key(text):
    return text, digest_text("root-key", text)


class _Headers:
    def __init__(self, headers=(), *, fail=False):
        self.headers = sorted(headers, key=lambda header: (header.typed_key[1], header.typed_key[0]))
        self.fail = fail
        self.started = self.complete = self.closed = False

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        self.started = True
        try:
            for header in self.headers:
                await asyncio.sleep(0)
                yield header
            if self.fail:
                raise RuntimeError("late base stream failure")
            self.complete = True
        finally:
            self.closed = True


class _Metadata:
    def __init__(self, assignments):
        self.assignments = assignments
        self.complete = self.closed = False

    def __iter__(self):
        try:
            yield from self.assignments
            self.complete = True
        finally:
            self.closed = True


@asynccontextmanager
async def _audit(tmp_path, monkeypatch, rows=None, *, mode="snapshot", complete=True):
    async with _inputs(tmp_path, monkeypatch, rows, definition=_definition(mode), complete_scope=complete) as case:
        descriptor = _build_audit(tmp_path, *case, _LIMITS, complete_scope=complete)
        try:
            yield replace(case[0][1], complete_scope=complete), descriptor
        finally:
            admission.cleanup_admission_output(descriptor)


async def _build(
    tmp_path, request, descriptor, *, headers=None, assignments=None, scope=None, limits=_LIMITS, check=None
):
    registrations = []
    output = await planner.build_family_plan(
        request,
        descriptor,
        headers,
        _assignments(descriptor) if assignments is None else assignments,
        work=planner.PlanWork(tmp_path / "final-plan", limits, registrations.append, check),
        base_scope=scope,
    )
    assert registrations == [tmp_path / "final-plan"]
    assert pickle.loads(pickle.dumps(output)) == output
    assert {entry.name for entry in Path(output.root).iterdir()} == {"family-plan.mpk"}
    return output


def _cleanup(descriptor):
    planner.cleanup_family_plan(descriptor)
    assert not Path(descriptor.root).exists()


@pytest.mark.parametrize("mode", ("snapshot", "upsert"))
async def test_original_source_selection_and_exact_identity_without_base(tmp_path, monkeypatch, mode):
    async with _audit(tmp_path, monkeypatch, mode=mode) as (request, descriptor):
        output = await _build(tmp_path, request, descriptor)
        try:
            (selected,) = planner.read_family_plan(output)
            original = _roots(descriptor)[0]
            assert selected.selection_kind == "source"
            assert selected.source_root == original
            assert selected.root_record_id == original.root_record_id
            assert selected.source_metadata == _assignments(descriptor)[0][1]
            assert selected.source_header["source_facts"] == original.facts_by_name
            assert selected.base_header is None
            assert list(planner.read_invalid_plan_audit(output)) == []
            assert output.counts_by_kind["source"] == output.counts_by_kind["families"] == 1
            assert output.admission_sha256 == descriptor.sha256
        finally:
            _cleanup(output)


@pytest.mark.parametrize("mode,missing", [("snapshot", "absent"), ("upsert", "retained")])
async def test_union_keeps_every_base_key_and_source_replaces_matching_base(tmp_path, monkeypatch, mode, missing):
    async with _audit(tmp_path, monkeypatch, mode=mode) as (request, descriptor):
        root = _roots(descriptor)[0]
        matching = _base(root.record.typed_parent_key, root=root.root_record_id)
        old_key = _opaque_key(' { "former_field" : "opaque lexical identity" } ')
        other = _base(old_key, root=100003, index=2)
        headers = _Headers([matching, other])
        output = await _build(
            tmp_path,
            replace(request, expected_base_generation_id=77, expected_pointer_version=3),
            descriptor,
            headers=headers,
            scope=(77, 31),
        )
        try:
            selections = list(planner.read_family_plan(output))
            by_key = {entry.typed_key: entry for entry in selections}
            assert len(selections) == output.counts_by_kind["families"] == 2
            assert by_key[matching.typed_key].selection_kind == "source"
            assert by_key[matching.typed_key].base_header == matching
            assert by_key[old_key].selection_kind == missing
            assert by_key[old_key].source_root is None
            assert by_key[old_key].base_header == other
            assert by_key[old_key].root_record_id == other.root_record_id
            assert headers.started and headers.complete and headers.closed
        finally:
            _cleanup(output)


@pytest.mark.parametrize("mode", ("snapshot", "upsert"))
@pytest.mark.parametrize("has_base", (False, True))
async def test_original_rejected_source_retains_only_actual_matching_base(tmp_path, monkeypatch, mode, has_base):
    rows_by_stream = {"root_source": [[_root(), _root()]], "detail_source": [[_child()]]}
    async with _audit(tmp_path, monkeypatch, rows_by_stream, mode=mode) as (request, descriptor):
        root = _roots(descriptor)[0]
        base = _base(root.record.typed_parent_key, root=root.root_record_id)
        headers = _Headers([base]) if has_base else None
        if has_base:
            request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
        output = await _build(tmp_path, request, descriptor, headers=headers, scope=(77, 31) if has_base else None)
        try:
            (selected,) = planner.read_family_plan(output)
            assert selected.selection_kind == ("retained" if has_base else "absent")
            assert selected.source_header["selection"] == "absent"  # Provisional worker had no base.
            assert selected.source_resolution == next(
                resolution
                for resolution in admission.read_admission_resolutions(descriptor)
                if resolution.target == root.coordinate
            )
            assert selected.source_root == root
            assert selected.base_header == (base if has_base else None)
        finally:
            _cleanup(output)


@pytest.mark.parametrize("snapshot", (31, None))
async def test_empty_pinned_base_requires_actual_async_eof_and_explicit_scope(tmp_path, monkeypatch, snapshot):
    async with _audit(tmp_path, monkeypatch) as (request, descriptor):
        request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
        headers = _Headers()
        output = await _build(tmp_path, request, descriptor, headers=headers, scope=(77, snapshot))
        assert headers.complete and headers.closed and output.base_scope == (77, snapshot)
        _cleanup(output)
        with pytest.raises(ValueError, match="actual pinned base header stream"):
            await _build(tmp_path, request, descriptor, headers=None, scope=(77, snapshot))
        assert not (tmp_path / "final-plan").exists()


@pytest.mark.parametrize("scope", (None, (78, 31), (77, 32)))
async def test_scope_or_individual_base_snapshot_mismatch_refuses(tmp_path, monkeypatch, scope):
    async with _audit(tmp_path, monkeypatch) as (request, descriptor):
        request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
        root = _roots(descriptor)[0]
        headers = _Headers([_base(root.record.typed_parent_key, root=root.root_record_id)])
        with pytest.raises(ValueError, match="scope|snapshot"):
            await _build(tmp_path, request, descriptor, headers=headers, scope=scope)
        assert not (tmp_path / "final-plan").exists()


def _damaged_assignments(assignments, damage):
    if damage == "missing":
        assignments.pop()
    elif damage == "extra":
        assignments.append((_opaque_key("extra original key"), (800000, 900000)))
        assignments.sort(key=lambda entry: (entry[0][1], entry[0][0]))
    elif damage == "duplicate":
        assignments.insert(0, assignments[0])
    elif damage == "changed":
        assignments[0] = assignments[0][0], (800000, assignments[0][1][1])
    else:
        assignments.reverse()
    return assignments


@pytest.mark.parametrize("damage", ("missing", "extra", "duplicate", "changed", "unordered"))
async def test_source_metadata_must_match_complete_original_native_reservations(tmp_path, monkeypatch, damage):
    rows_by_stream = {"root_source": [[_root(), _root("1234567893")]], "detail_source": [[_child()]]}
    async with _audit(tmp_path, monkeypatch, rows_by_stream) as (request, descriptor):
        assignments = _assignments(descriptor)
        assignments = _damaged_assignments(assignments, damage)
        metadata = _Metadata(assignments)
        with pytest.raises(ValueError, match="metadata|reservations"):
            await _build(tmp_path, request, descriptor, assignments=metadata)
        assert metadata.closed and not (tmp_path / "final-plan").exists()


def _damaged_base(first, other, root, damage):
    if damage == "duplicate-key":
        other = replace(first, family_revision_id=100009)
    elif damage == "root-rebind":
        first = replace(first, root_record_id=root.root_record_id + 100000)
    elif damage == "base-family-rebind":
        other = replace(other, family_revision_id=first.family_revision_id)
    elif damage == "bool-id":
        first = replace(first, root_record_id=True)
    else:
        first = replace(first, typed_key=(first.typed_key[0], bytes(32)))
    return first, other


@pytest.mark.parametrize("damage", ("duplicate-key", "root-rebind", "base-family-rebind", "bool-id", "key-sha"))
async def test_native_base_identity_duplicates_and_rebinding_fail_closed(tmp_path, monkeypatch, damage):
    async with _audit(tmp_path, monkeypatch) as (request, descriptor):
        root = _roots(descriptor)[0]
        first = _base(root.record.typed_parent_key, root=root.root_record_id)
        other = _base(_opaque_key("different legacy key"), root=500001, index=2)
        first, other = _damaged_base(first, other, root, damage)
        request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
        with pytest.raises(ValueError, match="repeat|rebind|different exact|identity|typed-key"):
            await _build(tmp_path, request, descriptor, headers=_Headers([first, other]), scope=(77, 31))
        assert not (tmp_path / "final-plan").exists()


@pytest.mark.parametrize("problem", ("scope-incomplete", "candidate-error", "fatal"))
async def test_original_errors_refuse_after_complete_inputs(tmp_path, monkeypatch, problem):
    document = json.loads(_projected_definition().canonical)
    rows_by_stream = (
        {"root_source": [[_root(npi=None)]], "detail_source": [[]]} if problem == "candidate-error" else None
    )
    if problem == "fatal":
        original_stage = planner._stage_admission

        def stage_with_fatal(work, descriptor):
            # Fault injection tests only the preserved fatal guard after REAL EOF.
            staged = original_stage(work, descriptor)
            return staged[:2] + (((1, 0), "original raw-key collision"),) + staged[3:]

        monkeypatch.setattr(planner, "_stage_admission", stage_with_fatal)
    is_complete = problem != "scope-incomplete"
    async with _inputs(
        tmp_path,
        monkeypatch,
        rows_by_stream,
        definition=CustomImportDefinition.from_mapping(document),
        complete_scope=is_complete,
    ) as case:
        descriptor = _build_audit(tmp_path, *case, _LIMITS, complete_scope=is_complete)
        try:
            request = replace(
                case[0][1], complete_scope=is_complete, expected_base_generation_id=77, expected_pointer_version=3
            )
            headers, metadata = _Headers(), _Metadata(_assignments(descriptor))
            exception = ValueError if problem == "fatal" else CandidateRejected
            with pytest.raises(exception):
                await _build(tmp_path, request, descriptor, headers=headers, assignments=metadata, scope=(77, 31))
            assert headers.complete and headers.closed and metadata.complete and metadata.closed
            assert list(admission.read_admission_occurrences(descriptor))
            assert not (tmp_path / "final-plan").exists()
        finally:
            admission.cleanup_admission_output(descriptor)


async def test_late_base_failure_closes_borrowed_iterator_and_exact_scratch(tmp_path, monkeypatch):
    async with _audit(tmp_path, monkeypatch) as (request, descriptor):
        metadata, headers = _Metadata(_assignments(descriptor)), _Headers(fail=True)
        request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
        with pytest.raises(RuntimeError, match="late base stream"):
            await _build(tmp_path, request, descriptor, headers=headers, assignments=metadata, scope=(77, 31))
        assert metadata.complete and metadata.closed and headers.closed and not headers.complete
        assert not (tmp_path / "final-plan").exists()
        assert list(admission.read_admission_occurrences(descriptor))


async def test_resource_exhaustion_and_owner_cancellation_cleanup_exact_work(tmp_path, monkeypatch):
    async with _audit(tmp_path, monkeypatch) as (request, descriptor):
        with pytest.raises(SourceFactBudgetExceeded):
            await _build(tmp_path, request, descriptor, limits=SourceFactLimits(64, 64, 64))
        assert not (tmp_path / "final-plan").exists()
        polls = []

        def check():
            polls.append(None)
            if len(polls) == 15:
                raise RuntimeError("owner cancelled")

        with pytest.raises(RuntimeError, match="owner cancelled"):
            await _build(tmp_path, request, descriptor, check=check)
        assert len(polls) == 15 and not (tmp_path / "final-plan").exists()


@pytest.mark.parametrize("damage", ("truncated", "changed", "extra-file", "inode"))
async def test_closed_plan_custody_checksum_and_complete_eof_are_required(tmp_path, monkeypatch, damage):
    async with _audit(tmp_path, monkeypatch) as (request, descriptor):
        output = await _build(tmp_path, request, descriptor)
        target = Path(output.root) / "family-plan.mpk"
        original = target.read_bytes()
        if damage == "extra-file":
            (Path(output.root) / "extra").write_bytes(b"extra")
        elif damage == "inode":
            previous = target.rename(tmp_path / "original-family-plan")
            target.write_bytes(original)
            target.chmod(0o400)
            assert target.stat().st_ino != previous.stat().st_ino
        else:
            target.chmod(0o600)
            target.write_bytes(original[:-1] if damage == "truncated" else original[:-1] + bytes([original[-1] ^ 1]))
        with pytest.raises(ValueError):
            list(planner.read_family_plan(output))
        # The exact descriptor must not authorize deleting a changed directory.
        if damage in ("extra-file", "inode", "truncated"):
            with pytest.raises(ValueError):
                planner.cleanup_family_plan(output)
        if damage == "extra-file":
            (Path(output.root) / "extra").unlink()
            _cleanup(output)
        else:
            # Explicit test-owned replacement custody; production cleanup refuses it.
            target.unlink()
            Path(output.root).rmdir()


@pytest.mark.parametrize("mode", ("snapshot", "upsert"))
async def test_initial_rejected_typed_root_has_original_identity_without_family_reservation(
    tmp_path, monkeypatch, mode
):
    rows_by_stream = {"root_source": [[_root(score=None)]], "detail_source": [[]]}
    async with _audit(tmp_path, monkeypatch, rows_by_stream, mode=mode) as (request, descriptor):
        (root,) = _roots(descriptor)
        assert root.revision_id is None and root.initial_rejection_id is not None
        assert _assignments(descriptor) == []
        base = _base(root.record.typed_parent_key, root=root.root_record_id)
        request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
        output = await _build(tmp_path, request, descriptor, headers=_Headers([base]), scope=(77, 31))
        try:
            (selected,) = planner.read_family_plan(output)
            assert selected.selection_kind == "retained"
            assert selected.source_root == root
            assert selected.source_metadata is None
            assert selected.source_resolution.event.origin == "source"
            assert selected.source_resolution.event.rejection_id == root.initial_rejection_id
        finally:
            _cleanup(output)


@pytest.mark.parametrize("mode,complete,selected", [("snapshot", True, "absent"), ("upsert", False, "retained")])
async def test_empty_source_still_consumes_all_base_headers_and_preserves_selection(
    tmp_path, monkeypatch, mode, complete, selected
):
    rows_by_stream = {"root_source": [[]], "detail_source": [[]]}
    async with _audit(tmp_path, monkeypatch, rows_by_stream, mode=mode, complete=complete) as (request, descriptor):
        headers = _Headers([_base(_opaque_key("original definition key"))])
        metadata = _Metadata(())
        request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
        output = await _build(tmp_path, request, descriptor, headers=headers, assignments=metadata, scope=(77, 31))
        try:
            (family,) = planner.read_family_plan(output)
            assert family.selection_kind == selected and family.source_root is None
            assert headers.complete and headers.closed and metadata.complete and metadata.closed
            assert output.counts_by_kind["source_families"] == 0
        finally:
            _cleanup(output)


async def test_empty_snapshot_still_refuses_incomplete_scope_after_all_eof(tmp_path, monkeypatch):
    rows_by_stream = {"root_source": [[]], "detail_source": [[]]}
    async with _audit(tmp_path, monkeypatch, rows_by_stream, complete=False) as (request, descriptor):
        request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
        headers, metadata = _Headers(), _Metadata(())
        with pytest.raises(CandidateRejected, match="snapshot_scope_incomplete"):
            await _build(tmp_path, request, descriptor, headers=headers, assignments=metadata, scope=(77, 31))
        assert headers.complete and headers.closed and metadata.complete and metadata.closed
        assert not (tmp_path / "final-plan").exists()


async def test_exact_key_variants_stay_distinct_and_root_digest_collision_refuses(tmp_path, monkeypatch):
    rows_by_stream = {"root_source": [[]], "detail_source": [[]]}
    async with _audit(tmp_path, monkeypatch, rows_by_stream, mode="upsert") as (request, descriptor):
        request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
        variants = [
            _base(_opaque_key(text), root=500 + index, index=index + 1)
            for index, text in enumerate(('{"x":"a"}', '{ "x" : "a" }'))
        ]
        output = await _build(tmp_path, request, descriptor, headers=_Headers(variants), scope=(77, 31))
        assert {entry.typed_key for entry in planner.read_family_plan(output)} == {
            entry.typed_key for entry in variants
        }
        _cleanup(output)
        # The collision branch is a component check, not a different production hash.
        monkeypatch.setattr(planner, "digest_text", lambda _domain, _text: b"c" * 32)
        collisions = [replace(header, typed_key=(header.typed_key[0], b"c" * 32)) for header in variants]
        with pytest.raises(ValueError, match="digest collision"):
            await _build(tmp_path, request, descriptor, headers=_Headers(collisions), scope=(77, 31))
        assert not (tmp_path / "final-plan").exists()


async def test_task_cancellation_joins_cpu_stage_before_deleting_owned_scratch(tmp_path, monkeypatch):
    async with _audit(tmp_path, monkeypatch) as (request, descriptor):
        entered, cancelled = asyncio.Event(), asyncio.Event()
        loop = asyncio.get_running_loop()
        original = planner._stage_source

        def slow_stage(work, *arguments):
            staged = original(work, *arguments)
            loop.call_soon_threadsafe(entered.set)
            while not work.stop.wait(0.01):
                work.current()
            loop.call_soon_threadsafe(cancelled.set)
            work.current()
            return staged

        monkeypatch.setattr(planner, "_stage_source", slow_stage)
        task = asyncio.create_task(_build(tmp_path, request, descriptor))
        await asyncio.wait_for(entered.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert cancelled.is_set() and not (tmp_path / "final-plan").exists()
        assert list(admission.read_admission_occurrences(descriptor))
