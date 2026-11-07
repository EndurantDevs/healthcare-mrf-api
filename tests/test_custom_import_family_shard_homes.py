# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real completed SOURCE/admission artifacts; explicitly fake only native SQL."""

import asyncio
import hashlib
import threading
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql

from process.custom_import import family_shard_admission as audit
from process.custom_import import family_shard_allocate as allocation
from process.custom_import import family_shard_homes as homes
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_shard_common import SourceCommonContext
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_output import _pack_row
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.runner_types import CancellationRequested
from tests import test_custom_import_family_shard_admission as fixtures
from tests.test_custom_import_family_shard_source_plan import _child, _root
from tests.test_custom_import_family_shard_worker import _projected_definition

_STAMP = datetime(2030, 1, 2, tzinfo=timezone.utc)
_LIMITS = SourceFactLimits(64 * 1024**2, 1024**2, 4096)


def _allocation_ids(monkeypatch, mode):
    original_boundary = fixtures._native_boundary

    def boundary(*arguments):
        observations = original_boundary(*arguments)
        original_allocate = allocation._allocate_record_ids
        original_bound = allocation.allocate_bound_ids
        offsets_by_kind = {}

        async def allocated(session, kinds, sequences=None):
            native_ids = list(
                await (
                    original_allocate(session, kinds)
                    if sequences is None
                    else original_bound(session, kinds, sequences)
                )
            )
            for index, kind in enumerate(kinds):
                if kind in {"root_revision", "child_revision"}:
                    offset = offsets_by_kind.get(kind, 0)
                    native_ids[index] = 2**63 - 1 - (0 if mode == "duplicate" else offset)
                    offsets_by_kind[kind] = offset + 17
            return tuple(native_ids)

        monkeypatch.setattr(allocation, "_allocate_record_ids", allocated)
        monkeypatch.setattr(allocation, "allocate_bound_ids", allocated)
        return observations

    monkeypatch.setattr(fixtures, "_native_boundary", boundary)


@asynccontextmanager
async def _case(tmp_path, monkeypatch, *, empty=False, collapse=False, ids=None, base=False):
    if ids is not None:
        _allocation_ids(monkeypatch, ids)
    rows_by_stream = {
        "root_source": [[]] if empty else [[_root(), _root(npi=None)]],
        "detail_source": [[]] if empty else [[_child(), _child(), _child(key=None)]],
    }
    async with fixtures._inputs(
        tmp_path, monkeypatch, rows_by_stream, definition=_projected_definition(collapse=collapse)
    ) as (source_case, allocated, workers):
        admission = fixtures._build_audit(tmp_path, source_case, allocated, workers, _LIMITS)
        case = SimpleNamespace(admission=admission)
        try:
            request = replace(source_case[1], complete_scope=True)
            if base:
                request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
            case.common = SourceCommonContext(
                request, FamilyBlockLayout(request.definition, source_case[2].child_collection_slots), allocated
            )
            yield case
        finally:
            audit.cleanup_admission_output(case.admission)


class _NativeBoundary:
    """Transaction/SQL transport fake, never evidence for PostgreSQL constraints."""

    def __init__(self, case):
        self.case, self.observations = case, []
        self.homes_by_id, self.canonical_ids = {}, set()
        self.owner_only, self.phase = True, "loading"
        self.fail_commit, self.damage_result = None, None
        self.fail_commit_after = 0
        self.entered, self.release = None, None
        self.build = SimpleNamespace(
            phase="graph",
            source_frozen_at=_STAMP,
            verified_at=None,
            source_occurrence_count=case.common.source.source.counts[0],
            capture_bundle_id=dict(case.common.source.source.scope[0])["capture_bundle_id"],
        )

    @asynccontextmanager
    async def page(self, factory, request, build_id):
        assert factory is None and request is self.case.common.request and build_id == 31
        original_homes_by_id = dict(self.homes_by_id)
        self.observations.append("begin")
        try:
            yield self, self.build
            self.observations.append("fresh_authority")
            if self.fail_commit is not None and self.observations.count("commit") >= self.fail_commit_after:
                raise self.fail_commit
        except BaseException:
            self.homes_by_id = original_homes_by_id
            self.observations.append("rollback")
            raise
        self.observations.append("commit")

    async def candidate(self, session, request, family, layout, phases):
        assert session is self and request is self.case.common.request
        assert family == self.case.common.source.snapshot_family_id
        assert layout is self.case.common.layout and phases == {"loading", "closed"}
        self.observations.append("candidate")
        return SimpleNamespace(phase=self.phase), None

    async def capture(self, session, common):
        assert session is self and common is self.case.common
        self.observations.append("capture")
        return b"c" * 32

    async def schema(self, session):
        assert session is self
        return postgresql.dialect().identifier_preparer, "synthetic"

    async def execute(self, session, statement, parameters=None):
        assert session is self
        if statement.startswith("LOCK TABLE"):
            self.observations.append("home_lock")
            return None
        if statement == homes._HOME_OWNER:
            self.observations.append("home_owner")
            return SimpleNamespace(scalar_one_or_none=lambda: self.owner_only)
        assert statement == homes._APPEND.format(control="synthetic")
        self.observations.append(("append", tuple(zip(parameters["kinds"], parameters["ids"], strict=True))))
        native_counts = self._append(parameters)
        if self.entered is not None:
            self.entered.set()
            await self.release.wait()
        return SimpleNamespace(one=lambda: native_counts)

    def _append(self, parameters):
        family = parameters["family_id"]
        pairs = tuple(zip(parameters["kinds"], parameters["ids"], strict=True))
        assert pairs == tuple(sorted(set(pairs)))
        invalid = any(pair in self.canonical_ids or self.homes_by_id.get(pair, family) != family for pair in pairs)
        missing_ids = tuple(pair for pair in pairs if pair not in self.homes_by_id)
        if not invalid:
            self.homes_by_id.update(dict.fromkeys(missing_ids, family))
        counts = (
            invalid,
            sum(kind == 1 for kind, _identifier in pairs),
            sum(kind == 2 for kind, _identifier in pairs),
            sum(kind == 1 for kind, _identifier in missing_ids),
            sum(kind == 2 for kind, _identifier in missing_ids),
            0 if invalid else len({kind for kind, _identifier in missing_ids}),
            0 if invalid else len(missing_ids),
        )
        return counts if self.damage_result is None else self.damage_result(counts)

    def install(self, monkeypatch):
        monkeypatch.setattr(homes, "_page_session", self.page)
        monkeypatch.setattr(homes, "_candidate", self.candidate)
        monkeypatch.setattr(homes, "_source_digest", self.capture)
        monkeypatch.setattr(homes, "_control_schema", self.schema)
        monkeypatch.setattr(homes, "_execute", self.execute)


async def _register(tmp_path, case, *, limits=None, work=None, registered=None):
    registered = [] if registered is None else registered
    return await homes.register_source_homes(
        None,
        case.common,
        31,
        case.admission,
        work=work or PlanWork(tmp_path / "homes", _LIMITS, registered.append),
        limits=limits or allocation.SourceAllocationLimits(_LIMITS, 2, 4096),
    )


def _original_ids(case):
    return {
        (1 if occurrence.collection_slot == 0 else 2, occurrence.revision_id)
        for occurrence in audit.read_admission_occurrences(case.admission)
        if occurrence.revision_id is not None
    }


@pytest.mark.parametrize("collapse", (False, True))
@pytest.mark.parametrize("base", (False, True))
async def test_all_initial_ids_survive_selection_and_exact_retry(tmp_path, monkeypatch, collapse, base):
    async with _case(tmp_path, monkeypatch, collapse=collapse, base=base) as case:
        native = _NativeBoundary(case)
        native.install(monkeypatch)
        registered_roots = []
        result = await _register(tmp_path, case, registered=registered_roots)
        assert registered_roots == [tmp_path / "homes"]
        assert result == homes.SourceHomeCounts(1, 2, 1, 2)
        assert set(native.homes_by_id) == _original_ids(case)
        assert {family for family in native.homes_by_id.values()} == {case.common.source.snapshot_family_id}
        assert (1, 57) in native.homes_by_id and (2, 57) in native.homes_by_id
        assert (2, 58) not in native.homes_by_id  # Observed gaps are never reserved intervals.
        assert native.observations.count("capture") == native.observations.count("fresh_authority") == 2
        assert await _register(tmp_path, case) == homes.SourceHomeCounts(1, 2, 0, 0)
        assert not (tmp_path / "homes").exists()


@pytest.mark.parametrize(
    "phase,build_phase",
    [
        ("loading", "admission"),
        ("loading", "graph"),
        ("loading", "output"),
        ("closed", "output"),
        ("closed", "verifying"),
    ],
)
async def test_empty_source_still_requires_fresh_native_authority(tmp_path, monkeypatch, phase, build_phase):
    async with _case(tmp_path, monkeypatch, empty=True) as case:
        native = _NativeBoundary(case)
        native.phase, native.build.phase = phase, build_phase
        native.install(monkeypatch)
        assert await _register(tmp_path, case) == homes.SourceHomeCounts(0, 0, 0, 0)
        assert native.observations == [
            "begin",
            "candidate",
            "capture",
            "home_lock",
            "home_owner",
            ("append", ()),
            "fresh_authority",
            "commit",
        ]
        assert not (tmp_path / "homes").exists()


async def test_bigint_max_remains_an_observed_id(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, ids="max") as case:
        native = _NativeBoundary(case)
        native.install(monkeypatch)
        assert await _register(tmp_path, case) == homes.SourceHomeCounts(1, 2, 1, 2)
        assert (1, 2**63 - 1) in native.homes_by_id and (2, 2**63 - 1) in native.homes_by_id
        assert (2, 2**63 - 18) in native.homes_by_id


async def test_duplicate_actual_kind_id_refuses_before_any_native_transaction(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, ids="duplicate") as case:
        native = _NativeBoundary(case)
        native.install(monkeypatch)
        with pytest.raises(FamilyBlockUnavailable, match="revision ID repeats"):
            await _register(tmp_path, case)
        assert native.observations == [] and not (tmp_path / "homes").exists()


@pytest.mark.parametrize("conflict", ("foreign", "canonical_root", "canonical_child"))
async def test_foreign_or_canonical_coverage_never_becomes_own_home(tmp_path, monkeypatch, conflict):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        if conflict == "foreign":
            native.homes_by_id[(1, 57)] = 99
        else:
            native.canonical_ids.add((1 if conflict == "canonical_root" else 2, 57))
        original_homes_by_id = dict(native.homes_by_id)
        native.install(monkeypatch)
        with pytest.raises(FamilyBlockUnavailable, match="canonical or foreign"):
            await _register(tmp_path, case)
        assert native.homes_by_id == original_homes_by_id and native.observations[-1] == "rollback"
        assert not (tmp_path / "homes").exists()


@pytest.mark.parametrize(
    "damage",
    (
        "source_phase",
        "rejected",
        "verified",
        "closed_graph",
        "logged",
        "source_frozen",
        "verified_stamp",
        "capture",
        "count",
        "owner",
    ),
)
async def test_fresh_phase_capture_and_owner_refusals_are_not_skipped(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        match damage:
            case "source_phase":
                native.build.phase = "source"
            case "rejected":
                native.build.phase = "rejected"
            case "verified":
                native.build.phase = "verified"
            case "closed_graph":
                native.phase = "closed"
            case "logged":
                native.phase = "logged"
            case "source_frozen":
                native.build.source_frozen_at = None
            case "verified_stamp":
                native.build.verified_at = _STAMP
            case "capture":
                native.build.capture_bundle_id += 1
            case "count":
                native.build.source_occurrence_count += 1
            case "owner":
                native.owner_only = False
        native.install(monkeypatch)
        with pytest.raises(FamilyBlockUnavailable):
            await _register(tmp_path, case)
        assert native.homes_by_id == {} and native.observations[-1] == "rollback"


def _rewrite_admission(descriptor, mutation):
    entries = list(audit._entries(descriptor, None))
    mutation(entries)
    path = Path(descriptor.root) / "admission.mpk"
    path.chmod(0o600)
    with path.open("wb") as output:
        for entry in entries:
            encoded = _pack_row(entry)
            output.write(audit._LENGTH.pack(len(encoded)))
            output.write(encoded)
    path.chmod(0o400)
    return replace(descriptor, sha256=hashlib.sha256(path.read_bytes()).digest(), file_bytes=path.stat().st_size)


@pytest.mark.parametrize(
    "damage",
    (
        "scope",
        "source_sha",
        "allocation_sha",
        "admission_sha",
        "late_eof",
        "admission_id",
        "false_id",
        "source_bytes",
        "allocation_bytes",
        "admission_bytes",
    ),
)
async def test_complete_original_artifacts_are_required_before_writes(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        native.install(monkeypatch)
        match damage:
            case "scope":
                case.admission = replace(case.admission, snapshot_family_id=99)
            case "source_sha":
                case.common = replace(
                    case.common,
                    source=replace(case.common.source, source=replace(case.common.source.source, sha256=b"s" * 32)),
                )
            case "allocation_sha":
                case.common = replace(case.common, source=replace(case.common.source, sha256=b"a" * 32))
            case "admission_sha":
                case.admission = replace(case.admission, sha256=b"e" * 32)
            case "late_eof":
                case.admission = _rewrite_admission(case.admission, lambda entries: entries.pop())
            case "admission_id" | "false_id":

                def mutate(entries):
                    index = next(index for index, entry in enumerate(entries) if entry[0] == "occurrence")
                    cells = list(entries[index][1])
                    cells[5] = False if damage == "false_id" else cells[5] + 1
                    entries[index] = ("occurrence", tuple(cells))

                case.admission = _rewrite_admission(case.admission, mutate)
            case "source_bytes" | "allocation_bytes" | "admission_bytes":
                descriptor = {
                    "source_bytes": case.common.source.source,
                    "allocation_bytes": case.common.source,
                    "admission_bytes": case.admission,
                }[damage]
                path = next(Path(descriptor.root).iterdir())
                path.chmod(0o600)
                with path.open("r+b") as output:
                    output.seek(-1, 2)
                    original = output.read(1)
                    output.seek(-1, 2)
                    output.write(bytes([original[0] ^ 1]))
                path.chmod(0o400)
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _register(tmp_path, case)
        assert native.observations == [] and not (tmp_path / "homes").exists()


@pytest.mark.parametrize("damage", ("false_count", "missing", "extra"))
async def test_native_count_damage_rolls_back_the_batch(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        index = 1 if damage == "false_count" else 6
        value = False if damage == "false_count" else 0 if damage == "missing" else 999
        native.damage_result = lambda counts: (*counts[:index], value, *counts[index + 1 :])
        native.install(monkeypatch)
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _register(tmp_path, case)
        assert native.homes_by_id == {} and native.observations[-1] == "rollback"


async def test_changed_precommit_authority_rolls_back_and_retry_is_exact(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        native.fail_commit = RuntimeError("synthetic changed lease")
        native.install(monkeypatch)
        with pytest.raises(RuntimeError, match="changed lease"):
            await _register(tmp_path, case)
        assert native.homes_by_id == {} and not (tmp_path / "homes").exists()
        native.fail_commit = None
        assert await _register(tmp_path, case) == homes.SourceHomeCounts(1, 2, 1, 2)


async def test_later_failed_batch_keeps_only_exact_committed_homes(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        native.fail_commit, native.fail_commit_after = RuntimeError("synthetic later lease loss"), 1
        native.install(monkeypatch)
        with pytest.raises(RuntimeError, match="later lease loss"):
            await _register(tmp_path, case)
        assert set(native.homes_by_id) == {(1, 57), (2, 57)}
        assert not (tmp_path / "homes").exists()
        native.fail_commit = None
        assert await _register(tmp_path, case) == homes.SourceHomeCounts(1, 2, 0, 1)
        assert set(native.homes_by_id) == _original_ids(case)


async def test_self_consistent_allocation_tail_fails_before_any_native_write(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        path = Path(case.common.source.root) / "source-ids.mpk"
        path.chmod(0o600)
        with path.open("ab") as output:
            output.write(b"extra")
        path.chmod(0o400)
        allocated = replace(
            case.common.source, file_bytes=path.stat().st_size, sha256=hashlib.sha256(path.read_bytes()).digest()
        )
        case.common = replace(case.common, source=allocated)

        def header(entries):
            entries[0] = (*entries[0][:3], allocated.sha256, *entries[0][4:])

        case.admission = _rewrite_admission(replace(case.admission, allocation_sha256=allocated.sha256), header)
        native = _NativeBoundary(case)
        native.install(monkeypatch)
        with pytest.raises(ValueError, match="trailing identities"):
            await _register(tmp_path, case)
        assert native.observations == [] and not (tmp_path / "homes").exists()


async def test_native_cancellation_rolls_back_before_spool_cleanup(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        native.entered, native.release = asyncio.Event(), asyncio.Event()
        native.install(monkeypatch)
        pending = asyncio.create_task(_register(tmp_path, case))
        try:
            await asyncio.wait_for(native.entered.wait(), 2)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
        finally:
            native.release.set()
            if not pending.done():
                pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        assert native.homes_by_id == {} and native.observations[-1] == "rollback"
        assert not (tmp_path / "homes").exists()


async def test_repeated_cancellation_joins_source_thread_before_cleanup(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        native.install(monkeypatch)
        entered, stopped, release = threading.Event(), threading.Event(), threading.Event()

        def stage(work, *_arguments):
            entered.set()
            try:
                while not release.wait(0.01):
                    work.current()
            except CancellationRequested:
                stopped.set()
                release.wait(2)
                raise

        monkeypatch.setattr(homes, "_stage_ids", stage)
        pending = asyncio.create_task(_register(tmp_path, case))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            pending.cancel()
            assert await asyncio.to_thread(stopped.wait, 2)
            pending.cancel()
            assert (tmp_path / "homes").exists()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await pending
        finally:
            release.set()
            await asyncio.gather(pending, return_exceptions=True)
        assert native.observations == [] and not (tmp_path / "homes").exists()


@pytest.mark.parametrize("budget", ("spool", "batch"))
async def test_resource_exhaustion_never_registers_partial_source(tmp_path, monkeypatch, budget):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        native.install(monkeypatch)
        work = PlanWork(
            tmp_path / "homes", SourceFactLimits(8, 8, 8) if budget == "spool" else _LIMITS, lambda path: None
        )
        limits = allocation.SourceAllocationLimits(_LIMITS, 2, 1 if budget == "batch" else 4096)
        with pytest.raises((SourceFactBudgetExceeded, FamilyBlockResourceLimit)):
            await _register(tmp_path, case, limits=limits, work=work)
        assert native.observations == [] and not (tmp_path / "homes").exists()


def test_native_append_keeps_original_gap_and_privilege_contract():
    sql = homes._APPEND
    assert "CAST(:kinds AS smallint[])" in sql and "CAST(:ids AS bigint[])" in sql
    assert sql.count("WHEN revision_id=9223372036854775807 THEN NULL ELSE revision_id+1 END") == 2
    assert "range_agg(int8range" in sql and "h.revision_ids&&w.revision_ids" in sql
    assert "WHERE family_id<>:family_id" in sql and "SELECT r.* FROM requested r WHERE NOT EXISTS" in sql
    assert "custom_import_root_revision" in sql and "custom_import_child_revision" in sql
    assert "UPDATE " not in sql and "DELETE " not in sql and "ON CONFLICT" not in sql
    assert "a.grantee<>c.relowner" in homes._HOME_OWNER and "x.attacl" in homes._HOME_OWNER
    assert "'INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN'" in homes._HOME_OWNER
