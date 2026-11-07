# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real closed artifacts and parsers; explicitly fake only native SQL/pins/IDs.

These component tests do not establish PostgreSQL ownership or constraint proof.
"""

import asyncio
import hashlib
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import msgpack
import pytest

from db.models import custom_import as models
from process.custom_import import family_shard_admission as audit
from process.custom_import import family_shard_base_values as originals
from process.custom_import import family_shard_retained_allocate as allocation
from process.custom_import import family_shard_retained_homes as homes
from process.custom_import.family import RootFamily
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_records import record_parts
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_common import SourceCommonContext
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_output import _pack_row
from process.custom_import.family_shard_plan import (
    BaseFamilyHeader,
    PlanWork,
    _plan_cells,
    _Scratch,
    cleanup_family_plan,
)
from process.custom_import.runner_codec import digest_text, new_family_hash, root_key_document
from tests import test_custom_import_family_shard_admission as admission_fixture
from tests import test_custom_import_family_shard_base_values as base_fixture
from tests import test_custom_import_family_shard_common as common_fixture
from tests.test_custom_import_family_shard_homes import _NativeBoundary as _SourceNative
from tests.test_custom_import_family_shard_plan import _build, _definition, _Headers

_FACTS = SourceFactLimits(64 * 1024**2, 1024**2, 1024**2)
_LIMITS = SourceAllocationLimits(_FACTS, 2, 4096)


def _retained_view(context, entries):
    family_entry = next(entry[1] for entry in entries if entry[0] == "family")
    plan = next(plan for plan in allocation.read_family_plan(context.final_plan) if plan.selection_kind == "retained")
    retained_records = [entry for entry in entries if entry[0] == "record"]
    root_entry = next(entry for entry in retained_records if entry[2] == 0)
    root = allocation._record(context.common.request.definition, None, root_entry[3], retained=True)
    view = SimpleNamespace(
        root_id=plan.root_record_id,
        source=SimpleNamespace(
            plan=SimpleNamespace(base_family_revision_id=plan.base_header.family_revision_id),
            record=SimpleNamespace(logical_key_sha256=plan.typed_key[1], canonical_logical_key=plan.typed_key[0]),
            root=SimpleNamespace(
                payload_sha256=digest_text(
                    "root-payload",
                    record_parts(context.common.request.definition, None, root, retained=True)[1][2],
                )
            ),
            entity_binding_id=plan.base_header.entity_binding_id,
            family_sha256=plan.base_header.family_sha256,
            child_count=plan.base_header.child_count,
        ),
        original_cells=(
            (None, None, _plan_cells(plan)),
            (None, root_entry[3], None, root_entry[4]),
            family_entry[2],
        ),
    )
    return view, retained_records, root


def _write_retained_children(output, context, view, retained_records):
    child_entries = [entry for entry in retained_records if entry[2] != 0]
    output.observed(("child_revision",) * len(child_entries) + ("pack",), (57, 74, 9002), True)
    hashes = []
    for ordinal, entry in enumerate(child_entries):
        child = allocation._record(context.common.request.definition, "details", entry[3], retained=True)
        assigned = replace(child, revision_id=(57, 74)[ordinal], pack_id=9002, source_ordinal=ordinal)
        output.record(view, entry[2], child, assigned, allocation._unpack_row(entry[4]))
        hashes.append(
            digest_text(
                "child-payload",
                record_parts(context.common.request.definition, "details", child, retained=True)[1][2],
            )
        )
    output.pack(9002, 2, "details", tuple(hashes))


def _destination(tmp_path, context, registered):
    work = _Scratch(PlanWork(tmp_path / "destination", _FACTS, registered.append))
    prefix = allocation._source_prefix(context, work)
    entries = tuple(originals.read_retained_input(context.retained_input))
    with allocation._Output(context, work, prefix) as output:
        if context.final_plan.counts_by_kind["retained"]:
            view, retained_records, root = _retained_view(context, entries)
            # Explicit fake native sequence observations, including rejected IDs.
            # Root/child IDs may overlap; each observed kind/ID remains distinct.
            output.observed(("family_revision", "root_revision", "pack"), (6999, 56, 8999), False)
            output.observed(("family_revision", "root_revision", "pack"), (7001, 57, 9001), True)
            output.root(
                view,
                {"original_root": root, "root_revision_id": 57, "family_id": 7001, "pack_id": 9001},
                SimpleNamespace(root_stream_slot=1),
            )
            _write_retained_children(output, context, view, retained_records)
    path = work.write(entry for entry in entries if entry[0] in {"pack", "origin"})
    owner_work = allocation.RetainedAllocationWork(tmp_path / "unused", _LIMITS, registered.append)
    return allocation._write_complete(context, allocation._scope(context, owner_work), work, {"original": path}, output)


@asynccontextmanager
async def _case(tmp_path, monkeypatch, *, empty=False):
    retained_rows_by_key = {"root_source": [[]], "detail_source": [[]]}
    registered_paths = []
    async with admission_fixture._inputs(
        tmp_path, monkeypatch, retained_rows_by_key, definition=_definition("upsert"), complete_scope=False
    ) as (original_source, allocated, workers):
        admission = admission_fixture._build_audit(
            tmp_path, original_source, allocated, workers, _FACTS, complete_scope=False
        )
        request = replace(original_source[1], expected_base_generation_id=77, expected_pointer_version=3)
        root, children = base_fixture._records()
        key = root_key_document(request.definition, root.values_by_field)
        family_hash = new_family_hash(
            request.definition,
            RootFamily(
                ("1234567893",), root.values_by_field, {"details": tuple(child.values_by_field for child in children)}
            ),
        )
        header = BaseFamilyHeader(
            (key, digest_text("root-key", key)), 500, 1001, root.revision_id, 3001, family_hash, len(children), 77, 31
        )
        plan = await _build(
            tmp_path, request, admission, headers=_Headers([] if empty else [header]), scope=(77, 31), limits=_FACTS
        )
        base = base_fixture._pinned_context(request, monkeypatch, False)
        native_rows = base_fixture._legacy_rows(base, header, root, children)
        native_rows["pack"] = [base_fixture._pack_row(base, 201), base_fixture._pack_row(base, 202, child=True)]
        base_fixture._transport(monkeypatch, native_rows, [])
        retained = common = destination = None
        try:
            retained = await base_fixture._run(tmp_path, base, plan)
            context = SourceCommonContext(request, FamilyBlockLayout(request.definition, {"details": 7}), allocated)
            common_fixture._lookup_fixture(monkeypatch, context)
            common = await common_fixture._build_common(tmp_path, context, selected=())
            context = allocation.RetainedAllocationContext(context, 31, admission, plan, retained, base, common)
            destination = _destination(tmp_path, context, registered_paths)
            case = SimpleNamespace(
                context=context, common=context.common, destination=destination, registered=registered_paths
            )
            yield case
        finally:
            if destination is not None:
                allocation.cleanup_retained_destination(case.destination)
            if common is not None:
                common.close()
            if retained is not None:
                originals.cleanup_retained_input(retained)
            cleanup_family_plan(plan)
            audit.cleanup_admission_output(admission)


class _NativeBoundary(_SourceNative):
    """Fake native transaction/catalog/SQL results, never a PostgreSQL receipt."""

    def __init__(self, case):
        super().__init__(case)
        for name in ("graph_frozen_at", "output_frozen_at", "generation_id"):
            setattr(self.build, name, None)
        self.build.candidate_error_count = 0

    async def retained_candidate(self, session, request, family, layout, phases):
        assert session is self and request is self.case.common.request
        assert family == self.case.common.source.snapshot_family_id
        assert layout is self.case.common.layout and phases == {"loading"}
        self.observations.append("candidate")
        if self.phase != "loading":
            raise FamilyBlockUnavailable("synthetic native candidate phase changed")
        return None, None

    async def retained_execute(self, session, statement, parameters=None):
        assert session is self
        model = statement.column_descriptions[0]["entity"]
        if model is models.CustomImportBuildAttempt:
            return SimpleNamespace(scalar_one_or_none=lambda: self.build)
        assert model is models.CustomImportBuildStream
        rows = [
            SimpleNamespace(stream_slot=slot, next_pack_ordinal=ordinal, replay_verified_at=object())
            for slot, ordinal in self.case.destination.source_pack_prefix
        ]
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: rows))

    def install(self, monkeypatch):
        super().install(monkeypatch)
        monkeypatch.setattr(homes, "_page_session", self.page)
        monkeypatch.setattr(allocation, "_candidate", self.retained_candidate)
        monkeypatch.setattr(allocation, "_execute", self.retained_execute)


async def _register(tmp_path, case, *, limits=_LIMITS):
    return await homes.register_retained_homes(
        None,
        case.context,
        case.destination,
        work=allocation.RetainedAllocationWork(tmp_path / "homes", limits, case.registered.append),
    )


@pytest.mark.parametrize("memory_bytes", (2048, 1024**2))
async def test_exact_observed_ids_exclude_failed_attempts_and_retry_only_missing(tmp_path, monkeypatch, memory_bytes):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        native.install(monkeypatch)
        limits = replace(_LIMITS, spool=replace(_FACTS, memory_bytes=memory_bytes))
        assert await _register(tmp_path, case, limits=limits) == homes.RetainedHomeCounts(1, 2, 1, 2)
        assert set(native.homes_by_id) == {(1, 57), (2, 57), (2, 74)}
        assert (1, 56) not in native.homes_by_id and (2, 58) not in native.homes_by_id
        assert native.observations.count("candidate") == native.observations.count("commit") == 2
        assert await _register(tmp_path, case, limits=limits) == homes.RetainedHomeCounts(1, 2, 0, 0)
        assert not (tmp_path / "homes").exists()


async def test_empty_still_checks_live_graph_pin_and_native_owner(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, empty=True) as case:
        native = _NativeBoundary(case)
        native.install(monkeypatch)
        assert await _register(tmp_path, case) == homes.RetainedHomeCounts(0, 0, 0, 0)
        assert ("append", ()) in native.observations and native.observations.count("candidate") == 1
        native.build.phase = "output"
        with pytest.raises(FamilyBlockUnavailable, match="unfrozen GRAPH"):
            await _register(tmp_path, case)
        assert native.observations[-1] == "rollback" and not (tmp_path / "homes").exists()


def _rewrite(destination, mutation):
    entries = list(allocation.read_retained_destination(destination))
    # Replay expands destination wires; the stored form contains only IDs.
    for index, entry in enumerate(entries):
        if entry[0] == "record":
            entries[index] = (*entry[:5], tuple(msgpack.unpackb(entry[5], raw=False)[:3]), entry[6])
    mutation(entries)
    path = Path(destination.root) / allocation._FILE
    header = (
        "header",
        "custom-import/retained-destination/v1",
        destination.scope,
        destination.source_pack_prefix,
        destination.definition_sha256,
        destination.collection_slots,
    )
    footer = ("complete", destination.counts, destination.destination_pack_tail)
    path.chmod(0o600)
    with path.open("wb") as output:
        for entry in (header, *entries, footer):
            payload = _pack_row(entry)
            output.write(allocation._LENGTH.pack(len(payload)))
            output.write(payload)
    path.chmod(0o400)
    return replace(destination, sha256=hashlib.sha256(path.read_bytes()).digest(), file_bytes=path.stat().st_size)


@pytest.mark.parametrize(
    "damage", ("accepted_missing", "assigned_missing", "duplicate", "failed_assigned", "scope", "late_eof")
)
async def test_destination_damage_refuses_before_any_native_write(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        native.install(monkeypatch)
        if damage == "scope":
            case.destination = replace(case.destination, scope=())
        elif damage == "late_eof":
            path = Path(case.destination.root) / allocation._FILE
            path.chmod(0o600)
            path.write_bytes(path.read_bytes()[:-1])
            path.chmod(0o400)
        else:

            def change(entries):
                index = next(
                    index for index, row in enumerate(entries) if row[0] == "allocation" and row[3] == "child_revision"
                )
                row = entries[index]
                if damage == "accepted_missing":
                    entries[index] = (*row[:-1], False)
                elif damage == "assigned_missing":
                    entries[index] = (*row[:4], row[4] + 1, row[5])
                elif damage == "duplicate":
                    other = next(
                        other
                        for other in entries[index + 1 :]
                        if other[0] == "allocation" and other[3] == "child_revision"
                    )
                    entries[index] = (*row[:4], other[4], row[5])
                else:
                    failed = next(
                        index
                        for index, row in enumerate(entries)
                        if row[0] == "allocation" and row[3] == "root_revision" and not row[5]
                    )
                    entries[failed] = (*entries[failed][:4], 57, False)

            case.destination = _rewrite(case.destination, change)
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _register(tmp_path, case)
        assert native.observations == [] and not (tmp_path / "homes").exists()
        # Mutated artifacts still have exact task custody but cleanup's census
        # check uses size; retain the current observed size for the fixture exit.
        case.destination = replace(
            case.destination, file_bytes=(Path(case.destination.root) / allocation._FILE).stat().st_size
        )


@pytest.mark.parametrize("damage", ("source", "allocation", "common", "admission", "plan", "retained"))
async def test_original_artifact_tamper_refuses_before_any_native_write(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        native.install(monkeypatch)
        artifacts_by_key = {
            "source": case.context.common.source.source,
            "allocation": case.context.common.source,
            "admission": case.context.admission,
            "plan": case.context.final_plan,
            "retained": case.context.retained_input,
        }
        path = (
            case.context.common_output._replay.path
            if damage == "common"
            else next(Path(artifacts_by_key[damage].root).iterdir())
        )
        path.chmod(0o600)
        if path.stat().st_size:
            with path.open("r+b") as output:
                output.seek(-1, 2)
                observed_value = output.read(1)
                output.seek(-1, 2)
                output.write(bytes([observed_value[0] ^ 1]))
        else:
            with path.open("ab") as output:
                output.write(b"x")
        path.chmod(0o400)
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _register(tmp_path, case)
        assert native.observations == [] and not (tmp_path / "homes").exists()


async def test_authority_loss_rolls_back_current_batch_for_exact_retry(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        native.fail_commit, native.fail_commit_after = RuntimeError("synthetic lease loss"), 1
        native.install(monkeypatch)
        with pytest.raises(RuntimeError, match="lease loss"):
            await _register(tmp_path, case)
        assert set(native.homes_by_id) == {(1, 57), (2, 57)}
        assert not (tmp_path / "homes").exists()
        native.fail_commit = None
        assert await _register(tmp_path, case) == homes.RetainedHomeCounts(1, 2, 0, 1)


async def test_native_cancellation_rolls_back_before_exact_scratch_cleanup(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        native = _NativeBoundary(case)
        native.entered, native.release = asyncio.Event(), asyncio.Event()
        native.install(monkeypatch)
        task = asyncio.create_task(_register(tmp_path, case))
        try:
            await asyncio.wait_for(native.entered.wait(), 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            native.release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        assert native.homes_by_id == {} and native.observations[-1] == "rollback"
        assert not (tmp_path / "homes").exists()
