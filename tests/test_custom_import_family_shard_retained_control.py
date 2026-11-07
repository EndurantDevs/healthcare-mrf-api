# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real retained artifacts/proofs; synthetic native counter transaction boundary."""

from contextlib import asynccontextmanager, closing
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects.postgresql import dialect

from db.models.custom_import import CustomImportBuildAttempt
from process.custom_import import family_shard_control as source_control
from process.custom_import import family_shard_retained_control as control
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_store import CustomImportFamilyBlockLayout, ShardOutputProof
from process.custom_import.family_shard_source_plan import read_source_parts
from tests import test_custom_import_family_shard_retained_storage_validation as fixture

_STAMP = datetime(2030, 1, 2, tzinfo=UTC)


def _rows(values):
    return SimpleNamespace(
        all=lambda: values,
        one=lambda: values[0],
        scalar_one=lambda: values[0],
        mappings=lambda: SimpleNamespace(all=lambda: values, one=lambda: values[0]),
    )


def _common_receipt(case):
    common, destination = case.context.common, case.destination
    proof = ShardOutputProof(common.layout, 16, bytes.fromhex(common.source.source.partition_sha256), 0)
    with closing(destination.batches(maximum_rows=2, maximum_bytes=4096)) as batches:
        for batch in batches:
            for role, rows in batch.items():
                proof.add(role, rows)
    return proof.receipt()


def _pack_rows(case):
    columns = case.context.common.columns_by_role["pack"]
    rows = []
    for output in (case.context.common_output, case.destination):
        with closing(output.batches(maximum_rows=2, maximum_bytes=4096)) as batches:
            for batch in batches:
                rows.extend(dict(zip(columns, cells, strict=True)) for cells in batch.get("pack", ()))
    return rows


def _build(case):
    request, source = case.context.common.request, case.context.common.source.source
    values_by_name = {column.name: None for column in CustomImportBuildAttempt.__table__.c}
    values_by_name.update(
        build_id=case.context.build_id,
        phase="graph",
        source_frozen_at=_STAMP,
        candidate_error_count=0,
        source_occurrence_count=source.counts[0],
        capture_bundle_id=dict(source.scope[0])["capture_bundle_id"],
        plan_stage="base",
        selected_family_count=0,
        completed_family_count=0,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        producing_fence=request.fence,
        producing_token_sha256=control.producer_token_sha256(request),
    )
    return SimpleNamespace(__table__=CustomImportBuildAttempt.__table__, **values_by_name)


def _capture_rows(case):
    counts_by_slot = {slot: [0, count] for slot, count in case.context.common.source.source.source_counts}
    with closing(read_source_parts(case.context.common.source.source)) as parts:
        for slot, ordinal, _count in parts:
            counts_by_slot[slot][0] = ordinal
    return [(slot, *counts) for slot, counts in counts_by_slot.items()]


class _Native:
    def __init__(self, case, receipt):
        self.case, self.build = case, _build(case)
        self.events, self.updates, self.failure, self.uncertain = [], 0, None, False
        self.descriptor = SimpleNamespace(**{column.name: None for column in CustomImportFamilyBlockLayout.__table__.c})
        self.descriptor.phase = "loading"
        common = case.context.common
        self.binding = SimpleNamespace(
            layout=common.layout, tables_by_role=common.layout.tables(common.source.snapshot_family_id)
        )
        self.receipts = [
            {**expected_receipt, "completed_at": _STAMP} for expected_receipt in (receipt, case.output.receipt)
        ]
        self.packs, self.captures = _pack_rows(case), _capture_rows(case)
        prefix_by_slot = dict(case.destination.source_pack_prefix)
        self.streams = [
            dict(
                build_id=case.context.build_id,
                stream_slot=slot,
                next_part_ordinal=parts + 1,
                next_part_row_ordinal=0,
                next_source_ordinal=source_record_count,
                next_pack_ordinal=prefix_by_slot[slot],
                replay_verified_at=_STAMP,
            )
            for slot, parts, source_record_count in self.captures
        ]
        capture_by_name = dict(common.source.source.scope[0])
        self.bundle = SimpleNamespace(
            **{
                name: capture_by_name[name]
                for name in (
                    "capture_bundle_id",
                    "dataset_id",
                    "definition_revision_id",
                    "schema_revision_id",
                    "source_binding_revision_id",
                )
            },
            capture_state="sealed",
            manifest_sha256=bytes.fromhex(capture_by_name["manifest_sha256"]),
            source_binding_sha256=None
            if capture_by_name["source_binding_sha256"] is None
            else bytes.fromhex(capture_by_name["source_binding_sha256"]),
        )
        self.partial_result, self.after_update = False, None

    @asynccontextmanager
    async def page(self, factory, request, build_id):
        assert (
            factory is self and request is self.case.context.common.request and build_id == self.case.context.build_id
        )
        before = deepcopy(self.streams), deepcopy(control.allocation._model_cells(self.build))
        self.events.append("authority-locked")
        try:
            yield self, self.build
            self.events.append("fresh-authority")
            if self.failure is not None:
                raise self.failure
        except BaseException:
            self.streams = before[0]
            for name, cell in before[1].items():
                setattr(self.build, name, cell)
            self.events.append("rollback")
            raise
        else:
            self.events.append("commit")
            if self.uncertain:
                self.uncertain = False
                raise ConnectionError("commit succeeded before reply was lost")

    async def candidate(self, session, request, family_id, layout, phases):
        assert session is self and phases == {"loading"} and self.events[0] == "authority-locked"
        assert family_id == self.case.context.common.source.snapshot_family_id and layout is self.binding.layout
        self.events.append("candidate-NOWAIT")
        if self.descriptor.phase != "loading":
            raise FamilyBlockUnavailable("native candidate phase differs")
        return self.descriptor, self.binding

    async def authority(self, session, name, arguments):
        assert session is self and name == "lock_custom_import_snapshot_attempt"
        assert arguments == control._authority(self.case.context.common.request)
        self.events.append("native-attempt")

    def _pack_census(self):
        request = self.case.context.common.request
        expected_by_name = dict(
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            execution_id=request.execution_id,
            capture_bundle_id=dict(self.case.context.common.source.source.scope[0])["capture_bundle_id"],
            producing_fence=request.fence,
            producing_token_sha256=control.producer_token_sha256(request),
        )
        rows = []
        for slot in sorted({row["stream_slot"] for row in self.packs}):
            packs = [row for row in self.packs if row["stream_slot"] == slot]
            ordinals = [row["pack_ordinal"] for row in packs]
            valid = sum(all(row[name] == cell for name, cell in expected_by_name.items()) for row in packs)
            rows.append((slot, len(packs), len(set(ordinals)), min(ordinals), max(ordinals), valid))
        return rows

    async def execute_native(self, session, statement, parameters=None):
        assert session is self
        sql = str(statement.compile(dialect=dialect()))
        if getattr(statement, "is_update", False):
            assert "SET next_pack_ordinal=" in sql and "FROM (VALUES" in sql
            assert "next_part_ordinal=" not in sql and "next_source_ordinal=" not in sql
            self.events.append("counter-update")
            self.updates += 1
            changed = statement._values["next_pack_ordinal"].table._data[0]
            returned_rows = []
            for slot, before, after in changed:
                stream = next(candidate for candidate in self.streams if candidate["stream_slot"] == slot)
                if stream["next_pack_ordinal"] == before:
                    stream["next_pack_ordinal"] = after
                    returned_rows.append((slot, after))
            if self.after_update is not None:
                self.after_update()
            return _rows(returned_rows[:-1] if self.partial_result else returned_rows)
        if "custom_import_build_stream" in sql:
            return _rows(deepcopy(self.streams))
        if "custom_import_block_shard" in sql:
            return _rows(deepcopy(self.receipts))
        if "custom_import_capture_bundle" in sql:
            return _rows([self.bundle])
        if "custom_import_capture" in sql:
            return _rows(self.captures)
        if "custom_import_build_attempt" in sql:
            return _rows([control.allocation._model_cells(self.build)])
        assert "custom_import_pack" in sql and "count(distinct(" in sql.lower()
        assert "FILTER (WHERE" in sql and "producing_token_sha256" in sql
        return _rows(self._pack_census())

    def install(self, monkeypatch):
        monkeypatch.setattr(control, "_page_session", self.page)
        monkeypatch.setattr(control, "_candidate", self.candidate)
        monkeypatch.setattr(control, "_call", self.authority)
        monkeypatch.setattr(control, "_execute", self.execute_native)
        monkeypatch.setattr(source_control, "_execute", self.execute_native)


async def _case(tmp_path, monkeypatch, case):
    stored = await fixture._prove(tmp_path, case)
    receipt = _common_receipt(case)
    native = _Native(case, receipt)
    native.install(monkeypatch)
    return native, stored, receipt


async def _run(tmp_path, case, native, stored, receipt, **changes):
    arguments_by_name = dict(original=case.comparison, stored=stored, encoded=case.output, common_receipt=receipt)
    arguments_by_name.update(changes)
    return await control.complete_retained_pack_counters(
        native,
        case.context,
        case.destination,
        work=control.allocation.RetainedAllocationWork(
            tmp_path / "counter-control", fixture.fixture._LIMITS, lambda _path: None
        ),
        **arguments_by_name,
    )


@pytest.mark.parametrize("empty", [False, True])
async def test_exact_graph_counter_completion_and_replay_preserve_every_other_field(tmp_path, monkeypatch, empty):
    async with fixture._case(tmp_path, monkeypatch, empty=empty) as case:
        native, stored, receipt = await _case(tmp_path, monkeypatch, case)
        before_streams, before_build = deepcopy(native.streams), vars(native.build).copy()
        assert await _run(tmp_path, case, native, stored, receipt) == case.destination.destination_pack_tail
        assert native.updates == int(not empty)
        assert native.streams == [
            dict(row, next_pack_ordinal=dict(case.destination.destination_pack_tail)[row["stream_slot"]])
            for row in before_streams
        ]
        assert vars(native.build) == before_build and native.events[-1] == "commit"
        assert (
            native.events.index("authority-locked")
            < native.events.index("native-attempt")
            < native.events.index("candidate-NOWAIT")
        )
        assert await _run(tmp_path, case, native, stored, receipt) == case.destination.destination_pack_tail
        assert native.updates == int(not empty) and native.events.count("commit") == 2
        assert not (tmp_path / "counter-control").exists()


@pytest.mark.parametrize("damage", ["original", "stored", "scope", "destination", "counts", "split", "mutable_sha"])
async def test_independent_comparisons_must_match_before_native_transaction(tmp_path, monkeypatch, damage):
    async with fixture._case(tmp_path, monkeypatch) as case:
        native, stored, receipt = await _case(tmp_path, monkeypatch, case)
        original = case.comparison
        if damage == "original":
            original = None
        if damage == "stored":
            stored = None
        if damage == "scope":
            stored = replace(stored, scope=())
        if damage == "destination":
            stored = replace(stored, destination_sha256=b"x" * 32)
        if damage == "counts":
            stored = replace(stored, counts=())
        if damage == "split":
            original = replace(original, root_scalar_count=True)
        if damage == "mutable_sha":
            stored = replace(stored, sha256=memoryview(stored.sha256))
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _run(tmp_path, case, native, stored, receipt, original=original)
        assert not native.events and not (tmp_path / "counter-control").exists()


@pytest.mark.parametrize(
    "damage", ["common_hash", "same_shard", "native_missing", "native_hash", "native_source", "timestamp"]
)
async def test_both_copy_commitments_are_mandatory_and_exact(tmp_path, monkeypatch, damage):
    async with fixture._case(tmp_path, monkeypatch) as case:
        native, stored, receipt_by_name = await _case(tmp_path, monkeypatch, case)
        if damage == "common_hash":
            receipt_by_name = dict(receipt_by_name, result_sha256=b"x" * 32)
        if damage == "same_shard":
            receipt_by_name = dict(receipt_by_name, shard_id=case.output.receipt["shard_id"])
        if damage == "native_missing":
            native.receipts.pop()
        if damage == "native_hash":
            native.receipts[-1]["result_sha256"] = b"x" * 32
        if damage == "native_source":
            native.receipts[-1]["source_record_count"] = 1
        if damage == "timestamp":
            native.receipts[-1]["completed_at"] = _STAMP.replace(tzinfo=None)
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _run(tmp_path, case, native, stored, receipt_by_name)
        assert native.updates == 0


@pytest.mark.parametrize(
    "damage", ["mixed", "unknown", "missing", "cursor", "phase", "frozen", "pack_hole", "pack_scope"]
)
async def test_unknown_native_state_never_changes_counters(tmp_path, monkeypatch, damage):
    async with fixture._case(tmp_path, monkeypatch) as case:
        native, stored, receipt = await _case(tmp_path, monkeypatch, case)
        if damage == "mixed":
            native.streams[0]["next_pack_ordinal"] = dict(case.destination.destination_pack_tail)[
                native.streams[0]["stream_slot"]
            ]
        if damage == "unknown":
            native.streams[0]["next_pack_ordinal"] = 999
        if damage == "missing":
            native.streams.pop()
        if damage == "cursor":
            native.streams[0]["next_source_ordinal"] += 1
        if damage == "phase":
            native.build.phase = "output"
        if damage == "frozen":
            native.build.graph_frozen_at = _STAMP
        if damage == "pack_hole":
            native.packs[0]["pack_ordinal"] += 1
        if damage == "pack_scope":
            native.packs[0]["producing_token_sha256"] = b"x" * 32
        before = deepcopy(native.streams)
        with pytest.raises(FamilyBlockUnavailable):
            await _run(tmp_path, case, native, stored, receipt)
        assert native.updates == 0 and native.streams == before


async def test_dense_pack_census_checks_distinct_ordinals_beyond_count_and_endpoints(tmp_path, monkeypatch):
    async with fixture._case(tmp_path, monkeypatch) as case:
        native, _stored, _receipt = await _case(tmp_path, monkeypatch, case)
        first = native.packs[0]
        native.packs = [
            dict(first, pack_id=100 + index, pack_ordinal=ordinal) for index, ordinal in enumerate((0, 0, 2))
        ]
        with pytest.raises(FamilyBlockUnavailable, match="dense ordinal"):
            await control._pack_census(native, case.context, native.binding, ((first["stream_slot"], 3),))


@pytest.mark.parametrize("damage", ["destination_eof", "encoded_eof", "closed_output"])
async def test_complete_original_producer_eof_and_lifetime_are_required(tmp_path, monkeypatch, damage):
    async with fixture._case(tmp_path, monkeypatch) as case:
        native, stored, receipt = await _case(tmp_path, monkeypatch, case)
        if damage == "closed_output":
            case.output.close()
        else:
            path = control._owned_destination(case.destination) if damage == "destination_eof" else case.output.path
            payload = path.read_bytes()
            path.chmod(0o600)
            path.write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))
            path.chmod(0o400)
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _run(tmp_path, case, native, stored, receipt)
        assert not native.events and not (tmp_path / "counter-control").exists()


@pytest.mark.parametrize("damage", ["prefix", "footer_tail", "boolean", "overflow"])
async def test_destination_tail_declarations_cannot_change_the_native_transition(tmp_path, monkeypatch, damage):
    async with fixture._case(tmp_path, monkeypatch) as case:
        native, stored, receipt = await _case(tmp_path, monkeypatch, case)
        if damage == "prefix":
            prefix_rows = list(case.destination.source_pack_prefix)
            prefix_rows[0] = (prefix_rows[0][0], 1)
            case.destination = replace(case.destination, source_pack_prefix=tuple(prefix_rows))
        else:
            tail_rows = list(case.destination.destination_pack_tail)
            if damage == "footer_tail":
                tail_rows[0], tail_rows[1] = (
                    (tail_rows[0][0], tail_rows[0][1] + 1),
                    (tail_rows[1][0], tail_rows[1][1] - 1),
                )
            else:
                tail_rows[0] = (tail_rows[0][0], True if damage == "boolean" else 2**31)
            case.destination = replace(case.destination, destination_pack_tail=tuple(tail_rows))
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _run(tmp_path, case, native, stored, receipt)
        assert not native.events


@pytest.mark.parametrize("damage", ["authority", "partial_update", "source_cursor", "build", "base"])
async def test_late_failure_rolls_back_the_entire_counter_vector(tmp_path, monkeypatch, damage):
    async with fixture._case(tmp_path, monkeypatch) as case:
        native, stored, receipt = await _case(tmp_path, monkeypatch, case)
        before = deepcopy(native.streams)
        if damage == "authority":
            native.failure = RuntimeError("late current authority expired")
        if damage == "partial_update":
            native.partial_result = True
        if damage == "source_cursor":
            native.after_update = lambda: native.streams[0].update(next_part_ordinal=999)
        if damage == "build":
            native.after_update = lambda: setattr(native.build, "candidate_error_count", 1)
        if damage == "base":
            original, calls = control.allocation._check_base, []

            async def changed(context):
                await original(context)
                calls.append(True)
                if len(calls) == 3:
                    raise FamilyBlockUnavailable("late original base changed")

            monkeypatch.setattr(control.allocation, "_check_base", changed)
        with pytest.raises((RuntimeError, ValueError, FamilyBlockUnavailable)):
            await _run(tmp_path, case, native, stored, receipt)
        assert native.updates == 1 and native.streams == before and native.events[-1] == "rollback"
        assert not (tmp_path / "counter-control").exists()


async def test_uncertain_commit_replays_the_same_artifacts_without_reallocation_or_update(tmp_path, monkeypatch):
    async with fixture._case(tmp_path, monkeypatch) as case:
        native, stored, receipt = await _case(tmp_path, monkeypatch, case)
        native.uncertain = True
        with pytest.raises(ConnectionError, match="reply was lost"):
            await _run(tmp_path, case, native, stored, receipt)
        assert native.updates == 1 and native.events[-1] == "commit"
        assert await _run(tmp_path, case, native, stored, receipt) == case.destination.destination_pack_tail
        assert native.updates == 1 and native.events.count("commit") == 2
