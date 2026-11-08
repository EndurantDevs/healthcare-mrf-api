# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual source workers, lazy group spools and full codecs; native IDs are fake."""

import hashlib
import json
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_output_groups as groups
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_codec import FamilyBlockLimits, FamilyBlockResourceLimit
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_storage_codec import verify_stored_family
from process.custom_import.family_block_store import ShardOutputProof
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork
from tests.test_custom_import_family_shard_admission import _child, _inputs, _root
from tests.test_custom_import_family_shard_worker import _projected_definition

_LIMITS = SourceFactLimits(64 * 1024**2, 4 * 1024**2, 8 * 1024**2)


def _work(tmp_path, *, check=None, limits=_LIMITS):
    return PlanWork(tmp_path / "groups", limits, lambda _path: None, check)


def _layout(case):
    return FamilyBlockLayout(case[1].definition, case[2].child_collection_slots)


def _frames(worker):
    return list(worker._output._work.rows(worker._output.path))


def _rewrite_output(worker, layout, frames):
    """Construct malformed completed producer output with valid file/receipt hashes."""
    output = worker._output
    receipt = worker.receipt
    proof = ShardOutputProof(layout, receipt["shard_id"], receipt["partition_sha256"], receipt["source_record_count"])
    output.path.chmod(0o600)
    with output.path.open("wb") as target:
        for role, packed in frames:
            cells = _unpack_row(packed)
            proof.add(role, (cells,))
            output._work.frame(target, (role, packed))
    output.path.chmod(0o400)
    output._identity = output.path.stat()
    output.sha256 = hashlib.sha256(output.path.read_bytes()).digest()
    output._receipt = proof.receipt()


def _selected(outputs):
    return [worker for worker in outputs if worker.receipt["role_counts"]["family"]]


def _rewrite_audit(worker, frames):
    worker.path.chmod(0o600)
    with worker.path.open("wb") as target:
        for frame in frames:
            worker._work.frame(target, frame)
    worker.path.chmod(0o400)
    worker._identity = worker.path.stat()
    worker.sha256 = hashlib.sha256(worker.path.read_bytes()).digest()


async def test_groups_match_all_original_cells_in_global_order(monkeypatch, tmp_path):
    rows_by_stream = {
        "root_source": [[_root(), _root("1234567893")]],
        "detail_source": [[_child(key="a"), _child("1234567893", key="b")]],
    }
    async with _inputs(tmp_path, monkeypatch, rows_by_stream) as (case, allocated, outputs):
        expected_by_family = {}
        for worker in outputs:
            current = None
            for role, packed in _frames(worker):
                cells = _unpack_row(packed)
                if role == "family":
                    current = cells[0]
                    expected_by_family[current] = {"family": cells, **{name: [] for name in groups._ROLES}}
                else:
                    expected_by_family[current][role].append(cells)
        seen_keys, old_group = [], None
        with groups.source_family_output_groups(
            allocated, _layout(case), tuple(reversed(outputs)), work=_work(tmp_path), complete_scope=True
        ) as families:
            for family in families:
                if old_group is not None:
                    assert not old_group.is_active and all(
                        replay.file.closed for replay in old_group.replays_by_role.values()
                    )
                expected = expected_by_family[family.family_row["family_revision_id"]]
                assert tuple(family.family_row.values()) == expected["family"]
                for role in groups._ROLES:
                    assert list(family.rows(role)) == expected[role]
                with verify_stored_family(
                    case[1].definition,
                    _layout(case).collection_slots,
                    family.fragments(),
                    family.memberships(),
                    _layout(case).manifest(family.family_row),
                    limits=FamilyBlockLimits(16 * 1024**2, 1024**2, 4096, 65536),
                ) as verified:
                    assert len(list(verified.records())) == 1
                seen_keys.append(family.header["typed_key"])
                old_group = family
        assert len(seen_keys) == len(expected_by_family) == 2
        assert seen_keys == sorted(seen_keys, key=lambda key: (key[1], key[0].encode("utf-8")))
        assert not (tmp_path / "groups").exists()
        assert all(worker.path.is_file() and worker._output.path.is_file() for worker in outputs)
        with pytest.raises(ValueError, match="outlived"):
            old_group.query_rows()


async def test_complete_empty_source_exhausts_every_worker(monkeypatch, tmp_path):
    async with _inputs(tmp_path, monkeypatch, {"root_source": [[]], "detail_source": [[]]}) as (
        case,
        allocated,
        outputs,
    ):
        with groups.source_family_output_groups(
            allocated, _layout(case), outputs, work=_work(tmp_path), complete_scope=True
        ) as families:
            assert list(families) == []
        assert not (tmp_path / "groups").exists()


def _direct_expected(worker):
    expected_groups = []
    for role, packed in _frames(worker):
        if role == "family":
            expected_groups.append({"family": _unpack_row(packed), **{name: [] for name in groups._ROLES}})
        else:
            expected_groups[-1][role].append(_unpack_row(packed))
    return expected_groups


def _assert_direct_group(family, expected):
    assert dict(family.header) == {}
    assert tuple(family.family_row.values()) == expected["family"]
    for role in groups._ROLES:
        assert list(family.rows(role)) == expected[role]


async def test_direct_reader_uses_actual_output_without_source_audit(monkeypatch, tmp_path):
    async with _inputs(tmp_path, monkeypatch) as (case, allocated, outputs):
        for worker in outputs:
            expected_groups = _direct_expected(worker)
            with groups.family_output_groups(
                worker._output, _layout(case), allocated.snapshot_family_id, work=_work(tmp_path)
            ) as families:
                seen = 0
                for family in families:
                    _assert_direct_group(family, expected_groups[seen])
                    seen += 1
                assert seen == len(expected_groups)
            assert not (tmp_path / "groups").exists() and worker._output.path.is_file()


async def test_direct_reader_requires_complete_output_eof(monkeypatch, tmp_path):
    async with _inputs(tmp_path, monkeypatch) as (case, allocated, outputs):
        worker = _selected(outputs)[0]
        with pytest.raises(ValueError, match="complete physical output EOF"):
            with groups.family_output_groups(
                worker._output, _layout(case), allocated.snapshot_family_id, work=_work(tmp_path)
            ) as families:
                family = next(families)
                assert family.is_active
        assert not family.is_active and not (tmp_path / "groups").exists()
        assert worker._output.path.is_file()


@pytest.mark.parametrize("damage", ["missing", "extra", "role", "membership"])
async def test_direct_reader_rejects_resealed_physical_damage(monkeypatch, tmp_path, damage):
    async with _inputs(tmp_path, monkeypatch) as (case, allocated, outputs):
        worker = _selected(outputs)[0]
        frames = _frames(worker)
        if damage == "missing":
            frames = frames[:-1]
        elif damage == "extra":
            frames.append(frames[-1])
        elif damage == "role":
            frames[1] = "revision_run", frames[1][1]
        else:
            index = next(index for index, entry in enumerate(frames) if entry[0] == "revision_run")
            cells = list(_unpack_row(frames[index][1]))
            cells[0] += 1
            frames[index] = "revision_run", _pack_row(tuple(cells))
        _rewrite_output(worker, _layout(case), frames)
        with pytest.raises(ValueError):
            with groups.family_output_groups(
                worker._output, _layout(case), allocated.snapshot_family_id, work=_work(tmp_path)
            ) as families:
                list(families)
        assert not (tmp_path / "groups").exists() and worker._output.path.is_file()


async def test_packed_membership_entries_cover_multiple_child_records(monkeypatch, tmp_path):
    rows_by_stream = {"root_source": [[_root()]], "detail_source": [[_child(key="a"), _child(key="b")]]}
    async with _inputs(tmp_path, monkeypatch, rows_by_stream) as (case, allocated, outputs):
        layout = _layout(case)
        with groups.source_family_output_groups(
            allocated, layout, outputs, work=_work(tmp_path), complete_scope=True
        ) as families:
            for family in families:
                members = list(family.memberships())
                assert family.family_row["child_count"] == 2
                assert len(members) == family.family_row["membership_count"] == 2
                assert sum(member.record_count for member in members) == 3
                assert any(member.record_count == 2 for member in members)
                assert len(list(family.query_rows())) == 2
                with verify_stored_family(
                    case[1].definition,
                    layout.collection_slots,
                    family.fragments(),
                    iter(members),
                    layout.manifest(family.family_row),
                    limits=FamilyBlockLimits(16 * 1024**2, 1024**2, 4096, 65536),
                ) as verified:
                    assert len(list(verified.records())) == 1
                    assert len(list(verified.records("details"))) == 2


async def test_absent_family_header_consumes_no_output_group(monkeypatch, tmp_path):
    rows_by_stream = {
        "root_source": [[_root(), _root("1234567893")]],
        "detail_source": [[_child(), _child(), _child("1234567893", key="b")]],
    }
    async with _inputs(tmp_path, monkeypatch, rows_by_stream) as (case, allocated, outputs):
        assert sum(worker.counts["absent"] for worker in outputs) == 1
        with groups.source_family_output_groups(
            allocated, _layout(case), outputs, work=_work(tmp_path), complete_scope=True
        ) as families:
            assert len(list(families)) == 1
        assert not (tmp_path / "groups").exists()


async def test_no_query_collection_keeps_all_memberships(monkeypatch, tmp_path):
    document = json.loads(_projected_definition().canonical)
    document["query"].pop("child")
    document["selection_profiles"] = [{"id": "default", "selection": [], "context_dimensions": []}]
    definition = CustomImportDefinition.from_mapping(document)
    async with _inputs(tmp_path, monkeypatch, definition=definition) as (case, allocated, outputs):
        with groups.source_family_output_groups(
            allocated, _layout(case), outputs, work=_work(tmp_path), complete_scope=True
        ) as families:
            for family in families:
                assert list(family.query_rows()) == []
                assert (
                    sum(member.record_count for member in family.memberships()) == family.family_row["child_count"] + 1
                )


@pytest.mark.parametrize(
    "damage", ["missing_eof", "trailing", "absent_with_output", "missing_selected_ids", "root_key_hash"]
)
async def test_audit_header_pairing_and_eof_cannot_be_skipped(monkeypatch, tmp_path, damage):
    async with _inputs(tmp_path, monkeypatch) as (case, allocated, outputs):
        worker = _selected(outputs)[0]
        frames = list(worker.audit_rows())
        header = next(frame[1] for frame in frames if frame[0] == "family")
        match damage:
            case "missing_eof":
                frames.pop()
            case "trailing":
                frames.append(("unexpected",))
            case "absent_with_output":
                header["selection"] = "absent"
                worker._counts["source"] -= 1
                worker._counts["absent"] += 1
                frames[-1] = "eof", worker.counts
            case "missing_selected_ids":
                header["family_ids"] = None
            case "root_key_hash":
                header["typed_key"] = header["typed_key"][0], b"x" * 32
        _rewrite_audit(worker, frames)
        with pytest.raises(ValueError):
            with groups.source_family_output_groups(
                allocated, _layout(case), outputs, work=_work(tmp_path), complete_scope=True
            ) as families:
                list(families)
        assert not (tmp_path / "groups").exists()


async def test_early_exit_closes_borrowed_group_and_preserves_workers(monkeypatch, tmp_path):
    async with _inputs(tmp_path, monkeypatch) as (case, allocated, outputs):
        with pytest.raises(ValueError, match="complete audit and output EOF"):
            with groups.source_family_output_groups(
                allocated, _layout(case), outputs, work=_work(tmp_path), complete_scope=True
            ) as families:
                family = next(families)
                reader = family.fragments()
                next(reader)
        assert not family.is_active and not (tmp_path / "groups").exists()
        reader.close()
        assert all(worker.path.is_file() for worker in outputs)


@pytest.mark.parametrize(
    "damage",
    [
        "missing_family",
        "extra_family",
        "foreign_fragment",
        "missing_fragment",
        "fragment_order",
        "membership",
        "query",
        "columns",
    ],
)
async def test_group_structure_refuses_validly_hashed_bad_output(monkeypatch, tmp_path, damage):
    async with _inputs(tmp_path, monkeypatch) as (case, allocated, outputs):
        worker = _selected(outputs)[0]
        frames = _frames(worker)
        if damage == "missing_family":
            frames = []
        elif damage == "extra_family":
            frames += list(frames)
        else:
            role = "revision_run" if damage == "membership" else "query_record" if damage == "query" else "fragment"
            index = next(index for index, framed_row in enumerate(frames) if framed_row[0] == role)
            cells = list(_unpack_row(frames[index][1]))
            match damage:
                case "foreign_fragment":
                    cells[0] += 1
                case "missing_fragment":
                    frames.pop(index)
                case "fragment_order":
                    cells[1] += 1
                case "membership":
                    cells[4] += 1
                case "query":
                    cells[2] += 1
                case "columns":
                    cells.pop()
            if damage != "missing_fragment":
                frames[index] = role, _pack_row(tuple(cells))
        _rewrite_output(worker, _layout(case), frames)
        with pytest.raises((ValueError, FamilyBlockResourceLimit)):
            with groups.source_family_output_groups(
                allocated, _layout(case), outputs, work=_work(tmp_path), complete_scope=True
            ) as families:
                list(families)
        assert not (tmp_path / "groups").exists()


@pytest.mark.parametrize("damage", ["scope", "duplicate", "complete_scope", "collections", "receipt", "file_sha"])
async def test_original_scope_and_custody_are_required(monkeypatch, tmp_path, damage):
    async with _inputs(tmp_path, monkeypatch) as (case, allocated, outputs):
        layout, has_complete_scope = _layout(case), True
        worker = _selected(outputs)[0]
        match damage:
            case "scope":
                allocated = replace(allocated, snapshot_family_id=999)
            case "duplicate":
                outputs = [outputs[0]] * len(outputs)
            case "complete_scope":
                has_complete_scope = False
            case "collections":
                layout = FamilyBlockLayout(layout.definition, {"details": 99})
            case "receipt":
                worker._output._receipt["result_sha256"] = b"x" * 32
            case "file_sha":
                worker._output.sha256 = b"x" * 32
        with pytest.raises(ValueError):
            with groups.source_family_output_groups(
                allocated, layout, outputs, work=_work(tmp_path), complete_scope=has_complete_scope
            ) as families:
                list(families)
        assert not (tmp_path / "groups").exists()


async def test_only_current_family_spools_and_owner_cancel_cleans(monkeypatch, tmp_path):
    state = SimpleNamespace(is_cancelled=False)

    def check():
        if state.is_cancelled:
            raise RuntimeError("synthetic grouping cancellation")

    async with _inputs(tmp_path, monkeypatch) as (case, allocated, outputs):
        with pytest.raises(RuntimeError, match="grouping cancellation"):
            with groups.source_family_output_groups(
                allocated, _layout(case), outputs, work=_work(tmp_path, check=check), complete_scope=True
            ) as families:
                family = next(families)
                assert len(family.replays_by_role) == 3
                state.is_cancelled = True
                next(families)
        assert not family.is_active and not (tmp_path / "groups").exists()
        assert all(worker.path.is_file() for worker in outputs)


async def test_small_head_reservation_is_operational_refusal(monkeypatch, tmp_path):
    async with _inputs(tmp_path, monkeypatch) as (case, allocated, outputs):
        limits = SourceFactLimits(65536, 4096, 4096)
        with pytest.raises((FamilyBlockResourceLimit, SourceFactBudgetExceeded)):
            with groups.source_family_output_groups(
                allocated, _layout(case), outputs, work=_work(tmp_path, limits=limits), complete_scope=True
            ) as families:
                list(families)
        assert not (tmp_path / "groups").exists()
