# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Compare real admitted SOURCE response counts with the existing SQL reducer."""

import itertools
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path

import pytest

from process.custom_import import family_shard_counts as counts
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_shard_admission import cleanup_admission_output, read_admission_occurrences
from process.custom_import.family_shard_common import SourceCommonContext
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.runner_types import CancellationRequested, CandidateRunnerError
from tests.test_custom_import_build_counts import _evaluate_outcome_query
from tests.test_custom_import_family_shard_admission import _build_audit, _inputs
from tests.test_custom_import_family_shard_build import _compound_definition
from tests.test_custom_import_family_shard_source_plan import _child, _root
from tests.test_custom_import_family_shard_worker import _projected_definition

_LIMITS = SourceFactLimits(64 * 1024**2, 1024**2, 512)


@asynccontextmanager
async def _case(tmp_path, monkeypatch, roots, children, *, collapse=False, definition=None):
    rows_by_stream = {"root_source": [roots], "detail_source": [children]}
    async with _inputs(
        tmp_path,
        monkeypatch,
        rows_by_stream,
        definition=definition or _projected_definition(collapse=collapse),
        complete_scope=False,
    ) as inputs:
        case, source, workers = inputs
        admission = _build_audit(tmp_path, case, source, workers, _LIMITS, complete_scope=case[1].complete_scope)
        try:
            context = SourceCommonContext(
                case[1], FamilyBlockLayout(case[1].definition, case[2].child_collection_slots), source
            )
            yield context, admission
        finally:
            cleanup_admission_output(admission)


def _old_rows(admission):
    codes_by_target = {entry.target: entry.event.code for entry in counts.read_admission_resolutions(admission)}
    rows = []
    for occurrence in read_admission_occurrences(admission):
        record = occurrence.record
        raw_key, raw_hash = (None, None) if record.raw_parent_key is None else record.raw_parent_key
        rows.append(
            dict(
                record_kind="root" if record.collection_slot == 0 else "child",
                # The SQL oracle registers collections by definition ordinal.
                collection_slot=record.collection_slot,
                raw_parent_key_canonical=raw_key,
                raw_parent_key_sha256=raw_hash,
                root_record_id=occurrence.root_record_id,
                root_revision_id=occurrence.revision_id if record.collection_slot == 0 else None,
                initial=record.rejection_code,
                resolved=codes_by_target.get(occurrence.coordinate),
                origin="source",
                build_id=5,
            )
        )
    return rows


def _count(tmp_path, context, admission, *, check=None):
    return counts.count_admitted_source_outcomes(
        context, admission, work=PlanWork(tmp_path / "outcomes", _LIMITS, lambda _path: None, check)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "roots,children,expected",
    [
        ([], [], (0, 0)),
        ([_root()], [], (1, 0)),
        ([], [_child()], (0, 0)),
        ([_root()], [_child()], (1, 0)),
        ([_root(), _root()], [], (0, 1)),
        ([_root(score=None)], [], (0, 1)),
        ([_root(npi=None), _root()], [], (1, 1)),
        ([{}, _root()], [], (1, 1)),
        ([_root(score=None), _root()], [], (0, 2)),
        ([_root()], [_child(amount="bad")], (0, 1)),
        ([_root()], [_child(amount="bad"), _child(amount="bad")], (0, 1)),
        ([_root()], [_child(), _child()], (0, 1)),
        ([_root()], [_child(key=None)], (0, 1)),
        ([_root()], [{}], (1, 0)),
        ([_root()], [_child("orphan", amount="bad")], (1, 0)),
        ([_root(score=None)], [_child(amount="bad")], (0, 2)),
    ],
)
async def test_complete_admission_counts_match_legacy_sql(tmp_path, monkeypatch, roots, children, expected):
    async with _case(tmp_path, monkeypatch, roots, children) as (context, admission):
        oracle = _evaluate_outcome_query(context.request, _old_rows(admission))
        result = _count(tmp_path, context, admission)
        assert result == oracle == counts.legacy.SourceOutcomeCounts(*expected)
        assert not (tmp_path / "outcomes").exists()
        assert Path(admission.root).exists() and Path(context.source.root).exists()


@pytest.mark.asyncio
async def test_identical_duplicate_collapse_uses_original_policy(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, [_root()], [_child(), _child()], collapse=True) as (context, admission):
        result = _count(tmp_path, context, admission)
        assert result == _evaluate_outcome_query(context.request, _old_rows(admission))
        assert result == counts.legacy.SourceOutcomeCounts(1, 0)


@pytest.mark.asyncio
async def test_raw_decimal_variants_keep_global_typed_duplicate_counts(tmp_path, monkeypatch):
    roots = [_root(score="1.0"), _root(score="1.00")]
    async with _case(tmp_path, monkeypatch, roots, [], definition=_compound_definition("decimal")) as (
        context,
        admission,
    ):
        result = _count(tmp_path, context, admission)
        assert result == _evaluate_outcome_query(context.request, _old_rows(admission))
        assert result == counts.legacy.SourceOutcomeCounts(0, 2)


@pytest.mark.asyncio
async def test_split_family_errors_count_once_under_external_merge(tmp_path, monkeypatch):
    child_rows = [_child(key=f"key-{index}", amount="bad") for index in range(80)]
    async with _case(tmp_path, monkeypatch, [_root()], child_rows) as (context, admission):
        result = _count(tmp_path, context, admission)
        assert result == _evaluate_outcome_query(context.request, _old_rows(admission))
        assert result == counts.legacy.SourceOutcomeCounts(0, 1)


@pytest.mark.asyncio
async def test_counts_require_exact_original_source_scope(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, [_root()], []) as (context, admission):
        wrong = replace(admission, allocation_sha256=b"x" * 32)
        with pytest.raises(FamilyBlockUnavailable, match="scope"):
            _count(tmp_path, context, wrong)
        assert not (tmp_path / "outcomes").exists()


@pytest.mark.asyncio
async def test_cancellation_preserves_originals_and_removes_scratch(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, [_root()], [_child()]) as (context, admission):
        visits = itertools.count(1)
        failure = CancellationRequested("stop counting original SOURCE")

        def check():
            if next(visits) == 8:
                raise failure

        with pytest.raises(CancellationRequested) as captured:
            _count(tmp_path, context, admission, check=check)
        assert captured.value is failure
        assert not (tmp_path / "outcomes").exists()
        assert Path(admission.root).exists() and Path(context.source.root).exists()


@pytest.mark.asyncio
async def test_resolution_eof_must_finish_before_counts_return(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, [_root()], [_child(), _child()]) as (context, admission):
        original = counts.read_admission_resolutions

        def incomplete(*arguments, **keywords):
            yield from original(*arguments, **keywords)
            raise ValueError("late resolution EOF failure")

        monkeypatch.setattr(counts, "read_admission_resolutions", incomplete)
        with pytest.raises(ValueError, match="late resolution EOF"):
            _count(tmp_path, context, admission)
        assert not (tmp_path / "outcomes").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["duplicate", "foreign", "trailing", "unknown_code"])
async def test_resolution_damage_cannot_be_hidden_by_totals(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch, [_root()], [_child(), _child()]) as (context, admission):
        original = counts.read_admission_resolutions

        def damaged(*arguments, **keywords):
            entries = list(original(*arguments, **keywords))
            assert entries
            first = entries[0]
            match damage:
                case "duplicate":
                    entries.insert(0, first)
                case "foreign":
                    entries.insert(0, replace(first, target=(-1, 0)))
                case "trailing":
                    entries.append(replace(entries[-1], target=(999, 999)))
                case "unknown_code":
                    entries = [replace(entry, event=replace(entry.event, code="unsupported")) for entry in entries]
            yield from entries

        monkeypatch.setattr(counts, "read_admission_resolutions", damaged)
        with pytest.raises(CandidateRunnerError):
            _count(tmp_path, context, admission)
        assert not (tmp_path / "outcomes").exists()
