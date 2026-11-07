# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Optional SOURCE audits over genuine worker files; database identities are fake."""

import asyncio
import json
import os
import threading
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from db.models.custom_import import CustomImportBuildAttempt, CustomImportGeneration
from process.custom_import import family_shard_original_validation as proof
from process.custom_import import family_shard_semantic_audit as audit
from process.custom_import import family_shard_source_values as values
from process.custom_import.continuation_runtime import producer_token_sha256
from process.custom_import.family import _is_valid_npi
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.family_shard_plan import PlanWork
from tests.test_custom_import_family_shard_original_validation import _LIMITS, _case
from tests.test_custom_import_family_shard_output_groups import _frames, _rewrite_output
from tests.test_custom_import_family_shard_source_plan import _child, _root


def _identity_models(native):
    request = native.context.request
    scope_by_name = {
        name: getattr(request, name)
        for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id")
    }
    owner_by_name = dict(
        capture_bundle_id=dict(native.context.source.source.scope[0])["capture_bundle_id"],
        producing_fence=request.fence,
        producing_token_sha256=producer_token_sha256(request),
    )
    generation = CustomImportGeneration(generation_id=107, **scope_by_name, **owner_by_name)
    build = CustomImportBuildAttempt(
        build_id=109,
        generation_id=107,
        phase="verifying",
        **scope_by_name,
        **owner_by_name,
        base_generation_id=request.expected_base_generation_id,
        base_pointer_version=request.expected_pointer_version,
        source_frozen_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        graph_frozen_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        output_frozen_at=datetime(2026, 1, 3, tzinfo=timezone.utc),
    )
    return generation, build


def _identity(native):
    generation, build = _identity_models(native)
    return audit.source_audit_identity(native.context, generation, build)


def _prepared(root, native):
    return values.prepared_source_family_inputs(
        native.context,
        native.admission,
        native.plan,
        work=PlanWork(root / "prepared", _LIMITS, lambda _path: None),
    )


def _selection(root, native, prepared, registered):
    return audit.source_audit_selection(
        native.context,
        prepared,
        _identity(native),
        work=PlanWork(root / "selection", _LIMITS, lambda _path: None),
        receipt_root=root / "retained-audit",
        register_receipt_cleanup=registered.append,
    )


def _prove(root, native, prepared, selection, **arguments):
    workers = arguments.pop("workers", native.workers)
    keys = arguments.pop("keys", iter(native.keys))
    check = arguments.pop("check", None)
    limits = arguments.pop("limits", _LIMITS)
    return proof.prove_original_outputs(
        native.context,
        native.admission,
        native.plan,
        workers,
        keys,
        work=PlanWork(root / "proof", limits, lambda _path: None, check),
        prepared=prepared,
        semantic_selection=selection,
        **arguments,
    )


@pytest.mark.asyncio
async def test_small_source_audit_keeps_full_counts_and_retained_receipt(tmp_path, monkeypatch):
    registered_roots = []
    async with _case(tmp_path, monkeypatch) as native:
        with (
            _prepared(tmp_path, native) as prepared,
            _selection(tmp_path, native, prepared, registered_roots) as selection,
        ):
            comparison = _prove(tmp_path, native, prepared, selection)
            assert comparison.counts_by_name == selection.counts
            assert comparison.semantic_audit.coverage == (("root", 0, 1, 1), ("child", 7, 1, 1))
            assert "hidden_record_lineage" in comparison.semantic_audit.unproven_boundaries
            assert "closed_acceptance" in comparison.semantic_audit.unproven_boundaries
            comparison.semantic_audit.check()
            document = json.loads(Path(comparison.semantic_audit.path).read_text())
            assert document["contract"] == "custom-import/owner-semantic-audit/v1"
            assert dict(document["full_observed_counts"]) == selection.counts
            assert document["seed_selection_evidence"]["contract"] == audit.compact.SAMPLE_CONTRACT
            assert document["risk_coverage"] == {"population": 0, "audited": 0}
    assert registered_roots == [tmp_path / "retained-audit"]
    assert not (tmp_path / "selection").exists() and not (tmp_path / "prepared").exists()
    assert Path(comparison.semantic_audit.path).stat().st_mode & 0o777 == 0o400
    comparison.semantic_audit.check()


def _normal_rows(count):
    npis = []
    for base in range(100300000, 100301000):
        for digit in range(10):
            npi = str(base) + str(digit)
            if _is_valid_npi(npi):
                npis.append(npi)
                break
        if len(npis) == count:
            break
    return {
        "root_source": [[_root(npi) for npi in npis]],
        "detail_source": [[_child(npi, key="a", amount="1.00") for npi in npis]],
    }


@pytest.mark.asyncio
async def test_unselected_source_skips_assembly_and_full_decode(tmp_path, monkeypatch):
    monkeypatch.setattr(audit.compact, "ROOT_SAMPLE_LIMIT", 1)
    monkeypatch.setattr(audit.compact, "CHILD_SAMPLE_LIMIT", 1)
    async with _case(tmp_path, monkeypatch, _normal_rows(8)) as native:
        with _prepared(tmp_path, native) as prepared, _selection(tmp_path, native, prepared, []) as selection:
            summaries = list(selection.summaries())
            chosen_keys = {summary["typed_key"] for summary in summaries if selection.chosen(summary)}
            assert 0 < len(chosen_keys) < len(summaries)
            assembled, decoded = [], []
            constructor, verify = values._SourceFamily.__init__, proof._verify_family
            group_constructor = proof.outputs.BorrowedSourceFamily.__init__
            spooled_keys = []

            def construct_group(group, work, header, family):
                assert header["typed_key"] in chosen_keys
                spooled_keys.append(header["typed_key"])
                group_constructor(group, work, header, family)

            def initialize(family, context, plan, *arguments):
                assert plan.typed_key in chosen_keys
                assembled.append(plan.typed_key)
                constructor(family, context, plan, *arguments)

            def verify_family(context, plan, *arguments):
                assert plan.typed_key in chosen_keys
                decoded.append(plan.typed_key)
                return verify(context, plan, *arguments)

            monkeypatch.setattr(values._SourceFamily, "__init__", initialize)
            monkeypatch.setattr(proof, "_verify_family", verify_family)
            monkeypatch.setattr(proof.outputs.BorrowedSourceFamily, "__init__", construct_group)
            comparison = _prove(tmp_path, native, prepared, selection)
            assert set(assembled) == set(decoded) == set(spooled_keys) == chosen_keys
            assert comparison.counts_by_name == dict(
                family_count=8, family_child_count=8, root_scalar_count=24, child_scalar_count=24
            )
            assert dict(comparison.semantic_audit.counts)["family_count"] == len(chosen_keys)
            assert comparison.dictionary_key_count == comparison.used_query_key_count == 1


@pytest.mark.asyncio
async def test_missing_retained_receipt_root_is_refused(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            with pytest.raises(ValueError, match="retained receipt root"):
                with audit.source_audit_selection(
                    native.context,
                    prepared,
                    _identity(native),
                    work=PlanWork(tmp_path / "selection", _LIMITS, lambda _path: None),
                    receipt_root=None,
                    register_receipt_cleanup=lambda _path: None,
                ):
                    pytest.fail("a transient receipt cannot authorize sampling")
    assert not (tmp_path / "selection").exists()


@pytest.mark.asyncio
async def test_sampled_empty_source_still_requires_complete_census_and_receipt(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, {"root_source": [[]], "detail_source": [[]]}) as native:
        with _prepared(tmp_path, native) as prepared, _selection(tmp_path, native, prepared, []) as selection:
            comparison = _prove(tmp_path, native, prepared, selection)
            assert comparison.counts_by_name == dict.fromkeys(audit._COUNTS, 0)
            assert comparison.semantic_audit.coverage == (("root", 0, 0, 0), ("child", 7, 0, 0))
            assert comparison.dictionary_key_count == comparison.used_query_key_count == 0
            comparison.semantic_audit.check()


@pytest.mark.asyncio
async def test_sampled_source_without_query_child_keeps_all_children(tmp_path, monkeypatch):
    from process.custom_import.definition import CustomImportDefinition
    from tests.test_custom_import_family_shard_source_values import _definition

    document = json.loads(_definition().canonical)
    document["query"].pop("child")
    async with _case(tmp_path, monkeypatch, definition=CustomImportDefinition.from_mapping(document)) as native:
        with _prepared(tmp_path, native) as prepared, _selection(tmp_path, native, prepared, []) as selection:
            comparison = _prove(tmp_path, native, prepared, selection, keys=iter(()))
            assert comparison.counts_by_name == dict(
                family_count=1, family_child_count=1, root_scalar_count=3, child_scalar_count=3
            )
            assert comparison.dictionary_key_count == comparison.used_query_key_count == 0
            assert comparison.semantic_audit.coverage == (("root", 0, 1, 1), ("child", 7, 1, 1))


@pytest.mark.asyncio
async def test_selection_and_receipt_are_stable_on_retry(tmp_path, monkeypatch):
    monkeypatch.setattr(audit.compact, "ROOT_SAMPLE_LIMIT", 1)
    monkeypatch.setattr(audit.compact, "CHILD_SAMPLE_LIMIT", 1)
    receipts, selected_keys = [], []
    async with _case(tmp_path, monkeypatch, _normal_rows(8)) as native:
        with _prepared(tmp_path, native) as prepared:
            for _attempt in range(2):
                with _selection(tmp_path, native, prepared, []) as selection:
                    selected_keys.append(tuple(selection.source_decisions()))
                    comparison = _prove(tmp_path, native, prepared, selection, workers=tuple(reversed(native.workers)))
                    receipts.append(comparison.semantic_audit)
            assert receipts[0] == receipts[1] and selected_keys[0] == selected_keys[1]
            assert len(receipts[0].selected_keys_sha256) == 32


@pytest.mark.asyncio
async def test_original_risk_lane_is_complete(tmp_path, monkeypatch):
    monkeypatch.setattr(audit.compact, "ROOT_SAMPLE_LIMIT", 1)
    monkeypatch.setattr(audit.compact, "CHILD_SAMPLE_LIMIT", 1)
    rows = _normal_rows(8)
    rows["detail_source"][0].append(dict(rows["detail_source"][0][-1]))
    from process.custom_import.definition import CustomImportDefinition
    from tests.test_custom_import_family_shard_source_values import _definition

    document = json.loads(_definition().canonical)
    document["streams"][1]["duplicate_policy"] = "collapse_identical"
    async with _case(tmp_path, monkeypatch, rows, definition=CustomImportDefinition.from_mapping(document)) as native:
        with _prepared(tmp_path, native) as prepared, _selection(tmp_path, native, prepared, []) as selection:
            risky_keys = [summary["typed_key"] for summary in selection.summaries() if summary["risky"]]
            assert len(risky_keys) == 1
            assert all(keep for key, keep in selection.source_decisions() if key in risky_keys)
            receipt = _prove(tmp_path, native, prepared, selection).semantic_audit
            receipt.check()
            assert json.loads(Path(receipt.path).read_text())["risk_coverage"] == {"population": 1, "audited": 1}


def _damage_unchosen(native, family_id, damage):
    role = "revision_run" if damage == "membership" else "fragment" if damage == "storage" else "query_record"
    for worker in native.workers:
        frames = _frames(worker)
        for index, (current, packed) in enumerate(frames):
            cells = list(_unpack_row(packed))
            if current != role or cells[1 if role == "query_record" else 0] != family_id:
                continue
            if damage == "query_duplicate":
                frames.insert(index, (current, packed))
                _rewrite_output(worker, native.context.layout, frames)
                return
            _change_native_cells(cells, damage)
            frames[index] = role, _pack_row(tuple(cells))
            _rewrite_output(worker, native.context.layout, frames)
            return
    raise AssertionError("the unchanged source family was absent")


def _change_native_cells(cells, damage):
    if damage == "membership":
        cells[2] += 1000000
        cells[3] += 1000000
        return
    if damage == "storage":
        cells[3] = cells[3][:-1] + bytes((cells[3][-1] ^ 1,))
        return
    if damage == "query_identity":
        cells[0] += 1000000
        return
    if damage == "query_rank":
        cells[3] += 1000000
        return
    cells[5] = b"\0" * len(cells[5])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ("membership", "storage", "query_identity", "query_rank", "query_declaration", "query_duplicate")
)
async def test_unchosen_native_claims_are_still_exhaustive(tmp_path, monkeypatch, damage):
    monkeypatch.setattr(audit.compact, "ROOT_SAMPLE_LIMIT", 1)
    monkeypatch.setattr(audit.compact, "CHILD_SAMPLE_LIMIT", 1)
    async with _case(tmp_path, monkeypatch, _normal_rows(8)) as native:
        with _prepared(tmp_path, native) as prepared, _selection(tmp_path, native, prepared, []) as selection:
            summary = next(summary for summary in selection.summaries() if not selection.chosen(summary))
            _damage_unchosen(native, summary["family_revision_id"], damage)
            with pytest.raises((ValueError, proof.FamilyBlockValidationError)):
                _prove(tmp_path, native, prepared, selection)
    assert not (tmp_path / "proof").exists()
    assert not (tmp_path / "retained-audit").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("missing", "wrong_text", "tail_failure"))
async def test_sampled_source_keeps_dictionary_eof(tmp_path, monkeypatch, change):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared, _selection(tmp_path, native, prepared, []) as selection:
            keys = list(native.keys)
            if change == "missing":
                keys.clear()
            elif change == "wrong_text":
                digest, text, rank, variant = keys[0]
                keys[0] = digest, text + " ", rank, variant
            else:
                original_keys = tuple(keys)

                def failed_tail():
                    yield from original_keys
                    raise RuntimeError("synthetic dictionary EOF failure")

                keys = failed_tail()
            with pytest.raises((ValueError, RuntimeError)):
                _prove(tmp_path, native, prepared, selection, keys=keys)
    assert not (tmp_path / "retained-audit").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("keys", "seed", "file", "owner_close", "counts"))
async def test_selection_custody_cannot_be_rebound(tmp_path, monkeypatch, change):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared, _selection(tmp_path, native, prepared, []) as selection:
            _change_selection(selection, prepared, change)
            with pytest.raises(ValueError):
                _prove(tmp_path, native, prepared, selection)
    assert not (tmp_path / "retained-audit").exists()


def _change_selection(selection, prepared, change):
    if change == "keys":
        selection.normal_keys = frozenset()
        return
    if change == "seed":
        object.__setattr__(selection.identity, "seed", b"x" * 32)
        return
    if change == "file":
        selection.summaries_path.chmod(0o600)
        selection.summaries_path.chmod(0o400)
        return
    if change == "counts":
        selection.counts = dict(selection.counts, family_count=1000000)
        return
    prepared.close()


@pytest.mark.asyncio
async def test_selection_directory_rebinding_is_refused_without_losing_owned_cleanup(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared, _selection(tmp_path, native, prepared, []) as selection:
            root, moved = selection.work.root, tmp_path / "moved-selection"
            root.rename(moved)
            root.symlink_to(moved, target_is_directory=True)
            try:
                with pytest.raises(ValueError, match="work directory custody"):
                    _prove(tmp_path, native, prepared, selection)
            finally:
                root.unlink()
                moved.rename(root)
    assert not root.exists() and not (tmp_path / "retained-audit").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("temporary", ("selection", "prepared", "worker", "proof"))
async def test_retained_receipt_cannot_hide_in_temporary_roots(tmp_path, monkeypatch, temporary):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            root = (
                tmp_path / "selection"
                if temporary == "selection"
                else prepared.work.root
                if temporary == "prepared"
                else tmp_path / "proof"
                if temporary == "proof"
                else native.workers[0]._work.root
            )
            with pytest.raises(ValueError, match="temporary scratch"):
                with audit.source_audit_selection(
                    native.context,
                    prepared,
                    _identity(native),
                    work=PlanWork(tmp_path / "selection", _LIMITS, lambda _path: None),
                    receipt_root=root / "audit",
                    register_receipt_cleanup=lambda _path: None,
                ) as selection:
                    _prove(tmp_path, native, prepared, selection)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("bytes", "missing", "directory"))
async def test_retained_audit_custody_remains_required(tmp_path, monkeypatch, change):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared, _selection(tmp_path, native, prepared, []) as selection:
            receipt = _prove(tmp_path, native, prepared, selection).semantic_audit
        path = Path(receipt.path)
        if change == "bytes":
            path.chmod(0o600)
            path.write_bytes(path.read_bytes() + b" ")
            path.chmod(0o400)
        elif change == "missing":
            path.unlink()
        else:
            original = path.parent.with_name("original-audit")
            path.parent.rename(original)
            path.parent.symlink_to(original, target_is_directory=True)
        with pytest.raises((ValueError, FileNotFoundError)):
            receipt.check()


def test_semantic_identity_does_not_accept_a_caller_seed():
    with pytest.raises(TypeError):
        audit.OwnerSemanticIdentity((), b"caller-seed")


@pytest.mark.asyncio
async def test_frozen_identity_cannot_move_between_actual_request_contexts(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        request = replace(native.context.request, expected_base_generation_id=77, expected_pointer_version=3)
        other = replace(native.context, request=request)
        generation, build = _identity_models(native)
        build.base_generation_id, build.base_pointer_version = 77, 3
        identity = audit.source_audit_identity(other, generation, build)
        with _prepared(tmp_path, native) as prepared:
            with pytest.raises(ValueError, match="different original context"):
                with audit.source_audit_selection(
                    native.context,
                    prepared,
                    identity,
                    work=PlanWork(tmp_path / "selection", _LIMITS, lambda _path: None),
                    receipt_root=tmp_path / "retained-audit",
                    register_receipt_cleanup=lambda _path: None,
                ):
                    pytest.fail("a frozen seed cannot be rebound to another original context")
        assert not (tmp_path / "selection").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("freeze", "producer", "capture", "base"))
async def test_identity_requires_exact_frozen_models(tmp_path, monkeypatch, change):
    async with _case(tmp_path, monkeypatch) as native:
        generation, build = _identity_models(native)
        if change == "freeze":
            build.graph_frozen_at = None
        elif change == "producer":
            generation.producing_fence += 1
        elif change == "capture":
            generation.capture_bundle_id += 1
        else:
            build.base_pointer_version += 1
        with pytest.raises(ValueError, match="frozen owner scope"):
            audit.source_audit_identity(native.context, generation, build)


@pytest.mark.asyncio
async def test_large_selected_family_is_complete_and_spills(tmp_path, monkeypatch):
    rows_by_stream = {
        "root_source": [[_root()]],
        "detail_source": [[_child(key=f"key-{index}") for index in range(300)]],
    }
    async with _case(tmp_path, monkeypatch, rows_by_stream) as native:
        with (
            values.prepared_source_family_inputs(
                native.context,
                native.admission,
                native.plan,
                work=PlanWork(tmp_path / "prepared", replace(_LIMITS, memory_bytes=16384), lambda _path: None),
            ) as prepared,
            audit.source_audit_selection(
                native.context,
                prepared,
                _identity(native),
                work=PlanWork(tmp_path / "selection", replace(_LIMITS, memory_bytes=65536), lambda _path: None),
                receipt_root=tmp_path / "retained-audit",
                register_receipt_cleanup=lambda _path: None,
            ) as selection,
        ):
            staged_bytes = prepared.work.used
            comparison = _prove(tmp_path, native, prepared, selection)
            assert comparison.counts_by_name["family_child_count"] == 300
            assert comparison.semantic_audit.coverage == (("root", 0, 1, 1), ("child", 7, 300, 300))
            assert prepared.work.peak_bytes > staged_bytes
            assert selection.work.peak_bytes <= _LIMITS.max_spool_bytes


@pytest.mark.asyncio
async def test_census_cancellation_drains_without_leaking_replay(tmp_path, monkeypatch):
    from process.custom_import.family_shard_plan import _cpu, _Scratch

    rows_by_stream = {
        "root_source": [[_root()]],
        "detail_source": [[_child(key=f"key-{index}") for index in range(100)]],
    }
    async with _case(tmp_path, monkeypatch, rows_by_stream) as native:
        with _prepared(tmp_path, native) as prepared:
            outer = _Scratch(PlanWork(tmp_path / "cpu", _LIMITS, lambda _path: None))
            identity = _identity(native)
            entered, visits, original = threading.Event(), [], audit._scalar_count

            def scalar_count(*arguments):
                visits.append(True)
                if len(visits) == 32:
                    entered.set()
                    assert outer.stop.wait(10), "owner did not cancel the source census"
                return original(*arguments)

            def prepare():
                with audit.source_audit_selection(
                    native.context,
                    prepared,
                    identity,
                    work=PlanWork(tmp_path / "selection", _LIMITS, lambda _path: None, outer.current),
                    receipt_root=tmp_path / "retained-audit",
                    register_receipt_cleanup=lambda _path: None,
                ):
                    pytest.fail("cancelled census cannot return a selected audit")

            monkeypatch.setattr(audit, "_scalar_count", scalar_count)
            task = asyncio.create_task(_cpu(outer, prepare))
            try:
                await _wait_for_cpu(entered)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert len(visits) == 32
                assert prepared._active is None and not (tmp_path / "selection").exists()
                with closing(prepared.observations()) as families:
                    assert sum(sum(1 for _record in occurrences) for _plan, occurrences in families) == 101
            finally:
                try:
                    await _drain_cancelled(task)
                finally:
                    outer.close()
    assert not (tmp_path / "retained-audit").exists()


async def _wait_for_cpu(entered):
    async with asyncio.timeout(10):
        while not entered.is_set():
            await asyncio.sleep(0)


async def _drain_cancelled(task):
    if not task.done():
        task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return


@pytest.mark.asyncio
async def test_census_checks_all_original_field_declarations(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared, _selection(tmp_path, native, prepared, []) as selection:
            with closing(prepared.observations()) as families:
                _plan, occurrences = next(families)
                occurrence = next(occurrences)
                collection = values._collection(native.context, occurrence, occurrence.record)
                record = replace(occurrence.record, values=occurrence.record.values[:-1])
                with pytest.raises(ValueError, match="field declarations"):
                    audit._scalar_count(selection, occurrence, record, collection)


@pytest.mark.asyncio
async def test_census_budget_refusal_cleans_exact_scratch(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared(tmp_path, native) as prepared:
            with pytest.raises(SourceFactBudgetExceeded, match="resource budget"):
                with audit.source_audit_selection(
                    native.context,
                    prepared,
                    _identity(native),
                    work=PlanWork(
                        tmp_path / "selection",
                        replace(_LIMITS, max_spool_bytes=1, max_record_bytes=1, memory_bytes=1),
                        lambda _path: None,
                    ),
                    receipt_root=tmp_path / "retained-audit",
                    register_receipt_cleanup=lambda _path: None,
                ):
                    pytest.fail("an exhausted census cannot return an audit selection")
            assert not (tmp_path / "selection").exists()
            with closing(prepared.observations()) as families:
                assert sum(sum(1 for _record in occurrences) for _plan, occurrences in families) > 0


@pytest.mark.parametrize("failure", ("file", "directory"))
def test_receipt_interrupted_fsync_preserves_only_exact_retry(tmp_path, monkeypatch, failure):
    root, receipt_bytes = tmp_path / "retained", b'{"contract":"synthetic"}'
    original_fsync = audit.os.fsync

    def interrupted(descriptor):
        is_directory = audit.stat.S_ISDIR(os.fstat(descriptor).st_mode)
        if is_directory == (failure == "directory"):
            raise OSError("synthetic receipt fsync failure")
        return original_fsync(descriptor)

    monkeypatch.setattr(audit.os, "fsync", interrupted)
    with pytest.raises(OSError, match="synthetic receipt"):
        audit._persist(root, receipt_bytes)
    names = list(root.iterdir())
    assert [path.name for path in names] == ([audit._FILE] if failure == "directory" else [])
    identity = names[0].stat().st_ino if names else None
    monkeypatch.setattr(audit.os, "fsync", original_fsync)
    path, root_identity = audit._persist(root, receipt_bytes)
    assert path.read_bytes() == receipt_bytes and path.stat().st_mode & 0o777 == 0o400
    assert root_identity == (root.stat().st_dev, root.stat().st_ino)
    if identity is not None:
        assert path.stat().st_ino == identity
    with pytest.raises(ValueError, match="retry receipt differs"):
        audit._persist(root, receipt_bytes + b" ")
    assert path.read_bytes() == receipt_bytes
