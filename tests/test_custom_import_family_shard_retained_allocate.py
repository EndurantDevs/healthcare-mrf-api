# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure destination grouping/replay tests; native ownership is qualified separately."""

import pickle
import time
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import msgpack
import pytest

from process.custom_import import build_graph_sets as original
from process.custom_import import family_shard_retained_allocate as allocate
from process.custom_import import family_shard_retained_pages as pages
from process.custom_import import runner_codec as codec
from process.custom_import.family import RootFamily, _is_valid_npi
from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_records import FamilyRecord, record_parts
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.family_shard_plan import BaseFamilyHeader, FinalFamilyPlan, PlanWork, _plan_cells, _Scratch
from process.custom_import.materialization import TypedScalar
from process.custom_import.runner_types import CandidateRegistry
from tests.test_custom_import_family_shard_worker import _projected_definition

_FACTS = SourceFactLimits(64 * 1024**2, 4 * 1024**2, 8 * 1024**2)
_LIMITS = SourceAllocationLimits(_FACTS, 100000, 32 * 1024**2)
_STAMP = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _context(*, row_limit=64, byte_limit=1024**2):
    definition = _projected_definition()
    request = SimpleNamespace(
        dataset_id=101,
        definition_revision_id=103,
        schema_revision_id=19,
        execution_id=888,
        fence=9,
        lease_token=b"owner-token",
        continuation=None,
        definition=definition,
        page_row_limit=row_limit,
        page_byte_limit=byte_limit,
        operation_deadline=None,
        authorization_expires_at=None,
        build_deadline_at=None,
        statement_timeout_ms=1000,
    )
    layout = FamilyBlockLayout(definition, {"details": 7})
    common = SimpleNamespace(
        request=request,
        layout=layout,
        columns_by_role={role: tuple(table.c.keys()) for role, table in layout.tables(31).items()},
        source=SimpleNamespace(source=SimpleNamespace(scope=((("capture_bundle_id", 123),),))),
    )
    base = SimpleNamespace(
        generation=SimpleNamespace(
            dataset_id=101,
            definition_revision_id=102,
            schema_revision_id=19,
            execution_id=777,
            producing_fence=8,
            producing_token_sha256=b"t" * 32,
        ),
        check=AsyncMock(),
    )
    common_output = SimpleNamespace(proof_inputs={"owner_created_at": _STAMP.isoformat()})
    return SimpleNamespace(common=common, base=base, build_id=555, common_output=common_output)


def _originals(root_id, count):
    prefix = str(120000000 + root_id)
    npi = next(prefix + str(digit) for digit in range(10) if _is_valid_npi(prefix + str(digit)))
    root = FamilyRecord(
        root_id * 10 + 1,
        root_id * 10 + 2,
        71,
        {"npi": npi, "score": root_id, "enabled": True},
        {"score": TypedScalar("integer", "value", integer_value=999)},
    )
    child_records = tuple(
        FamilyRecord(
            root_id * 100 + 3 + index * 17,
            root_id * 10 + 4,
            91 + index,
            {"detail_npi": npi, "detail_id": f"key-{index:03}", "amount": Decimal("1.2300")},
            {"amount": TypedScalar("decimal", "value", decimal_value=Decimal("8.000000000000"))},
        )
        for index in range(count)
    )
    return root, child_records


def _identity_rows(context, root_id, base, root, key):
    definition = context.common.request.definition
    family_by_name = dict(
        family_revision_id=base.family_revision_id,
        root_record_id=root_id,
        root_revision_id=root.revision_id,
        entity_binding_id=base.entity_binding_id,
        family_sha256=base.family_sha256,
        child_count=base.child_count,
    )
    root_dictionary = dict(
        root_record_id=root_id,
        dataset_id=101,
        canonical_logical_key=key,
        logical_key_sha256=base.typed_key[1],
        key_contract_sha256=codec.root_key_contract_hash(definition),
        created_at=_STAMP,
    )
    entity_by_name = dict(
        entity_binding_id=base.entity_binding_id,
        dataset_id=101,
        adapter_id="npi",
        canonical_value=root.values_by_field["npi"],
        value_sha256=codec.digest_text("entity:npi", root.values_by_field["npi"]),
        created_at=_STAMP,
    )
    return family_by_name, root_dictionary, entity_by_name


def _view(context, root_id, *, kind="retained", children=3):
    definition = context.common.request.definition
    root, originals = _originals(root_id, children)
    key = codec.root_key_document(definition, root.values_by_field)
    digest = codec.new_family_hash(
        definition,
        RootFamily(
            (root.values_by_field["npi"],),
            root.values_by_field,
            {"details": tuple(child_record.values_by_field for child_record in originals)},
        ),
    )
    base = BaseFamilyHeader(
        (key, codec.digest_text("root-key", key)),
        root_id,
        root_id + 2000,
        root.revision_id,
        root_id + 3000,
        digest,
        children,
        77,
        31,
    )
    plan = FinalFamilyPlan(
        base.typed_key,
        kind,
        None,
        None,
        (root_id + 4000, base.entity_binding_id) if kind == "source" else None,
        base if kind == "retained" else None,
        None,
    )
    metadata = (
        root_id,
        kind,
        _plan_cells(plan),
        root_id + 4000 if kind == "source" else base.family_revision_id,
        root_id,
        base.entity_binding_id,
        digest,
        children,
        b"",
    )
    family_by_name, root_dictionary, entity_by_name = _identity_rows(context, root_id, base, root, key)
    lineage = _pack_row(({"source_ordinal": root.source_ordinal}, ("original-scalar-declaration",)))
    cells = (
        metadata,
        (root_id, allocate._wire(definition, None, root, kind == "retained"), 0, lineage),
        (root_id, _pack_row((family_by_name, {}))) if kind == "retained" else None,
        None,
        _pack_row(root_dictionary),
        _pack_row(entity_by_name),
    )
    return allocate._legacy_input(context, cells), originals


def _child_spool(context, work, views):
    path = work.path()
    with path.open("xb") as spool_output:
        indexed_views = []
        for view, originals in views:
            start = spool_output.tell()
            kind = view.source.plan.selection_kind
            ordered = sorted(
                originals,
                key=lambda original_record: (
                    original_record.revision_id
                    if kind == "retained"
                    else record_parts(context.common.request.definition, "details", original_record, retained=False)[1][
                        0
                    ]
                ),
            )
            for original_record in ordered:
                parts = record_parts(
                    context.common.request.definition, "details", original_record, retained=kind == "retained"
                )[1]
                order = (
                    (7, original_record.revision_id)
                    if kind == "retained"
                    else ("details", parts[0], original_record.revision_id)
                )
                work.frame(
                    spool_output,
                    (
                        view.root_id,
                        kind,
                        order,
                        7,
                        allocate._wire(
                            context.common.request.definition, "details", original_record, kind == "retained"
                        ),
                        17,
                        _pack_row(({"original": original_record.revision_id}, ())) if kind == "retained" else None,
                    ),
                    path,
                )
            indexed_views.append(replace(view, child_start=start, child_end=spool_output.tell()))
    return path, tuple(indexed_views)


def _registry():
    return CandidateRegistry({"details": 7}, {"root_source": 11, "detail_source": 23}, 11)


def _ids(monkeypatch, *, fail_measure=None, start=None):
    seen, current = [], dict(family_revision=10000, root_revision=20000, child_revision=30000, pack=40000)
    if start is not None:
        current = dict.fromkeys(current, start)

    async def observe(_factory, context, _prefix, kinds, _limits, *, remembered=None, cursor=()):
        await context.base.check()
        assigned_ids = []
        for kind in kinds:
            current[kind] += 3 if start is None else 1
            assigned_ids.append(current[kind])
        seen.append((kinds, tuple(assigned_ids)))
        if remembered is not None:
            from process.custom_import.build_source import _PageWindow

            remembered(_PageWindow(time.monotonic() + 60, 1000))
        return tuple(assigned_ids)

    async def is_measured_allowed(_factory, _context, _prefix, costs, _limits, *, control, kind, roots=False):
        if fail_measure is not None and fail_measure(kind, roots):
            return False
        assert all(page.work_bytes >= 0 and page.model_bytes >= 0 for page in costs)
        return True

    monkeypatch.setattr(allocate, "_observe_ids", observe)
    monkeypatch.setattr(allocate, "_is_measured_work_allowed", is_measured_allowed)
    return seen


async def _produce_destination(factory, context, *, work):
    """Pure fixtures supply their explicit fake ID transport, never native sequences."""

    async def observe(prefix, kinds, remembered, *, cursor=()):
        return await allocate._observe_ids(
            factory, context, prefix, kinds, work.limits, remembered=remembered, cursor=cursor
        )

    return await allocate._produce_retained_destination(factory, context, work=work, observe=observe)


async def _prepared(
    tmp_path, monkeypatch, *, kinds=("retained",), count=3, failure=None, start=None, reservation_class=None
):
    context = _context(row_limit=18 if failure else 64)
    work = _Scratch(PlanWork(tmp_path / "work", _FACTS, lambda _path: None))
    pairs = tuple(_view(context, index + 1, kind=kind, children=count) for index, kind in enumerate(kinds))
    child_path, views = _child_spool(context, work, pairs)
    ids = _ids(monkeypatch, fail_measure=failure, start=start)
    output = allocate._Output(context, work, ((11, 3), (23, 5)))

    async def observed(kinds, remembered, *, cursor=()):
        return await allocate._observe_ids(
            None, context, output.prefix, kinds, _LIMITS, remembered=remembered, cursor=cursor
        )

    control = allocate.RetainedControlWindow(work, _LIMITS, observed)
    with output, (reservation_class or allocate.RetainedReservations)(work, _LIMITS, control) as bank:
        retained_views = tuple(view for view in views if view.source.plan.selection_kind == "retained")
        states = (
            list(await allocate._start_retained(None, context, work, _registry(), views, output, bank))
            if retained_views
            else []
        )
        for view in views:
            if view.source.plan.selection_kind == "source":
                states.append(
                    pages.started_view(
                        view.source,
                        family_id=view.original_cells[0][3],
                        revision_id=view.source.root.root_revision_id,
                        entity_id=view.original_cells[0][5],
                    )
                )
        states.sort(key=lambda state: state.current.root_record_id)
        await allocate._consume_children(
            None, context, work, (_registry(), views, tuple(states), child_path), output, bank, control
        )
        await bank.finish(output)
    return context, work, output, views, ids


async def test_retained_packs_preserve_original_hashes_positions_and_observed_gaps(tmp_path, monkeypatch):
    context, work, output, views, ids = await _prepared(tmp_path, monkeypatch, count=5)
    try:
        record_rows = list(work.rows(output.paths["record"]))
        pairs = [
            (
                record_row[1],
                allocate._record(
                    context.common.request.definition,
                    None if record_row[1] == 0 else "details",
                    record_row[3],
                    retained=True,
                ),
                allocate._record(
                    context.common.request.definition,
                    None if record_row[1] == 0 else "details",
                    allocate._assigned_wire(record_row[3], record_row[4]),
                    retained=True,
                ),
            )
            for record_row in record_rows
        ]
        assert pairs[0][1].source_ordinal == 71 and pairs[0][2].source_ordinal == 0
        child_pairs = [(before, after) for slot, before, after in pairs if slot]
        assert [after.source_ordinal for _before, after in child_pairs] == list(range(5))
        assert all(
            after.revision_id != before.revision_id and after.scalars == before.scalars for before, after in child_pairs
        )
        packs = [
            dict(zip(context.common.columns_by_role["pack"], _unpack_row(record_row[1]), strict=True))
            for record_row in work.rows(output.paths["copy"])
            if record_row[0] == "pack"
        ]
        assert [(pack["stream_slot"], pack["pack_ordinal"], pack["record_count"]) for pack in packs] == [
            (11, 3, 1),
            (23, 5, 5),
        ]
        expected_hashes = tuple(
            codec.digest_text(
                "child-payload", record_parts(context.common.request.definition, "details", before, retained=True)[1][2]
            )
            for before, _after in child_pairs
        )
        assert packs[1]["pack_sha256"] == codec.pack_hash("details", expected_hashes)
        assert all(
            right_id - left_id == 3
            for left_id, right_id in zip(
                [after.revision_id for _before, after in child_pairs],
                [after.revision_id for _before, after in child_pairs][1:],
            )
        )
        assert context.base.check.await_count and ids
        assert views[0].source.child_count == 5
    finally:
        work.close()


async def test_source_cost_backoff_changes_retained_groups_before_acceptance(tmp_path, monkeypatch):
    calls = []

    def should_fail_once(kind, roots):
        calls.append((kind, roots))
        return not roots and kind == "source" and calls.count(("source", False)) == 1

    _context_value, work, output, _views, _ids_seen = await _prepared(
        tmp_path,
        monkeypatch,
        kinds=("source", "retained"),
        failure=should_fail_once,
    )
    try:
        records = [
            msgpack.unpackb(allocate._assigned_wire(row[3], row[4]), raw=False)
            for row in work.rows(output.paths["record"])
            if row[1]
        ]
        assert [record[2] for record in records] == [0, 0, 0]
        assert len({record[1] for record in records}) == 3
        assert calls[1:3] == [("source", False), ("retained", False)]
    finally:
        work.close()


async def test_failed_retained_native_cost_attempt_keeps_gaps_without_output_prefix(tmp_path, monkeypatch):
    failures = []

    def should_fail_once(kind, roots):
        if kind == "retained" and not roots:
            failures.append(True)
            return len(failures) == 1
        return False

    _context_value, work, output, _views, _seen = await _prepared(tmp_path, monkeypatch, failure=should_fail_once)
    try:
        observed_rows = list(work.rows(output.paths["allocation"]))
        failed_ids = {row[3] for row in observed_rows if row[0] == 1 and not row[4] and row[2] == "child_revision"}
        accepted_ids = {row[3] for row in observed_rows if row[4] and row[2] == "child_revision"}
        assert failed_ids and accepted_ids and failed_ids.isdisjoint(accepted_ids)
        assert max(failed_ids) < min(accepted_ids)
        assert output.counts["child"] == 3
        allocate._unique_observed(work, output)
    finally:
        work.close()


def test_original_root_and_child_arguments_and_shared_backoff_are_unchanged(tmp_path):
    context = _context()
    work = _Scratch(PlanWork(tmp_path / "work", _FACTS, lambda _path: None))
    try:
        path, views = _child_spool(context, work, (_view(context, 1),))
        family_input = views[0].source
        assert tuple(pages.root_attempts(context.common.request, _registry(), (family_input,))) == tuple(
            original._root_batches(context.common.request, _registry(), (family_input,))
        )
        state = pages.started_view(
            family_input, family_id=111, revision_id=222, entity_id=family_input.entity_binding_id
        )
        limits = pages.initial_limits(context.common.request, _registry())
        with closing(allocate._ChildReader(context, work, path, views)) as reader:
            attempt = pages.child_attempt(
                context.common.request,
                _registry(),
                (family_input,),
                (state,),
                "retained",
                reader.candidates((state,), "retained", 100),
                limits,
            )
        costs = allocate._child_costs(
            context,
            attempt,
            reader,
            {(1, child.child_revision_id): 4000 + index for index, child in enumerate(attempt.groups[0].children)},
        )
        assert len(costs) == len(attempt.pages)
        assert pages.smaller_limits(limits, attempt) == (
            min(limits[0], max(attempt.arguments[-1][1])) // 2,
            min(limits[1], max(1, attempt.consumed // 2)),
        )
    finally:
        work.close()


async def test_closed_replay_pickle_census_lineage_and_factory_lifetime(tmp_path, monkeypatch):
    context, work, output, _views, _seen = await _prepared(tmp_path, monkeypatch)
    original = work.write(())
    context.retained_input = SimpleNamespace(counts_by_role={"pack": 0, "origin": 0})
    descriptor = allocate._write_complete(context, ("test-scope",), work, {"original": original}, output)
    try:
        assert pickle.loads(pickle.dumps(descriptor)) == descriptor
        destination_rows = list(allocate.read_retained_destination(descriptor))
        assert len([entry for entry in destination_rows if entry[0] == "record"]) == 4
        assert (
            _unpack_row(next(entry for entry in destination_rows if entry[0] == "record" and entry[2] == 0)[6])[0][
                "source_ordinal"
            ]
            == 71
        )
        with allocate.retained_destination_families(
            descriptor,
            context.common.request.definition,
            work=PlanWork(tmp_path / "families", _FACTS, lambda _path: None),
        ) as families:
            _plan, before, after = next(families)
            old_child_records = tuple(before.children_by_collection["details"]())
            new_child_records = tuple(after.children_by_collection["details"]())
            assert sorted(child_record.source_ordinal for child_record in old_child_records) == [91, 92, 93]
            assert sorted(child_record.source_ordinal for child_record in new_child_records) == [0, 1, 2]
            factory = after.children_by_collection["details"]
            assert list(families) == []
            with pytest.raises(ValueError, match="outlived"):
                tuple(factory())
        assert len(list(descriptor.batches(maximum_rows=1, maximum_bytes=1024**2))) == 2
    finally:
        allocate.cleanup_retained_destination(descriptor)


@pytest.mark.parametrize("damage", ["bytes", "suffix", "directory", "cancel"])
async def test_tamper_custody_or_late_owner_cancellation_cannot_complete_replay(tmp_path, monkeypatch, damage):
    context, work, output, _views, _seen = await _prepared(tmp_path, monkeypatch)
    context.retained_input = SimpleNamespace(counts_by_role={"pack": 0, "origin": 0})
    descriptor = allocate._write_complete(context, (), work, {"original": work.write(())}, output)
    path = Path(descriptor.root) / allocate._FILE
    try:
        check = None
        if damage == "bytes":
            path.chmod(0o600)
            packed_bytes = bytearray(path.read_bytes())
            packed_bytes[-1] ^= 1
            path.write_bytes(packed_bytes)
            path.chmod(0o400)
        elif damage == "suffix":
            path.chmod(0o600)
            path.write_bytes(path.read_bytes() + b"x")
            path.chmod(0o400)
        elif damage == "directory":
            (Path(descriptor.root) / "unowned").touch()
        else:
            polls = []

            def check():
                polls.append(None)
                if len(polls) == 5:
                    raise RuntimeError("late owner cancel")

        with pytest.raises((ValueError, RuntimeError)):
            list(allocate.read_retained_destination(descriptor, check=check))
    finally:
        if damage == "directory":
            (Path(descriptor.root) / "unowned").unlink()
        if damage in ("bytes", "suffix"):
            import shutil

            shutil.rmtree(descriptor.root)
        else:
            allocate.cleanup_retained_destination(descriptor)


def test_operational_owner_memory_does_not_silently_split_legacy_cohorts():
    context = _context()
    view, _children = _view(context, 1)
    with pytest.raises(FamilyBlockResourceLimit, match="owner buffer"):
        tuple(pages.root_cohorts(context.common.request, _registry(), (view,), memory_bytes=1))


async def test_late_original_failure_happens_before_any_native_assignment(tmp_path, monkeypatch):
    context = _context()
    context.final_plan = SimpleNamespace(counts_by_kind={"retained": 1})
    observed = AsyncMock()
    monkeypatch.setattr(allocate, "_scope", lambda _context, _work: ())
    monkeypatch.setattr(allocate, "_observe_ids", observed)

    def broken(_context, work):
        work.write(((1,),))
        raise ValueError("late original EOF")

    monkeypatch.setattr(allocate, "_stage_inputs", broken)
    with pytest.raises(ValueError, match="late original EOF"):
        await _produce_destination(
            None, context, work=allocate.RetainedAllocationWork(tmp_path / "allocate", _LIMITS, lambda _path: None)
        )
    observed.assert_not_called()
    assert not (tmp_path / "allocate").exists()


@pytest.mark.parametrize("has_common,conflict", [(False, False), (True, False), (True, True)])
def test_independent_common_dictionary_overlap_is_deduplicated_or_refused(tmp_path, has_common, conflict):
    context = _context()
    work = _Scratch(PlanWork(tmp_path / "work", _FACTS, lambda _path: None))
    try:
        view, _children = _view(context, 3)
        dictionary_path = work.write((view.original_cells,))
        paths_by_role = {}
        for role, position in (("root_record", -2), ("entity_binding", -1)):
            packed = view.original_cells[position]
            native = _unpack_row(packed)
            identity = native["root_record_id" if role == "root_record" else "entity_binding_id"]
            common_rows = [(identity - 1, _pack_row({"unselected_source": True}))]
            if has_common:
                if conflict and role == "entity_binding":
                    native["canonical_value"] = "conflicting original bytes"
                    packed = _pack_row(native)
                common_rows.append((identity, packed))
            paths_by_role["common_" + role] = work.write(common_rows)
        with allocate._Output(context, work, ((11, 0), (23, 0))) as output:
            if conflict:
                with pytest.raises(allocate.FamilyBlockUnavailable, match="independent SOURCE"):
                    allocate._dictionary_difference(work, paths_by_role, dictionary_path, output)
            else:
                allocate._dictionary_difference(work, paths_by_role, dictionary_path, output)
                assert (output.counts["root_record"], output.counts["entity_binding"]) == (
                    (0, 0) if has_common else (1, 1)
                )
    finally:
        work.close()


async def test_root_cost_rejection_keeps_split_order_and_failed_observed_ids(tmp_path, monkeypatch):
    polls = []

    def should_fail_once(kind, roots):
        if roots:
            polls.append(kind)
            return len(polls) == 1
        return False

    _context_value, work, output, _views, _seen = await _prepared(
        tmp_path,
        monkeypatch,
        kinds=("retained", "retained"),
        count=0,
        failure=should_fail_once,
    )
    try:
        observed_rows = list(work.rows(output.paths["allocation"]))
        assert len([row for row in observed_rows if row[0] == 0 and not row[4]]) == 6
        assert len([row for row in observed_rows if row[4]]) == 6
        assert [row[1] for row in work.rows(output.paths["family"])] == [1, 2]
        allocate._unique_observed(work, output)
    finally:
        work.close()


async def test_empty_retained_output_has_real_footer_without_native_assignments(tmp_path, monkeypatch):
    context, work, output, _views, seen = await _prepared(tmp_path, monkeypatch, kinds=("source",), count=0)
    context.retained_input = SimpleNamespace(counts_by_role={"pack": 0, "origin": 0})
    descriptor = allocate._write_complete(context, (), work, {"original": work.write(())}, output)
    try:
        assert seen == [] and list(allocate.read_retained_destination(descriptor)) == []
        assert descriptor.role_counts == {"root_record": 0, "entity_binding": 0, "pack": 0}
        with allocate.retained_destination_families(
            descriptor,
            context.common.request.definition,
            work=PlanWork(tmp_path / "families", _FACTS, lambda _path: None),
        ) as families:
            assert list(families) == []
    finally:
        allocate.cleanup_retained_destination(descriptor)


async def test_original_revision_and_destination_native_identity_bijections_are_required(tmp_path, monkeypatch):
    _context_value, work, output, _views, _seen = await _prepared(tmp_path, monkeypatch)
    try:
        rows = list(work.rows(output.paths["allocation"]))
        duplicate = work.write((*rows, rows[-1]))
        output.paths["allocation"] = duplicate
        with pytest.raises(allocate.FamilyBlockUnavailable, match="identity repeats"):
            allocate._unique_observed(work, output)
    finally:
        work.close()


@pytest.mark.parametrize("block", [False, True])
async def test_actual_closed_retained_producer_stages_exact_records_and_all_lineage(tmp_path, monkeypatch, block):
    from contextlib import ExitStack

    from process.custom_import import family_shard_base_values as retained
    from tests.test_custom_import_family_shard_base_values import _case, _run

    async with _case(tmp_path, monkeypatch, block=block) as (base, plan, _rows, root, children, _calls):
        descriptor = await _run(tmp_path, base, plan)
        context = SimpleNamespace(
            common=SimpleNamespace(request=base.request), retained_input=descriptor, final_plan=plan
        )
        work = _Scratch(PlanWork(tmp_path / "stage", _FACTS, lambda _path: None))
        try:
            paths_by_role = {role: work.path() for role in ("family", "root", "child", "base_family", "original")}
            with ExitStack() as stack:
                outputs_by_role = {
                    role: (path, stack.enter_context(path.open("xb"))) for role, path in paths_by_role.items()
                }
                allocate._stage_retained(context, work, outputs_by_role)
            staged_root_rows = list(work.rows(paths_by_role["root"]))
            staged_child_rows = list(work.rows(paths_by_role["child"]))
            assert len(staged_root_rows) == 1 and len(staged_child_rows) == len(children)
            assert msgpack.unpackb(staged_root_rows[0][1], raw=False)[0:3] == [
                root.revision_id,
                root.pack_id,
                root.source_ordinal,
            ]
            assert staged_root_rows[0][3] and all(staged_row[6] for staged_row in staged_child_rows)
            original_rows = list(work.rows(paths_by_role["original"]))
            assert len(original_rows) == descriptor.counts_by_role["pack"] + descriptor.counts_by_role["origin"]
            assert {msgpack.unpackb(staged_row[4], raw=False)[0] for staged_row in staged_child_rows} == {
                original_record.revision_id for original_record in children
            }
        finally:
            work.close()
            retained.cleanup_retained_input(descriptor)


@pytest.mark.parametrize("damaged", [False, True])
async def test_zero_retained_drains_real_source_and_admission_without_value_reconstruction(
    tmp_path, monkeypatch, damaged
):
    from process.custom_import import family_shard_base_values as retained
    from tests.test_custom_import_family_shard_source_values import _case

    async with _case(tmp_path, monkeypatch) as native:
        descriptor = await retained.build_retained_input(
            None, native.plan, work=retained.RetainedWork(tmp_path / "empty-retained", _FACTS, lambda _path: None)
        )
        common = SimpleNamespace(role_counts={}, batches=lambda **_bounds: (_unused for _unused in ()))
        admission = replace(native.admission, sha256=b"x" * 32) if damaged else native.admission
        context = SimpleNamespace(
            common=native.context,
            common_output=common,
            final_plan=native.plan,
            admission=admission,
            retained_input=descriptor,
        )
        work = _Scratch(PlanWork(tmp_path / "empty-stage", _FACTS, lambda _path: None))

        def forbidden(*_args):
            raise AssertionError("zero retained must not reconstruct SOURCE cost values")

        monkeypatch.setattr(allocate, "_stage_sources", forbidden)
        try:
            if damaged:
                with pytest.raises(ValueError):
                    allocate._stage_inputs(context, work)
            else:
                paths, prefix = allocate._stage_inputs(context, work)
                assert prefix and list(work.rows(paths["family"])) == []
        finally:
            work.close()
            retained.cleanup_retained_input(descriptor)


async def test_final_live_authority_refusal_removes_even_atomic_completed_assignment(tmp_path, monkeypatch):
    from contextlib import asynccontextmanager

    context = _context()
    context.final_plan = SimpleNamespace(counts_by_kind={"retained": 0})
    context.retained_input = SimpleNamespace(counts_by_role={"pack": 0, "origin": 0})
    registered_paths = []
    monkeypatch.setattr(allocate, "_scope", lambda _context, _work: ())

    def inputs(_context, work):
        return {"original": work.write(())}, ((11, 0), (23, 0))

    monkeypatch.setattr(allocate, "_stage_inputs", inputs)

    @asynccontextmanager
    async def native_boundary(*_args):
        yield None, None

    monkeypatch.setattr(allocate, "_page_session", native_boundary)

    async def refuse(*_args):
        path = tmp_path / "assignments" / allocate._FILE
        assert path.is_file() and path.stat().st_mode & 0o777 == 0o400
        raise allocate.FamilyBlockUnavailable("final owner revoked")

    monkeypatch.setattr(allocate, "_guard", refuse)
    with pytest.raises(allocate.FamilyBlockUnavailable, match="final owner revoked"):
        await _produce_destination(
            None,
            context,
            work=allocate.RetainedAllocationWork(tmp_path / "assignments", _LIMITS, registered_paths.append),
        )
    assert registered_paths == [tmp_path / "assignments"]
    assert not (tmp_path / "assignments").exists()


@pytest.mark.parametrize(("name", "lexical"), [("amount", "1E+0"), ("stamp", "2026-01-01T01:00:00+01:00")])
def test_compact_destination_identity_restores_exact_original_sparse_wire(name, lexical):
    import json

    from process.custom_import.family_shard_retained import restore_legacy_record
    from process.custom_import.family_shard_retained_output import _destination_ids
    from tests.test_custom_import_family_shard_retained import _case, _legacy

    definition, _root, children = _case(1)
    original = children[0]
    fields = codec.fields_by_collection(definition)["rates"]
    document = json.loads(codec.record_payload(fields, original.values_by_field))
    field = next(field for field in fields if field.field_id == name)
    encoded = next(encoded for encoded in document["fields"] if encoded["field"] == name)
    encoded["value"] = {"type": field.value_type, "state": "value", "value": lexical}
    payload = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    revision, key = _legacy(definition, original, "rates", payload_text=payload)
    scalars_by_field = {"amount": TypedScalar("decimal", "value", decimal_value=Decimal("8.000000000000"))}
    restored = restore_legacy_record(definition, "rates", revision, key=key, scalars=scalars_by_field)
    assert restored.sparse is not None
    destination = replace(restored, revision_id=5001, pack_id=7001, source_ordinal=0)
    identities = _destination_ids(restored, destination)
    before_wire = allocate._wire(definition, "rates", restored, True)
    after_wire = allocate._assigned_wire(before_wire, identities)
    assert after_wire == allocate._wire(definition, "rates", destination, True)
    decoded = allocate._record(definition, "rates", after_wire, retained=True)
    assert decoded.sparse.payload_text == payload and decoded.scalars == restored.scalars
    assert len(msgpack.packb(identities)) < len(after_wire)
    with pytest.raises(allocate.FamilyBlockUnavailable, match="only native identity"):
        _destination_ids(restored, replace(destination, scalars=dict(restored.scalars)))
