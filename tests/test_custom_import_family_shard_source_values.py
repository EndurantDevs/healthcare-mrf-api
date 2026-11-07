# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Original admitted source values, native identities and scalar/hash parity."""

import json
from contextlib import asynccontextmanager, closing
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_admission as audit
from process.custom_import import family_shard_plan as planner
from process.custom_import import family_shard_source_values as source_values
from process.custom_import.bulk_page_codec import LandingRow
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import RootFamily
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_records import FamilyRecord, record_parts
from process.custom_import.family_shard_common import SourceCommonContext
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.runner_codec import new_family_hash
from process.custom_import.runner_types import CancellationRequested
from tests.test_custom_import_family_shard_admission import _build_audit, _inputs
from tests.test_custom_import_family_shard_plan import _assignments, _base, _Headers, _opaque_key, _roots
from tests.test_custom_import_family_shard_source_plan import _child, _root
from tests.test_custom_import_family_shard_worker import _projected_definition

_LIMITS = SourceFactLimits(64 * 1024**2, 1024**2, 512)


def _definition(*, collapse=False, nullable=False, child_first=False, unicode_parent=False):
    document = json.loads(_projected_definition(collapse=collapse).canonical)
    document["refresh_mode"] = "upsert"
    document["schema"]["children"][0]["fields"][2]["nullable"] = nullable
    if child_first:
        document["streams"].reverse()
    if unicode_parent:
        document["schema"]["root"]["logical_key"].append("panel")
        document["schema"]["root"]["fields"].append({"id": "panel", "slot": 7, "type": "string", "nullable": False})
        child = document["schema"]["children"][0]
        child["fields"].append({"id": "detail_panel", "slot": 8, "type": "string", "nullable": False})
        child["parent_key"].append({"child": "detail_panel", "root": "panel"})
    return CustomImportDefinition.from_mapping(document)


@asynccontextmanager
async def _case(tmp_path, monkeypatch, rows_by_stream=None, *, definition=None, base=False):
    async with _inputs(
        tmp_path,
        monkeypatch,
        rows_by_stream
        or {
            "root_source": [[_root()]],
            "detail_source": [[_child(key="z", amount="1.2300"), _child(key="a", amount="2.50")]],
        },
        definition=definition or _definition(),
        complete_scope=False,
    ) as (case, allocated, workers):
        request = case[1]
        admission = _build_audit(tmp_path, case, allocated, workers, _LIMITS, complete_scope=request.complete_scope)
        plan = None
        try:
            headers, base_scope = None, None
            if base:
                root = _roots(admission)[0]
                headers = _Headers(
                    [
                        _base(root.record.typed_parent_key, root=root.root_record_id),
                        _base(_opaque_key("opaque-base"), root=100003, index=2),
                    ]
                )
                request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
                base_scope = 77, 31
            plan = await planner.build_family_plan(
                request,
                admission,
                headers,
                _assignments(admission),
                work=planner.PlanWork(tmp_path / "plan", _LIMITS, lambda path: None),
                base_scope=base_scope,
            )
            context = SourceCommonContext(
                request, FamilyBlockLayout(request.definition, case[2].child_collection_slots), allocated
            )
            yield SimpleNamespace(context=context, admission=admission, plan=plan)
        finally:
            if plan is not None:
                planner.cleanup_family_plan(plan)
            audit.cleanup_admission_output(admission)


def _inputs_for(tmp_path, native, *, check=None, limits=_LIMITS, register=None):
    return source_values.source_family_inputs(
        native.context,
        native.admission,
        native.plan,
        work=planner.PlanWork(tmp_path / "values", limits, register or (lambda path: None), check),
    )


def _children(family):
    return tuple(family.children_by_collection["details"]())


def _assert_original_children(native, children):
    occurrences_by_revision = {
        occurrence.revision_id: occurrence
        for occurrence in audit.read_admission_occurrences(native.admission)
        if occurrence.record.collection_slot
    }
    assert {record.revision_id for record in children} == set(occurrences_by_revision)
    for record in children:
        occurrence = occurrences_by_revision[record.revision_id]
        assert (record.pack_id, record.source_ordinal) == (occurrence.pack_id, occurrence.coordinate[1])
        assert dict(record.values_by_field) == dict(occurrence.record.values)


@pytest.mark.asyncio
@pytest.mark.parametrize("child_first", [False, True])
async def test_source_records_preserve_original_ids_values_scalars_and_family_hash(tmp_path, monkeypatch, child_first):
    async with _case(tmp_path, monkeypatch, definition=_definition(child_first=child_first)) as native:
        observed_slots = []
        original_projection = source_values.project_child_scalars

        def projection(*arguments, **keywords):
            observed_slots.append(keywords["child_target"].collection_slot)
            return original_projection(*arguments, **keywords)

        monkeypatch.setattr(source_values, "project_child_scalars", projection)
        registered_roots = []
        with _inputs_for(tmp_path, native, register=registered_roots.append) as families:
            plan, allocated = next(families)
            children = _children(allocated)
            assert all(
                type(family_record) is FamilyRecord and family_record.sparse is None
                for family_record in (allocated.root, *children)
            )
            assert (allocated.family_revision_id, allocated.entity_binding_id) == plan.source_metadata
            assert allocated.root_record_id == plan.source_root.root_record_id
            assert (allocated.root.revision_id, allocated.root.pack_id, allocated.root.source_ordinal) == (
                plan.source_root.revision_id,
                plan.source_root.pack_id,
                plan.source_root.coordinate[1],
            )
            _assert_original_children(native, children)
            assert {family_record.values_by_field["amount"] for family_record in children} == {"1.2300", "2.50"}
            assert {family_record.scalars["amount"].decimal_value for family_record in children} == {
                Decimal("1.23"),
                Decimal("2.5"),
            }
            assert (
                allocated.root.scalars["score"].integer_value == 7
                and allocated.root.scalars["enabled"].boolean_value is True
            )
            orders = [
                record_parts(native.context.request.definition, "details", family_record, retained=False)[1]
                for family_record in children
            ]
            assert orders == sorted(orders)
            eager = RootFamily(
                ("1003000126",),
                _root(),
                {"details": (_child(key="z", amount="1.2300"), _child(key="a", amount="2.50"))},
            )
            assert allocated.expected_family_sha256 == new_family_hash(native.context.request.definition, eager)
            assert children == _children(allocated) and set(observed_slots) == {7}
            with pytest.raises(TypeError):
                allocated.root.values_by_field["score"] = 9
            assert next(families, None) is None
        assert registered_roots == [tmp_path / "values"] and not (tmp_path / "values").exists()
        assert Path(native.admission.root).exists() and Path(native.plan.root).exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("collapse", [False, True])
async def test_duplicate_selection_preserves_audit_and_last_identical_child(tmp_path, monkeypatch, collapse):
    child = _child(amount="1.00")
    rows_by_stream = {"root_source": [[_root()]], "detail_source": [[child, child]]}
    async with _case(tmp_path, monkeypatch, rows_by_stream, definition=_definition(collapse=collapse)) as native:
        originals = list(audit.read_admission_occurrences(native.admission))
        with _inputs_for(tmp_path, native) as families:
            if collapse:
                _plan, allocated = next(families)
                (selected,) = _children(allocated)
                original = originals[-1]
                assert selected.revision_id == original.revision_id and selected.source_ordinal == 1
                assert selected.revision_id != originals[-2].revision_id
                assert allocated.expected_family_sha256 == new_family_hash(
                    native.context.request.definition, RootFamily(("1003000126",), _root(), {"details": (child,)})
                )
            assert list(families) == []
        assert len(list(audit.read_admission_occurrences(native.admission))) == len(originals) == 3


@pytest.mark.asyncio
async def test_retained_and_absent_plans_never_fall_back_to_source_values(tmp_path, monkeypatch):
    rows_by_stream = {
        "root_source": [[_root(), _root("1234567893")]],
        "detail_source": [[_child(), _child(), _child("1234567893", "new")]],
    }
    async with _case(tmp_path, monkeypatch, rows_by_stream, base=True) as native:
        selections = tuple(planner.read_family_plan(native.plan))
        assert sorted(plan.selection_kind for plan in selections) == ["retained", "retained", "source"]
        with _inputs_for(tmp_path, native) as families:
            plan, allocated = next(families)
            assert plan.selection_kind == "source" and allocated.root.values_by_field["npi"] == "1234567893"
            assert len(_children(allocated)) == 1 and next(families, None) is None


@pytest.mark.asyncio
async def test_nullable_projection_and_borrowed_factory_lifetime(tmp_path, monkeypatch):
    rows_by_stream = {
        "root_source": [[_root(), _root("1234567893")]],
        "detail_source": [[_child(amount=None), _child("1234567893", "other", "3.0")]],
    }
    async with _case(tmp_path, monkeypatch, rows_by_stream, definition=_definition(nullable=True)) as native:
        with _inputs_for(tmp_path, native) as families:
            _first_plan, first = next(families)
            factory = first.children_by_collection["details"]
            reader = factory()
            first_record = next(reader)
            evidence = factory.original_occurrences()
            assert next(evidence).revision_id == first_record.revision_id
            _second_plan, second = next(families)
            with pytest.raises(ValueError, match="no longer active"):
                next(reader)
            with pytest.raises(ValueError, match="no longer active"):
                tuple(factory())
            with pytest.raises(ValueError, match="no longer active"):
                next(evidence)
            second_children = _children(second)
            assert len(second_children) == 1
            assert {first_record.scalars["amount"].value_state, second_children[0].scalars["amount"].value_state} == {
                "null",
                "value",
            }
        assert not (tmp_path / "values").exists()


@pytest.mark.asyncio
async def test_selected_original_evidence_preserves_raw_utf8_and_last_native_identity(tmp_path, monkeypatch):
    root = _root() | {"panel": "éλ"}
    child = _child(amount="1.2300") | {"detail_panel": "éλ"}
    rows_by_stream = {"root_source": [[root]], "detail_source": [[child, child]]}
    async with _case(
        tmp_path, monkeypatch, rows_by_stream, definition=_definition(collapse=True, unicode_parent=True)
    ) as native:
        original_occurrences = tuple(audit.read_admission_occurrences(native.admission))
        raw_index = LandingRow._fields[1:].index("raw_parent_key_canonical")
        with _inputs_for(tmp_path, native) as families:
            plan, allocated = next(families)
            inputs = allocated.children_by_collection["details"]
            (family_record,) = inputs()
            (occurrence,) = inputs.original_occurrences()
            assert occurrence == original_occurrences[-1] and occurrence != original_occurrences[-2]
            assert (occurrence.revision_id, occurrence.pack_id, occurrence.coordinate[1]) == (
                family_record.revision_id,
                family_record.pack_id,
                family_record.source_ordinal,
            )
            assert occurrence.collection_slot == 7 and occurrence.record.collection_slot == 1
            raw_parent = occurrence.landing[raw_index]
            assert raw_parent == occurrence.record.raw_parent_key[0] == original_occurrences[-1].landing[raw_index]
            assert len(raw_parent.encode("utf-8")) > len(raw_parent)
            assert raw_parent != plan.typed_key[0]
            assert tuple(inputs.original_occurrences()) == (occurrence,)
        assert len(tuple(audit.read_admission_occurrences(native.admission))) == len(original_occurrences) == 3


@pytest.mark.asyncio
async def test_empty_original_inputs_exhaust_closed_artifacts_and_cleanup(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, {"root_source": [[]], "detail_source": [[]]}) as native:
        checks = []
        with _inputs_for(tmp_path, native, check=lambda: checks.append(True)) as families:
            assert list(families) == []
        assert checks and not (tmp_path / "values").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["allocation", "plan", "complete_scope"])
async def test_scope_mismatch_refuses_before_allocating_work(tmp_path, monkeypatch, changed):
    async with _case(tmp_path, monkeypatch) as native:
        if changed == "allocation":
            native.admission = replace(native.admission, allocation_sha256=b"x" * 32)
        elif changed == "plan":
            native.plan = replace(native.plan, admission_sha256=b"x" * 32)
        else:
            native.context = SourceCommonContext(
                replace(native.context.request, complete_scope=True), native.context.layout, native.context.source
            )
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            with _inputs_for(tmp_path, native) as families:
                list(families)
        assert not (tmp_path / "values").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["root_id", "metadata", "native_slot"])
async def test_original_source_identity_mismatches_never_create_inputs(tmp_path, monkeypatch, changed):
    async with _case(tmp_path, monkeypatch) as native:
        original_plans, original_occurrences = source_values.read_family_plan, source_values.read_admission_occurrences

        def plans(*arguments, **keywords):
            for plan in original_plans(*arguments, **keywords):
                if changed == "root_id":
                    plan = replace(plan, source_root=replace(plan.source_root, root_record_id=plan.root_record_id + 1))
                elif changed == "metadata":
                    plan = replace(plan, source_metadata=(plan.source_metadata[0] + 1, plan.source_metadata[1]))
                yield plan

        def occurrences(*arguments, **keywords):
            for occurrence in original_occurrences(*arguments, **keywords):
                if changed == "native_slot" and occurrence.record.collection_slot:
                    occurrence = replace(occurrence, collection_slot=1)
                yield occurrence

        monkeypatch.setattr(source_values, "read_family_plan", plans)
        monkeypatch.setattr(source_values, "read_admission_occurrences", occurrences)
        with pytest.raises(ValueError):
            with _inputs_for(tmp_path, native) as families:
                next(families)
        assert not (tmp_path / "values").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["admission", "plan", "budget", "late_projection", "cancel"])
async def test_late_failures_clean_exact_work_and_preserve_original_artifacts(tmp_path, monkeypatch, changed):
    async with _case(tmp_path, monkeypatch) as native:
        if changed in {"admission", "plan"}:
            descriptor, name = (
                (native.admission, "admission.mpk") if changed == "admission" else (native.plan, "family-plan.mpk")
            )
            path = Path(descriptor.root) / name
            original_bytes = path.read_bytes()
            path.chmod(0o600)
            path.write_bytes(original_bytes[:-1] + bytes([original_bytes[-1] ^ 1]))
            path.chmod(0o400)
        original_projection, projected = source_values.project_child_scalars, []

        def projection(*arguments, **keywords):
            projected.append(True)
            if len(projected) == 2:
                raise RuntimeError("late scalar projection failed")
            return original_projection(*arguments, **keywords)

        if changed == "late_projection":
            monkeypatch.setattr(source_values, "project_child_scalars", projection)
        cancellation_requests = []

        def check():
            if cancellation_requests:
                raise CancellationRequested("source input cancelled")

        limits = SourceFactLimits(128, 128, 128) if changed == "budget" else _LIMITS
        sibling = tmp_path / "preserve"
        sibling.write_text("keep")
        with pytest.raises((ValueError, RuntimeError, SourceFactBudgetExceeded, CancellationRequested)):
            with _inputs_for(tmp_path, native, check=check, limits=limits) as families:
                _plan, allocated = next(families)
                if changed == "cancel":
                    cancellation_requests.append(True)
                _children(allocated)
        assert not (tmp_path / "values").exists() and sibling.read_text() == "keep"
        assert Path(native.admission.root).exists() and Path(native.plan.root).exists()


def _prepared_for(tmp_path, native, check=None):
    return source_values.prepared_source_family_inputs(
        native.context,
        native.admission,
        native.plan,
        work=planner.PlanWork(tmp_path / "values", _LIMITS, lambda _path: None, check),
    )


def _prepared_rows(prepared):
    with closing(prepared.families()) as families:
        return tuple(
            (
                plan,
                family.family_revision_id,
                family.root_record_id,
                family.entity_binding_id,
                family.root,
                _children(family),
                family.expected_family_sha256,
            )
            for plan, family in families
        )


def _prepared_files(prepared):
    paths = (prepared.selected, prepared.occurrences, *(path for path, _stamp in prepared.inputs))
    return {str(path): (source_values._file_stamp(path), path.read_bytes()) for path in paths}


def _replay_work(tmp_path, prepared, *, check=None, register=None):
    return planner.PlanWork(
        tmp_path / "source-replay",
        prepared.policy.limits,
        register or (lambda _path: None),
        check or prepared.policy.check,
    )


def _change_prepared_file(prepared, descriptor_cells, index, changed):
    role, name, stamp, digest = descriptor_cells[index]
    path = prepared.work.root / name
    if changed == "hash":
        descriptor_cells[index] = role, name, stamp, b"x" * 32
    elif changed == "descriptor_inode":
        descriptor_cells[index] = role, name, (stamp[0], stamp[1] + 1, *stamp[2:]), digest
    elif changed == "physical_inode":
        replacement = path.with_name("replacement")
        replacement.write_bytes(path.read_bytes())
        replacement.chmod(0o400)
        replacement.replace(path)
    else:
        contents = path.read_bytes()
        path.chmod(0o600)
        path.write_bytes(contents[:-1] + bytes((contents[-1] ^ 1,)))
        path.chmod(0o400)


def _read_cancelled_prepared_source(native, descriptor, work, replay_work, cancelled):
    reopened = source_values.reopen_prepared_source(
        native.context, native.admission, native.plan, descriptor, work=work, replay_work=replay_work
    )
    try:
        with closing(reopened.families()) as families:
            _plan, family = next(families)
            cancelled.append(True)
            _children(family)
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_reopened_prepared_source_replays_original_files_without_repreparation(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared_for(tmp_path, native) as original:
            descriptor = source_values.prepared_source_descriptor(original)
            expected = _prepared_rows(original)
            before = _prepared_files(original)

            def forbidden(*_arguments, **_keywords):
                pytest.fail("reopening reconstructed a completed SOURCE producer")

            monkeypatch.setattr(source_values.PreparedSourceFamilies, "__init__", forbidden)
            registered_roots = []
            replay_work = _replay_work(tmp_path, original, register=registered_roots.append)
            reopened = source_values.reopen_prepared_source(
                native.context,
                native.admission,
                native.plan,
                descriptor,
                work=original.policy,
                replay_work=replay_work,
            )
            try:
                assert _prepared_rows(reopened) == expected
                assert source_values.prepared_source_descriptor(reopened) == descriptor
                assert reopened.selected == original.selected and reopened.occurrences == original.occurrences
                assert _prepared_files(reopened) == before
            finally:
                reopened.close()
            assert registered_roots == [replay_work.root] and not replay_work.root.exists()
            assert _prepared_files(original) == before and original.work.root.is_dir()
        assert not (tmp_path / "values").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ("live", "closed"))
async def test_prepared_source_descriptor_refuses_closed_or_borrowed_owner(tmp_path, monkeypatch, state):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared_for(tmp_path, native) as prepared:
            reader = prepared.families() if state == "live" else None
            if state == "closed":
                prepared.close()
            try:
                with pytest.raises(ValueError, match="idle original|closed"):
                    source_values.prepared_source_descriptor(prepared)
            finally:
                if reader is not None:
                    reader.close()
            if state == "live":
                assert prepared._active is None and prepared.work.root.is_dir()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ("descriptor", "file"))
async def test_prepared_source_reopen_refuses_changed_original_admission(tmp_path, monkeypatch, changed):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared_for(tmp_path, native) as prepared:
            descriptor = source_values.prepared_source_descriptor(prepared)
            admission = native.admission
            if changed == "descriptor":
                admission = replace(admission, sha256=b"x" * 32)
            else:
                path = Path(admission.root) / "admission.mpk"
                contents = path.read_bytes()
                path.chmod(0o600)
                path.write_bytes(contents[:-1] + bytes((contents[-1] ^ 1,)))
                path.chmod(0o400)
            before = _prepared_files(prepared)
            replay_work = _replay_work(tmp_path, prepared)
            with pytest.raises(ValueError):
                source_values.reopen_prepared_source(
                    native.context,
                    admission,
                    native.plan,
                    descriptor,
                    work=prepared.policy,
                    replay_work=replay_work,
                )
            assert not replay_work.root.exists()
            assert _prepared_files(prepared) == before and prepared.work.root.is_dir()


@pytest.mark.asyncio
@pytest.mark.parametrize("index", (5, 6), ids=("selected", "occurrences"))
@pytest.mark.parametrize("changed", ("hash", "descriptor_inode", "physical_inode", "bytes"))
async def test_prepared_source_reopen_refuses_changed_staged_file_custody(tmp_path, monkeypatch, index, changed):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared_for(tmp_path, native) as prepared:
            descriptor_cells = list(source_values.prepared_source_descriptor(prepared))
            _change_prepared_file(prepared, descriptor_cells, index, changed)
            before = _prepared_files(prepared)
            replay_work = _replay_work(tmp_path, prepared)
            with pytest.raises(ValueError):
                source_values.reopen_prepared_source(
                    native.context,
                    native.admission,
                    native.plan,
                    tuple(descriptor_cells),
                    work=prepared.policy,
                    replay_work=replay_work,
                )
            assert not replay_work.root.exists()
            assert _prepared_files(prepared) == before and prepared.work.root.is_dir()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ("extra_file", "filename", "swapped"))
async def test_prepared_source_reopen_requires_exact_file_roles_and_census(tmp_path, monkeypatch, changed):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared_for(tmp_path, native) as prepared:
            descriptor_cells = list(source_values.prepared_source_descriptor(prepared))
            extra = prepared.work.root / "unexpected"
            if changed == "extra_file":
                extra.write_bytes(b"unregistered")
            elif changed == "filename":
                descriptor_cells[5] = descriptor_cells[5][0], "../elsewhere", *descriptor_cells[5][2:]
            else:
                descriptor_cells[5], descriptor_cells[6] = descriptor_cells[6], descriptor_cells[5]
            before = _prepared_files(prepared)
            reopened = None
            replay_work = _replay_work(tmp_path, prepared)
            try:
                with pytest.raises(ValueError):
                    reopened = source_values.reopen_prepared_source(
                        native.context,
                        native.admission,
                        native.plan,
                        tuple(descriptor_cells),
                        work=prepared.policy,
                        replay_work=replay_work,
                    )
                assert _prepared_files(prepared) == before
            finally:
                if reopened is not None:
                    reopened.close()
                if extra.exists():
                    extra.unlink()
            assert not replay_work.root.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("ceiling", ("max_spool_bytes", "max_record_bytes", "memory_bytes"))
async def test_prepared_source_reopen_never_enlarges_original_replay_limits(tmp_path, monkeypatch, ceiling):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared_for(tmp_path, native) as prepared:
            descriptor = source_values.prepared_source_descriptor(prepared)
            limits = replace(prepared.policy.limits, **{ceiling: getattr(prepared.policy.limits, ceiling) + 1})
            replay_work = replace(_replay_work(tmp_path, prepared), limits=limits)
            before = _prepared_files(prepared)
            reopened = None
            try:
                with pytest.raises(ValueError):
                    reopened = source_values.reopen_prepared_source(
                        native.context,
                        native.admission,
                        native.plan,
                        descriptor,
                        work=prepared.policy,
                        replay_work=replay_work,
                    )
            finally:
                if reopened is not None:
                    reopened.close()
            assert not replay_work.root.exists() and _prepared_files(prepared) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ("work", "replay_work"))
async def test_prepared_source_reopen_requires_typed_original_and_replay_work(tmp_path, monkeypatch, invalid):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared_for(tmp_path, native) as prepared:
            descriptor = source_values.prepared_source_descriptor(prepared)
            replay_work = _replay_work(tmp_path, prepared)
            arguments_by_name = {"work": prepared.policy, "replay_work": replay_work}
            arguments_by_name[invalid] = SimpleNamespace(**vars(arguments_by_name[invalid]))
            before = _prepared_files(prepared)
            with pytest.raises(ValueError):
                source_values.reopen_prepared_source(
                    native.context, native.admission, native.plan, descriptor, **arguments_by_name
                )
            assert not replay_work.root.exists() and _prepared_files(prepared) == before


@pytest.mark.asyncio
async def test_prepared_source_reopen_keeps_original_deadline_without_optional_checks(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared_for(tmp_path, native) as prepared:
            descriptor = source_values.prepared_source_descriptor(prepared)
            replay_work = _replay_work(tmp_path, prepared)
            before = _prepared_files(prepared)

            def elapsed(_deadline):
                raise TimeoutError("original SOURCE deadline elapsed")

            monkeypatch.setattr(source_values, "_require_deadline", elapsed)
            assert prepared.policy.check is None and replay_work.check is None
            with pytest.raises(TimeoutError, match="original SOURCE deadline"):
                source_values.reopen_prepared_source(
                    native.context,
                    native.admission,
                    native.plan,
                    descriptor,
                    work=prepared.policy,
                    replay_work=replay_work,
                )
            assert not replay_work.root.exists() and _prepared_files(prepared) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ("reopen", "replay"))
async def test_reopened_prepared_source_cancellation_drains_readers_and_preserves_files(
    tmp_path, monkeypatch, boundary
):
    async with _case(tmp_path, monkeypatch) as native:
        with _prepared_for(tmp_path, native) as prepared:
            descriptor = source_values.prepared_source_descriptor(prepared)
            before = _prepared_files(prepared)
            cancelled, opened = [], []
            fdopen = source_values.os.fdopen

            def tracked(*arguments, **keywords):
                stream = fdopen(*arguments, **keywords)
                opened.append(stream)
                if boundary == "reopen":
                    cancelled.append(True)
                return stream

            def check():
                if cancelled:
                    raise CancellationRequested("borrowed SOURCE cancelled")

            work = replace(prepared.policy, check=check)
            replay_work = _replay_work(tmp_path, prepared, check=check)
            monkeypatch.setattr(source_values.os, "fdopen", tracked)
            with pytest.raises(CancellationRequested, match="borrowed SOURCE"):
                _read_cancelled_prepared_source(native, descriptor, work, replay_work, cancelled)
            assert opened and all(stream.closed for stream in opened)
            assert not replay_work.root.exists()
            assert _prepared_files(prepared) == before and prepared.work.root.is_dir()
