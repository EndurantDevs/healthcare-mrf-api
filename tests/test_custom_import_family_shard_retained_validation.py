# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Independent retained artifacts; only existing native transport is faked."""

import asyncio
import json
import threading
from contextlib import closing
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import msgpack
import pytest

from process.custom_import import family_shard_retained_validation as validation
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_output import _pack_row, _unpack_row
from process.custom_import.materialization import TypedScalar
from tests import test_custom_import_family_shard_base_values as base_fixture
from tests import test_custom_import_family_shard_retained as lexical_fixture
from tests import test_custom_import_family_shard_retained_allocate as allocation_fixture
from tests import test_custom_import_family_shard_retained_homes as fixture


def _work(tmp_path, case, *, memory=None, check=None):
    limits = fixture._LIMITS
    if memory is not None:
        limits = replace(limits, spool=replace(limits.spool, memory_bytes=memory))
    return fixture.allocation.RetainedAllocationWork(tmp_path / "comparison", limits, case.registered.append, check)


async def _verify(tmp_path, case, **options):
    return await validation.verify_retained_destination(
        case.context, case.destination, work=_work(tmp_path, case, **options)
    )


@pytest.mark.parametrize("empty,memory", ((False, None), (True, None), (False, 4096)))
async def test_original_equality_and_complete_pack_membership(tmp_path, monkeypatch, empty, memory):
    async with fixture._case(tmp_path, monkeypatch, empty=empty) as case:
        result = await _verify(tmp_path, case, memory=memory)
        assert dict(result.counts) == case.context.retained_input.counts_by_role
        assert result.destination_sha256 == case.destination.sha256
        assert (result.root_scalar_count, result.child_scalar_count) == ((0, 0) if empty else (1, 2))
        assert result.scope == fixture.allocation._scope(case.context, _work(tmp_path, case))
        assert result.unproven_boundaries == ("native_output", "stored_output_families", "winner_contexts")
        with pytest.raises(FrozenInstanceError):
            result.counts = ()
        assert not (tmp_path / "comparison").exists()


async def test_destination_comparison_does_not_reconstruct_payload_values(tmp_path, monkeypatch):
    async with fixture._case(tmp_path, monkeypatch) as case:
        original, checked = validation.record_from_wire, []

        def record_from_wire(*args, **kwargs):
            checked.append(args[2][0])
            return original(*args, **kwargs)

        monkeypatch.setattr(validation, "record_from_wire", record_from_wire)
        assert dict((await _verify(tmp_path, case)).counts)["child"] == 2
        assert len(checked) == len(set(checked)) == 3


def _changed_record(case, entries, damage):
    index = next(i for i, entry in enumerate(entries) if entry[0] == "record" and entry[2] != 0)
    entry = entries[index]
    before = fixture.allocation._record(case.common.request.definition, "details", entry[4], retained=True)
    changes_by_damage = {
        "value": dict(values_by_field=dict(before.values_by_field, amount=Decimal("7.5"))),
        "scalar": dict(scalars={"amount": TypedScalar("decimal", "value", decimal_value=Decimal("9"))}),
        "null_scalar": dict(scalars={"amount": TypedScalar("decimal", "null")}),
        "old_ordinal": dict(source_ordinal=before.source_ordinal + 1),
        "old_pack": dict(pack_id=before.pack_id + 10),
    }
    before = replace(before, **changes_by_damage.get(damage, {}))
    if damage == "lineage":
        lineage, declarations = _unpack_row(entry[6])
        entries[index] = (*entry[:6], _pack_row((dict(lineage, root_record_id=999), declarations)))
        return
    if damage == "declaration":
        lineage, declarations = _unpack_row(entry[6])
        entries[index] = (*entry[:6], _pack_row((lineage, ((*declarations[0][:1], 19, *declarations[0][2:]),))))
        return
    wire = fixture.allocation._wire(case.common.request.definition, "details", before, True)
    order = validation.record_parts(case.common.request.definition, "details", before, retained=True)[1]
    entries[index] = (*entry[:3], order, wire, *entry[5:])


@pytest.mark.parametrize(
    "damage", ("value", "scalar", "null_scalar", "old_ordinal", "old_pack", "lineage", "declaration")
)
async def test_resealed_destination_original_is_compared_to_independent_input(tmp_path, monkeypatch, damage):
    async with fixture._case(tmp_path, monkeypatch) as case:
        case.destination = fixture._rewrite(case.destination, lambda entries: _changed_record(case, entries, damage))
        with pytest.raises(FamilyBlockUnavailable, match="semantic evidence"):
            await _verify(tmp_path, case)
        assert not (tmp_path / "comparison").exists()


def _changed_pack(case, entries, damage):
    index = next(i for i, entry in enumerate(entries) if entry[0:2] == ("copy", "pack"))
    kind, role, packed = entries[index]
    columns = case.common.columns_by_role["pack"]
    native_by_name = dict(zip(columns, _unpack_row(packed), strict=True))
    changes_by_damage = {
        "count": {"record_count": native_by_name["record_count"] + 1},
        "hash": {"pack_sha256": b"x" * 32},
        "stream": {"stream_slot": 2},
        "ordinal": {"pack_ordinal": native_by_name["pack_ordinal"] + 1},
        "producer": {"producing_fence": native_by_name["producing_fence"] + 1},
        "capture": {"capture_bundle_id": native_by_name["capture_bundle_id"] + 1},
    }
    native_by_name.update(changes_by_damage[damage])
    entries[index] = kind, role, _pack_row(tuple(native_by_name[name] for name in columns))


@pytest.mark.parametrize("damage", ("count", "hash", "stream", "ordinal", "producer", "capture"))
async def test_allocated_pack_id_does_not_prove_copy_pack_content(tmp_path, monkeypatch, damage):
    async with fixture._case(tmp_path, monkeypatch) as case:
        case.destination = fixture._rewrite(case.destination, lambda entries: _changed_pack(case, entries, damage))
        with pytest.raises(FamilyBlockUnavailable, match="pack"):
            await _verify(tmp_path, case)


@pytest.mark.parametrize("root", (False, True))
async def test_destination_ordinals_remain_pack_local(tmp_path, monkeypatch, root):
    async with fixture._case(tmp_path, monkeypatch) as case:

        def change(entries):
            index = next(i for i, entry in enumerate(entries) if entry[0] == "record" and (entry[2] == 0) == root)
            entry = entries[index]
            identities = (*entry[5][:2], entry[5][2] + 1)
            entries[index] = (*entry[:5], identities, entry[6])

        case.destination = fixture._rewrite(case.destination, change)
        with pytest.raises(FamilyBlockUnavailable, match="ordinal|position"):
            await _verify(tmp_path, case)


@pytest.mark.parametrize("damage", ("family", "plan", "original_pack", "accepted"))
async def test_family_plan_provenance_and_allocations_are_independent(tmp_path, monkeypatch, damage):
    async with fixture._case(tmp_path, monkeypatch) as case:

        def change(entries):
            if damage in {"family", "plan"}:
                index = next(i for i, entry in enumerate(entries) if entry[0] == "family")
                cells = list(entries[index])
                cells[6 if damage == "family" else 2] += 1
                entries[index] = tuple(cells)
            elif damage == "original_pack":
                index = next(i for i, entry in enumerate(entries) if entry[0:2] == ("original", "pack"))
                entry = entries[index]
                native_by_name = dict(_unpack_row(entry[2]), pack_pack_sha256=b"x" * 32)
                entries[index] = (*entry[:2], _pack_row(native_by_name))
            else:
                index = next(i for i, entry in enumerate(entries) if entry[0] == "allocation" and entry[-1])
                entries[index] = (*entries[index][:-1], False)

        case.destination = fixture._rewrite(case.destination, change)
        with pytest.raises(FamilyBlockUnavailable):
            await _verify(tmp_path, case)


async def test_opaque_sparse_payload_and_independent_scalars_stay_exact(tmp_path, monkeypatch):
    original_rows = base_fixture._legacy_rows

    def opaque_rows(base, header, root, children):
        return original_rows(base, header, root, children, opaque=True)

    monkeypatch.setattr(base_fixture, "_legacy_rows", opaque_rows)
    async with fixture._case(tmp_path, monkeypatch) as case:
        result = await _verify(tmp_path, case)
        assert dict(result.counts)["root"] == 1 and dict(result.counts)["child"] == 2


async def test_current_block_originals_use_the_same_independent_comparison(tmp_path, monkeypatch):
    def block_rows(base, header, root, children):
        return base_fixture._block_rows(tmp_path, base, header, root, children)

    monkeypatch.setattr(base_fixture, "_legacy_rows", block_rows)
    async with fixture._case(tmp_path, monkeypatch) as case:
        result = await _verify(tmp_path, case)
        assert dict(result.counts)["family"] == 1 and case.context.base.binding is not None


def test_native_scalar_variants_preserve_exact_payload_bytes():
    definition, _root, children = lexical_fixture._case(1)
    original = children[0]
    scalars_by_field = {
        "amount": TypedScalar("decimal", "value", decimal_value=Decimal("8.00")),
        "stamp": TypedScalar("timestamp", "value", timestamp_value=datetime(2030, 1, 1, tzinfo=timezone.utc)),
    }
    before = replace(original, scalars=scalars_by_field)
    after = replace(
        original,
        scalars=dict(
            scalars_by_field,
            amount=TypedScalar("decimal", "value", decimal_value=Decimal("8.000000000000")),
            stamp=TypedScalar(
                "timestamp", "value", timestamp_value=datetime(2030, 1, 1, 1, tzinfo=timezone(timedelta(hours=1)))
            ),
        ),
    )
    evidence, wires = [], []
    for record in (before, after):
        wire = fixture.allocation._wire(definition, "rates", record, True)
        wires.append(wire)
        evidence.append(validation._record_evidence(definition, "rates", wire, None)[2])
    assert wires[0] != wires[1]
    assert validation._equal_native(*evidence)


def _assigned_record(definition, before, after, monkeypatch):
    context = SimpleNamespace(
        common=SimpleNamespace(
            request=SimpleNamespace(definition=definition), layout=SimpleNamespace(collection_slots={"rates": 1})
        )
    )
    order = (b"order", "family")
    parts = validation.record_parts(definition, "rates", before, retained=True)[1]
    wires = tuple(fixture.allocation._wire(definition, "rates", record, True) for record in (before, after))

    def forbid(*_args, **_kwargs):
        pytest.fail("destination mapping must reuse the independent original payload validation")

    monkeypatch.setattr(validation, "record_from_wire", forbid)
    monkeypatch.setattr(validation.allocation, "_record", forbid)
    entry = ("record", order, 1, parts, *wires, None)
    return validation._assigned_record(context, entry, ("family", order), None)


def test_assigned_outer_cells_keep_typed_scalar_variants(monkeypatch):
    definition, _root, children = lexical_fixture._case(1)
    before = replace(
        children[0],
        scalars={
            "amount": TypedScalar("decimal", "value", decimal_value=Decimal("8.00")),
            "stamp": TypedScalar("timestamp", "value", timestamp_value=datetime(2030, 1, 1, tzinfo=timezone.utc)),
        },
    )
    after = replace(
        before,
        revision_id=before.revision_id + 100,
        pack_id=before.pack_id + 100,
        source_ordinal=0,
        scalars={
            "amount": TypedScalar("decimal", "value", decimal_value=Decimal("8.000000000000")),
            "stamp": TypedScalar(
                "timestamp", "value", timestamp_value=datetime(2030, 1, 1, 1, tzinfo=timezone(timedelta(hours=1)))
            ),
        },
    )
    old_ids, assigned_ids, _evidence, _order = _assigned_record(definition, before, after, monkeypatch)
    assert old_ids == (before.revision_id, before.pack_id, before.source_ordinal)
    assert assigned_ids == (after.revision_id, after.pack_id, after.source_ordinal)


@pytest.mark.parametrize("damage", ("value", "null", "missing"))
def test_assigned_outer_cells_refuse_changed_scalars(monkeypatch, damage):
    definition, _root, children = lexical_fixture._case(1)
    before = replace(children[0], scalars={"amount": TypedScalar("decimal", "value", decimal_value=Decimal("8"))})
    scalar_by_damage = {
        "value": {"amount": TypedScalar("decimal", "value", decimal_value=Decimal("9"))},
        "null": {"amount": TypedScalar("decimal", "null")},
        "missing": {},
    }
    after = replace(before, revision_id=before.revision_id + 100, scalars=scalar_by_damage[damage])
    with pytest.raises(FamilyBlockUnavailable, match="changes original content"):
        _assigned_record(definition, before, after, monkeypatch)


def test_outer_scalar_encoding_keeps_existing_round_trip_refusal():
    definition, _root, children = lexical_fixture._case(1)
    record = replace(
        children[0],
        scalars={"stamp": TypedScalar("timestamp", "value", timestamp_value=datetime(2030, 1, 1, tzinfo=timezone.utc))},
    )
    cells = msgpack.unpackb(fixture.allocation._wire(definition, "rates", record, True), raw=False)
    cells[4][0][3] = "2030-01-01T00:00:00Z"
    with pytest.raises(FamilyBlockUnavailable, match="scalar encoding"):
        validation._outer_record(definition, "rates", msgpack.packb(cells, use_bin_type=True))


def test_missing_and_null_independent_scalars_are_distinct():
    definition, _root, children = lexical_fixture._case(1)
    original = children[0]
    evidence_rows = []
    for scalars in ({}, {"amount": TypedScalar("decimal", "null")}):
        record = replace(original, scalars=scalars)
        wire = fixture.allocation._wire(definition, "rates", record, True)
        evidence_rows.append(validation._record_evidence(definition, "rates", wire, None)[2])
    assert not validation._equal_native(*evidence_rows)


@pytest.mark.parametrize("name,lexical", (("amount", "1E+0"), ("stamp", "2026-01-01T01:00:00+01:00")))
def test_sparse_decimal_and_timestamp_payload_lexical_evidence_is_not_normalized(name, lexical):
    definition, _root, children = lexical_fixture._case(1)
    original = children[0]
    fields = validation.codec.fields_by_collection(definition)["rates"]
    document = json.loads(validation.codec.record_payload(fields, original.values_by_field))
    field = definition.fields_by_id[name]
    encoded = next(cell for cell in document["fields"] if cell["field"] == name)
    encoded["value"] = {"type": field.value_type, "state": "value", "value": lexical}
    payload = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    native, key = lexical_fixture._legacy(definition, original, "rates", payload_text=payload)
    restored = lexical_fixture.restore_legacy_record(definition, "rates", native, key=key, scalars={})
    wire = fixture.allocation._wire(definition, "rates", restored, True)
    record, _parts, evidence = validation._record_evidence(definition, "rates", wire, None)
    assert record.sparse is not None and evidence[1] == payload
    assert evidence[2] == native["payload_sha256"]


@pytest.mark.parametrize("damage", ("truncated", "trailing", "duplicate"))
async def test_complete_destination_eof_is_required(tmp_path, monkeypatch, damage):
    async with fixture._case(tmp_path, monkeypatch) as case:
        if damage == "duplicate":

            def change(entries):
                entries.append(next(entry for entry in entries if entry[0] == "record"))

            case.destination = fixture._rewrite(case.destination, change)
        else:
            path = Path(case.destination.root) / fixture.allocation._FILE
            path.chmod(0o600)
            payload = path.read_bytes()
            path.write_bytes(payload[:-1] if damage == "truncated" else payload + b"extra")
            path.chmod(0o400)
            case.destination = replace(case.destination, file_bytes=path.stat().st_size)
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _verify(tmp_path, case)
        assert not (tmp_path / "comparison").exists()


async def test_original_footer_failure_never_returns_comparison(tmp_path, monkeypatch):
    async with fixture._case(tmp_path, monkeypatch) as case:
        path = Path(case.context.retained_input.root) / base_fixture.retained._FILE
        original = path.read_bytes()
        path.chmod(0o600)
        path.write_bytes(original[:-1] + bytes([original[-1] ^ 1]))
        path.chmod(0o400)
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            await _verify(tmp_path, case)
        assert not (tmp_path / "comparison").exists()


def test_origin_semantic_equality_checks_every_cell_and_both_eofs(tmp_path):
    original_by_name = dict(family_revision_id=7, kind="child", revision_id=19, ancestor_generation_id=4)
    work = validation._Scratch(validation.PlanWork(tmp_path / "origin-work", fixture._FACTS, lambda _path: None))
    try:
        expected_rows = [validation._provenance("origin", _pack_row(original_by_name))]
        assert validation._compare(work, iter(expected_rows), iter(expected_rows))["origin"] == 1
        different_rows = [validation._provenance("origin", _pack_row(dict(original_by_name, ancestor_generation_id=5)))]
        with pytest.raises(FamilyBlockUnavailable, match="semantic evidence"):
            validation._compare(work, iter(expected_rows), iter(different_rows))
        with pytest.raises(FamilyBlockUnavailable, match="complete membership"):
            validation._compare(work, iter(expected_rows), iter(()))
    finally:
        work.close()


def _append_destination_family(context, output, plan, family, record_entries, number):
    root_entry = next(entry for entry in record_entries if entry[2] == 0)
    definition = context.common.request.definition
    root = fixture.allocation._record(definition, None, root_entry[3], retained=True)
    view = SimpleNamespace(
        root_id=plan.root_record_id,
        source=SimpleNamespace(
            plan=SimpleNamespace(base_family_revision_id=plan.base_header.family_revision_id),
            record=SimpleNamespace(logical_key_sha256=plan.typed_key[1], canonical_logical_key=plan.typed_key[0]),
            root=SimpleNamespace(
                payload_sha256=validation.codec.digest_text(
                    "root-payload", validation.record_parts(definition, None, root, retained=True)[1][2]
                )
            ),
            entity_binding_id=plan.base_header.entity_binding_id,
            family_sha256=plan.base_header.family_sha256,
            child_count=plan.base_header.child_count,
        ),
        original_cells=(
            (None, None, fixture._plan_cells(plan)),
            (None, root_entry[3], None, root_entry[4]),
            (plan.root_record_id, family[2]),
        ),
    )
    family_id, root_id, pack_id = 7000 + number, 10000 + number, 9000 + number * 10
    output.observed(("family_revision", "root_revision", "pack"), (family_id, root_id, pack_id), True)
    output.root(
        view,
        dict(original_root=root, root_revision_id=root_id, family_id=family_id, pack_id=pack_id),
        SimpleNamespace(root_stream_slot=1),
    )
    child_entries = [entry for entry in record_entries if entry[2] != 0]
    assigned_ids = tuple(20000 + number * 10 + index for index in range(len(child_entries)))
    output.observed(("child_revision",) * len(child_entries) + ("pack",), (*assigned_ids, pack_id + 1), True)
    hashes = []
    for ordinal, (entry, assigned_id) in enumerate(zip(child_entries, assigned_ids, strict=True)):
        child = fixture.allocation._record(definition, "details", entry[3], retained=True)
        after = replace(child, revision_id=assigned_id, pack_id=pack_id + 1, source_ordinal=ordinal)
        output.record(view, entry[2], child, after, _unpack_row(entry[4]))
        hashes.append(
            validation.codec.digest_text(
                "child-payload", validation.record_parts(definition, "details", child, retained=True)[1][2]
            )
        )
    output.pack(pack_id + 1, 2, "details", tuple(hashes))


def _multiple_destination(tmp_path, context, registered):
    work = validation._Scratch(validation.PlanWork(tmp_path / "destination", fixture._FACTS, registered.append))
    prefix = fixture.allocation._source_prefix(context, work)
    entries = tuple(fixture.originals.read_retained_input(context.retained_input))
    with fixture.allocation._Output(context, work, prefix) as output:
        with closing(fixture.allocation.read_family_plan(context.final_plan)) as plans:
            for number, plan in enumerate(plans, 1):
                if plan.selection_kind != "retained":
                    continue
                identity = plan.base_header.family_revision_id
                family = next(entry[1] for entry in entries if entry[0] == "family" and entry[1][0] == identity)
                records = [entry for entry in entries if entry[0] == "record" and entry[1] == identity]
                _append_destination_family(context, output, plan, family, records, number)
    original = work.write(entry for entry in entries if entry[0] in {"pack", "origin"})
    owner = fixture.allocation.RetainedAllocationWork(tmp_path / "unused", fixture._LIMITS, registered.append)
    return fixture.allocation._write_complete(
        context, fixture.allocation._scope(context, owner), work, {"original": original}, output
    )


def _second_family_header(first, root, children):
    definition = fixture._definition("upsert")
    key = validation.codec.root_key_document(definition, root.values_by_field)
    family_hash = validation.codec.new_family_hash(
        definition,
        fixture.RootFamily(
            (root.values_by_field["npi"],),
            root.values_by_field,
            {"details": tuple(child.values_by_field for child in children)},
        ),
    )
    return replace(
        first,
        typed_key=(key, validation.codec.digest_text("root-key", key)),
        root_record_id=501,
        family_revision_id=1002,
        root_revision_id=root.revision_id,
        entity_binding_id=3002,
        family_sha256=family_hash,
        child_count=len(children),
    )


@pytest.mark.parametrize("cross_family_pack", (False, True))
async def test_multiple_families_join_original_membership_without_a_corpus_map(
    tmp_path, monkeypatch, cross_family_pack
):
    original_headers, original_rows = fixture._Headers, base_fixture._legacy_rows
    second_root, second_child_records = allocation_fixture._originals(2, 3)
    second_root = replace(second_root, pack_id=201)
    second_child_records = tuple(replace(child, pack_id=202) for child in second_child_records)
    selected_headers = []

    def headers(values):
        first = values[0]
        second = _second_family_header(first, second_root, second_child_records)
        selected_headers.extend((first, second))
        return original_headers(sorted(selected_headers, key=lambda header: (header.typed_key[1], header.typed_key[0])))

    def rows(base, header, root, children):
        native = original_rows(base, header, root, children)
        extra = original_rows(base, selected_headers[1], second_root, second_child_records)
        for role in native:
            native[role].extend(extra[role])
        return native

    monkeypatch.setattr(fixture, "_Headers", headers)
    monkeypatch.setattr(base_fixture, "_legacy_rows", rows)
    monkeypatch.setattr(fixture, "_destination", _multiple_destination)
    async with fixture._case(tmp_path, monkeypatch) as case:
        if cross_family_pack:

            def change(entries):
                first = next(entry for entry in entries if entry[0] == "record" and entry[2] != 0)
                index = next(
                    i
                    for i, entry in enumerate(entries)
                    if entry[0] == "record" and entry[2] != 0 and entry[1] != first[1]
                )
                entry = entries[index]
                identities = entry[5][0], first[5][1], entry[5][2]
                entries[index] = (*entry[:5], identities, entry[6])

            case.destination = fixture._rewrite(case.destination, change)
            with pytest.raises(FamilyBlockUnavailable, match="pack"):
                await _verify(tmp_path, case, memory=4096)
            return
        comparison = await _verify(tmp_path, case, memory=4096)
        assert dict(comparison.counts) == dict(family=2, root=2, child=5, scalar=7, pack=2, origin=0)
        assert (comparison.root_scalar_count, comparison.child_scalar_count) == (2, 5)


@pytest.mark.parametrize("wrong_root", (False, True))
async def test_actual_allocator_family_wrapper_binds_its_root(tmp_path, monkeypatch, wrong_root):
    async with fixture._case(tmp_path, monkeypatch) as case:

        def change(entries):
            index = next(i for i, entry in enumerate(entries) if entry[0] == "family")
            entry = entries[index]
            entries[index] = (*entry[:9], (entry[2] + int(wrong_root), entry[9]))

        case.destination = fixture._rewrite(case.destination, change)
        if wrong_root:
            with pytest.raises(FamilyBlockUnavailable, match="root wrapper"):
                await _verify(tmp_path, case)
        else:
            assert dict((await _verify(tmp_path, case)).counts)["family"] == 1


async def test_changed_base_after_comparison_never_returns_evidence(tmp_path, monkeypatch):
    async with fixture._case(tmp_path, monkeypatch) as case:
        original_check, checks = validation.allocation._check_base, []

        async def check(context):
            checks.append(context)
            await original_check(context)
            if len(checks) == 2:
                raise FamilyBlockUnavailable("synthetic base pin changed")

        monkeypatch.setattr(validation.allocation, "_check_base", check)
        with pytest.raises(FamilyBlockUnavailable, match="base pin changed"):
            await _verify(tmp_path, case)
        assert len(checks) == 2 and not (tmp_path / "comparison").exists()


async def test_cancellation_joins_the_cpu_worker_before_cleanup(tmp_path, monkeypatch):
    async with fixture._case(tmp_path, monkeypatch) as case:
        entered, release = asyncio.Event(), threading.Event()
        loop = asyncio.get_running_loop()
        loop_thread = threading.get_ident()

        def check():
            if threading.get_ident() == loop_thread:
                return
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(5), "test must release the owner check"

        task = asyncio.create_task(_verify(tmp_path, case, check=check))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not (tmp_path / "comparison").exists()
