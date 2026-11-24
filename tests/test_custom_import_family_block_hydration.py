# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact native query boundaries for bounded complete-family hydration pages."""

import asyncio
import struct
from bisect import bisect_left, bisect_right
from dataclasses import replace
from decimal import Decimal
from itertools import islice
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy import select, tuple_
from sqlalchemy.sql import operators
from sqlalchemy.sql.elements import BinaryExpression, BindParameter, Tuple

from process.custom_import import family_block_hydration as hydration
from process.custom_import import family_block_read as reader
from process.custom_import import read_core, runner_codec
from process.custom_import.family_block_codec import FamilyRecord, SparseRecord, encode_retained_family, verify_family
from process.custom_import.family_block_directory import _membership, iter_revision_memberships
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_storage_codec import iter_fragment_directory, stored_manifest
from process.custom_import.materialization import TypedScalar
from process.custom_import.read_payload import full_family_payload
from tests.test_custom_import_family_block_raw_integration import _definition
from tests.test_custom_import_family_block_read import _Rows, _StoredRows
from tests.test_custom_import_family_block_storage import _storage_case


def _evaluate(expression, native_row):
    if isinstance(expression, BindParameter):
        return expression.value
    if isinstance(expression, Tuple):
        return tuple(_evaluate(cell, native_row) for cell in expression.clauses)
    if isinstance(expression, BinaryExpression):
        left, right = _evaluate(expression.left, native_row), _evaluate(expression.right, native_row)
        if expression.operator is operators.in_op:
            return left in right
        if expression.operator is operators.gt:
            return left > right
        if expression.operator is operators.eq:
            return left == right
        raise AssertionError(expression.operator)
    return native_row[expression.name]


class _PageRows:
    """Query-evaluating boundary fake; it grants no native custody or API proof."""

    def __init__(self, count=3, *, children=7, fragment_bytes=65536, sparse=False, case=None):
        stored = _StoredRows(case or _storage_case(count=children, fragment_bytes=fragment_bytes, sparse=sparse))
        self.binding, self.records = stored.binding, stored.records
        self.families, self.members, self.fragments = [], [], []
        self.statements, self.results, self.blob_sizes = [], [], []
        self._indexes = {}
        self.damage = None
        for index in range(count):
            family_id = 9 + index * 11
            self.families.append(
                dict(
                    stored.family, family_revision_id=family_id, root_record_id=23 + index, entity_binding_id=11 + index
                )
            )
            self.members.extend(dict(native_row, family_revision_id=family_id) for native_row in stored.members)
            self.fragments.extend(dict(native_row, family_revision_id=family_id) for native_row in stored.fragments)

    def selected(self):
        return tuple(
            (
                SimpleNamespace(entity_binding_id=family["entity_binding_id"], context_key_sha256=b"0" * 32),
                SimpleNamespace(**family),
                SimpleNamespace(root_revision_id=family["root_revision_id"]),
                None,
            )
            for family in self.families
        )

    def context(self):
        layout = self.binding.layout
        target = read_core.PinnedReadTarget(1, 2, 3, 4, layout.definition.selection_profiles[0].profile_id)
        return read_core._ReadContext(
            target,
            layout.definition,
            1,
            0,
            layout.collection_slots,
            {slot: name for name, slot in layout.collection_slots.items()},
            family_block_binding=self.binding,
        )

    def _starts(self, statement):
        wanted = statement.get_final_froms()[0].left.element._data[0]
        native_rows = []
        for family_id, ordinal in wanted:
            fragments = [
                fragment
                for fragment in self.fragments
                if fragment["family_revision_id"] == family_id
                and fragment["first_record_ordinal"] is not None
                and fragment["first_record_ordinal"] <= ordinal
            ]
            start = max((fragment["fragment_ordinal"] for fragment in fragments), default=None)
            native_rows.append(dict(family_revision_id=family_id, record_ordinal=ordinal, fragment_ordinal=start))
        if self.damage == "start_missing":
            native_rows.pop()
        elif self.damage == "start_family":
            native_rows[-1]["family_revision_id"] += 1
        return native_rows

    def _select(self, statement):
        keys = set(statement.selected_columns.keys())
        if "record_ordinal" in keys:
            return self._starts(statement)
        if "fragment_ordinal" in keys:
            candidates, role, blob = self.fragments, "fragment", "encoded_fragment"
        elif "record_count" in keys:
            candidates, role, blob = self.members, "membership", "encoded_membership"
        else:
            candidates, role, blob = self.families, "family", None
        order_columns = tuple(term.name for term in statement._order_by_clauses)
        index_key = role, order_columns
        if index_key not in self._indexes:
            # Callers finish fixture mutations before the first query; retain duplicate coordinates.
            ordered = sorted(candidates, key=lambda stored_row: tuple(stored_row[name] for name in order_columns))
            self._indexes[index_key] = (
                tuple(tuple(stored_row[name] for name in order_columns) for stored_row in ordered),
                ordered,
            )
        coordinates, ordered = self._indexes[index_key]
        positions = range(len(ordered))
        for criterion in statement._where_criteria:
            if (
                isinstance(criterion.left, Tuple)
                and tuple(cell.name for cell in criterion.left.clauses) == order_columns
            ):
                if criterion.operator is operators.gt:
                    positions = range(bisect_right(coordinates, _evaluate(criterion.right, {})), len(ordered))
                elif criterion.operator is operators.in_op:
                    positions = (
                        index
                        for coordinate in sorted(set(_evaluate(criterion.right, {})))
                        for index in range(bisect_left(coordinates, coordinate), bisect_right(coordinates, coordinate))
                    )
        matching_rows = (
            dict(ordered[index])
            for index in positions
            if all(_evaluate(criterion, ordered[index]) for criterion in statement._where_criteria)
        )
        limit = statement._limit_clause.value if statement._limit_clause is not None else None
        native_rows = list(islice(matching_rows, limit))
        if blob and blob in keys:
            self.blob_sizes.append(sum(len(native_row[blob] or b"") + 256 for native_row in native_rows))
            if self.damage == role + "_drift":
                native_rows[-1]["family_revision_id"] += 1
        return native_rows

    async def execute(self, statement):
        self.statements.append(statement)
        native_rows = self._select(statement)
        result = _Rows(native_rows)
        self.results.append(result)
        return result


def test_page_fake_seeks_coordinates_without_losing_filters_order_or_duplicates(monkeypatch):
    stored = _PageRows(1, children=0)
    template = stored.fragments[0]
    family_id = template["family_revision_id"]
    stored.fragments = [dict(template, fragment_ordinal=index) for index in reversed(range(1000))]
    stored.fragments.append(dict(template, fragment_ordinal=997))
    stored.fragments.pop(1)  # Pre-query changes must be included in the lazy index.
    table = stored.binding.tables_by_role["fragment"]
    columns = table.c.family_revision_id, table.c.fragment_ordinal
    query = select(table).where(table.c.family_revision_id.in_([family_id])).order_by(*columns)
    assert len(stored._select(query)) == 1000
    evaluate = Mock(wraps=_evaluate)
    monkeypatch.setitem(globals(), "_evaluate", evaluate)

    page = stored._select(query.where(tuple_(*columns) > (family_id, 995)).limit(3))
    assert [row["fragment_ordinal"] for row in page] == [996, 997, 997]
    exact = query.where(tuple_(*columns).in_([(family_id, 999), (family_id, 997), (family_id, 997)]))
    assert [row["fragment_ordinal"] for row in stored._select(exact)] == [997, 997, 999]
    assert stored._select(exact.where(table.c.fragment_ordinal > 997)) == [dict(template, fragment_ordinal=999)]
    assert stored._select(exact.where(table.c.family_revision_id.in_([family_id + 1]))) == []
    assert evaluate.call_count < 200


def _damage_membership(stored, damage):
    native = stored.members[-1]
    membership = reader._membership(native)
    changed = _membership(
        99 if damage == "membership_collection" else membership.collection_slot,
        [(revision, 1) for revision, _ordinal in membership.records()],
    )
    native.update(zip(reader._MEMBERSHIP, changed.cells(), strict=True))
    native["size"] = len(changed.encoded_membership or b"")


@pytest.mark.asyncio
@pytest.mark.parametrize("families", [1, 3, 50])
async def test_small_families_share_six_queries_and_preserve_exact_order(families):
    stored = _PageRows(families)
    selected = stored.selected()[::-1]
    actual = await reader.hydrate_families(stored, stored.context(), selected, "scope")
    assert [detail.winner.family_revision_id for detail in actual] == [row[1].family_revision_id for row in selected]
    expected_children = sorted(
        (child.source_ordinal, child.revision_id) for (slot, _id), child in stored.records.items() if slot
    )
    assert all(
        [child.child_revision_id for child in detail.children] == [revision for _, revision in expected_children]
        for detail in actual
    )
    assert all(detail.authorization_scope_sha256 == "scope" for detail in actual)
    assert len(stored.statements) == 6
    assert max(stored.blob_sizes) <= reader.FamilyBlockReadLimits().bytes
    assert all(result.closed for result in stored.results)


@pytest.mark.asyncio
@pytest.mark.parametrize("fragment_bytes", [1, 7, 37, 65536])
@pytest.mark.parametrize("sparse", [False, True])
async def test_native_pages_cross_families_and_frames_without_changing_records(monkeypatch, fragment_bytes, sparse):
    stored = _PageRows(3, children=2, fragment_bytes=fragment_bytes, sparse=sparse)
    limits = reader.FamilyBlockReadLimits(rows=7, bytes=2048, fragment_bytes=fragment_bytes)
    monkeypatch.setattr(reader, "FamilyBlockReadLimits", lambda: limits)
    decoded = Mock(wraps=reader.decode_fragment)
    monkeypatch.setattr(reader, "decode_fragment", decoded)
    actual = await reader.hydrate_families(stored, stored.context(), stored.selected(), "scope")
    assert len(actual) == 3 and all(len(detail.children) == 2 for detail in actual)
    assert decoded.call_count <= len(stored.fragments)
    assert max(stored.blob_sizes) <= limits.bytes
    assert all(result.closed for result in stored.results)


@pytest.mark.asyncio
async def test_repeated_family_keeps_each_original_winner_and_result_order():
    stored = _PageRows(2)
    first, second = stored.selected()
    other_winner = SimpleNamespace(entity_binding_id=first[0].entity_binding_id, context_key_sha256=b"1" * 32)
    selected = (second, first, (other_winner, *first[1:]))
    actual = await reader.hydrate_families(stored, stored.context(), selected, "scope")
    assert [detail.winner.context_key_sha256 for detail in actual] == [b"0" * 32, b"0" * 32, b"1" * 32]
    assert actual[1].children == actual[2].children
    assert len(stored.statements) == 6


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "family_missing",
        "family_identity",
        "membership_missing",
        "membership_overlap",
        "membership_collection",
        "membership_drift",
        "fragment_missing",
        "fragment_crc",
        "fragment_drift",
        "start_missing",
        "start_family",
    ],
)
async def test_cross_family_damage_refuses_without_partial_details(damage):
    stored = _PageRows(3)
    selected = stored.selected()
    stored.damage = damage
    if damage == "family_missing":
        stored.families.pop()
    if damage == "family_identity":
        stored.families[-1]["root_record_id"] += 1
    if damage == "membership_missing":
        stored.members.pop()
    if damage in ("membership_overlap", "membership_collection"):
        _damage_membership(stored, damage)
    if damage == "fragment_missing":
        stored.fragments.pop()
    if damage == "fragment_crc":
        native = stored.fragments[-1]
        frame = native["encoded_fragment"]
        native["encoded_fragment"] = frame[:-1] + bytes((frame[-1] ^ 1,))
    with pytest.raises(reader.FamilyBlockError):
        await reader.hydrate_families(stored, stored.context(), selected, "scope")
    assert all(result.closed for result in stored.results)


@pytest.mark.asyncio
async def test_many_families_use_bounded_parameter_chunks_without_an_input_cap(monkeypatch):
    stored = _PageRows(1025, children=0)
    monkeypatch.setattr(hydration, "_NATIVE_IDS", 128)
    actual = await reader.hydrate_families(stored, stored.context(), stored.selected(), "scope")
    assert len(actual) == 1025
    assert len(stored.statements) == 6 * 9
    assert (
        max(
            len(statement.compile(compile_kwargs={"render_postcompile": True}).params)
            for statement in stored.statements
        )
        < 32767
    )


@pytest.mark.asyncio
async def test_coordinate_groups_bound_parameters_without_limiting_families():
    stored = _PageRows(5, children=1000)
    actual = await reader.hydrate_families(stored, stored.context(), stored.selected(), "scope")
    starts = [statement for statement in stored.statements if "record_ordinal" in statement.selected_columns]
    assert len(actual) == 5 and all(len(detail.children) == 1000 for detail in actual)
    assert [len(statement.get_final_froms()[0].left.element._data[0]) for statement in starts] == [4004, 1001]
    assert (
        max(
            len(statement.compile(compile_kwargs={"render_postcompile": True}).params)
            for statement in stored.statements
        )
        < 32767
    )


@pytest.mark.asyncio
async def test_empty_complete_selection_needs_no_native_queries():
    stored = _PageRows(0)
    assert await reader.hydrate_families(stored, stored.context(), (), "scope") == ()
    assert not stored.statements


@pytest.mark.asyncio
async def test_cancelled_page_closes_results_and_returns_no_partial_details(monkeypatch):
    stored = _PageRows(100, children=0)
    original = reader.record_from_wire

    def cancel_during_decode(*args, **kwargs):
        if decoded.call_count == 2:
            asyncio.current_task().cancel()
        return original(*args, **kwargs)

    decoded = Mock(side_effect=cancel_during_decode)
    monkeypatch.setattr(reader, "record_from_wire", decoded)
    task = asyncio.create_task(reader.hydrate_families(stored, stored.context(), stored.selected(), "scope"))
    with pytest.raises(asyncio.CancelledError):
        await task
    assert 2 <= decoded.call_count <= 64
    assert all(result.closed for result in stored.results)


@pytest.mark.asyncio
async def test_blob_singleton_exhaustion_refuses_before_fetch(monkeypatch):
    stored = _PageRows(3, children=2)
    limits = reader.FamilyBlockReadLimits(bytes=1024)
    monkeypatch.setattr(reader, "FamilyBlockReadLimits", lambda: limits)
    stored.fragments[-1]["size"] = limits.bytes + 1
    with pytest.raises(reader.FamilyBlockError, match="exceeds the admitted read budget"):
        await reader.hydrate_families(stored, stored.context(), stored.selected(), "scope")
    assert all(size <= limits.bytes for size in stored.blob_sizes)
    assert all(result.closed for result in stored.results)


def _retained_records(base, layout, wrong_kind):
    root = replace(
        base[2],
        scalars={
            "npi": TypedScalar("string", "value", string_value="1234567893"),
        },
    )
    rates = [
        replace(
            child,
            scalars={
                "service_code": TypedScalar("string", "value", string_value="service"),
                "amount": TypedScalar("decimal", "null"),
            },
        )
        for child in base[3]
    ]
    sparse = SparseRecord(
        " exact prior child bytes ",
        " child key bytes ",
        runner_codec.digest_text("child-payload", " exact prior child bytes "),
        runner_codec.digest_text("child-key", " child key bytes "),
    )
    amount = (
        TypedScalar("string", "value", string_value="original actual kind")
        if wrong_kind
        else TypedScalar("decimal", "value", decimal_value=Decimal("12.5000"))
    )
    rates[0] = replace(rates[0], values_by_field=None, sparse=sparse, scalars={**rates[0].scalars, "amount": amount})
    # Sparse children retain their original key/hash ordering, independent of their scalars.
    rates.sort(
        key=lambda child: (
            child.sparse.key_sha256
            if child.sparse
            else runner_codec.child_key_hash(layout.definition, "rates", child.values_by_field)
        )
    )
    note = FamilyRecord(
        2001,
        41,
        4,
        {"note_npi": "1234567893", "note_key": "note", "note_value": 7},
        {
            "note_key": TypedScalar("string", "value", string_value="note"),
            "note_value": TypedScalar("integer", "value", integer_value=7),
        },
    )
    return root, {"notes": [note], "rates": rates}


def _retained_case(fragment_bytes, wrong_kind):
    base = _storage_case(count=2, fragment_bytes=fragment_bytes, sparse=True)
    layout = FamilyBlockLayout(_definition(), {"rates": 7, "notes": 9})
    root, children_by_collection = _retained_records(base, layout, wrong_kind)
    with encode_retained_family(layout.definition, root, children_by_collection, b"S" * 32, limits=base[1]) as encoded:
        frames, manifest = tuple(encoded.fragments()), encoded.manifest
    with verify_family(layout.definition, frames, manifest, limits=base[1]) as verified:
        directory_rows = tuple(
            iter_fragment_directory(
                verified, layout.collection_slots, (struct.unpack("!QQ", frame[:16])[1] for frame in frames)
            )
        )
    ordered_records = [
        (0, root),
        *(
            (layout.collection_slots[name], family_record)
            for name in sorted(children_by_collection)
            for family_record in children_by_collection[name]
        ),
    ]
    memberships = tuple(
        member
        for slot in (0, 7, 9)
        for member in iter_revision_memberships(
            slot,
            sorted(
                (family_record.revision_id, ordinal)
                for ordinal, (collection, family_record) in enumerate(ordered_records)
                if slot == collection
            ),
            chunk_records=3,
        )
    )
    case = (
        layout,
        base[1],
        root,
        [family_record for _slot, family_record in ordered_records[1:]],
        frames,
        directory_rows,
        memberships,
        stored_manifest(manifest, directory_rows, memberships),
    )
    return case, {(slot, family_record.revision_id): family_record for slot, family_record in ordered_records}


@pytest.mark.asyncio
@pytest.mark.parametrize("fragment_bytes", [37, 65536])
@pytest.mark.parametrize("wrong_kind", [False, True])
async def test_retained_sparse_scalars_and_collections_keep_exact_projection(fragment_bytes, wrong_kind):
    case, original_by_key = _retained_case(fragment_bytes, wrong_kind)
    stored = _PageRows(2, case=case)
    context, selected = stored.context(), stored.selected()
    if wrong_kind:
        with pytest.raises(reader.CustomImportReadUnavailableError, match="persisted scalar row is invalid"):
            await reader.hydrate_families(stored, context, selected, "scope")
    else:
        actual = await reader.hydrate_families(stored, context, selected, "scope")
        expected_details = [
            reader._project_family(context, winner, family, original_by_key, "scope")
            for winner, family, _root, _child in selected
        ]
        assert [full_family_payload(detail) for detail in actual] == [
            full_family_payload(detail) for detail in expected_details
        ]
        assert actual[0].root_fields[1].state == "missing"
        amounts = [field for child in actual[0].children for field in child.fields if field.field_id == "amount"]
        assert {field.state for field in amounts} == {"null", "value"}
        assert [format(field.value, "f") for field in amounts if field.state == "value"] == ["12.500000000000"]
    assert all(result.closed for result in stored.results)


@pytest.mark.asyncio
async def test_late_result_close_failure_refuses_the_whole_page(monkeypatch):
    stored = _PageRows(3)
    original_execute = stored.execute

    async def fail_blob_close(statement):
        native_result = await original_execute(statement)
        if "encoded_fragment" in statement.selected_columns:
            native_result.close = Mock(side_effect=RuntimeError("synthetic close failure"))
        return native_result

    monkeypatch.setattr(stored, "execute", fail_blob_close)
    with pytest.raises(RuntimeError, match="synthetic close failure"):
        await reader.hydrate_families(stored, stored.context(), stored.selected(), "scope")
    assert all(result.closed for result in stored.results[:-1])


@pytest.mark.asyncio
async def test_per_family_fragment_budget_is_not_multiplied_by_page_size(monkeypatch):
    stored = _PageRows(3, children=0, fragment_bytes=37)
    first_family = stored.families[0]["family_revision_id"]
    budget_state = SimpleNamespace(
        max_spool_bytes=sum(
            len(fragment["encoded_fragment"])
            for fragment in stored.fragments
            if fragment["family_revision_id"] == first_family
        )
    )

    class SmallSpoolLimits(reader.FamilyBlockReadLimits):
        def codec_limits(self):
            return replace(super().codec_limits(), max_spool_bytes=budget_state.max_spool_bytes, memory_bytes=37)

    limits = SmallSpoolLimits(bytes=8192, max_record_bytes=512, fragment_bytes=37)
    monkeypatch.setattr(reader, "FamilyBlockReadLimits", lambda: limits)
    actual = await reader.hydrate_families(stored, stored.context(), stored.selected(), "scope")
    assert len(actual) == 3
    budget_state.max_spool_bytes -= 1
    with pytest.raises(reader.FamilyBlockError, match="cumulative read budget"):
        await reader.hydrate_families(stored, stored.context(), stored.selected(), "scope")
    assert all(result.closed for result in stored.results)
