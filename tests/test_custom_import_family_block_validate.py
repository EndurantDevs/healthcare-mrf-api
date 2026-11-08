# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed-candidate checks over synthetic source and stored-family evidence."""

import asyncio
import struct
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from process.custom_import import runner_codec
from process.custom_import.family_block_codec import encode_family, verify_family
from process.custom_import.family_block_records import record_parts
from process.custom_import.family_block_scalar_layout import pack_query_scalars, query_scalar_fields, scalar_column
from process.custom_import.family_block_storage_codec import (
    iter_fragment_directory,
    stored_manifest,
    verify_stored_family,
)
from process.custom_import.family_block_validate import (
    CandidateScope,
    FamilyBlockValidationError,
    check_closed_identity,
    check_family_records,
    check_key_dictionary,
    check_pack_allocations,
    check_retained_records,
    check_revision_ranges,
    check_selected_allocations,
    check_source_coverage,
    compare_role_cursor,
    compare_rows,
    require_readonly_snapshot,
    run_structural_checks,
    structural_checks,
)
from process.custom_import.materialization import TypedScalar, _typed_scalar
from tests.test_custom_import_family_block_storage import _storage_case


def _projected_record(layout, record, collection):
    return replace(
        record,
        scalars={
            field.field_id: _typed_scalar(field, record.values_by_field[field.field_id])
            for field in layout.definition.fields
            if field.collection == collection
            and field.projection_slot is not None
            and field.field_id in record.values_by_field
        },
    )


def _query_rows(layout, child_records, sparse):
    query_items = []
    for child in child_records:
        _, (digest, canonical, _) = record_parts(layout.definition, "rates", child, retained=sparse)
        query_items.append(
            (
                301,
                child.revision_id,
                7,
                digest,
                canonical,
                *pack_query_scalars(query_scalar_fields(layout.definition, "rates"), child.scalars),
            )
        )
    return query_items


def _input(*, count=3, sparse=False):
    layout, limits, root, child_records, frames, directory_entries, members, manifest = _storage_case(
        count=count, sparse=sparse
    )
    if not sparse:
        root = _projected_record(layout, root, None)
        child_records = [_projected_record(layout, family_record, "rates") for family_record in child_records]
        with encode_family(
            layout.definition, root, {"rates": child_records}, manifest.family_sha256, limits=limits
        ) as encoded:
            frames, codec_manifest = tuple(encoded.fragments()), encoded.manifest
        with verify_family(layout.definition, frames, codec_manifest, limits=limits) as verified:
            directory_entries = tuple(
                iter_fragment_directory(
                    verified, layout.collection_slots, (struct.unpack("!QQ", frame[:16])[1] for frame in frames)
                )
            )
        manifest = stored_manifest(codec_manifest, directory_entries, members)
    family_by_column = {name: getattr(manifest, name) for name in vars(manifest) if name != "definition_sha256"}
    family_by_column.update(
        family_revision_id=301, root_record_id=401, root_revision_id=root.revision_id, entity_binding_id=501
    )
    fields = query_scalar_fields(layout.definition)
    family_by_column.update(
        zip(
            ("raw_states", "raw_type_overrides", *(scalar_column(field_definition) for field_definition in fields)),
            pack_query_scalars(fields, root.scalars),
            strict=True,
        )
    )
    _, (_, key, _) = record_parts(layout.definition, None, root, retained=sparse)
    query_items = _query_rows(layout, child_records, sparse)
    arguments_by_name = {
        "expected_manifest": manifest,
        "limits": limits,
        "root_key_contract": runner_codec.root_key_contract_hash(layout.definition),
        "root_key": (runner_codec.digest_text("root-key", key), key),
        "entity": ("npi", "1234567893", runner_codec.digest_text("entity:npi", "1234567893")),
        "query_rows": query_items,
    }
    return (
        layout,
        family_by_column,
        list(zip(directory_entries, frames, strict=True)),
        members,
        [(0, root), *((7, child) for child in child_records)],
        arguments_by_name,
    )


def _verify(case):
    layout, row, fragments, members, expected, arguments = case
    manifest = arguments["expected_manifest"]
    if layout.manifest(row) != manifest:
        raise FamilyBlockValidationError("candidate family manifest differs from trusted assembly")
    arguments_by_name = {name: cell for name, cell in arguments.items() if name not in {"expected_manifest", "limits"}}
    with verify_stored_family(
        layout.definition, layout.collection_slots, fragments, members, manifest, limits=arguments["limits"]
    ) as verified:
        return check_family_records(layout, row, verified, expected, **arguments_by_name)


def test_complete_envelope_decoding_and_raw_query_bijection():
    assert _verify(_input()) == (3, 2, 6)


@pytest.mark.parametrize(
    "change",
    (
        "manifest",
        "source_ordinal",
        "revision",
        "pack",
        "root_key",
        "entity",
        "key_contract",
        "query_scalar",
        "query_family",
        "missing_query",
        "extra_query",
        "extra_origin",
        "missing_origin",
        "fragment",
    ),
)
def test_decoded_family_refuses_independent_identity_or_content_changes(change):
    case = _input()
    _layout, family_row, fragments, _members, expected, arguments = case
    match change:
        case "manifest":
            family_row["storage_sha256"] = b"x" * 32
        case "source_ordinal" | "revision" | "pack":
            field = {"source_ordinal": "source_ordinal", "revision": "revision_id", "pack": "pack_id"}[change]
            expected[1] = (7, replace(expected[1][1], **{field: getattr(expected[1][1], field) + 1}))
        case "root_key":
            arguments["root_key"] = (b"x" * 32, "wrong")
        case "entity":
            arguments["entity"] = ("npi", "9999999999", b"x" * 32)
        case "key_contract":
            arguments["root_key_contract"] = b"x" * 32
        case "query_scalar" | "query_family":
            cells = list(arguments["query_rows"][0])
            cells[5 if change == "query_scalar" else 0] = b"\x01" if change == "query_scalar" else 302
            arguments["query_rows"][0] = tuple(cells)
        case "missing_query":
            arguments["query_rows"].pop()
        case "extra_query":
            arguments["query_rows"].append(arguments["query_rows"][-1])
        case "extra_origin":
            expected.append(expected[-1])
        case "missing_origin":
            expected.pop()
        case _:
            fragments.pop()
    with pytest.raises((FamilyBlockValidationError, ValueError)):
        _verify(case)


def test_retained_opaque_root_is_not_reparsed_or_rehashed_with_new_semantics():
    assert _verify(_input(count=0, sparse=True)) == (0, 0, 0)


def test_raw_scalar_presence_and_actual_type_remain_independent():
    case = _input(count=0, sparse=True)
    layout, row, _fragments, _members, expected, arguments = case
    root = expected[0][1]
    name = layout.definition.query.root_fields[0]
    scalar = TypedScalar("decimal", "value", decimal_value=Decimal("1.2300"))
    root = replace(root, scalars={name: scalar})
    fields = query_scalar_fields(layout.definition)
    row.update(
        zip(
            ("raw_states", "raw_type_overrides", *(scalar_column(field_definition) for field_definition in fields)),
            pack_query_scalars(fields, root.scalars),
            strict=True,
        )
    )
    verified = SimpleNamespace(records=lambda collection: iter([root]) if collection is None else iter(()))
    # This exercises the post-envelope comparison only; the complete wrapper's
    # original checksum/envelope proof is covered separately above.
    arguments_by_name = {key: value for key, value in arguments.items() if key not in {"expected_manifest", "limits"}}
    assert check_family_records(layout, row, verified, [(0, root)], **arguments_by_name) == (0, 1, 0)
    row["raw_type_overrides"] = None
    with pytest.raises(FamilyBlockValidationError, match="scalar"):
        check_family_records(layout, row, verified, [(0, root)], **arguments_by_name)


def test_fresh_source_cannot_self_attest_missing_projected_scalars():
    layout, row, _fragments, _members, expected, arguments = _input(count=0)
    root = replace(expected[0][1], scalars={})
    verified = SimpleNamespace(records=lambda collection: iter([root]) if collection is None else iter(()))
    arguments_by_name = {key: value for key, value in arguments.items() if key not in {"expected_manifest", "limits"}}
    with pytest.raises(FamilyBlockValidationError, match="projection semantics"):
        check_family_records(layout, row, verified, [(0, root)], **arguments_by_name)


def test_family_child_hash_tie_remains_duplicate_even_for_distinct_revision_ids():
    layout, row, _fragments, _members, expected, arguments = _input(count=1)
    root, child = expected[0][1], expected[1][1]
    duplicate = replace(child, revision_id=child.revision_id + 100)
    row["child_count"] = 2
    verified = SimpleNamespace(
        records=lambda collection: iter([root]) if collection is None else iter([child, duplicate])
    )
    arguments_by_name = {key: value for key, value in arguments.items() if key not in {"expected_manifest", "limits"}}
    with pytest.raises(FamilyBlockValidationError, match="logical child key"):
        check_family_records(layout, row, verified, [(0, root), (7, child), (7, duplicate)], **arguments_by_name)


def test_dictionary_preserves_equal_hash_ties_and_scoped_collision_policy():
    a, b = b"a" * 32, b"b" * 32
    assert check_key_dictionary([(1, 1, a, "one"), (1, 2, a, "two"), (2, 1, b, "one")]) == 3
    with pytest.raises(FamilyBlockValidationError, match="tie"):
        check_key_dictionary([(1, 1, a, "one"), (2, 1, a, "two")])
    with pytest.raises(FamilyBlockValidationError, match="repeats"):
        check_key_dictionary([(1, 1, a, "one"), (1, 2, a, "one")])


def test_native_homes_use_all_actual_ids_and_preserve_gaps_and_maximum():
    largest = 2**63 - 1
    ranges = [(1, 7, 8), (2, 11, 11), (2, largest, largest)]
    assert check_revision_ranges(ranges, [(1, 7), (1, 8), (2, 11), (2, largest)]) == 4


@pytest.mark.parametrize(
    "ranges,ids",
    (
        ([(1, 7, 9)], [(1, 7), (1, 9)]),
        ([(1, 7, 7)], [(1, 7), (1, 8)]),
        ([(1, 7, 8)], [(1, 7), (1, 7), (1, 8)]),
        ([(1, 7, 7), (1, 7, 7)], [(1, 7)]),
        ([(1, 7, 7)], [(2, 7)]),
        ([(True, 7, 7)], [(1, 7)]),
    ),
)
def test_revision_home_rejects_reservations_or_missing_extra_duplicate_ids(ranges, ids):
    with pytest.raises(FamilyBlockValidationError):
        check_revision_ranges(ranges, ids)


def test_complete_source_coverage_includes_empty_parts_and_invalid_lane_positions():
    parts = [(1, 1, 0), (1, 2, 2), (2, 1, 1)]
    occurrences = [(1, 2, 0, 0), (1, 2, 1, 1), (2, 1, 0, 0)]
    assert check_source_coverage(parts, occurrences, {1: 2, 2: 1, 3: 0}) == 3


@pytest.mark.parametrize(
    "parts,occurrences,counts",
    (
        ([(1, 2, 1)], [(1, 2, 0, 0)], {1: 1}),
        ([(1, 1, 2)], [(1, 1, 0, 0)], {1: 2}),
        ([(1, 1, 1)], [(1, 1, 0, 0), (1, 1, 1, 1)], {1: 1}),
        ([(1, 1, 1)], [(1, 1, 0, 1)], {1: 1}),
        ([(1, 1, 1)], [(1, 1, 0, 0)], {1: 2}),
    ),
)
def test_source_coverage_refuses_gaps_prefixes_and_count_only_claims(parts, occurrences, counts):
    with pytest.raises(FamilyBlockValidationError):
        check_source_coverage(parts, occurrences, counts)


def test_pack_census_counts_eliminated_allocations_and_checks_stream_scope():
    packs = [(10, 1, 0, 1), (20, 2, 7, 2)]
    allocations = [(10, 0, 1, 0, 11), (20, 0, 2, 7, 21), (20, 9, 2, 7, 23)]
    assert check_pack_allocations(packs, allocations) == 3
    with pytest.raises(FamilyBlockValidationError, match="count"):
        check_pack_allocations(packs, allocations[:-1])
    with pytest.raises(FamilyBlockValidationError, match="stream"):
        check_pack_allocations(packs, [allocations[0], (20, 0, 2, 8, 21), allocations[2]])
    with pytest.raises(FamilyBlockValidationError, match="ordinal"):
        check_pack_allocations(packs, [allocations[0], allocations[1], allocations[1]])


def test_selected_members_are_subset_of_full_allocations_with_exact_provenance():
    all_rows = [(1, 11, 10, 0, 0), (2, 21, 20, 0, 7), (2, 23, 20, 9, 7)]
    assert check_selected_allocations([all_rows[0], all_rows[2]], all_rows) == 2
    with pytest.raises(FamilyBlockValidationError, match="provenance"):
        check_selected_allocations([(2, 23, 20, 8, 7)], all_rows)
    with pytest.raises(FamilyBlockValidationError):
        check_selected_allocations([all_rows[2], all_rows[2]], all_rows)
    with pytest.raises(FamilyBlockValidationError, match="unordered"):
        check_selected_allocations([], [*all_rows, all_rows[-1]])


def test_retained_copy_keeps_original_scalar_spelling_and_opaque_payload():
    case = _input(count=0, sparse=True)
    layout, _row, _fragments, _members, expected, _arguments = case
    root = expected[0][1]
    scalar = TypedScalar("decimal", "value", decimal_value=Decimal("1.2300"))
    root = replace(root, scalars={layout.definition.query.root_fields[0]: scalar})
    destination = replace(root, revision_id=root.revision_id + 20, pack_id=root.pack_id + 20)
    assert check_retained_records(layout, [(0, root)], [(0, destination)]) == 1
    changed = replace(
        destination, scalars={layout.definition.query.root_fields[0]: replace(scalar, decimal_value=Decimal("1.23"))}
    )
    with pytest.raises(FamilyBlockValidationError, match="scalars"):
        check_retained_records(layout, [(0, root)], [(0, changed)])
    with pytest.raises(FamilyBlockValidationError):
        check_retained_records(layout, [(0, root)], [])


def test_stream_comparison_exhausts_and_keeps_native_types():
    observed_items = []

    def expected():
        observed_items.append("started")
        yield (Decimal("1.230"), b"x")
        observed_items.append("finished")

    assert compare_rows([(Decimal("1.23"), memoryview(b"x"))], expected(), label="synthetic") == 1
    assert observed_items == ["started", "finished"]
    with pytest.raises(FamilyBlockValidationError):
        compare_rows([(True,)], [(1,)], label="synthetic")
    with pytest.raises(FamilyBlockValidationError):
        compare_rows([], [(1,)], label="synthetic")


def _binding():
    layout = _input(count=0)[0]
    tables = layout.tables(11)
    return SimpleNamespace(
        family_id=11,
        layout=layout,
        tables_by_role=tables,
        owner_oid=91,
        oids_by_role={role: 1000 + index for index, role in enumerate(tables)},
    )


def test_closed_identity_binds_producer_and_layout_without_leasing_cpu_work():
    binding = _binding()
    scope = CandidateScope(11, 12, 13, 14, 15, 16, 17, 18, b"x" * 32)
    snapshot = scope.parameters() | {"frozen_at": None}
    descriptor_by_key = {
        "family_id": 11,
        "phase": "closed",
        "closed_at": datetime(2026, 1, 1, tzinfo=UTC),
        "layout_contract": "custom-import/family-block-layout/v1",
        "layout_sha256": binding.layout.digest,
        "definition_sha256": bytes.fromhex(binding.layout.definition.digest),
        "collection_slots": dict(binding.layout.collection_slots),
    }
    assert set(descriptor_by_key) <= set(CustomImportFamilyBlockLayout.__table__.c.keys())
    checked = check_closed_identity(binding, scope, snapshot, descriptor_by_key)
    assert checked.owner_oid == 91 and checked.scope == scope
    for target, key, value in (
        (snapshot, "producing_token_sha256", b"y" * 32),
        (descriptor_by_key, "phase", "loading"),
        (descriptor_by_key, "layout_contract", "wrong"),
        (descriptor_by_key, "layout_sha256", b"y" * 32),
        (descriptor_by_key, "closed_at", None),
    ):
        old = target[key]
        target[key] = value
        with pytest.raises(FamilyBlockValidationError):
            check_closed_identity(binding, scope, snapshot, descriptor_by_key)
        target[key] = old


@pytest.mark.parametrize(
    "state,passes",
    (
        (("repeatable read", "on"), True),
        (("serializable", "on"), True),
        (("read committed", "on"), False),
        (("repeatable read", "off"), False),
    ),
)
def test_expensive_validation_requires_one_read_only_snapshot(state, passes):
    class Session:
        async def execute(self, statement):
            assert "FOR UPDATE" not in str(statement)
            return SimpleNamespace(one=lambda: state)

    if passes:
        asyncio.run(require_readonly_snapshot(Session()))
    else:
        with pytest.raises(FamilyBlockValidationError):
            asyncio.run(require_readonly_snapshot(Session()))


def test_structural_plan_retains_indexed_any_match_and_full_scope():
    checks = structural_checks(_binding(), "control", base_schema="base")
    by_code = {check.code: check.sql for check in checks}
    assert len(by_code) == len(checks)
    assert "NOT EXISTS" in by_code["winner_reference"]
    assert "q.child_revision_id=w.context_child_revision_id" in by_code["winner_reference"]
    assert "q.family_revision_id=w.family_revision_id" in by_code["winner_reference"]
    assert "p.schema_revision_id=m.schema_revision_id" in by_code["winner_reference"]
    assert "IS NOT DISTINCT FROM" in by_code["pack_reference"]
    assert 'canonical_logical_key COLLATE "C" IS NOT DISTINCT FROM' in by_code["root_dictionary_canonical_identity"]
    assert "k.key_rank=q.key_rank AND k.key_variant=q.key_variant" in by_code["query_record_key"]
    assert "sum(8+octet_length(encoded_fragment))" in by_code["fragment_census"]
    assert not any("custom_import_child_scalar" in c.sql or "custom_import_family_child" in c.sql for c in checks)
    assert not any("FOR UPDATE" in c.sql or "GRANT " in c.sql or "CREATE " in c.sql for c in checks)


def test_structural_queries_bind_every_value_and_stop_on_first_failure():
    binding = _binding()
    scope = CandidateScope(11, 12, 13, 14, 15, 16, 17, 18, b"x" * 32)

    class Session:
        def __init__(self, fail=False):
            self.calls = []
            self.fail = fail

        async def execute(self, statement, parameters=None):
            if parameters is None:
                return SimpleNamespace(one=lambda: ("repeatable read", "on"))
            assert set(statement._bindparams) <= set(parameters)
            self.calls.append(str(statement))
            return SimpleNamespace(first=lambda: ("failure", 1) if self.fail else None)

    session = Session()
    completed = asyncio.run(run_structural_checks(session, binding, scope, "control"))
    assert len(completed) == len(session.calls) > 20
    assert all(":base_generation_id::" not in sql for sql in session.calls)
    failing = Session(True)
    with pytest.raises(FamilyBlockValidationError, match="metadata_scope"):
        asyncio.run(run_structural_checks(failing, binding, scope, "control"))
    assert len(failing.calls) == 1


@pytest.mark.parametrize("actual,expected,refuses", (([(1,)], [(1,)], False), ([(1,)], [], True), ([], [(1,)], True)))
def test_role_cursor_is_single_bounded_and_closes_on_failure(actual, expected, refuses):
    from sqlalchemy import BigInteger, Column, MetaData, Table

    table = Table("synthetic", MetaData(), Column("id", BigInteger, primary_key=True))

    class Cursor:
        closed = False

        def __aiter__(self):
            self.rows = iter(actual)
            return self

        async def __anext__(self):
            row = next(self.rows, _MISSING)
            if row is _MISSING:
                raise StopAsyncIteration
            return row

        async def close(self):
            self.closed = True

    class Session:
        calls = 0

        async def stream(self, statement):
            self.calls += 1
            assert statement.get_execution_options()["yield_per"] == 2
            return cursor

    _MISSING = object()
    cursor, session = Cursor(), Session()
    call = compare_role_cursor(session, table, expected, order_by=("id",), fetch_rows=2)
    if refuses:
        with pytest.raises(FamilyBlockValidationError):
            asyncio.run(call)
    else:
        assert asyncio.run(call) == 1
    assert cursor.closed and session.calls == 1


@pytest.mark.parametrize("sparse", (False, True))
def test_batch_validation_preserves_codec_call_counts(monkeypatch, sparse):
    from collections import Counter

    from process.custom_import import family_block_validate as validation

    child_count = 0 if sparse else 3
    record_count = child_count + 1
    case = _input(count=child_count, sparse=sparse)
    calls = Counter({"record_parts": 0, "_scalar_wire": 0, "pack_query_scalars": 0})
    for name in ("record_parts", "_scalar_wire", "pack_query_scalars"):
        original = getattr(validation, name)

        def observed(*args, _name=name, _original=original, **kwargs):
            calls[_name] += 1
            return _original(*args, **kwargs)

        monkeypatch.setattr(validation, name, observed)
    assert _verify(case)[0] == child_count
    assert calls == {
        "record_parts": 2 * record_count,
        "_scalar_wire": 0 if sparse else 2 * record_count,
        "pack_query_scalars": record_count,
    }
