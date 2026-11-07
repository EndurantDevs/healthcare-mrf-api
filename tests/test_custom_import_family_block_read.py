# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded selected records and ordinary read SQL over synthetic block storage."""

import asyncio
import struct
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from sqlalchemy import func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.compiler import FROM_LINTING

from process.custom_import import family_block_read as reader
from process.custom_import import grouped_read, read_core
from process.custom_import.family_block_binding import FamilyBlockBinding
from process.custom_import.family_block_read_models import read_models
from process.custom_import.family_block_read_sql import query_projection_views
from process.custom_import.family_block_records import FamilyBlockError
from process.custom_import.materialization import TypedScalar
from tests.test_custom_import_family_block_storage import _storage_case
from tests.test_custom_import_grouped_projection_query import _context, _prepare
from tests.test_custom_import_query_projection_evidence import _evidence, _generation, _manifest, _seal


def _window(case, ordinal, *, revision=None):
    layout, _limits, root, children, _frames, _directory, _members, manifest = case
    selected = root if ordinal == 0 else children[ordinal - 1]
    return reader.SelectedRecordWindow(
        layout.definition,
        None if ordinal == 0 else "rates",
        selected.revision_id if revision is None else revision,
        ordinal,
        retained=manifest.retained,
        limits=reader.FamilyBlockReadLimits(),
    )


def _selected_fragments(case, ordinal):
    _layout, _limits, _root, _children, frames, directory, _members, _manifest = case
    first = max(row[0] for row in directory if row[2] is not None and row[2] <= ordinal)
    return zip(directory[first:], frames[first:], strict=True)


@pytest.mark.parametrize("fragment_bytes", [1, 7, 16, 37, 65536])
@pytest.mark.parametrize("ordinal", [0, 1, 7])
def test_selected_record_prefix_and_payload_cross_fragments_without_family_scan(fragment_bytes, ordinal):
    case = _storage_case(fragment_bytes=fragment_bytes)
    selected = _window(case, ordinal)
    consumed_ordinals = []
    for metadata, frame in _selected_fragments(case, ordinal):
        consumed_ordinals.append(metadata[0])
        if selected.feed(metadata, frame) is not None:
            break
    assert selected.result == (case[2] if ordinal == 0 else case[3][ordinal - 1])
    assert selected.next_fragment == consumed_ordinals[-1] + 1
    if fragment_bytes < 100:
        assert len(consumed_ordinals) < len(case[4])


@pytest.mark.parametrize("damage", ["identity", "crc", "sequence", "offset", "ordinal", "missing"])
def test_selected_record_refuses_damage_before_exposure(damage):
    case = _storage_case(fragment_bytes=37)
    selected = _window(case, 1, revision=17 if damage == "identity" else None)
    fragments = list(_selected_fragments(case, 1))
    if damage == "crc":
        metadata, frame = fragments[0]
        fragments[0] = metadata, frame[:-1] + bytes((frame[-1] ^ 1,))
    elif damage in ("offset", "ordinal"):
        metadata, frame = fragments[0]
        metadata_cells = list(metadata)
        metadata_cells[4 if damage == "offset" else 2] = metadata[1] if damage == "offset" else 2
        fragments[0] = tuple(metadata_cells), frame
    elif damage == "sequence":
        del fragments[1]
    elif damage == "missing":
        fragments = fragments[:1]
    if damage == "missing":
        for metadata, frame in fragments:
            assert selected.feed(metadata, frame) is None
        assert selected.result is None
    else:
        with pytest.raises(FamilyBlockError):
            for metadata, frame in fragments:
                selected.feed(metadata, frame)
                if selected.result is not None:
                    break


def test_selected_sparse_record_preserves_original_opaque_bytes():
    case = _storage_case(sparse=True, fragment_bytes=16)
    selected = _window(case, 0)
    for metadata, frame in _selected_fragments(case, 0):
        if selected.feed(metadata, frame) is not None:
            break
    assert selected.result == case[2]
    assert selected.result.values_by_field is None
    assert selected.result.sparse.payload_text == " opaque original payload "


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1.0000000000005", "1.000000000001"),
        ("-1.0000000000005", "-1.000000000001"),
        ("-0.0000000000004", "0.000000000000"),
        ("999999999999999999.123456789012", "999999999999999999.123456789012"),
    ],
)
def test_scalar_presentation_matches_native_numeric_rounding_sign_and_context(value, expected):
    scalar = TypedScalar("decimal", "value", decimal_value=Decimal(value))
    with localcontext() as context:
        context.prec = 3
        actual = reader.present_scalar(scalar).decimal_value
    assert actual.as_tuple() == Decimal(expected).as_tuple()
    assert scalar.decimal_value.as_tuple() == Decimal(value).as_tuple()


def test_timestamp_presentation_is_utc_and_retained_actual_kind_is_not_reinterpreted():
    original = datetime.fromisoformat("2024-02-03T04:05:06.123456+05:30")
    actual = reader.present_scalar(TypedScalar("timestamp", "value", timestamp_value=original))
    assert actual.timestamp_value == original and actual.timestamp_value.tzinfo == timezone.utc
    field = _context().definition.fields_by_id["score"]
    wrong = reader.present_scalar(TypedScalar("integer", "value", integer_value=3))
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="persisted scalar row is invalid"):
        read_core._field_value(field, wrong)


def _block_context():
    context = _context()
    from process.custom_import.family_block_layout import FamilyBlockLayout

    layout = FamilyBlockLayout(context.definition, context.collection_slots_by_name)
    tables = layout.tables(19)
    binding = FamilyBlockBinding(
        19, layout, tables, MappingProxyType({name: index + 1 for index, name in enumerate(tables)}), 7
    )
    return replace(
        context,
        family_block_binding=binding,
        storage_models=read_models(binding, context.target),
        projection_tables=query_projection_views(binding),
    )


@pytest.mark.filterwarnings("error:SELECT statement has a cartesian product:sqlalchemy.exc.SAWarning")
@pytest.mark.parametrize("entity_values", [None, (), ("1234567893",)])
@pytest.mark.parametrize("operator", ["eq", "gt", "gte", "lt", "lte"])
def test_grouped_projection_and_raw_filters_compile_only_checked_block_roles(entity_values, operator):
    prepared = _prepare(
        _block_context(),
        entity_values=entity_values,
        context_filters=(
            read_core.ReadFilter("period", "eq", 2024),
            read_core.ReadFilter("segment", "eq", "segment_a"),
            read_core.ReadFilter("service_code", "eq", "chosen"),
        ),
        filters=(read_core.ReadFilter("output_quality", operator, 2), read_core.ReadFilter("score", "lt", 9)),
        order_terms=(read_core.ReadOrderTerm("output_cost", "desc", "last"),),
    )
    sql = str(prepared.statement.compile(dialect=postgresql.dialect(), linting=FROM_LINTING))
    assert "custom_import_block_family" in sql and "custom_import_block_query_record" in sql
    assert "custom_import_block_key" in sql and "custom_import_generation_seal" in sql
    for legacy in (
        "custom_import_root_revision",
        "custom_import_child_revision",
        "custom_import_family_child",
        "custom_import_root_scalar",
        "custom_import_child_scalar",
        "custom_import_derived_root ON",
    ):
        assert "." + legacy not in sql
    assert "canonical_child_key COLLATE" in sql


@pytest.mark.filterwarnings("error:SELECT statement has a cartesian product:sqlalchemy.exc.SAWarning")
def test_identity_count_and_selected_winner_compile_without_legacy_deferred_payloads():
    context = _block_context()
    statements = [read_core._winner_statement(context)]
    statements.append(select(func.count()).select_from(statements[0].order_by(None).subquery()))
    for statement in statements:
        sql = str(statement.compile(dialect=postgresql.dialect(), linting=FROM_LINTING))
        assert "canonical_payload" not in sql and "payload_sha256" not in sql
        assert ".custom_import_root_revision" not in sql and ".custom_import_family_revision" not in sql


@pytest.mark.filterwarnings("error:.*:sqlalchemy.exc.SAWarning")
@pytest.mark.parametrize("operator", ["eq", "neq", "gt", "gte", "lt", "lte", "is_missing", "is_null"])
def test_selected_child_profile_keeps_scalar_guards_and_original_tie_order(operator):
    layout = _storage_case()[0]
    pinned_target = read_core.PinnedReadTarget(1, 2, 3, 4, "default")
    binding = FamilyBlockBinding(19, layout, layout.tables(19), {}, 7)
    context = read_core._ReadContext(
        pinned_target,
        layout.definition,
        1,
        7,
        {"rates": 7},
        {7: "rates"},
        storage_models=read_models(binding, pinned_target),
        family_block_binding=binding,
    )
    assert read_core._profile_scope_slot(layout.definition.selection_profiles[0], layout.definition, {"rates": 7}) == (
        7,
        "rates",
    )
    field = layout.definition.fields_by_id["amount"]
    predicate = read_core._NormalizedFilter(field, operator, Decimal("2"), "2")
    statement = (
        read_core._winner_statement(context)
        .where(read_core._predicate_condition(predicate, context))
        .order_by(*read_core._winner_order_terms(context, (read_core.ReadOrderTerm("amount", "desc", "last"),)))
    )
    sql = str(statement.compile(dialect=postgresql.dialect(), linting=FROM_LINTING))
    assert "custom_import_block_query_record" in sql and "block_query_membership" in sql
    assert "context_child_revision_id" in sql and "context_collection_slot" in sql
    assert ".custom_import_child_revision" not in sql and ".custom_import_child_scalar" not in sql
    assert "canonical_payload" not in sql and "payload_sha256" not in sql
    order = sql.rsplit("ORDER BY", 1)[1]
    assert "context_key_sha256 ASC" in order and "key_variant" not in order


class _Rows:
    def __init__(self, values):
        self.values = values
        self.closed = False

    def mappings(self):
        return self

    def all(self):
        return self.values

    def one_or_none(self):
        assert len(self.values) <= 1
        return self.values[0] if self.values else None

    def __iter__(self):
        return iter(self.values)

    def close(self):
        self.closed = True


class _StoredRows:
    """Explicit in-memory query boundary fake; native SQL/custody is not proven."""

    def __init__(self, case):
        layout, _limits, root, children, frames, directory, members, manifest = case
        self.binding = FamilyBlockBinding(19, layout, layout.tables(19), {}, 7)
        self.family = dict(
            family_revision_id=9,
            root_record_id=23,
            root_revision_id=root.revision_id,
            entity_binding_id=11,
            **vars(manifest),
        )
        self.fragments = [
            dict(
                zip(reader._DIRECTORY, metadata, strict=True),
                family_revision_id=9,
                encoded_fragment=frame,
                size=len(frame),
            )
            for metadata, frame in zip(directory, frames, strict=True)
        ]
        self.members = [
            dict(
                zip(reader._MEMBERSHIP, member.cells(), strict=True),
                family_revision_id=9,
                size=len(member.encoded_membership or b""),
            )
            for member in members
        ]
        self.records = {(0, root.revision_id): root} | {(7, row.revision_id): row for row in children}
        self.results, self.statements = [], []

    def query_result(self, statement):
        self.statements.append(statement)
        keys = set(statement.selected_columns.keys())
        params = statement.compile().params
        if "record_ordinal" in keys:
            selected_rows = [
                (
                    ordinal,
                    max(
                        fragment_row["fragment_ordinal"]
                        for fragment_row in self.fragments
                        if fragment_row["first_record_ordinal"] is not None
                        and fragment_row["first_record_ordinal"] <= ordinal
                    ),
                )
                for ordinal in range(self.family["child_count"] + 1)
            ]
            if "family_revision_id" in keys:
                selected_rows = [
                    {
                        "family_revision_id": self.family["family_revision_id"],
                        "record_ordinal": ordinal,
                        "fragment_ordinal": start,
                    }
                    for ordinal, start in selected_rows
                ]
        elif "fragment_ordinal" in keys:
            start = params.get("fragment_ordinal_1", 0)
            selected_rows = [stored_row for stored_row in self.fragments if stored_row["fragment_ordinal"] >= start]
        elif "record_count" in keys:
            selected_rows = self.members
        elif "root_revision_id" in keys:
            selected_rows = [self.family]
        else:
            raise AssertionError(f"unexpected read query columns: {keys}")
        if statement._limit_clause is not None:
            selected_rows = selected_rows[: statement._limit_clause.value]
        query_result = _Rows(selected_rows)
        self.results.append(query_result)
        return query_result

    async def execute(self, statement):
        return self.query_result(statement)

    async def run_sync(self, operation):
        return operation(SimpleNamespace(execute=self.query_result))


@pytest.mark.asyncio
@pytest.mark.parametrize("fragment_bytes", [1, 7, 16, 37, 65536])
@pytest.mark.parametrize("sparse", [False, True])
async def test_selected_records_batches_start_lookup_and_reuses_bounded_pages(fragment_bytes, sparse):
    stored = _StoredRows(_storage_case(fragment_bytes=fragment_bytes, sparse=sparse))
    actual = await reader.selected_records(
        stored,
        stored.binding,
        stored.family,
        stored.records,
        limits=reader.FamilyBlockReadLimits(fragment_bytes=fragment_bytes),
    )
    assert actual == stored.records
    starts = [statement for statement in stored.statements if "record_ordinal" in statement.selected_columns]
    assert len(starts) == 1
    sql = str(starts[0].compile(dialect=postgresql.dialect()))
    assert "LATERAL" in sql and "first_record_ordinal <= requested_block_records.record_ordinal" in sql
    if fragment_bytes == 65536:
        assert len(stored.statements) == 5  # Two membership, one batched lookup, two fragment queries.


@pytest.mark.asyncio
async def test_many_selected_records_charge_shared_fragment_io_only_once():
    stored = _StoredRows(_storage_case(count=100, fragment_bytes=65536))
    limits = reader.FamilyBlockReadLimits(bytes=65536, max_record_bytes=2048)
    assert len(stored.fragments) == 1
    assert len(stored.fragments[0]["encoded_fragment"]) * len(stored.records) > limits.codec_limits().max_spool_bytes
    actual = await reader.selected_records(stored, stored.binding, stored.family, stored.records, limits=limits)
    assert actual == stored.records
    assert len(stored.statements) == 5


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [100, 500])
async def test_selected_records_decode_each_checked_fragment_once(monkeypatch, count):
    stored = _StoredRows(_storage_case(count=count, fragment_bytes=65536))
    decode = Mock(wraps=reader.decode_fragment)
    unpack = Mock(wraps=reader.msgpack.unpackb)
    monkeypatch.setattr(reader, "decode_fragment", decode)
    monkeypatch.setattr(reader.msgpack, "unpackb", unpack)

    actual = await reader.selected_records(stored, stored.binding, stored.family, stored.records)

    assert actual == stored.records
    assert list(actual) == list(stored.records)
    assert decode.call_count <= len(stored.fragments)
    assert unpack.call_count <= len(stored.records) + 2


@pytest.mark.parametrize("damage", ["identity", "extent", "collection", "order"])
def test_reused_fragment_tail_preserves_selected_target_checks(damage):
    case = _storage_case(fragment_bytes=65536)
    selected = _window(case, 0)
    metadata, frame = next(_selected_fragments(case, 0))
    assert selected.feed(metadata, frame) == case[2]
    revision, collection, ordinal = case[3][0].revision_id, "rates", 1
    if damage == "identity":
        revision += 1
    elif damage == "extent":
        ordinal = metadata[2] + metadata[3]
    elif damage == "collection":
        collection = "unconfigured"
    else:
        ordinal, collection = 0, None
    with pytest.raises(FamilyBlockError):
        selected.advance(collection, revision, ordinal)


@pytest.mark.asyncio
async def test_reused_fragment_reads_remain_cancellable_without_returning_a_partial_result(monkeypatch):
    stored = _StoredRows(_storage_case(count=100, fragment_bytes=65536))
    original = reader.record_from_wire

    def cancel_during_decode(*args, **kwargs):
        if decode.call_count == 2:
            asyncio.current_task().cancel()
        return original(*args, **kwargs)

    decode = Mock(side_effect=cancel_during_decode)
    monkeypatch.setattr(reader, "record_from_wire", decode)
    task = asyncio.create_task(reader.selected_records(stored, stored.binding, stored.family, stored.records))
    with pytest.raises(asyncio.CancelledError):
        await task
    assert 2 <= decode.call_count <= 64


def test_reused_fragment_skips_unselected_records_without_rewinding():
    case = _storage_case(fragment_bytes=65536)
    selected = _window(case, 0)
    metadata, frame = next(_selected_fragments(case, 0))
    assert selected.feed(metadata, frame) == case[2]
    assert selected.advance("rates", case[3][-1].revision_id, len(case[3])) == case[3][-1]
    assert selected.next_fragment == 1


@pytest.mark.parametrize("fragment_bytes", [37, 65536])
def test_reused_fragment_crosses_collection_markers_with_exact_original_records(fragment_bytes):
    from process.custom_import import runner_codec
    from process.custom_import.family import RootFamily
    from process.custom_import.family_block_codec import FamilyRecord, encode_family, verify_family
    from process.custom_import.family_block_storage_codec import iter_fragment_directory
    from tests.test_custom_import_family_block_raw_integration import _definition

    definition = _definition()
    case = _storage_case(fragment_bytes=fragment_bytes)
    root, children = case[2], case[3]
    children_by_collection = {
        "notes": [FamilyRecord(2001, 41, 4, {"note_npi": "1234567893", "note_key": "note", "note_value": 3})],
        "rates": children,
    }
    semantic = runner_codec.new_family_hash(
        definition,
        RootFamily(
            ("1234567893",),
            root.values_by_field,
            {
                name: tuple(family_record.values_by_field for family_record in collection_records)
                for name, collection_records in children_by_collection.items()
            },
        ),
    )
    with encode_family(definition, root, children_by_collection, semantic, limits=case[1]) as encoded:
        frames, manifest = tuple(encoded.fragments()), encoded.manifest
    with verify_family(definition, frames, manifest, limits=case[1]) as verified:
        fragment_directory_rows = tuple(
            iter_fragment_directory(
                verified, {"rates": 7, "notes": 9}, (struct.unpack("!QQ", frame[:16])[1] for frame in frames)
            )
        )
    window = None
    decode = Mock(wraps=reader.decode_fragment)
    expected_records = [
        (None, root),
        ("notes", children_by_collection["notes"][0]),
        *(("rates", child_record) for child_record in children),
    ]
    with patch.object(reader, "decode_fragment", decode):
        for ordinal, (collection, family_record) in enumerate(expected_records):
            start = max(entry[0] for entry in fragment_directory_rows if entry[2] is not None and entry[2] <= ordinal)
            if window is not None and start == window.next_fragment - 1:
                window.advance(collection, family_record.revision_id, ordinal)
            else:
                window = reader.SelectedRecordWindow(
                    definition,
                    collection,
                    family_record.revision_id,
                    ordinal,
                    retained=False,
                    limits=reader.FamilyBlockReadLimits(),
                )
            while window.result is None:
                next_fragment = start if window.next_fragment is None else window.next_fragment
                window.feed(fragment_directory_rows[next_fragment], frames[next_fragment])
            assert window.result == family_record
    assert decode.call_count <= len(frames)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["membership", "fragment", "crc", "census"])
async def test_selected_records_refuses_incomplete_storage_atomically(damage):
    stored = _StoredRows(_storage_case(fragment_bytes=37))
    if damage == "membership":
        stored.members.pop()
    elif damage == "fragment":
        first = next(item["fragment_ordinal"] for item in stored.fragments if item["first_record_ordinal"] == 0)
        stored.fragments.pop(first + 1)
    elif damage == "crc":
        first = next(item["fragment_ordinal"] for item in stored.fragments if item["first_record_ordinal"] == 0)
        frame = stored.fragments[first]["encoded_fragment"]
        stored.fragments[first]["encoded_fragment"] = frame[:-1] + bytes((frame[-1] ^ 1,))
    else:
        stored.family["fragment_count"] = 1
    with pytest.raises(FamilyBlockError):
        await reader.selected_records(
            stored,
            stored.binding,
            stored.family,
            stored.records,
            limits=reader.FamilyBlockReadLimits(fragment_bytes=37),
        )


@pytest.mark.asyncio
async def test_complete_family_api_preserves_original_source_order_without_full_verifier(monkeypatch):
    stored = _StoredRows(_storage_case(fragment_bytes=65536))

    def forbidden(*_args, **_kwargs):
        raise AssertionError("ordinary detail must not enter the full verification consumer")

    monkeypatch.setattr(reader, "verified_family", forbidden)
    layout = stored.binding.layout
    target = read_core.PinnedReadTarget(1, 2, 3, 4, layout.definition.selection_profiles[0].profile_id)
    context = read_core._ReadContext(
        target, layout.definition, 1, 0, {"rates": 7}, {7: "rates"}, family_block_binding=stored.binding
    )
    family = SimpleNamespace(**stored.family)
    winner = SimpleNamespace(entity_binding_id=11, context_key_sha256=b"0" * 32)
    root = SimpleNamespace(root_revision_id=family.root_revision_id)
    actual = await reader.hydrate_families(stored, context, ((winner, family, root, None),), "scope")
    expected = sorted(
        (record.source_ordinal, record.revision_id) for (slot, _id), record in stored.records.items() if slot
    )
    assert [child.child_revision_id for child in actual[0].children] == [revision for _ordinal, revision in expected]
    assert actual[0].authorization_scope_sha256 == "scope"


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [False, True])
async def test_full_verifier_bridge_exhausts_native_rows_and_closes_cursors(damage):
    case = _storage_case(fragment_bytes=37)
    stored = _StoredRows(case)
    if damage:
        stored.family["storage_sha256"] = b"0" * 32
        with pytest.raises(FamilyBlockError, match="envelope differs"):
            await reader.verified_family(stored, stored.binding, stored.family, limits=case[1])
    else:
        with await reader.verified_family(stored, stored.binding, stored.family, limits=case[1]) as verified:
            assert list(verified.records()) == [case[2]]
            assert list(verified.records("rates")) == case[3]
    assert len(stored.results) == 2 and all(result.closed for result in stored.results)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", [None, "phase", "capability", "chain", "index", "seal", "family", "definition", "missing"]
)
async def test_projection_dispatch_preserves_completed_evidence_and_scope(monkeypatch, damage):
    from process.custom_import import query_projection_evidence as evidence_reader
    from process.custom_import.query_projection_binding import QueryProjectionUnavailable
    from tests.test_custom_import_family_block_projection_binding import _binding, _custody

    definition = _context().definition
    evidence = _evidence(definition)
    binding = _binding(definition)
    generation, manifest, seal = _generation(), _manifest(definition, evidence, **_custody(binding)), _seal(evidence)
    match damage:
        case "phase":
            manifest.phase = "indexes"
        case "capability":
            manifest.capability_sha256 = b"0" * 32
        case "chain":
            manifest.digest_chain_sha256 = b"0" * 32
        case "index":
            manifest.index_count += 1
        case "seal":
            seal.sealing_token_sha256 = b"0" * 32
        case "family":
            binding = replace(binding, family_id=24)
        case "definition":
            binding = replace(binding, layout=_storage_case()[0])
        case "missing":
            for name in evidence.seal_values():
                setattr(seal, name, None)
    metadata = SimpleNamespace(one_or_none=lambda: (generation, seal, manifest))
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(return_value=metadata))
    legacy = AsyncMock(side_effect=AssertionError("block reads must not resolve missing legacy tables"))
    monkeypatch.setattr(evidence_reader, "lock_projection_tables", legacy)
    if damage is not None:
        with pytest.raises(QueryProjectionUnavailable):
            await evidence_reader.read_projection_tables(
                session, generation, definition, snapshot_family_id=23, family_block_binding=binding
            )
    else:
        tables = await evidence_reader.read_projection_tables(
            session, generation, definition, snapshot_family_id=23, family_block_binding=binding
        )
        assert set(tables) == {"custom_import_derived_root", "custom_import_derived_child"}
        assert "custom_import_block_query_child" in str(select(tables["custom_import_derived_child"]))
    legacy.assert_not_awaited()
    session.execute.assert_awaited_once()
