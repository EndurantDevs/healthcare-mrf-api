# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure retained transport/codec fidelity; native pins remain separate proof."""

import json
import pickle
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import msgpack
import pytest
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import import family_shard_base as base_reader
from process.custom_import import family_shard_base_values as retained
from process.custom_import import family_shard_base_values_sql as queries
from process.custom_import import runner_codec as codec
from process.custom_import.family import RootFamily
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockLimits, FamilyBlockResourceLimit
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_records import FamilyRecord, record_from_wire, record_parts
from process.custom_import.family_block_storage_codec import StoredFamilyManifest
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_output import (
    AllocatedFamily,
    FamilyOutputWork,
    _unpack_row,
    build_family_output,
)
from process.custom_import.family_shard_plan import BaseFamilyHeader, cleanup_family_plan
from process.custom_import.materialization import TypedScalar
from tests.test_custom_import_family_shard_plan import _audit, _build, _Headers
from tests.test_custom_import_family_shard_retained import _legacy

_LIMITS = SourceFactLimits(64 * 1024**2, 4 * 1024**2, 1024**2)


def _records():
    root = FamilyRecord(
        19,
        201,
        71,
        {"npi": "1234567893", "score": 7, "enabled": True},
        {"score": TypedScalar("integer", "value", integer_value=999)},
    )
    child_records = [
        FamilyRecord(
            23 + index * 17,
            202,
            91 + index,
            {"detail_npi": "1234567893", "detail_id": key, "amount": Decimal("1.2300")},
            {"amount": TypedScalar("decimal", "value", decimal_value=Decimal("8.000000000000"))},
        )
        for index, key in enumerate(("z", "a"))
    ]
    return root, child_records


def _prefixed(native, prefix):
    return {prefix + name: value for name, value in native.items()}


def _family_rows(base, header):
    family_by_name = dict(
        family_revision_id=header.family_revision_id,
        root_record_id=header.root_record_id,
        root_revision_id=header.root_revision_id,
        entity_binding_id=header.entity_binding_id,
        family_sha256=header.family_sha256,
        child_count=header.child_count,
        dataset_id=base.generation.dataset_id,
        schema_revision_id=base.generation.schema_revision_id,
        producing_execution_id=base.generation.execution_id,
        producing_fence=base.generation.producing_fence,
        producing_token_sha256=base.generation.producing_token_sha256,
    )
    member_by_name = dict(
        generation_id=77,
        dataset_id=base.generation.dataset_id,
        definition_revision_id=base.generation.definition_revision_id,
        schema_revision_id=base.generation.schema_revision_id,
        root_record_id=header.root_record_id,
        family_revision_id=header.family_revision_id,
    )
    return family_by_name, member_by_name


def _scalar_rows(base, header, record, collection):
    slot = 0 if collection is None else 7
    kind = "root" if collection is None else "child"
    rows = []
    for name, scalar in record.scalars.items():
        field = base.definition.fields_by_id[name]
        native_by_name = dict(
            dataset_id=base.generation.dataset_id,
            schema_revision_id=base.generation.schema_revision_id,
            root_record_id=header.root_record_id,
            field_slot=field.field_slot,
            projection_slot=field.projection_slot,
            field_collection_slot=slot,
            field_type=scalar.field_type,
            value_state=scalar.value_state,
            **{kind + "_revision_id": record.revision_id},
            **{lane: getattr(scalar, lane) for lane in retained._LANES},
        )
        if collection is not None:
            native_by_name["collection_slot"] = slot
        rows.append(dict(family_revision_id=header.family_revision_id, **_prefixed(native_by_name, "scalar_")))
    return rows


def _legacy_rows(base, header, root, children, *, opaque=False):
    family, member = _family_rows(base, header)
    rows_by_role = dict(
        family=[_prefixed(family, "family_") | _prefixed(member, "member_")],
        root=[],
        child=[],
        root_scalar=[],
        child_scalar=[],
        pack=[],
        origin=[],
    )
    for collection, original_record in [(None, root), *(("details", child) for child in children)]:
        revision, _key = _legacy(
            base.definition,
            original_record,
            collection,
            payload_text=" exact opaque payload " if opaque else None,
            parent=header.typed_key[0],
        )
        revision.update(
            dataset_id=base.generation.dataset_id,
            definition_revision_id=base.generation.definition_revision_id,
            schema_revision_id=base.generation.schema_revision_id,
            root_record_id=header.root_record_id,
        )
        if collection is not None:
            revision["collection_slot"] = 7
        role = "root" if collection is None else "child"
        native_by_name = dict(family_revision_id=header.family_revision_id, **_prefixed(revision, "record_"))
        if collection is not None:
            edge_by_name = dict(
                family_revision_id=header.family_revision_id,
                dataset_id=base.generation.dataset_id,
                schema_revision_id=base.generation.schema_revision_id,
                root_record_id=header.root_record_id,
                collection_slot=7,
                child_revision_id=original_record.revision_id,
            )
            native_by_name |= _prefixed(edge_by_name, "edge_")
        rows_by_role[role].append(native_by_name)
        rows_by_role[role + "_scalar"].extend(_scalar_rows(base, header, original_record, collection))
    return rows_by_role


def _pack_row(base, pack_id, *, child=False):
    owner_by_name = {name: getattr(base.generation, name) for name in retained._SCOPE}
    pack_by_name = dict(
        pack_id=pack_id,
        execution_id=base.generation.execution_id,
        **owner_by_name,
        stream_slot=2 if child else 1,
        pack_ordinal=0,
        capture_bundle_id=123,
        record_count=3,
        pack_sha256=b"k" * 32,
        producing_fence=base.generation.producing_fence,
        producing_token_sha256=base.generation.producing_token_sha256,
    )
    execution_by_name = dict(execution_id=pack_by_name["execution_id"], **owner_by_name, capture_bundle_id=123)
    capture_by_name = dict(
        capture_bundle_id=123, **owner_by_name, stream_slot=pack_by_name["stream_slot"], capture_state="sealed"
    )
    stream_by_name = dict(
        **owner_by_name,
        stream_slot=pack_by_name["stream_slot"],
        record_kind="child" if child else "root",
        collection_slot=7 if child else None,
    )
    return (
        _prefixed(pack_by_name, "pack_")
        | _prefixed(execution_by_name, "execution_")
        | _prefixed(capture_by_name, "capture_")
        | _prefixed(stream_by_name, "stream_")
    )


def _block_rows(tmp_path, base, header, root, children):
    layout = FamilyBlockLayout(base.definition, {"details": 7})
    family = AllocatedFamily(
        header.family_revision_id,
        header.root_record_id,
        header.entity_binding_id,
        root,
        {"details": lambda: iter(children)},
        header.family_sha256,
    )
    keys = sorted({record_parts(base.definition, "details", child, retained=False)[1][:2] for child in children})
    with build_family_output(
        layout,
        31,
        [family],
        {key: (index, 1) for index, key in enumerate(keys, 1)},
        work=FamilyOutputWork(
            tmp_path / "block-output", FamilyBlockLimits(32 * 1024**2, 1024**2, 256, 4096), lambda _path: None
        ),
        shard_id=1,
        partition_sha256=b"q" * 32,
        source_record_count=3,
    ) as output:
        rows_by_role = {role: [] for role in ("family", "fragment", "revision_run", "pack", "origin")}
        for batch in output.batches(maximum_rows=64, maximum_bytes=1024**2):
            for role in ("family", "fragment", "revision_run"):
                rows_by_role[role] += [
                    dict(zip(layout.tables(31)[role].c.keys(), cells, strict=True)) for cells in batch.get(role, ())
                ]
    block_family = rows_by_role["family"][0]
    _unused, member = _family_rows(base, header)
    rows_by_role["family"] = [_prefixed(block_family, "family_") | _prefixed(member, "member_")]
    base.binding = SimpleNamespace(
        layout=layout,
        tables_by_role=layout.tables(31),
        manifest=lambda row: StoredFamilyManifest(
            layout.definition.digest,
            *(
                row[name]
                for name in (
                    "family_sha256",
                    "storage_sha256",
                    "retained",
                    "child_count",
                    "fragment_count",
                    "raw_bytes",
                    "stored_bytes",
                    "membership_count",
                )
            ),
        ),
    )
    return rows_by_role


def _pinned_context(request, monkeypatch, block):
    transaction = SimpleNamespace(is_active=True)
    identities = transaction, None, transaction, None
    monkeypatch.setattr(base_reader, "_read_transaction_identity", AsyncMock(return_value=identities))
    generation = SimpleNamespace(
        generation_id=77,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=666,
        producing_fence=8,
        producing_token_sha256=b"t" * 32,
        base_generation_id=None,
    )
    snapshot = SimpleNamespace(family_id=31, frozen_at=object(), origin_table_oid=None)
    session = SimpleNamespace(get=AsyncMock(return_value=snapshot), scalar=AsyncMock(return_value=None))
    base = base_reader.PinnedBase(
        session,
        generation,
        SimpleNamespace(materialization_sha256=b"m" * 32),
        request.definition,
        SimpleNamespace(child_collection_slots={"details": 7}),
        SimpleNamespace(family_id=31, legacy_snapshot=not block),
        None,
        request,
        identities,
        None,
        1024**2,
    )
    base._complete = True  # Explicit pure fixture native pin/EOF boundary only.
    return base


def _transport(monkeypatch, rows_by_role, calls):
    @asynccontextmanager
    async def rows(_base, statement, ids, _limits):
        names = set(statement.selected_columns.keys())
        role = (
            "family"
            if "family_family_revision_id" in names
            else "pack"
            if "pack_pack_id" in names
            else "root_scalar"
            if "scalar_root_revision_id" in names
            else "child_scalar"
            if "scalar_child_revision_id" in names
            else "root"
            if "record_root_revision_id" in names
            else "child"
            if "record_child_revision_id" in names
            else "fragment"
            if "encoded_fragment" in names
            else "revision_run"
        )
        calls.append((role, tuple(ids)))

        async def stream():
            for native in rows_by_role[role]:
                yield native

        yield stream()

    monkeypatch.setattr(queries, "rows", rows)


@asynccontextmanager
async def _case(tmp_path, monkeypatch, *, block=False, opaque=False, selected=True):
    async with _audit(tmp_path, monkeypatch, mode="upsert") as (request, audit):
        root, children = _records()
        key = codec.root_key_document(request.definition, root.values_by_field)
        digest = codec.new_family_hash(
            request.definition,
            RootFamily(
                ("1234567893",), root.values_by_field, {"details": tuple(child.values_by_field for child in children)}
            ),
        )
        header = BaseFamilyHeader(
            (key, codec.digest_text("root-key", key)), 500, 1001, root.revision_id, 3001, digest, len(children), 77, 31
        )
        request = replace(request, expected_base_generation_id=77, expected_pointer_version=3)
        plan = await _build(tmp_path, request, audit, headers=_Headers([header] if selected else []), scope=(77, 31))
        base = _pinned_context(request, monkeypatch, block)
        rows_by_role = (
            _block_rows(tmp_path, base, header, root, children)
            if block
            else _legacy_rows(base, header, root, children, opaque=opaque)
        )
        rows_by_role["pack"] = [_pack_row(base, 201), _pack_row(base, 202, child=True)]
        calls = []

        _transport(monkeypatch, rows_by_role, calls)
        try:
            yield base, plan, rows_by_role, root, children, calls
        finally:
            cleanup_family_plan(plan)


async def _run(tmp_path, base, plan, *, limits=_LIMITS, check=None):
    registered_paths = []
    descriptor = await retained.build_retained_input(
        base, plan, work=retained.RetainedWork(tmp_path / "retained", limits, registered_paths.append, check)
    )
    assert registered_paths == [tmp_path / "retained"]
    assert pickle.loads(pickle.dumps(descriptor)) == descriptor
    return descriptor


@pytest.mark.parametrize("block,opaque", [(False, False), (False, True), (True, False)])
async def test_complete_selected_originals_keep_ids_independent_scalars_bytes_and_provenance(
    tmp_path, monkeypatch, block, opaque
):
    async with _case(tmp_path, monkeypatch, block=block, opaque=opaque) as (base, plan, _rows, root, children, calls):
        descriptor = await _run(tmp_path, base, plan)
        try:
            frames = list(retained.read_retained_input(descriptor))
            originals = [
                (
                    frame[2],
                    record_from_wire(
                        base.definition,
                        None if frame[2] == 0 else "details",
                        msgpack.unpackb(frame[3], raw=False),
                        retained=True,
                    )[0],
                )
                for frame in frames
                if frame[0] == "record"
            ]
            assert len(originals) == 3 and descriptor.counts_by_role == dict(
                family=1, root=1, child=2, scalar=3, pack=2, origin=0
            )
            expected_by_revision = {
                original_record.revision_id: original_record for original_record in [root, *children]
            }
            for slot, original_record in originals:
                original = expected_by_revision[original_record.revision_id]
                assert (original_record.pack_id, original_record.source_ordinal, original_record.scalars) == (
                    original.pack_id,
                    original.source_ordinal,
                    original.scalars,
                )
                assert record_parts(base.definition, None if slot == 0 else "details", original_record, retained=True)[
                    0
                ][3] == (
                    " exact opaque payload "
                    if opaque
                    else record_parts(base.definition, None if slot == 0 else "details", original, retained=True)[0][3]
                )
            assert {role for role, _ids in calls} == (
                {"family", "fragment", "revision_run", "pack"}
                if block
                else {"family", "root", "child", "root_scalar", "child_scalar", "pack"}
            )
            assert all(ids == (1001,) for role, ids in calls if role != "pack")
            assert dict(_unpack_row(next(frame[1] for frame in frames if frame[0] == "pack")))["pack_pack_id"] == 201
        finally:
            retained.cleanup_retained_input(descriptor)
        assert not Path(descriptor.root).exists()


@pytest.mark.parametrize(
    "damage",
    [
        "membership",
        "family-producer",
        "parent",
        "child-count",
        "scalar-slot",
        "scalar-projection",
        "scalar-collection",
        "scalar-type",
        "pack-producer",
        "capture",
        "stream",
        "missing-pack",
        "extra-scalar",
    ],
)
async def test_original_scope_bijections_and_declared_scalar_tuples_fail_closed(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as (base, plan, rows, _root, _children, _calls):
        _damage(rows, damage)
        with pytest.raises((FamilyBlockUnavailable, ValueError, TypeError)):
            await _run(tmp_path, base, plan)
        assert not (tmp_path / "retained").exists()


def _damage(rows_by_role, damage):
    if damage == "membership":
        rows_by_role["family"][0]["member_generation_id"] += 1
        return
    if damage == "family-producer":
        rows_by_role["family"][0]["family_producing_fence"] += 1
        return
    if damage == "parent":
        rows_by_role["child"][0]["record_canonical_parent_key"] = "different original parent"
        return
    if damage == "child-count":
        rows_by_role["child"].pop()
        return
    if damage.startswith("scalar-"):
        column = {
            "scalar-slot": "field_slot",
            "scalar-projection": "projection_slot",
            "scalar-collection": "field_collection_slot",
            "scalar-type": "field_type",
        }[damage]
        rows_by_role["child_scalar"][0]["scalar_" + column] = "integer" if column == "field_type" else 99
        return
    if damage == "extra-scalar":
        rows_by_role["child_scalar"].append(dict(rows_by_role["child_scalar"][0], scalar_child_revision_id=999999))
        return
    if damage == "missing-pack":
        rows_by_role["pack"].pop()
        return
    column = {
        "pack-producer": "pack_producing_fence",
        "capture": "capture_capture_state",
        "stream": "stream_collection_slot",
    }[damage]
    rows_by_role["pack"][1 if damage == "stream" else 0][column] = "open" if damage == "capture" else 9999


@pytest.mark.parametrize("role", ["fragment", "revision_run"])
async def test_block_full_envelope_and_observed_identity_directory_are_required(tmp_path, monkeypatch, role):
    async with _case(tmp_path, monkeypatch, block=True) as (base, plan, rows, _root, _children, _calls):
        if role == "fragment":
            rows[role][0]["encoded_fragment"] = b"corrupt original fragment"
        else:
            rows[role][0]["last_revision_id"] += 1
        with pytest.raises(ValueError):
            await _run(tmp_path, base, plan)
        assert not (tmp_path / "retained").exists()


async def test_zero_retained_checks_applicability_and_plan_eof_without_value_queries(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, selected=False) as (base, plan, _rows, _root, _children, calls):
        descriptor = await _run(tmp_path, base, plan)
        assert descriptor.counts_by_role == dict(family=0, root=0, child=0, scalar=0, pack=0, origin=0)
        assert list(retained.read_retained_input(descriptor)) == [("complete", descriptor.counts)]
        assert calls == [] and base.session.get.await_count == 1
        retained.cleanup_retained_input(descriptor)


async def test_resource_failure_and_closed_scope_refusal_cleanup(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as (base, plan, _rows, _root, _children, _calls):
        with pytest.raises(SourceFactBudgetExceeded):
            await _run(tmp_path, base, plan, limits=SourceFactLimits(64, 64, 64))
        assert not (tmp_path / "retained").exists()
        base._complete = False
        with pytest.raises(FamilyBlockUnavailable, match="EOF/scope"):
            await _run(tmp_path, base, plan)
        assert not (tmp_path / "retained").exists()


def test_native_statements_keep_scopes_joins_and_bounded_array_admission():
    base = SimpleNamespace(
        binding=None,
        pin=SimpleNamespace(family_id=31, legacy_snapshot=True),
        generation=SimpleNamespace(generation_id=77),
    )
    native = queries.tables(base)
    statements_by_role = dict(
        family=queries.family_statement(base, native),
        pack=queries.pack_statement(native),
        **queries.legacy_statements(base, native),
    )
    for statement in statements_by_role.values():
        compiled = str(statement.compile(dialect=dialect()))
        assert "ANY (" in compiled and "retained_ids" in compiled
        assert "ORDER BY" in compiled
        compiled_census = str(queries.census_statement(statement).compile(dialect=dialect()))
        assert "count(*)" in compiled_census and "AS BIGINT" in compiled_census
    assert "LEFT OUTER JOIN" in str(statements_by_role["child"])
    assert "producing_token_sha256" in str(statements_by_role["pack"])
    census = SimpleNamespace(row_count=74000000, row_bytes=1024)
    assert queries.fetch_rows(census, SourceFactLimits(64 * 1024**2, 16 * 1024**2, 16 * 1024**2)) == 4096
    with pytest.raises(FamilyBlockResourceLimit):
        queries.fetch_rows(SimpleNamespace(row_count=1, row_bytes=2 * 1024**2), _LIMITS)


def test_legacy_origin_statement_retains_all_four_original_checks_and_selected_scope():
    context = SimpleNamespace(
        family=SimpleNamespace(family_id=31), base_family=SimpleNamespace(family_id=29), quoted='"control"'
    )
    statement = str(queries.origin_statement(context, check=True))
    assert "retained_origins AS MATERIALIZED" in statement and "retained_ids" in statement
    assert all(
        code in statement
        for code in ("legacy_copy_root", "legacy_copy_child", "legacy_copy_missing_child", "legacy_copy_omitted_child")
    )
    assert '"ci_snapshot_29"' in statement and ":p_9" in statement
    assert str(queries.census_statement(queries.origin_statement(context)).compile(dialect=dialect()))


class _NativeCursor:
    def __init__(self, rows, *, late_error=False, close_error=False):
        self.rows = iter(rows)
        self.late_error, self.close_error = late_error, close_error
        self.closed = False

    def mappings(self):
        return self

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.rows)
        except StopIteration:
            if self.late_error:
                raise RuntimeError("native late EOF failure")
            raise StopAsyncIteration from None

    async def close(self):
        self.closed = True
        if self.close_error:
            raise RuntimeError("native close failure")


@pytest.mark.parametrize("failure", ["none", "late-eof", "close", "prefix", "census"])
async def test_native_transport_requires_real_eof_count_and_successful_close(failure):
    cursor = _NativeCursor([{"x": 1}, {"x": 2}], late_error=failure == "late-eof", close_error=failure == "close")
    census = SimpleNamespace(row_count=3 if failure == "census" else 2, row_bytes=1024)
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(one=lambda: census)), stream=AsyncMock(return_value=cursor)
    )
    base = SimpleNamespace(session=session, check=AsyncMock())
    from sqlalchemy import literal, select

    statement = select(literal(1).label("x"))
    observed_rows = []
    if failure == "none":
        async with queries.rows(base, statement, [1, 2], _LIMITS) as rows:
            observed_rows.extend([row async for row in rows])
        assert observed_rows == [{"x": 1}, {"x": 2}]
        assert session.stream.call_args.args[0].get_execution_options()["yield_per"] == 1024
    else:
        with pytest.raises((RuntimeError, FamilyBlockUnavailable)):
            async with queries.rows(base, statement, [1, 2], _LIMITS) as rows:
                if failure == "prefix":
                    observed_rows.append(await anext(rows))
                else:
                    observed_rows.extend([row async for row in rows])
    assert cursor.closed


async def test_unregistered_origin_table_refuses_even_when_retained_selection_is_empty(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, selected=False) as (base, plan, _rows, _root, _children, calls):
        base.session.scalar = AsyncMock(return_value=9999)
        with pytest.raises(FamilyBlockUnavailable, match="registration differs"):
            await _run(tmp_path, base, plan)
        assert calls == [] and not (tmp_path / "retained").exists()


async def test_late_value_failure_never_returns_descriptor_or_deletes_original_plan(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as (base, plan, _rows, _root, _children, _calls):
        original = queries.rows

        @asynccontextmanager
        async def damaged(*arguments):
            async with original(*arguments) as rows:

                async def stream():
                    async for native in rows:
                        yield native
                    raise RuntimeError("late value EOF failure")

                yield stream()

        monkeypatch.setattr(queries, "rows", damaged)
        with pytest.raises(RuntimeError, match="late value EOF"):
            await _run(tmp_path, base, plan)
        assert not (tmp_path / "retained").exists() and Path(plan.root).exists()


@pytest.mark.parametrize("block", [False, True])
async def test_typed_replay_keeps_original_identity_and_scalar_declarations(tmp_path, monkeypatch, block):
    async with _case(tmp_path, monkeypatch, block=block) as (base, plan, _rows, root, children, _calls):
        descriptor = await _run(tmp_path, base, plan)
        try:
            restored_records = list(retained.read_retained_records(descriptor))
            assert descriptor.collection_slots == (("details", 7),)
            expected_by_revision = {record.revision_id: record for record in [root, *children]}
            for original in restored_records:
                assert original.family_revision_id == 1001
                assert original.record.scalars == expected_by_revision[original.record.revision_id].scalars
                assert original.scalar_declarations
                assert all(declaration[2] == original.collection_slot for declaration in original.scalar_declarations)
                assert original.lineage["source_ordinal"] == original.record.source_ordinal
        finally:
            retained.cleanup_retained_input(descriptor)


@pytest.mark.parametrize("layout", ["canonical", "legacy", "unresolved"])
async def test_origin_ancestor_reuses_readonly_identity_binding_and_exact_leaf_pin(tmp_path, monkeypatch, layout):
    async with _case(tmp_path, monkeypatch) as (base, _plan, _rows, _root, _children, _calls):
        base.generation.base_generation_id = 88
        ancestor = SimpleNamespace(
            generation_id=88,
            dataset_id=base.generation.dataset_id,
            schema_revision_id=base.generation.schema_revision_id,
        )
        snapshot = SimpleNamespace(family_id=31, frozen_at=object(), origin_table_oid=900)
        base.session.get = AsyncMock(side_effect=[snapshot, ancestor])
        base.session.scalar = AsyncMock(return_value=900)
        identity = AsyncMock(return_value=(ancestor, object()))
        definition = AsyncMock(return_value=(base.definition, base.registry))
        pin = SimpleNamespace(family_id=None if layout == "canonical" else 29, legacy_snapshot=layout == "legacy")
        binding = AsyncMock(return_value=(pin, None))
        projection, leaf = AsyncMock(), AsyncMock()
        monkeypatch.setattr(queries, "_base_identity", identity)
        monkeypatch.setattr(queries, "_base_definition", definition)
        monkeypatch.setattr(queries, "_base_binding", binding)
        monkeypatch.setattr(queries, "validate_sealed_projection", projection)
        monkeypatch.setattr(queries, "_verify_leaf_pins", leaf)
        monkeypatch.setattr(queries, "_control_schema", AsyncMock(return_value='"control"'))
        if layout == "unresolved":
            with pytest.raises(FamilyBlockUnavailable, match="original legacy relations"):
                await queries.origin_context(base)
        else:
            context = await queries.origin_context(base)
            assert context.ancestor_scope == (88, pin.family_id)
        assert (
            identity.await_count
            == definition.await_count
            == binding.await_count
            == projection.await_count
            == leaf.await_count
            == 1
        )
        assert identity.call_args.args[1].expected_base_generation_id == 88
        assert leaf.call_args.args[-1] is base._transactions


@pytest.mark.parametrize("damage", ["checksum", "extra-file"])
async def test_closed_retained_replay_refuses_tamper_and_changed_custody(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as (base, plan, _rows, _root, _children, _calls):
        descriptor = await _run(tmp_path, base, plan)
        target = Path(descriptor.root) / "retained-input.mpk"
        saved = target.read_bytes()
        if damage == "checksum":
            target.chmod(0o600)
            target.write_bytes(saved[:-1] + bytes([saved[-1] ^ 1]))
        else:
            (Path(descriptor.root) / "extra").write_bytes(b"extra")
        with pytest.raises((FamilyBlockUnavailable, ValueError)):
            list(retained.read_retained_records(descriptor))
        if damage == "extra-file":
            with pytest.raises(FamilyBlockUnavailable):
                retained.cleanup_retained_input(descriptor)
            (Path(descriptor.root) / "extra").unlink()
        else:
            target.write_bytes(saved)
        retained.cleanup_retained_input(descriptor)


async def test_owner_cancel_during_retained_pass_removes_only_owned_output(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as (base, plan, _rows, _root, _children, _calls):
        polls = []

        def check():
            polls.append(None)
            if len(polls) == 30:
                raise RuntimeError("owner retained cancellation")

        with pytest.raises(RuntimeError, match="owner retained cancellation"):
            await _run(tmp_path, base, plan, check=check)
        assert len(polls) == 30 and not (tmp_path / "retained").exists() and Path(plan.root).exists()


def test_selected_batches_charge_actual_key_bytes_without_a_new_logical_cap(tmp_path):
    work = retained._Scratch(
        retained.PlanWork(tmp_path / "work", SourceFactLimits(1024**2, 100000, 6000), lambda _path: None)
    )
    try:
        path = work.write([(1, ("long original key" * 1000,)), (2, ("x",))])
        with pytest.raises(FamilyBlockResourceLimit, match="header exceeds"):
            list(retained._batches(work, path))
        assert work.root.exists()
    finally:
        work.close()


def test_native_scalar_values_preserve_decimal_date_timestamp_null_and_absence():
    from tests.test_custom_import_family_shard_output import prepared

    layout, family, child_records, _keys = prepared(1)
    base = SimpleNamespace(
        definition=layout.definition,
        registry=SimpleNamespace(child_collection_slots={"rates": 7}),
        generation=SimpleNamespace(dataset_id=101, schema_revision_id=103),
    )
    header = SimpleNamespace(family_revision_id=family.family_revision_id, root_record_id=family.root_record_id)
    child = child_records[0]
    declarations = _scalar_rows(base, header, child, "rates")
    observed_by_field = {}
    for native in declarations:
        # Actual driver types survive the same scalar extension spool path.
        restored = _unpack_row(retained._pack_row(native))
        name, typed, declaration = retained._decode_scalar_row(
            base,
            retained._prefix(restored, "scalar_"),
            "rates",
            family.root_record_id,
            child.revision_id,
            retained._field_by_slot(base),
        )
        observed_by_field[name] = typed
        assert declaration[2] == 7
    assert observed_by_field == child.scalars
    assert observed_by_field["amount"].decimal_value.as_tuple() == Decimal("1.230000000000").as_tuple()
    assert observed_by_field["stamp"].timestamp_value.utcoffset() == child.scalars["stamp"].timestamp_value.utcoffset()
    assert observed_by_field["note"].value_state == "null" and "rate_npi" not in observed_by_field


async def test_explicit_no_base_zero_input_still_consumes_the_complete_plan(tmp_path, monkeypatch):
    async with _audit(tmp_path, monkeypatch, mode="upsert") as (request, audit):
        plan = await _build(tmp_path, request, audit)
        try:
            descriptor = await _run(tmp_path, None, plan)
            assert descriptor.base_identity is None and descriptor.counts_by_role["family"] == 0
            assert list(retained.read_retained_records(descriptor)) == []
            retained.cleanup_retained_input(descriptor)
            contradictory = replace(plan, request_scope=plan.request_scope[:6] + (77, 3) + plan.request_scope[8:])
            with pytest.raises(FamilyBlockUnavailable, match="actual live base"):
                await _run(tmp_path, None, contradictory)
        finally:
            cleanup_family_plan(plan)
