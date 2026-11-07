# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual closed retained inputs/output; native owner and base transport is fake."""

from contextlib import asynccontextmanager, closing
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_retained_allocate as allocation
from process.custom_import import family_shard_retained_encoding as encoding
from process.custom_import import family_shard_retained_validation as retained_validation
from process.custom_import import runner_codec as codec
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyBlockLimits, FamilyBlockResourceLimit
from process.custom_import.family_block_keys import iter_key_dictionary
from process.custom_import.family_block_records import record_parts
from process.custom_import.family_block_validate import check_family_records
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_output import FamilyOutputWork, build_family_output
from process.custom_import.family_shard_plan import PlanWork, _Scratch
from tests import test_custom_import_family_shard_admission as admission_fixture
from tests import test_custom_import_family_shard_base_values as base_fixture
from tests import test_custom_import_family_shard_original_validation as original_fixture
from tests import test_custom_import_family_shard_winner_validation as winner_fixture
from tests.test_custom_import_family_shard_output import prepared, verified

_LIMITS = FamilyBlockLimits(64 * 1024**2, 4 * 1024**2, 4096, 1024**2)


def _global_metadata(monkeypatch, include_retained):
    original = admission_fixture._metadata
    original_capture = winner_fixture._wide_capture_case
    definition = winner_fixture._definition()
    _root, children = base_fixture._records()
    retained_keys = {
        (codec.digest_text("child-key", key), key)
        for child in children
        for key in (codec.child_key_document(definition, "details", child.values_by_field),)
    }

    def metadata(source):
        records, queries = original(source)
        keys = iter_key_dictionary(sorted(set(queries) | (retained_keys if include_retained else set())))
        return records, {(row.child_key_sha256, row.canonical_child_key): row[:2] for row in keys}

    async def capture(tmp_path, rows=None, **options):
        rows = rows or {
            "root_source": [[winner_fixture._root()]],
            "detail_source": [[winner_fixture._child(key="a"), winner_fixture._child(key="b")]],
        }
        return await original_capture(tmp_path, rows, **options)

    monkeypatch.setattr(admission_fixture, "_metadata", metadata)
    monkeypatch.setattr(original_fixture, "_metadata", metadata)
    monkeypatch.setattr(winner_fixture, "_wide_capture_case", capture)


def _encode(case, tmp_path, *, query_keys=None, context=None, comparison=None, bounds=None, check=None):
    return encoding.build_retained_output(
        case.context if context is None else context,
        case.destination,
        case.comparison if comparison is None else comparison,
        case.query_keys if query_keys is None else query_keys,
        work=FamilyOutputWork(tmp_path / "encoded", bounds or _LIMITS, case.registered.append, check),
        shard_id=37,
    )


from tests.test_custom_import_family_shard_retained_allocate import _produce_destination


@asynccontextmanager
async def _ready_case(tmp_path, native, context, destination, comparison, *, encode):
    case = SimpleNamespace(
        native=native,
        context=context,
        destination=destination,
        comparison=comparison,
        registered=[],
        reads=[],
        output=None,
    )

    def query_keys():
        case.reads.append(True)
        return iter(tuple(native.keys))

    case.query_keys = query_keys
    try:
        if encode:
            case.output = _encode(case, tmp_path)
        yield case
    finally:
        if case.output is not None:
            case.output.close()


@asynccontextmanager
async def _case(tmp_path, monkeypatch, *, encode=True, empty=False, include_retained=True):
    if not empty:
        _global_metadata(monkeypatch, include_retained)
        async with winner_fixture._retained_case(tmp_path, monkeypatch) as (case, originals):
            async with _ready_case(
                tmp_path, case[0], case[1], originals.destination, originals.comparison, encode=encode
            ) as ready:
                yield ready
        return
    async with winner_fixture._case(tmp_path, monkeypatch, empty=True) as case:
        native, context = case[:2]
        winner_fixture._dictionary_boundary(monkeypatch, context.common)
        destination = await _produce_destination(
            None, context, work=winner_fixture._allocation_work(tmp_path, "destination")
        )
        try:
            comparison = await retained_validation.verify_retained_destination(
                context, destination, work=winner_fixture._allocation_work(tmp_path, "retained-comparison")
            )
            async with _ready_case(tmp_path, native, context, destination, comparison, encode=encode) as ready:
                yield ready
        finally:
            allocation.cleanup_retained_destination(destination)


def _rows(output):
    by_role = {role: [] for role in output.receipt["role_counts"]}
    for batch in output.batches(maximum_rows=1, maximum_bytes=4 * 1024**2):
        for role, rows in batch.items():
            by_role[role].extend(rows)
    return by_role


async def test_actual_retained_input_encodes_exact_assigned_records_and_original_partition(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        output, context = case.output, case.context
        assert output.receipt["source_record_count"] == 0
        assert output.receipt["partition_sha256"] == bytes.fromhex(context.common.source.source.partition_sha256)
        assert output.receipt["partition_sha256"] != case.destination.sha256
        assert case.reads == [True]
        rows_by_role = _rows(output)
        assert len(rows_by_role["family"]) == 1 and len(rows_by_role["query_record"]) == 2
        root = tmp_path / "expected_records"
        with allocation.retained_destination_families(
            case.destination,
            context.common.request.definition,
            work=PlanWork(root, winner_fixture._LIMITS, lambda _path: None),
        ) as families:
            plan, before, assigned = next(families)
            assert assigned.root.sparse is not None
            with verified(rows_by_role, context.common.layout, _LIMITS) as decoded:
                expected_records = [(0, assigned.root)]
                expected_records += [(7, child_record) for child_record in assigned.children_by_collection["details"]()]
                keys_by_identity = {
                    (rank, variant): (digest, text) for digest, text, rank, variant in case.query_keys()
                }
                queries = (
                    (query_row[1], query_row[0], query_row[2], *keys_by_identity[query_row[3:5]], *query_row[5:])
                    for query_row in rows_by_role["query_record"]
                )
                columns = context.common.layout.tables(context.common.source.snapshot_family_id)["family"].c.keys()
                family_by_name = dict(zip(columns, rows_by_role["family"][0], strict=True))
                counts = check_family_records(
                    context.common.layout,
                    family_by_name,
                    decoded,
                    iter(expected_records),
                    root_key_contract=codec.root_key_contract_hash(context.common.request.definition),
                    root_key=(plan.typed_key[1], plan.typed_key[0]),
                    entity=("npi", "1234567893", codec.digest_text("entity:npi", "1234567893")),
                    query_rows=queries,
                )
                assert counts == (2, case.comparison.root_scalar_count, case.comparison.child_scalar_count)
                assert decoded.manifest.family_sha256 == before.expected_family_sha256
            assert list(families) == []
        assert not root.exists()


async def test_empty_retained_output_requires_complete_dictionary_and_has_zero_roles(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, empty=True) as case:
        assert not any(case.output.receipt["role_counts"].values())
        assert list(case.output.batches(maximum_rows=1, maximum_bytes=1024**2)) == []
        assert case.reads == [True]


@pytest.mark.parametrize("damage", ["missing_unused", "duplicate", "rank", "hash", "order", "factory"])
async def test_full_global_dictionary_refuses_changed_unused_rows_or_invalid_order(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch, encode=False) as case:
        keys = list(case.query_keys())
        assert len(keys) == 3
        match damage:
            case "missing_unused":
                keys = [row for row in keys if '"b"' not in row[1]]
            case "duplicate":
                keys.insert(1, keys[0])
            case "rank":
                keys[0] = (*keys[0][:2], 99, keys[0][3])
            case "hash":
                keys[0] = (b"x" * 32, *keys[0][1:])
            case "order":
                keys.reverse()
        factory = iter(keys) if damage == "factory" else lambda: iter(keys)
        with pytest.raises((ValueError, FamilyBlockUnavailable)):
            _encode(case, tmp_path, query_keys=factory)
        assert not (tmp_path / "encoded").exists()
        assert not (tmp_path / "encoded-retained-inputs").exists()


@pytest.mark.parametrize("damage", ["scope", "destination", "counts", "duplicate_counts", "split"])
async def test_original_comparison_scope_is_mandatory_before_output(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch, encode=False) as case:
        changes_by_damage = {
            "scope": {"scope": ()},
            "destination": {"destination_sha256": b"x" * 32},
            "counts": {"counts": tuple((name, value + (name == "child")) for name, value in case.comparison.counts)},
            "duplicate_counts": {"counts": (*case.comparison.counts, case.comparison.counts[0])},
            "split": {"root_scalar_count": True},
        }
        with pytest.raises(FamilyBlockUnavailable, match="comparison scope"):
            _encode(case, tmp_path, comparison=replace(case.comparison, **changes_by_damage[damage]))
        assert not case.reads and not (tmp_path / "encoded").exists()


async def test_cancel_and_total_spool_failure_preserve_original_owners(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, encode=False) as case:

        def stopped():
            raise RuntimeError("owner cancelled")

        with pytest.raises(RuntimeError, match="owner cancelled"):
            _encode(case, tmp_path, check=stopped)
        with pytest.raises(FamilyBlockResourceLimit, match="total spool budget"):
            _encode(case, tmp_path, bounds=replace(_LIMITS, max_spool_bytes=4 * 1024**2))
        assert not (tmp_path / "encoded").exists() and not (tmp_path / "encoded-retained-inputs").exists()
        assert list(allocation.read_retained_destination(case.destination))


async def test_unknown_required_key_refuses_even_with_a_complete_valid_common_dictionary(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, encode=False, include_retained=False) as case:
        with pytest.raises(FamilyBlockUnavailable, match="absent from the original global dictionary"):
            _encode(case, tmp_path)
        assert not (tmp_path / "encoded").exists() and not (tmp_path / "encoded-retained-inputs").exists()


async def test_small_current_family_buffers_preserve_complete_output_bytes(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch, encode=False) as case:
        with _encode(case, tmp_path) as first:
            original_sha, original_receipt = first.sha256, first.receipt
        with _encode(case, tmp_path, bounds=replace(_LIMITS, memory_bytes=4096)) as small:
            assert small.sha256 == original_sha and small.receipt == original_receipt
            assert len(_rows(small)["family"]) == 1
        assert case.reads == [True, True]


def test_current_family_disk_lookup_precedes_revision_sort_and_preserves_canonical_wire(tmp_path):
    layout, family, children, _old_keys = prepared(4)
    children = sorted(children, key=lambda child: record_parts(layout.definition, "rates", child, retained=True)[1])
    assigned_records = tuple(replace(child, revision_id=10000 - index * 7) for index, child in enumerate(children))
    family = replace(family, retained=True, children_by_collection={"rates": lambda: iter(reversed(assigned_records))})
    family_order = b"f" * 32, "retained family"
    limits = SourceFactLimits(32 * 1024**2, 4 * 1024**2, 65536)
    work = _Scratch(PlanWork(tmp_path / "lookup", limits, lambda _path: None))
    try:
        pairs = (record_parts(layout.definition, "rates", child, retained=True)[1][:2] for child in children)
        path = work.write(
            (family_order, (key_row[2], key_row[3]), key_row[:2]) for key_row in iter_key_dictionary(pairs)
        )
        with closing(encoding._FamilyKeys(work, path)) as keys:
            keys.begin(family_order)
            assert len(keys) == len(children)
            with build_family_output(
                layout,
                17,
                (family,),
                keys,
                work=FamilyOutputWork(tmp_path / "mapped", _LIMITS, lambda _path: None),
                shard_id=1,
                partition_sha256=b"p" * 32,
                source_record_count=0,
            ) as output:
                rows_by_role = _rows(output)
                assert [key_row[0] for key_row in rows_by_role["query_record"]] == [
                    child.revision_id for child in assigned_records
                ]
                with verified(rows_by_role, layout, _LIMITS) as decoded:
                    assert [
                        record_parts(layout.definition, "rates", child, retained=True)[0]
                        for child in decoded.records("rates")
                    ] == [
                        record_parts(layout.definition, "rates", child, retained=True)[0] for child in assigned_records
                    ]
            keys.end()
            keys.finish()
            with pytest.raises(ValueError, match="outlived"):
                keys[(b"x" * 32, "foreign")]
    finally:
        work.close()
