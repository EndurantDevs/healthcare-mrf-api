# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure completed family output, exact retained evidence and owned spool cleanup."""

import json
import pickle
from collections.abc import Mapping
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from process.custom_import import family_shard_output as output_module
from process.custom_import import runner_codec
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import RootFamily
from process.custom_import.family_block_codec import FamilyBlockLimits, FamilyBlockResourceLimit
from process.custom_import.family_block_directory import RevisionMembership
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_records import FamilyRecord, SparseRecord, record_parts
from process.custom_import.family_block_scalar_layout import pack_query_scalars, query_scalar_fields
from process.custom_import.family_block_storage_codec import StoredFamilyManifest, verify_stored_family
from process.custom_import.family_block_store import ShardOutputProof, check_copy_batch
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded
from process.custom_import.family_shard_output import AllocatedFamily, FamilyOutputWork, build_family_output
from process.custom_import.materialization import TypedScalar
from tests.test_custom_import_family_block_codec import _case, _family_hash


def prepared(count=7):
    definition, root, child_records = _case(count)
    document = json.loads(definition.canonical)
    projection_by_field = {"day": 5, "stamp": 6, "note": 7, "enabled": 8}
    for field in document["schema"]["children"][0]["fields"]:
        if field["id"] in projection_by_field:
            field["projection_slot"] = projection_by_field[field["id"]]
    document["query"]["child"]["fields"] += list(projection_by_field)
    definition = CustomImportDefinition.from_mapping(document)
    root = replace(
        root,
        scalars={
            "npi": TypedScalar("string", "value", string_value=root.values_by_field["npi"]),
            "display_name": TypedScalar("string", "value", string_value="Independent projection"),
        },
    )
    stamp = datetime(2026, 2, 3, 12, 34, 56, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    child_records = tuple(
        replace(
            child,
            scalars={
                "service_code": TypedScalar("string", "value", string_value=child.values_by_field["service_code"]),
                "amount": TypedScalar("decimal", "value", decimal_value=Decimal("1.230000000000")),
                "day": TypedScalar("date", "value", date_value=date(2026, 2, 3)),
                "stamp": TypedScalar("timestamp", "value", timestamp_value=stamp),
                "note": TypedScalar("string", "null"),
                "enabled": TypedScalar("boolean", "value", boolean_value=False),
            },
        )
        for child in reversed(child_records)
    )
    layout = FamilyBlockLayout(definition, {"rates": 7})
    family = AllocatedFamily(
        79, 23, 47, root, {"rates": lambda: iter(child_records)}, _family_hash(definition, root, child_records)
    )
    key_ids_by_key = {}
    ordered_keys = sorted(
        child_records, key=lambda child: record_parts(definition, "rates", child, retained=False)[1][:2]
    )
    for rank, child in enumerate(ordered_keys, 1):
        _wire, parts = record_parts(definition, "rates", child, retained=False)
        key_ids_by_key[(parts[0], parts[1])] = (rank, 3)
    return layout, family, child_records, key_ids_by_key


def limits(**changes):
    return replace(FamilyBlockLimits(32 * 1024**2, 1024**2, 4096, 65536), **changes)


def build(tmp_path, layout, family, keys, *, bounds=None, name="output", registered=None, check=None):
    registered = [] if registered is None else registered
    return build_family_output(
        layout,
        17,
        [family],
        keys,
        work=FamilyOutputWork(tmp_path / name, bounds or limits(), registered.append, check),
        shard_id=4,
        partition_sha256=b"p" * 32,
        source_record_count=27,
    )


def rows(spool, layout, *, maximum_rows=5):
    rows_by_role = {role: [] for role in spool.receipt["role_counts"]}
    proof = ShardOutputProof(layout, 4, b"p" * 32, 27)
    for batch in spool.batches(maximum_rows=maximum_rows, maximum_bytes=2 * 1024**2):
        check_copy_batch(layout, 17, batch, maximum_rows=maximum_rows, maximum_bytes=2 * 1024**2)
        for role, items in batch.items():
            proof.add(role, items)
            rows_by_role[role].extend(items)
    assert proof.receipt() == spool.receipt
    return rows_by_role


def verified(rows, layout, bounds):
    row = rows["family"][0]
    manifest = StoredFamilyManifest(layout.definition.digest, *row[4:12])
    fragments = (((item[1], item[2], *item[4:]), item[3]) for item in rows["fragment"])
    memberships = (RevisionMembership(*item[1:]) for item in rows["revision_run"])
    return verify_stored_family(
        layout.definition, layout.collection_slots, fragments, memberships, manifest, limits=bounds
    )


@pytest.mark.parametrize("phase", ["codec", "storage", "initial_hash", "final_hash"])
def test_owner_callback_interrupts_proof_and_hashing_with_exact_cleanup(tmp_path, monkeypatch, phase):
    layout, family, _children, keys = prepared(3)
    armed, checks = [], []

    def current():
        checks.append(True)
        if armed:
            raise RuntimeError("owner cancelled")

    target = {"codec": "verify_family", "storage": "verify_stored_family"}.get(phase, "_output_digest")
    original = getattr(output_module, target)

    def interrupted(*arguments, **options):
        if target != "_output_digest":
            assert options["check"] is current
        armed.append(True)
        return original(*arguments, **options)

    if phase != "final_hash":
        monkeypatch.setattr(output_module, target, interrupted)
        with pytest.raises(RuntimeError, match="owner cancelled"):
            build(tmp_path, layout, family, keys, check=current)
    else:
        with build(tmp_path, layout, family, keys, check=current) as spool:
            monkeypatch.setattr(output_module, target, interrupted)
            with pytest.raises(RuntimeError, match="owner cancelled"):
                list(spool.batches(maximum_rows=2, maximum_bytes=1024**2))
    assert armed and checks and not (tmp_path / "output").exists()


def test_fresh_output_has_complete_native_rows_original_scalars_and_exact_proof(tmp_path):
    layout, family, children, keys = prepared()
    registered_roots = []
    with build(tmp_path, layout, family, keys, registered=registered_roots) as spool:
        all_rows = rows(spool, layout)
        assert registered_roots == [tmp_path / "output"]
        assert set(role for role, role_rows in all_rows.items() if role_rows) == {
            "family",
            "fragment",
            "revision_run",
            "query_record",
        }
        assert spool._work.used == spool.path.stat().st_size
        assert spool._work.peak_bytes <= limits().max_spool_bytes
        assert all_rows["family"][0][0:4] == (79, 23, family.root.revision_id, 47)
        assert all_rows["family"][0][12:] == pack_query_scalars(
            query_scalar_fields(layout.definition), family.root.scalars
        )
        by_id = {child.revision_id: child for child in children}
        for cells in all_rows["query_record"]:
            original = by_id[cells[0]]
            assert cells[1:3] == (79, 7)
            assert cells[3:5] in keys.values()
            assert cells[5:] == pack_query_scalars(query_scalar_fields(layout.definition, "rates"), original.scalars)
            assert isinstance(cells[8], Decimal)
            assert cells[8].as_tuple() == original.scalars["amount"].decimal_value.as_tuple()
        with verified(all_rows, layout, limits()) as decoded:
            assert list(decoded.records()) == [family.root]
            decoded_records = list(decoded.records("rates"))
            assert sorted(decoded_records, key=lambda cells: (cells.source_ordinal, cells.revision_id)) == sorted(
                children, key=lambda cells: (cells.source_ordinal, cells.revision_id)
            )
    assert not (tmp_path / "output").exists()


def test_copy_chunk_boundaries_do_not_change_completed_receipt(tmp_path):
    layout, family, _children, keys = prepared()
    with build(tmp_path, layout, family, keys) as spool:
        assert rows(spool, layout, maximum_rows=1) == rows(spool, layout, maximum_rows=17)
        saved = spool.receipt
        saved["role_counts"]["family"] = 123
        assert spool.receipt["role_counts"]["family"] == 1
        restored = pickle.loads(pickle.dumps(spool))
        assert rows(restored, layout) == rows(spool, layout)


def test_large_family_spills_and_gapped_ids_use_packed_memberships(tmp_path):
    layout, family, children, keys = prepared(1000)
    with build(tmp_path, layout, family, keys) as spool:
        all_rows = rows(spool, layout, maximum_rows=13)
        assert len(all_rows["fragment"]) > 10
        assert len(all_rows["revision_run"]) < len(children) // 4
        members = [RevisionMembership(*item[1:]) for item in all_rows["revision_run"] if item[1] == 7]
        assert any(row.encoded_membership is not None for row in members)
        assert {item for row in members for item, _ordinal in row.records()} == {row.revision_id for row in children}
        assert set(spool._work.paths) == {spool.path}


def test_proven_retained_sparse_bytes_and_independent_scalar_kinds_survive(tmp_path):
    layout, family, children, keys = prepared(3)
    payload, key = " opaque prior payload ", " prior exact root key "
    sparse = SparseRecord(
        payload, key, runner_codec.digest_text("root-payload", payload), runner_codec.digest_text("root-key", key)
    )
    root = replace(
        family.root,
        values_by_field=None,
        sparse=sparse,
        scalars={"display_name": TypedScalar("integer", "value", integer_value=7)},
    )
    family = replace(family, root=root, retained=True, expected_family_sha256=b"i" * 32)
    with build(tmp_path, layout, family, keys) as spool:
        all_rows = rows(spool, layout)
        assert all_rows["family"][0][4] == b"i" * 32 and all_rows["family"][0][6] is True
        assert all_rows["family"][0][12:] == pack_query_scalars(query_scalar_fields(layout.definition), root.scalars)
        with verified(all_rows, layout, limits()) as decoded:
            assert list(decoded.records()) == [root]
            assert set(row.revision_id for row in decoded.records("rates")) == set(row.revision_id for row in children)


def test_collection_slots_preserve_hidden_children(tmp_path):
    layout, family, children, keys = prepared(3)
    document = json.loads(layout.definition.canonical)
    document["schema"]["children"].append(
        {
            "name": "auxiliary",
            "parent_key": [{"child": "aux_npi", "root": "npi"}],
            "child_key": ["aux_code"],
            "fields": [
                {"id": "aux_npi", "slot": 14, "type": "string", "nullable": False},
                {"id": "aux_code", "slot": 15, "type": "string", "nullable": False},
            ],
        }
    )
    document["streams"].append(
        {
            "id": "aux_source",
            "kind": "child",
            "child": "auxiliary",
            "format": "ndjson",
            "compression": "none",
            "snapshot_token": "snapshot_id",
        }
    )
    document["aliases"]["aux_source"] = {}
    definition = CustomImportDefinition.from_mapping(document)
    layout = FamilyBlockLayout(definition, {"rates": 2, "auxiliary": 7})
    auxiliary = (
        FamilyRecord(7001, 83, 4, {"aux_npi": "1234567893", "aux_code": "b"}),
        FamilyRecord(7109, 83, 2, {"aux_npi": "1234567893", "aux_code": "a"}),
    )
    semantic = runner_codec.new_family_hash(
        definition,
        RootFamily(
            ("1234567893",),
            family.root.values_by_field,
            {
                "rates": tuple(child_record.values_by_field for child_record in children),
                "auxiliary": tuple(child_record.values_by_field for child_record in auxiliary),
            },
        ),
    )
    family = replace(
        family,
        children_by_collection={"rates": lambda: iter(children), "auxiliary": lambda: iter(auxiliary)},
        expected_family_sha256=semantic,
    )
    with build(tmp_path, layout, family, keys) as spool:
        all_rows = rows(spool, layout)
        assert {child_record[0] for child_record in all_rows["query_record"]} == {
            child_record.revision_id for child_record in children
        }
        assert {child_record[1] for child_record in all_rows["revision_run"]} == {0, 2, 7}
        with verified(all_rows, layout, limits()) as decoded:
            assert {child_record.revision_id for child_record in decoded.records("auxiliary")} == {7001, 7109}
            assert {child_record.revision_id for child_record in decoded.records("rates")} == {
                child_record.revision_id for child_record in children
            }


def test_no_query_child_still_stores_complete_hidden_family_without_locator_rows(tmp_path):
    layout, family, children, _keys = prepared(3)
    document = json.loads(layout.definition.canonical)
    document["query"].pop("child")
    document["query"]["order"] = []
    document["selection_profiles"] = []
    definition = CustomImportDefinition.from_mapping(document)
    layout = FamilyBlockLayout(definition, {"rates": 7})
    family = replace(family, expected_family_sha256=_family_hash(definition, family.root, children))
    with build(tmp_path, layout, family, {}) as spool:
        all_rows = rows(spool, layout)
        assert all_rows["query_record"] == []
        with verified(all_rows, layout, limits()) as decoded:
            assert {row.revision_id for row in decoded.records("rates")} == {row.revision_id for row in children}


def test_later_family_failure_removes_earlier_pending_rows_before_atomic_publication(tmp_path):
    layout, family, children, keys = prepared(3)
    closed_inputs = []

    def source():
        try:
            yield from children
        finally:
            closed_inputs.append(True)

    first = replace(family, children_by_collection={"rates": source})

    def inputs():
        yield first
        assert closed_inputs == [True]
        yield replace(family, expected_family_sha256=b"x" * 32)

    root = tmp_path / "output"
    with pytest.raises(ValueError, match="semantic digest"):
        build_family_output(
            layout,
            17,
            inputs(),
            keys,
            work=FamilyOutputWork(root, limits(), lambda _path: None),
            shard_id=4,
            partition_sha256=b"p" * 32,
            source_record_count=27,
        )
    assert not root.exists()


@pytest.mark.parametrize(
    "problem", ["hash", "key", "duplicate", "fresh_sparse", "tampered_sparse", "record_budget", "spool_budget"]
)
def test_failed_family_leaves_no_partial_output_or_unrelated_deletion(tmp_path, problem):
    layout, family, children, key_ids_by_key = prepared(5)
    bounds = limits()
    match problem:
        case "hash":
            family = replace(family, expected_family_sha256=b"x" * 32)
        case "key":
            key_ids_by_key = {}
        case "duplicate":
            repeated = (replace(children[0], revision_id=children[1].revision_id), *children[1:])
            family = replace(family, children_by_collection={"rates": lambda: iter(repeated)})
        case "fresh_sparse" | "tampered_sparse":
            payload_text, key = " opaque payload ", " opaque key "
            evidence = SparseRecord(
                payload_text,
                key,
                runner_codec.digest_text("root-payload", payload_text),
                runner_codec.digest_text("root-key", key),
            )
            if problem == "tampered_sparse":
                evidence = replace(evidence, payload_sha256=b"x" * 32)
            family = replace(
                family,
                root=replace(family.root, values_by_field=None, sparse=evidence),
                retained=problem == "tampered_sparse",
            )
        case "record_budget":
            bounds = limits(max_record_bytes=128)
        case _:
            bounds = FamilyBlockLimits(8192, 1024, 256, 512)
    sibling = tmp_path / "unrelated"
    sibling.write_text("preserve")
    registered_roots = []
    with pytest.raises((ValueError, SourceFactBudgetExceeded, FamilyBlockResourceLimit)):
        build(tmp_path, layout, family, key_ids_by_key, bounds=bounds, registered=registered_roots)
    assert registered_roots == [tmp_path / "output"]
    assert not (tmp_path / "output").exists()
    assert sibling.read_text() == "preserve"


def test_input_failure_closes_borrowed_generators_and_owned_spools(tmp_path):
    layout, family, children, keys = prepared()
    closed_inputs = []

    def broken():
        try:
            yield children[0]
            raise OSError("synthetic source failure")
        finally:
            closed_inputs.append(True)

    family = replace(family, children_by_collection={"rates": broken})
    with pytest.raises(OSError, match="synthetic source"):
        build(tmp_path, layout, family, keys)
    assert closed_inputs == [True] and not (tmp_path / "output").exists()


def test_completed_spool_refuses_tampering_and_singleton_copy_budget(tmp_path):
    layout, family, _children, keys = prepared()
    with build(tmp_path, layout, family, keys) as spool:
        with pytest.raises(FamilyBlockResourceLimit):
            list(spool.batches(maximum_rows=1, maximum_bytes=1))
        spool.path.chmod(0o600)
        with spool.path.open("r+b") as target:
            target.seek(-1, 2)
            value = target.read(1)
            target.seek(-1, 2)
            target.write(bytes([value[0] ^ 1]))
        with pytest.raises(ValueError):
            list(spool.batches(maximum_rows=5, maximum_bytes=2 * 1024**2))


def test_owner_dictionary_is_only_looked_up_and_never_collected(tmp_path):
    layout, family, _children, keys = prepared()

    class Lookup(Mapping):
        def __getitem__(self, key):
            return keys[key]

        def __iter__(self):
            raise AssertionError("the worker must not collect the global dictionary")

        def __len__(self):
            raise AssertionError("the worker must not require a resident dictionary")

    with build(tmp_path, layout, family, Lookup()) as spool:
        assert len(rows(spool, layout)["query_record"]) == 7


def test_preexisting_work_and_closed_output_remain_safe(tmp_path):
    layout, family, _children, keys = prepared()
    root = tmp_path / "output"
    root.mkdir()
    existing = root / "existing"
    existing.write_text("preserve")
    registered_roots = []
    with pytest.raises(FileExistsError):
        build(tmp_path, layout, family, keys, registered=registered_roots)
    assert existing.read_text() == "preserve" and registered_roots == []
    spool = build(tmp_path, layout, family, keys, name="owned")
    spool.close()
    spool.close()
    with pytest.raises(ValueError, match="closed"):
        list(spool.batches(maximum_rows=1, maximum_bytes=1024))
