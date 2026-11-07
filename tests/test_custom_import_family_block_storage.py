# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic complete-directory proofs and bounded native storage preparation."""

import importlib.util
import json
import random
import struct
from compression import zstd
from dataclasses import replace
from pathlib import Path

import pytest

from db.models.custom_import_family_block import CustomImportFamilyBlockLayout, CustomImportFamilyBlockRelation
from process.custom_import import runner_codec
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import RootFamily
from process.custom_import.family_block_binding import FamilyBlockUnavailable, _column_shape, checked_binding
from process.custom_import.family_block_codec import (
    FamilyBlockError,
    FamilyBlockLimits,
    FamilyBlockResourceLimit,
    FamilyRecord,
    SparseRecord,
    encode_family,
    encode_retained_family,
    verify_family,
)
from process.custom_import.family_block_directory import RevisionMembership, iter_revision_memberships
from process.custom_import.family_block_layout import ROLE_SPECS, FamilyBlockLayout, index_statements, load_statements
from process.custom_import.family_block_storage_codec import (
    decode_fragment,
    iter_fragment_directory,
    stored_manifest,
    verify_stored_family,
)
from process.custom_import.family_block_store import check_copy_batch


def _layout():
    data = json.loads((Path(__file__).parent / "fixtures/custom_import/v1_valid.json").read_text())
    return FamilyBlockLayout(CustomImportDefinition.from_mapping(data), {"rates": 7})


def _storage_buffers(layout, root, child_records, semantic, limits, sparse):
    definition = layout.definition
    encode = encode_retained_family if sparse else encode_family
    with encode(definition, root, {"rates": child_records}, semantic, limits=limits) as encoded:
        frames, codec_manifest = tuple(encoded.fragments()), encoded.manifest
    with verify_family(definition, frames, codec_manifest, limits=limits) as verified:
        directory_entries = tuple(
            iter_fragment_directory(
                verified, layout.collection_slots, (struct.unpack("!QQ", frame[:16])[1] for frame in frames)
            )
        )
    members = tuple(iter_revision_memberships(0, [(root.revision_id, 0)], chunk_records=3)) + tuple(
        iter_revision_memberships(
            7,
            sorted((child_record.revision_id, index + 1) for index, child_record in enumerate(child_records)),
            chunk_records=3,
        )
    )
    manifest = stored_manifest(codec_manifest, directory_entries, members)
    return frames, directory_entries, members, manifest


def _storage_case(*, count=7, fragment_bytes=37, sparse=False):
    layout = _layout()
    definition = layout.definition
    limits = FamilyBlockLimits(8 * 1024 * 1024, 1024 * 1024, fragment_bytes, 512)
    root = FamilyRecord(19, 31, 7, {"npi": "1234567893", "display_name": "Synthetic Clinic"})
    child_records = [
        FamilyRecord(
            1003 + index * 11,
            41,
            1000 - index,
            {"rate_npi": "1234567893", "service_code": f"code-{index}", "amount": str(index)},
        )
        for index in range(count)
    ]
    # Use only the existing accepted scalar domain.
    from decimal import Decimal

    child_records = [
        replace(
            child_record,
            values_by_field={**child_record.values_by_field, "amount": Decimal(child_record.values_by_field["amount"])},
        )
        for child_record in child_records
    ]
    child_records.sort(
        key=lambda row: (
            runner_codec.child_key_hash(definition, "rates", row.values_by_field),
            runner_codec.child_key_document(definition, "rates", row.values_by_field),
            runner_codec.record_payload(definition.child_fields, row.values_by_field),
        )
    )
    semantic = runner_codec.new_family_hash(
        definition,
        RootFamily(
            ("1234567893",),
            root.values_by_field,
            {"rates": tuple(child_record.values_by_field for child_record in child_records)},
        ),
    )
    if sparse:
        payload_text, key = " opaque original payload ", '{ "npi" : "1234567893" }'
        root = replace(
            root,
            values_by_field=None,
            sparse=SparseRecord(
                payload_text,
                key,
                runner_codec.digest_text("root-payload", payload_text),
                runner_codec.digest_text("root-key", key),
            ),
        )
    frames, directory_entries, members, manifest = _storage_buffers(
        layout, root, child_records, semantic, limits, sparse
    )
    return layout, limits, root, child_records, frames, directory_entries, members, manifest


@pytest.mark.parametrize("fragment_bytes", (1, 7, 16, 37, 256, 65536))
@pytest.mark.parametrize("count", (0, 1, 7))
def test_complete_storage_proof_preserves_ids_ordinals_and_fragment_spans(fragment_bytes, count):
    layout, limits, root, children, frames, directory, members, manifest = _storage_case(
        count=count, fragment_bytes=fragment_bytes
    )
    with verify_stored_family(
        layout.definition,
        layout.collection_slots,
        zip(directory, frames, strict=True),
        members,
        manifest,
        limits=limits,
    ) as verified:
        assert list(verified.records()) == [root]
        assert list(verified.records("rates")) == children
        assert verified.stored_manifest == manifest
    assert sum(row[3] for row in directory) == count + 1
    assert sum(row.record_count for row in members) == count + 1


def test_sparse_original_text_and_inherited_semantic_hash_survive_full_wrapper():
    layout, limits, root, _, frames, directory, members, manifest = _storage_case(sparse=True)
    with verify_stored_family(
        layout.definition,
        layout.collection_slots,
        zip(directory, frames, strict=True),
        members,
        manifest,
        limits=limits,
    ) as verified:
        assert next(verified.records()).sparse == root.sparse
        assert verified.manifest.family_sha256 == manifest.family_sha256


@pytest.mark.parametrize(
    "change", ("missing_frame", "repeat_frame", "changed_directory", "changed_membership", "changed_digest")
)
def test_full_wrapper_refuses_modified_storage_before_any_records_are_returned(change):
    layout, limits, _, _, frames, directory, members, manifest = _storage_case()
    rows = list(zip(directory, frames, strict=True))
    match change:
        case "missing_frame":
            rows.pop()
        case "repeat_frame":
            rows.insert(1, rows[0])
        case "changed_directory":
            family_row_items = list(rows[0][0])
            family_row_items[5] += 1
            rows[0] = (tuple(family_row_items), rows[0][1])
        case "changed_membership":
            members = (replace(members[0], first_revision_id=21, last_revision_id=21), *members[1:])
        case _:
            manifest = replace(manifest, storage_sha256=b"x" * 32)
    with pytest.raises(FamilyBlockError):
        verify_stored_family(layout.definition, layout.collection_slots, rows, members, manifest, limits=limits)


@pytest.mark.parametrize("kind", ("offset", "ordinal", "overlap", "wrong_identity"))
def test_even_a_recommitted_false_directory_cannot_pass_decoded_record_comparison(kind):
    layout, limits, _, _, frames, directory_entries, members, manifest = _storage_case()
    # Reconstruct transient codec commitment from the original completed encoding.
    import hashlib

    from process.custom_import.family_block_codec import _ENVELOPE_DOMAIN

    digest = hashlib.sha256(_ENVELOPE_DOMAIN)
    for frame in frames:
        digest.update(struct.pack("!Q", len(frame)) + frame)
    codec_manifest = manifest.codec_manifest(digest.digest())
    match kind:
        case "offset":
            directory_entries = list(directory_entries)
            index = next(i for i, family_row_items in enumerate(directory_entries) if family_row_items[3])
            family_row_items = list(directory_entries[index])
            family_row_items[4] += 1
            directory_entries[index] = tuple(family_row_items)
        case "ordinal":
            directory_entries = list(directory_entries)
            index = next(i for i, family_row_items in enumerate(directory_entries) if family_row_items[3])
            family_row_items = list(directory_entries[index])
            family_row_items[2] += 1
            directory_entries[index] = tuple(family_row_items)
        case "overlap":
            pairs = sorted(
                (revision, 1) for family_row_items in members[1:] for revision, _ in family_row_items.records()
            )
            members = (members[0], *iter_revision_memberships(7, pairs, chunk_records=3))
        case _:
            members = (replace(members[0], first_revision_id=22, last_revision_id=22), *members[1:])
    recommitted = stored_manifest(codec_manifest, directory_entries, members)
    with pytest.raises(FamilyBlockError, match="directory|membership"):
        verify_stored_family(
            layout.definition,
            layout.collection_slots,
            zip(directory_entries, frames, strict=True),
            members,
            recommitted,
            limits=limits,
        )


def test_checksum_detects_plaintext_flip_inside_raw_zstd():
    payload = b"synthetic checksum witness"
    frame = struct.pack("!QQ", 0, len(payload)) + zstd.compress(
        payload, options={zstd.CompressionParameter.compression_level: 3, zstd.CompressionParameter.checksum_flag: True}
    )
    changed = bytearray(frame)
    changed[frame.index(b"synthetic")] ^= 1
    metadata = (0, len(payload), None, 0, None, 0)
    limits = FamilyBlockLimits(4096, 1024, 256, 64)
    assert decode_fragment(frame, metadata, limits=limits) == payload
    with pytest.raises(FamilyBlockError, match="checksum"):
        decode_fragment(bytes(changed), metadata, limits=limits)
    missing = struct.pack("!QQ", 0, len(payload)) + zstd.compress(payload)
    with pytest.raises(FamilyBlockError, match="checksum"):
        decode_fragment(missing, metadata, limits=limits)


def test_membership_compresses_actual_ranges_and_preserves_arbitrary_order_and_bigint_edge():
    limit = 2**63 - 1
    pairs = [(limit - 5 + index, index + 1) for index in range(6)]
    ranged_items = tuple(iter_revision_memberships(7, pairs, chunk_records=6))
    assert len(ranged_items) == 1 and ranged_items[0].encoded_membership is None
    assert list(ranged_items[0].records()) == pairs
    random.Random(421).shuffle(pairs)
    pairs = sorted((revision, ordinal) for ordinal, (revision, _) in enumerate(pairs, 1))
    packed_items = tuple(iter_revision_memberships(7, pairs, chunk_records=3))
    assert [pair for row in packed_items for pair in row.records()] == pairs
    assert len(packed_items) == 2 and any(row.encoded_membership for row in packed_items)


@pytest.mark.parametrize("pairs", ([(4, 1), (4, 2)], [(5, 1), (3, 2)], [(0, 1)], [(7, 0)]))
def test_membership_refuses_invalid_or_repeated_actual_ids(pairs):
    with pytest.raises(FamilyBlockError):
        list(iter_revision_memberships(7, pairs, chunk_records=2))


def test_membership_rejects_trailing_or_noncanonical_varints():
    member = tuple(iter_revision_memberships(7, [(4, 2), (8, 1)], chunk_records=3))[0]
    with pytest.raises(FamilyBlockError):
        list(replace(member, encoded_membership=member.encoded_membership + b"\x00").records())
    with pytest.raises(FamilyBlockError):
        list(replace(member, encoded_membership=b"\x80\x00").records())


def _catalog(layout):
    return [
        {
            "role_name": role,
            "table_oid": slot + 1000,
            "table_owner": 91,
            "schema_owner": 91,
            "relkind": "r",
            "relpersistence": "p",
            "inherited": False,
            "foreign_keys": False,
            "triggers": False,
            "columns_sha256": bytes([slot % 256]) * 32,
            "columns": [_column_shape(column, i) for i, column in enumerate(table.c, 1)],
            "primary_key": list(table.primary_key.columns.keys()),
        }
        for (role, (slot, _)), table in zip(ROLE_SPECS.items(), layout.tables(11).values(), strict=True)
    ]


def test_layout_uses_typed_single_digest_manifests_without_cold_child_mirrors():
    layout = _layout()
    tables = layout.tables(11)
    assert len(tables) == 14
    assert {
        name
        for name in tables["family"].c.keys()
        if name not in {"raw_states", "raw_type_overrides"} and not name.startswith("raw_field_")
    } == {
        "family_revision_id",
        "root_record_id",
        "root_revision_id",
        "entity_binding_id",
        "family_sha256",
        "storage_sha256",
        "retained",
        "child_count",
        "fragment_count",
        "raw_bytes",
        "stored_bytes",
        "membership_count",
    }
    assert all(not table.foreign_keys for table in tables.values())
    assert all(
        "CREATE UNLOGGED TABLE" in ddl and "BIGSERIAL" not in ddl and "FOREIGN KEY" not in ddl
        for ddl in load_statements(11, layout)
    )
    assert "rates" not in layout.comment and "collection_slots" not in layout.comment
    assert (
        "key_rank" in tables["query_child"].primary_key.columns
        and "key_variant" not in tables["query_child"].primary_key.columns
    )
    assert all("CREATE INDEX" in statement for statement in index_statements(11, layout))
    assert not CustomImportFamilyBlockLayout.__table__.foreign_keys
    assert not CustomImportFamilyBlockRelation.__table__.foreign_keys
    assert (
        checked_binding(
            11, layout.definition, layout.collection_slots, layout.comment, _catalog(layout), persistence="logged"
        ).layout
        == layout
    )


def test_catalog_text_type_is_separate_from_exact_collation():
    layout = _layout()
    table = layout.tables(11)["key_dictionary"]
    column = table.c.canonical_child_key
    shape = _column_shape(column, 4)
    assert shape[2] == "text"
    assert shape[-2:] == ["pg_catalog", "C"]
    rows = _catalog(layout)
    dictionary = next(row for row in rows if row["role_name"] == "key_dictionary")
    dictionary["columns"][-1][-1] = "default"
    with pytest.raises(FamilyBlockUnavailable, match="custody"):
        checked_binding(11, layout.definition, layout.collection_slots, layout.comment, rows, persistence="logged")


@pytest.mark.parametrize("changed", ("oid", "owner", "unlogged", "columns", "pk", "trigger", "missing_role", "comment"))
def test_binding_refuses_native_custody_or_layout_drift(changed):
    layout = _layout()
    rows = _catalog(layout)
    comment = layout.comment
    match changed:
        case "oid":
            rows[0]["table_oid"] = rows[1]["table_oid"]
        case "owner":
            rows[0]["table_owner"] = 92
        case "unlogged":
            rows[0]["relpersistence"] = "u"
        case "columns":
            rows[0]["columns"] = []
        case "pk":
            rows[0]["primary_key"] = []
        case "trigger":
            rows[0]["triggers"] = True
        case "missing_role":
            rows.pop()
        case _:
            comment += " "
    with pytest.raises(FamilyBlockUnavailable):
        checked_binding(11, layout.definition, layout.collection_slots, comment, rows, persistence="logged")


def test_copy_resource_refusal_preserves_logical_acceptance():
    layout = _layout()
    batches_by_key = {"key_dictionary": [(2**31, 2**40, b"x" * 32, "synthetic")]}
    assert check_copy_batch(layout, 11, batches_by_key, maximum_rows=1, maximum_bytes=1000) == 1
    with pytest.raises(FamilyBlockResourceLimit):
        check_copy_batch(layout, 11, batches_by_key, maximum_rows=1, maximum_bytes=1)
    with pytest.raises(ValueError):
        check_copy_batch(layout, 11, {"unknown": []}, maximum_rows=1, maximum_bytes=1)


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261104020000_custom_import_family_blocks.py"
    spec = importlib.util.spec_from_file_location("family_storage_test_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("retirement", (False, True))
@pytest.mark.parametrize("schema", ("synthetic_control", 'synthetic "control'))
def test_migration_retains_exact_v1_guard_and_all_authority_lock_paths(schema, retirement):
    migration = _migration()
    definitions = list(migration._definitions(schema, retirement=retirement))
    assert len(definitions) == 5
    assert migration.down_revision == "20261107010000_custom_import_child_reference_aggregate"
    storage = migration._previous("20261104010000_custom_import_unlogged_landing")._continuation()._bulk()._storage()
    render = lambda value: storage._sql(schema, storage._body(schema, value))
    for name, _, old, new in definitions:
        if name == "resolve_custom_import_snapshot_relations":
            old_suffix = old[old.index("    IF (SELECT count(*)") :]
            assert new.endswith(old_suffix)
            for guard in (
                "constraints_sha256",
                "columns_sha256",
                "pg_inherits",
                "pg_trigger",
                "pg_constraint",
                "d.phase='logged'",
                "acl.grantee=0",
            ):
                assert guard in new
        else:
            assert render(migration._LOCK) in new
            assert render(migration._LOCK_NEW) in new
            if "lock_custom_import_snapshot_attempt" in old:
                assert new.count("lock_custom_import_snapshot_attempt") == old.count(
                    "lock_custom_import_snapshot_attempt"
                )
            if "custom_import_snapshot_retired" in old:
                assert new.count("custom_import_snapshot_retired") == old.count("custom_import_snapshot_retired")
    assert all("CREATE FUNCTION" not in new and "GRANT " not in new for _, _, _, new in definitions)
    assert all("FOREIGN KEY" not in ddl for ddl in migration._DDL)
    with pytest.raises(RuntimeError, match="forward_migration"):
        migration.downgrade()


def test_failed_full_proof_closes_every_owned_spool(monkeypatch):
    import process.custom_import.family_block_storage_codec as codec

    layout, limits, _, _, frames, directory, members, manifest = _storage_case()
    original = codec.tempfile.SpooledTemporaryFile
    created_items = []

    def record_spool(*args, **kwargs):
        result = original(*args, **kwargs)
        created_items.append(result)
        return result

    monkeypatch.setattr(codec.tempfile, "SpooledTemporaryFile", record_spool)
    with pytest.raises(FamilyBlockError):
        codec.verify_stored_family(
            layout.definition,
            layout.collection_slots,
            zip(directory, frames, strict=True),
            members,
            replace(manifest, storage_sha256=b"x" * 32),
            limits=limits,
        )
    assert len(created_items) >= 4 and all(source.closed for source in created_items)


def test_combined_verification_budget_refuses_before_any_spool_is_opened(monkeypatch):
    import process.custom_import.family_block_storage_codec as codec

    layout, limits, _, _, frames, directory, members, manifest = _storage_case()

    def unexpected_spool(*_args, **_kwargs):
        pytest.fail("no resource may open before the combined budget check")

    monkeypatch.setattr(codec.tempfile, "SpooledTemporaryFile", unexpected_spool)
    small = FamilyBlockLimits(1024, 128, 37, 64)
    with pytest.raises(FamilyBlockResourceLimit):
        codec.verify_stored_family(
            layout.definition,
            layout.collection_slots,
            zip(directory, frames, strict=True),
            members,
            manifest,
            limits=small,
        )


def test_owner_only_creation_has_no_new_grant_or_privilege_execution():
    import ast
    import inspect

    from process.custom_import import family_block_store

    tree = ast.parse(inspect.getsource(family_block_store))
    # Inspect executable SQL rather than docstrings.
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "_execute":
            for child in ast.walk(node):
                if isinstance(child, ast.Constant) and isinstance(child.value, str):
                    assert not child.value.lstrip().upper().startswith("GRANT ")
