# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic complete-directory proofs and bounded native storage preparation."""

import hashlib
import importlib.util
import json
import random
import struct
from compression import zstd
from dataclasses import replace
from pathlib import Path

import pytest

from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from process.custom_import import runner_codec
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import RootFamily
from process.custom_import.family_block_binding import FamilyBlockUnavailable, checked_binding
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


def _layout():
    data = json.loads((Path(__file__).parent / "fixtures/custom_import/v1_valid.json").read_text())
    return FamilyBlockLayout(CustomImportDefinition.from_mapping(data), {"rates": 7})


def _storage_buffers(layout, root, child_records, semantic, limits, sparse):
    definition = layout.definition
    encode = encode_retained_family if sparse else encode_family
    with encode(definition, root, {"rates": child_records}, semantic, limits=limits) as encoded:
        encoded_directory, frames = zip(*encoded.indexed_fragments(), strict=True)
        assert tuple(encoded.fragments()) == frames
        codec_manifest = encoded.manifest
    with verify_family(definition, frames, codec_manifest, limits=limits) as verified:
        directory_entries = tuple(
            iter_fragment_directory(
                verified, layout.collection_slots, (struct.unpack("!QQ", frame[:16])[1] for frame in frames)
            )
        )
    assert encoded_directory == directory_entries
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


@pytest.mark.parametrize("count,word_space", ((0, 512), (7, 4096), (7, 512)))
def test_packed_writer_rows_match_full_storage_verification(count, word_space):
    from process.custom_import.family_block_write import (
        AllocatedBlockFamily,
        AllocatedChildCollection,
        pack_family_rows,
    )

    layout, limits, root, children, _, _, _, manifest = _storage_case(count=count)
    limits = replace(limits, memory_bytes=word_space)
    allocated_records = [
        replace(child, revision_id=100 + 997 * (count - offset)) for offset, child in enumerate(children)
    ]
    family = AllocatedBlockFamily(
        47,
        11,
        13,
        None,
        root,
        {"rates": AllocatedChildCollection(count, iter(allocated_records))},
    )
    family_row, fragment_rows, memberships = _packed_roles(pack_family_rows(layout, family, limits=limits))
    binding_table = layout.tables(11)["family"]
    metadata_by_name = dict(zip(binding_table.c.keys(), family_row, strict=True))
    assert metadata_by_name["family_sha256"] == manifest.family_sha256
    assert metadata_by_name["root_payload_sha256"] == runner_codec.digest_text(
        "root-payload", runner_codec.record_payload(layout.definition.root_fields, root.values_by_field)
    )
    with verify_stored_family(
        layout.definition,
        layout.collection_slots,
        fragment_rows,
        memberships,
        layout.manifest(metadata_by_name),
        limits=limits,
    ) as verified:
        assert list(verified.records()) == [root]
        assert list(verified.records("rates")) == allocated_records


@pytest.mark.parametrize("fragment_bytes", (1, 37, 65536))
def test_source_digest_matches_existing_block_bytes_from_single_use_stream(fragment_bytes):
    layout, limits, root, children, frames, directory, _, manifest = _storage_case(fragment_bytes=fragment_bytes)
    with encode_family(layout.definition, root, {"rates": iter(children)}, None, limits=limits) as encoded:
        assert tuple(encoded.indexed_fragments()) == tuple(zip(directory, frames, strict=True))
        assert encoded.manifest.family_sha256 == manifest.family_sha256


def test_native_copy_batch_limits_include_each_role_header(monkeypatch):
    from process.custom_import import family_block_write

    monkeypatch.setattr(family_block_write, "MAX_BATCH_ROWS", 3)
    monkeypatch.setattr(family_block_write, "MAX_BATCH_BYTES", 62)
    source_rows = [("family", (1,)), ("family", (2,)), ("fragment", (3,)), ("revision_run", (4,))]
    buffer = family_block_write.BlockCopyBuffer()
    packets = [batch for role, values in source_rows if (batch := buffer.append(role, values))]
    assert [*packets, buffer.take()] == [
        {"family": ((1,), (2,))},
        {"fragment": ((3,),)},
        {"revision_run": ((4,),)},
    ]
    with pytest.raises(FamilyBlockResourceLimit, match="row exceeds the native byte budget"):
        buffer.append("fragment", (b"x" * 62,))


def _packed_roles(role_rows):
    role, family_row = next(role_rows)
    fragments = []
    memberships = []
    for name, cells in role_rows:
        if name == "fragment":
            fragments.append(((cells[1], cells[2], *cells[4:]), cells[3]))
        else:
            assert name == "revision_run"
            memberships.append(RevisionMembership(*cells[1:]))
    assert role == "family"
    return family_row, fragments, memberships


def test_packed_writer_checks_source_count_before_exposing_rows():
    from process.custom_import.family_block_write import (
        AllocatedBlockFamily,
        AllocatedChildCollection,
        pack_family_rows,
    )

    layout, limits, root, children, _, _, _, manifest = _storage_case(count=1)
    mismatched = AllocatedChildCollection(2, iter(children))
    family = AllocatedBlockFamily(47, 11, 13, manifest.family_sha256, root, {"rates": mismatched})
    with pytest.raises(FamilyBlockUnavailable, match="before the SOURCE count"):
        next(pack_family_rows(layout, family, limits=limits))


def test_packed_writer_rejects_duplicate_ids_before_copy():
    from process.custom_import.family_block_write import (
        AllocatedBlockFamily,
        AllocatedChildCollection,
        pack_family_rows,
    )

    layout, limits, root, children, _, _, _, manifest = _storage_case(count=2)
    children[1] = replace(children[1], revision_id=children[0].revision_id)
    family = AllocatedBlockFamily(
        47, 11, 13, manifest.family_sha256, root, {"rates": AllocatedChildCollection(2, children)}
    )
    with pytest.raises(FamilyBlockError, match="membership order"):
        next(pack_family_rows(layout, family, limits=limits))


def test_revision_spill_shares_the_family_spool_budget():
    from process.custom_import.family_block_write import (
        AllocatedBlockFamily,
        AllocatedChildCollection,
        pack_family_rows,
    )

    layout, limits, root, children, _, _, _, manifest = _storage_case(count=7, fragment_bytes=37)
    with encode_family(layout.definition, root, {"rates": children}, manifest.family_sha256, limits=limits) as normal:
        encoding_bytes = normal._file.seek(0, 2)
    limited = replace(limits, max_spool_bytes=encoding_bytes, max_record_bytes=encoding_bytes)
    family = AllocatedBlockFamily(
        47, 11, 13, manifest.family_sha256, root, {"rates": AllocatedChildCollection(7, children)}
    )
    with pytest.raises(FamilyBlockResourceLimit, match="family exceeds the caller's spool budget"):
        next(pack_family_rows(layout, family, limits=limited))


@pytest.mark.parametrize("fragment_bytes", (1, 7, 16, 37, 256, 65536))
@pytest.mark.parametrize("count", (0, 1, 7))
def test_complete_storage_proof_preserves_ids_ordinals_and_fragment_spans(fragment_bytes, count):
    layout, limits, root, children, frames, directory, members, manifest = _storage_case(
        count=count, fragment_bytes=fragment_bytes
    )
    observed_records = []

    def observe(collection, record, _parts):
        """Capture SOURCE audit inputs from the existing verification pass."""
        observed_records.append((collection, record))

    with verify_stored_family(
        layout.definition,
        layout.collection_slots,
        zip(directory, frames, strict=True),
        members,
        manifest,
        limits=limits,
        observe=observe,
    ) as verified:
        assert list(verified.records()) == [root]
        assert list(verified.records("rates")) == children
        assert verified.stored_manifest == manifest
    assert observed_records == [(None, root), *(("rates", child) for child in children)]
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


def _codec_manifest(manifest, frames):
    from process.custom_import.family_block_codec import _ENVELOPE_DOMAIN

    digest = hashlib.sha256(_ENVELOPE_DOMAIN)
    for frame in frames:
        digest.update(struct.pack("!Q", len(frame)) + frame)
    return manifest.codec_manifest(digest.digest())


@pytest.mark.parametrize("kind", ("offset", "ordinal", "overlap", "wrong_identity"))
@pytest.mark.parametrize("fragment_bytes", (37, 65536))
def test_even_a_recommitted_false_directory_cannot_pass_decoded_record_comparison(kind, fragment_bytes):
    layout, limits, _, _, frames, directory_entries, members, manifest = _storage_case(fragment_bytes=fragment_bytes)
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
    recommitted = stored_manifest(_codec_manifest(manifest, frames), directory_entries, members)
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


def test_memberships_cover_every_decoded_ordinal():
    layout, limits, _, children, frames, directory, members, manifest = _storage_case(count=24)
    with verify_stored_family(
        layout.definition,
        layout.collection_slots,
        zip(directory, frames, strict=True),
        members,
        manifest,
        limits=limits,
    ) as verified:
        assert list(verified.records("rates")) == children

    root_only, *child_members = members
    malformed = (
        (replace(root_only, first_revision_id=1, last_revision_id=1), *child_members),
        (root_only, replace(child_members[0], encoded_membership=b"\x80\x00"), *child_members[1:]),
        (root_only, child_members[0], *child_members),
        (root_only, *child_members[:-1]),
    )
    for invalid in malformed:
        with pytest.raises(FamilyBlockError):
            recommitted = stored_manifest(_codec_manifest(manifest, frames), directory, invalid)
            verify_stored_family(
                layout.definition,
                layout.collection_slots,
                zip(directory, frames, strict=True),
                invalid,
                recommitted,
                limits=limits,
            )


def test_layout_uses_typed_single_digest_manifests_without_cold_child_mirrors():
    layout = _layout()
    tables = layout.tables(11)
    assert len(tables) == 13
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
        "root_payload_sha256",
        "root_source_ordinal",
    }
    assert all(not table.foreign_keys for table in tables.values())
    assert all(
        "CREATE UNLOGGED TABLE" in ddl and "BIGSERIAL" not in ddl and "FOREIGN KEY" not in ddl
        for ddl in load_statements(11, layout)
    )
    assert (
        "key_rank" in tables["query_child"].primary_key.columns
        and "key_variant" not in tables["query_child"].primary_key.columns
    )
    assert all("CREATE INDEX" in statement for statement in index_statements(11, layout))
    assert not CustomImportFamilyBlockLayout.__table__.foreign_keys
    assert checked_binding(11, layout.definition, layout.collection_slots).layout == layout


def test_binding_uses_the_published_schema_name_for_a_copied_snapshot():
    layout = _layout()
    binding = checked_binding(11, layout.definition, layout.collection_slots, schema_name="copied_snapshot")
    assert binding.layout == layout
    assert set(binding.tables_by_role) == set(ROLE_SPECS)
    assert all(table.schema == "copied_snapshot" for table in binding.tables_by_role.values())
    dictionary = binding.tables_by_role["key_dictionary"]
    assert dictionary.c.canonical_child_key.type.collation == "C"


def _migration():
    path = Path(__file__).resolve().parents[1] / "alembic/versions/20261104020000_custom_import_family_blocks.py"
    spec = importlib.util.spec_from_file_location("family_storage_test_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("schema", ("synthetic_control", 'synthetic "control'))
def test_compact_binding_changes_only_the_native_leaf_locks(schema):
    migration = _migration()
    names = migration._previous("20261114010000_custom_import_snapshot_names")
    storage = names._storage()
    retirement = migration._previous("20261102010000_custom_import_snapshot_retirement")
    read = next(
        new
        for name, _, _, new in retirement._definitions(schema)
        if name == "resolve_custom_import_generation_snapshot"
    )
    finality = storage._sql(schema, storage._body(schema, storage._FINALITY_BINDING_BODY))
    marker = f"    PERFORM {storage._quote(schema)}.resolve_custom_import_snapshot_relations(f.family_id);"
    tables = [name for _slot, _role, name in migration._ROLES]
    assert tables == [table.name for table in _layout().tables(17).values()]
    for source in (names._optional_relations(read), names._optional_relations(finality)):
        before, legacy, after = source.split(marker)
        changed = migration._compact_relation_locks(source, schema)
        assert changed.startswith(before) and changed.endswith(after)
        compact, fallback = changed[len(before) : -len(after)].split("    ELSE\n")
        assert fallback == marker + legacy + marker + "\n    END IF;"
        assert "WHERE family_id=f.family_id AND phase='logged'" in compact
        assert "IN ACCESS SHARE MODE" in compact
        assert [compact.index(storage._literal(table)) for table in tables] == sorted(
            compact.index(storage._literal(table)) for table in tables
        )
        with pytest.raises(RuntimeError, match="binding_mismatch"):
            migration._compact_relation_locks(source.replace(marker, "", 1), schema)
        with pytest.raises(RuntimeError, match="lock_mismatch"):
            migration._compact_relation_locks(source.replace("IN ACCESS SHARE MODE", "IN SHARE MODE"), schema)


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
