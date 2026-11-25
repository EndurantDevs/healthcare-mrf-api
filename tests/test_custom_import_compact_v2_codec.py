# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Unit tests for compact v2 positional codec, compiled serializer, and inline roots."""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from pathlib import Path

import pytest

from process.custom_import import runner_codec
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import RootFamily
from process.custom_import.family_block_codec import FamilyBlockLimits, FamilyBlockResourceLimit
from process.custom_import.family_block_records import FamilyBlockError, FamilyRecord
from process.custom_import.family_block_scalar_layout import pack_query_scalars, query_scalar_fields, scalar_column
from process.custom_import.family_block_v2_codec import (
    COMPACT_V2_CONTRACT,
    InlineEncodedFamily,
    can_inline_root,
    compiled_serializer,
    decode_wire_record,
    encode_family_v2,
    encode_slot_values,
    inline_storage_sha256,
    needs_inline_entity_join,
    pack_staged_values,
    scalars_from_v2_wire,
    unpack_inline_root_scalars,
    unpack_staged_values,
    verify_inline_root_family,
    verify_v2_fragments_and_records,
)
from process.custom_import.materialization import _typed_scalar


def _multi_collection_definition() -> CustomImportDefinition:
    document = json.loads((Path(__file__).parent / "fixtures/custom_import/v1_valid.json").read_text())
    document["schema"]["children"][0]["fields"].extend(
        [
            {"id": "day", "slot": 6, "type": "date", "nullable": True},
            {"id": "stamp", "slot": 7, "type": "timestamp", "nullable": True},
            {"id": "note", "slot": 8, "type": "string", "nullable": True},
            {"id": "enabled", "slot": 9, "type": "boolean", "nullable": True},
            {"id": "rank", "slot": 10, "type": "integer", "nullable": True},
        ]
    )
    return CustomImportDefinition.from_mapping(document)


def _inline_root_only_definition(
    *, include_entity_in_query: bool = False, extra_unqueried_field: bool = False
) -> CustomImportDefinition:
    root_query = (
        ["npi", "badge_code", "active", "score"] if include_entity_in_query else ["badge_code", "active", "score"]
    )
    fields = [
        {"id": "npi", "slot": 1, "type": "string", "nullable": False, "projection_slot": 1},
        {"id": "badge_code", "slot": 2, "type": "string", "nullable": False, "projection_slot": 2},
        {"id": "active", "slot": 3, "type": "boolean", "nullable": True, "projection_slot": 3},
        {"id": "score", "slot": 4, "type": "decimal", "nullable": True, "projection_slot": 4},
    ]
    if extra_unqueried_field:
        fields.append({"id": "hidden_note", "slot": 5, "type": "string", "nullable": True})
    document_by_key = {
        "contract": "custom-import/v1",
        "revision": {"definition": 1, "schema": 1},
        "refresh_mode": "snapshot",
        "streams": [
            {
                "id": "roots",
                "kind": "root",
                "format": "parquet",
                "compression": "none",
                "snapshot_token": "snapshot_id",
            }
        ],
        "schema": {
            "root": {
                "logical_key": ["npi", "badge_code"],
                "entity": {"adapter": "npi", "field": "npi"},
                "fields": fields,
            },
            "children": [],
        },
        "aliases": {"roots": {field["id"]: field["id"] for field in fields}},
        "query": {
            "root_fields": root_query,
            "order": [{"field": "badge_code", "direction": "asc", "nulls": "last"}],
        },
        "selection_profiles": [
            {
                "id": "default",
                "selection": [{"field": "badge_code", "direction": "asc", "nulls": "last"}],
                "context_dimensions": ["badge_code"],
            }
        ],
    }
    return CustomImportDefinition.from_mapping(document_by_key)


def _limits() -> FamilyBlockLimits:
    return FamilyBlockLimits(32 * 1024 * 1024, 12 * 1024 * 1024, 256, 1024)


def test_compiled_serializer_matches_runner_codec_byte_for_byte():
    definition = _multi_collection_definition()
    child_fields = runner_codec.fields_by_collection(definition)["rates"]
    serializer = compiled_serializer(child_fields)
    sample_records = (
        {
            "rate_npi": "1234567893",
            "service_code": 'CPT "99213" \\ café\nline2',
            "amount": Decimal("12.5000"),
            "day": dt.date(2026, 11, 25),
            "stamp": dt.datetime(2026, 11, 25, 14, 30, 15, 123456, tzinfo=dt.timezone(dt.timedelta(hours=-5))),
            "note": None,
            "enabled": True,
            "rank": -42,
        },
        {
            "rate_npi": "1234567893",
            "service_code": "CPT-000",
            "amount": Decimal("-0.00"),
            "enabled": False,
            # Omit optional day/stamp/note/rank to verify missing state handling.
        },
    )
    for sample_by_field in sample_records:
        expected_payload = runner_codec.record_payload(child_fields, sample_by_field)
        direct_payload = serializer.serialize_payload(sample_by_field)
        slot_entries = encode_slot_values(child_fields, sample_by_field)
        from_slots_payload = serializer.serialize_from_slots(slot_entries)
        assert direct_payload == expected_payload
        assert from_slots_payload == expected_payload


def test_staged_slot_round_trip_preserves_missing_null_and_types():
    definition = _multi_collection_definition()
    child_fields = runner_codec.fields_by_collection(definition)["rates"]
    original_by_field = {
        "rate_npi": "1234567893",
        "service_code": "S-1",
        "amount": Decimal("42.125"),
        "day": dt.date(2026, 5, 1),
        "stamp": dt.datetime(2026, 5, 1, 8, 0, tzinfo=dt.UTC),
        "note": None,
        "enabled": True,
    }
    packed = pack_staged_values(child_fields, original_by_field)
    unpacked_by_field = unpack_staged_values(child_fields, packed)
    assert unpacked_by_field == original_by_field
    assert "rank" not in unpacked_by_field


def test_can_inline_root_checks_full_lossless_coverage():
    inline_def = _inline_root_only_definition(include_entity_in_query=False)
    assert can_inline_root(inline_def) is True
    assert needs_inline_entity_join(inline_def) is True

    inline_with_entity = _inline_root_only_definition(include_entity_in_query=True)
    assert can_inline_root(inline_with_entity) is True
    assert needs_inline_entity_join(inline_with_entity) is False

    non_inline = _inline_root_only_definition(include_entity_in_query=False, extra_unqueried_field=True)
    assert can_inline_root(non_inline) is False


def test_inline_root_encode_and_unpack_round_trip():
    definition = _inline_root_only_definition(include_entity_in_query=False)
    root_values_by_field = {
        "npi": "1234567893",
        "badge_code": "GOLD",
        "active": True,
        "score": Decimal("99.5"),
    }
    root_scalars_by_field = {
        field.field_id: _typed_scalar(field, root_values_by_field[field.field_id]) for field in definition.root_fields
    }
    root_rec = FamilyRecord(501, 11, 0, root_values_by_field, root_scalars_by_field)
    with encode_family_v2(
        definition,
        root_rec,
        {},
        None,
        limits=_limits(),
        retained=False,
        total_child_count=0,
    ) as encoded:
        assert isinstance(encoded, InlineEncodedFamily)
        manifest = encoded.stored_manifest
        assert manifest.fragment_count == 0
        assert manifest.raw_bytes == 0
        assert manifest.stored_bytes == 0
        assert manifest.membership_count == 0
        assert tuple(encoded.indexed_fragments()) == ()
        expected_family_hash = runner_codec.new_family_hash(
            definition, RootFamily(("1234567893", "GOLD"), root_values_by_field, {})
        )
        assert manifest.family_sha256 == expected_family_hash
        assert manifest.storage_sha256 == inline_storage_sha256(
            definition.digest, expected_family_hash, encoded.root_payload_sha256, 501
        )

    query_fields = query_scalar_fields(definition)
    raw_states, raw_overrides, *packed_cols = pack_query_scalars(query_fields, root_scalars_by_field)
    family_row_by_col = {
        "root_revision_id": 501,
        "family_sha256": manifest.family_sha256,
        "root_payload_sha256": encoded.root_payload_sha256,
        "storage_sha256": manifest.storage_sha256,
        "raw_states": raw_states,
        "raw_type_overrides": raw_overrides,
        **{scalar_column(field): col_val for field, col_val in zip(query_fields, packed_cols, strict=True)},
    }
    restored_by_field = unpack_inline_root_scalars(definition, family_row_by_col, entity_canonical_value="1234567893")
    assert restored_by_field == root_scalars_by_field
    verified_vals, verified_scalars = verify_inline_root_family(
        definition, family_row_by_col, entity_canonical_value="1234567893"
    )
    assert verified_vals == root_values_by_field and verified_scalars == root_scalars_by_field
    tampered_by_col = dict(family_row_by_col)
    tampered_by_col[scalar_column(definition.fields_by_id["badge_code"])] = "SILVER"
    with pytest.raises(FamilyBlockError, match="root_payload_sha256"):
        verify_inline_root_family(definition, tampered_by_col, entity_canonical_value="1234567893")


def _sample_child_records(definition: CustomImportDefinition):
    child_fields = runner_codec.fields_by_collection(definition)["rates"]
    child_dicts = [
        {
            "rate_npi": "1234567893",
            "service_code": f"svc-{idx}",
            "amount": Decimal(f"{10 + idx}.25"),
            "day": dt.date(2026, 1, idx + 1),
            "stamp": dt.datetime(2026, 1, idx + 1, 12, 0, tzinfo=dt.UTC),
            "note": None if idx == 0 else f"note-{idx}",
            "enabled": bool(idx % 2),
            "rank": idx,
        }
        for idx in range(4)
    ]
    child_dicts.sort(
        key=lambda vals_by_field: (
            runner_codec.child_key_hash(definition, "rates", vals_by_field),
            runner_codec.child_key_document(definition, "rates", vals_by_field),
            runner_codec.record_payload(child_fields, vals_by_field),
        )
    )
    child_records = [
        FamilyRecord(
            1000 + idx,
            20,
            idx,
            vals_by_field,
            {
                field.field_id: _typed_scalar(field, vals_by_field[field.field_id])
                for field in child_fields
                if field.projection_slot
            },
        )
        for idx, vals_by_field in enumerate(child_dicts)
    ]
    return child_fields, child_dicts, child_records


def test_v2_fragmented_encode_verify_and_decode():
    """Verify v2 fragmented encoding, tamper rejection, and fast wire scalar hydration."""
    definition = _multi_collection_definition()
    root_values_by_field = {"npi": "1234567893", "display_name": "Synthetic Clinic"}
    root_rec = FamilyRecord(
        91,
        17,
        9,
        root_values_by_field,
        {
            field.field_id: _typed_scalar(field, root_values_by_field[field.field_id])
            for field in definition.root_fields
            if field.projection_slot
        },
    )
    child_fields, child_dicts, child_records = _sample_child_records(definition)
    expected_hash = runner_codec.new_family_hash(
        definition, RootFamily(("1234567893",), root_values_by_field, {"rates": tuple(child_dicts)})
    )
    with encode_family_v2(
        definition,
        root_rec,
        {"rates": child_records},
        None,
        limits=_limits(),
        retained=False,
        total_child_count=len(child_records),
    ) as encoded:
        manifest = encoded.stored_manifest
        assert manifest.family_sha256 == expected_hash
        assert manifest.fragment_count >= 1
        assert manifest.membership_count == 0
        stored_fragments = list(encoded.indexed_fragments())

    observed_collections = []
    with verify_v2_fragments_and_records(
        definition,
        stored_fragments,
        manifest,
        limits=_limits(),
        observe=lambda col_name, rec, _parts: observed_collections.append((col_name, rec.revision_id)),
    ) as verified:
        assert verified.manifest.family_sha256 == expected_hash
        replayed_roots = list(verified.records(None))
        replayed_child_records = list(verified.records("rates"))
        assert len(replayed_roots) == 1 and replayed_roots[0].revision_id == 91
        assert [child.revision_id for child in replayed_child_records] == [1000 + idx for idx in range(4)]
    assert observed_collections == [(None, 91), *(("rates", 1000 + idx) for idx in range(4))]

    tampered_fragments = [
        (stored_fragments[0][0], stored_fragments[0][1][:-1] + bytes([stored_fragments[0][1][-1] ^ 0xFF]))
    ]
    with pytest.raises(FamilyBlockError):
        verify_v2_fragments_and_records(
            definition, tampered_fragments + stored_fragments[1:], manifest, limits=_limits()
        )
    assert COMPACT_V2_CONTRACT == "custom-import/family-block/v2"


def test_v2_wire_scalars_streaming_spool_limit_and_incremental_digest():
    """Fast v2 wire decode matches full decode, streaming verifier enforces spool caps, and incremental hash matches."""
    definition = _multi_collection_definition()
    child_fields, child_dicts, child_records = _sample_child_records(definition)
    slot_entries = encode_slot_values(child_fields, child_dicts[1])
    v2_wire_cells = [1001, 20, 1, slot_entries]
    rev_id, pack_id, ordinal, fast_scalars_by_field = scalars_from_v2_wire(child_fields, v2_wire_cells)
    full_entry, _parts = decode_wire_record(definition, "rates", v2_wire_cells, retained=False)
    assert (rev_id, pack_id, ordinal) == (1001, 20, 1)
    assert fast_scalars_by_field == dict(full_entry.scalars)

    root_values_by_field = {"npi": "1234567893", "display_name": "Synthetic Clinic"}
    root_rec = FamilyRecord(
        91,
        17,
        9,
        root_values_by_field,
        {
            field.field_id: _typed_scalar(field, root_values_by_field[field.field_id])
            for field in definition.root_fields
            if field.projection_slot
        },
    )
    with encode_family_v2(
        definition,
        root_rec,
        {"rates": child_records},
        None,
        limits=_limits(),
        retained=False,
        total_child_count=len(child_records),
    ) as encoded:
        manifest = encoded.stored_manifest
        stored_fragments = list(encoded.indexed_fragments())

    tight_cap = max(1, manifest.stored_bytes - 1)
    tight_limits = FamilyBlockLimits(tight_cap, tight_cap, min(256, tight_cap), min(1024, tight_cap))
    seen_fragments: list[int] = []

    def _counting_stream():
        for entry in stored_fragments:
            seen_fragments.append(1)
            yield entry

    with pytest.raises(FamilyBlockResourceLimit):
        verify_v2_fragments_and_records(definition, _counting_stream(), manifest, limits=tight_limits)
    assert len(seen_fragments) <= len(stored_fragments)

    sample_hashes = [bytes([idx]) * 32 for idx in range(1, 5)]
    inc = runner_codec.IncrementalCandidateDigest(execution_id=7, fence=2, base_generation_id=1)
    for digest_bytes in sample_hashes:
        inc.update(digest_bytes)
    assert inc.digest() == runner_codec.candidate_hash_ordered(
        execution_id=7, fence=2, base_generation_id=1, root_key_hashes=sample_hashes
    )
