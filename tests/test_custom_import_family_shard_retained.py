# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Legacy compaction is conditional on exact original bytes and independent scalars."""

import json
from dataclasses import replace
from decimal import Decimal

import pytest

from process.custom_import import runner_codec as codec
from process.custom_import.family_block_records import FamilyBlockError, record_parts
from process.custom_import.family_shard_retained import restore_legacy_record
from process.custom_import.materialization import TypedScalar
from tests.test_custom_import_family_block_codec import _case


def _legacy(definition, original, collection, *, payload_text=None, key_text=None, parent=None):
    kind = "root" if collection is None else "child"
    fields = definition.root_fields if collection is None else codec.fields_by_collection(definition)[collection]
    payload_text = codec.record_payload(fields, original.values_by_field) if payload_text is None else payload_text
    key_text = (
        (
            codec.root_key_document(definition, original.values_by_field)
            if collection is None
            else codec.child_key_document(definition, collection, original.values_by_field)
        )
        if key_text is None
        else key_text
    )
    revision_by_name = {
        f"{kind}_revision_id": original.revision_id,
        "pack_id": original.pack_id,
        "source_ordinal": original.source_ordinal,
        "canonical_payload": payload_text,
        "payload_sha256": codec.digest_text(f"{kind}-payload", payload_text),
    }
    if collection is not None:
        parent = (
            codec.root_key_document(definition, {"npi": original.values_by_field["rate_npi"]})
            if parent is None
            else parent
        )
        revision_by_name.update(
            canonical_parent_key=parent,
            parent_key_sha256=codec.digest_text("root-key", parent),
            canonical_child_key=key_text,
            child_key_sha256=codec.digest_text("child-key", key_text),
        )
    return revision_by_name, (key_text, codec.digest_text(f"{kind}-key", key_text))


@pytest.mark.parametrize("child", [False, True])
def test_canonical_records_drop_only_stored_hashes_and_preserve_independent_scalars(child):
    definition, root, children = _case(1)
    collection, original = ("rates", children[0]) if child else (None, root)
    field = "amount" if child else "display_name"
    scalars_by_field = {field: TypedScalar("string", "value", string_value="independent original scalar")}
    revision, key = _legacy(definition, original, collection)
    restored = restore_legacy_record(definition, collection, revision, key=key, scalars=scalars_by_field)
    assert restored.sparse is None and dict(restored.values_by_field) == original.values_by_field
    assert restored.scalars == scalars_by_field
    assert (restored.revision_id, restored.pack_id, restored.source_ordinal) == (
        original.revision_id,
        original.pack_id,
        original.source_ordinal,
    )
    scalars_by_field.clear()
    assert restored.scalars[field].string_value == "independent original scalar"
    assert record_parts(definition, collection, restored, retained=True)[0][3] == revision["canonical_payload"]


@pytest.mark.parametrize("payload", ["opaque legacy bytes", '{"fields": []}', " noncanonical whitespace "])
@pytest.mark.parametrize("child", [False, True])
def test_opaque_or_noncanonical_payloads_keep_exact_bytes_hashes_and_scalar_absence(payload, child):
    definition, root, children = _case(1)
    collection, original = ("rates", children[0]) if child else (None, root)
    revision, key = _legacy(definition, original, collection, payload_text=payload)
    restored = restore_legacy_record(definition, collection, revision, key=key, scalars={})
    assert restored.values_by_field is None and restored.scalars == {}
    assert restored.sparse.payload_text == payload and restored.sparse.payload_sha256 == revision["payload_sha256"]
    assert (restored.sparse.key_text, restored.sparse.key_sha256) == key
    record_parts(definition, collection, restored, retained=True)


def test_decimal_lexical_and_nonroundtrip_key_or_parent_stay_lossless():
    definition, root, children = _case(1)
    original = replace(children[0], values_by_field={**children[0].values_by_field, "amount": Decimal("1.00")})
    scalars_by_field = {"amount": TypedScalar("decimal", "value", decimal_value=Decimal("1.000000000000"))}
    for changes in ({"key_text": "exact legacy key"}, {"parent": "exact legacy parent"}):
        revision, key = _legacy(definition, original, "rates", **changes)
        restored = restore_legacy_record(definition, "rates", revision, key=key, scalars=scalars_by_field)
        assert restored.sparse is not None and restored.values_by_field is None
        assert restored.sparse.payload_text == revision["canonical_payload"]
        assert restored.scalars["amount"].decimal_value.as_tuple() == Decimal("1.000000000000").as_tuple()


@pytest.mark.parametrize("damage", ["payload", "key", "parent", "revision"])
def test_corrupt_native_inputs_are_refused_before_sparse_fallback(damage):
    definition, root, children = _case(1)
    revision, key = _legacy(definition, children[0], "rates")
    if damage == "payload":
        revision["payload_sha256"] = b"x" * 32
    elif damage == "key":
        key = key[0], b"x" * 32
    elif damage == "parent":
        revision["parent_key_sha256"] = b"x" * 32
    else:
        revision["child_revision_id"] = 0
    with pytest.raises(FamilyBlockError):
        restore_legacy_record(definition, "rates", revision, key=key, scalars={})


@pytest.mark.parametrize(("name", "lexical"), [("amount", "1E+0"), ("stamp", "2026-01-01T01:00:00+01:00")])
def test_decimal_and_timestamp_wire_variants_remain_exact_sparse_inputs(name, lexical):
    definition, root, children = _case(1)
    original = children[0]
    fields = codec.fields_by_collection(definition)["rates"]
    document = json.loads(codec.record_payload(fields, original.values_by_field))
    field = next(field for field in fields if field.field_id == name)
    encoded = next(encoded for encoded in document["fields"] if encoded["field"] == name)
    encoded["value"] = {"type": field.value_type, "state": "value", "value": lexical}
    payload = json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    revision, key = _legacy(definition, original, "rates", payload_text=payload)
    restored = restore_legacy_record(definition, "rates", revision, key=key, scalars={})
    assert restored.sparse is not None and restored.values_by_field is None
    assert restored.sparse.payload_text == payload
    assert restored.sparse.payload_sha256 == codec.digest_text("child-payload", payload)
