# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic wire-codec checks; JSON never constitutes native Source authority."""

import json
from copy import deepcopy

import pytest

from process import network_cms_registry_address_equivalence as equivalence


def _semantic_receipt(schema_hash):
    module = equivalence.receipt
    tables = tuple(
        module.EntityAddressArchiveTableReceipt(model.__name__, model.__tablename__, schema_hash, 0, "a" * 64)
        for model in module._models()
    )
    identity = module._canonical_digest(
        [
            {"model_name": table.model_name, "table_name": table.table_name, "schema_sha256": table.schema_sha256}
            for table in tables
        ]
    )
    return module.EntityAddressArchiveReceipt(
        tables, identity, module._content_identity(tables, "b" * 64), "b" * 64
    ).as_dict()


def _wire_side(schema, base, offset):
    models_by_name = {model.__tablename__: model for model in equivalence.receipt._models()}
    oids = [[name, base + number] for number, name in enumerate(sorted(models_by_name))]
    ordinals = [
        [name, [[column.name, number + offset, number] for number, column in enumerate(model.__table__.columns, 1)]]
        for name, model in sorted(models_by_name.items())
    ]
    evidence = dict(ordinals)["entity_address_evidence"]
    sequence_name = "owned_evidence_sequence"
    return {
        "schema_name": schema,
        "schema_oid": base - 1,
        "relation_oids": oids,
        "receipt": _semantic_receipt("c" * 64 if offset else "d" * 64),
        "catalog_sha256": "e" * 64,
        "ordinals": ordinals,
        "sequence": {
            "sequence_oid": base + 100,
            "schema_name": schema,
            "sequence_name": sequence_name,
            "attribute_number": next(column[1] for column in evidence if column[0] == "evidence_id"),
            "default_expression": f"nextval('{sequence_name}'::regclass)",
            "table_oid": dict(oids)["entity_address_evidence"],
            "settings": {
                "type_oid": 20,
                "seqstart": 1,
                "seqincrement": 1,
                "seqmax": 2**63 - 1,
                "seqmin": 1,
                "seqcache": 1,
                "seqcycle": False,
            },
        },
    }


def _witness():
    return {
        "version": "registry_cms_address_copy.v1",
        "logical_sha256": "f" * 64,
        "source": _wire_side("synthetic_source", 100, 1),
        "clone": _wire_side("synthetic_clone", 300, 0),
    }


def test_decoder_returns_bounded_immutable_wire_only():
    document = _witness()
    wire = equivalence.decode_registry_cms_address_copy_witness(document)
    assert type(wire) is str
    assert json.loads(wire) == document
    assert len(wire.encode()) <= 32768
    assert equivalence.decode_registry_cms_address_copy_witness(json.loads(wire)) == wire


@pytest.mark.parametrize(
    "path",
    [(), ("source",), ("clone",), ("source", "sequence"), ("clone", "sequence", "settings"), ("source", "receipt")],
)
def test_decoder_rejects_unknown_keys(path):
    document = _witness()
    destination = document
    for name in path:
        destination = destination[name]
    destination["unknown"] = True
    with pytest.raises(ValueError):
        equivalence.decode_registry_cms_address_copy_witness(document)


@pytest.mark.parametrize(
    "path,key,value",
    [
        (("source",), "schema_oid", True),
        (("clone",), "catalog_sha256", "bad"),
        (("source", "relation_oids", 0), 1, 0),
        (("source", "ordinals", 0, 1, 0), 0, "unknown_column"),
        (("source", "ordinals", 0, 1, 0), 2, 2),
        (("source", "sequence"), "table_oid", 999),
        (("source", "sequence"), "default_expression", "nextval('foreign_sequence'::regclass)"),
        (("clone", "sequence", "settings"), "seqincrement", 2),
        (("source", "receipt", "tables", 0), "row_count", 1),
    ],
)
def test_decoder_rejects_identity_content_mapping_and_sequence_drift(path, key, value):
    document = deepcopy(_witness())
    destination = document
    for name in path:
        destination = destination[name]
    destination[key] = value
    with pytest.raises(ValueError):
        equivalence.decode_registry_cms_address_copy_witness(document)


def test_decoder_rejects_oversized_wire_before_native_use():
    document = _witness()
    document["source"]["schema_name"] = "a" * 32768
    with pytest.raises(ValueError, match="bound"):
        equivalence.decode_registry_cms_address_copy_witness(document)


@pytest.mark.parametrize("malformed", [[], {}, None, True])
def test_decoder_rejects_malformed_table_mapping_with_value_error(malformed):
    document = _witness()
    document["source"]["ordinals"][0][0] = malformed
    with pytest.raises(ValueError):
        equivalence.decode_registry_cms_address_copy_witness(document)


def test_decoder_rejects_integer_hash_even_with_consistent_legacy_digest():
    document = _witness()
    for side in ("source", "clone"):
        raw = document[side]["receipt"]
        raw["tables"][0]["row_sha256"] = int("1" * 64)
        tables = tuple(equivalence.receipt.EntityAddressArchiveTableReceipt(**table) for table in raw["tables"])
        raw["content_sha256"] = equivalence.receipt._content_identity(tables, raw["main_input_sha256"])
    with pytest.raises(ValueError, match="scalar type"):
        equivalence.decode_registry_cms_address_copy_witness(document)
