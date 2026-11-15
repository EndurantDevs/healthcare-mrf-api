# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure retained-pair identity checks; wire values confer no native authority."""

import json
from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_cms_registry_source_pair as retained
from process import network_registry_cms_prepared_pair as prepared
from process.entity_address_snapshot_ownership import validate_entity_address_archive_stage_ownership
from process.entity_address_snapshot_receipt import validate_entity_address_archive_receipt
from process.registry_source_recipe_store import canonical_registry_source_recipes
from tests.test_cms_registry_source_pair_slot import _prepared, _row
from tests.test_network_cms_registry_address_witness_decode import _named_witness, _witness


def _retained_pair():
    """Reuse closed parser inputs, without claiming receipt visibility or custody."""
    candidate = _prepared()
    return prepared._pair_from_prepared_receipt(candidate, _row(candidate))


def _witness_pair(*, named=False):
    """Bind the existing synthetic seven-table wire to one retained parser input."""
    pair = _retained_pair()
    document = _named_witness() if named else _witness()
    for side, schema in (
        ("source", pair.proof.source_pin.schema_name),
        ("clone", pair.address_ownership.schema_name),
    ):
        document[side]["schema_name"] = schema
        document[side]["sequence"]["schema_name"] = schema
        if "relations" in document[side]:
            for relation in document[side]["relations"]:
                relation["schema_name"] = schema
    clone = document["clone"]
    coordinates = (
        {
            "relations": [
                {"schema_name": clone["schema_name"], "relation_name": entry["relation_name"]}
                for entry in clone["relations"]
            ]
        }
        if named
        else {
            "schema_oid": clone["schema_oid"],
            "relation_oids": [{"table_name": name, "oid": token} for name, token in clone["relation_oids"]],
        }
    )
    ownership = validate_entity_address_archive_stage_ownership(
        {
            "dataset_id": str(pair.address_ownership.dataset_id),
            "schema_name": clone["schema_name"],
            **coordinates,
        }
    )
    return replace(
        pair,
        address_ownership=ownership,
        address_receipt=validate_entity_address_archive_receipt(clone["receipt"]),
        address_catalog_sha256=clone["catalog_sha256"],
        witness_json=retained.decode_registry_cms_address_copy_witness(document),
    )


@pytest.mark.parametrize("make_pair", [_retained_pair, _witness_pair], ids=["original", "copy-witness"])
def test_retained_wire_roundtrip_preserves_original_capture(make_pair):
    pair = make_pair()
    original = pair.as_dict()
    replay = retained.decode_registry_cms_source_pair(json.loads(retained._json(original)))
    assert replay == pair and replay.as_dict() == original
    assert replay.pair_sha256 == pair.pair_sha256
    assert replay.proof.source_pin.retained_epoch is None
    assert replay.recipe.source_pin.retained_epoch.epoch_id == replay.address_ownership.dataset_id
    assert replay.recipe.source_pin.read_schema_name != replay.proof.source_pin.schema_name


@pytest.mark.parametrize("pin_kind", ["foreign-source", "retained", "untyped"])
def test_publication_proof_refuses_prebound_or_foreign_source(pin_kind):
    pair = _retained_pair()
    pin = pair.proof.source_pin
    if pin_kind == "foreign-source":
        pin = replace(pin, source_id="other-source")
    elif pin_kind == "retained":
        pin = pair.recipe.source_pin
    else:
        pin = pair.proof.source_pin.coordinates
    with pytest.raises(ValueError, match="^registry_cms_publication_proof_invalid$"):
        replace(pair.proof, source_pin=pin)


@pytest.mark.parametrize(
    "position,value",
    [(1, "other-endpoint"), (2, "other-dataset"), (3, "0" * 64), (5, "other-release"), (6, True), (6, 1)],
    ids=["endpoint", "dataset", "dataset-digest", "release", "boolean-version", "old-version"],
)
def test_publication_proof_refuses_mixed_acquisition_coordinates(position, value):
    proof = _retained_pair().proof
    authority_values = list(proof.cms_authority)
    authority_values[position] = value
    with pytest.raises(ValueError, match="^registry_cms_publication_proof_invalid$"):
        replace(proof, cms_authority=tuple(authority_values))


@pytest.mark.parametrize(
    "field",
    ["serving_receipt_id", "selection_proof_id", "expected_admission_sha256", "expected_metadata_sha256"],
)
def test_publication_proof_refuses_unbound_receipt_digests(field):
    with pytest.raises(ValueError, match="^registry_cms_publication_proof_invalid$"):
        replace(_retained_pair().proof, **{field: "0" * 63})


@pytest.mark.parametrize(
    "field,value",
    [("publication_xid", "0"), ("publication_xid", True), ("profile_generation_id", "unpublished")],
)
def test_publication_proof_refuses_missing_commit_or_profile_identity(field, value):
    with pytest.raises(ValueError, match="^registry_cms_publication_proof_invalid$"):
        replace(_retained_pair().proof, **{field: value})


@pytest.mark.parametrize("root_run", ["", "r" * 65])
def test_publication_proof_refuses_invalid_acquisition_root(root_run):
    proof = _retained_pair().proof
    authority = (*proof.cms_authority[:4], root_run, *proof.cms_authority[5:])
    with pytest.raises(ValueError, match="^registry_cms_publication_proof_invalid$"):
        replace(proof, cms_authority=authority)


@pytest.mark.parametrize("coordinate", ["producer_id", "edition_id"])
def test_retained_pair_refuses_different_binding_producer(coordinate):
    pair = _retained_pair()
    foreign_coordinates = replace(pair.recipe.binding_coordinates, **{coordinate: "other-producer"})
    foreign_recipe = replace(pair.recipe, binding_coordinates=foreign_coordinates)
    with pytest.raises(ValueError, match="^registry_cms_source_pair_invalid$"):
        replace(pair, recipe=foreign_recipe)
    assert retained.decode_registry_cms_source_pair(pair.as_dict()) == pair


def test_retained_pair_refuses_address_from_another_epoch():
    pair = _retained_pair()
    other_capture = UUID(int=2)
    other_schema = "entity_address_archive_" + other_capture.hex
    ownership = replace(pair.address_ownership, dataset_id=other_capture, schema_name=other_schema)
    with pytest.raises(ValueError, match="^registry_cms_source_pair_invalid$"):
        replace(
            pair, address_ownership=ownership, address_source=replace(pair.address_source, schema_name=other_schema)
        )
    assert pair.address_ownership.dataset_id == pair.recipe.source_pin.retained_epoch.epoch_id


@pytest.mark.parametrize("coordinate", ["generation_id", "selection_proof_id", "profile_as_of"])
def test_retained_pair_refuses_another_profile_completion(coordinate):
    pair = _retained_pair()
    payload = json.loads(pair.cms_receipt_payload)
    payload["profile"][coordinate] = {
        "generation_id": "pdprofile_" + "2" * 32,
        "selection_proof_id": "0" * 64,
        "profile_as_of": "2026-02-30",
    }[coordinate]
    with pytest.raises(ValueError, match="^registry_cms_source_pair_invalid$"):
        replace(pair, cms_receipt_payload=retained._json(payload))


def test_decoder_refuses_two_individually_valid_membership_recipes():
    pair = _retained_pair()
    other_epoch = replace(
        pair.recipe.source_pin.retained_epoch,
        epoch_id=UUID(int=2),
        schema_name="registry_cms_epoch_" + UUID(int=2).hex,
    )
    other_recipe = replace(pair.recipe, source_pin=replace(pair.recipe.source_pin, retained_epoch=other_epoch))
    document = pair.as_dict()
    document["recipes"] = json.loads(canonical_registry_source_recipes((pair.recipe, other_recipe)))
    with pytest.raises(ValueError, match="^registry_cms_source_pair_invalid$"):
        retained.decode_registry_cms_source_pair(document)
    assert pair.as_dict()["recipes"] != document["recipes"]


@pytest.mark.parametrize("container", ["pair", "proof", "runtime_roles"])
def test_decoder_refuses_unclosed_pair_metadata(container):
    pair = _retained_pair()
    document = pair.as_dict()
    if container == "runtime_roles":
        document["runtime_roles"] = pair.runtime_roles
    else:
        destination = document if container == "pair" else document["proof"]
        destination["unbound"] = True
    with pytest.raises(ValueError, match="^registry_cms_source_pair_invalid$"):
        retained.decode_registry_cms_source_pair(document)


@pytest.mark.parametrize(
    "side,coordinate",
    [
        ("source", "schema_name"),
        ("clone", "schema_name"),
        ("clone", "schema_oid"),
        ("clone", "relation_oids"),
    ],
)
def test_pair_binds_witness_names_and_preserves_opaque_legacy_tokens(side, coordinate):
    pair = _witness_pair()
    document = json.loads(pair.witness_json)
    if coordinate == "schema_name":
        document[side][coordinate] += "_other"
        document[side]["sequence"]["schema_name"] = document[side][coordinate]
    elif coordinate == "schema_oid":
        document[side][coordinate] += 1000
    else:
        next(entry for entry in document[side][coordinate] if entry[0] == "entity_address_unified")[1] += 1000
    foreign_wire = retained.decode_registry_cms_address_copy_witness(document)
    if coordinate == "schema_name":
        with pytest.raises(ValueError, match="^registry_cms_source_pair_invalid$"):
            replace(pair, witness_json=foreign_wire)
    else:
        rebound = replace(pair, witness_json=foreign_wire)
        assert rebound.address_ownership.relation_names == pair.address_ownership.relation_names
        assert rebound.witness_json == foreign_wire
    assert retained.decode_registry_cms_source_pair(pair.as_dict()) == pair


def test_named_witness_pair_roundtrip_uses_complete_qualified_names():
    pair = _witness_pair(named=True)
    document = pair.as_dict()
    assert set(document["address_ownership"]) == {"dataset_id", "schema_name", "relations"}
    assert set(document["address_copy_witness"]["clone"]) >= {"schema_name", "relations", "receipt"}
    assert "schema_oid" not in document["address_copy_witness"]["clone"]
    assert retained.decode_registry_cms_source_pair(document).as_dict() == document


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "rows", "layout"])
async def test_legacy_pair_uses_semantic_receipt_after_catalog_renumbering(monkeypatch, drift):
    pair = replace(_witness_pair(), witness_json=None)
    original_wire = retained._json(pair.as_dict())
    table = pair.address_receipt.tables[0]
    if drift == "rows":
        table = replace(table, row_count=table.row_count + 1, row_sha256="e" * 64)
    elif drift == "layout":
        table = replace(table, schema_sha256="e" * 64)
    observed = replace(pair.address_receipt, tables=(table, *pair.address_receipt.tables[1:]))
    capture = AsyncMock(return_value=observed)
    monkeypatch.setattr(retained, "capture_entity_address_archive_receipt", capture)
    session = object()
    if drift is None:
        await retained._require_address_pair_layout(session, pair, "f" * 64, verify_content=True)
    else:
        with pytest.raises(ValueError, match="^registry_cms_source_pair_legacy_semantic_receipt_changed$"):
            await retained._require_address_pair_layout(session, pair, "f" * 64, verify_content=True)
    capture.assert_awaited_once_with(
        session,
        schema_name=pair.address_ownership.schema_name,
        contract=pair.address_receipt.contract,
        read_only=True,
    )
    assert retained._json(pair.as_dict()) == original_wire
