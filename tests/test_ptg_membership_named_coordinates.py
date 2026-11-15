# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Copied named membership coordinates preserve data and retained legacy pins."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import ptg_membership_contract as contract
from api import ptg_membership_storage as storage
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_ptg2_physical_binding import _binding


def _named_identity():
    binding = contract.physical_identity(SimpleNamespace(physical_binding=_binding()))
    return {
        "contract": contract.CONTRACT,
        "source_key": "synthetic-network",
        "snapshot_id": "synthetic-destination",
        "code_system": "CPT",
        "code": "99213",
        "physical_binding": binding,
        "source_storage": {
            "schema_name": binding["schema_name"],
            "relation_names": sorted(name for _, name in binding["relations"]),
        },
        "graph_root": {"map_digest": "a" * 64, "entry_count": 7},
        "finalizer": {
            "state": "packed",
            "map_digest": "b" * 64,
            "canonical_mapping_digest": "c" * 64,
            "target_identity_digest": "d" * 64,
        },
    }


def _legacy_identity(offset=0):
    value = _named_identity()
    binding = value["physical_binding"]
    binding.update(
        database_oid=10 + offset,
        schema_oid=20 + offset,
        owner_oid=30 + offset,
        schema_owner_oid=40 + offset,
        relfilenode=50 + offset,
    )
    binding["relations"] = [[name, 100 + offset + index] for index, (_schema, name) in enumerate(binding["relations"])]
    binding["sequences"] = [
        [name, 200 + offset + index, table, column]
        for index, (_schema, name, table, column) in enumerate(binding["sequences"])
    ]
    stored = value["source_storage"]
    stored["relation_oids"] = [[name, 300 + offset + index] for index, name in enumerate(stored.pop("relation_names"))]
    stored["database_oid"], stored["schema_oid"] = 400 + offset, 500 + offset
    return value


def test_copied_tokens_preserve_named_source_and_legacy_bytes():
    first, copied, named = _legacy_identity(), _legacy_identity(1000), _named_identity()
    originals = [contract.encoded(value).encode() for value in (first, copied, named)]
    assert contract.logical_source_identity(first) == contract.logical_source_identity(copied) == named
    assert [contract.encoded(value).encode() for value in (first, copied, named)] == originals


@pytest.mark.parametrize("field", ["graph_root", "finalizer", "code", "snapshot_id"])
async def test_named_comparison_still_refuses_changed_semantic_data_before_coverage_read(field):
    original = _legacy_identity()
    expected = _named_identity()
    expected[field] = "different" if field in {"code", "snapshot_id"} else {"different": True}
    header_by_field = {"identity": original, "content_digest": "e" * 64, "counts": {"code_coverage": []}}
    session = SimpleNamespace(execute=AsyncMock(return_value=_Rows([header_by_field])))
    with pytest.raises(PTG2ManifestArtifactError, match="identity differs"):
        await storage._sealed_header(session, "reference_family_archive_" + UUID(int=2).hex, expected)
    assert session.execute.await_count == 1


@pytest.mark.parametrize(
    "damage",
    [
        "schema",
        "relation",
        "duplicate_relation",
        "crossed_relation",
        "sequence",
        "duplicate_sequence",
        "crossed_sequence",
    ],
)
def test_raw_named_binding_refuses_unsafe_or_duplicate_coordinates(damage):
    value = _named_identity()
    binding = value["physical_binding"]
    match damage:
        case "schema":
            binding["schema_name"] = 'unsafe"namespace'
        case "relation":
            binding["relations"][0][1] = 'unsafe"table'
        case "duplicate_relation":
            binding["relations"].append(deepcopy(binding["relations"][0]))
        case "crossed_relation":
            binding["relations"][0] = [binding["relations"][0][1], "foreign"]
        case "sequence":
            binding["sequences"][0][1] = "unsafe;sequence"
        case "crossed_sequence":
            entry = binding["sequences"][0]
            binding["sequences"][0] = [entry[1], "foreign", entry[2], entry[3]]
        case _:
            binding["sequences"].append(deepcopy(binding["sequences"][0]))
    with pytest.raises(ValueError):
        contract.logical_source_identity(value)


@pytest.mark.parametrize("damage", ["schema", "duplicate", "relation"])
def test_receipt_normalization_refuses_unusable_named_storage(damage):
    identity = _named_identity()
    value_by_field = {
        **identity,
        "active_attachment": {"name": storage.attachment_name(identity)},
        "storage": {"dataset_id": str(UUID(int=2)), "relation_names": list(contract.SPEC.table_names)},
    }
    if damage == "schema":
        value_by_field["storage"]["schema_name"] = "foreign"
    elif damage == "duplicate":
        value_by_field["storage"]["relation_names"].append(value_by_field["storage"]["relation_names"][0])
    else:
        value_by_field["storage"]["relation_names"][0] = "unsafe.table"
    with pytest.raises(ValueError):
        contract.logical_receipt(value_by_field)


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def first(self):
        return self.rows[0] if self.rows else None

    def one(self):
        assert len(self.rows) == 1
        return self.rows[0]

    def __iter__(self):
        return iter(self.rows)


async def test_retained_old_lookup_keeps_raw_pin_identity_while_emitting_named_receipt(monkeypatch):
    legacy = _legacy_identity()
    encoded_before = contract.encoded(legacy).encode()
    owner = SimpleNamespace(
        dataset_id=UUID(int=2),
        schema_name="reference_family_archive_" + UUID(int=2).hex,
        relation_names=contract.SPEC.table_names,
    )
    header_by_field = {"identity": legacy, "content_digest": "e" * 64, "counts": {"code_coverage": []}}
    seen_pins = []

    async def execute(statement, params=None):
        query = str(statement)
        if "relkind='v'" in query:
            assert params == {"name": storage.attachment_name(legacy)}
            return _Rows([(True, True)])
        if "SELECT identity,content_digest,counts" in query:
            return _Rows([header_by_field])
        assert "SELECT code_key,occurrence_count,set_count,set_digest" in query
        return _Rows([])

    async def has_pin(statement, params):
        assert "ptg2_snapshot_pin" in str(statement)
        seen_pins.append(deepcopy(params))
        return True

    session = SimpleNamespace(execute=execute, scalar=has_pin)
    monkeypatch.setattr(storage, "_attachment_storage", AsyncMock(return_value=owner))
    found = await storage.locate(session, legacy)
    fresh = storage.receipt(found)
    raw_pin_by_field = {
        "owner_type": contract.CONTRACT,
        "owner_id": contract.digest(legacy),
        "snapshot_id": legacy["snapshot_id"],
    }
    assert seen_pins == [raw_pin_by_field] and fresh["source_pin"] == raw_pin_by_field
    assert fresh["projection_id"] == contract.digest(contract.logical_source_identity(legacy))
    assert set(fresh["storage"]) == {"dataset_id", "schema_name", "relation_names"}
    assert fresh["active_attachment"] == {
        "schema_name": "hp_snapshot_retention",
        "name": storage.attachment_name(legacy),
    }
    assert fresh["physical_binding"] == _named_identity()["physical_binding"]
    assert fresh["source_storage"] == _named_identity()["source_storage"]
    assert contract.logical_receipt(fresh) == fresh
    assert contract.encoded(legacy).encode() == encoded_before


def test_old_and_named_receipts_keep_data_and_pin_identity():
    legacy = _legacy_identity()
    pin_by_field = {
        "owner_type": contract.CONTRACT,
        "owner_id": contract.digest(legacy),
        "snapshot_id": legacy["snapshot_id"],
    }
    owner = SimpleNamespace(
        dataset_id=UUID(int=2),
        schema_name="reference_family_archive_" + UUID(int=2).hex,
        relation_names=contract.SPEC.table_names,
    )
    header_by_field = {"identity": legacy, "content_digest": "e" * 64, "counts": {"code_coverage": []}}
    view = "hp_snapshot_retention." + storage.attachment_name(legacy)
    fresh = storage.receipt((owner, header_by_field, None, view, pin_by_field))
    old_by_field = {
        **legacy,
        "state": "ready",
        "projection_id": contract.digest(legacy),
        "content_digest": header_by_field["content_digest"],
        "counts": header_by_field["counts"],
        "code_coverage": [],
        "active_attachment": {"name": storage.attachment_name(legacy), "relation_oid": 501, "owner_oid": 502},
        "source_pin": pin_by_field,
        "storage": {
            "dataset_id": str(owner.dataset_id),
            "schema_oid": 503,
            "relation_oids": [[name, 600 + index] for index, name in enumerate(contract.SPEC.table_names)],
        },
    }
    encoded_before = contract.encoded(old_by_field).encode()
    assert contract.logical_receipt(old_by_field) == fresh
    assert contract.encoded(old_by_field).encode() == encoded_before


def test_historical_parent_binding_without_schema_name_remains_portable_and_raw():
    named = _named_identity()
    old = deepcopy(named)
    binding = old["physical_binding"]
    binding.pop("schema_name")
    binding["schema_oid"], binding["owner_oid"] = 91, 92
    binding["relation_oids"] = [[name, 100 + index] for index, (_schema, name) in enumerate(binding.pop("relations"))]
    binding["sequence_oids"] = [
        [name, 200 + index, table, column]
        for index, (_schema, name, table, column) in enumerate(binding.pop("sequences"))
    ]
    old["source_storage"] = {
        "database_oid": 90,
        "schema_oid": 91,
        "relation_oids": [[name, 100 + index] for index, name in enumerate(named["source_storage"]["relation_names"])],
    }
    copied = deepcopy(old)
    copied["physical_binding"]["schema_oid"] += 1000
    copied["physical_binding"]["owner_oid"] += 1000
    copied["physical_binding"]["relation_oids"] = [
        [name, token + 1000] for name, token in copied["physical_binding"]["relation_oids"]
    ]
    copied["physical_binding"]["sequence_oids"] = [
        [name, token + 1000, table, column]
        for name, token, table, column in copied["physical_binding"]["sequence_oids"]
    ]
    encoded_before_values = [contract.encoded(value).encode() for value in (old, copied)]
    assert contract.logical_source_identity(old) == contract.logical_source_identity(copied) == named
    assert [contract.encoded(value).encode() for value in (old, copied)] == encoded_before_values
