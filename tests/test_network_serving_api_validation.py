# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic identity and retained-custody checks without database authority mocks."""

import json
from unittest.mock import Mock

import pytest

from process import network_candidate_custody as custody
from process import network_compact_readiness as compact
from process import network_serving_api_validation as validation
from process.network_compact_layout import CompactAddressRelation
from process.network_membership_writer_closure import writer_closure_receipt
from process.network_serving_read import NetworkServingReadUnavailable
from process.registry_management_permissions import _TABLES
from tests.test_network_candidate_custody import TARGET, _control_catalog


@pytest.mark.parametrize(
    "field,value",
    [
        ("role_oid", 24),
        ("database_oid", 13),
        ("server_address", "127.0.0.2"),
        ("server_port", 5433),
        ("backend_pid", 43),
        ("current_user", "another_reader"),
        ("isolation", "read committed"),
        ("read_only", "off"),
    ],
)
def test_independent_reader_identity_refuses_before_metadata_decode(field, value):
    reader_identity_by_field = dict(
        role_oid=23, database_oid=12, server_address="127.0.0.1", server_port=5432, backend_pid=42
    )
    metadata = reader_identity_by_field | {
        "session_user": "sample_reader",
        "current_user": "sample_reader",
        "isolation": "repeatable read",
        "read_only": "on",
        field: value,
    }
    with pytest.raises(NetworkServingReadUnavailable):
        validation.validate_api_serving_pin(
            metadata, schema="sample_control", backend_pid=42, reader_identity=reader_identity_by_field
        )


def _control_tables(control):
    oids_by_name = {
        "network_membership_candidate": 13,
        "network_candidate_custody": 14,
        "network_serving_manifest": 20,
        "registry_approved_record": 21,
        "network_serving_control": 22,
    }
    return [
        dict(
            relname=name,
            oid=oid,
            relowner=control["owner_oid"] if name == "network_candidate_custody" else control["schema_owner_oid"],
            relkind="r",
            relpersistence="p",
            heap=True,
            relrowsecurity=False,
            relforcerowsecurity=False,
            relispartition=False,
            inherited=False,
            rules=False,
            columns=list(_TABLES[name]),
        )
        for name, oid in oids_by_name.items()
    ]


@pytest.mark.parametrize("split_owner", [False, True])
def test_five_consumed_controls_keep_exact_owner_and_approved_relation(split_owner):
    control = _control_catalog()
    if split_owner:
        control["schema_owner_oid"] = 77
        control["candidate_owner_oid"] = 77
    metadata = {"control_tables_json": json.dumps(_control_tables(control)), "control_consumed": True}
    assert validation._verified_control_pins(metadata, control, "sample_control") == CompactAddressRelation(
        "sample_control", "registry_approved_record", 21
    )


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "extra",
        "candidate_oid",
        "seal_oid",
        "owner",
        "rls",
        "columns",
        "consumed",
        "owned",
        "closed",
        "login",
    ],
)
def test_control_pin_drift_refuses(damage):
    control = _control_catalog()
    tables = _control_tables(control)
    metadata = {"control_consumed": True}
    match damage:
        case "missing":
            tables.pop()
        case "extra":
            tables.append(tables[0] | {"relname": "unexpected_table", "oid": 99})
        case "candidate_oid":
            tables[0]["oid"] = 99
        case "seal_oid":
            tables[1]["oid"] = 99
        case "owner":
            tables[2]["relowner"] = 99
        case "rls":
            tables[2]["relrowsecurity"] = True
        case "columns":
            tables[2]["columns"].pop()
        case "consumed":
            metadata["control_consumed"] = False
        case "owned" | "closed":
            control[damage] = False
        case "login":
            control["rolcanlogin"] = True
    metadata["control_tables_json"] = json.dumps(tables)
    with pytest.raises(NetworkServingReadUnavailable):
        validation._verified_control_pins(metadata, control, "sample_control")


def _edge_indexes(heap_oids_by_name):
    paths = [
        ("network_slice_ref", ["network_id", "office_bucket"]),
        ("network_office_edge", ["slice_id", "location_key"]),
        ("network_office_edge", ["location_key", "slice_id"]),
    ]
    return [
        dict(
            table_name=table,
            index_oid=50 + position,
            table_oid=heap_oids_by_name[table],
            indisvalid=True,
            indisready=True,
            indislive=True,
            indisprimary=position < 2,
            indisunique=position < 2,
            indnkeyatts=2,
            indnatts=2,
            amname="btree",
            no_predicate=True,
            no_expressions=True,
            columns=columns,
            definition=f"CREATE {'UNIQUE ' if position < 2 else ''}INDEX index_{position} ON {table} ({','.join(columns)})",
        )
        for position, (table, columns) in enumerate(paths)
    ]


def _retained_edge():
    """Build protected native inventory and its exact projected custody header."""
    control = _control_catalog()
    heap_oids_by_name = dict(
        network_membership=40, provider_location_binding=41, network_slice_ref=42, network_office_edge=43
    )
    indexes = _edge_indexes(heap_oids_by_name)
    relation_by_field = dict(relpersistence="p", relowner=control["owner_oid"], relacl=None)
    relations = [
        dict(relname=name, oid=oid, relkind="r", indrelid=None, **relation_by_field)
        for name, oid in heap_oids_by_name.items()
    ]
    relations += [
        dict(
            relname=f"index_{position}",
            oid=index["index_oid"],
            relkind="i",
            indrelid=index["table_oid"],
            **relation_by_field,
        )
        for position, index in enumerate(indexes)
    ]
    catalog_by_field = {
        "namespace": {"oid": 30, "nspowner": control["owner_oid"]},
        "heaps": heap_oids_by_name,
        "relations": [relation | {"relkind": relation["relkind"].encode()} for relation in relations],
    }
    roles = custody._final_roles(control["rolname"], ("sample_reader",))
    closure = writer_closure_receipt(TARGET, roles, catalog_by_field)
    receipt_by_field = {
        "component": "network_candidate_custody",
        "revision": 4,
        "control": custody._control_identity(control, 4),
        "closure": closure,
        "index_definition_sha256": compact._digest(indexes),
        "runtime_roles": ["sample_reader", "sample_worker"],
    }
    family_by_field = dict(
        candidate_id=TARGET.candidate_id,
        candidate={field: getattr(TARGET, field) for field in custody._STABLE[:5]} | {"state": "published"},
        receipt_json=receipt_by_field,
        seal_sha256="a" * 64,
        **dict.fromkeys(("projected_edge", "closed", "q_reach_closed", "census_closed"), True),
        indexes=indexes,
        catalog=dict(
            schema_oid=30,
            schema_owner_oid=control["owner_oid"],
            owner_oid=control["owner_oid"],
            owner=control.copy(),
            relations=relations,
            has_routines=False,
            heaps_safe=True,
        ),
    )
    expected_by_field = dict(
        candidate_id=TARGET.candidate_id, schema_name=TARGET.schema_name, table_oid=43, seal_sha256="a" * 64
    )
    return family_by_field, control, expected_by_field


@pytest.mark.parametrize("revision", [4, 5])
def test_retained_edge_uses_bound_projection_without_replaying_historical_seal(monkeypatch, revision):
    family_by_field, control, expected_by_field = _retained_edge()
    family_by_field["receipt_json"]["revision"] = revision
    full_seal = Mock(side_effect=AssertionError("historical full proof replayed"))
    monkeypatch.setattr(custody, "_sealed_receipt", full_seal)
    candidate, evidence = validation._retained_edge_metadata(family_by_field, control, expected_by_field)
    assert candidate == family_by_field["candidate"] and evidence["receipt"] == family_by_field["receipt_json"]
    assert evidence["seal_sha256"] == expected_by_field["seal_sha256"] and evidence["api_projected_edge"] is True
    full_seal.assert_not_called()


@pytest.mark.parametrize(
    "damage",
    [
        "anchor",
        "ledger_sha",
        "projection",
        "edge_oid",
        "closure",
        "extra_object",
        "index_digest",
        "index_path",
        "closed",
        "q_reach_closed",
        "census_closed",
        "unknown_revision",
        "extra_proof",
    ],
)
def test_retained_edge_projection_preserves_live_identity_and_mutation_guards(damage):
    family_by_field, control, expected_by_field = _retained_edge()
    receipt_by_field = family_by_field["receipt_json"]
    match damage:
        case "anchor":
            expected_by_field["seal_sha256"] = "b" * 64
        case "ledger_sha":
            family_by_field["seal_sha256"] = "b" * 64
        case "projection":
            family_by_field["projected_edge"] = False
        case "edge_oid":
            expected_by_field["table_oid"] = 99
        case "closure":
            receipt_by_field["closure"]["relation_oids"]["network_office_edge"] = 99
        case "extra_object":
            family_by_field["catalog"]["relations"].append(
                family_by_field["catalog"]["relations"][0] | {"relname": "extra", "oid": 99}
            )
        case "index_digest":
            family_by_field["indexes"][0]["definition"] += " changed"
        case "index_path":
            family_by_field["indexes"].pop()
            receipt_by_field["index_definition_sha256"] = compact._digest(family_by_field["indexes"])
        case "closed" | "q_reach_closed" | "census_closed":
            family_by_field[damage] = False
        case "unknown_revision":
            receipt_by_field["revision"] = 6
        case "extra_proof":
            receipt_by_field["semantic_validation"] = {}
    with pytest.raises(ValueError):
        validation._retained_edge_metadata(family_by_field, control, expected_by_field)
