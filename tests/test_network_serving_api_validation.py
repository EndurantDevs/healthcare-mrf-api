# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic identity, native-layout metadata and component assembly checks."""

import copy
import hashlib
import json
from types import SimpleNamespace
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


def _native_index_digest(records):
    """Hash independent deparsed definitions using the supported stable encoding."""
    return hashlib.sha256(
        json.dumps(
            sorted(row["index_definition"] for row in records), separators=(",", ":"), ensure_ascii=False
        ).encode()
    ).hexdigest()


def _native_serving_records(schema):
    """Keep canonical and legacy deparser projections independently selectable."""
    index_records = [
        dict(
            relname=name,
            indrelid=52,
            table_oid=52,
            index_oid=100 + index,
            indisvalid=True,
            indisready=True,
            indislive=True,
            index_definition=sql,
        )
        for index, (name, sql) in enumerate(validation._serving_plan(schema))
    ]
    legacy_records = [
        row | {"index_definition": row["index_definition"].replace(f'"{schema}".', "")} for row in index_records
    ]
    return index_records, legacy_records


def _indexed_base_inputs():
    """Model independent native shape/index projections without issuing authority."""
    from tests.test_network_indexed_address_artifact import _descriptor

    descriptor = _descriptor()
    schema = descriptor["namespace"]["name"]
    index_records, legacy = _native_serving_records(schema)
    digest = _native_index_digest
    descriptor["index_definition_sha256"] = digest(index_records)
    columns = [{"attname": "location_key", "type": "text", "default_expression": None}]
    shape_by_field = dict(table_name="entity_address_unified", columns=columns, constraints=[], indexes=[])
    descriptor["catalog"]["shapes"] = [
        ["entity_address_unified", 52, validation.shape._canonical_digest(shape_by_field)]
    ]
    physical_by_field = dict(
        schema_name=schema,
        schema_oid=50,
        owner_oid=51,
        oid=52,
        relfilenode=53,
        relname="entity_address_unified",
        relkind="r",
        relpersistence="p",
        **dict.fromkeys(("relrowsecurity", "relforcerowsecurity", "relispartition", "rules", "inherited"), False),
    )
    evidence_by_field = dict(
        native_structure_safe=True,
        native_execution_safe=True,
        native_subjects_safe=True,
        subjects=["reader", "worker"],
        original_path="pg_catalog",
        restored_path="pg_catalog",
        physical=physical_by_field,
        schema_name=schema,
        table_name="entity_address_unified",
        relation_oid=52,
        shape_columns=columns,
        shape_constraints=[],
        shape_indexes=[],
        qualified_index_records=index_records,
        legacy_index_records=legacy,
        **{"catalog_" + key: descriptor["catalog"][key] for key in ("columns", "constraints", "indexes")},
    )
    metadata = dict(
        database_oid=1,
        database_name="example",
        indexed_base_json=evidence_by_field,
        indexed_actor_safety_json=dict.fromkeys(
            ("descriptor_roles_safe", "ordinary_mutation_closed", "owner_safe", "runtime_closure_safe"), True
        ),
    )
    candidate_by_field = {
        "validation_json": {"writer_closure": {"loader_roles": [], "reader_roles": ["reader", "worker"]}}
    }
    return metadata, descriptor, candidate_by_field, digest


@pytest.mark.parametrize("legacy", [False, True])
def test_indexed_base_accepts_exact_canonical_or_legacy_native_digest(legacy):
    metadata, descriptor, candidate, digest = _indexed_base_inputs()
    if legacy:
        descriptor["index_definition_sha256"] = digest(metadata["indexed_base_json"]["legacy_index_records"])
    before = copy.deepcopy((metadata, descriptor, candidate))
    assert validation._validated_indexed_base(metadata, descriptor, candidate) == CompactAddressRelation(
        descriptor["namespace"]["name"], "entity_address_unified", 52
    )
    assert (metadata, descriptor, candidate) == before


@pytest.mark.parametrize(
    "damage",
    [
        "actor",
        "native",
        "subjects",
        "path",
        "heap",
        "rls",
        "database",
        "namespace",
        "relfilenode",
        "roles",
        "shape",
        "catalog",
        "index_digest",
        "index_missing",
        "index_invalid",
    ],
)
def test_indexed_base_refuses_projection_drift(damage):
    metadata, descriptor, candidate, _digest = _indexed_base_inputs()
    evidence = metadata["indexed_base_json"]
    match damage:
        case "actor":
            metadata["indexed_actor_safety_json"]["owner_safe"] = False
        case "native":
            evidence["native_execution_safe"] = False
        case "subjects":
            evidence["subjects"] = ["worker", "reader"]
        case "path":
            evidence["restored_path"] = "public"
        case "heap":
            evidence["physical"]["relkind"] = "v"
        case "rls":
            evidence["physical"]["relrowsecurity"] = True
        case "database":
            metadata["database_oid"] = 99
        case "namespace":
            evidence["physical"]["schema_oid"] = 99
        case "relfilenode":
            evidence["physical"]["relfilenode"] = 99
        case "roles":
            candidate["validation_json"]["writer_closure"]["reader_roles"] = ["other"]
        case "shape":
            evidence["shape_columns"][0]["type"] = "bigint"
        case "catalog":
            evidence["catalog_indexes"] = []
        case "index_digest":
            descriptor["index_definition_sha256"] = "0" * 64
        case "index_missing":
            evidence["qualified_index_records"].pop()
        case "index_invalid":
            evidence["qualified_index_records"][0]["indisvalid"] = False
    with pytest.raises(NetworkServingReadUnavailable):
        validation._validated_indexed_base(metadata, descriptor, candidate)


@pytest.mark.parametrize("damage", [None, "base", "owned_edge", "retained_seal", "retained_oid", "ref", "extra_field"])
def test_compact_layout_keeps_exact_owned_and_retained_coordinates(damage):
    from tests.test_network_reader_metadata import _candidate

    candidate, retained, expected = _candidate()
    descriptor = candidate["validation_json"]["serving_readiness"]["address_artifact"]
    base = CompactAddressRelation("synthetic_addresses", "entity_address_unified", 41)
    families_by_id = {
        retained["candidate_id"]: (
            retained,
            {
                "seal_sha256": expected["seal_sha256"],
                "receipt": {"closure": {"relation_oids": {"network_office_edge": 53}}},
            },
        )
    }
    report = candidate["validation_json"]
    match damage:
        case "base":
            descriptor["table"]["oid"] = 99
        case "owned_edge":
            report["serving_readiness"]["edge_artifacts"][0]["table_oid"] = 99
        case "retained_seal":
            expected["seal_sha256"] = "d" * 64
        case "retained_oid":
            expected["table_oid"] = 99
        case "ref":
            report["writer_closure"]["relation_oids"]["network_slice_ref"] = 99
        case "extra_field":
            expected["extra"] = True
    if damage is not None:
        with pytest.raises(NetworkServingReadUnavailable):
            validation._verified_compact_layout(candidate, base, descriptor, families_by_id)
    else:
        layout = validation._verified_compact_layout(candidate, base, descriptor, families_by_id)
        assert layout.base is base and layout.ref_map.table_oid == 51 and layout.custom is None
        assert [entry.table_oid for entry in layout.artifacts] == [52, 53]


def _api_metadata_with_components():
    """Model assembly only; component doubles do not establish native custody."""
    from tests.test_network_reader_metadata import _candidate

    metadata, descriptor, _, _ = _indexed_base_inputs()
    candidate, _, _ = _candidate()
    report = candidate["validation_json"]
    report["candidate_readiness"].update(ref_map_oid=83, edge_heap_oid=84)
    report["writer_closure"].update(loader_roles=[], reader_roles=["reader", "worker"])
    report["writer_closure"]["relation_oids"].update(network_slice_ref=83, network_office_edge=84)
    family, control, expected = _retained_edge()
    report["serving_readiness"]["edge_artifacts"] = [
        expected,
        dict(
            candidate_id=candidate["candidate_id"], schema_name=candidate["schema_name"], table_oid=84, seal_sha256=None
        ),
    ]
    descriptor["descriptor_sha256"] = validation.indexed.artifact._digest(
        {key: field_value for key, field_value in descriptor.items() if key != "descriptor_sha256"}
    )
    current_by_field = {"candidate_id": candidate["candidate_id"], "projected_edge": False}
    identity_by_field = dict(role_oid=23, database_oid=1, server_address="127.0.0.1", server_port=5432, backend_pid=42)
    metadata.update(
        identity_by_field,
        session_user="reader",
        current_user="reader",
        isolation="repeatable read",
        read_only="on",
        control_json=control,
        control_tables_json=json.dumps(_control_tables(control)),
        control_consumed=True,
        candidate_json=candidate,
        created_at="2026-01-01T00:00:00+00:00",
        indexed_descriptor_json=descriptor,
        families_json=json.dumps([current_by_field, family]),
        edge_candidate_count=1,
        manifest_json={"eligible": True},
        approved_json={"is_bounded": True, "generation_id": "a" * 64, "total_rows": 4},
    )
    manifest = SimpleNamespace(
        generation_id=3, source_generations={"custom_membership": "a" * 64}, approved_custom_revision=7
    )
    seal_by_field = {"receipt": {"component": "verified_current"}, "seal_sha256": "b" * 64}
    return metadata, identity_by_field, candidate, current_by_field, manifest, seal_by_field


@pytest.mark.parametrize("damage", [None, "generation", "eligibility", "approval"])
def test_validated_components_assemble_only_one_matching_generation(monkeypatch, damage):
    metadata, identity, candidate, current, manifest, seal = _api_metadata_with_components()
    current_custody = Mock(return_value=(candidate, seal))
    pinned_manifest = Mock(return_value=manifest)
    monkeypatch.setattr(validation, "_full_candidate_custody", current_custody)
    monkeypatch.setattr(validation, "_pinned_manifest", pinned_manifest)
    if damage == "eligibility":
        metadata["manifest_json"]["eligible"] = False
    if damage == "approval":
        metadata["approved_json"]["generation_id"] = "c" * 64
    arguments_by_field = dict(
        schema="sample_control",
        backend_pid=42,
        reader_identity=identity,
        generation_id=99 if damage == "generation" else 3,
    )
    if damage is not None:
        with pytest.raises(NetworkServingReadUnavailable):
            validation.validate_api_serving_pin(metadata, **arguments_by_field)
    else:
        actual, approved, relation = validation.validate_api_serving_pin(metadata, **arguments_by_field)
        assert actual is manifest and approved.generation_id == "a" * 64 and approved.total_rows == 4
        assert relation == CompactAddressRelation("sample_control", "registry_approved_record", 21)
    current_custody.assert_called_once_with(current, metadata["control_json"])
    assert pinned_manifest.call_count == 1


@pytest.mark.parametrize("pairs,rows", [(0, 0), (0, 1), (1, 1), (1, None)])
def test_compact_custom_layout_keeps_the_verified_carrier_oid(monkeypatch, pairs, rows):
    from process.network_address_projection import PinnedAddressSource
    from tests.test_network_reader_metadata import ROLES, _candidate

    candidate, _, _ = _candidate()
    report = candidate["validation_json"]
    descriptor = report["serving_readiness"]["address_artifact"]
    common_by_field = dict(
        base_source=PinnedAddressSource(**report["address_source"]),
        base_artifact_sha256=descriptor["descriptor_sha256"],
        binding_table_oid=55,
        runtime_roles=ROLES,
        address_table_oid=54,
    )
    monkeypatch.setattr(
        validation,
        "compact_custom_receipt",
        lambda _candidate: SimpleNamespace(**common_by_field, custom_pair_count=pairs),
    )
    monkeypatch.setattr(
        validation,
        "compact_carrier_receipt",
        lambda _candidate: None if rows is None else SimpleNamespace(**common_by_field, materialized_address_rows=rows),
    )
    expected = CompactAddressRelation(candidate["schema_name"], "entity_address_unified", 54) if pairs or rows else None
    assert validation._custom_address_relation(candidate, descriptor) == expected
