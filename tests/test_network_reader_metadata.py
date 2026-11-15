# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Address artifact and compact dependency verification on a synthetic connection."""

from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest

from process import entity_address_snapshot_receipt as receipt
from process import network_address_artifact as artifact
from process import network_candidate_custody as custody
from process import network_compact_custom_preparation as custom
from process import network_compact_dependencies as dependencies
from process.network_address_projection import PinnedAddressSource
from process.network_compact_layout import CompactAddressRelation

SCHEMA = "synthetic_addresses"
ROLES = ("loader", "reader")
CONTROL = "synthetic_control"
OWN = str(UUID(int=10))
RETAINED = str(UUID(int=20))


def _descriptor():
    origin_by_field = {
        "local_lineage_id": str(UUID(int=1)),
        "local_generation": 1,
        "origin_lineage_id": str(UUID(int=2)),
        "origin_generation": 7,
        "published_at": "2026-01-01T00:00:00.000000Z",
        "relation_oids": list(range(41, 41 + len(artifact.generation.RELATION_NAMES))),
    }
    document_by_field = {
        "contract": artifact.CONTRACT,
        "database": {"oid": 11, "name": "synthetic"},
        "namespace": {"name": SCHEMA, "oid": 12, "owner_oid": 100},
        "protected_owner_oid": 100,
        "table": {"oid": 41, "relfilenode": 91},
        "authority_table_oid": 80,
        "authority": origin_by_field,
        "generation_id": artifact._digest(origin_by_field),
        "alias_generation": 0,
        "alias_catalog_sha256": "a" * 64,
        "catalog": {
            "address": {"shapes": [["entity_address_unified", 41, "a" * 64]]},
            "authority": {"shapes": [[artifact._AUTHORITY, 80, "b" * 64]]},
        },
        "runtime_roles": list(ROLES),
    }
    document_by_field["descriptor_sha256"] = artifact._digest(document_by_field)
    return artifact._descriptor(document_by_field)


def _physical(oid, name):
    return {
        "oid": oid,
        "relname": name,
        "relfilenode": 91 if oid == 41 else 92,
        "owner_oid": 100,
        "schema_oid": 12,
        "schema_name": SCHEMA,
        "schema_owner_oid": 100,
        "relkind": "r",
        "relpersistence": "p",
        "relrowsecurity": False,
        "relforcerowsecurity": False,
        "relispartition": False,
        "rules": False,
        "inherited": False,
    }


class Connection:
    def __init__(self):
        self.calls = []
        self.physical = {41: _physical(41, "entity_address_unified"), 80: _physical(80, artifact._AUTHORITY)}
        self.schema_hashes = {"entity_address_unified": "a" * 64, artifact._AUTHORITY: "b" * 64}
        self.active, self.denied_name = True, None
        self.candidate_by_name = None
        self.origin_by_field = {
            field: _descriptor()["authority"][field]
            for field in ("origin_lineage_id", "origin_generation", "published_at")
        }

    def is_in_transaction(self):
        return self.active

    async def fetchrow(self, statement, *arguments):
        self.calls.append((statement, arguments))
        if statement.startswith("SELECT origin_lineage_id,origin_generation,published_at"):
            if self.denied_name == artifact._AUTHORITY:
                raise asyncpg.InsufficientPrivilegeError("native SELECT denied")
            if not any(row["relname"] == artifact._AUTHORITY for row in self.physical.values()):
                raise asyncpg.UndefinedTableError("named authority absent")
            return self.origin_by_field
        if statement.startswith("SELECT c.oid::bigint,c.relname"):
            return self.physical.get(arguments[0])
        assert statement.startswith('SELECT * FROM "synthetic_control".network_membership_candidate')
        assert arguments == (UUID(RETAINED),)
        return self.candidate_by_name

    async def fetchval(self, statement, *arguments):
        self.calls.append((statement, arguments))
        assert statement.startswith("SELECT to_regclass")
        selected = next(
            (oid for oid, row in self.physical.items() if arguments == (f'"{row["schema_name"]}"."{row["relname"]}"',)),
            None,
        )
        return selected is not None if statement.endswith("IS NOT NULL") else selected

    async def execute(self, statement, *arguments):
        self.calls.append((statement, arguments))
        assert statement.startswith("LOCK TABLE ONLY ")
        relation = statement.removeprefix("LOCK TABLE ONLY ").removesuffix(" IN ACCESS SHARE MODE NOWAIT")
        if relation == f'"{SCHEMA}"."{self.denied_name}"':
            raise asyncpg.InsufficientPrivilegeError("native SELECT denied")
        if relation not in {f'"{row["schema_name"]}"."{row["relname"]}"' for row in self.physical.values()}:
            raise asyncpg.UndefinedTableError("named relation absent")
        return "LOCK TABLE"

    async def prepare(self, statement):
        self.calls.append((statement, ()))
        assert statement in (
            "SHOW search_path",
            "SET LOCAL search_path=pg_catalog,pg_temp",
            "SELECT set_config('search_path',$1::VARCHAR,true)",
        )
        rows = [{"search_path": "public"}] if statement == "SHOW search_path" else []
        return SimpleNamespace(get_attributes=lambda: (), fetch=AsyncMock(return_value=rows))


def _schema_spy(monkeypatch, connection):
    async def schema_identity(session, oid, schema, logical, *, names_by_stage):
        assert session.native_connection is connection and schema == SCHEMA
        assert names_by_stage == {connection.physical[oid]["relname"]: logical}
        return connection.schema_hashes[logical]

    schema = AsyncMock(side_effect=schema_identity)
    monkeypatch.setattr(receipt, "_schema_identity", schema)
    return schema


@pytest.mark.parametrize("name", ["live", "portable", "legacy"])
async def test_address_verification_keeps_both_explicit_locks(monkeypatch, name):
    descriptor = _descriptor()
    connection = Connection()
    if name == "portable":
        origin_by_field = {
            field: descriptor["authority"][field]
            for field in ("origin_lineage_id", "origin_generation", "published_at")
        }
        connection.physical[41]["relname"] = artifact.retained_address_name("entity_address_unified", origin_by_field)
    elif name == "legacy":
        connection.physical[41]["relname"] = "entity_address_unified_retained_29"
    schema = _schema_spy(monkeypatch, connection)
    assert (await artifact.verify_network_address_artifact(connection, descriptor, runtime_roles=ROLES)).table_oid == 41
    statements = [sql for sql, arguments in connection.calls]
    assert [sql for sql in statements if sql.startswith("LOCK TABLE")] == [
        f'LOCK TABLE ONLY "{SCHEMA}"."{connection.physical[41]["relname"]}" IN ACCESS SHARE MODE NOWAIT',
        f'LOCK TABLE ONLY "{SCHEMA}"."entity_address_result_generation" IN ACCESS SHARE MODE NOWAIT',
    ]
    assert [call.args[1:4] for call in schema.await_args_list] == [
        (41, SCHEMA, "entity_address_unified"),
        (80, SCHEMA, artifact._AUTHORITY),
    ]
    assert not any("transaction_read_only" in sql for sql in statements)
    assert any("to_regclass" in sql for sql in statements)
    assert statements[-1] == "SELECT set_config('search_path',$1::VARCHAR,true)"


@pytest.mark.parametrize("oid", [41, 80], ids=["address", "authority"])
@pytest.mark.parametrize(
    "damage",
    [
        "absent",
        "schema",
        "name",
        "heap_kind",
        "temporary",
        "rls",
        "force_rls",
        "partition",
        "rules",
        "inherited",
        "shape",
    ],
)
async def test_address_verification_keeps_named_schema_refusals(monkeypatch, oid, damage):
    descriptor, connection = _descriptor(), Connection()
    _schema_spy(monkeypatch, connection)
    changes_by_fault = {
        "schema": ("schema_name", "other_schema"),
        "name": ("relname", "other_table"),
        "heap_kind": ("relkind", "v"),
        "temporary": ("relpersistence", "t"),
        "rls": ("relrowsecurity", True),
        "force_rls": ("relforcerowsecurity", True),
        "partition": ("relispartition", True),
        "rules": ("rules", True),
        "inherited": ("inherited", True),
    }
    if damage in changes_by_fault:
        field, changed = changes_by_fault[damage]
        connection.physical[oid][field] = changed
    if damage == "absent":
        connection.physical.pop(oid)
    if damage == "shape":
        connection.schema_hashes[connection.physical[oid]["relname"]] = "c" * 64
    with pytest.raises((artifact.NetworkAddressArtifactError, asyncpg.UndefinedTableError)):
        await artifact.verify_network_address_artifact(connection, descriptor, runtime_roles=ROLES)


@pytest.mark.parametrize("name", ["entity_address_unified", artifact._AUTHORITY])
async def test_address_verification_preserves_native_permission_failure(monkeypatch, name):
    connection = Connection()
    connection.denied_name = name
    schema = _schema_spy(monkeypatch, connection)
    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="native SELECT denied"):
        await artifact.verify_network_address_artifact(connection, _descriptor(), runtime_roles=ROLES)
    schema.assert_not_awaited()


@pytest.mark.parametrize("damage", ["transaction", "roles", "digest", "generation", "shape_count", "shape_name"])
async def test_address_verification_keeps_descriptor_and_transaction_refusals(monkeypatch, damage):
    descriptor, connection = _descriptor(), Connection()
    _schema_spy(monkeypatch, connection)
    if damage == "transaction":
        connection.active = False
    if damage == "roles":
        descriptor["runtime_roles"] = ["reader", "reader"]
    if damage == "digest":
        descriptor["descriptor_sha256"] = "0" * 64
    if damage == "generation":
        descriptor["generation_id"] = "c" * 64
    if damage == "shape_count":
        descriptor["catalog"]["address"]["shapes"] *= 2
    if damage == "shape_name":
        descriptor["catalog"]["address"]["shapes"][0][0] = "other_table"
    if damage != "digest":
        descriptor["descriptor_sha256"] = artifact._digest(
            {key: field for key, field in descriptor.items() if key != "descriptor_sha256"}
        )
    with pytest.raises(artifact.NetworkAddressArtifactError):
        await artifact.verify_network_address_artifact(connection, descriptor, runtime_roles=ROLES)


async def test_address_verification_preserves_historical_metadata_after_copy(monkeypatch):
    descriptor, connection = _descriptor(), Connection()
    before = deepcopy(descriptor)
    connection.physical = {
        oid + 100: {
            **row,
            "oid": oid + 100,
            "relfilenode": 900,
            "owner_oid": 901,
            "schema_oid": 902,
            "schema_owner_oid": 903,
        }
        for oid, row in connection.physical.items()
    }
    schema = _schema_spy(monkeypatch, connection)
    relation = await artifact.verify_network_address_artifact(connection, descriptor, runtime_roles=("target_reader",))
    assert relation == CompactAddressRelation(SCHEMA, "entity_address_unified", 141)
    assert [call.args[1] for call in schema.await_args_list] == [141, 180]
    assert descriptor == before


@pytest.mark.parametrize("field", ["origin_lineage_id", "origin_generation", "published_at", "absent"])
async def test_live_fallback_requires_saved_logical_generation(monkeypatch, field):
    connection = Connection()
    schema = _schema_spy(monkeypatch, connection)
    if field == "absent":
        connection.origin_by_field = None
    else:
        connection.origin_by_field[field] = {
            "origin_lineage_id": str(UUID(int=99)),
            "origin_generation": 99,
            "published_at": "2026-02-01T00:00:00Z",
        }[field]
    with pytest.raises(artifact.NetworkAddressArtifactError, match="source_generation_changed"):
        await artifact.verify_network_address_artifact(connection, _descriptor(), runtime_roles=ROLES)
    schema.assert_not_awaited()


@pytest.mark.parametrize(
    "damage",
    [None, "missing", "extra", "duplicate_name", "duplicate_position", "zero", "boolean", "incomplete", "reordered"],
)
def test_legacy_shape_positions_require_exact_named_bijection(damage):
    columns = [{"attname": "first", "attnum": 1}, {"attname": "second", "attnum": 2}]
    recorded_columns = [{"attname": "first", "attnum": 1}, {"attname": "second", "attnum": 3}]
    if damage == "missing":
        columns.pop()
    if damage == "extra":
        columns.append({"attname": "third", "attnum": 3})
    if damage == "duplicate_name":
        recorded_columns[1]["attname"] = "first"
    if damage in {"duplicate_position", "zero", "boolean"}:
        recorded_columns[1]["attnum"] = {"duplicate_position": 1, "zero": 0, "boolean": True}[damage]
    if damage == "incomplete":
        recorded_columns[1].pop("attnum")
    if damage == "reordered":
        columns[0]["attnum"], columns[1]["attnum"] = 2, 1
    if damage is None:
        assert artifact._legacy_address_positions(columns, recorded_columns) == {0: 0, 1: 1, 2: 3}
    else:
        with pytest.raises(artifact.NetworkAddressArtifactError, match="structural_catalog_changed"):
            artifact._legacy_address_positions(columns, recorded_columns)


@pytest.mark.parametrize("damage", ["constraint", "index", "attribute", "reference"])
async def test_copied_shape_refuses_unknown_or_foreign_positions(monkeypatch, damage):
    columns = [{"attnum": 1, "attname": "first"}]
    constraint_by_field = {"contype": "p", "key_columns": "{1}", "referenced_table": None, "referenced_columns": None}
    index_by_field = {"keys": "1", "key_attributes": [{"attribute_number": 1}]}
    if damage == "constraint":
        constraint_by_field["key_columns"] = "{9}"
    elif damage == "index":
        index_by_field["keys"] = "9"
    elif damage == "attribute":
        index_by_field["key_attributes"][0]["attribute_number"] = 9
    else:
        constraint_by_field["referenced_table"], constraint_by_field["referenced_columns"] = "other_table", "{1}"
    monkeypatch.setattr(receipt, "_catalog_columns", AsyncMock(return_value=columns))
    monkeypatch.setattr(receipt, "_catalog_constraints", AsyncMock(return_value=[constraint_by_field]))
    monkeypatch.setattr(receipt, "_catalog_indexes", AsyncMock(return_value=[index_by_field]))
    with pytest.raises(artifact.NetworkAddressArtifactError, match="structural_catalog_changed"):
        await artifact._copied_address_shape(
            object(), {"columns": deepcopy(columns)}, {"oid": 41, "schema_name": SCHEMA}, "entity_address_unified"
        )


def _candidate():
    descriptor = _descriptor()
    candidate_by_name = {
        field: str(UUID(int=number)) for number, field in enumerate(("dataset_id", "schema_id", "producer_id"), 1)
    }
    candidate_by_name.update(
        candidate_id=OWN, schema_name="network_candidate_" + UUID(OWN).hex, state="published", source_generations={}
    )
    expected_by_field = {
        "candidate_id": RETAINED,
        "schema_name": "network_candidate_" + UUID(RETAINED).hex,
        "table_oid": 53,
        "seal_sha256": "c" * 64,
    }
    candidate_by_name["validation_json"] = {
        "address_source": asdict(PinnedAddressSource(SCHEMA, "entity_address_unified", descriptor["generation_id"])),
        "serving_readiness": {
            "address_artifact": descriptor,
            "edge_artifacts": [
                {
                    "candidate_id": OWN,
                    "schema_name": candidate_by_name["schema_name"],
                    "table_oid": 52,
                    "seal_sha256": None,
                },
                expected_by_field,
            ],
        },
        "candidate_readiness": {"ref_map_oid": 51, "edge_heap_oid": 52},
        "writer_closure": {
            "loader_roles": ["loader"],
            "reader_roles": ["reader"],
            "relation_oids": {
                "network_slice_ref": 51,
                "network_office_edge": 52,
                "entity_address_unified": 54,
                "provider_location_binding": 55,
            },
        },
    }
    retained_by_field = {**candidate_by_name, "candidate_id": RETAINED, "schema_name": expected_by_field["schema_name"]}
    return candidate_by_name, retained_by_field, expected_by_field


def _edge_spies(monkeypatch, connection, expected_by_field):
    seal_by_field = {
        "seal_sha256": expected_by_field["seal_sha256"],
        "receipt": {
            "revision": 4,
            "closure": {"relation_oids": {"network_office_edge": expected_by_field["table_oid"]}},
        },
    }

    async def verified(actual, target, *, control_schema):
        assert actual is connection and control_schema == CONTROL
        assert target.candidate_id == RETAINED and target.schema_name == expected_by_field["schema_name"]
        return seal_by_field

    writer = AsyncMock(side_effect=verified)
    monkeypatch.setattr(custody, "verify_network_candidate_custody", writer)
    return writer, seal_by_field


async def test_compact_dependencies_reach_address_and_retained_seal_verifiers(monkeypatch):
    candidate_by_name, retained, expected_by_field = _candidate()
    connection = Connection()
    connection.candidate_by_name = retained
    writer, _ = _edge_spies(monkeypatch, connection, expected_by_field)
    address = AsyncMock(return_value=CompactAddressRelation(SCHEMA, "entity_address_unified", 41))
    monkeypatch.setattr(artifact, "verify_network_address_artifact", address)
    layout = await dependencies.verify_compact_dependencies(connection, candidate_by_name, control_schema=CONTROL)
    assert layout.ref_map.table_oid == 51 and layout.base.table_oid == 41
    assert [edge.table_oid for edge in layout.artifacts] == [52, 53]
    assert address.await_args.kwargs == {"runtime_roles": ROLES}
    writer.assert_awaited_once()


@pytest.mark.parametrize("damage", ["seal", "oid", "revision", "custody"])
async def test_retained_edge_cannot_bypass_exact_independent_seal(monkeypatch, damage):
    candidate_by_name, retained, expected_by_field = _candidate()
    connection = Connection()
    connection.candidate_by_name = retained
    writer, seal_by_field = _edge_spies(monkeypatch, connection, expected_by_field)
    if damage == "seal":
        seal_by_field["seal_sha256"] = "d" * 64
    if damage == "oid":
        seal_by_field["receipt"]["closure"]["relation_oids"]["network_office_edge"] = 99
    if damage == "revision":
        seal_by_field["receipt"]["revision"] = 3
    if damage == "custody":
        writer.side_effect = ValueError("genuine custody refused")
    with pytest.raises(ValueError):
        await dependencies._verified_layout(
            connection, candidate_by_name, CompactAddressRelation(SCHEMA, "entity_address_unified", 41), CONTROL
        )


@pytest.mark.parametrize("damage", [None, "copied_base", "binding", "carrier", "heap", "base_schema", "own_edge"])
async def test_layout_preserves_custom_carrier_and_owned_edge_checks(monkeypatch, damage):
    candidate_by_name, retained, expected_by_field = _candidate()
    connection = Connection()
    connection.candidate_by_name = retained
    _edge_spies(monkeypatch, connection, expected_by_field)
    report = candidate_by_name["validation_json"]
    shared_by_field = {
        "base_source": PinnedAddressSource(**report["address_source"]),
        "base_artifact_sha256": report["serving_readiness"]["address_artifact"]["descriptor_sha256"],
        "binding_table_oid": 55,
        "runtime_roles": ROLES,
        "address_table_oid": 54,
    }
    custom_receipt = SimpleNamespace(**shared_by_field, custom_pair_count=1)
    carriers = SimpleNamespace(**shared_by_field, materialized_address_rows=1)
    if damage == "binding":
        custom_receipt.binding_table_oid = 99
    if damage == "carrier":
        carriers.base_artifact_sha256 = "d" * 64
    if damage == "heap":
        report["writer_closure"]["relation_oids"]["entity_address_unified"] = 99
    if damage == "own_edge":
        report["serving_readiness"]["edge_artifacts"][0]["seal_sha256"] = "d" * 64
    monkeypatch.setattr(custom, "compact_custom_receipt", lambda candidate_by_name: custom_receipt)
    monkeypatch.setattr(custom, "compact_carrier_receipt", lambda candidate_by_name: carriers)
    base = CompactAddressRelation(
        "other_schema" if damage == "base_schema" else SCHEMA,
        "entity_address_unified",
        141 if damage == "copied_base" else 41,
    )
    if damage in (None, "copied_base"):
        layout = await dependencies._verified_layout(connection, candidate_by_name, base, CONTROL)
        assert layout.base == base
        assert layout.custom == CompactAddressRelation(candidate_by_name["schema_name"], "entity_address_unified", 54)
    else:
        with pytest.raises(ValueError):
            await dependencies._verified_layout(connection, candidate_by_name, base, CONTROL)
