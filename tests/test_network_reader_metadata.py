# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Metadata-only API verification; mocks do not establish physical consumption authority."""

import copy
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

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
        "catalog": {"address": {}, "authority": {}},
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
    def __init__(self, *, retained=False, modes=("repeatable read", "on")):
        self.modes, self.active, self.calls = modes, True, []
        name = "entity_address_unified_retained_29" if retained else "entity_address_unified"
        self.physical = {41: _physical(41, name), 80: _physical(80, artifact._AUTHORITY)}
        self.owner = {
            "oid": 100,
            **dict.fromkeys(
                ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls"), False
            ),
        }
        self.database, self.runtime_safe, self.namespace_unsafe = {"oid": 11, "name": "synthetic"}, True, False
        self.candidate_by_name = None

    def is_in_transaction(self):
        return self.active

    async def fetchrow(self, statement, *arguments):
        self.calls.append((statement, arguments))
        if statement.startswith("SELECT current_setting('transaction_isolation')"):
            return self.modes
        if statement.startswith("SELECT oid::bigint AS oid,datname"):
            return self.database
        if statement.startswith("SELECT c.oid::bigint,c.relname"):
            return self.physical.get(arguments[0])
        if statement.startswith("SELECT r.oid::bigint"):
            return self.owner
        assert statement.startswith('SELECT * FROM "synthetic_control".network_membership_candidate')
        assert arguments == (UUID(RETAINED),)
        return self.candidate_by_name

    async def fetchval(self, statement, *arguments):
        self.calls.append((statement, arguments))
        if statement.startswith("SELECT to_regclass"):
            return 41
        if statement.startswith("SELECT count(*)=cardinality"):
            return self.runtime_safe
        if statement.startswith("SELECT EXISTS(SELECT FROM pg_roles principal"):
            return self.namespace_unsafe
        assert statement == "SELECT current_user"
        return "api_user"

    async def execute(self, statement, *arguments):
        self.calls.append((statement, arguments))
        assert statement.startswith("LOCK TABLE ONLY ")
        return "LOCK TABLE"


def _catalog_spies(monkeypatch, descriptor):
    mutation, subjects = AsyncMock(), AsyncMock()
    catalog = AsyncMock(return_value=descriptor["catalog"])
    alias = AsyncMock(return_value=descriptor["alias_catalog_sha256"])
    monkeypatch.setattr(artifact.protected, "_require_no_untrusted_mutation", mutation)
    monkeypatch.setattr(artifact, "require_native_read_catalog_subjects", subjects)
    monkeypatch.setattr(artifact, "_catalog", catalog)
    monkeypatch.setattr(artifact.protected, "_require_alias_authority", alias)
    return mutation, subjects, catalog, alias


@pytest.mark.parametrize("retained", [False, True])
@pytest.mark.parametrize("modes", [("repeatable read", "on"), ("serializable", "on")])
async def test_api_address_metadata_preserves_custody_checks_without_target_locks(monkeypatch, retained, modes):
    descriptor = _descriptor()
    connection = Connection(retained=retained, modes=modes)
    mutation, subjects, catalog, alias = _catalog_spies(monkeypatch, descriptor)
    relation = await artifact.verify_network_address_artifact(
        connection, descriptor, runtime_roles=ROLES, api_read=True
    )
    assert relation == CompactAddressRelation(SCHEMA, connection.physical[41]["relname"], 41)
    mutation.assert_awaited_once()
    assert mutation.await_args.args[1:] == ([41, 80], 100)
    subjects.assert_awaited_once()
    assert subjects.await_args.args[1:] == ([41, 80], ("api_user", *ROLES))
    catalog.assert_awaited_once()
    alias.assert_awaited_once()
    statements = [sql for sql, arguments in connection.calls]
    assert all(sql.startswith("SELECT ") for sql in statements)
    assert not any("advisory" in sql.lower() or "to_regclass" in sql for sql in statements)
    assert [
        arguments[0] for sql, arguments in connection.calls if sql.startswith("SELECT c.oid::bigint,c.relname")
    ] == [41, 80, 80]


@pytest.mark.parametrize("explicit", [False, True])
async def test_default_writer_address_verification_keeps_both_explicit_locks(monkeypatch, explicit):
    descriptor = _descriptor()
    connection = Connection(modes=("read committed", "off"))
    _catalog_spies(monkeypatch, descriptor)
    options = {"api_read": False} if explicit else {}
    assert (
        await artifact.verify_network_address_artifact(connection, descriptor, runtime_roles=ROLES, **options)
    ).table_oid == 41
    statements = [sql for sql, arguments in connection.calls]
    assert [sql for sql in statements if sql.startswith("LOCK TABLE")] == [
        f'LOCK TABLE ONLY "{SCHEMA}"."entity_address_unified" IN ACCESS SHARE MODE NOWAIT',
        f'LOCK TABLE ONLY "{SCHEMA}"."entity_address_result_generation" IN ACCESS SHARE MODE NOWAIT',
    ]
    assert not any("transaction_read_only" in sql for sql in statements)
    assert any("to_regclass" in sql for sql in statements)


@pytest.mark.parametrize(
    "damage",
    [
        "absent",
        "schema",
        "name",
        "filenode",
        "namespace_oid",
        "schema_owner",
        "owner_role",
        "owner",
        "authority_missing",
        "authority_schema",
        "authority_name",
        "heap_kind",
        "temporary",
        "rls",
        "rules",
        "inherited",
        "runtime",
        "namespace_privileges",
        "mutation",
        "subjects",
        "catalog",
        "alias",
        "database",
        "roles",
        "digest",
    ],
)
async def test_api_address_metadata_keeps_every_existing_refusal(monkeypatch, damage):
    descriptor, connection = _descriptor(), Connection()
    mutation, subjects, catalog, alias = _catalog_spies(monkeypatch, descriptor)
    changes_by_fault = {
        "schema": ("schema_name", "other_schema"),
        "name": ("relname", "other_table"),
        "filenode": ("relfilenode", 999),
        "namespace_oid": ("schema_oid", 999),
        "schema_owner": ("schema_owner_oid", 999),
        "owner": ("owner_oid", 999),
        "heap_kind": ("relkind", "v"),
        "temporary": ("relpersistence", "t"),
        "rls": ("relrowsecurity", True),
        "rules": ("rules", True),
        "inherited": ("inherited", True),
    }
    if damage in changes_by_fault:
        field, changed = changes_by_fault[damage]
        connection.physical[41][field] = changed
    if damage == "absent":
        connection.physical.pop(41)
    if damage == "authority_missing":
        connection.physical.pop(80)
    if damage == "authority_schema":
        connection.physical[80]["schema_oid"] = 999
    if damage == "authority_name":
        connection.physical[80]["relname"] = "other_authority"
    if damage == "owner_role":
        connection.owner["rolsuper"] = True
    if damage == "runtime":
        connection.runtime_safe = False
    if damage == "namespace_privileges":
        connection.namespace_unsafe = True
    if damage == "mutation":
        mutation.side_effect = RuntimeError("ordinary mutation available")
    if damage == "subjects":
        subjects.side_effect = RuntimeError("unsafe executable catalog")
    if damage == "catalog":
        catalog.return_value = {}
    if damage == "alias":
        alias.return_value = "b" * 64
    if damage == "database":
        connection.database = {"oid": 99, "name": "other"}
    if damage == "roles":
        descriptor["runtime_roles"] = ["other_reader"]
    if damage == "digest":
        descriptor["descriptor_sha256"] = "0" * 64
    if damage == "roles":
        descriptor["descriptor_sha256"] = artifact._digest(
            {key: field for key, field in descriptor.items() if key != "descriptor_sha256"}
        )
    with pytest.raises((ValueError, RuntimeError)):
        await artifact.verify_network_address_artifact(connection, descriptor, runtime_roles=ROLES, api_read=True)
    assert not any(sql.startswith("LOCK TABLE") for sql, arguments in connection.calls)


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

    metadata, writer = AsyncMock(side_effect=verified), AsyncMock(side_effect=verified)
    monkeypatch.setattr(custody, "verify_network_candidate_read_metadata", metadata)
    monkeypatch.setattr(custody, "verify_network_candidate_custody", writer)
    return metadata, writer, seal_by_field


@pytest.mark.parametrize("api_read", [False, True])
async def test_compact_dependency_flag_reaches_address_and_retained_seal_verifiers(monkeypatch, api_read):
    candidate_by_name, retained, expected_by_field = _candidate()
    connection = Connection()
    connection.candidate_by_name = retained
    metadata, writer, _ = _edge_spies(monkeypatch, connection, expected_by_field)
    address = AsyncMock(return_value=CompactAddressRelation(SCHEMA, "entity_address_unified", 41))
    monkeypatch.setattr(artifact, "verify_network_address_artifact", address)
    layout = await dependencies.verify_compact_dependencies(
        connection, candidate_by_name, control_schema=CONTROL, **({"api_read": True} if api_read else {})
    )
    assert layout.ref_map.table_oid == 51 and layout.base.table_oid == 41
    assert [edge.table_oid for edge in layout.artifacts] == [52, 53]
    assert address.await_args.kwargs == {"runtime_roles": ROLES, **({"api_read": True} if api_read else {})}
    assert metadata.await_count == int(api_read) and writer.await_count == int(not api_read)


@pytest.mark.parametrize("damage", ["seal", "oid", "revision", "custody"])
async def test_api_retained_edge_cannot_bypass_exact_independent_seal(monkeypatch, damage):
    candidate_by_name, retained, expected_by_field = _candidate()
    connection = Connection()
    connection.candidate_by_name = retained
    metadata, writer, seal_by_field = _edge_spies(monkeypatch, connection, expected_by_field)
    if damage == "seal":
        seal_by_field["seal_sha256"] = "d" * 64
    if damage == "oid":
        seal_by_field["receipt"]["closure"]["relation_oids"]["network_office_edge"] = 99
    if damage == "revision":
        seal_by_field["receipt"]["revision"] = 3
    if damage == "custody":
        metadata.side_effect = ValueError("genuine metadata custody refused")
    with pytest.raises(ValueError):
        await dependencies._verified_layout(
            connection,
            candidate_by_name,
            CompactAddressRelation(SCHEMA, "entity_address_unified", 41),
            CONTROL,
            api_read=True,
        )
    writer.assert_not_awaited()


@pytest.mark.parametrize("entry", ["address", "dependencies", "layout", "edge"])
@pytest.mark.parametrize("modes", [("repeatable read", "off"), ("read committed", "on"), ("serializable", "off")])
async def test_api_entrypoints_require_genuine_read_only_repeatable_context(monkeypatch, entry, modes):
    candidate_by_name, retained, expected_by_field = _candidate()
    connection = Connection(modes=modes)
    connection.candidate_by_name = retained
    metadata, writer, _ = _edge_spies(monkeypatch, connection, expected_by_field)
    with pytest.raises(ValueError, match="read_snapshot_required"):
        if entry == "address":
            await artifact.verify_network_address_artifact(
                connection, _descriptor(), runtime_roles=ROLES, api_read=True
            )
        if entry == "dependencies":
            await dependencies.verify_compact_dependencies(
                connection, candidate_by_name, control_schema=CONTROL, api_read=True
            )
        if entry == "layout":
            await dependencies._verified_layout(
                connection,
                candidate_by_name,
                CompactAddressRelation(SCHEMA, "entity_address_unified", 41),
                CONTROL,
                api_read=True,
            )
        if entry == "edge":
            await dependencies._retained_edge(connection, RETAINED, CONTROL, api_read=True)
    assert len(connection.calls) == 1
    metadata.assert_not_awaited()
    writer.assert_not_awaited()


@pytest.mark.parametrize("entry", ["address", "dependencies", "layout", "edge"])
@pytest.mark.parametrize("invalid", [0, 1, None, "true"])
async def test_read_mode_never_accepts_bool_like_markers(entry, invalid):
    candidate_by_name, _, _ = _candidate()
    connection = Connection()
    with pytest.raises(ValueError, match="read_mode_invalid"):
        if entry == "address":
            await artifact.verify_network_address_artifact(
                connection, _descriptor(), runtime_roles=ROLES, api_read=invalid
            )
        if entry == "dependencies":
            await dependencies.verify_compact_dependencies(
                connection, candidate_by_name, control_schema=CONTROL, api_read=invalid
            )
        if entry == "layout":
            await dependencies._verified_layout(
                connection,
                candidate_by_name,
                CompactAddressRelation(SCHEMA, "entity_address_unified", 41),
                CONTROL,
                api_read=invalid,
            )
        if entry == "edge":
            await dependencies._retained_edge(connection, RETAINED, CONTROL, api_read=invalid)
    assert not connection.calls


@pytest.mark.parametrize("damage", [None, "binding", "carrier", "heap", "base_oid", "own_edge"])
async def test_api_layout_preserves_custom_carrier_and_owned_edge_checks(monkeypatch, damage):
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
    if damage == "base_oid":
        report["serving_readiness"]["address_artifact"]["table"]["oid"] = 99
    if damage == "own_edge":
        report["serving_readiness"]["edge_artifacts"][0]["seal_sha256"] = "d" * 64
    monkeypatch.setattr(custom, "compact_custom_receipt", lambda candidate_by_name: custom_receipt)
    monkeypatch.setattr(custom, "compact_carrier_receipt", lambda candidate_by_name: carriers)
    base = CompactAddressRelation(SCHEMA, "entity_address_unified", 41)
    if damage is None:
        layout = await dependencies._verified_layout(connection, candidate_by_name, base, CONTROL, api_read=True)
        assert layout.custom == CompactAddressRelation(candidate_by_name["schema_name"], "entity_address_unified", 54)
    else:
        with pytest.raises(ValueError):
            await dependencies._verified_layout(connection, candidate_by_name, base, CONTROL, api_read=True)


@pytest.mark.parametrize("entry", ["address", "dependencies", "layout", "edge"])
async def test_api_metadata_cannot_use_a_transaction_marker_without_native_transaction(entry):
    candidate_by_name, _, _ = _candidate()
    connection = Connection()
    connection.active = False
    with pytest.raises(ValueError, match="transaction_required"):
        if entry == "address":
            await artifact.verify_network_address_artifact(
                connection, _descriptor(), runtime_roles=ROLES, api_read=True
            )
        if entry == "dependencies":
            await dependencies.verify_compact_dependencies(
                connection, candidate_by_name, control_schema=CONTROL, api_read=True
            )
        if entry == "layout":
            await dependencies._verified_layout(
                connection,
                candidate_by_name,
                CompactAddressRelation(SCHEMA, "entity_address_unified", 41),
                CONTROL,
                api_read=True,
            )
        if entry == "edge":
            await dependencies._retained_edge(connection, RETAINED, CONTROL, api_read=True)
    assert not connection.calls
