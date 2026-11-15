# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named snapshot reads retain transaction modes and logical candidate scope."""

import copy
import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_address_artifact as artifact
from process import network_compact_custom_preparation as custom
from process import network_compact_dependencies as dependencies
from process.network_address_projection import PinnedAddressSource
from process.network_compact_layout import CompactAddressRelation

SCHEMA = "synthetic_addresses"
CONTROL = "synthetic_control"
OWN = str(UUID(int=10))
RETAINED = str(UUID(int=20))


def _descriptor(*, legacy=False):
    authority_by_field = dict(
        local_lineage_id=str(UUID(int=1)),
        local_generation=1,
        origin_lineage_id=str(UUID(int=2)),
        origin_generation=7,
        published_at="2026-01-01T00:00:00.000000Z",
    )
    descriptor_by_field = dict(
        contract=artifact.CONTRACT,
        revision=2,
        namespace={"name": SCHEMA},
        table={"schema_name": SCHEMA, "table_name": "entity_address_unified"},
        retained_table={"schema_name": SCHEMA, "table_name": "entity_address_unified_retained_29"},
        authority=authority_by_field,
        generation_id=artifact._digest(authority_by_field),
        alias_generation=0,
    )
    if legacy:
        descriptor_by_field.pop("revision")
        descriptor_by_field.pop("retained_table")
        descriptor_by_field.update(
            database={"oid": 11, "name": "synthetic"},
            namespace={"name": SCHEMA, "oid": 12, "owner_oid": 100},
            protected_owner_oid=100,
            table={"oid": 41, "relfilenode": 91},
            authority_table_oid=80,
            alias_catalog_sha256="a" * 64,
            catalog={},
            runtime_roles=["reader"],
        )
        descriptor_by_field["authority"]["relation_oids"] = list(range(1, len(artifact.generation.RELATION_NAMES) + 1))
        # Historical editions keyed the generation by the complete saved authority.
        descriptor_by_field["generation_id"] = artifact._digest(descriptor_by_field["authority"])
    descriptor_by_field["descriptor_sha256"] = artifact._digest(descriptor_by_field)
    return descriptor_by_field


class Connection:
    def __init__(self, *, modes=("repeatable read", "on")):
        self.modes, self.active = modes, True
        self.calls = []
        self.candidate = None
        self.present = {"entity_address_unified", "entity_address_result_generation"}
        self.authority = {**_descriptor()["authority"], "relation_oids": []}
        self.is_stable = True

    def is_in_transaction(self):
        return self.active

    async def fetchrow(self, statement, *arguments):
        self.calls.append((statement, arguments))
        if statement.startswith("SELECT current_setting"):
            return self.modes
        if statement.startswith("SELECT local_lineage_id"):
            return self.authority
        if statement.startswith("SELECT origin_lineage_id,origin_generation,published_at"):
            assert "relation.relname=$2::text AND namespace.nspname=$3::text" in statement
            assert arguments == (f'"{SCHEMA}"."entity_address_unified"', "entity_address_unified", SCHEMA)
            if not self.is_stable or "entity_address_unified" not in self.present:
                return None
            return {
                field: self.authority[field] for field in ("origin_lineage_id", "origin_generation", "published_at")
            }
        if statement.startswith("SELECT to_regclass($1) IS NOT NULL AS authority"):
            names = [argument.split(".")[-1].strip('"') for argument in arguments]
            return dict(zip(("authority", "live", "retained"), (name in self.present for name in names), strict=True))
        assert statement.startswith(f'SELECT * FROM "{CONTROL}".network_membership_candidate')
        return self.candidate

    async def fetchval(self, statement, *arguments):
        self.calls.append((statement, arguments))
        if statement == "SHOW transaction_isolation":
            return self.modes[0]
        if statement.startswith("SELECT to_regclass($1)=(SELECT c.oid"):
            return self.is_stable
        assert statement == "SELECT to_regclass($1) IS NOT NULL"
        return arguments[0].split(".")[-1].strip('"') in self.present

    async def execute(self, statement, *arguments):
        self.calls.append((statement, arguments))
        assert statement.startswith("LOCK TABLE ONLY ") and statement.endswith(" IN ACCESS SHARE MODE NOWAIT")
        return "LOCK TABLE"

    async def fetch(self, statement, *arguments):
        self.calls.append((statement, arguments))
        assert statement.endswith(" LIMIT 0")
        return []


def _candidate(identity=OWN):
    address_source = PinnedAddressSource(SCHEMA, "entity_address_unified", _descriptor()["generation_id"])
    schema = "network_candidate_" + UUID(identity).hex
    scope_by_field = {"candidate_id": identity}
    counts_by_field = dict(membership_rows=1, distinct_memberships=1, projected_locations=1, orphan_bindings=0)
    report_by_field = {
        "address_source": asdict(address_source),
        **counts_by_field,
        "candidate_readiness": dict(
            component="compact_office_slices",
            readiness_revision=1,
            ready=True,
            scope=scope_by_field,
            ref_map={"schema_name": schema, "table_name": "network_slice_ref"},
            edge_heap={"schema_name": schema, "table_name": "network_office_edge"},
            **counts_by_field,
        ),
        "serving_readiness": dict(
            component="compact_network_serving",
            readiness_revision=1,
            ready=True,
            scope=scope_by_field,
            index_profile="compact_membership",
            index_definition_sha256="a" * 64,
            slice_ref_sha256="b" * 64,
            address_artifact=_descriptor(),
            edge_artifacts=[dict(candidate_id=identity, schema_name=schema)],
        ),
    }
    return dict(
        candidate_id=identity, schema_name=schema, state="published", index_ready=True, validation_json=report_by_field
    )


@pytest.fixture(autouse=True)
def _logical_scope(monkeypatch):
    monkeypatch.setattr(
        dependencies, "_scope_report", lambda candidate, source: {"candidate_id": str(candidate["candidate_id"])}
    )


@pytest.mark.parametrize("modes", [("repeatable read", "on"), ("serializable", "on")])
async def test_named_api_address_reads_the_qualified_relation(modes):
    connection = Connection(modes=modes)
    relation = await artifact.verify_network_address_artifact(connection, _descriptor(), api_read=True)
    assert relation == CompactAddressRelation(SCHEMA, "entity_address_unified")
    assert connection.calls[-1][0] == f'SELECT * FROM "{SCHEMA}"."entity_address_unified" LIMIT 0'


async def test_named_read_refuses_a_name_renamed_after_its_snapshot_began():
    connection = Connection()
    connection.is_stable = False
    with pytest.raises(ValueError, match="retained_table_unavailable"):
        await artifact.verify_network_address_artifact(connection, _descriptor(), api_read=True)


@pytest.mark.parametrize(
    "present,is_current,expected",
    [
        ({"entity_address_unified"}, True, "entity_address_unified"),
        ({"entity_address_unified", "entity_address_unified_retained_29"}, False, "entity_address_unified_retained_29"),
        ({"entity_address_unified"}, False, None),
        ({"entity_address_unified", "entity_address_unified_retained_29"}, True, None),
    ],
)
async def test_named_edition_reads_only_its_own_generation(present, is_current, expected):
    connection = Connection()
    connection.present = present | {"entity_address_result_generation"}
    if not is_current:
        connection.authority = {**connection.authority, "origin_generation": 8}
    if expected is None:
        with pytest.raises(ValueError, match="retained_table_unavailable"):
            await artifact.verify_network_address_artifact(connection, _descriptor(), api_read=True)
        return
    relation = await artifact.verify_network_address_artifact(connection, _descriptor(), api_read=True)
    assert relation == CompactAddressRelation(SCHEMA, expected)


@pytest.mark.parametrize(
    "present,expected",
    [
        ({"entity_address_unified"}, "entity_address_unified"),
        ({"entity_address_unified_retained_29"}, "entity_address_unified_retained_29"),
    ],
)
async def test_copied_legacy_metadata_resolves_saved_historical_names(present, expected):
    saved = _descriptor(legacy=True)
    connection = Connection()
    connection.present = present | {"entity_address_result_generation"}
    connection.database_oid = 98765
    connection.owner_oid = 87654
    connection.authority = {**connection.authority, "local_generation": 2}
    relation = await artifact.verify_network_address_artifact(connection, saved, api_read=True)
    assert relation == CompactAddressRelation(SCHEMA, expected)
    projected = artifact._descriptor(saved)
    assert projected["generation_id"] == saved["generation_id"]
    assert projected["descriptor_sha256"] == saved["descriptor_sha256"]


@pytest.mark.parametrize(
    "retained_present,is_current,expected",
    [
        (False, True, None),
        (True, False, "entity_address_unified_retained_29"),
        (False, False, ValueError),
        (True, True, ValueError),
    ],
)
async def test_api_pin_reads_only_the_generation_row_after_its_own_presence(retained_present, is_current, expected):
    """The combined pin result needs no additional generation statement."""
    from process import network_published_api_pin as pin

    candidate_by_field = {"validation_json": {"serving_readiness": {"address_artifact": _descriptor()}}}
    presence_by_name = {"authority": True, "current": is_current, "live": True, "retained": retained_present}
    entry_by_field = {"candidate_json": json.dumps(candidate_by_field), "address_heap": json.dumps(presence_by_name)}
    connection = Connection()
    if not is_current:
        connection.authority = {**connection.authority, "origin_generation": 8}
    if expected is ValueError:
        with pytest.raises(ValueError, match="retained_table_unavailable"):
            await pin._rotated_base_name(connection, entry_by_field)
    else:
        assert await pin._rotated_base_name(connection, entry_by_field) == expected
    assert connection.calls == []


async def test_historical_capture_key_binds_its_saved_authority():
    saved_by_field = {key: value for key, value in _descriptor().items() if key != "descriptor_sha256"}
    legacy_by_field = {**saved_by_field["authority"], "relation_oids": [41, 42]}
    saved_by_field.update(legacy_authority=legacy_by_field, generation_id=artifact._digest(legacy_by_field))
    saved_by_field["descriptor_sha256"] = artifact._digest(saved_by_field)
    assert artifact._descriptor(saved_by_field)["legacy_authority"] == legacy_by_field
    tampered_by_field = {**saved_by_field, "authority": {**saved_by_field["authority"], "origin_generation": 8}}
    tampered_by_field["descriptor_sha256"] = artifact._digest(
        {key: value for key, value in tampered_by_field.items() if key != "descriptor_sha256"}
    )
    with pytest.raises(ValueError, match="generation_invalid"):
        artifact._descriptor(tampered_by_field)


def test_historical_key_must_name_its_saved_authority():
    saved = _descriptor(legacy=True)
    saved["authority"]["origin_generation"] += 1
    saved["descriptor_sha256"] = artifact._digest(
        {key: value for key, value in saved.items() if key != "descriptor_sha256"}
    )
    with pytest.raises(ValueError, match="generation_invalid"):
        artifact._descriptor(saved)


@pytest.mark.parametrize("present", [set(), {"entity_address_unified", "entity_address_unified_retained_29"}])
async def test_legacy_name_resolution_refuses_missing_or_ambiguous_names(present):
    connection = Connection()
    connection.present = present
    with pytest.raises(ValueError, match="retained_table_unavailable"):
        await artifact.verify_network_address_artifact(connection, _descriptor(legacy=True), api_read=True)


async def _entry(entry, connection, *, api_read):
    candidate = _candidate()
    if entry == "address":
        return await artifact.verify_network_address_artifact(connection, _descriptor(), api_read=api_read)
    if entry == "dependencies":
        return await dependencies.verify_compact_dependencies(
            connection, candidate, control_schema=CONTROL, api_read=api_read
        )
    if entry == "layout":
        return await dependencies._verified_layout(
            connection, candidate, CompactAddressRelation(SCHEMA, "entity_address_unified"), CONTROL, api_read=api_read
        )
    return await dependencies._retained_edge(connection, RETAINED, CONTROL, api_read=api_read)


@pytest.mark.parametrize("entry", ["address", "dependencies", "layout", "edge"])
@pytest.mark.parametrize("modes", [("repeatable read", "off"), ("read committed", "on"), ("serializable", "off")])
async def test_api_entrypoints_require_read_only_repeatable_context(entry, modes):
    connection = Connection(modes=modes)
    with pytest.raises(ValueError, match="read_snapshot_required"):
        await _entry(entry, connection, api_read=True)
    assert len(connection.calls) == 1


@pytest.mark.parametrize("entry", ["address", "dependencies", "layout", "edge"])
@pytest.mark.parametrize("invalid", [0, 1, None, "true"])
async def test_read_mode_requires_a_boolean(entry, invalid):
    connection = Connection()
    with pytest.raises(ValueError, match="read_mode_invalid"):
        await _entry(entry, connection, api_read=invalid)
    assert not connection.calls


@pytest.mark.parametrize("entry", ["address", "dependencies", "layout", "edge"])
async def test_api_entrypoints_require_an_active_transaction(entry):
    connection = Connection()
    connection.active = False
    with pytest.raises(ValueError, match="transaction_required"):
        await _entry(entry, connection, api_read=True)
    assert not connection.calls


@pytest.mark.parametrize("damage", [None, "owned_schema", "retained_schema", "retained_state", "retained_readiness"])
async def test_layout_binds_owned_and_retained_logical_candidates(damage):
    candidate, retained = _candidate(), _candidate(RETAINED)
    edges = candidate["validation_json"]["serving_readiness"]["edge_artifacts"]
    edges.append(dict(candidate_id=RETAINED, schema_name=retained["schema_name"], table_oid=123, seal_sha256="c" * 64))
    connection = Connection()
    connection.candidate = retained
    if damage == "owned_schema":
        edges[0]["schema_name"] = "foreign"
    elif damage == "retained_schema":
        edges[1]["schema_name"] = "foreign"
    elif damage == "retained_state":
        retained["state"] = "open"
    elif damage == "retained_readiness":
        retained["validation_json"]["candidate_readiness"]["ready"] = False
    if damage:
        with pytest.raises(ValueError):
            await dependencies.verify_compact_dependencies(connection, candidate, control_schema=CONTROL, api_read=True)
    else:
        layout = await dependencies.verify_compact_dependencies(
            connection, candidate, control_schema=CONTROL, api_read=True
        )
        assert layout.ref_map == CompactAddressRelation(candidate["schema_name"], "network_slice_ref")
        assert [edge.candidate_id for edge in layout.artifacts] == [OWN, RETAINED]


@pytest.mark.parametrize("damage", [None, "custom_source", "carrier_source"])
async def test_layout_keeps_custom_and_carrier_address_scope(monkeypatch, damage):
    candidate = _candidate()
    source = PinnedAddressSource(**candidate["validation_json"]["address_source"])
    direct = SimpleNamespace(base_source=source, custom_pair_count=1)
    carrier = SimpleNamespace(base_source=source, materialized_address_rows=1)
    if damage:
        wrong = PinnedAddressSource("foreign", "entity_address_unified", source.generation_id)
        (direct if damage == "custom_source" else carrier).base_source = wrong
    monkeypatch.setattr(custom, "compact_custom_receipt", lambda _: direct)
    monkeypatch.setattr(custom, "compact_carrier_receipt", lambda _: carrier)
    before = copy.deepcopy(candidate)
    if damage:
        with pytest.raises(ValueError):
            await dependencies._verified_layout(
                Connection(),
                candidate,
                CompactAddressRelation(SCHEMA, "entity_address_unified"),
                CONTROL,
                api_read=True,
            )
    else:
        layout = await dependencies._verified_layout(
            Connection(), candidate, CompactAddressRelation(SCHEMA, "entity_address_unified"), CONTROL, api_read=True
        )
        assert layout.custom == CompactAddressRelation(candidate["schema_name"], "entity_address_unified")
    assert candidate == before


@pytest.mark.parametrize("index_valid", [False, True])
async def test_npi_capture_requires_logical_generation_and_unique_key_index(index_valid):
    from datetime import datetime, timezone

    from process import network_npi_artifact as npi_artifact

    publication_by_field = dict(
        singleton=True,
        local_lineage_id=UUID(int=1),
        local_generation=3,
        origin_lineage_id=UUID(int=2),
        origin_generation=4,
        published_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        relation_oids=[111, 222],
        canonical_publication_ref=None,
        canonical_publication_generation=None,
        canonical_chain_ref=None,
        canonical_import_date=None,
    )

    async def value(statement, *arguments):
        return "repeatable read" if statement == "SHOW transaction_isolation" else index_valid

    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value=publication_by_field),
        fetchval=AsyncMock(side_effect=value),
        execute=AsyncMock(),
    )
    if not index_valid:
        with pytest.raises(ValueError, match="indexed_npi_required"):
            await npi_artifact.capture_network_npi_artifact(connection, SCHEMA)
    else:
        pinned, reference = await npi_artifact.capture_network_npi_artifact(connection, SCHEMA)
        assert reference == asdict(pinned) and pinned.table_name == "npi"
        probes = [call.args for call in connection.execute.await_args_list]
        assert probes and all(len(probe) == 1 and probe[0].endswith(" LIMIT 0") for probe in probes)
        assert await npi_artifact.verify_network_npi_artifact(connection, pinned, reference) == reference
        with pytest.raises(ValueError, match="source_changed"):
            await npi_artifact.capture_network_npi_artifact(connection, SCHEMA, generation_id="wrong-edition")


@pytest.mark.parametrize("failure", ["unpublished", "missing_authority"])
async def test_npi_capture_wraps_a_missing_native_proof(failure):
    import asyncpg

    from process import network_npi_artifact as npi_artifact

    publication_by_field = dict(
        singleton=True,
        local_lineage_id=UUID(int=1),
        local_generation=3,
        origin_lineage_id=None,
        origin_generation=None,
        published_at=None,
        canonical_publication_ref=None,
        canonical_publication_generation=None,
        canonical_chain_ref=None,
        canonical_import_date=None,
    )
    read = (
        AsyncMock(return_value=publication_by_field)
        if failure == "unpublished"
        else AsyncMock(side_effect=asyncpg.UndefinedTableError("relation does not exist"))
    )
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=read,
        fetchval=AsyncMock(return_value="repeatable read"),
        execute=AsyncMock(),
    )
    with pytest.raises(npi_artifact.NetworkNPIArtifactError, match="native_proof_unavailable"):
        await npi_artifact.capture_network_npi_artifact(connection, SCHEMA)
    connection.execute.assert_not_awaited()
