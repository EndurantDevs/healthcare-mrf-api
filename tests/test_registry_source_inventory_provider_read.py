# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure provider-read wiring checks; resolved DTO spies establish no native custody."""

import copy
import json
import re
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from api import network_provider_only_read as readers
from api.network_address_scope import NetworkAddressReadScope
from api.network_provider_read import NetworkProviderReadError, NetworkProviderReadUnavailable
from process import network_candidate_custody as custody
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_compact_layout import CompactAddressRelation
from process.network_provider_only_projection import provider_only_page_sql
from process.network_read_identity import NetworkReadIdentityError
from process.network_serving_read import NetworkServingReadUnavailable, PinnedNetworkServingManifest
from process.registry_source_contribution_store import (
    SUMMARY_KEY,
    locally_owned_source_descriptor,
    registry_source_contribution_sha256,
)
from process.registry_source_inventory_contract import make_source_inventory_entry, source_inventory_root
from process.registry_source_inventory_reference import INVENTORY_DIGEST_KEY, SourceInventoryReference
from process.registry_source_inventory_summaries import contribution_summary_v3
from process.registry_source_recipe_store import RECIPE_DIGEST_KEY, canonical_registry_source_recipes
from tests.test_registry_source_contributions import _descriptor
from tests.test_registry_source_recipe_store_postgres import _example

CONTROL = "synthetic_control"
APPROVED = ApprovedMembershipSource(3, "b" * 64, 0)
_PID = 77
_CANDIDATE_RELATION = CompactAddressRelation(CONTROL, "network_membership_candidate", 11)
_APPROVED_RELATION = CompactAddressRelation(CONTROL, "registry_approved_record", 13)


def _fixture(count=1, *, legacy=False):
    """Build exact synthetic inventory and manifest inputs for isolated provider-read predicates."""
    recipes = tuple(
        sorted(
            (_example(index) for index in range(count)), key=lambda recipe: canonical_registry_source_recipes((recipe,))
        )
    )
    source_descriptors = tuple(locally_owned_source_descriptor(_descriptor(recipe)) for recipe in recipes)
    root = source_inventory_root(
        make_source_inventory_entry(index, recipe, contribution=source_descriptor)
        for index, (recipe, source_descriptor) in enumerate(zip(recipes, source_descriptors), 1)
    )
    generations_by_name = {INVENTORY_DIGEST_KEY: root.entries_sha256, "custom_membership": APPROVED.generation_id}
    validation_by_field = {}
    if count:
        generations_by_name[RECIPE_DIGEST_KEY] = root.recipes_sha256
        summary_by_field = contribution_summary_v3(
            root, sorted(source_descriptors, key=lambda source_descriptor: source_descriptor["recipe_sha256"])
        )
        generations_by_name[SUMMARY_KEY] = registry_source_contribution_sha256(summary_by_field, inventory_root=root)
        validation_by_field[SUMMARY_KEY] = summary_by_field
    candidate_by_field = {
        "source_recipes_json": SourceInventoryReference(root).as_dict(),
        "source_generations": generations_by_name,
        "validation_json": validation_by_field,
    }
    if legacy:
        candidate_by_field["source_recipes_json"] = json.loads(canonical_registry_source_recipes(recipes))
        generations_by_name.pop(INVENTORY_DIGEST_KEY)
        summary_by_field = {
            "component": SUMMARY_KEY,
            "revision": 2,
            "table_name": "network_source_contribution",
            "sources": sorted(source_descriptors, key=lambda source_descriptor: source_descriptor["recipe_sha256"]),
        }
        generations_by_name[SUMMARY_KEY] = registry_source_contribution_sha256(summary_by_field)
        validation_by_field[SUMMARY_KEY] = summary_by_field
    identity = str(uuid4())
    manifest = PinnedNetworkServingManifest(
        7,
        identity,
        "network_candidate_" + identity.replace("-", ""),
        1,
        copy.deepcopy(generations_by_name),
        APPROVED.approved_revision,
        "d" * 64,
        42,
    )
    _pin_provider_fixture_relations(manifest, legacy)
    return SimpleNamespace(
        candidate=candidate_by_field, manifest=manifest, scope=NetworkAddressReadScope(manifest, (11,), None)
    )


@pytest.fixture(scope="module")
def full_inventory():
    return _fixture(1000)


def _result(fixture, *, providers=True, limit=50, offset=0):
    provider_by_field = {
        "provider_system": "npi",
        "provider_id": "1000000491",
        "display_name": None,
        "npi": 1000000491,
        "network_memberships": [{"network_id": 11, "evidence_ids": ["e" * 64]}],
    }
    total = int(providers)
    return {
        "valid": True,
        "bounded": True,
        "result": json.dumps(
            {
                "membership_level": "provider",
                "generation_id": fixture.manifest.generation_id,
                "total_count": total,
                "limit": limit,
                "offset": offset,
                "has_more": total > offset + limit,
                "providers": [provider_by_field] if providers and offset == 0 else [],
            }
        ),
    }


def _spies(monkeypatch, fixture, *, receipt=None, refusal_at=None):
    events = []
    connection = AsyncMock()
    connection.is_in_transaction = lambda: True
    connection.fetchval.return_value = "repeatable read"
    connection.get_server_pid = lambda: _PID

    async def resolve(actual, *, generation_id, control_schema, api_read):
        assert actual is connection and api_read is True
        assert generation_id == fixture.manifest.generation_id and control_schema == CONTROL
        events.append("resolve")
        return fixture.manifest

    async def control(actual, names, *, control_schema, expected_candidate_table_oid):
        assert actual is connection and names == ("network_membership_candidate",)
        assert control_schema == CONTROL and expected_candidate_table_oid == _CANDIDATE_RELATION.table_oid
        return (_CANDIDATE_RELATION,)

    class PageReceipt(dict):
        def __getitem__(self, key):
            assert events[-1] == "page_guard"
            return super().__getitem__(key)

    async def row(query, *arguments):
        assert events and events[0] == "resolve"
        events.append("candidate" if len(arguments) == 2 else "page")
        return (
            fixture.candidate
            if len(arguments) == 2
            else PageReceipt(receipt if receipt is not None else _result(fixture))
        )

    async def guard(actual, relations, *, expected_backend_pid):
        assert actual is connection and expected_backend_pid == _PID
        stage = events[-1]
        assert stage in {"candidate", "page"}
        if stage == "candidate":
            assert relations == (_CANDIDATE_RELATION,)
        else:
            expected = fixture.manifest.candidate_relation_pins
            if "network_source_inventory" in {pin.table_name for pin in expected}:
                expected += (_APPROVED_RELATION,)
            assert relations == expected
        if refusal_at == stage:
            raise NetworkReadIdentityError("synthetic identity refusal")
        events.append(stage + "_guard")

    resolver = AsyncMock(side_effect=resolve)
    approved = AsyncMock(return_value=(APPROVED, _APPROVED_RELATION))
    connection.fetchrow.side_effect = row
    monkeypatch.setattr(readers, "resolve_network_serving_manifest", resolver)
    monkeypatch.setattr(custody, "read_network_control_relation_pins", AsyncMock(side_effect=control))
    monkeypatch.setattr(readers, "pin_api_retained_approved_membership_source", approved)
    monkeypatch.setattr(readers, "verify_consumed_network_relations", AsyncMock(side_effect=guard))
    return connection, resolver, approved, events


@pytest.mark.asyncio
@pytest.mark.parametrize("full", [False, True])
async def test_reference_read_uses_one_fixed_query_after_actual_resolver(monkeypatch, full_inventory, full):
    fixture = full_inventory if full else _fixture()
    connection, resolver, approved, events = _spies(monkeypatch, fixture)
    legacy = AsyncMock(side_effect=AssertionError("Referenced inventory reached legacy recipe hydration"))
    monkeypatch.setattr(readers, "_source_bindings", legacy)
    monkeypatch.setattr(readers, "verify_registry_source_recipes", lambda *_: pytest.fail("Hydrated recipe payloads"))
    bindings = AsyncMock(side_effect=AssertionError("Generated a per-source binding query"))
    monkeypatch.setattr(readers, "require_retained_approved_network_source_bindings", bindings)
    result = await readers.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    assert result["membership_level"] == "provider" and "locations" not in json.dumps(result)
    assert events == ["resolve", "candidate", "candidate_guard", "page", "page_guard"]
    resolver.assert_awaited_once()
    approved.assert_awaited_once_with(connection, fixture.manifest, control_schema=CONTROL)
    query, *arguments = connection.fetchrow.await_args.args
    assert query == provider_only_page_sql(fixture.manifest.schema_name, control_schema=CONTROL)
    assert set(re.findall(r"\$(\d+)", query)) == set("1234567")
    assert arguments == [
        APPROVED.approved_revision,
        [11],
        50,
        0,
        readers.MAX_PROVIDER_EVIDENCE,
        readers.MAX_RESPONSE_BYTES,
        fixture.manifest.generation_id,
    ]
    assert "recipe_json" not in query and "fact_json" not in query and "UNION ALL" not in query
    legacy.assert_not_awaited()
    bindings.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_valid_network_with_no_evidence_stays_an_empty_page(monkeypatch):
    fixture = _fixture()
    connection, _, _, _ = _spies(monkeypatch, fixture, receipt=_result(fixture, providers=False))
    result = await readers.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    assert result["providers"] == [] and result["total_count"] == 0 and result["has_more"] is False


@pytest.mark.asyncio
async def test_pagination_remains_bound_to_requested_generation(monkeypatch):
    fixture = _fixture()
    receipt = _result(fixture, limit=1, offset=1)
    connection, _, _, _ = _spies(monkeypatch, fixture, receipt=receipt)
    result = await readers.read_network_provider_only_page(
        connection, fixture.scope, limit=1, offset=1, control_schema=CONTROL
    )
    assert result["providers"] == [] and result["total_count"] == 1 and result["offset"] == 1
    assert connection.fetchrow.await_args.args[-5:] == (
        1,
        1,
        readers.MAX_PROVIDER_EVIDENCE,
        readers.MAX_RESPONSE_BYTES,
        fixture.manifest.generation_id,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 1])
async def test_reference_marker_never_replaces_real_manifest_resolution(monkeypatch, count):
    fixture = _fixture(count)
    connection, resolver, approved, _ = _spies(monkeypatch, fixture)
    resolver.side_effect = NetworkServingReadUnavailable("synthetic custody refusal")
    with pytest.raises(NetworkProviderReadUnavailable):
        await readers.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    connection.fetchrow.assert_not_awaited()
    approved.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 1])
async def test_every_reference_requires_resolved_native_seal(monkeypatch, count):
    fixture = _fixture(count)
    object.__delattr__(fixture.manifest, "_custody_seal_sha256")
    connection, _, approved, events = _spies(monkeypatch, fixture)
    with pytest.raises(NetworkProviderReadUnavailable):
        await readers.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    assert events == ["resolve", "candidate", "candidate_guard"]
    approved.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["entries", "recipes", "generation", "summary", "reference", "approved", "manifest"])
async def test_reference_approval_and_generation_drift_are_terminal(monkeypatch, damage):
    fixture = _fixture()
    connection, resolver, approved, events = _spies(monkeypatch, fixture)
    if damage in {"entries", "recipes", "generation"}:
        key = {"entries": INVENTORY_DIGEST_KEY, "recipes": RECIPE_DIGEST_KEY, "generation": "custom_membership"}[damage]
        fixture.candidate["source_generations"][key] = "f" * 64
    elif damage == "summary":
        fixture.candidate["validation_json"][SUMMARY_KEY]["sources_sha256"] = "f" * 64
    elif damage == "reference":
        fixture.candidate["source_recipes_json"]["table_name"] = "synthetic_other_heap"
    elif damage == "approved":
        approved.return_value = (replace(APPROVED, generation_id="f" * 64), _APPROVED_RELATION)
    else:
        resolver.side_effect = None
        resolver.return_value = replace(fixture.manifest, generation_id=8)
    with pytest.raises(NetworkProviderReadUnavailable):
        await readers.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    assert "page" not in events


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["control", "network_provider_only_evidence", "network_source_inventory"])
async def test_missing_read_identity_refuses_before_page(monkeypatch, missing):
    fixture = _fixture()
    if missing == "control":
        object.__delattr__(fixture.manifest, "_custody_control")
    else:
        object.__setattr__(
            fixture.manifest,
            "_candidate_relation_pins",
            tuple(pin for pin in fixture.manifest.candidate_relation_pins if pin.table_name != missing),
        )
    connection, _, _, events = _spies(monkeypatch, fixture)
    with pytest.raises(NetworkProviderReadUnavailable):
        await readers.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    assert "page" not in events
    assert connection.fetchrow.await_count == int(missing != "control")


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal_at", ["candidate", "page"])
@pytest.mark.parametrize("providers", [False, True])
async def test_identity_refusal_never_returns_a_page(monkeypatch, refusal_at, providers):
    fixture = _fixture()
    connection, _, approved, events = _spies(
        monkeypatch, fixture, receipt=_result(fixture, providers=providers), refusal_at=refusal_at
    )
    with pytest.raises(NetworkProviderReadUnavailable):
        await readers.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    assert events[-1] == refusal_at
    assert connection.fetchrow.await_count == (1 if refusal_at == "candidate" else 2)
    assert approved.await_count == int(refusal_at == "page")


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["valid", "bounded", "bytes", "evidence", "office", "network", "generation"])
async def test_fixed_read_preserves_response_shape_and_all_bounds(monkeypatch, damage):
    fixture = _fixture()
    receipt = _damaged_provider_receipt(fixture, damage)
    connection, _, _, _ = _spies(monkeypatch, fixture, receipt=receipt)
    with pytest.raises(NetworkProviderReadUnavailable):
        await readers.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)


@pytest.mark.asyncio
@pytest.mark.parametrize("limit,offset", [(0, 0), (101, 0), (1, -1), (1, 1000001)])
async def test_existing_page_bounds_precede_all_native_reads(monkeypatch, limit, offset):
    fixture = _fixture()
    connection, resolver, approved, _ = _spies(monkeypatch, fixture)
    with pytest.raises(NetworkProviderReadError):
        await readers.read_network_provider_only_page(
            connection, fixture.scope, limit=limit, offset=offset, control_schema=CONTROL
        )
    resolver.assert_not_awaited()
    approved.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_metadata_keeps_legacy_source_query_branch(monkeypatch):
    fixture = _fixture(legacy=True)
    connection, _, _, _ = _spies(monkeypatch, fixture)
    legacy_result = (["synthetic_bindings AS (SELECT 1)"], ["SELECT 1"], ["legacy-argument"])

    async def source_bindings(actual, current, control_schema, *, candidate, read_relations):
        assert actual is connection and current is fixture.manifest and control_schema == CONTROL
        assert candidate == fixture.candidate
        read_relations.extend(current.candidate_relation_pins)
        return legacy_result

    legacy = AsyncMock(side_effect=source_bindings)
    monkeypatch.setattr(readers, "_source_bindings", legacy)
    await readers.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    legacy.assert_awaited_once_with(
        connection,
        fixture.manifest,
        CONTROL,
        candidate=fixture.candidate,
        read_relations=list(fixture.manifest.candidate_relation_pins),
    )
    query, *arguments = connection.fetchrow.await_args.args
    assert query == readers._query(fixture.manifest, *legacy_result)
    assert arguments[0] == "legacy-argument"


def _pin_provider_fixture_relations(manifest, legacy):
    """Attach the same authenticated physical relation pins to the synthetic manifest."""
    object.__setattr__(manifest, "_custody_seal_sha256", "a" * 64)
    object.__setattr__(manifest, "_custody_control", {"candidate_table_oid": _CANDIDATE_RELATION.table_oid})
    object.__setattr__(
        manifest,
        "_candidate_relation_pins",
        tuple(
            CompactAddressRelation(manifest.schema_name, name, oid)
            for name, oid in (
                (("network_source_contribution", 23),)
                if legacy
                else (("network_provider_only_evidence", 21), ("network_source_inventory", 22))
            )
        ),
    )


def _damaged_provider_receipt(fixture, damage):
    """Keep each malformed result independent while preserving the original refusal matrix."""
    receipt = _result(fixture)
    match damage:
        case "valid" | "bounded":
            receipt[damage] = False
        case "bytes":
            receipt["result"] += " " * readers.MAX_RESPONSE_BYTES
        case _:
            document = json.loads(receipt["result"])
            provider = document["providers"][0]
            match damage:
                case "evidence":
                    provider["network_memberships"][0]["evidence_ids"] = [
                        f"{index:064x}" for index in range(readers.MAX_PROVIDER_EVIDENCE + 1)
                    ]
                case "office":
                    provider["locations"] = []
                case "network":
                    provider["network_memberships"][0]["network_id"] = 12
                case _:
                    document["generation_id"] += 1
            receipt["result"] = json.dumps(document)
    return receipt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    ["display_name", "npi_type", "empty_memberships", "duplicate_network", "duplicate_provider", "evidence_order"],
)
async def test_provider_projection_refuses_unbound_fields_and_duplicate_identity(monkeypatch, damage):
    fixture = _fixture()
    receipt = _result(fixture)
    document = json.loads(receipt["result"])
    provider = document["providers"][0]
    if damage == "duplicate_provider":
        document["providers"] *= 2
        document["total_count"] = 2
    elif damage == "duplicate_network":
        provider["network_memberships"] *= 2
    elif damage == "evidence_order":
        provider["network_memberships"][0]["evidence_ids"] = ["f" * 64, "e" * 64]
    else:
        field, value = {
            "display_name": ("display_name", "Unreviewed display"),
            "npi_type": ("npi", "1000000491"),
            "empty_memberships": ("network_memberships", []),
        }[damage]
        provider[field] = value
    receipt["result"] = json.dumps(document)
    connection, _, _, events = _spies(monkeypatch, fixture, receipt=receipt)
    with pytest.raises(NetworkProviderReadUnavailable):
        await readers.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    assert events[-1] == "page_guard"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["no_transaction", "read_committed"])
async def test_provider_projection_requires_repeatable_caller_snapshot_before_manifest(monkeypatch, mode):
    fixture = _fixture()
    connection, resolver, approved, _ = _spies(monkeypatch, fixture)
    connection.is_in_transaction = lambda: mode != "no_transaction"
    connection.fetchval.return_value = "read committed"
    with pytest.raises(NetworkProviderReadError, match="repeatable transaction"):
        await readers.read_network_provider_only_page(connection, fixture.scope, control_schema=CONTROL)
    resolver.assert_not_awaited()
    approved.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["generation_marker", "source_receipt"])
async def test_legacy_provider_facts_require_registered_contribution_evidence(missing):
    fixture = _fixture(legacy=True)
    candidate_by_field = {"source_generations": {}, "validation_json": {}, "source_recipes_json": []}
    current = fixture.manifest
    if missing == "generation_marker":
        current = replace(current, source_generations={})
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(NetworkProviderReadUnavailable, match="source evidence"):
        await readers._source_bindings(connection, current, CONTROL, candidate=candidate_by_field)
    connection.fetchrow.assert_not_awaited()
