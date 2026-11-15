# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Component contracts for the real named preparation implementation; no native proof."""

import asyncio
import copy
import importlib
import json
import subprocess
import sys
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from db.migration_named_preparation import CATALOG
from process import registry_named_preparation_catalog as catalog
from process import registry_named_preparation_lifecycle as lifecycle
from process import registry_named_preparation_store as stores
from process.network_address_projection import PinnedAddressSource
from process.registry_named_preparation_records import (
    NativePreparationAdmission,
    PreparationClaim,
    SnapshotContractError,
    admission_values,
    digest,
)

IDENTITY = str(UUID(int=1))
ROLES = {
    "namespace_owner_role": "named_owner",
    "publisher_role": "publisher",
    "application_roles": ["publisher", "builder", "reader"],
}


def document(kind="catalog"):
    return {
        "kind": kind,
        "candidate_id": IDENTITY,
        "catalog_schema": "registry",
        "storage_schema": "candidate_rows",
        "tables": [{"schema": "candidate_rows", "name": "snapshot_data"}],
        "snapshot": {"edition": "retained-é"},
    }


@pytest.mark.parametrize("kind", ["catalog", "network", "office"])
def test_named_admission_preserves_complete_original_wire_and_does_not_infer_pins(kind):
    handoff = document(kind)
    request = NativePreparationAdmission(
        IDENTITY, "registry", kind, "edition", IDENTITY, IDENTITY, IDENTITY, kind + ":edition", digest(handoff)
    )
    actual, predecessor = admission_values(None, request, {}, named_kind=kind)
    assert predecessor is None and actual == {**asdict(request), "kind": kind}
    assert digest(actual) == digest(json.loads(json.dumps(actual, sort_keys=True)))
    for field in ("expected_generation_id", "expected_fence_token", "predecessor_pin_id", "predecessor_pin_fence"):
        with pytest.raises(SnapshotContractError, match="predecessor_invalid"):
            admission_values(None, replace(request, **{field: IDENTITY}), {}, named_kind=kind)


@pytest.mark.parametrize("damage", ["kind", "extra", "duplicate", "physical", "schema", "target"])
def test_named_handoff_refuses_partial_foreign_or_physical_metadata(damage):
    handoff = document()
    match damage:
        case "kind":
            handoff["kind"] = "unknown"
        case "extra":
            handoff["extra"] = True
        case "duplicate":
            handoff["tables"] *= 2
        case "physical":
            handoff["snapshot"]["table_oid"] = 3
        case "schema":
            handoff["tables"][0]["schema"] = "other_rows"
        case _:
            handoff["candidate_id"] = "bad"
    with pytest.raises((ValueError, SnapshotContractError)):
        stores.named_preparation_document(handoff, "catalog")


def test_named_handoff_is_detached_and_role_capacity_is_not_reduced():
    handoff = document()
    detached = stores.named_preparation_document(handoff, "catalog")
    detached["snapshot"]["edition"] = "changed"
    assert handoff["snapshot"]["edition"] == "retained-é"
    roles_by_field = {**ROLES, "application_roles": ["publisher", *("reader_" + str(i) for i in range(40))]}
    assert len(stores.NamedPreparationStore(object(), roles_by_field, kind="network").application_roles) == 41


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["expired", "token", "attempt", "canceled", "published"])
async def test_claim_refusal_precedes_every_advancing_write(monkeypatch, damage):
    claim = PreparationClaim(IDENTITY, str(UUID(int=2)), 1)
    state_by_field = {
        "preparation_id": UUID(IDENTITY),
        "state": "frozen",
        "claim_token": UUID(claim.lease_token),
        "claim_attempt": 1,
        "claim_expires_at": object(),
        "cancellation_requested": False,
    }
    if damage == "token":
        state_by_field["claim_token"] = UUID(int=3)
    elif damage == "attempt":
        state_by_field["claim_attempt"] = 2
    elif damage == "canceled":
        state_by_field["cancellation_requested"] = True
    elif damage == "published":
        state_by_field["state"] = "published"
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=damage != "expired"), execute=AsyncMock())
    monkeypatch.setattr(catalog, "locked_preparation", AsyncMock(return_value=state_by_field))
    with pytest.raises(SnapshotContractError):
        await catalog.locked_claim(SimpleNamespace(connection=connection), claim)
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_frozen_inventory_requires_complete_reserved_names_and_same_digest():
    handoff = document()
    inventory_by_field = {"relations": [{"schema_name": "candidate_rows", "relation_name": "snapshot_data"}]}
    state_by_field = {
        "preparation_id": UUID(IDENTITY),
        "intake_inventory": inventory_by_field,
        "state": "frozen",
        "frozen_inventory": copy.deepcopy(inventory_by_field),
        "frozen_sha256": digest(inventory_by_field),
    }
    connection = SimpleNamespace(
        fetch=AsyncMock(side_effect=[inventory_by_field["relations"], []]), execute=AsyncMock()
    )
    store = SimpleNamespace(connection=connection, authority={}, named_kind="catalog")
    assert await lifecycle._frozen_inventory(store, state_by_field) == inventory_by_field
    assert connection.execute.await_args.args[0] == (
        'LOCK TABLE ONLY "candidate_rows"."snapshot_data" IN ACCESS SHARE MODE NOWAIT'
    )
    connection.fetch.side_effect = [[]]
    with pytest.raises(SnapshotContractError, match="stage_claim_changed"):
        await lifecycle._frozen_inventory(store, state_by_field)
    state_by_field["frozen_sha256"] = "f" * 64
    connection.fetch.side_effect = [inventory_by_field["relations"], []]
    with pytest.raises(SnapshotContractError, match="freeze_changed"):
        await lifecycle._frozen_inventory(store, state_by_field)
    assert stores.named_preparation_document(handoff, "catalog") == handoff


def test_catalog_ddl_preserves_constraints_without_hooks_or_foreign_keys():
    sql = "\n".join(CATALOG)
    assert "FOREIGN KEY" not in sql and "CREATE FUNCTION" not in sql and "CREATE TRIGGER" not in sql
    assert "relation_oid" not in sql and "builder_oid" not in sql
    assert "UNIQUE (node_id, importer_id, run_id, dataset_key)" in sql
    assert "UNIQUE (preparation_id, attempt)" in sql and "lease_seconds BETWEEN 1 AND 120" in sql
    assert "PRIMARY KEY (schema_name, relation_name)" in sql
    assert "published_fence_token IS NULL" in sql and "publication_fence::text" in sql
    assert all(not statement.rstrip().endswith(",\n)") for statement in CATALOG)


def test_standalone_public_imports_use_core_and_installed_dependencies():
    program = """
import sys
from pathlib import Path
from unittest.mock import AsyncMock
import asyncpg
blocked = AsyncMock(side_effect=AssertionError('unexpected default database transport'))
asyncpg.connect = blocked
sys.path.insert(0, str(Path.cwd()))
from process.registry_named_preparation_store import NamedPreparationStore
from process.registry_named_preparation_administration import _freeze_registry_generation
from process.network_membership_publication import admit_network_preparation
from process.registry_fhir_catalog_store import admit_catalog_preparation
from process.registry_ptg_office_preparation import admit_office_preparation
assert not blocked.call_count
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", program], capture_output=True, text=True, timeout=20, check=False
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
@pytest.mark.parametrize("preexisting", ["empty_schema", "absent_schema", "current_tables"])
@pytest.mark.parametrize("setup_fails", [False, True])
async def test_foundation_registers_exact_cleanup_before_bootstrap(preexisting, setup_fails):
    from db.migration_named_preparation import TABLES, upgrade_named_preparation_catalog
    from tests.registry_named_preparation_postgres_support import (
        cleanup_registry_preparation_foundation,
        prepare_registry_catalog_foundation,
    )

    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value=preexisting != "absent_schema"),
        fetch=AsyncMock(return_value=list(TABLES) if preexisting == "current_tables" else []),
        execute=AsyncMock(),
    )
    fixture = SimpleNamespace(connection=connection, created_control_namespaces=[])

    async def run_sync(function):
        assert function is upgrade_named_preparation_catalog
        assert fixture.named_preparation_created_tables == ([] if preexisting == "current_tables" else list(TABLES))
        if setup_fails:
            raise RuntimeError("bootstrap failed")

    @asynccontextmanager
    async def transaction():
        yield SimpleNamespace(run_sync=run_sync)

    fixture.engine = SimpleNamespace(begin=transaction)
    if setup_fails:
        with pytest.raises(RuntimeError, match="bootstrap failed"):
            await prepare_registry_catalog_foundation(fixture)
    else:
        await prepare_registry_catalog_foundation(fixture)
    await cleanup_registry_preparation_foundation(fixture)
    statements = [call.args[0] for call in connection.execute.await_args_list]
    expected_drops = 0 if preexisting == "current_tables" else 4
    assert sum(statement.startswith("DROP TABLE") for statement in statements) == expected_drops
    assert sum(statement.startswith("DROP SCHEMA") for statement in statements) == int(preexisting == "absent_schema")
    assert not any("CASCADE" in statement for statement in statements)


@pytest.mark.asyncio
async def test_foundation_cleanup_preserves_the_original_body_failure():
    from tests.registry_named_preparation_postgres_support import cleanup_registry_preparation_foundation

    primary, cleanup = ValueError("original body"), RuntimeError("cleanup failed")
    fixture = SimpleNamespace(
        connection=SimpleNamespace(execute=AsyncMock(side_effect=cleanup)),
        named_preparation_created_tables=["preparation"],
        created_control_namespaces=[],
    )
    with pytest.raises(ValueError) as observed:
        await cleanup_registry_preparation_foundation(fixture, primary=primary)
    assert observed.value is primary and observed.value.__cause__ is cleanup


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["catalog", "network", "office"])
@pytest.mark.parametrize("damage", ["kind", "candidate", "snapshot"])
async def test_ready_loader_binds_exact_kind_candidate_and_snapshot(monkeypatch, kind, damage):
    modules_by_kind = {
        "catalog": ("registry_fhir_catalog_store", "load_named_catalog_preparation"),
        "network": ("network_membership_publication", "load_named_network_preparation"),
        "office": ("registry_ptg_office_preparation", "load_named_office_preparation"),
    }
    handoff = document(kind)
    returned = copy.deepcopy(handoff)
    if damage == "kind":
        returned["kind"] = "office" if kind != "office" else "catalog"
    elif damage == "candidate":
        returned["candidate_id"] = str(UUID(int=2))
    else:
        returned["snapshot"]["edition"] = "different"
    module, attribute = modules_by_kind[kind]
    loader = AsyncMock(return_value=returned)
    monkeypatch.setattr(importlib.import_module("process." + module), attribute, loader)
    connection = object()
    with pytest.raises(SnapshotContractError):
        await stores.require_named_ready(connection, handoff)
    loader.assert_awaited_once_with(connection, IDENTITY, control_schema="registry")


@pytest.mark.asyncio
async def test_historical_address_bootstrap_uses_admin_and_one_snapshot(monkeypatch):
    """Keep historical writes administrative and worker access limited to declared reads."""
    from tests import fhir_catalog_sync_integration_support as support

    phases = []

    @asynccontextmanager
    async def transaction(**options):
        assert options == {"isolation": "repeatable_read"}
        phases.append("begin")
        yield
        assert admin.execute.await_count == 2
        phases.append("end")

    admin = SimpleNamespace(transaction=transaction, execute=AsyncMock())
    fixture = SimpleNamespace(connection=admin, base=object(), composition_ids=[], control_schema="registry")
    case = SimpleNamespace(
        fixture=fixture,
        seed=SimpleNamespace(revision=7),
        address_custody=SimpleNamespace(custodian=object(), roles={"custody": "frozen_owner"}),
    )
    dependency_id, request_id = UUID(int=1), UUID(int=2)
    selected_address = PinnedAddressSource("addresses", "entity_address_unified", "c" * 64)
    artifact_result = SimpleNamespace(
        address_source=PinnedAddressSource("prepared_addresses", "entity_address_unified", "d" * 64)
    )
    approved = SimpleNamespace(generation_id="b" * 64)

    async def prepare(same_connection, *_args, **options):
        assert same_connection is admin and phases == ["begin"]
        assert options["owner_role"] == "frozen_owner" and options["runtime_roles"] == ("worker", "reader")
        phases.append("dependency")
        return SimpleNamespace(address_source=selected_address)

    async def pin(same_connection, **options):
        assert same_connection is admin and phases[-1] == "dependency"
        assert options == {"approved_revision": 7, "control_schema": "registry"}
        return approved

    async def artifact(same_connection, **options):
        assert same_connection is admin and options["request_id"] == request_id
        assert options["custody"].owner_role == "frozen_owner"
        assert options["custody"].runtime_roles == ("worker", "reader")
        phases.append("artifact")
        return artifact_result

    monkeypatch.setattr(support, "uuid4", lambda: dependency_id)
    monkeypatch.setattr(support, "prepare_custom_address_source", prepare)
    monkeypatch.setattr(support, "pin_approved_membership_source", pin)
    monkeypatch.setattr(support, "prepare_registry_composition_address_artifact", artifact)
    assert await support._initial_address_artifact(case, request_id, ("worker", "reader")) is artifact_result
    assert [call.args[0] for call in admin.execute.await_args_list] == [
        'GRANT USAGE ON SCHEMA "prepared_addresses" TO "worker","reader"',
        'GRANT SELECT ON ALL TABLES IN SCHEMA "prepared_addresses" TO "worker","reader"',
    ]
    assert phases == ["begin", "dependency", "artifact", "end"]
    assert fixture.composition_ids[0] == dependency_id and len(fixture.composition_ids) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["prepared", "absent", "preparation_error", "grant_error"])
async def test_rollover_artifact_reader_grants_share_preparation_transaction(monkeypatch, outcome):
    """Grant declared reads before commit, and preserve optional absence and both failures."""
    from tests import fhir_catalog_serving_rollover_support as support

    events = []

    @asynccontextmanager
    async def transaction(**options):
        assert options == {"isolation": "repeatable_read"}
        events.append("begin")
        try:
            yield
        finally:
            events.append("end")

    async def grant(statement):
        assert events == ["begin", "prepared"]
        if outcome == "grant_error":
            raise RuntimeError("grant_error")

    custodian = SimpleNamespace(transaction=transaction, execute=AsyncMock(side_effect=grant))
    case = SimpleNamespace(
        fixture=SimpleNamespace(control_schema="registry"),
        address_custody=SimpleNamespace(custodian=custodian, roles={"custody": "frozen_owner"}),
    )
    artifact = None if outcome == "absent" else SimpleNamespace(address_source=SimpleNamespace(schema_name="prepared"))
    request, predecessor, addresses = UUID(int=1), SimpleNamespace(generation_id=5), object()
    readers = ("worker_execution", "source_reader", "catalog_observer")

    async def prepare(connection, **options):
        assert connection is custodian and events == ["begin"]
        assert options["request_id"] == request and options["approved_revision"] == 7
        assert options["source_manifest"] is predecessor and options["expected_head"] == 5
        assert options["address_sources"] is addresses and options["custody"].runtime_roles == readers
        if outcome == "preparation_error":
            raise RuntimeError("preparation_error")
        events.append("prepared")
        return artifact

    monkeypatch.setattr(support, "prepare_registry_composition_address_artifact", prepare)
    arguments = (case, request, {"approved_revision": 7}, predecessor, addresses, readers)
    if outcome.endswith("error"):
        with pytest.raises(RuntimeError, match=outcome):
            await support._prepare_directory_artifact(*arguments)
    else:
        assert await support._prepare_directory_artifact(*arguments) is artifact
    assert events == (["begin", "end"] if outcome == "preparation_error" else ["begin", "prepared", "end"])
    statements = [call.args[0] for call in custodian.execute.await_args_list]
    expected_statements = [
        'GRANT USAGE ON SCHEMA "prepared" TO "worker_execution","source_reader","catalog_observer"',
        'GRANT SELECT ON ALL TABLES IN SCHEMA "prepared" TO "worker_execution","source_reader","catalog_observer"',
    ]
    assert statements == (
        expected_statements if outcome == "prepared" else expected_statements[:1] if outcome == "grant_error" else []
    )
