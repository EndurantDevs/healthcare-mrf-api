# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control/fact metadata API wiring; mocks establish no native authority."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import network_approved_membership_source as approved
from process import network_approved_source_bindings as bindings
from process import network_candidate_custody as custody
from process import network_serving_read as serving
from process import registry_source_contribution_artifacts as artifacts
from process import registry_source_inventory_store as inventory
from process.network_compact_layout import CompactAddressRelation
from process.network_read_identity import NetworkReadIdentityError
from process.registry_management_permissions import _TABLES
from tests.test_network_approved_source_bindings import COORDINATES, boundary, scope_row
from tests.test_network_raw_provider_api_identity import _manifest
from tests.test_registry_source_inventory_fact_owners import CONTROL, _owner, _selected

pytestmark = pytest.mark.asyncio
_PID = 77


def _control_fixture(monkeypatch):
    control_by_field = {"schema_oid": 10, "schema_owner_oid": 501, "candidate_table_oid": 11, "owner_oid": 500}
    table_records = [
        dict(
            relname=name,
            oid=oid,
            relowner=501,
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
        for name, oid in (
            ("network_membership_candidate", 11),
            ("network_serving_manifest", 12),
            ("registry_approved_record", 13),
        )
    ]
    native = AsyncMock(return_value=control_by_field)
    monkeypatch.setattr(custody, "_control", native)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value=("repeatable read", "on")),
        fetch=AsyncMock(return_value=table_records),
        execute=AsyncMock(),
    )
    return connection, control_by_field, table_records, native


async def test_current_control_profiles_use_actual_owner(monkeypatch):
    connection, control, rows, native = _control_fixture(monkeypatch)
    pins = await custody.read_network_control_relation_pins(
        connection, tuple(row["relname"] for row in rows), control_schema=CONTROL, expected_candidate_table_oid=11
    )
    assert [pin.table_oid for pin in pins] == [11, 12, 13]
    assert all(pin.schema_name == CONTROL for pin in pins)
    assert connection.fetch.await_args.args[1] == control["schema_oid"]
    assert native.await_count == 2
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("relowner", 500),
        ("relkind", "v"),
        ("heap", False),
        ("relrowsecurity", True),
        ("rules", True),
        ("inherited", True),
        ("columns", ["wrong"]),
        ("oid", 99),
    ],
)
async def test_changed_control_profile_refuses(monkeypatch, field, value):
    connection, _, rows, _ = _control_fixture(monkeypatch)
    rows[0][field] = value
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await custody.read_network_control_relation_pins(
            connection, tuple(row["relname"] for row in rows), control_schema=CONTROL
        )
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("names", [(), [], ("other_table",), ("registry_approved_record",) * 2])
async def test_control_selection_is_closed(monkeypatch, names):
    connection, _, _, native = _control_fixture(monkeypatch)
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await custody.read_network_control_relation_pins(connection, names, control_schema=CONTROL)
    native.assert_not_awaited()


async def test_read_write_control_context_refuses(monkeypatch):
    connection, _, _, native = _control_fixture(monkeypatch)
    connection.fetchrow.return_value = ("repeatable read", "off")
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await custody.read_network_control_relation_pins(
            connection, ("registry_approved_record",), control_schema=CONTROL
        )
    native.assert_not_awaited()


async def test_approved_helper_reauthenticates_caller_manifest(monkeypatch):
    manifest = _manifest()
    manifest.source_generations["custom_membership"] = "b" * 64
    object.__setattr__(manifest, "_custody_control", {"candidate_table_oid": 11})
    caller = replace(manifest)
    object.__setattr__(caller, "_custody_control", {"candidate_table_oid": 999})
    pin = CompactAddressRelation(CONTROL, "registry_approved_record", 13)
    resolver = AsyncMock(return_value=manifest)
    native = AsyncMock(return_value=(pin,))
    scope = AsyncMock(return_value=approved.ApprovedMembershipSource(3, "b" * 64))
    monkeypatch.setattr(serving, "resolve_network_serving_manifest", resolver)
    monkeypatch.setattr(custody, "read_network_control_relation_pins", native)
    monkeypatch.setattr(approved, "_scope", scope)
    connection = SimpleNamespace(get_server_pid=lambda: _PID)
    result, relation = await approved.pin_api_retained_approved_membership_source(
        connection, caller, control_schema=CONTROL
    )
    assert result.generation_id == "b" * 64 and relation == pin
    assert resolver.await_args.kwargs["api_read"] is True
    assert native.await_args.kwargs["expected_candidate_table_oid"] == 11
    assert scope.await_args.kwargs["read_identity"] == (pin, _PID)


@pytest.mark.parametrize("failure", [False, True])
async def test_scope_guard_runs_before_scalar_interpretation(monkeypatch, failure):
    events = []

    class Receipt(dict):
        def __getitem__(self, key):
            assert events == ["guard"]
            return super().__getitem__(key)

    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=Receipt(scope_row(3, "b" * 64)))
    )
    pin = CompactAddressRelation(CONTROL, "registry_approved_record", 13)

    async def verify(actual, relations, *, expected_backend_pid):
        assert actual is connection and relations == (pin,) and expected_backend_pid == _PID
        if failure:
            raise NetworkReadIdentityError("synthetic mismatch")
        events.append("guard")

    monkeypatch.setattr(approved, "verify_consumed_network_relations", AsyncMock(side_effect=verify))
    if failure:
        with pytest.raises(approved.ApprovedMembershipSourceError):
            await approved._scope(connection, 3, '"' + CONTROL + '"', retained=True, read_identity=(pin, _PID))
    else:
        assert (
            await approved._scope(connection, 3, '"' + CONTROL + '"', retained=True, read_identity=(pin, _PID))
        ).generation_id == "b" * 64


async def test_binding_checks_guard_each_actual_fetch(monkeypatch):
    connection, calls = boundary(current_revision=3, fingerprint="b" * 64)
    pin = CompactAddressRelation("sample_control", "registry_approved_record", 13)
    guard = AsyncMock()
    monkeypatch.setattr(approved, "verify_consumed_network_relations", guard)
    result = await bindings.require_retained_approved_network_source_bindings(
        connection,
        approved.ApprovedMembershipSource(3, "b" * 64),
        COORDINATES,
        control_schema="sample_control",
        read_identity=(pin, _PID),
    )
    assert result == 2 and len(calls) == guard.await_count == 2
    assert all(call.kwargs["expected_backend_pid"] == _PID for call in guard.await_args_list)


@pytest.mark.parametrize("legacy", [False, True])
async def test_fact_owner_api_preserves_required_authentication(monkeypatch, legacy):
    owner = _owner(2, legacy=legacy)
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        get_server_pid=lambda: _PID,
        fetchval=AsyncMock(side_effect=["repeatable read", "on"]),
        fetch=AsyncMock(return_value=[owner.candidate]),
    )
    pins = (
        CompactAddressRelation(CONTROL, "network_membership_candidate", 11),
        CompactAddressRelation(CONTROL, "network_serving_manifest", 12),
    )
    native = AsyncMock(return_value=owner.custody)
    loader = AsyncMock(return_value=(owner.reference, owner.entries))
    locked = AsyncMock(side_effect=AssertionError("API selected locking owner verification"))
    monkeypatch.setattr(custody, "read_network_control_relation_pins", AsyncMock(return_value=pins))
    monkeypatch.setattr(custody, "verify_network_candidate_read_metadata", native)
    monkeypatch.setattr(artifacts, "verify_network_candidate_custody", locked)
    monkeypatch.setattr(inventory, "load_source_inventory_for_api", loader)
    guard = AsyncMock()
    monkeypatch.setattr(artifacts, "verify_consumed_network_relations", guard)
    selected = _selected(owner, owner.sources[:1])
    resolved = await artifacts.authenticate_anchored_source_fact_metadata(
        connection, selected, control_schema=CONTROL, api_read=True
    )
    assert len(resolved) == 1 and resolved[selected[0]["recipe_sha256"]].reference.fact_table_oid == 42
    assert native.await_args.kwargs["required"] is True
    assert loader.await_count == int(not legacy)
    assert guard.await_args.args[1] == pins
    locked.assert_not_awaited()


async def test_empty_fact_metadata_api_still_requires_readonly():
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchval=AsyncMock(side_effect=["repeatable read", "off"])
    )
    with pytest.raises(ValueError):
        await artifacts.authenticate_anchored_source_fact_metadata(
            connection, (), control_schema=CONTROL, api_read=True
        )
