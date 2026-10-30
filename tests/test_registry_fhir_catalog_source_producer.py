# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source export lifetime contracts; fake streams establish no native authority."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.registry_fhir_catalog_extraction import fhir_catalog_json_bytes
from process.registry_fhir_catalog_source_producer import (
    CATALOG_DATABASE_IDENTITY_SQL,
    iter_source_owned_observation_batches,
    open_source_owned_catalog_export,
)
from tests.test_registry_fhir_catalog_sources import read_profile, scoped_read_profile


def _boundary():
    identity_by_field = {
        "database_name": "synthetic_catalog",
        "database_oid": 104,
        "system_identifier": "123456789",
        "postmaster_started": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "recovering": False,
    }

    async def inventory():
        yield ("source-page",)

    async def legacy_bindings(descriptor, organization_ids):
        yield (descriptor, organization_ids)

    export_by_name = {
        "database_identity": identity_by_field,
        "inventory": Mock(side_effect=inventory),
        "replay": AsyncMock(),
        "legacy_bindings": Mock(side_effect=legacy_bindings),
    }
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=dict(identity_by_field)))
    events = []

    @asynccontextmanager
    async def source(*, source_profile):
        events.append(("enter", source_profile))
        try:
            yield export_by_name
        except BaseException as error:
            events.append(("error", error))
            raise
        finally:
            events.append(("exit",))

    return Mock(side_effect=source), export_by_name, connection, events


@pytest.mark.asyncio
async def test_export_keeps_exact_profile_mapping_and_source_lifetime():
    factory, export, connection, events = _boundary()
    profile = read_profile()
    observations = tuple(
        ("source-generation", "InsurancePlan", "plan-one", "edge", index, '{"network_ref":"Organization/network-one"}')
        for index in range(1001)
    )

    async def replay(descriptor, collector):
        for batch in iter_source_owned_observation_batches(observations):
            await collector(batch)
        return 7

    export["replay"].side_effect = replay
    collector = AsyncMock()
    async with open_source_owned_catalog_export(factory, profile, connection) as selected:
        assert selected is export and events == [("enter", profile)]
        assert [page async for page in selected["inventory"]()] == [("source-page",)]
        source_rows = await selected["replay"]("descriptor", collector)
        assert type(source_rows) is int and source_rows == 7
        assert [page async for page in selected["legacy_bindings"]("descriptor", ("organization-one",))] == [
            ("descriptor", ("organization-one",))
        ]
    assert events == [("enter", profile), ("exit",)]
    factory.assert_called_once_with(source_profile=profile)
    connection.fetchrow.assert_awaited_once_with(CATALOG_DATABASE_IDENTITY_SQL)
    export["inventory"].assert_called_once_with()
    export["replay"].assert_awaited_once_with("descriptor", collector)
    batches = [call.args[0] for call in collector.await_args_list]
    assert [len(batch) for batch in batches] == [1000, 1]
    assert tuple(observation for batch in batches for observation in batch) == observations
    assert all(len(fhir_catalog_json_bytes(batch)) <= 8 * 1024 * 1024 for batch in batches)
    export["legacy_bindings"].assert_called_once_with("descriptor", ("organization-one",))


@pytest.mark.parametrize("as_lists", [False, True])
def test_observation_batch_bytes_include_json_framing_and_utf8(as_lists):
    rows = [
        ("generation", "Organization", f"network-{index}", "organization", 0, '{"name":"界é"}') for index in range(3)
    ]
    if as_lists:
        rows = [list(row) for row in rows]
    exact_bytes = len(fhir_catalog_json_bytes(rows[:2]))
    exact_batches = tuple(iter_source_owned_observation_batches(iter(rows), page_bytes=exact_bytes))
    smaller_batches = tuple(iter_source_owned_observation_batches(iter(rows), page_bytes=exact_bytes - 1))
    assert [len(batch) for batch in exact_batches] == [2, 1]
    assert [len(batch) for batch in smaller_batches] == [1, 1, 1]
    assert [row for batch in exact_batches for row in batch] == rows
    assert all(len(fhir_catalog_json_bytes(batch)) <= exact_bytes for batch in exact_batches)
    assert all(len(fhir_catalog_json_bytes(batch)) <= exact_bytes - 1 for batch in smaller_batches)


def test_observation_too_large_for_one_batch_refuses_without_truncation():
    row = ("generation", "Organization", "network-one", "organization", 0, '{"name":"界"}')
    exact_bytes = len(fhir_catalog_json_bytes((row,)))
    assert tuple(iter_source_owned_observation_batches((row,), page_bytes=exact_bytes)) == ((row,),)
    with pytest.raises(ValueError, match="observation_too_large"):
        tuple(iter_source_owned_observation_batches((row,), page_bytes=exact_bytes - 1))


@pytest.mark.parametrize(
    "bounds_by_name",
    [
        {"page_rows": 0},
        {"page_rows": True},
        {"page_rows": 1001},
        {"page_bytes": 0},
        {"page_bytes": 8 * 1024 * 1024 + 1},
    ],
)
def test_observation_batches_refuse_caps_above_the_contract_or_invalid_bounds(bounds_by_name):
    with pytest.raises(ValueError, match="batch_bounds_invalid"):
        tuple(iter_source_owned_observation_batches((), **bounds_by_name))


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", [None, {}, scoped_read_profile()])
async def test_invalid_or_scoped_profile_refuses_before_source_entry(profile):
    factory, _, connection, events = _boundary()
    with pytest.raises(ValueError, match="profile_invalid"):
        async with open_source_owned_catalog_export(factory, profile, connection):
            pytest.fail("invalid profile yielded")
    factory.assert_not_called()
    connection.fetchrow.assert_not_awaited()
    assert events == []


@pytest.mark.asyncio
async def test_noncallable_factory_refuses_before_registry_access():
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(ValueError, match="factory_invalid"):
        async with open_source_owned_catalog_export(None, read_profile(), connection):
            pytest.fail("invalid factory yielded")
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("export", [None, (), []])
async def test_nonmapping_export_refuses_and_exits_the_source(export):
    events = []

    @asynccontextmanager
    async def source(*, source_profile):
        try:
            yield export
        finally:
            events.append("exit")

    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(ValueError, match="export_invalid"):
        async with open_source_owned_catalog_export(source, read_profile(), connection):
            pytest.fail("invalid export yielded")
    assert events == ["exit"]
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "extra", "inventory", "replay", "legacy_bindings", "identity_shape"])
async def test_invalid_export_refuses_before_inventory_and_closes_source(damage):
    factory, export, connection, events = _boundary()
    inventory = export["inventory"]
    if damage == "missing":
        del export["replay"]
    elif damage == "extra":
        export["reader"] = object()
    elif damage == "identity_shape":
        export["database_identity"] = tuple(export["database_identity"].values())
    else:
        export[damage] = None
    with pytest.raises(ValueError):
        async with open_source_owned_catalog_export(factory, read_profile(), connection):
            pytest.fail("invalid export yielded")
    inventory.assert_not_called()
    connection.fetchrow.assert_not_awaited()
    assert [event[0] for event in events] == ["enter", "error", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["database_name", "database_oid", "system_identifier", "postmaster_started", "recovering"]
)
async def test_each_database_identity_mismatch_refuses_before_inventory(field):
    factory, export, connection, events = _boundary()
    connection.fetchrow.return_value[field] = {
        "database_name": "other_catalog",
        "database_oid": 105,
        "system_identifier": "987654321",
        "postmaster_started": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "recovering": True,
    }[field]
    with pytest.raises(ValueError, match="database_changed"):
        async with open_source_owned_catalog_export(factory, read_profile(), connection):
            pytest.fail("mismatched database yielded")
    export["inventory"].assert_not_called()
    assert [event[0] for event in events] == ["enter", "error", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_field", "extra_field", "recovering", "missing_registry_identity"])
async def test_identity_shape_recovery_and_missing_registry_row_refuse(damage):
    factory, export, connection, events = _boundary()
    identity = export["database_identity"]
    if damage == "missing_field":
        del identity["database_oid"]
    elif damage == "extra_field":
        identity["actor"] = "source_reader"
    elif damage == "recovering":
        identity["recovering"] = connection.fetchrow.return_value["recovering"] = True
    else:
        connection.fetchrow.return_value = None
    with pytest.raises(ValueError, match="database_changed"):
        async with open_source_owned_catalog_export(factory, read_profile(), connection):
            pytest.fail("invalid identity yielded")
    export["inventory"].assert_not_called()
    assert [event[0] for event in events] == ["enter", "error", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("registry_side", [False, True])
@pytest.mark.parametrize(
    "field,value", [("database_oid", True), ("postmaster_started", "2026-01-01"), ("recovering", 0)]
)
async def test_native_identity_types_cannot_pass_through_value_equality(registry_side, field, value):
    factory, export, connection, events = _boundary()
    export["database_identity"]["database_oid"] = connection.fetchrow.return_value["database_oid"] = 1
    identity = connection.fetchrow.return_value if registry_side else export["database_identity"]
    identity[field] = value
    with pytest.raises(ValueError, match="database_changed"):
        async with open_source_owned_catalog_export(factory, read_profile(), connection):
            pytest.fail("wrong identity type yielded")
    export["inventory"].assert_not_called()
    assert [event[0] for event in events] == ["enter", "error", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_at", ["registry", "replay", "consumer"])
@pytest.mark.parametrize("cancelled", [False, True])
async def test_original_error_or_cancellation_reaches_source_exit_once(failure_at, cancelled):
    factory, export, connection, events = _boundary()
    failure = asyncio.CancelledError() if cancelled else RuntimeError("synthetic failure")
    if failure_at == "registry":
        connection.fetchrow.side_effect = failure
    elif failure_at == "replay":
        export["replay"].side_effect = failure
    with pytest.raises(type(failure)) as raised:
        async with open_source_owned_catalog_export(factory, read_profile(), connection) as selected:
            if failure_at == "replay":
                await selected["replay"]("descriptor", "collector")
            else:
                raise failure
    assert raised.value is failure
    assert [event[0] for event in events] == ["enter", "error", "exit"]
    assert events[1][1] is failure


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_source_entry_failure_remains_original_and_cleans_up_once(cancelled):
    failure = asyncio.CancelledError() if cancelled else RuntimeError("synthetic source entry failure")
    events = []

    @asynccontextmanager
    async def source(*, source_profile):
        try:
            yield await AsyncMock(side_effect=failure)()
        finally:
            events.append("exit")

    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(type(failure)) as raised:
        async with open_source_owned_catalog_export(source, read_profile(), connection):
            pytest.fail("source entry failure yielded")
    assert raised.value is failure and events == ["exit"]
    connection.fetchrow.assert_not_awaited()
