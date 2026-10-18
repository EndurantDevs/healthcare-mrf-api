# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Input capabilities and transport boundaries; no native profile claim."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import network_fhir_source_epoch as epoch
from process import network_fhir_source_epoch_pin as pin
from process import network_registry_cms_prepared_pair as prepared
from process import provider_directory_generic_source_policy as policy


def _input():
    source_pin = SimpleNamespace(schema_name="synthetic", dataset_id="dataset", release_id="release")
    driver = SimpleNamespace(is_in_transaction=lambda: True)
    return pin.PinnedCMSFHIRSourceEpochInput(source_pin, "a" * 64, "b" * 64, driver)


@pytest.mark.asyncio
async def test_unissued_input_cannot_reach_destination():
    source = _input()
    destination = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(epoch.FHIRSourceEpochError):
        await epoch.capture_source_cms_fhir_source_epoch(
            destination, source, epoch_id=uuid4(), owner_role="retained_owner", runtime_roles=("reader",)
        )
    destination.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_copy_input_expires_and_cannot_cross_tasks():
    source = _input()
    pin._INPUTS[source] = dict(
        task=asyncio.current_task(),
        connection=source.connection,
        identity=(source.source_pin, source.admission_sha256, source.metadata_sha256),
    )
    try:
        assert source.require_active() is None

        async def another_task():
            with pytest.raises(epoch.FHIRSourceEpochError):
                source.require_active()

        await asyncio.create_task(another_task())
    finally:
        pin._INPUTS.pop(source)
    with pytest.raises(epoch.FHIRSourceEpochError):
        source.require_active()


@pytest.mark.asyncio
async def test_source_target_cannot_inherit_source_owner():
    source = _input()
    pin._INPUTS[source] = dict(
        task=asyncio.current_task(),
        connection=source.connection,
        identity=(source.source_pin, source.admission_sha256, source.metadata_sha256),
        policy=SimpleNamespace(owner_role="source_owner"),
        virtual_xid="1/1",
    )
    source.connection.fetchval = AsyncMock(return_value="1/1")
    destination = SimpleNamespace(
        fetchrow=AsyncMock(return_value=dict(login="retained_publisher", actor="retained_publisher", source_owner=True))
    )
    try:
        with pytest.raises(epoch.FHIRSourceEpochError):
            await source.require_target(destination, "retained_owner")
    finally:
        pin._INPUTS.pop(source)


@pytest.mark.asyncio
async def test_export_and_import_keep_distinct_original_connections():
    exported, imported = AsyncMock(return_value="COPY 2"), AsyncMock(return_value="COPY 2")
    transaction = object()
    source = SimpleNamespace(verify_transaction=AsyncMock(), connection=SimpleNamespace(copy_from_query=exported))
    target = SimpleNamespace(transaction=lambda: transaction, copy_to_table=imported)
    connections = pin._CopyConnections(source, target)
    assert connections.transaction() is transaction
    assert await connections.copy_from_query("SELECT") == "COPY 2"
    assert await connections.copy_to_table("target") == "COPY 2"
    exported.assert_awaited_once_with("SELECT")
    imported.assert_awaited_once_with("target")


def test_all_epoch_queries_use_exact_pinned_children_including_nested_scope():
    relations_by_root = {root: '"synthetic"."closed_' + str(index) + '"' for index, root in enumerate(epoch._TABLES)}
    query = 'SELECT head.* FROM "synthetic"."provider_directory_entity_source_binding" head WHERE ' + epoch._scope(
        epoch._TABLES[2], '"synthetic"'
    )
    changed = epoch._source_query(query, '"synthetic"', relations_by_root)
    assert relations_by_root[epoch._TABLES[2]] in changed
    assert relations_by_root[epoch._TABLES[1]] in changed
    assert '"synthetic".provider_directory_' not in changed
    assert '"synthetic"."provider_directory_' not in changed
    with pytest.raises(epoch.FHIRSourceEpochError):
        epoch._source_query(query, '"synthetic"', {})


@pytest.mark.asyncio
async def test_source_context_refuses_legacy_capture_before_any_io(monkeypatch):
    monkeypatch.setattr(policy, "is_source_storage_required", lambda: True)
    destination = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(epoch.FHIRSourceEpochError):
        await epoch.capture_retained_cms_fhir_source_epoch(
            destination,
            None,
            epoch_id=uuid4(),
            owner_role="retained_owner",
            runtime_roles=("reader",),
            expected_admission_sha256="a" * 64,
            expected_metadata_sha256="b" * 64,
        )
    destination.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_preparation_uses_original_input_and_cannot_choose_legacy_capture(monkeypatch):
    source = object()
    capture = AsyncMock(return_value="epoch")
    monkeypatch.setattr(prepared, "capture_source_cms_fhir_source_epoch", capture)
    monkeypatch.setattr(
        prepared, "capture_retained_cms_fhir_source_epoch", AsyncMock(side_effect=AssertionError("legacy fallback"))
    )
    admission = prepared.RegistryCMSRawEpochAdmission(None, None, "synthetic", object(), 0, "0/0", "{}")
    capture_id = uuid4()
    assert await admission.capture(capture_id, "retained_owner", ("reader",), source) == "epoch"
    capture.assert_awaited_once_with(
        admission.connection,
        source,
        epoch_id=capture_id,
        owner_role="retained_owner",
        runtime_roles=("reader",),
        copy_admission=admission,
    )


@pytest.mark.asyncio
async def test_input_context_closes_before_retention_returns(monkeypatch):
    events = []
    source = object()

    @asynccontextmanager
    async def issued(directory, source_pin, **coordinates):
        events.append("pin")
        try:
            yield source
        finally:
            events.append("reader_close")

    monkeypatch.setattr(policy, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(pin, "pinned_cms_source_epoch", issued)
    request = SimpleNamespace(
        source_pin=object(), expected_admission_sha256="a" * 64, expected_metadata_sha256="b" * 64
    )
    with pytest.raises(asyncio.CancelledError):
        async with prepared._source_epoch_input(object(), request) as source_input:
            assert source_input is source
            events.append("retention")
            raise asyncio.CancelledError()
    assert events == ["pin", "retention", "reader_close"]


@pytest.mark.asyncio
async def test_source_copy_checks_complete_aggregate_before_receipt(monkeypatch):
    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_error):
            return False

    destination = SimpleNamespace(transaction=Transaction, execute=AsyncMock(), fetchval=AsyncMock(return_value=None))
    source = SimpleNamespace(
        require_active=lambda: None, connection=object(), source_pin=SimpleNamespace(schema_name="synthetic")
    )
    monkeypatch.setattr(epoch, "_origin", lambda _pin: tuple(range(9)))
    monkeypatch.setattr(epoch, "_require_transaction", AsyncMock())
    monkeypatch.setattr(epoch, "_validate_capture_options", lambda *_args: None)
    monkeypatch.setattr(epoch, "_copy_epoch", AsyncMock())
    monkeypatch.setattr(epoch, "_catalog", AsyncMock(return_value=(1, (), "c" * 64)))
    monkeypatch.setattr(epoch, "_content", AsyncMock(side_effect=["d" * 64, "e" * 64]))
    with pytest.raises(epoch.FHIRSourceEpochError):
        await epoch._freeze_epoch(
            destination, source.source_pin, (uuid4(), "retained_owner", ("reader",), "a" * 64, "b" * 64), None, source
        )
    destination.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_new_reader_transaction_does_not_inherit_original_pins():
    source = _input()
    source.connection.fetchval = AsyncMock(return_value="1/2")
    pin._INPUTS[source] = dict(
        task=asyncio.current_task(),
        connection=source.connection,
        identity=(source.source_pin, source.admission_sha256, source.metadata_sha256),
        virtual_xid="1/1",
    )
    try:
        with pytest.raises(epoch.FHIRSourceEpochError):
            await source.verify_transaction()
    finally:
        pin._INPUTS.pop(source)


@pytest.mark.asyncio
async def test_constructed_policy_cannot_authorize_source_custody():
    from process import network_fhir_source_custody as custody

    supplied = policy.GenericSourcePolicy("synthetic", "source_owner", "source_publisher", ("reader",))
    connection = SimpleNamespace(fetch=AsyncMock(), fetchrow=AsyncMock())
    with pytest.raises(ValueError):
        await custody._generic_partitioned_custody(
            connection, SimpleNamespace(schema_name="synthetic"), "source_owner", ("reader",), source_policy=supplied
        )
    connection.fetch.assert_not_awaited()
    connection.fetchrow.assert_not_awaited()
