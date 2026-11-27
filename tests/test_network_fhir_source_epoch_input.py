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
    driver = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value={"isolation": "repeatable read", "readonly": "on"}),
    )
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
async def test_copy_input_expires_and_allows_sequential_tasks():
    source = _input()
    pin._INPUTS[source] = dict(
        identity=(source.source_pin, source.admission_sha256, source.metadata_sha256),
    )
    try:
        assert source.require_active() is None

        async def another_task():
            await source.verify_transaction()

        await asyncio.create_task(another_task())
    finally:
        pin._INPUTS.pop(source)
    with pytest.raises(epoch.FHIRSourceEpochError):
        source.require_active()


@pytest.mark.asyncio
@pytest.mark.parametrize("target_isolation", [None, "read committed"])
async def test_source_capture_requires_actual_target_transaction(monkeypatch, target_isolation):
    source = _input()
    pin._INPUTS[source] = dict(
        identity=(source.source_pin, source.admission_sha256, source.metadata_sha256),
    )
    destination = SimpleNamespace(
        is_in_transaction=lambda: target_isolation is not None,
        fetchval=AsyncMock(return_value=target_isolation),
        execute=AsyncMock(),
    )
    monkeypatch.setattr(epoch, "_origin", lambda _pin: tuple(range(9)))
    try:
        with pytest.raises(ValueError, match="repeatable-read transaction"):
            await epoch.capture_source_cms_fhir_source_epoch(
                destination, source, epoch_id=uuid4(), owner_role="retained_owner", runtime_roles=("reader",)
            )
        source.connection.fetchrow.assert_awaited_once()
        destination.execute.assert_not_awaited()
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
@pytest.mark.parametrize(
    "isolation,readonly,allowed",
    [
        ("repeatable read", "on", True),
        ("serializable", "on", True),
        ("read committed", "on", False),
        ("repeatable read", "off", False),
    ],
)
async def test_named_input_requires_stable_native_readonly_transaction(isolation, readonly, allowed):
    source = _input()
    source.connection.fetchrow.return_value = {"isolation": isolation, "readonly": readonly}
    source.connection.fetchval = AsyncMock(side_effect=AssertionError("unexpected physical identity query"))
    pin._INPUTS[source] = dict(
        identity=(source.source_pin, source.admission_sha256, source.metadata_sha256),
    )
    try:
        if allowed:
            await source.verify_transaction()
        else:
            with pytest.raises(epoch.FHIRSourceEpochError):
                await source.verify_transaction()
        source.connection.fetchval.assert_not_awaited()
    finally:
        pin._INPUTS.pop(source)


@pytest.mark.asyncio
async def test_closed_source_refuses_before_native_transaction_probe():
    source = _input()
    source.connection.is_in_transaction = lambda: False
    pin._INPUTS[source] = dict(identity=(source.source_pin, source.admission_sha256, source.metadata_sha256))
    try:
        with pytest.raises(epoch.FHIRSourceEpochError):
            await source.verify_transaction()
        source.connection.fetchrow.assert_not_awaited()
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


@pytest.mark.asyncio
@pytest.mark.parametrize("observation", ["copied", "missing-header", "empty-binding", "wrong-release", "missing-child"])
async def test_retained_fhir_source_binds_published_partition_names(monkeypatch, observation):
    from process import network_fhir_source_custody as custody
    from tests.test_registry_generic_accepted_capture import _published_binding

    binding = _published_binding()
    binding["auxiliary"] = {"phase": "published", "tables": {}, "sealed_sha256": "a" * 64}
    release = "e" * 64
    identity = binding["identity"]
    identity["metadata"].update(source_ids=["cms-npd"], source_release={"vector_sha256": release})
    header_by_field = {
        "endpoint_id": identity["endpoint_id"],
        "acquisition_root_run_id": identity["acquisition_root_run_id"],
        "candidate_storage_json": binding,
    }
    source_pin = SimpleNamespace(
        schema_name="source_example",
        dataset_id=identity["dataset_id"],
        endpoint_id=identity["endpoint_id"],
        dataset_sha256="f" * 64,
        release_id=release,
    )
    family_rows = []
    for index, (root, table) in enumerate(binding["tables"].items(), 1):
        family_rows.extend(
            [{"relname": root, "oid": 4000 + index}, {"relname": table["name"], "parents": [4000 + index]}]
        )
    if observation == "missing-child":
        family_rows.pop()
    elif observation == "wrong-release":
        source_pin.release_id = "0" * 64
    elif observation == "empty-binding":
        header_by_field["candidate_storage_json"] = {}
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=None if observation == "missing-header" else header_by_field)
    )
    monkeypatch.setattr(custody, "_generic_custody_family", AsyncMock(return_value=family_rows))
    if observation == "copied":
        selected = await custody._generic_partitioned_custody(
            connection, source_pin, "source_owner", ("source_reader",)
        )
        assert selected["binding"] == binding and selected["family"] == family_rows
        assert 'FROM "source_example"."provider_directory_endpoint_dataset"' in connection.fetchrow.call_args.args[0]
        assert connection.fetchrow.call_args.args[1:] == (
            source_pin.dataset_id,
            source_pin.endpoint_id,
            source_pin.dataset_sha256,
        )
    else:
        with pytest.raises(custody.FHIRSourceCustodyError, match="^fhir_source_custody_unavailable$"):
            await custody._generic_partitioned_custody(connection, source_pin, "source_owner", ("source_reader",))


@pytest.mark.asyncio
async def test_reader_pin_does_not_reprove_family_or_control_receipts(monkeypatch):
    from process import provider_directory_generic_source_provision as provision
    from process import provider_directory_generic_source_storage as storage

    source_pin = SimpleNamespace(schema_name="synthetic", endpoint_id="endpoint", dataset_sha256="d" * 64)
    source_policy = SimpleNamespace(schema="synthetic", owner_role="owner", runtime_roles=("reader",))
    publisher = SimpleNamespace(connection=object(), policy=source_policy)
    roots = (storage.RESOURCE, storage.WITNESS, storage.RELATIONSHIP)
    binding_by_field = {
        "identity": {"acquisition_root_run_id": "run"},
        "tables": {root: {"name": root + "_child"} for root in roots},
    }
    header_by_field = dict(
        acquisition_root_run_id="run", content_proof_admission_sha256="a" * 64, publication_metadata_sha256="b" * 64
    )
    coverage, controls, family = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(pin, "_header", AsyncMock(return_value=header_by_field))
    monkeypatch.setattr(
        pin.custody, "_generic_partitioned_custody", AsyncMock(return_value={"binding": binding_by_field})
    )
    monkeypatch.setattr(pin, "capture_source_coverage_control", coverage)
    monkeypatch.setattr(pin, "capture_source_cms_epoch_controls", controls)
    monkeypatch.setattr(provision, "verify_generic_source_family", family)
    monkeypatch.setattr(storage, "_verify_tables", AsyncMock())

    _policy, _header, native, relations = await pin._capture(publisher, source_pin, "a" * 64, "b" * 64)
    driver = SimpleNamespace(execute=AsyncMock())
    await pin._pin(driver, source_policy, native, relations)

    coverage.assert_awaited_once_with(publisher.connection, source_policy)
    controls.assert_awaited_once_with(publisher.connection, source_policy)
    family.assert_not_awaited()
    assert driver.execute.await_args.args[0].startswith("LOCK TABLE ")
