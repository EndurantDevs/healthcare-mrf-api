# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Component dispatch and refusal checks; no native actor/profile acceptance."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

from process import network_fhir_source_epoch_pin as epoch_pin
from process import provider_directory_cms_nonprofile_observation as observation
from process import provider_directory_cms_raw_layout as raw_layout
from process.network_fhir_source_epoch import _TABLES
from process.network_fhir_source_epoch_pin import PinnedCMSFHIRSourceEpochInput
from process.provider_directory_cms_preparation import OwnedRelation
from tests.test_provider_directory_cms_nonprofile_capacity import _cutover_producer, _producer
from tests.test_provider_directory_cms_preparation import _admission

fhir = importlib.import_module("process.provider_directory_fhir")


def test_legacy_catalog_context_keeps_original_module():
    assert observation.catalog_observation(fhir, None) is fhir


@pytest.mark.asyncio
@pytest.mark.parametrize("source_required", [False, True])
async def test_missing_source_pin_is_only_valid_for_legacy_mode(monkeypatch, source_required):
    from process import provider_directory_generic_source_policy as policy

    monkeypatch.setattr(policy, "is_source_storage_required", lambda: source_required)
    if source_required:
        with pytest.raises(RuntimeError, match="observation_changed"):
            async with observation.retained_nonprofile_observations(None, None, None, None):
                raise AssertionError("missing pin entered SOURCE mode")
    else:
        async with observation.retained_nonprofile_observations(None, None, None, None):
            assert observation.active_observations() is None


@pytest.mark.asyncio
async def test_explicit_relation_catalog_does_not_use_ambient_database(monkeypatch):
    ambient = SimpleNamespace(all=AsyncMock(side_effect=AssertionError("ambient query")))
    database = SimpleNamespace(all=AsyncMock(return_value=[{"attribute": "native"}]))
    monkeypatch.setattr(fhir, "db", ambient)
    rows = await fhir._profile_capacity_relation_catalog([123], database=database)
    assert rows == tuple([[{"attribute": "native"}]] * 4)
    assert [call.args[0] for call in database.all.call_args_list] == [
        fhir._PROFILE_CAPACITY_ATTRIBUTE_SQL,
        fhir._PROFILE_CAPACITY_INDEX_SQL,
        fhir._PROFILE_CAPACITY_CONSTRAINT_SQL,
        fhir._PROFILE_CAPACITY_TRIGGER_SQL,
    ]
    assert all(call.kwargs == {"relation_oids": [123]} for call in database.all.call_args_list)
    ambient.all.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_relation_row_preserves_shape_denial(monkeypatch):
    database = SimpleNamespace(first=AsyncMock(return_value={"relkind": "v", "toast_oid": 0}))
    ambient = SimpleNamespace(first=AsyncMock(side_effect=AssertionError("ambient query")))
    monkeypatch.setattr(fhir, "db", ambient)
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="storage_shape_unsupported"):
        await fhir._profile_capacity_relation_row(123, "p", 0, database=database)
    database.first.assert_awaited_once_with(fhir._PROFILE_CAPACITY_RELATION_SQL, relation_oid=123)
    ambient.first.assert_not_awaited()


@pytest.mark.asyncio
async def test_catalog_facade_forwards_only_existing_constructors():
    directory = SimpleNamespace(
        _profile_capacity_relation_row=AsyncMock(),
        _profile_capacity_relation_catalog=AsyncMock(),
        _profile_bucket_index_row=AsyncMock(),
        _provider_directory_profile_relation_storage_fingerprint=AsyncMock(),
    )
    database = object()
    reader = observation.catalog_observation(directory, database)
    await reader._profile_capacity_relation_row(1, "p", 0)
    await reader._profile_capacity_relation_catalog([1])
    await reader._profile_bucket_index_row("s", "t", "i", "r")
    await reader._provider_directory_profile_relation_storage_fingerprint(1, expected_persistence="p")
    assert reader.db is database
    for constructor in vars(directory).values():
        assert constructor.await_args.kwargs["database"] is database


def test_observer_routing_keeps_source_reader_controls_and_ret_separate():
    source_input, controls = object(), object()
    scope = SimpleNamespace(
        require_active=Mock(),
        source_input=source_input,
        source_relations={("source", "address"): 1, ("source", "private"): 2, ("source", "address_other"): 3},
        address_relations={("source", "address"): 1, ("source", "address_other"): 3},
        retained_tables={"retained": ("copy",)},
        retained_relations={},
    )
    readers = observation._Observers(scope, controls)
    pinned = readers.database_for(OwnedRelation("source", "address", 1, 4, "p"))
    assert isinstance(pinned, observation._ReaderCatalog) and pinned.source_input is source_input
    assert readers.database_for(OwnedRelation("source", "address_other", 3, 4, "p")) is pinned
    assert readers.database_for(OwnedRelation("source", "private", 2, 8, "p")) is controls
    assert readers.database_for(OwnedRelation("retained", "copy", 3, 16, "p")) is None
    for relation in (OwnedRelation("source", "address", 99, 0, "p"), OwnedRelation("retained", "foreign", 4, 0, "p")):
        with pytest.raises(RuntimeError, match="observation_changed"):
            readers.database_for(relation)


@pytest.mark.asyncio
async def test_measure_keeps_complete_aggregate_and_rejects_oid_substitution():
    admission = _admission()
    admission._started = True
    admission._relations = {
        ("source", "closed"): 11,
        ("source", "closed_other"): 12,
        ("retained", "copy"): 22,
        ("retained", "copy_other"): 23,
    }
    source_database = SimpleNamespace(
        all=AsyncMock(
            return_value=[
                {"relation_ref": "source.closed_other", "oid": 12, "total_bytes": 64, "persistence": "p"},
                {"relation_ref": "source.closed", "oid": 11, "total_bytes": 128, "persistence": "p"},
            ]
        )
    )
    target_database = SimpleNamespace(
        all=AsyncMock(
            return_value=[
                {"relation_ref": "retained.copy_other", "oid": 23, "total_bytes": 32, "persistence": "p"},
                {"relation_ref": "retained.copy", "oid": 22, "total_bytes": 256, "persistence": "p"},
            ]
        )
    )
    readers = SimpleNamespace(
        database_for=lambda relation: source_database if relation.schema == "source" else None, record=Mock()
    )
    directory = SimpleNamespace(
        db=target_database,
        _unscoped_qt=lambda schema, name: f"{schema}.{name}",
        _pagination_checkpoint_row_mapping=lambda catalog_record: catalog_record,
    )
    measured_relations = await admission._measure(directory, "retained", (), observers=readers)
    assert [(relation.schema, relation.relation, relation.oid) for relation in measured_relations] == [
        ("retained", "copy", 22),
        ("retained", "copy_other", 23),
        ("source", "closed", 11),
        ("source", "closed_other", 12),
    ]
    assert sum(relation.total_bytes for relation in measured_relations) == 480
    assert source_database.all.await_count == target_database.all.await_count == 1
    assert source_database.all.await_args.kwargs == {"relation_refs": ["source.closed", "source.closed_other"]}
    assert target_database.all.await_args.kwargs == {"relation_refs": ["retained.copy", "retained.copy_other"]}
    assert readers.record.call_count == 4
    source_database.all.return_value[0]["oid"] = 99
    with pytest.raises(RuntimeError, match="relation_changed"):
        await admission._measure(directory, "retained", (), observers=readers)


@pytest.mark.asyncio
async def test_raw_columns_reject_constructed_epoch_input_before_catalog_io():
    source_input = object.__new__(PinnedCMSFHIRSourceEpochInput)
    with pytest.raises(ValueError, match="fhir_source_epoch_unavailable"):
        await observation.pinned_raw_attributes(SimpleNamespace(), source_input, {}, _TABLES[1])


@pytest.mark.asyncio
async def test_raw_columns_use_issued_input_branch_without_parent_lookup(monkeypatch):
    source_attribute_by_field = {"relation_oid": 1, "attname": "id", "atttypid": 25, "attnum": 1}
    target_attribute_by_field = {**source_attribute_by_field, "relation_oid": 2, **raw_layout._UNCOPIED_FIELDS}
    captured = AsyncMock(return_value=(1, [source_attribute_by_field]))
    monkeypatch.setattr(observation, "pinned_raw_attributes", captured)
    directory = SimpleNamespace(db=SimpleNamespace(scalar=AsyncMock(side_effect=AssertionError("parent lookup"))))
    relation = OwnedRelation("retained", "provider_directory_resource_raw", 2, 0, "p")
    source_input = object()
    await raw_layout._require_source_columns(directory, relation, {}, [target_attribute_by_field], source_input)
    captured.assert_awaited_once_with(directory, source_input, {}, relation.relation)
    directory.db.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_phase_preserves_ret_backend_check_and_routes_fresh_controls(monkeypatch):
    producer = _cutover_producer(monkeypatch)
    producer._assert_physical = AsyncMock()
    controls = object()
    readers = SimpleNamespace(controls=controls)
    from process.provider_directory_cms_preparation import NonprofileAdmissionCheck

    request = NonprofileAdmissionCheck("readiness", producer.lease, producer.plan, ())
    receipt = await producer._check_phase(request, observers=readers)
    assert receipt.relations == request.relations
    producer._assert_consumption.assert_awaited_once_with(database=controls)
    from process import provider_directory_cms_nonprofile_capacity as capacity

    capacity.observe_profile_runtime.assert_awaited_once_with(controls)
    assert capacity._database_observation.await_args.args[0] is producer.fhir
    assert producer._assert_physical.await_args.kwargs == {"observers": readers}


def test_unissued_observation_constructor_cannot_authorize_io():
    scope = object.__new__(observation._RetainedObservations)
    token = observation._ACTIVE.set(scope)
    try:
        with pytest.raises(RuntimeError, match="observation_changed"):
            scope.require_active()
    finally:
        observation._ACTIVE.reset(token)


@pytest.mark.parametrize("field", ["expected_admission_sha256", "expected_metadata_sha256"])
def test_same_origin_still_requires_both_exact_signed_seals(field):
    from process.network_fhir_source_epoch import _ORIGIN_FIELDS

    pin_by_field = dict.fromkeys(_ORIGIN_FIELDS, "synthetic")
    source_input = SimpleNamespace(
        source_pin=SimpleNamespace(**pin_by_field), admission_sha256="aa" * 32, metadata_sha256="bb" * 32
    )
    policy_by_field = dict(
        source_pin=pin_by_field,
        expected_admission_sha256=source_input.admission_sha256,
        expected_metadata_sha256=source_input.metadata_sha256,
    )
    observation._require_epoch_policy(source_input, policy_by_field)
    policy_by_field[field] = "cc" * 32
    with pytest.raises(RuntimeError, match="observation_changed"):
        observation._require_epoch_policy(source_input, policy_by_field)


@pytest.mark.parametrize(
    "coordinate,changed",
    [
        (("source", "closed"), None),
        (("source", "closed"), 99),
        (("retained", "copy"), None),
        (("retained", "copy"), 99),
    ],
)
def test_budget_inventory_cannot_drop_or_replace_an_observed_member(coordinate, changed):
    scope = object.__new__(observation._RetainedObservations)
    scope.source_relations = {("source", "closed"): 1}
    scope.retained_relations = {("retained", "copy"): 2}
    scope.admission = SimpleNamespace(_relations={**scope.source_relations, **scope.retained_relations})
    scope.require_inventory()
    if changed is None:
        del scope.admission._relations[coordinate]
    else:
        scope.admission._relations[coordinate] = changed
    with pytest.raises(RuntimeError, match="observation_changed"):
        scope.require_inventory()


@pytest.mark.asyncio
async def test_wal_meter_stays_on_ret_and_paired_consumption_uses_controls():
    from process.provider_directory_cms_preparation import NonprofileAdmissionCheck

    producer = _producer()
    profile = SimpleNamespace(run_id=producer.run_id, lease=producer.profile_lease)
    producer.paused_profile = SimpleNamespace(admission=profile, spent_wal_bytes=0)
    producer.fhir._schema = lambda: "source"
    producer.fhir._unscoped_qt = lambda schema, name: f"{schema}.{name}"
    producer.fhir.ProviderDirectoryProfileCapacityLeaseConsumption = (
        fhir.ProviderDirectoryProfileCapacityLeaseConsumption
    )
    producer.fhir._pagination_checkpoint_row_mapping = lambda row: row
    controls = SimpleNamespace(
        first=AsyncMock(
            return_value={
                "attestation_id": producer.profile_lease.attestation_id,
                "lease_digest": producer.profile_lease.lease_digest,
                "selection_proof_id": producer.plan.selection_proof_id,
            }
        )
    )
    request = NonprofileAdmissionCheck("readiness", producer.lease, producer.plan, ())
    await producer._assert_wal_budget(request, controls=controls)
    assert "pg_wal_lsn_diff" in producer.fhir.db.scalar.await_args.args[0]
    assert controls.first.await_args.kwargs == {"run_id": producer.run_id}
    controls.first.return_value["lease_digest"] = "changed"
    with pytest.raises(RuntimeError, match="active_profile_consumption_changed"):
        await producer._assert_wal_budget(request, controls=controls)


@pytest.mark.asyncio
async def test_source_observer_opens_after_continuation_response(monkeypatch):
    from process.provider_directory_cms_preparation import NonprofileAdmissionCheck

    producer = _cutover_producer(monkeypatch)
    producer.resumed_profile = None
    producer._assert_physical = AsyncMock()
    entered_phases = []
    authority = producer.fresh_storage_envelope.side_effect

    async def response(request):
        assert entered_phases == []
        return await authority(request)

    @asynccontextmanager
    async def observers():
        entered_phases.append("observer")
        yield SimpleNamespace(controls=object())
        entered_phases.append("closed")

    scope = SimpleNamespace(verify_transaction=AsyncMock(), observers=observers)
    monkeypatch.setattr(observation, "active_observations", lambda **_kwargs: scope)
    producer.fresh_storage_envelope.side_effect = response
    request = NonprofileAdmissionCheck("readiness", producer.lease, producer.plan, ())
    await producer.check_phase(request)
    assert entered_phases == ["observer", "closed"]


@pytest.mark.asyncio
async def test_aggregate_budget_denial_is_not_skipped_for_routed_observations():
    from process.provider_directory_cms_preparation import NonprofileAdmissionCheck

    producer = _producer()
    producer.plan = replace(producer.plan, reservation_bytes=(("data", 1), ("temp", 1), ("wal", 1)))
    request = NonprofileAdmissionCheck(
        "readiness", producer.lease, producer.plan, (OwnedRelation("source", "closed", 1, 2, "p"),)
    )
    with pytest.raises(RuntimeError, match="data_budget_exceeded"):
        await producer._assert_physical(request, {}, observers=object())


@pytest.mark.asyncio
async def test_retention_cannot_restart_admission_or_enter_resumed_profile(monkeypatch):
    producer = _producer()
    from process.provider_directory_cms_preparation import NonprofileAdmissionCheck

    @asynccontextmanager
    async def forbidden():
        raise AssertionError("observer opened before phase refusal")
        yield

    monkeypatch.setattr(observation, "active_observations", lambda **_kwargs: SimpleNamespace(observers=forbidden))
    request = NonprofileAdmissionCheck("pre_scratch", producer.lease, producer.plan, ())
    with pytest.raises(RuntimeError, match="retention_phase_changed"):
        await producer.check_phase(request)
    producer.resumed_profile = object()
    request = NonprofileAdmissionCheck("readiness", producer.lease, producer.plan, ())
    with pytest.raises(RuntimeError, match="retention_phase_changed"):
        await producer.check_phase(request)


@asynccontextmanager
async def _raw_attribute_input(monkeypatch, *, attribute_rows=None, change=None, oid=17):
    """Model an issued input's catalog transport; this is not native custody acceptance."""
    pin_by_field = dict.fromkeys(epoch_pin.epoch._ORIGIN_FIELDS, "synthetic")
    driver = SimpleNamespace(is_in_transaction=lambda: True, fetchval=AsyncMock(return_value="1/1"))
    source_input = PinnedCMSFHIRSourceEpochInput(SimpleNamespace(**pin_by_field), "aa" * 32, "bb" * 32, driver)
    statements, parameters = [], []
    attributes = (
        [
            {"relation_oid": 17, "attnum": 1, "attname": "identifier", "atttypid": 25, "attnotnull": True},
            {"relation_oid": 17, "attnum": 4, "attname": "metadata", "atttypid": 3802, "attnotnull": False},
        ]
        if attribute_rows is None
        else attribute_rows
    )

    async def prepare(statement):
        statements.append(statement)
        catalog_rows = [{"oid": oid}] if "FROM pg_class WHERE oid=to_regclass" in statement else attributes
        if not ("FROM pg_class WHERE oid=to_regclass" in statement or "FROM pg_attribute AS attribute" in statement):
            catalog_rows = []

        async def fetch(*args):
            parameters.append(args)
            if change is not None:
                change(driver, statement)
            return catalog_rows

        return SimpleNamespace(get_attributes=lambda: (), fetch=fetch)

    driver.prepare = AsyncMock(side_effect=prepare)
    epoch_pin._INPUTS[source_input] = dict(
        task=asyncio.current_task(),
        connection=driver,
        identity=(source_input.source_pin, source_input.admission_sha256, source_input.metadata_sha256),
        virtual_xid="1/1",
        relations={_TABLES[1]: '"synthetic"."closed_raw"'},
    )
    policy_by_field = dict(
        source_pin=pin_by_field,
        expected_admission_sha256=source_input.admission_sha256,
        expected_metadata_sha256=source_input.metadata_sha256,
    )
    ambient = AsyncMock(side_effect=AssertionError("ambient catalog query"))
    monkeypatch.setattr(fhir, "db", SimpleNamespace(all=ambient, scalar=ambient))
    try:
        yield SimpleNamespace(
            source_input=source_input,
            policy=policy_by_field,
            driver=driver,
            attributes=attributes,
            statements=statements,
            parameters=parameters,
            ambient=ambient,
        )
    finally:
        epoch_pin._INPUTS.pop(source_input, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("empty", [False, True])
async def test_pinned_raw_attributes_read_only_exact_attribute_catalog(monkeypatch, empty):
    async with _raw_attribute_input(monkeypatch, attribute_rows=[] if empty else None) as case:
        oid, attributes = await observation.pinned_raw_attributes(fhir, case.source_input, case.policy, _TABLES[1])
        assert oid == 17 and attributes == case.attributes
        assert len(case.statements) == 2
        expected = (
            text(fhir._PROFILE_CAPACITY_ATTRIBUTE_SQL)
            .bindparams(relation_oids=[17])
            .compile(dialect=PGDialect_asyncpg())
        )
        assert case.statements[1] == str(expected)
        assert case.parameters == [('"synthetic"."closed_raw"',), ([17],)]
        assert case.driver.fetchval.await_count == 4
        case.ambient.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["virtual_xid", "transaction", "connection"])
async def test_pinned_raw_attributes_refuse_custody_change_after_read(monkeypatch, field):
    def change(driver, statement):
        if "FROM pg_attribute AS attribute" not in statement:
            return
        if field == "virtual_xid":
            driver.fetchval.return_value = "1/2"
        elif field == "transaction":
            driver.is_in_transaction = lambda: False
        else:
            epoch_pin._INPUTS[case.source_input]["connection"] = object()

    async with _raw_attribute_input(monkeypatch, change=change) as case:
        with pytest.raises(epoch_pin.epoch.FHIRSourceEpochError):
            await observation.pinned_raw_attributes(fhir, case.source_input, case.policy, _TABLES[1])
        assert len(case.statements) == 2
        case.ambient.assert_not_awaited()


@pytest.mark.asyncio
async def test_pinned_raw_attributes_refuse_changed_policy_before_catalog_read(monkeypatch):
    async with _raw_attribute_input(monkeypatch) as case:
        case.policy["expected_admission_sha256"] = "cc" * 32
        with pytest.raises(RuntimeError, match="observation_changed"):
            await observation.pinned_raw_attributes(fhir, case.source_input, case.policy, _TABLES[1])
        assert case.statements == []
        case.ambient.assert_not_awaited()


@pytest.mark.asyncio
async def test_pinned_raw_attributes_refuse_child_task_before_catalog_read(monkeypatch):
    async with _raw_attribute_input(monkeypatch) as case:
        with pytest.raises(epoch_pin.epoch.FHIRSourceEpochError):
            await asyncio.create_task(
                observation.pinned_raw_attributes(fhir, case.source_input, case.policy, _TABLES[1])
            )
        assert case.statements == []
        case.ambient.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("oid", [None, True, 0, -1])
async def test_pinned_raw_attributes_refuse_invalid_oid_before_attribute_read(monkeypatch, oid):
    async with _raw_attribute_input(monkeypatch, oid=oid) as case:
        with pytest.raises(RuntimeError, match="observation_changed"):
            await observation.pinned_raw_attributes(fhir, case.source_input, case.policy, _TABLES[1])
        assert len(case.statements) == 1
        case.ambient.assert_not_awaited()
