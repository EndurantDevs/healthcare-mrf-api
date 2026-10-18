# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact signed nonprofile plans, aggregate budgets and independent phase authority."""

import contextvars
import copy
import datetime
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_nonprofile_capacity as capacity
from process.provider_directory_cms_preparation import NonprofileAdmissionCheck, OwnedRelation
from tests.provider_directory_cms_capacity_test_support import cms_execution, cms_plan, signed_cms_plan
from tests.test_provider_directory_cms_storage_continuation import _envelope
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME, _trust


def _plan():
    """Bind an exact synthetic full source and native projection to a paired lease."""
    return cms_plan()


def _signed_plan(plan):
    """Sign exact geometry with the existing closed guard and synthetic key."""
    return signed_cms_plan(plan)


def _producer():
    """Construct a real signature-verified producer with bounded fake database reads."""

    @asynccontextmanager
    async def transaction():
        yield

    plan, fence, profile = _plan()
    lease = _signed_plan(plan)
    execution = cms_execution()
    fhir = SimpleNamespace(
        _PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION=contextvars.ContextVar("capacity", default=None),
        PROFILE_EXECUTION_CONTRACT_ID="execution-contract",
        _profile_capacity_preflight_clock=AsyncMock(return_value=VALIDATION_TIME),
        db=SimpleNamespace(
            scalar=AsyncMock(return_value=0), transaction=transaction, _transaction_binding=lambda: None
        ),
        _profile_capacity_expected_execution_identity=lambda _execution: lease.signing_preflight_guard[
            "healthcare_receipt"
        ]["profile_execution_identity"],
    )
    producer = capacity._CapacityProducer(
        fhir, execution, fence, "run_" + "a" * 32, lease, profile, plan, AsyncMock(), "0/10"
    )
    return producer


@pytest.mark.parametrize("source_required", [False, True])
async def test_consumption_establishes_serializable_before_source_identity(monkeypatch, source_required):
    from process import provider_directory_generic_source_policy as policy
    from process import provider_directory_profile_scope_copy as scope

    producer = _producer()
    producer.fhir._schema = lambda: "fixture"
    producer.fhir.db.status = AsyncMock()
    producer.fhir._lock_profile_capacity_preflight_state = AsyncMock(side_effect=RuntimeError("lock reached"))
    entries = []

    @asynccontextmanager
    async def transaction(directory, *, isolation, readonly):
        assert directory is producer.fhir and isolation == "serializable" and readonly is False
        entries.append(isolation)
        yield

    monkeypatch.setattr(scope, "preflight_transaction", transaction)
    monkeypatch.setattr(policy, "is_source_storage_required", lambda: source_required)
    with pytest.raises(RuntimeError, match="lock reached"):
        await producer._consume({})
    assert entries == ["serializable"]
    if source_required:
        producer.fhir.db.status.assert_not_awaited()
    else:
        producer.fhir.db.status.assert_awaited_once_with("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")


def _cutover_producer(monkeypatch):
    """Use real signed witnesses and the existing local WAL checker for one operation."""
    producer = _producer()
    producer.plan = replace(producer.plan, cutover_wal_upper_bound_bytes=50_000)
    producer.lease = _signed_plan(producer.plan)
    producer._assert_consumption = AsyncMock()
    producer._assert_active_profile = AsyncMock()
    producer.fhir._assert_provider_directory_profile_wal_budget = AsyncMock()
    producer.resumed_profile = SimpleNamespace(lease=producer.profile_lease, run_id=producer.run_id)
    producer.preparation_wal_end_bytes = 0
    producer.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(producer.resumed_profile)
    producer.fhir.db.binding = None
    producer.fhir.db._transaction_binding = lambda: producer.fhir.db.binding
    producer.fhir.db.scalar.side_effect = lambda query, **_params: VALIDATION_TIME if "clock_timestamp" in query else 0
    observation_by_field = {
        "temp_limit_bytes": 1024,
        "query_parallel_workers": 0,
        "maintenance_parallel_workers": 0,
        "database_system_identifier": producer.lease.database_system_identifier,
        "database_oid": producer.lease.database_oid,
        "database_name": producer.lease.database_name,
        "data_tablespace_oid": 1663,
        "data_tablespace_name": "pg_default",
        "temp_tablespace_oid": 1663,
        "temp_tablespace_name": "pg_default",
    }
    monkeypatch.setattr(capacity, "_database_observation", AsyncMock(return_value=observation_by_field))
    monkeypatch.setattr(capacity.capacity_runtime, "configured_capacity_lease_trust", _trust)
    monkeypatch.setattr(capacity, "observe_profile_runtime", AsyncMock(return_value={}))
    monkeypatch.setattr(capacity, "assert_capacity_lease_matches_runtime_observation", lambda *args: None)

    async def authority(phase_request):
        assert producer.fhir.db._transaction_binding() is None
        storage = copy.deepcopy(producer.lease.signing_preflight_guard["control_plane_request"]["storage_observation"])
        storage.update(observed_at="2026-07-30T12:00:02Z", issued_at="2026-07-30T12:00:02Z")
        return _envelope(
            {
                **phase_request.binding_by_field,
                "issued_at": "2026-07-30T12:00:02Z",
                "expires_at": "2026-07-30T12:00:32Z",
                "storage_observation": storage,
            }
        )

    producer.fresh_storage_envelope = AsyncMock(side_effect=authority)
    return producer


@pytest.mark.asyncio
async def test_cutover_witness_is_local_and_cannot_cross_sessions_or_retries(monkeypatch):
    """Only a fresh nonce authorizes each operation; local checks retain original bytes."""
    producer = _cutover_producer(monkeypatch)
    request = NonprofileAdmissionCheck("cutover", producer.lease, producer.plan, ())
    original = producer.lease.canonical_lease_json
    async with producer.cutover_operation():
        await producer.check_phase(request)
        witness = producer._cutover_witness
        producer.fhir.db.binding = SimpleNamespace(session=object())
        await producer.assert_cutover(())
        await producer.assert_cutover(None)
        with pytest.raises(RuntimeError, match="cutover_operation_changed"):
            await producer.check_phase(request)
        producer.fhir.db.binding = SimpleNamespace(session=producer.fhir.db.binding.session)
        with pytest.raises(RuntimeError, match="cutover_operation_changed"):
            await producer.assert_cutover(None)
    assert producer._cutover_witness is None and producer._cutover_request is None
    with pytest.raises(RuntimeError, match="cutover_operation_changed"):
        await producer.assert_cutover(None)
    producer.fhir.db.binding = None
    async with producer.cutover_operation():
        await producer.check_phase(request)
        assert producer._cutover_witness.request_nonce != witness.request_nonce
    assert producer.fresh_storage_envelope.await_count == 2
    assert producer.lease.canonical_lease_json == original


def _set_cutover_failure(producer, failure):
    """Change one measured value or authority identity after fresh authorization."""
    if failure in {"expiry", "deadline"}:
        now = producer._cutover_witness.expires_at if failure == "expiry" else producer.profile_lease.max_build_deadline
        producer.fhir.db.scalar.side_effect = lambda query, **_params: now if "clock_timestamp" in query else 0
    elif failure == "wal":
        producer.fhir.db.scalar.side_effect = lambda *_args, **_params: producer.plan.cutover_wal_upper_bound_bytes + 1
    elif failure == "consumption":
        producer._assert_consumption.side_effect = RuntimeError("consumption_changed")
    elif failure == "profile":
        producer.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(object())


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["expiry", "deadline", "wal", "consumption", "relations", "profile"])
async def test_local_cutover_rejects_expiry_overrun_and_identity_drift(monkeypatch, failure):
    """The final check retains paired deadlines, immutable use, owned OIDs and WAL caps."""
    producer = _cutover_producer(monkeypatch)
    request = NonprofileAdmissionCheck("cutover", producer.lease, producer.plan, ())
    async with producer.cutover_operation():
        await producer.check_phase(request)
        producer.fhir.db.binding = SimpleNamespace(session=object())
        _set_cutover_failure(producer, failure)
        relations = (OwnedRelation("synthetic", "replaced", 7, 0, "p"),) if failure == "relations" else None
        with pytest.raises(RuntimeError):
            await producer.assert_cutover(relations)
    assert producer._cutover_witness is None and producer.fresh_storage_envelope.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("data_bytes", [16_666, 16_667])
async def test_logging_reserve_uses_measured_owned_storage_before_rewrite(data_bytes):
    producer = _producer()
    producer.fhir._provider_directory_profile_relation_storage_fingerprint = AsyncMock(
        return_value=SimpleNamespace(relation_oid=1, effective_tablespace_oids=(2,))
    )
    relation = OwnedRelation("synthetic", "profile_scope", 1, data_bytes, "u")
    request = NonprofileAdmissionCheck(
        "pre_logging", producer.lease, producer.plan, (relation,), ((relation.schema, relation.relation),)
    )
    if 3 * data_bytes > producer.plan.logging_wal_upper_bound_bytes:
        with pytest.raises(RuntimeError, match="measured_logging_reserve_exceeded"):
            await producer._assert_physical(request, {"data_tablespace_oid": 2})
        producer.fhir._provider_directory_profile_relation_storage_fingerprint.assert_not_awaited()
    else:
        await producer._assert_physical(request, {"data_tablespace_oid": 2})
        producer.fhir._provider_directory_profile_relation_storage_fingerprint.assert_awaited_once()


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("replaced", [False, True])
async def test_physical_pass_reuses_original_binding_and_checks_every_relation(existing, replaced):
    producer = _producer()
    database, original, events = producer.fhir.db, object(), []
    database.binding = original if existing else None
    database._transaction_binding = lambda: database.binding

    @asynccontextmanager
    async def transaction():
        assert database.binding is None
        database.binding = original
        events.append("opened")
        try:
            yield
        finally:
            database.binding = None
            events.append("closed")

    async def capture(relation, *_args):
        assert database.binding is original
        events.append(relation.relation)
        return SimpleNamespace(
            relation_oid=relation.oid + int(replaced and relation.relation == "second"),
            effective_tablespace_oids=(2,),
        )

    database.transaction = transaction
    producer._capture_relation_layout = AsyncMock(side_effect=capture)
    relations = tuple(
        OwnedRelation("synthetic", name, oid, 100, "p") for oid, name in enumerate(("first", "second"), 1)
    )
    request = NonprofileAdmissionCheck("readiness", producer.lease, producer.plan, relations)
    if replaced:
        with pytest.raises(RuntimeError, match="physical_tablespace_changed"):
            await producer._assert_physical(request, {"data_tablespace_oid": 2})
    else:
        await producer._assert_physical(request, {"data_tablespace_oid": 2})
    assert events == (["first", "second"] if existing else ["opened", "first", "second", "closed"])
    assert database.binding is (original if existing else None)


@pytest.mark.parametrize("changed", [None, "oid", "tablespace"])
async def test_physical_pass_batches_only_scratch_and_keeps_original_backend(monkeypatch, changed):
    from process import provider_directory_cms_native_layout as layout

    producer = _producer()
    database, events = producer.fhir.db, []
    original = object()
    database._transaction_binding = lambda: original
    relations = tuple(
        OwnedRelation("synthetic", name, oid, 100, "p") for oid, name in enumerate(("scratch", "ordinary"), 1)
    )

    @asynccontextmanager
    async def batch(actual, members, *, database_for):
        assert actual is producer.fhir and members == relations and database_for is None
        assert database._transaction_binding() is original
        events.append("before")
        yield {
            ("synthetic", "scratch"): SimpleNamespace(
                relation_oid=1 + int(changed == "oid"), effective_tablespace_oids=(3 if changed == "tablespace" else 2,)
            )
        }
        events.append("after")

    async def ordinary(relation, *_args):
        assert database._transaction_binding() is original
        events.append(relation.relation)
        return SimpleNamespace(relation_oid=relation.oid, effective_tablespace_oids=(2,))

    monkeypatch.setattr(layout, "scratch_layout_batch", batch)
    producer._capture_relation_layout = AsyncMock(side_effect=ordinary)
    request = NonprofileAdmissionCheck("readiness", producer.lease, producer.plan, relations)
    if changed:
        with pytest.raises(RuntimeError, match="physical_tablespace_changed"):
            await producer._assert_physical(request, {"data_tablespace_oid": 2})
        producer._capture_relation_layout.assert_not_awaited()
    else:
        await producer._assert_physical(request, {"data_tablespace_oid": 2})
        assert events == ["before", "ordinary", "after"]
        producer._capture_relation_layout.assert_awaited_once()


@pytest.mark.parametrize("kind", ["raw", "native"])
async def test_scratch_batch_cannot_replace_stronger_retained_policy(monkeypatch, kind):
    from process import provider_directory_cms_native_layout as layout

    producer, policy = _producer(), object()
    relation = OwnedRelation("synthetic", "scratch", 11, 100, "p")
    coordinate = (relation.schema, relation.relation)
    producer._raw_relation_policies = lambda _request: {coordinate: policy} if kind == "raw" else {}
    producer._native_relation_policies = lambda _request: {coordinate: policy} if kind == "native" else {}

    @asynccontextmanager
    async def batch(_fhir, members, **_kwargs):
        assert members == ()
        yield {}

    monkeypatch.setattr(layout, "scratch_layout_batch", batch)
    observed_catalog = ({"relation": {"relation_oid": relation.oid}}, None)
    native_capture = AsyncMock(return_value={(relation.schema, relation.relation, relation.oid): observed_catalog})
    monkeypatch.setattr(layout, "observe_native_catalog_batch", native_capture)
    producer._capture_relation_layout = AsyncMock(
        return_value=SimpleNamespace(relation_oid=11, effective_tablespace_oids=(2,))
    )
    request = NonprofileAdmissionCheck("readiness", producer.lease, producer.plan, (relation,))
    await producer._assert_physical(request, {"data_tablespace_oid": 2})
    assert producer._capture_relation_layout.await_args.args == (
        relation,
        policy if kind == "raw" else None,
        policy if kind == "native" else None,
    )
    if kind == "native":
        native_capture.assert_awaited_once_with(producer.fhir, [relation], database=producer.fhir.db)
        assert producer._capture_relation_layout.await_args.kwargs == {"observed_catalog": observed_catalog}
    else:
        native_capture.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("native_address_input_hash", ""),
        ("native_address_targets", ()),
        ("paired_profile_lease_digest", ""),
        ("temp_file_limit_bytes_per_backend", 100_000),
        ("worker_count", True),
        ("logging_wal_upper_bound_bytes", 100_000),
        ("reservation_bytes", (("data", 1000), ("temp", 1000), ("wal", 1000))),
    ],
)
def test_incomplete_or_unbounded_signed_plan_is_rejected(field, value):
    plan, _fence, _profile = _plan()
    capacity._validate_plan(plan)
    with pytest.raises(RuntimeError, match="plan_"):
        capacity._validate_plan(replace(plan, **{field: value}))


def test_scope_and_paired_lease_are_in_signed_geometry():
    producer = _producer()
    capacity._assert_signed_plan(producer.fhir, producer.execution, producer.plan, producer.lease)
    with pytest.raises(RuntimeError, match="signed_plan_changed"):
        capacity._assert_signed_plan(
            producer.fhir,
            producer.execution,
            replace(producer.plan, paired_profile_lease_digest="aa" * 32),
            producer.lease,
        )
    with pytest.raises(RuntimeError, match="signed_plan_changed"):
        capacity._assert_signed_plan(
            producer.fhir,
            producer.execution,
            replace(producer.plan, native_address_input_hash="aa" * 32),
            producer.lease,
        )


def test_quiescence_reuses_exact_existing_profile_contract_predicate():
    producer = _producer()
    receipt = producer.lease.signing_preflight_guard["healthcare_receipt"]
    params_by_field = capacity._quiescence_parameters(producer.fhir, producer, receipt, VALIDATION_TIME)
    assert json.loads(params_by_field["profile_params"]) == {
        "provider_directory_profile_contract_id": "execution-contract",
        "publish_artifacts_only": True,
        "publish_artifacts_targets": ["profile"],
    }
    assert (
        params_by_field["paired_request_sha256"]
        == producer.profile_lease.signing_preflight_guard["healthcare_receipt"]["request_sha256"]
    )


def test_logging_reservation_is_spent_once_and_sealed_before_profile():
    producer = _producer()
    assert producer._remaining_logging_wal("readiness", 30) == 49_970
    assert producer._remaining_logging_wal("pre_logging", 100) == 49_900
    assert producer._remaining_logging_wal("pre_logging", 30_100) == 19_900
    producer.preparation_wal_end_bytes = 40_100
    assert producer._remaining_logging_wal("cutover", 90_000) == 0
    producer.preparation_wal_end_bytes = 50_101
    with pytest.raises(RuntimeError, match="logging_wal_budget_exceeded"):
        producer._remaining_logging_wal("cutover", 90_000)


def test_pair_reuse_and_aggregate_colocated_free_space_fail_closed():
    producer = _producer()
    with pytest.raises(RuntimeError, match="profile_reservation_reused"):
        capacity._assert_paired_reservation(producer.lease, producer.lease, 1000)
    volumes = tuple(
        replace(volume, available_bytes=1, available_after_all_reservations_bytes=1)
        for volume in producer.lease.volumes
    )
    with pytest.raises(RuntimeError, match="aggregate_remaining_capacity_too_small"):
        capacity._assert_paired_reservation(replace(producer.lease, volumes=volumes), producer.profile_lease, 1000)


def test_paired_capacity_keeps_independent_reservations_on_same_storage():
    producer = _producer()
    assert producer.lease.reservation_bytes_by_storage_class == dict(producer.plan.reservation_bytes)
    assert (
        producer.lease.reservation_bytes_by_storage_class != producer.profile_lease.reservation_bytes_by_storage_class
    )
    capacity._assert_paired_reservation(producer.lease, producer.profile_lease, 1000)
    volumes = tuple(replace(volume, volume_digest="99" * 32) for volume in producer.lease.volumes)
    with pytest.raises(RuntimeError, match="paired_storage_changed"):
        capacity._assert_paired_reservation(replace(producer.lease, volumes=volumes), producer.profile_lease, 1000)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["pre_scratch", "pre_logging"])
async def test_phase_rejects_unsigned_transport_and_retains_original_bytes(monkeypatch, phase):
    producer = _producer()
    original = producer.lease.canonical_lease_json
    producer._consume = AsyncMock()
    producer._assert_consumption = AsyncMock()
    producer._assert_physical = AsyncMock()
    observation_by_field = {
        "temp_limit_bytes": 1024,
        "query_parallel_workers": 0,
        "maintenance_parallel_workers": 0,
        "database_system_identifier": "7527713908662902214",
        "database_oid": 16401,
        "database_name": "healthporta_test",
        "data_tablespace_oid": 1663,
        "data_tablespace_name": "pg_default",
        "temp_tablespace_oid": 1663,
        "temp_tablespace_name": "pg_default",
    }
    monkeypatch.setattr(capacity, "_database_observation", AsyncMock(return_value=observation_by_field))
    monkeypatch.setattr(capacity.capacity_runtime, "configured_capacity_lease_trust", _trust)
    monkeypatch.setattr(capacity, "observe_profile_runtime", AsyncMock(return_value={}))
    monkeypatch.setattr(capacity, "assert_capacity_lease_matches_runtime_observation", lambda *args: None)
    producer.fresh_storage_envelope.return_value = {"available_bytes": 999_999}
    logging_relations = (("synthetic", "stage"),) if phase == "pre_logging" else ()
    request = NonprofileAdmissionCheck(phase, producer.lease, producer.plan, (), logging_relations)
    with pytest.raises(RuntimeError, match="envelope_invalid"):
        await producer.check_phase(request)
    producer._consume.assert_not_awaited()

    async def authority(phase_request):
        """Return one independently signed witness for the fresh requested nonce."""
        storage = copy.deepcopy(producer.lease.signing_preflight_guard["control_plane_request"]["storage_observation"])
        storage.update(observed_at="2026-07-30T12:00:02Z", issued_at="2026-07-30T12:00:02Z")
        return _envelope(
            {
                **phase_request.binding_by_field,
                "issued_at": "2026-07-30T12:00:02Z",
                "expires_at": "2026-07-30T12:00:32Z",
                "storage_observation": storage,
            }
        )

    producer.fresh_storage_envelope = authority
    receipt = await producer.check_phase(request)
    assert receipt.lease_digest == producer.lease.lease_digest and len(producer.continuation_digests) == 1
    assert receipt.logging_relations == logging_relations
    assert producer.lease.canonical_lease_json == original
    if phase == "pre_scratch":
        producer._consume.assert_awaited_once()
        producer._assert_consumption.assert_not_awaited()
    else:
        producer._consume.assert_not_awaited()
        producer._assert_consumption.assert_awaited_once()


@pytest.mark.asyncio
async def test_missing_phase_authority_stops_before_database_access():
    plan, fence, _profile = _plan()
    with pytest.raises(RuntimeError, match="authority_transport_required"):
        await capacity.produce_nonprofile_admission(
            object(),
            object(),
            fence,
            run_id="run_" + "a" * 32,
            assigned_envelope={},
            profile_envelope={},
            signed_plan=plan,
            fresh_storage_envelope=None,
        )
