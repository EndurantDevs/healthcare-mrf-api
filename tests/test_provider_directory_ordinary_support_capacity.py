# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete ordinary support/key retention and conservative common-cut reservations."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_ordinary_retention_capacity as retention
from process import provider_directory_ordinary_scope_retention as scopes
from process import provider_directory_serving_capacity_contract as contract
from process import provider_directory_serving_capacity_signing as signing
from process.provider_directory_cms_preparation import NonprofileAdmission, NonprofileAdmissionReceipt, OwnedRelation
from tests.provider_directory_serving_capacity_signing_test_support import serving_case, verify_serving_envelope
from tests.test_provider_directory_ordinary_retention_capacity import _captured_owner, _measured_request


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _scope_layout(kind):
    relation = OwnedRelation("fixture", "issued_" + kind, 40, 8192, "u")
    attributes = [
        dict(
            relation_oid=40,
            attnum=ordinal,
            attname=name,
            atttypid=type_oid,
            default_expression="",
            attnotnull=kind == "npi",
        )
        for ordinal, (name, type_oid) in enumerate(scopes._COLUMNS[kind], 1)
    ]
    number_by_name = {column["attname"]: column["attnum"] for column in attributes}
    keys = scopes._GROUP_INDEX if kind == "group" else tuple(number_by_name)
    index_by_field = dict(
        relation_oid=40,
        index_am="btree",
        index_predicate="",
        index_expressions="",
        indkey=" ".join(str(number_by_name[name]) for name in keys),
        indisunique=kind != "group",
        indisprimary=kind == "npi",
    )
    return relation, attributes, [index_by_field]


@pytest.mark.parametrize("kind", scopes.KINDS)
def test_actual_issued_scope_keys_preserve_nullable_group_without_fabricated_primary_key(kind):
    relation, attributes, indexes = _scope_layout(kind)
    scopes.require_scope_layout(kind, relation, attributes, indexes)
    if kind == "group":
        assert not indexes[0]["indisunique"] and all(not column["attnotnull"] for column in attributes)
    elif kind == "location":
        assert indexes[0]["indisunique"] and not attributes[0]["attnotnull"]


@pytest.mark.parametrize("kind", scopes.KINDS)
@pytest.mark.parametrize("changed", ["type", "key", "unique", "default"])
def test_scope_layout_refuses_semantic_changes(kind, changed):
    relation, attributes, indexes = _scope_layout(kind)
    if changed == "type":
        attributes[0]["atttypid"] = 25
    elif changed == "key":
        indexes[0]["indkey"] = "99"
    elif changed == "unique":
        indexes[0]["indisunique"] = not indexes[0]["indisunique"]
    else:
        attributes[0]["default_expression"] = "0"
    with pytest.raises(RuntimeError, match="retained_scope_changed"):
        scopes.require_scope_layout(kind, relation, attributes, indexes)


def test_all_ten_row_families_and_three_scope_copies_share_one_quota(monkeypatch):
    owner, _admission = _captured_owner(monkeypatch, SimpleNamespace(db=object()), "ab" * 32)
    request = _measured_request(owner)
    assert len(retention.retained_relations(owner, request)) == 13
    assert retention.allocated_data_bytes(owner, request) == 1300
    oversized_scope = replace(
        request.relations[-1], total_bytes=owner.captured_inputs.scope_payload["max_retention_data_bytes"]
    )
    with pytest.raises(RuntimeError, match="retention_changed"):
        retention.allocated_data_bytes(owner, replace(request, relations=(*request.relations[:-1], oversized_scope)))
    missing_family = replace(request, ordinary_relations=request.ordinary_relations[1:])
    with pytest.raises(RuntimeError, match="retention_changed"):
        retention.retained_relations(owner, missing_family)


@pytest.mark.parametrize("initial", [False, True])
def test_real_signed_common_cut_geometry_accounts_full_paired_profile_wal(initial):
    case = serving_case(initial_mode=initial)
    admission = case.request.serving_nonprofile_admission
    limits, scope = admission["limits"], admission["ordinary_scope"]
    pair_wal = case.profile.signing_preflight_guard["healthcare_receipt"][
        "required_reservation_bytes_by_storage_class"
    ]["wal"]
    assert (
        dict(case.plan.reservation_bytes)["wal"]
        == limits["max_wal_bytes"] + scope["max_retention_wal_bytes"] + scope["max_commit_wal_bytes"] + pair_wal
    )
    assert (
        case.plan.cutover_wal_upper_bound_bytes
        == limits["max_wal_bytes"] - limits["max_wal_bytes"] // 2 + scope["max_commit_wal_bytes"] + pair_wal
    )
    assert case.plan.native_address_targets == contract.NATIVE_ADDRESS_TARGETS
    signing.assert_serving_geometry_matches_request(case.plan, case.request)
    assert verify_serving_envelope(case.envelope).capacity_geometry_hash == case.plan.capacity_geometry_hash


def test_omitted_paired_profile_interval_cannot_satisfy_signed_request():
    case = serving_case()
    pair_wal = contract.paired_profile_wal_bytes(case.profile.signing_preflight_guard["healthcare_receipt"])
    underfunded = replace(case.plan, cutover_wal_upper_bound_bytes=case.plan.cutover_wal_upper_bound_bytes - pair_wal)
    with pytest.raises(ValueError, match="geometry_request_changed"):
        signing.assert_serving_geometry_matches_request(underfunded, case.request)


def test_derived_common_cut_reservation_refuses_bigint_overflow():
    case = serving_case()
    base = deepcopy(case.plan.payload)
    scope = base.pop("ordinary_scope")
    base.update(contract_id=contract.BASE_GEOMETRY, admission_purpose="cms_nonprofile")
    with pytest.raises(ValueError, match="reservations"):
        contract.extend_plan_values(base, scope, paired_profile_wal=2**63 - 1)


async def test_wrapper_permits_only_signed_resident_allowance_before_exact_producer_checks(monkeypatch):
    case = serving_case(initial_mode=True)
    scope = case.plan.payload["ordinary_scope"]
    scope["resident_retention_data_bytes"] = 8192
    plan = replace(case.plan, ordinary_scope_json=contract.canonical(scope))
    verified = verify_serving_envelope(case.envelope)
    admission = NonprofileAdmission(verified, plan, AsyncMock())
    monkeypatch.setattr(admission, "_assert_lease", lambda: None)
    ceiling = dict(plan.reservation_bytes)["data"]
    relation = OwnedRelation("fixture", "provider_directory_profile", 1, ceiling + 8192, "p")
    expected = NonprofileAdmissionReceipt(
        "readiness", verified.lease_digest, verified.reservation_id, plan.capacity_geometry_hash, (relation,)
    )
    admission.check_phase.return_value = expected
    await admission._check("readiness", (relation,))
    admission.check_phase.assert_awaited_once()
    with pytest.raises(RuntimeError, match="data_budget_exceeded"):
        await admission._check("readiness", (replace(relation, total_bytes=relation.total_bytes + 1),))
    assert admission.check_phase.await_count == 1


@pytest.mark.parametrize("changed", [None, "phase", "quota", "producer"])
async def test_bounded_copy_growth_reuses_original_producer_without_external_renewal(monkeypatch, changed):
    from process.provider_directory_cms_nonprofile_capacity import _CapacityProducer

    case = serving_case()
    lease = verify_serving_envelope(case.envelope)
    directory = SimpleNamespace(db=object(), _schema=lambda: "fixture")
    owner, admission = _captured_owner(monkeypatch, directory, "ab" * 32)
    producer = _CapacityProducer(
        directory,
        case.execution,
        case.fence,
        "run_" + "a" * 32,
        lease,
        case.profile,
        owner.plan,
        AsyncMock(),
        "0/10",
        captured_inputs=owner.captured_inputs,
    )
    request = _measured_request(owner)
    admission.check_phase = producer.check_phase
    admission.lease = lease
    admission._started = True
    admission._assert_lease = lambda: None
    admission.measure = AsyncMock(return_value=request.relations)
    admission._ordinary_relations = {
        (annotation.schema, annotation.relation): annotation for annotation in request.ordinary_relations
    }
    producer._assert_consumption = AsyncMock()
    producer._assert_wal_budget = AsyncMock()
    if changed == "phase":
        producer.resumed_profile = object()
    elif changed == "quota":
        admission.measure.return_value = (*request.relations[:-1], replace(request.relations[-1], total_bytes=8192))
    elif changed == "producer":
        admission.check_phase = AsyncMock()
    if changed:
        with pytest.raises(RuntimeError, match="retention_changed"):
            await retention.check_ordinary_retained_growth(admission, directory)
    else:
        await retention.check_ordinary_retained_growth(admission, directory)
        producer._assert_consumption.assert_awaited_once()
        producer._assert_wal_budget.assert_awaited_once()
    producer.fresh_storage_envelope.assert_not_awaited()


def _initial_rename_case(monkeypatch):
    from process.provider_directory_cms_nonprofile_capacity import _CapacityProducer
    from tests.test_provider_directory_serving_retention_allocation import _initial_measurements

    owner, request = _initial_measurements(monkeypatch)
    admission, directory = owner.admission, owner.fhir
    admission.plan = owner.plan
    case = serving_case(initial_mode=True)
    lease = verify_serving_envelope(case.envelope)
    producer = _CapacityProducer(
        directory,
        case.execution,
        case.fence,
        "run_" + "a" * 32,
        lease,
        case.profile,
        owner.plan,
        AsyncMock(),
        "0/10",
        captured_inputs=owner.captured_inputs,
    )
    binding = object()
    directory.db._transaction_binding = lambda: binding
    directory.db.scalar = AsyncMock(return_value=999)
    directory._unscoped_qt = lambda schema, name: f"{schema}.{name}"
    producer._cutover_active, producer._cutover_binding = True, binding
    admission.check_cutover = producer.assert_cutover
    admission.lease = lease
    original = replace(request.ordinary_relations[0], index_oids=(41, 42))
    old_coordinate = (original.schema, original.relation)
    admission._relations = {old_coordinate: original.oid}
    admission._ordinary_relations = {old_coordinate: original}
    admission._external_relations = {old_coordinate}
    admission._logged_relations = {old_coordinate}
    producer._cutover_request = replace(
        request,
        phase="cutover",
        lease=lease,
        relations=(request.relations[0],),
        ordinary_relations=(original,),
    )
    producer._cutover_witness = object()
    producer._cutover_observation = {"unchanged": True}
    custody_by_field = dict(owner_oid=original.owner_oid, schema_oid=original.schema_oid, persistence="p")
    indexes = [dict(index_oid=oid, relation_oid=original.oid, index_name=f"pdsrv_i_{oid:x}") for oid in (41, 42)]
    monkeypatch.setattr(retention, "_custody", AsyncMock(return_value=custody_by_field))
    monkeypatch.setattr(retention, "_catalog", AsyncMock(return_value=({"indexes": indexes}, None)))
    monkeypatch.setattr(retention, "_layout_digest", lambda _catalog: original.layout_sha256)
    return admission, directory, producer, original, custody_by_field, indexes


@pytest.mark.parametrize(
    "changed", [None, "name", "owner", "index", "binding", "request", "lease", "phase", "annotation"]
)
async def test_initial_retention_acknowledges_only_exact_original_oid_rename(monkeypatch, changed):
    admission, directory, producer, original, custody_by_field, indexes = _initial_rename_case(monkeypatch)
    old_coordinate = (original.schema, original.relation)
    match changed:
        case "name":
            indexes[0]["index_name"] = "arbitrary_index"
        case "owner":
            custody_by_field["owner_oid"] += 1
        case "index":
            indexes[0]["index_oid"] = 43
        case "binding":
            producer._cutover_binding = object()
        case "request":
            producer._cutover_request = None
        case "lease":
            producer._cutover_request = replace(producer._cutover_request, lease=object())
        case "phase":
            producer._cutover_request = replace(producer._cutover_request, phase="readiness")
        case "annotation":
            producer._cutover_request = replace(producer._cutover_request, ordinary_relations=(replace(original),))
    previous_request = producer._cutover_request
    witness, observation = producer._cutover_witness, producer._cutover_observation
    if changed:
        with pytest.raises(RuntimeError, match="retention_changed"):
            await retention.acknowledge_initial_retained_relation(admission, directory, family="profile")
        assert admission._relations == {old_coordinate: original.oid}
        assert producer._cutover_request is previous_request
    else:
        await retention.acknowledge_initial_retained_relation(admission, directory, family="profile")
        retained_coordinate = (original.schema, f"pdsrv_r_{original.oid:x}")
        assert admission._relations == {retained_coordinate: original.oid}
        assert admission._ordinary_relations[retained_coordinate].index_oids == original.index_oids
        assert producer._cutover_request.ordinary_relations == (admission._ordinary_relations[retained_coordinate],)
        assert producer._cutover_request.relations == (
            replace(previous_request.relations[0], relation=retained_coordinate[1]),
        )
        assert old_coordinate not in admission._ordinary_relations
    assert producer._cutover_witness is witness and producer._cutover_observation is observation


@pytest.mark.parametrize("changed", [None, "oid", "old_name"])
async def test_original_cutover_guard_accepts_only_acknowledged_same_oid_name(monkeypatch, changed):
    from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME

    admission, directory, producer, _original, _custody, _indexes = _initial_rename_case(monkeypatch)
    producer._cutover_witness = SimpleNamespace(expires_at=producer.lease.max_build_deadline)
    producer._check_captured_inputs = AsyncMock()
    producer._assert_consumption = AsyncMock()
    producer._assert_physical = AsyncMock()
    previous_request = producer._cutover_request
    await retention.acknowledge_initial_retained_relation(admission, directory, family="profile")
    directory.db.scalar.return_value = VALIDATION_TIME
    relations = producer._cutover_request.relations
    if changed == "oid":
        relations = (replace(relations[0], oid=relations[0].oid + 1),)
    elif changed == "old_name":
        relations = previous_request.relations
    if changed:
        with pytest.raises(RuntimeError, match="cutover_relations_changed"):
            await producer.assert_cutover(relations)
        producer._assert_physical.assert_not_awaited()
    else:
        await producer.assert_cutover(relations)
        producer._assert_physical.assert_awaited_once_with(producer._cutover_request, producer._cutover_observation)
    producer.fresh_storage_envelope.assert_not_awaited()
