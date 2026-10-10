# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original capacity consumption and exact retained-source recovery boundaries."""

import asyncio
import datetime
import importlib
import json
from compression import zstd
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from process import provider_directory_cms_capacity_contract as capacity_contract
from process import provider_directory_cms_nonprofile_capacity as capacity
from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_npd_recovery as recovery
from process import provider_directory_cms_serving_coverage as coverage
from process import provider_directory_cms_storage_continuation as continuation
from process import provider_directory_profile_capacity_preflight_contract as preflight
from process import provider_directory_profile_runtime_observation as runtime
from process.provider_directory_cms_preparation import NonprofileAdmissionCheck
from tests.provider_directory_cms_capacity_test_support import (
    cms_guard,
    paired_profile_envelope,
    rehash_guard,
    sign_guard,
)
from tests.test_cms_npd_source import _client, _source
from tests.test_provider_directory_cms_nonprofile_capacity import _producer
from tests.test_provider_directory_cms_npd import _observation, _receipt
from tests.test_provider_directory_cms_serving import _Database
from tests.test_provider_directory_cms_storage_continuation import _envelope
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME, _trust, _verify

fhir = importlib.import_module("process.provider_directory_fhir")
pytestmark = pytest.mark.asyncio


def _capacity_producer():
    producer = _producer()
    runtime_dict = {
        "contract_id": runtime.PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID,
        **{
            name: getattr(producer.lease.runtime_witness, name)
            for name in runtime.CAPACITY_LEASE_LOCALLY_VERIFIED_RUNTIME_FIELDS
        },
    }
    profile_guard = deepcopy(paired_profile_envelope(producer.execution)["lease"]["signing_preflight_guard"])
    profile_guard["healthcare_receipt"]["runtime_observation"] = deepcopy(runtime_dict)
    rehash_guard(profile_guard)
    profile_envelope = sign_guard(profile_guard)
    producer.profile_lease = _verify(profile_envelope)
    producer.plan = replace(producer.plan, paired_profile_lease_digest=producer.profile_lease.lease_digest)
    assigned_guard = cms_guard(producer.plan, execution=producer.execution, paired_envelope=profile_envelope)
    assigned_guard["healthcare_receipt"]["runtime_observation"] = deepcopy(runtime_dict)
    rehash_guard(assigned_guard)
    assigned_envelope = sign_guard(assigned_guard, cms=True)
    producer.lease = _verify(assigned_envelope, expected_capacity_geometry_hash=producer.plan.capacity_geometry_hash)
    return producer, runtime_dict, assigned_envelope, profile_envelope


def _capacity_receipts(producer, database, events, ledger):
    rows_by_nonce = {}
    for lease in (producer.lease, producer.profile_lease):
        guard = lease.signing_preflight_guard
        request = preflight.validated_capacity_preflight_request(guard["healthcare_request"])
        rows_by_nonce[lease.nonce] = capacity_contract.capacity_preflight_receipt_row_values(
            request,
            guard["healthcare_receipt"],
            issued_at=preflight._utc_timestamp(guard["healthcare_receipt"]["issued_at"]),
        )
    counters_dict = {
        "active_profile_run_count": 0,
        "claimed_profile_checkpoint_count": 0,
        "unexpired_capacity_consumption_count": 0,
        "outstanding_preflight_receipt_count": 0,
    }
    profile_row_dict = {
        "attestation_id": producer.profile_lease.attestation_id,
        "lease_digest": producer.profile_lease.lease_digest,
        "selection_proof_id": producer.plan.selection_proof_id,
    }

    async def first(query, **params):
        if "receipt_sha256" in params:
            return rows_by_nonce.get(params["receipt_sha256"])
        if "admission_purpose='profile'" in query:
            return profile_row_dict
        return counters_dict

    async def insert(*, schema, values_by_name):
        assert schema == "synthetic" and database.binding is not None
        events.append("consume-lease")
        ledger.append(deepcopy(values_by_name))

    database.first = AsyncMock(side_effect=first)
    database.all = AsyncMock(side_effect=lambda *_args, **_params: deepcopy(ledger))
    database.scalar = AsyncMock(return_value=0)
    database.status = AsyncMock(return_value=1)
    _bind_capacity_importer(producer, database, events, insert)
    return rows_by_nonce, counters_dict, profile_row_dict


def _bind_capacity_importer(producer, database, events, insert):
    producer.fhir.db = database
    producer.fhir._schema = lambda: "synthetic"
    producer.fhir._unscoped_qt = lambda schema, table: f"{schema}.{table}"
    producer.fhir._pagination_checkpoint_row_mapping = fhir._pagination_checkpoint_row_mapping
    producer.fhir._profile_capacity_preflight_receipt_ref = lambda schema: f"{schema}.preflight"
    producer.fhir._profile_capacity_preflight_stored_receipt = fhir._profile_capacity_preflight_stored_receipt
    producer.fhir._assert_profile_capacity_receipt_open = fhir._assert_profile_capacity_receipt_open
    producer.fhir._profile_capacity_quiescence_sql = fhir._profile_capacity_quiescence_sql
    producer.fhir._provider_directory_profile_capacity_consumption_identity = (
        fhir._provider_directory_profile_capacity_consumption_identity
    )
    producer.fhir.ProviderDirectoryProfileCapacityLeaseConsumption = (
        fhir.ProviderDirectoryProfileCapacityLeaseConsumption
    )
    producer.fhir._lock_profile_capacity_preflight_state = AsyncMock()
    producer.fhir._lock_provider_directory_profile_capacity_control_run = AsyncMock()
    producer.fhir.assert_profile_selection_current_in_transaction = AsyncMock()
    producer.fhir._provider_directory_profile_selection_catalog = lambda: {}
    producer.fhir._lock_and_verify_artifact_dataset_fence = AsyncMock()
    producer.fhir._assert_profile_capacity_receipt_storage = AsyncMock()
    producer.fhir._mark_profile_capacity_receipt_consumed = AsyncMock(
        side_effect=lambda *_args: events.append("consume-preflight")
    )
    producer.fhir._consume_provider_directory_profile_capacity_lease = AsyncMock(side_effect=insert)
    producer.fhir._provider_directory_profile_capacity_acceptance_time = AsyncMock(return_value=VALIDATION_TIME)
    producer.fhir._assert_provider_directory_profile_wal_budget = AsyncMock()


def _capacity_observation(producer):
    observation_dict = {
        **{
            name: getattr(producer.lease, name)
            for name in ("database_system_identifier", "database_oid", "database_name")
        },
        "temp_limit_bytes": producer.plan.temp_file_limit_bytes_per_backend,
        "query_parallel_workers": 0,
        "maintenance_parallel_workers": 0,
        "wal_lsn": "0/10",
        **{
            usage + "_tablespace_" + field: getattr(
                next(tablespace for tablespace in producer.lease.tablespaces if tablespace.usage == usage),
                "tablespace_" + field,
            )
            for usage in ("data", "temp")
            for field in ("oid", "name")
        },
    }
    return observation_dict


@pytest.fixture
def capacity_case(monkeypatch):
    """Use signed leases and record exact transaction-bound consumption."""
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    producer, runtime_dict, assigned_envelope, profile_envelope = _capacity_producer()
    events, ledger = [], []
    database = _Database(events)
    rows_by_nonce, counters_dict, profile_row_dict = _capacity_receipts(producer, database, events, ledger)
    observation_dict = _capacity_observation(producer)
    database_observation = AsyncMock(return_value=observation_dict)
    database_reader = capacity._database_observation
    runtime_observation = AsyncMock(return_value=runtime_dict)
    monkeypatch.setattr(capacity, "_database_observation", database_observation)
    monkeypatch.setattr(capacity, "observe_profile_runtime", runtime_observation)
    monkeypatch.setattr(capacity.capacity_runtime, "configured_capacity_lease_trust", _trust)

    async def authority(request):
        storage = deepcopy(producer.lease.signing_preflight_guard["control_plane_request"]["storage_observation"])
        storage.update(observed_at=continuation._utc(VALIDATION_TIME), issued_at=continuation._utc(VALIDATION_TIME))
        return _envelope(
            {
                **request.binding_by_field,
                "issued_at": continuation._utc(VALIDATION_TIME),
                "expires_at": continuation._utc(VALIDATION_TIME + datetime.timedelta(seconds=30)),
                "storage_observation": storage,
            }
        )

    producer.fresh_storage_envelope = AsyncMock(side_effect=authority)
    return SimpleNamespace(
        producer=producer,
        rows=rows_by_nonce,
        counters=counters_dict,
        profile_row=profile_row_dict,
        ledger=ledger,
        events=events,
        observation=observation_dict,
        database_observation=database_observation,
        runtime_observation=runtime_observation,
        assigned_envelope=assigned_envelope,
        profile_envelope=profile_envelope,
        database_reader=database_reader,
    )


async def _admission(case, *, execution=None, plan=None):
    producer = case.producer
    return await capacity.produce_nonprofile_admission(
        producer.fhir,
        execution or producer.execution,
        producer.fence,
        run_id=producer.run_id,
        assigned_envelope=deepcopy(case.assigned_envelope),
        profile_envelope=deepcopy(case.profile_envelope),
        signed_plan=plan or producer.plan,
        fresh_storage_envelope=producer.fresh_storage_envelope,
    )


async def _start(admission, case):
    producer = case.producer
    await admission.before_scratch(
        producer.execution,
        producer.fence,
        SimpleNamespace(projection_hash=producer.plan.artifact_scope_projection_hash),
        set(producer.plan.publish_targets),
        frozenset(producer.plan.resource_types),
    )


async def test_signed_admission_consumes_original_bytes_once_and_rechecks_readiness(capacity_case):
    case = capacity_case
    admission = await _admission(case)
    await _start(admission, case)
    owner = admission.check_phase.__self__
    receipt = await owner.check_phase(NonprofileAdmissionCheck("readiness", admission.lease, admission.plan, ()))
    assert receipt.phase == "readiness" and receipt.lease_digest == admission.lease.lease_digest
    assert len(case.ledger) == 1
    stored = case.ledger[0]
    assert stored["admission_purpose"] == "cms_nonprofile"
    assert stored["canonical_lease_json"] == admission.lease.canonical_lease_json
    assert stored["signature"] == admission.lease.signature
    assert stored["lease_digest"] != admission.paired_profile_lease.lease_digest
    assert stored["selection_proof_id"] == admission.plan.selection_proof_id
    assert stored["source_vector_hash"] == admission.plan.desired_fence_hash
    assert case.events == ["snapshot-start", "consume-preflight", "consume-lease", "snapshot-end"]
    assert len(owner.continuation_digests) == 2
    with pytest.raises(RuntimeError, match="provider_directory_nonprofile_capacity_same_run_replay_unsupported"):
        await owner.check_phase(NonprofileAdmissionCheck("pre_scratch", admission.lease, admission.plan, ()))
    assert len(case.ledger) == 1 and case.producer.fhir.db.binding is None
    case.producer.fhir._mark_profile_capacity_receipt_consumed.assert_awaited_once()
    case.producer.fhir._consume_provider_directory_profile_capacity_lease.assert_awaited_once()


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("nonprofile_missing", "nonprofile_capacity_preflight_missing"),
        ("paired_missing", "nonprofile_capacity_paired_preflight_missing"),
        ("nonprofile_changed", "profile_capacity_preflight_receipt_invalid"),
        ("paired_changed", "profile_capacity_preflight_receipt_invalid"),
        ("paired_consumed", "profile_capacity_preflight_receipt_expired"),
        ("competing", "nonprofile_capacity_competing_admission"),
        ("runtime_changed", "runtime_observation_capacity_lease_runtime_mismatch"),
    ],
)
async def test_admission_rejects_changed_authority_before_any_consumption(capacity_case, fault, reason):
    case, producer = capacity_case, capacity_case.producer
    admission = await _admission(case)
    if fault.endswith("missing"):
        lease = producer.lease if fault == "nonprofile_missing" else producer.profile_lease
        del case.rows[lease.nonce]
    elif fault.endswith("changed") and fault != "runtime_changed":
        lease = producer.lease if fault == "nonprofile_changed" else producer.profile_lease
        receipt = json.loads(case.rows[lease.nonce]["receipt_json"])
        receipt["request_sha256"] = "f" * 64
        case.rows[lease.nonce]["receipt_json"] = json.dumps(receipt)
    elif fault == "paired_consumed":
        case.rows[producer.profile_lease.nonce]["consumed_at"] = VALIDATION_TIME
    elif fault == "competing":
        case.counters["active_profile_run_count"] = 1
    else:
        changed_dict = dict(case.runtime_observation.return_value, healthcare_source_commit="f" * 40)
        case.runtime_observation.side_effect = [case.runtime_observation.return_value, changed_dict]
    before = deepcopy(case.rows)
    with pytest.raises(RuntimeError, match=reason):
        await _start(admission, case)
    assert not admission._started and case.ledger == [] and case.rows == before
    assert producer.fhir.db.binding is None
    producer.fhir._mark_profile_capacity_receipt_consumed.assert_not_awaited()
    producer.fhir._consume_provider_directory_profile_capacity_lease.assert_not_awaited()


@pytest.mark.parametrize("fault", ["missing", "duplicate", "lease", "purpose"])
async def test_readiness_requires_one_exact_original_consumption(capacity_case, fault):
    case = capacity_case
    admission = await _admission(case)
    await _start(admission, case)
    owner = admission.check_phase.__self__
    if fault == "missing":
        case.ledger.clear()
    elif fault == "duplicate":
        case.ledger.append(deepcopy(case.ledger[0]))
    elif fault == "lease":
        case.ledger[0]["lease_digest"] = "f" * 64
    else:
        case.ledger[0]["admission_purpose"] = "profile"
    before = deepcopy(case.ledger)
    with pytest.raises(RuntimeError, match="nonprofile_capacity_consumption_changed"):
        await owner.check_phase(NonprofileAdmissionCheck("readiness", admission.lease, admission.plan, ()))
    assert case.ledger == before and len(owner.continuation_digests) == 1
    case.producer.fhir._consume_provider_directory_profile_capacity_lease.assert_awaited_once()
    assert "admission_purpose='cms_nonprofile'" in case.producer.fhir.db.all.call_args.args[0]


@pytest.mark.parametrize(
    "field,value",
    [
        ("selection_proof_id", "f" * 64),
        ("desired_fence_hash", "f" * 64),
        ("desired_profile_as_of", "2026-07-29"),
    ],
)
async def test_changed_selection_cannot_request_signed_admission(capacity_case, field, value):
    case = capacity_case
    with pytest.raises(RuntimeError, match="nonprofile_capacity_selection_changed"):
        await _admission(case, plan=replace(case.producer.plan, **{field: value}))
    case.database_observation.assert_not_awaited()
    assert case.ledger == [] and case.events == []


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("run_id", "run_" + "b" * 32, "nonprofile_capacity_active_profile_changed"),
        ("lease_digest", "f" * 64, "nonprofile_capacity_active_profile_consumption_changed"),
        ("selection_proof_id", "f" * 64, "nonprofile_capacity_active_profile_consumption_changed"),
    ],
)
async def test_profile_wal_window_requires_its_durable_paired_consumption(capacity_case, field, value, reason):
    case = capacity_case
    admission = await _admission(case)
    await _start(admission, case)
    owner = admission.check_phase.__self__
    profile = SimpleNamespace(run_id=owner.run_id, lease=owner.profile_lease)
    if field == "run_id":
        profile.run_id = value
    else:
        case.profile_row[field] = value
    owner.resumed_profile = profile
    owner.preparation_wal_end_bytes = 0
    token = owner.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(profile)
    before = deepcopy(case.ledger)
    try:
        with pytest.raises(RuntimeError, match=reason):
            await owner.check_phase(NonprofileAdmissionCheck("readiness", owner.lease, owner.plan, ()))
    finally:
        owner.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)
    assert case.ledger == before
    owner.fhir._assert_provider_directory_profile_wal_budget.assert_not_awaited()
    owner.fhir._consume_provider_directory_profile_capacity_lease.assert_awaited_once()


def _candidate_state(identity):
    return {
        "dataset_id": "dataset-synthetic",
        "endpoint_id": "endpoint-synthetic",
        "acquisition_root_run_id": "root-synthetic",
        "previous_dataset_id": None,
        "status": fhir.ENDPOINT_DATASET_ACQUIRING,
        "is_current": False,
        "published_at": None,
        "validated_at": None,
        "dataset_hash": "d" * 64,
        "publication_metadata_json": {
            "source_release": identity,
            "source_ids": ["cms-npd"],
            fhir.RESOURCE_HASH_CONTRACT_METADATA_KEY: fhir.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            fhir.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY: identity["generated_at"],
        },
    }


@pytest.fixture
def npd_case():
    """Preserve candidate identity while recording importer transport effects."""
    identity = cms.release_identity(_receipt())
    state_dict = _candidate_state(identity)
    database = _Database([])
    database.first = AsyncMock()
    database.all = AsyncMock(return_value=[])
    database.scalar = AsyncMock(return_value=False)
    database.status = AsyncMock(return_value=1)
    importer = SimpleNamespace(
        db=database,
        _schema=lambda: "synthetic",
        _qt=lambda schema, table: f"{schema}.{table}",
        _pagination_checkpoint_row_mapping=fhir._pagination_checkpoint_row_mapping,
        _coerce_rowcount=fhir._coerce_rowcount,
        _endpoint_dataset_state=AsyncMock(return_value=state_dict),
        _endpoint_dataset_candidate_id=lambda *_args: state_dict["dataset_id"],
        _current_endpoint_dataset_id=AsyncMock(return_value=None),
        _dataset_resource_hash_contract=fhir._dataset_resource_hash_contract,
        _dataset_semantic_projection_as_of=fhir._dataset_semantic_projection_as_of,
        _initialize_endpoint_dataset_candidate=AsyncMock(side_effect=lambda candidate, _rows: candidate),
        _lock_endpoint_dataset_candidate_admission=AsyncMock(),
        delete_dataset_proof_shards=AsyncMock(),
        _persist_endpoint_dataset_rows=AsyncMock(),
        _raise_if_resource_import_cancelled=AsyncMock(),
        _finalize_endpoint_dataset_candidate=AsyncMock(),
        EndpointDatasetCandidate=fhir.EndpointDatasetCandidate,
        FHIRAcquisitionContext=fhir.FHIRAcquisitionContext,
        parse_fhir_resource=fhir.parse_fhir_resource,
        ProviderDirectoryOrganization=fhir.ProviderDirectoryOrganization,
        ProviderDirectoryDatasetResource=fhir.ProviderDirectoryDatasetResource,
        _insurance_plan_network_references=fhir._insurance_plan_network_references,
        RESOURCE_MODELS_BY_TYPE=fhir.RESOURCE_MODELS_BY_TYPE,
        ENDPOINT_DATASET_ACQUIRING=fhir.ENDPOINT_DATASET_ACQUIRING,
        ENDPOINT_DATASET_VALIDATED=fhir.ENDPOINT_DATASET_VALIDATED,
        ENDPOINT_DATASET_FAILED=fhir.ENDPOINT_DATASET_FAILED,
        ENDPOINT_DATASET_PUBLISHED=fhir.ENDPOINT_DATASET_PUBLISHED,
        ENDPOINT_DATASET_SUPERSEDED=fhir.ENDPOINT_DATASET_SUPERSEDED,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT=fhir.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    candidate = fhir.EndpointDatasetCandidate(
        endpoint_id=state_dict["endpoint_id"],
        dataset_id=state_dict["dataset_id"],
        acquisition_root_run_id=state_dict["acquisition_root_run_id"],
        source_ids=("cms-npd",),
        selected_resources=tuple(cms.RESOURCE_TYPES),
        import_run_id="run-synthetic",
        previous_dataset_id=None,
        resource_hash_contract=fhir.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=identity["generated_at"],
        source_release=identity,
    )
    return SimpleNamespace(fhir=importer, state=state_dict, candidate=candidate, identity=identity)


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("release", "cms_npd_candidate_release_changed"),
        ("endpoint", "cms_npd_candidate_identity_changed"),
        ("date", "cms_npd_candidate_identity_changed"),
        ("root", "cms_npd_candidate_root_invalid"),
        ("status", "cms_npd_candidate_state_invalid"),
        ("disposed", "cms_npd_candidate_disposed"),
    ],
)
async def test_reused_candidate_identity_fails_before_ingestion(npd_case, fault, reason):
    case = npd_case
    if fault == "release":
        case.state["publication_metadata_json"]["source_release"] = {"vector_sha256": "f" * 64}
    if fault == "endpoint":
        case.state["endpoint_id"] = "different-endpoint"
    if fault == "date":
        case.state["publication_metadata_json"][fhir.SEMANTIC_PROJECTION_AS_OF_METADATA_KEY] = "2026-09-23"
    if fault == "root":
        case.state["acquisition_root_run_id"] = None
    if fault == "status":
        case.state["status"] = fhir.ENDPOINT_DATASET_FAILED
    if fault == "disposed":
        case.fhir.db.scalar.return_value = True
    before = deepcopy(case.state)
    with pytest.raises(RuntimeError, match=reason):
        await cms._candidate(case.fhir, "endpoint-synthetic", "run-synthetic", case.identity)
    assert case.state == before
    case.fhir._initialize_endpoint_dataset_candidate.assert_not_awaited()
    case.fhir._persist_endpoint_dataset_rows.assert_not_awaited()
    case.fhir.db.status.assert_not_awaited()


async def test_current_replay_refreshes_exact_coverage_without_reingesting(npd_case, monkeypatch):
    case = npd_case
    case.state.update(
        status=fhir.ENDPOINT_DATASET_PUBLISHED,
        is_current=True,
        published_at="published-synthetic",
        validated_at="validated-synthetic",
        resource_count=8,
    )
    case.fhir._current_endpoint_dataset_id.return_value = case.state["dataset_id"]
    cleanup, prior = AsyncMock(), AsyncMock()
    monkeypatch.setattr(recovery, "resume_pending_cleanup", cleanup)
    monkeypatch.setattr(recovery, "dispose_prior_vectors", prior)
    proof_dict = {name: case.state[name] for name in ("dataset_id", "endpoint_id", "dataset_hash")}
    proof_dict.update(release_id=case.identity["vector_sha256"], proof_version=2)
    prepare = AsyncMock(return_value=proof_dict)
    monkeypatch.setattr(coverage, "prepare_cms_candidate_coverage", prepare)
    before = deepcopy(case.state)
    replay_result = await cms._prepare_current_serving_candidate(case.fhir, case.state, case.identity["vector_sha256"])
    assert replay_result["status"] == "ready"
    assert replay_result["desired_cms_dataset"]["dataset_id"] == case.candidate.dataset_id
    assert replay_result["expected_cms_incumbent"]["dataset_id"] == case.candidate.dataset_id
    selected = prepare.call_args.args[1]
    assert (selected.dataset_id, selected.endpoint_id) == (case.candidate.dataset_id, case.candidate.endpoint_id)
    assert prepare.call_args.args[2:] == (case.identity["vector_sha256"], case.state["dataset_hash"])
    cleanup.assert_awaited_once_with(case.fhir, case.candidate.endpoint_id)
    prior.assert_awaited_once_with(case.fhir, case.candidate.endpoint_id, case.identity["vector_sha256"])
    assert case.state == before
    case.fhir._initialize_endpoint_dataset_candidate.assert_not_awaited()
    case.fhir._persist_endpoint_dataset_rows.assert_not_awaited()
    case.fhir._finalize_endpoint_dataset_candidate.assert_not_awaited()


@pytest.mark.parametrize(
    "body,reason",
    [
        (b"{not-json}\n", "cms_npd_ndjson_invalid"),
        (b"\xff\n", "cms_npd_ndjson_invalid"),
        (b'{"resourceType":"Organization","id":"site-1"}\n', "cms_npd_resource_invalid"),
        (b"null\n", "cms_npd_resource_invalid"),
        (b"x" * 65, "cms_npd_decoded_size_invalid"),
    ],
)
async def test_decoded_source_failure_never_reaches_batch_writer(npd_case, monkeypatch, tmp_path, body, reason):
    case = npd_case
    monkeypatch.setattr(cms.source, "MAX_RESOURCE_LINE_BYTES", 64)
    path = tmp_path / "source.zst"
    path.write_bytes(zstd.compress(body))
    writer = AsyncMock()
    monkeypatch.setattr(cms, "_persist_source_batch", writer)
    with pytest.raises(cms.source.CmsNpdSourceError, match=reason):
        await cms._stream_file(case.fhir, path, case.candidate, "Location", {}, {})
    writer.assert_not_awaited()
    case.fhir._finalize_endpoint_dataset_candidate.assert_not_awaited()
    assert case.fhir.db.binding is None


async def test_valid_source_batch_binds_exact_raw_and_normalized_witness(npd_case, monkeypatch, tmp_path):
    case = npd_case
    resource_dict = {"resourceType": "Location", "id": "site-1", "name": "Café"}
    path = tmp_path / "source.zst"
    path.write_bytes(zstd.compress(json.dumps(resource_dict, ensure_ascii=False).encode() + b"\n"))
    witnessed_resources = []

    async def persist(importer, session, _model, rows, candidate):
        assert importer is case.fhir and session is case.fhir.db.session
        assert case.fhir.db.binding is not None and candidate is case.candidate
        assert candidate.resource_hash_contract == fhir.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        assert candidate.semantic_projection_as_of == case.identity["generated_at"]
        assert rows[0]["resource_id"] == resource_dict["id"] and rows[0]["name"] == resource_dict["name"]
        return [{**rows[0], "dataset_id": candidate.dataset_id, "resource_type": "Location", "payload_hash": "a" * 64}]

    async def witness(session, by_id):
        assert session is case.fhir.db.session and case.fhir.db.binding is not None
        witnessed_resources.append(deepcopy(by_id))

    batch_writer = AsyncMock(side_effect=persist)
    monkeypatch.setattr(cms, "persist_cms_dataset_rows", batch_writer)
    insert = AsyncMock(side_effect=witness)
    monkeypatch.setattr(cms, "_insert_verified_witnesses", insert)
    assert await cms._stream_file(case.fhir, path, case.candidate, "Location", {}, {}) == 1
    stored = witnessed_resources[0][resource_dict["id"]]
    assert stored["raw_payload_json"] == resource_dict
    assert stored["normalized_payload_hash"] == "a" * 64
    assert (stored["dataset_id"], stored["release_id"], stored["resource_type"]) == (
        case.candidate.dataset_id,
        case.identity["vector_sha256"],
        "Location",
    )
    batch_writer.assert_awaited_once()
    case.fhir._persist_endpoint_dataset_rows.assert_not_awaited()
    insert.assert_awaited_once()
    assert case.fhir.db.binding is None


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("length", "cms_npd_witness_batch_invalid"),
        ("duplicate", "cms_npd_witness_payload_conflict"),
        ("resource", "cms_npd_witness_projection_mismatch"),
        ("dataset", "cms_npd_witness_projection_mismatch"),
        ("type", "cms_npd_witness_projection_mismatch"),
    ],
)
async def test_source_witnesses_cannot_commit_another_projection(npd_case, monkeypatch, fault, reason):
    case = npd_case
    raw_resources = [{"resourceType": "Location", "id": "site-1", "name": "Café"}]
    normalized_resources = [
        {
            "resource_id": "site-1",
            "dataset_id": case.candidate.dataset_id,
            "resource_type": "Location",
            "payload_hash": "a" * 64,
        }
    ]
    submitted_resources = [{}]
    if fault == "length":
        submitted_resources.clear()
    elif fault == "duplicate":
        raw_resources.append({**raw_resources[0], "name": "Changed"})
        submitted_resources.append({})
    elif fault == "resource":
        normalized_resources[0]["resource_id"] = "other-site"
    elif fault == "dataset":
        normalized_resources[0]["dataset_id"] = "other-dataset"
    else:
        normalized_resources[0]["resource_type"] = "Organization"
    batch_writer = AsyncMock(return_value=normalized_resources)
    monkeypatch.setattr(cms, "persist_cms_dataset_rows", batch_writer)
    witnesses = AsyncMock()
    monkeypatch.setattr(cms, "_insert_verified_witnesses", witnesses)
    with pytest.raises(RuntimeError, match=reason):
        await cms._persist_source_batch(
            case.fhir, object, submitted_resources, raw_resources, case.candidate, "Location"
        )
    witnesses.assert_not_awaited()
    if fault in {"length", "duplicate"}:
        batch_writer.assert_not_awaited()
    else:
        batch_writer.assert_awaited_once()
    case.fhir._persist_endpoint_dataset_rows.assert_not_awaited()
    case.fhir._finalize_endpoint_dataset_candidate.assert_not_awaited()
    assert case.fhir.db.binding is None


async def test_identity_replay_count_failure_stops_before_completeness(npd_case, monkeypatch, tmp_path):
    case = npd_case
    for name, kind in cms.source.RESOURCE_FILES:
        body = json.dumps({"resourceType": kind, "id": "resource-1"}).encode() + b"\n"
        (tmp_path / (name + ".zst")).write_bytes(zstd.compress(body))
    first = next(name for name, kind in cms.source.RESOURCE_FILES if kind == "Organization")
    case.identity["files"][first]["row_count"] = 2
    write, complete = AsyncMock(), AsyncMock()
    monkeypatch.setattr(cms, "_write_identity_batch", write)
    monkeypatch.setattr(cms, "_assert_identity_evidence", complete)
    with pytest.raises(RuntimeError, match="cms_npd_identity_file_row_count_changed"):
        await cms._materialize_identity_evidence(case.fhir, tmp_path, case.candidate, case.identity, {}, {})
    assert write.await_count == 1 and write.call_args.args[4] == "Organization"
    complete.assert_not_awaited()
    case.fhir._finalize_endpoint_dataset_candidate.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("endpoint_id", "other-endpoint", "cms_npd_stale_disposition_identity_changed"),
        ("acquisition_root_run_id", "other-root", "cms_npd_stale_disposition_identity_changed"),
        ("is_current", True, "cms_npd_stale_disposition_identity_changed"),
        ("published_at", "published-synthetic", "cms_npd_stale_disposition_identity_changed"),
        ("status", "failed", "cms_npd_stale_disposition_state_invalid"),
    ],
)
async def test_stale_disposal_never_touches_changed_parent(npd_case, field, value, reason):
    case = npd_case
    case.state[field] = value
    case.fhir.db.first.side_effect = [case.state, None]
    before = deepcopy(case.state)
    with pytest.raises(RuntimeError, match=reason):
        await recovery.dispose_changed_vector(case.fhir, case.candidate, case.identity)
    case.fhir.db.status.assert_not_awaited()
    case.fhir.delete_dataset_proof_shards.assert_not_awaited()
    assert case.state == before and case.fhir.db.binding is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("endpoint_id", "other-endpoint"),
        ("acquisition_root_run_id", "other-root"),
        ("vector_sha256", "f" * 64),
        ("prior_status", "validated"),
    ],
)
async def test_conflicting_stale_observation_cannot_be_replayed(npd_case, field, value):
    case = npd_case
    case.state["status"] = fhir.ENDPOINT_DATASET_FAILED
    disposition_dict = {
        "prior_status": fhir.ENDPOINT_DATASET_ACQUIRING,
        "endpoint_id": case.candidate.endpoint_id,
        "acquisition_root_run_id": case.candidate.acquisition_root_run_id,
        "vector_sha256": case.identity["vector_sha256"],
    }
    disposition_dict[field] = value
    case.fhir.db.first.side_effect = [case.state, disposition_dict]
    before = deepcopy(disposition_dict)
    with pytest.raises(RuntimeError, match="cms_npd_stale_disposition_conflict"):
        await recovery.dispose_changed_vector(case.fhir, case.candidate, case.identity)
    assert disposition_dict == before
    case.fhir.db.status.assert_not_awaited()
    case.fhir.delete_dataset_proof_shards.assert_not_awaited()


async def test_exact_stale_replay_drains_only_its_failed_resource_batches(npd_case):
    case = npd_case
    case.state["status"] = fhir.ENDPOINT_DATASET_FAILED
    disposition_dict = {
        "prior_status": fhir.ENDPOINT_DATASET_ACQUIRING,
        "endpoint_id": case.candidate.endpoint_id,
        "acquisition_root_run_id": case.candidate.acquisition_root_run_id,
        "vector_sha256": case.identity["vector_sha256"],
    }
    case.fhir.db.first.side_effect = [
        case.state,
        disposition_dict,
        (case.candidate.dataset_id,),
        (case.candidate.dataset_id,),
    ]
    case.fhir.db.scalar.return_value = True
    counts = iter([2, 0])

    async def status(query, **params):
        assert params["dataset_id"] == case.candidate.dataset_id
        if "provider_directory_dataset_resource" in query:
            assert "WHERE ctid IN (SELECT ctid FROM " in query
            assert query.endswith("WHERE dataset_id=:dataset_id LIMIT :batch_size)")
            assert params["batch_size"] == recovery.DELETE_BATCH_SIZE
            return next(counts)
        assert query.endswith("WHERE dataset_id=:dataset_id")
        return 1

    case.fhir.db.status.side_effect = status
    await recovery.dispose_changed_vector(case.fhir, case.candidate, case.identity)
    assert case.fhir.db.status.await_count == 4
    assert all(call.args[0].startswith("DELETE") for call in case.fhir.db.status.await_args_list)
    case.fhir.delete_dataset_proof_shards.assert_awaited_once_with(case.fhir.db, "synthetic", case.candidate.dataset_id)
    assert case.state["status"] == fhir.ENDPOINT_DATASET_FAILED and case.fhir.db.binding is None


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("parent", "cms_npd_stale_cleanup_parent_changed"),
        ("disposition", "cms_npd_stale_cleanup_parent_changed"),
        ("metadata", "cms_npd_stale_cleanup_identity_changed"),
        ("vector", "cms_npd_stale_cleanup_identity_changed"),
    ],
)
async def test_pending_cleanup_fails_before_deleting_changed_evidence(npd_case, fault, reason):
    case = npd_case
    pending_dict = {
        "dataset_id": case.candidate.dataset_id,
        "acquisition_root_run_id": case.candidate.acquisition_root_run_id,
        "publication_metadata_json": {"source_release": deepcopy(case.identity)},
        "vector_sha256": case.identity["vector_sha256"],
    }
    if fault == "metadata":
        pending_dict["publication_metadata_json"] = None
    elif fault == "vector":
        pending_dict["vector_sha256"] = "f" * 64
    case.fhir.db.first.side_effect = [pending_dict, None if fault == "parent" else (case.candidate.dataset_id,)]
    case.fhir.db.scalar.return_value = fault != "disposition"
    with pytest.raises(RuntimeError, match=reason):
        await recovery.resume_pending_cleanup(case.fhir, case.candidate.endpoint_id)
    case.fhir.db.status.assert_not_awaited()
    case.fhir.delete_dataset_proof_shards.assert_not_awaited()
    assert case.fhir.db.binding is None


@pytest.mark.parametrize("release", [None, [], {"vector_sha256": 3}])
async def test_prior_vector_scan_rejects_unproved_release_before_disposal(npd_case, monkeypatch, release):
    case = npd_case
    case.fhir.db.all.return_value = [
        {
            "dataset_id": case.candidate.dataset_id,
            "acquisition_root_run_id": case.candidate.acquisition_root_run_id,
            "source_release": release,
        }
    ]
    dispose = AsyncMock()
    monkeypatch.setattr(recovery, "dispose_changed_vector", dispose)
    with pytest.raises(RuntimeError, match="cms_npd_stale_disposition_identity_changed"):
        await recovery.dispose_prior_vectors(case.fhir, case.candidate.endpoint_id, "f" * 64)
    dispose.assert_not_awaited()
    case.fhir.db.status.assert_not_awaited()


async def test_prior_vector_scan_preserves_json_identity_and_seek_cursor(npd_case, monkeypatch):
    case = npd_case
    created = datetime.datetime(2026, 9, 24, tzinfo=datetime.timezone.utc)
    row_dict = {
        "dataset_id": case.candidate.dataset_id,
        "acquisition_root_run_id": case.candidate.acquisition_root_run_id,
        "created_at": created,
        "source_release": json.dumps(case.identity),
    }
    case.fhir.db.all.side_effect = [[row_dict], []]
    dispose = AsyncMock()
    monkeypatch.setattr(recovery, "dispose_changed_vector", dispose)
    await recovery.dispose_prior_vectors(case.fhir, case.candidate.endpoint_id, "f" * 64)
    selected = dispose.call_args.args[1]
    assert (selected.dataset_id, selected.endpoint_id, selected.acquisition_root_run_id) == (
        case.candidate.dataset_id,
        case.candidate.endpoint_id,
        case.candidate.acquisition_root_run_id,
    )
    assert dispose.call_args.args[2] == case.identity
    assert case.fhir.db.all.call_args.kwargs["created_at"] == created
    assert case.fhir.db.all.call_args.kwargs["cursor_id"] == case.candidate.dataset_id
    assert "created_at" not in case.fhir.db.all.await_args_list[0].kwargs
    case.fhir.db.status.assert_not_awaited()


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("missing", "nonprofile_capacity_database_observation_missing"),
        ("unbounded", "nonprofile_capacity_temp_limit_unbounded"),
        ("ineffective", "nonprofile_capacity_execution_limits_changed"),
    ],
)
async def test_admission_reads_effective_bounded_backend_before_signatures(capacity_case, monkeypatch, fault, reason):
    case = capacity_case
    native = importlib.import_module("process.entity_address_unified")

    @asynccontextmanager
    async def bounded_backend(database, settings, _quote, _logger, *, temp_file_limit_bytes):
        assert temp_file_limit_bytes == case.producer.plan.temp_file_limit_bytes_per_backend
        assert dict(settings) == {
            "temp_file_limit": "1kB",
            "max_parallel_workers_per_gather": "0",
            "max_parallel_maintenance_workers": "0",
        }
        async with database.transaction():
            yield

    row_dict = {**case.observation, "temp_limit": "-1" if fault == "unbounded" else "1kB"}
    case.producer.fhir.db.first.side_effect = None
    case.producer.fhir.db.first.return_value = None if fault == "missing" else row_dict
    case.producer.fhir.db.scalar.return_value = 0
    monkeypatch.setattr(native, "entity_address_tuned_transaction", bounded_backend)
    monkeypatch.setattr(capacity, "_database_observation", case.database_reader)
    with pytest.raises(RuntimeError, match=reason):
        await _admission(case)
    assert case.ledger == [] and case.producer.fhir.db.binding is None
    case.producer.fhir._consume_provider_directory_profile_capacity_lease.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("temp_limit_bytes", 0, "nonprofile_capacity_execution_limits_changed"),
        ("query_parallel_workers", 1, "nonprofile_capacity_execution_limits_changed"),
        ("data_tablespace_oid", 1664, "nonprofile_capacity_tablespaces_changed"),
        ("database_oid", 16402, "nonprofile_capacity_database_changed"),
    ],
)
async def test_phase_rechecks_backend_identity_before_consumption(capacity_case, field, value, reason):
    case = capacity_case
    admission = await _admission(case)
    case.observation[field] = value
    with pytest.raises(RuntimeError, match=reason):
        await _start(admission, case)
    assert not admission._started and case.ledger == []
    case.producer.fhir._mark_profile_capacity_receipt_consumed.assert_not_awaited()
    case.producer.fhir._consume_provider_directory_profile_capacity_lease.assert_not_awaited()


async def test_logging_rejects_changed_physical_tablespace_after_original_consumption(capacity_case):
    case = capacity_case
    admission = await _admission(case)
    await _start(admission, case)
    case.producer.fhir.db.first.side_effect = None
    case.producer.fhir.db.first.return_value = {"oid": 42, "total_bytes": 100, "persistence": "u"}
    case.producer.fhir._provider_directory_profile_relation_storage_fingerprint = AsyncMock(
        return_value=SimpleNamespace(relation_oid=42, effective_tablespace_oids=(1664,))
    )
    before = deepcopy(case.ledger)
    with pytest.raises(RuntimeError, match="nonprofile_capacity_physical_tablespace_changed"):
        await admission.before_logging(case.producer.fhir, "synthetic", "staged_serving")
    assert case.ledger == before and admission._logged_relations == set()
    case.producer.fhir._provider_directory_profile_relation_storage_fingerprint.assert_awaited_once_with(
        42, expected_persistence="u"
    )
    case.producer.fhir._consume_provider_directory_profile_capacity_lease.assert_awaited_once()


async def test_readiness_cannot_precede_original_admission(capacity_case):
    case = capacity_case
    admission = await _admission(case)
    with pytest.raises(RuntimeError, match="nonprofile_capacity_consumption_missing"):
        await admission.check_phase(NonprofileAdmissionCheck("readiness", admission.lease, admission.plan, ()))
    assert case.ledger == [] and not admission._started
    case.producer.fhir._consume_provider_directory_profile_capacity_lease.assert_not_awaited()


async def test_changed_phase_plan_never_requests_authority(capacity_case):
    case = capacity_case
    admission = await _admission(case)
    changed = replace(admission.plan, batch_size=admission.plan.batch_size + 1)
    with pytest.raises(RuntimeError, match="nonprofile_capacity_phase_identity_changed"):
        await admission.check_phase(NonprofileAdmissionCheck("readiness", admission.lease, changed, ()))
    case.producer.fresh_storage_envelope.assert_not_awaited()
    assert case.ledger == []


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("colocated", "storage_continuation_colocated_observation_changed"),
        ("timestamp_type", "storage_continuation_time_invalid"),
        ("timestamp_text", "storage_continuation_time_invalid"),
        ("timestamp_canonical", "storage_continuation_time_invalid"),
    ],
)
async def test_signed_phase_observation_rejects_inconsistent_storage_and_time(capacity_case, fault, reason):
    case = capacity_case
    admission = await _admission(case)
    authority = case.producer.fresh_storage_envelope.side_effect

    async def changed(request):
        envelope = await authority(request)
        witness = deepcopy(envelope["observation"])
        if fault == "colocated":
            witness["storage_observation"]["volumes"][1]["available_bytes"] -= 1
        else:
            witness["issued_at"] = {
                "timestamp_type": 2,
                "timestamp_text": "invalid",
                "timestamp_canonical": "2026-7-30T12:00:02Z",
            }[fault]
        return _envelope(witness)

    case.producer.fresh_storage_envelope.side_effect = changed
    with pytest.raises(RuntimeError, match=reason):
        await _start(admission, case)
    assert case.ledger == [] and not admission._started
    case.producer.fhir._consume_provider_directory_profile_capacity_lease.assert_not_awaited()


async def test_fresh_phase_cannot_move_observed_storage_backwards(capacity_case):
    case = capacity_case
    admission = await _admission(case)
    await _start(admission, case)
    authority = case.producer.fresh_storage_envelope.side_effect

    async def older(request):
        envelope = await authority(request)
        witness = deepcopy(envelope["observation"])
        witness["storage_observation"]["observed_at"] = continuation._utc(
            VALIDATION_TIME - datetime.timedelta(seconds=1)
        )
        return _envelope(witness)

    case.producer.fresh_storage_envelope.side_effect = older
    before = deepcopy(case.ledger)
    with pytest.raises(RuntimeError, match="nonprofile_capacity_fresh_observation_required"):
        await admission.check_phase(NonprofileAdmissionCheck("readiness", admission.lease, admission.plan, ()))
    assert case.ledger == before and len(admission.check_phase.__self__.continuation_digests) == 1


async def test_phase_expiry_after_physical_check_does_not_issue_readiness(capacity_case):
    case = capacity_case
    admission = await _admission(case)
    await _start(admission, case)
    case.producer.fhir._profile_capacity_preflight_clock.side_effect = [
        VALIDATION_TIME,
        VALIDATION_TIME,
        VALIDATION_TIME + datetime.timedelta(seconds=30),
    ]
    before = deepcopy(case.ledger)
    with pytest.raises(RuntimeError, match="nonprofile_capacity_continuation_expired_during_check"):
        await admission.check_phase(NonprofileAdmissionCheck("readiness", admission.lease, admission.plan, ()))
    assert case.ledger == before and len(admission.check_phase.__self__.continuation_digests) == 1


async def test_retired_lease_key_cannot_sign_a_fresh_phase(capacity_case, monkeypatch):
    case = capacity_case
    admission = await _admission(case)
    await _start(admission, case)
    trust = _trust()
    retired = replace(
        trust.keys[0],
        status="retired",
        retired_at=VALIDATION_TIME,
        verify_until=case.producer.lease.expires_at,
    )
    active = replace(trust.keys[0], key_id="capacity-key-next")
    rotated = replace(trust, active_key_id=active.key_id, keys=(retired, active))
    monkeypatch.setattr(capacity.capacity_runtime, "configured_capacity_lease_trust", lambda: rotated)
    before = deepcopy(case.ledger)
    with pytest.raises(RuntimeError, match="storage_continuation_authority_not_active"):
        await admission.check_phase(NonprofileAdmissionCheck("readiness", admission.lease, admission.plan, ()))
    assert case.ledger == before and len(admission.check_phase.__self__.continuation_digests) == 1


async def test_invalid_engine_clock_stops_before_storage_request(capacity_case):
    case = capacity_case
    admission = await _admission(case)
    case.producer.fhir._profile_capacity_preflight_clock.return_value = VALIDATION_TIME.replace(tzinfo=None)
    with pytest.raises(RuntimeError, match="storage_continuation_time_invalid"):
        await _start(admission, case)
    case.producer.fresh_storage_envelope.assert_not_awaited()
    assert case.ledger == []


async def test_profile_pause_rejects_wal_overrun_without_suspending(capacity_case):
    case = capacity_case
    admission = await _admission(case)
    profile = SimpleNamespace(
        run_id=case.producer.run_id,
        lease=case.producer.profile_lease,
        wal_tracker=SimpleNamespace(lock=asyncio.Lock()),
        geometry=SimpleNamespace(reservation_bytes_by_storage_class={"wal": 100}),
    )
    case.producer.fhir._provider_directory_profile_current_wal_bytes = AsyncMock(return_value=101)
    with pytest.raises(RuntimeError, match="nonprofile_capacity_profile_admission_wal_invalid"):
        await admission.pause_profile(profile)
    assert admission.check_phase.__self__.paused_profile is None and case.ledger == []


async def test_signed_preflight_runtime_must_match_actual_admission(capacity_case):
    case = capacity_case
    guard = deepcopy(case.assigned_envelope["lease"]["signing_preflight_guard"])
    guard["healthcare_receipt"]["runtime_observation"]["healthcare_source_commit"] = "f" * 40
    rehash_guard(guard)
    case.assigned_envelope = sign_guard(guard, cms=True)
    old_nonce = case.producer.lease.nonce
    case.producer.lease = _verify(
        case.assigned_envelope, expected_capacity_geometry_hash=case.producer.plan.capacity_geometry_hash
    )
    del case.rows[old_nonce]
    request = preflight.validated_capacity_preflight_request(guard["healthcare_request"])
    case.rows[case.producer.lease.nonce] = capacity_contract.capacity_preflight_receipt_row_values(
        request,
        guard["healthcare_receipt"],
        issued_at=preflight._utc_timestamp(guard["healthcare_receipt"]["issued_at"]),
    )
    admission = await _admission(case)
    before = deepcopy(case.rows)
    with pytest.raises(RuntimeError, match="nonprofile_capacity_preflight_runtime_changed"):
        await _start(admission, case)
    assert case.rows == before and case.ledger == []
    case.producer.fhir._mark_profile_capacity_receipt_consumed.assert_not_awaited()
    case.producer.fhir._consume_provider_directory_profile_capacity_lease.assert_not_awaited()


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("vector", "cms_npd_rollback_vector_changed"),
        ("missing_parent", "cms_npd_rollback_prior_publication_missing"),
        ("unsealed_parent", "cms_npd_rollback_prior_publication_not_superseded"),
        ("superseded_current", "cms_npd_candidate_identity_changed"),
    ],
)
async def test_reacquisition_requires_exact_prior_publication(npd_case, fault, reason):
    case = npd_case
    task_dict = {"cms_npd_rollback_vector_sha256": case.identity["vector_sha256"]}
    case.fhir.db.first.return_value = ("prior-synthetic",)
    if fault == "vector":
        task_dict["cms_npd_rollback_vector_sha256"] = "f" * 64
    elif fault == "missing_parent":
        case.fhir._endpoint_dataset_state.return_value = None
    elif fault == "unsealed_parent":
        case.state["status"] = fhir.ENDPOINT_DATASET_VALIDATED
    else:
        task_dict = {}
        case.state.update(status=fhir.ENDPOINT_DATASET_SUPERSEDED, is_current=True, published_at="published-synthetic")
    with pytest.raises(RuntimeError, match=reason):
        await cms._admission_candidate(case.fhir, case.candidate.endpoint_id, "run-synthetic", case.identity, task_dict)
    case.fhir._initialize_endpoint_dataset_candidate.assert_not_awaited()
    case.fhir._persist_endpoint_dataset_rows.assert_not_awaited()
    case.fhir.db.status.assert_not_awaited()


@pytest.mark.parametrize(
    "body,reason",
    [
        (b"{invalid}\n", "cms_npd_ndjson_invalid"),
        (b"null\n", "cms_npd_resource_invalid"),
        (b"x" * 65, "cms_npd_decoded_size_invalid"),
    ],
)
async def test_identity_replay_rejects_invalid_retained_entity_before_binding(
    npd_case, monkeypatch, tmp_path, body, reason
):
    case = npd_case
    first = next(name for name, kind in cms.source.RESOURCE_FILES if kind == "Organization")
    (tmp_path / (first + ".zst")).write_bytes(zstd.compress(body))
    monkeypatch.setattr(cms.source, "MAX_RESOURCE_LINE_BYTES", 64)
    write, complete = AsyncMock(), AsyncMock()
    monkeypatch.setattr(cms, "_write_identity_batch", write)
    monkeypatch.setattr(cms, "_assert_identity_evidence", complete)
    with pytest.raises(cms.source.CmsNpdSourceError, match=reason):
        await cms._materialize_identity_evidence(case.fhir, tmp_path, case.candidate, case.identity, {}, {})
    write.assert_not_awaited()
    complete.assert_not_awaited()
    case.fhir._finalize_endpoint_dataset_candidate.assert_not_awaited()


async def test_identity_replay_flushes_complete_bounded_batches_without_losing_remainder(
    npd_case, monkeypatch, tmp_path
):
    case = npd_case
    for name, kind in cms.source.RESOURCE_FILES:
        size = 3 if kind == "Organization" else 1
        resources = [{"resourceType": kind, "id": f"resource-{index}"} for index in range(size)]
        (tmp_path / (name + ".zst")).write_bytes(
            zstd.compress(b"".join(json.dumps(resource).encode() + b"\n" for resource in resources))
        )
        case.identity["files"][name].update(row_count=size, distinct_count=size)
    monkeypatch.setattr(cms, "IDENTITY_BATCH_SIZE", 2)
    batches = []

    async def write(_fhir, _entity, _network, _resource, kind, resources, identity):
        assert identity is case.identity
        batches.append((kind, deepcopy(resources)))

    complete = AsyncMock()
    refresh = AsyncMock()
    monkeypatch.setattr(cms, "_write_identity_batch", write)
    monkeypatch.setattr(cms, "_assert_identity_evidence", complete)
    monkeypatch.setattr(cms, "_refresh_identity_statistics", refresh)
    ctx, task = {}, {}
    await cms._materialize_identity_evidence(case.fhir, tmp_path, case.candidate, case.identity, ctx, task)
    organization_resources = [resources for kind, resources in batches if kind == "Organization"]
    assert [len(resources) for resources in organization_resources] == [2, 1]
    assert [resource["id"] for resources in organization_resources for resource in resources] == [
        "resource-0",
        "resource-1",
        "resource-2",
    ]
    case.fhir._raise_if_resource_import_cancelled.assert_awaited_once_with(ctx, task)
    assert case.fhir._raise_if_resource_import_cancelled.await_args.args[0] is ctx
    assert case.fhir._raise_if_resource_import_cancelled.await_args.args[1] is task
    refresh.assert_awaited_once_with(case.fhir, ctx, task)
    complete.assert_awaited_once_with(case.fhir, case.candidate, case.identity)


async def test_stream_cancellation_stops_after_committed_bounded_batch(npd_case, monkeypatch, tmp_path):
    case = npd_case
    rows = [{"resourceType": "Location", "id": f"site-{index}"} for index in range(3)]
    path = tmp_path / "source.zst"
    path.write_bytes(zstd.compress(b"".join(json.dumps(row).encode() + b"\n" for row in rows)))
    batches = []

    async def write(_fhir, _model, _rows, raw, _candidate, _kind):
        batches.append(deepcopy(raw))

    monkeypatch.setattr(cms, "BATCH_SIZE", 2)
    monkeypatch.setattr(cms, "_persist_source_batch", write)
    case.fhir._raise_if_resource_import_cancelled.side_effect = RuntimeError("synthetic_cancelled")
    with pytest.raises(RuntimeError, match="synthetic_cancelled"):
        await cms._stream_file(case.fhir, path, case.candidate, "Location", {}, {})
    assert batches == [rows[:2]]
    case.fhir._finalize_endpoint_dataset_candidate.assert_not_awaited()


@pytest.fixture
def stage_case(npd_case, monkeypatch):
    case = npd_case
    for name in (
        "db",
        "_schema",
        "_qt",
        "_endpoint_dataset_state",
        "_current_endpoint_dataset_id",
        "_raise_if_resource_import_cancelled",
        "_finalize_endpoint_dataset_candidate",
    ):
        monkeypatch.setattr(fhir, name, getattr(case.fhir, name))
    monkeypatch.setattr(cms, "_verify_release", AsyncMock())
    monkeypatch.setattr(cms, "_admission_candidate", AsyncMock(return_value=case.candidate))
    monkeypatch.setattr(recovery, "resume_pending_cleanup", AsyncMock())
    monkeypatch.setattr(recovery, "dispose_prior_vectors", AsyncMock())
    return case


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("resource_counts", "cms_npd_candidate_counts_incomplete"),
        ("entity_binding", "cms_npd_identity_evidence_incomplete"),
        ("resource_binding", "cms_npd_identity_evidence_incomplete"),
        ("network_role", "cms_npd_identity_evidence_incomplete"),
    ],
)
async def test_sealed_replay_requires_complete_identity_before_relationships(
    stage_case, monkeypatch, tmp_path, fault, reason
):
    case = stage_case
    candidate = replace(case.candidate, already_validated=True)
    cms._admission_candidate.return_value = candidate
    counts = [{"resource_type": kind, "row_count": 1} for kind in cms.RESOURCE_TYPES]
    if fault == "resource_counts":
        counts.pop()
    case.fhir.db.all.side_effect = [counts, [(kind, 1, 1) for kind in cms.RESOURCE_TYPES], []]
    observations = [1, 1, 1, 1, 0, 0, 0]
    if fault == "entity_binding":
        observations[0] = 0
    elif fault == "resource_binding":
        observations[2] = 0
    elif fault == "network_role":
        observations[-1] = 1
    case.fhir.db.scalar.side_effect = observations
    relationships = AsyncMock()
    prepare = AsyncMock()
    monkeypatch.setattr(cms.relationships, "materialize", relationships)
    monkeypatch.setattr(coverage, "prepare_cms_candidate_coverage", prepare)
    before = deepcopy(case.state)
    with pytest.raises(RuntimeError, match=reason):
        await cms._stage_acquired({}, {}, "run-synthetic", tmp_path, _receipt(), None, candidate.endpoint_id)
    relationships.assert_not_awaited()
    prepare.assert_not_awaited()
    case.fhir._finalize_endpoint_dataset_candidate.assert_not_awaited()
    assert case.state == before and case.fhir.db.binding is None


async def test_new_staging_row_count_change_stops_before_vector_validation(stage_case, monkeypatch, tmp_path):
    case = stage_case
    name, kind = cms.source.RESOURCE_FILES[0]
    rows = [{"resourceType": kind, "id": f"resource-{index}"} for index in range(2)]
    (tmp_path / (name + ".zst")).write_bytes(zstd.compress(b"".join(json.dumps(row).encode() + b"\n" for row in rows)))
    writer = AsyncMock()
    monkeypatch.setattr(cms, "_persist_source_batch", writer)
    with pytest.raises(RuntimeError, match="cms_npd_file_row_count_changed"):
        await cms._stage_acquired({}, {}, "run-synthetic", tmp_path, _receipt(), None, case.candidate.endpoint_id)
    assert writer.await_count == 1 and len(writer.call_args.args[3]) == 2
    case.fhir.db.all.assert_not_awaited()
    case.fhir._finalize_endpoint_dataset_candidate.assert_not_awaited()


@pytest.mark.parametrize(
    "references",
    [
        ["https://example.test/Organization/external"],
        ["Organization/unresolved"],
    ],
)
async def test_plan_identity_sends_unresolved_references_unchanged_to_bulk_validation(npd_case, references):
    case = npd_case

    @asynccontextmanager
    async def session():
        async with AsyncSession() as current:
            yield current

    transaction_states = []

    async def record_network_batch(writer_session, **_fields):
        transaction_states.append(writer_session.in_transaction())

    case.fhir.db.session = session
    network = SimpleNamespace(record_insurance_network_batch=AsyncMock(side_effect=record_network_batch))
    resource = SimpleNamespace(bind_resource_identity_batch=AsyncMock())
    plan_dict = {
        "resourceType": "InsurancePlan",
        "id": "plan-synthetic",
        "network": [{"reference": reference} for reference in references],
    }
    await cms._write_identity_batch(case.fhir, None, network, resource, "InsurancePlan", [plan_dict], case.identity)
    resource.bind_resource_identity_batch.assert_awaited_once()
    network.record_insurance_network_batch.assert_awaited_once()
    assert transaction_states == [True]
    assert network.record_insurance_network_batch.await_args.kwargs == {
        "source_id": cms.SOURCE_ID,
        "release_id": case.identity["vector_sha256"],
        "resource_type": "InsurancePlan",
        "resources": [plan_dict],
    }
    case.fhir.db.all.assert_not_awaited()


async def test_failed_finalization_cannot_produce_serving_candidate(stage_case, monkeypatch, tmp_path):
    case = stage_case
    for name, kind in cms.source.RESOURCE_FILES:
        row_dict = {"resourceType": kind, "id": "resource-1"}
        (tmp_path / (name + ".zst")).write_bytes(zstd.compress(json.dumps(row_dict).encode() + b"\n"))
    case.fhir.db.all.side_effect = [
        [{"resource_type": kind, "row_count": 1} for kind in cms.RESOURCE_TYPES],
        [(kind, 1, 1) for kind in cms.RESOURCE_TYPES],
    ]
    case.fhir._finalize_endpoint_dataset_candidate.return_value = {"validated": False}
    monkeypatch.setattr(cms, "_persist_source_batch", AsyncMock())
    monkeypatch.setattr(cms, "_materialize_identity_evidence", AsyncMock())
    monkeypatch.setattr(cms.relationships, "materialize", AsyncMock())
    prepare = AsyncMock()
    monkeypatch.setattr(coverage, "prepare_cms_candidate_coverage", prepare)
    with pytest.raises(RuntimeError, match="cms_npd_candidate_validation_failed"):
        await cms._stage_acquired({}, {}, "run-synthetic", tmp_path, _receipt(), None, case.candidate.endpoint_id)
    diagnostics = case.fhir._finalize_endpoint_dataset_candidate.call_args.args[1]
    assert set(diagnostics) == set(cms.RESOURCE_TYPES)
    assert all(row_dict["rows_fetched"] == row_dict["rows_written"] == 1 for row_dict in diagnostics.values())
    prepare.assert_not_awaited()


async def test_preparation_rejects_candidate_without_hash_before_coverage(npd_case, monkeypatch, tmp_path):
    case = npd_case
    case.state["dataset_hash"] = None
    monkeypatch.setattr(cms, "_verify_release", AsyncMock())
    prepare = AsyncMock()
    monkeypatch.setattr(coverage, "prepare_cms_candidate_coverage", prepare)
    with pytest.raises(RuntimeError, match="cms_npd_coverage_candidate_hash_invalid"):
        await cms._prepare_serving_candidate(
            case.fhir, case.candidate, case.identity, tmp_path, _receipt(), None, {}, {}
        )
    prepare.assert_not_awaited()
    case.fhir._persist_endpoint_dataset_rows.assert_not_awaited()


async def test_current_replay_rejects_coverage_for_another_dataset_hash(npd_case, monkeypatch):
    case = npd_case
    case.state.update(
        status=fhir.ENDPOINT_DATASET_PUBLISHED,
        is_current=True,
        published_at="published-synthetic",
        validated_at="validated-synthetic",
        resource_count=8,
    )
    case.fhir._current_endpoint_dataset_id.return_value = case.candidate.dataset_id
    monkeypatch.setattr(recovery, "resume_pending_cleanup", AsyncMock())
    monkeypatch.setattr(recovery, "dispose_prior_vectors", AsyncMock())
    proof_dict = {name: case.state[name] for name in ("dataset_id", "endpoint_id", "dataset_hash")}
    proof_dict.update(dataset_hash="f" * 64, release_id=case.identity["vector_sha256"], proof_version=2)
    monkeypatch.setattr(coverage, "prepare_cms_candidate_coverage", AsyncMock(return_value=proof_dict))
    before = deepcopy(case.state)
    with pytest.raises(RuntimeError, match="cms_npd_candidate_coverage_changed"):
        await cms._prepare_current_serving_candidate(case.fhir, case.state, case.identity["vector_sha256"])
    assert case.state == before
    case.fhir._persist_endpoint_dataset_rows.assert_not_awaited()
    case.fhir._finalize_endpoint_dataset_candidate.assert_not_awaited()


@pytest.mark.parametrize("fault", ["coverage_unavailable", "another_release"])
async def test_current_observation_does_not_admit_uncovered_or_other_release(npd_case, monkeypatch, fault):
    case = npd_case
    accepted = importlib.import_module("api.provider_directory_cms_generation")
    contract = importlib.import_module("api.provider_directory_entities_contract")

    @asynccontextmanager
    async def session():
        yield object()

    monkeypatch.setattr(fhir, "db", SimpleNamespace(session=session))
    monkeypatch.setattr(fhir, "_schema", case.fhir._schema)
    monkeypatch.setattr(fhir, "_endpoint_dataset_state", case.fhir._endpoint_dataset_state)
    generation_dict = {
        "dataset_id": case.candidate.dataset_id,
        "dataset_hash": case.state["dataset_hash"],
        "release_id": case.identity["vector_sha256"],
        "observed_at": "published-synthetic",
    }
    if fault == "another_release":
        generation_dict["release_id"] = "f" * 64
    monkeypatch.setattr(accepted, "accepted_cms_generation", AsyncMock(return_value=generation_dict))
    covered = AsyncMock(
        side_effect=contract.DirectoryReadError("unavailable") if fault == "coverage_unavailable" else None
    )
    monkeypatch.setattr(coverage, "require_cms_coverage", covered)
    assert await cms._current_observed_publication(_observation()) is None
    covered.assert_awaited_once()
    case.fhir._endpoint_dataset_state.assert_not_awaited()
    case.fhir._persist_endpoint_dataset_rows.assert_not_awaited()


async def test_intake_connects_then_releases_backend_with_lost_lock_before_staging():
    engine = object()
    connection = SimpleNamespace(scalar=AsyncMock(side_effect=[123, False]), commit=AsyncMock())
    database = SimpleNamespace(engine=None)

    async def connect():
        database.engine = engine

    database.connect = AsyncMock(side_effect=connect)
    importer = SimpleNamespace(
        db=database,
        _provider_directory_database_pool_capacity=lambda: 2,
        _acquire_provider_directory_artifact_build_lock=AsyncMock(return_value=connection),
        _release_provider_directory_artifact_build_lock=AsyncMock(),
    )
    has_staged = False
    with pytest.raises(RuntimeError, match="cms_npd_intake_guard_lost"):
        async with cms._intake_guard(importer, "endpoint-synthetic"):
            has_staged = True
    assert not has_staged
    database.connect.assert_awaited_once()
    importer._acquire_provider_directory_artifact_build_lock.assert_awaited_once_with(
        engine, "provider-directory-cms-intake:endpoint-synthetic"
    )
    importer._release_provider_directory_artifact_build_lock.assert_awaited_once_with(
        connection, "provider-directory-cms-intake:endpoint-synthetic"
    )


@pytest.mark.parametrize(
    "configured,reason",
    [
        (False, "cms_npd_artifact_root_required"),
        (True, "cms_npd_artifact_root_unavailable"),
    ],
)
async def test_import_rejects_unavailable_durable_root_before_intake(monkeypatch, tmp_path, configured, reason):
    if configured:
        monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(tmp_path / "missing-durable-root"))
    else:
        monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", raising=False)
    intake = AsyncMock()
    monkeypatch.setattr(cms, "_run_upstream_check", intake)
    with pytest.raises(ValueError, match=reason):
        await cms.run({}, {"import_resources": True, "full_refresh": True}, "run-synthetic")
    intake.assert_not_awaited()


@pytest.mark.parametrize(
    "task,reason",
    [
        ({"resources": 42}, "cms_npd_resource_scope_incomplete"),
        ({"cms_npd_rollback_vector_sha256": 42}, "cms_npd_rollback_vector_invalid"),
        ({"cms_npd_rollback_root_run_id": "root-synthetic"}, "cms_npd_rollback_root_invalid"),
    ],
)
async def test_import_rejects_incoherent_scope_and_rollback_before_intake(monkeypatch, task, reason):
    intake = AsyncMock()
    monkeypatch.setattr(cms, "_run_upstream_check", intake)
    with pytest.raises(ValueError, match=reason):
        await cms.run({}, {"import_resources": True, "full_refresh": True, **task}, "run-synthetic")
    intake.assert_not_awaited()


async def test_published_replay_keeps_serving_result_when_optional_tax_retry_fails(stage_case, monkeypatch, tmp_path):
    case = stage_case
    candidate = replace(case.candidate, already_published=True)
    cms._admission_candidate.return_value = candidate
    case.state.update(
        status=fhir.ENDPOINT_DATASET_PUBLISHED,
        is_current=True,
        published_at="published-synthetic",
        validated_at="validated-synthetic",
        resource_count=8,
    )
    case.fhir._current_endpoint_dataset_id.return_value = candidate.dataset_id
    case.fhir.db.all.side_effect = [
        [{"resource_type": kind, "row_count": 1} for kind in cms.RESOURCE_TYPES],
        [(kind, 1, 1) for kind in cms.RESOURCE_TYPES],
        [],
    ]
    case.fhir.db.scalar.side_effect = [1, 1, 1, 1, 0, 0, 0]
    monkeypatch.setattr(cms.relationships, "materialize", AsyncMock())
    proof_dict = {name: case.state[name] for name in ("dataset_id", "endpoint_id", "dataset_hash")}
    proof_dict.update(release_id=case.identity["vector_sha256"], proof_version=2)
    monkeypatch.setattr(coverage, "prepare_cms_candidate_coverage", AsyncMock(return_value=proof_dict))
    tax = importlib.import_module("process.cms_npd_tax_candidate_followup")
    monkeypatch.setattr(tax, "cms_npd_tax_candidate_followup", AsyncMock(side_effect=RuntimeError("synthetic_retry")))
    replayed_admission = AsyncMock(return_value={"dataset_hash": case.state["dataset_hash"]})
    monkeypatch.setattr(cms, "_replayed_registry_admission", replayed_admission)
    before = deepcopy(case.state)
    replay_result = await cms._stage_acquired(
        {}, {}, "run-synthetic", tmp_path, _receipt(), None, candidate.endpoint_id
    )
    assert replay_result["status"] == "published" and replay_result["replayed"] is True
    assert replay_result["cms_serving_candidate"]["status"] == "ready"
    assert replay_result["tax_candidates"] == {"status": "failed", "retryable": True, "retry_via": "same_byte_import"}
    replayed_admission.assert_awaited_once_with(fhir, candidate)
    assert case.state == before
    case.fhir._persist_endpoint_dataset_rows.assert_not_awaited()
    case.fhir._finalize_endpoint_dataset_candidate.assert_not_awaited()


@pytest.fixture
def retained_case(tmp_path):
    manifest, payloads = _source()
    client, calls = _client(manifest, payloads)
    with client:
        directory, receipt = cms.source.acquire_release(
            tmp_path, client=client, base_url="https://example.test/downloads"
        )
    return SimpleNamespace(
        root=tmp_path, directory=directory, receipt=receipt, manifest=manifest, payloads=payloads, calls=calls
    )


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("manifest_json", "cms_npd_retained_release_invalid"),
        ("files", "cms_npd_receipt_invalid"),
        ("etag", "cms_npd_receipt_invalid"),
        ("date", "cms_npd_source_vector_changed"),
    ],
)
async def test_retained_replay_rejects_incomplete_local_seal_without_downloading(
    retained_case, monkeypatch, fault, reason
):
    case = retained_case
    if fault == "manifest_json":
        (case.directory / "manifest.json").write_text("{invalid}")
    else:
        if fault == "files":
            case.receipt["files"].pop(next(iter(case.receipt["files"])))
        elif fault == "etag":
            case.receipt["files"][next(iter(case.receipt["files"]))]["etag"] = "unquoted"
        else:
            case.receipt["generated_at"] = "2026-09-25"
        (case.directory / "receipt.json").write_text(json.dumps(case.receipt))
    network_client = Mock(side_effect=AssertionError("retained replay must not create a network client"))
    monkeypatch.setattr(cms.source.httpx, "Client", network_client)
    with pytest.raises(cms.source.CmsNpdSourceError, match=reason):
        cms.source.load_retained_release(case.root, case.directory.name)
    network_client.assert_not_called()
    assert all((case.directory / (name + ".zst")).is_file() for name, _kind in cms.source.RESOURCE_FILES)


@pytest.mark.parametrize("fault", ["missing", "invalid_json", "nonobject"])
async def test_rollback_loader_requires_object_receipt_before_local_verification(retained_case, fault):
    case = retained_case
    path = case.directory / "receipt.json"
    if fault == "missing":
        path.unlink()
    else:
        path.write_text("{invalid}" if fault == "invalid_json" else "[]")
    with pytest.raises(cms.source.CmsNpdSourceError, match="cms_npd_receipt_invalid"):
        cms.source.load_retained_release(case.root, case.directory.name)
    assert all((case.directory / (name + ".zst")).is_file() for name, _kind in cms.source.RESOURCE_FILES)


async def test_upstream_vector_change_preserves_the_verified_retained_release(retained_case):
    case = retained_case
    manifest_dict = {**case.manifest, "generated_at": "2026-09-25"}
    client, calls = _client(manifest_dict, case.payloads)
    before = (case.directory / "receipt.json").read_bytes()
    with client:
        with pytest.raises(cms.source.CmsNpdSourceError, match="cms_npd_source_vector_changed"):
            cms.source.verify_release(
                case.directory, case.receipt, client=client, base_url="https://example.test/downloads"
            )
    assert (case.directory / "receipt.json").read_bytes() == before
    assert not any(kind == "download" for _name, kind in calls)


async def test_acquisition_rejects_symlinked_root_without_creating_release(tmp_path):
    durable = tmp_path / "durable-synthetic"
    durable.mkdir()
    alias = tmp_path / "alias-synthetic"
    alias.symlink_to(durable, target_is_directory=True)
    manifest, payloads = _source()
    client, calls = _client(manifest, payloads)
    with client:
        with pytest.raises(cms.source.CmsNpdSourceError, match="cms_npd_artifact_path_unsafe"):
            cms.source.acquire_release(alias, client=client, base_url="https://example.test/downloads")
    assert list(durable.iterdir()) == []
    assert not any(kind == "download" for _name, kind in calls)


async def test_stale_acquiring_parent_must_transition_exactly_once_before_cleanup(npd_case):
    case = npd_case
    case.fhir.db.first.side_effect = [case.state, None]
    case.fhir.db.status.side_effect = [1, 0]
    with pytest.raises(RuntimeError, match="cms_npd_stale_disposition_lost"):
        await recovery.dispose_changed_vector(case.fhir, case.candidate, case.identity)
    assert case.fhir.db.status.await_count == 2
    assert all("DELETE" not in call.args[0] for call in case.fhir.db.status.await_args_list)
    case.fhir.delete_dataset_proof_shards.assert_not_awaited()
    assert case.fhir.db.binding is None


async def _exercise_cutover_fault(admission, owner, case, fault):
    if fault == "nested_owner":
        async with admission.cutover_operation():
            pytest.fail("nested publication owner was admitted")
    if fault == "naive_clock":
        case.producer.fhir.db.scalar.side_effect = lambda query, **_params: (
            VALIDATION_TIME.replace(tzinfo=None) if "clock_timestamp" in query else 0
        )
    if fault == "profile_context":
        owner.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
            SimpleNamespace(run_id=owner.run_id, lease=owner.profile_lease)
        )
    await admission.assert_cutover_complete()


@pytest.mark.parametrize(
    "fault,reason",
    [
        ("nested_owner", "nonprofile_capacity_cutover_operation_changed"),
        ("naive_clock", "nonprofile_capacity_continuation_expired_during_check"),
        ("profile_context", "nonprofile_capacity_paired_profile_admission_changed"),
    ],
)
async def test_publication_rechecks_one_local_owner_after_honest_cutover(capacity_case, fault, reason):
    case = capacity_case
    admission = await _admission(case)
    await _start(admission, case)
    owner = admission.check_phase.__self__
    profile = SimpleNamespace(run_id=owner.run_id, lease=owner.profile_lease)
    owner.resumed_profile = profile
    owner.preparation_wal_end_bytes = 0
    original_first = case.producer.fhir.db.first.side_effect

    async def first(query, **params):
        if "relation_ref" in params:
            return {"oid": 42, "total_bytes": 100, "persistence": "p"}
        return await original_first(query, **params)

    case.producer.fhir.db.first.side_effect = first
    case.producer.fhir._provider_directory_profile_relation_storage_fingerprint = AsyncMock(
        return_value=SimpleNamespace(relation_oid=42, effective_tablespace_oids=(1663,))
    )
    case.producer.fhir.db.scalar.side_effect = lambda query, **_params: (
        VALIDATION_TIME if "clock_timestamp" in query else 0
    )
    await admission.measure(case.producer.fhir, "synthetic", ("staged_serving",))
    token = owner.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(profile)
    before = deepcopy(case.ledger)
    try:
        async with admission.publication(case.producer.fhir, "synthetic"):
            async with case.producer.fhir.db.transaction():
                await admission.assert_cutover_complete()
                assert owner._cutover_binding is case.producer.fhir.db.binding
                with pytest.raises(RuntimeError, match=reason):
                    await _exercise_cutover_fault(admission, owner, case, fault)
    finally:
        owner.fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)
    assert case.ledger == before and len(case.ledger) == 1
    assert not admission._cutover_active and not owner._cutover_active
    assert owner._cutover_witness is None and owner._cutover_binding is None
    assert case.producer.fhir.db.binding is None
    assert case.producer.fresh_storage_envelope.await_count == 2
    case.producer.fhir._consume_provider_directory_profile_capacity_lease.assert_awaited_once()


async def test_readiness_rejects_wal_meter_before_the_admitted_start(capacity_case):
    case = capacity_case
    admission = await _admission(case)
    await _start(admission, case)
    case.producer.fhir.db.scalar.return_value = -1
    before = deepcopy(case.ledger)
    with pytest.raises(RuntimeError, match="nonprofile_capacity_wal_observation_invalid"):
        await admission.check_phase(NonprofileAdmissionCheck("readiness", admission.lease, admission.plan, ()))
    assert case.ledger == before and len(admission.check_phase.__self__.continuation_digests) == 1
