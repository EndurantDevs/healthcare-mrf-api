# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Production routing, snapshot ownership and complete-preparation commit ordering."""

import contextvars
import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_address as cms_address
from process import provider_directory_cms_serving as serving
from tests.provider_directory_cms_capacity_test_support import (
    cms_execution,
    cms_guard,
    cms_plan,
    rehash_guard,
    sign_guard,
)
from tests.test_provider_directory_cms_address import address_build_case as address_build_case
from tests.test_provider_directory_cms_preflight import _retention_policy
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME, _trust, _verify


class _Database:
    def __init__(self, events):
        self.events, self.binding = events, None
        self.session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=False))

    def _transaction_binding(self):
        return self.binding

    @asynccontextmanager
    async def transaction(self):
        assert self.binding is None
        self.binding = SimpleNamespace(session=self.session)
        self.events.append("snapshot-start")
        try:
            yield self.session
        finally:
            self.binding = None
            self.events.append("snapshot-end")


def _fhir(events):
    fhir = SimpleNamespace(
        db=_Database(events),
        _schema=lambda: "synthetic",
        _qt=lambda schema, table: f'"{schema}"."{table}"',
        PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS=("profile", "corroboration", "address_overlay"),
        _PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION=contextvars.ContextVar("serving-test", default=None),
        assert_registered_profile_selection_current=AsyncMock(),
        _provider_directory_profile_selection_catalog=lambda: {},
        _validated_admission_run_id=lambda run, control, execution: run,
        _attach_profile_selection_result=Mock(),
        _record_artifact_promotion_metrics=AsyncMock(side_effect=lambda _, result, count: result),
        ProviderDirectoryArtifactDatasetFence=lambda datasets: SimpleNamespace(datasets=datasets, source_ids=()),
    )

    @asynccontextmanager
    async def suppress(run_id):
        events.append("heartbeat-paused")
        try:
            yield
        finally:
            events.append("heartbeat-restored")

    fhir.suppress_control_run_heartbeat_persistence = suppress
    return fhir


@pytest.fixture
def execution(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    guard = cms_guard()
    return replace(
        cms_execution(),
        capacity_attestation=guard["healthcare_request"]["cms_nonprofile_admission"]["paired_profile_lease"],
        cms_nonprofile_capacity_attestation=sign_guard(guard, cms=True),
    )


def test_plan_comes_from_exact_signed_pair(execution):
    envelope, plan = serving._signed_plan(execution)
    assert envelope == execution.cms_nonprofile_capacity_attestation
    assert plan == cms_plan()[0]
    with pytest.raises(ValueError):
        serving._signed_plan(replace(execution, capacity_attestation={}))


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [None, "cms", "profile", "expired"])
async def test_capture_limits_verify_both_original_signatures(monkeypatch, execution, invalid):
    from process.provider_directory_profile_capacity_attestation_contract import (
        CAPACITY_LEASE_DIGEST_DOMAIN,
        _domain_hash,
    )

    plan = cms_plan()[0]
    if invalid == "profile":
        pair = deepcopy(execution.capacity_attestation)
        pair["signature"] = "A" * 86
        plan = replace(plan, paired_profile_lease_digest=_domain_hash(CAPACITY_LEASE_DIGEST_DOMAIN, pair))
        execution = replace(
            execution,
            capacity_attestation=pair,
            cms_nonprofile_capacity_attestation=sign_guard(cms_guard(plan, paired_envelope=pair), cms=True),
        )
    elif invalid == "cms":
        envelope = deepcopy(execution.cms_nonprofile_capacity_attestation)
        envelope["signature"] = "A" * 86
        execution = replace(execution, cms_nonprofile_capacity_attestation=envelope)
    now = VALIDATION_TIME.replace(hour=14) if invalid == "expired" else VALIDATION_TIME
    fhir = _fhir([])
    fhir._profile_capacity_preflight_clock = AsyncMock(return_value=now)
    monkeypatch.setattr(serving, "configured_capacity_lease_trust", _trust)
    if invalid:
        with pytest.raises(ValueError, match="invalid_signature|expired"):
            await serving._capture_publish_inputs(fhir, execution, "run_" + "a" * 32, {}, plan)
        assert fhir.db.events == []
    else:
        limits = await serving._verified_capture_limits(fhir, execution, plan)
        assert limits.lease.signature == execution.cms_nonprofile_capacity_attestation["signature"]
        assert limits.paired_profile_lease.lease_digest == plan.paired_profile_lease_digest
        assert limits.plan is plan


@pytest.mark.asyncio
async def test_capture_cannot_apply_unsigned_geometry_override(execution):
    fhir = _fhir([])
    plan = replace(cms_plan()[0], temp_file_limit_bytes_per_backend=2048)
    with pytest.raises(RuntimeError, match="capture_geometry_changed"):
        await serving._capture_publish_inputs(fhir, execution, "run_" + "a" * 32, {}, plan)
    assert fhir.db.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_fresh_desired_route_preserves_context_and_heartbeat(monkeypatch, execution, failure):
    events, result = [], {"profile": {"generation_id": "synthetic"}}
    fhir = _fhir(events)

    async def publish(*args, registry_source_invocation=None):
        assert registry_source_invocation is None
        assert fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is execution
        assert args[2:4] == ("run_" + "a" * 32,) * 2
        events.append("common-publish")
        if failure:
            raise RuntimeError("synthetic build failure")
        return result

    monkeypatch.setattr(serving, "_publish_cms", publish)
    ordinary = AsyncMock()
    monkeypatch.setattr(serving, "_ordinary_profile", ordinary)
    operation = serving.publish_current_attested_profile(
        fhir, run_id="run_" + "a" * 32, control_run_id="run_" + "a" * 32, execution=execution, metrics={}
    )
    if failure:
        with pytest.raises(RuntimeError, match="synthetic build failure"):
            await operation
        fhir._attach_profile_selection_result.assert_not_called()
    else:
        assert await operation is result
        fhir._attach_profile_selection_result.assert_called_once_with(execution, result)
    assert events == ["heartbeat-paused", "common-publish", "heartbeat-restored"]
    assert fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is None
    ordinary.assert_not_awaited()


@pytest.mark.asyncio
async def test_borrowed_transaction_cannot_start_preparation(execution):
    fhir = _fhir([])
    fhir.db.binding = object()
    with pytest.raises(RuntimeError, match="requires_own_transaction"):
        await serving.publish_current_attested_profile(
            fhir, run_id="run_" + "a" * 32, control_run_id="run_" + "a" * 32, execution=execution, metrics={}
        )
    assert fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("history", [False, True])
async def test_missing_current_receipt_is_only_bootstrap_without_history(monkeypatch, history):
    fhir = _fhir([])
    fhir.db.session.scalar.return_value = history
    monkeypatch.setattr(serving.receipts, "read_current_receipt", AsyncMock(return_value=None))
    if history:
        with pytest.raises(RuntimeError, match="current_receipt_unavailable"):
            await serving._current_predecessor(fhir, fhir.db.session)
    else:
        assert await serving._current_predecessor(fhir, fhir.db.session) is None


@pytest.mark.asyncio
async def test_contents_and_revisions_use_one_readonly_snapshot(monkeypatch, execution):
    events, fhir = [], None
    fhir = _fhir(events)
    outputs_by_name = dict(
        fence=object(), predecessor=object(), dependencies=object(), native_fence=object(), proof=object()
    )

    @asynccontextmanager
    async def bounded_sql(current, limits):
        assert current is fhir and limits is verified
        assert fhir.db.binding is not None
        events.append("bounded-start")
        try:
            yield
        finally:
            events.append("bounded-end")

    verified = SimpleNamespace(lease=object())
    outputs_by_name["capacity_lease"] = verified.lease
    monkeypatch.setattr(serving, "_verified_capture_limits", AsyncMock(return_value=verified))
    monkeypatch.setattr(serving, "remaining_build_seconds", AsyncMock(return_value=60))
    monkeypatch.setattr(serving, "nonprofile_sql_transaction", bounded_sql)
    apply_limit = AsyncMock(side_effect=lambda *_: events.append("signed-temp-cap"))
    monkeypatch.setattr(serving, "apply_temp_file_limit", apply_limit)

    def reader(name):
        async def observe(*args, **kwargs):
            assert fhir.db.binding is not None
            events.append(name)
            return outputs_by_name[name]

        return observe

    monkeypatch.setattr(serving, "prepare_desired_fence", reader("fence"))
    monkeypatch.setattr(serving, "_current_predecessor", reader("predecessor"))
    monkeypatch.setattr(serving.receipts, "capture_native_dependencies", reader("dependencies"))
    monkeypatch.setattr(serving, "capture_native_address_input_fence", reader("native_fence"))
    monkeypatch.setattr(serving, "_candidate_proof", reader("proof"))
    snapshot = await serving._capture_publish_inputs(fhir, execution, "run_" + "a" * 32, {}, cms_plan()[0])
    assert vars(snapshot) == outputs_by_name
    apply_limit.assert_awaited_once_with(fhir.db, cms_plan()[0].temp_file_limit_bytes_per_backend)
    assert events == [
        "snapshot-start",
        "signed-temp-cap",
        "bounded-start",
        "fence",
        "predecessor",
        "dependencies",
        "native_fence",
        "proof",
        "bounded-end",
        "snapshot-end",
    ]
    statements = [str(call.args[0]) for call in fhir.db.session.execute.await_args_list]
    assert statements == ["SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"]


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_preparation_lives_through_common_commit(monkeypatch, execution, changed):
    events, fhir = [], _fhir([])
    plan = cms_plan()[0]
    snapshot = SimpleNamespace(
        fence=object(),
        dependencies=object(),
        native_fence=object(),
        proof=object(),
        predecessor=None,
        capacity_lease=_verify(
            execution.cms_nonprofile_capacity_attestation, expected_capacity_geometry_hash=plan.capacity_geometry_hash
        ),
    )
    factory = SimpleNamespace(input_hash="00" * 32 if changed else plan.native_address_input_hash)
    admission = object()
    prepared = SimpleNamespace(address=object(), stages=(object(), object()))
    monkeypatch.setattr(serving, "_capture_publish_inputs", AsyncMock(return_value=snapshot))
    monkeypatch.setattr(serving, "configured_storage_continuation", lambda: object())
    monkeypatch.setattr(serving, "cms_address_preparation", lambda *args, **kwargs: factory)
    producer = AsyncMock(return_value=admission)
    monkeypatch.setattr(serving, "produce_nonprofile_admission", producer)

    @asynccontextmanager
    async def prepare(*args, **kwargs):
        assert fhir.db.binding is None
        assert kwargs["nonprofile_admission"] is admission
        assert kwargs["address_preparation"] is factory
        events.append("prepare")
        try:
            yield prepared
        finally:
            events.append("cleanup")

    async def commit(*args, **kwargs):
        assert events == ["prepare"] and args[2] is prepared
        assert kwargs["address"] is prepared.address
        assert kwargs["candidate_proof"] is snapshot.proof
        events.append("commit")
        return {"cms_serving": {"receipt_id": "synthetic"}}

    monkeypatch.setattr(serving, "prepare_serving_artifacts", prepare)
    monkeypatch.setattr(serving, "commit_prepared_serving_generation", commit)
    if changed:
        with pytest.raises(RuntimeError, match="admitted_inputs_changed"):
            await serving._publish_cms(fhir, execution, "run_" + "a" * 32, "run_" + "a" * 32, {})
        producer.assert_not_awaited()
        assert not events
    else:
        publication_by_field = await serving._publish_cms(fhir, execution, "run_" + "a" * 32, "run_" + "a" * 32, {})
        assert publication_by_field["cms_serving"]["receipt_id"] == "synthetic"
        assert events == ["prepare", "commit", "cleanup"]


def _changed_retention_policy(policy, change):
    """Change one valid declared coordinate while retaining the previously signed input hash."""
    changed_policy = deepcopy(policy)
    mutations_by_name = {
        "capture": (changed_policy, "capture_id", "00000000-0000-0000-0000-000000000002"),
        "source-oid": (
            changed_policy["source_pin"],
            "resource_table_oid",
            policy["source_pin"]["resource_table_oid"] + 1,
        ),
        "schema": (changed_policy["source_pin"], "schema_name", "other_schema"),
        "hash": (changed_policy["source_pin"], "dataset_sha256", "f" * 64),
        "date": (changed_policy["source_pin"], "as_of", "2026-07-31"),
        "selection": (changed_policy, "selection_proof_id", "f" * 64),
    }
    if change in mutations_by_name:
        policy_section, field_name, field_value = mutations_by_name[change]
        policy_section[field_name] = field_value
    if change == "schema":
        changed_policy["binding_coordinates"]["dataset_schema"] = "other_schema"
    return changed_policy


def _retained_execution(address_build_case, execution, change):
    """Sign a real closed request with a native input hash from the actual factory."""
    fhir = _fhir([])
    fhir._schema = lambda: "fixture"
    plan, fence, _profile = cms_plan()
    policy = _retention_policy(execution)
    bare = cms_address.cms_address_preparation(
        fhir,
        execution,
        fence,
        address_build_case[3],
        native_input_fence=address_build_case[4],
        run_id="run_" + "a" * 32,
        worker_count=plan.worker_count,
        temp_file_limit_bytes_per_backend=plan.temp_file_limit_bytes_per_backend,
    )
    retained = bare.with_registry_source_retention(policy)
    plan = replace(plan, native_address_input_hash=retained.input_hash, native_address_targets=retained.native_targets)
    guard = cms_guard(plan)
    changed_policy = _changed_retention_policy(policy, change)
    if change != "missing":
        for name in ("healthcare_request", "control_plane_request"):
            guard[name]["cms_nonprofile_admission"]["registry_source_retention"] = deepcopy(changed_policy)
    rehash_guard(guard)
    execution = replace(execution, cms_nonprofile_capacity_attestation=sign_guard(guard, cms=True))
    snapshot = SimpleNamespace(
        fence=fence,
        dependencies=address_build_case[3],
        native_fence=address_build_case[4],
        proof=object(),
        predecessor=None,
    )
    if change == "fence":
        next(dataset for dataset in fence.datasets if dataset.source_id == "cms-npd").dataset_hash = "f" * 64
    return fhir, execution, plan, snapshot, retained, policy


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", [None, "missing", "capture", "source-oid", "schema", "hash", "date", "selection", "fence"]
)
async def test_signed_retention_policy_is_reconstructed_before_admission_and_scratch(
    monkeypatch, execution, address_build_case, change
):
    fhir, execution, plan, snapshot, retained, policy = _retained_execution(address_build_case, execution, change)
    monkeypatch.setattr(serving, "_authenticated_source_job", AsyncMock(return_value=None))
    fhir._profile_capacity_preflight_clock = AsyncMock(return_value=VALIDATION_TIME)
    monkeypatch.setattr(serving, "configured_capacity_lease_trust", _trust)
    limits = await serving._verified_capture_limits(fhir, execution, plan)
    snapshot.capacity_lease = limits.lease
    capture = AsyncMock(return_value=snapshot)
    monkeypatch.setattr(serving, "_capture_publish_inputs", capture)
    monkeypatch.setattr(serving, "configured_storage_continuation", lambda: object())
    monkeypatch.setattr(serving, "cms_address_preparation", Mock(wraps=cms_address.cms_address_preparation))
    admission = AsyncMock(return_value=object())
    monkeypatch.setattr(serving, "produce_nonprofile_admission", admission)
    events = []

    @asynccontextmanager
    async def prepare(*args, **kwargs):
        factory = kwargs["address_preparation"]
        assert json.loads(factory.input_json)["registry_source_retention"] == policy
        assert factory.input_hash == retained.input_hash
        events.append("scratch")
        yield SimpleNamespace(address=object(), stages=())

    monkeypatch.setattr(serving, "prepare_serving_artifacts", prepare)
    monkeypatch.setattr(serving, "commit_prepared_serving_generation", AsyncMock(return_value={"published": True}))
    if change is not None:
        with pytest.raises((RuntimeError, ValueError), match="admitted_inputs_changed|retention_scope_changed"):
            await serving._publish_cms(fhir, execution, "run_" + "a" * 32, "run_" + "a" * 32, {})
        admission.assert_not_awaited()
        assert events == []
    else:
        assert await serving._publish_cms(fhir, execution, "run_" + "a" * 32, "run_" + "a" * 32, {}) == {
            "published": True
        }
        admission.assert_awaited_once()
        assert events == ["scratch"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["missing", "digest", "request"])
async def test_publish_requires_exact_verified_snapshot_request_before_factory(monkeypatch, execution, mismatch):
    plan = cms_plan()[0]
    lease = _verify(
        execution.cms_nonprofile_capacity_attestation, expected_capacity_geometry_hash=plan.capacity_geometry_hash
    )
    if mismatch == "missing":
        lease = None
    elif mismatch == "digest":
        lease = replace(lease, lease_digest="f" * 64)
    else:
        guard = deepcopy(lease.signing_preflight_guard)
        guard["healthcare_request"]["cms_nonprofile_admission"]["registry_source_retention"] = _retention_policy(
            execution
        )
        lease = replace(lease, signing_preflight_guard=guard)
    monkeypatch.setattr(
        serving, "_capture_publish_inputs", AsyncMock(return_value=SimpleNamespace(capacity_lease=lease))
    )
    monkeypatch.setattr(serving, "configured_storage_continuation", lambda: object())
    factory, admission, scratch = Mock(), AsyncMock(), Mock()
    monkeypatch.setattr(serving, "cms_address_preparation", factory)
    monkeypatch.setattr(serving, "produce_nonprofile_admission", admission)
    monkeypatch.setattr(serving, "prepare_serving_artifacts", scratch)
    with pytest.raises(RuntimeError, match="capture_signed_request_changed"):
        await serving._publish_cms(_fhir([]), execution, "run_" + "a" * 32, "run_" + "a" * 32, {})
    factory.assert_not_called()
    admission.assert_not_awaited()
    scratch.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("predecessor", [None, "matching", "changed"])
async def test_legacy_cms_requires_proved_exact_incumbent(monkeypatch, execution, predecessor):
    fhir = _fhir([])
    cms = next(pair for pair in execution.attestation.pairs if pair["source_id"] == "cms-npd")
    prior = None if predecessor is None else {"payload": {"cms": dict(cms)}}
    if predecessor == "changed":
        prior["payload"]["cms"]["acquisition_root_run_id"] = "another-run"
    monkeypatch.setattr(serving, "_current_predecessor", AsyncMock(return_value=prior))
    if predecessor == "matching":
        await serving._assert_retained_legacy_cms(fhir, execution)
    else:
        with pytest.raises(RuntimeError, match="desired_selection_required"):
            await serving._assert_retained_legacy_cms(fhir, execution)
