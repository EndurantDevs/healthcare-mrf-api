# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Owner capability placement and awaited receipt hooks; component proof only."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import network_registry_cms_prepared_pair as retained
from process import provider_directory_cms_preparation as preparation
from process import provider_directory_cms_publication as publication
from process import provider_directory_cms_serving as serving
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from process.provider_directory_cms_source_runtime import BoundCMSRegistrySourceJob
from tests.provider_directory_cms_capacity_test_support import cms_execution
from tests.test_provider_directory_cms_preflight import _retention_policy


def _bound_job():
    policy = _retention_policy(cms_execution())
    request = retained.RegistryCMSRetentionRequest(
        PinnedFHIRMembershipSource(**policy["source_pin"]),
        RegistryNetworkSourceCoordinates(**policy["binding_coordinates"]),
        policy["selection_proof_id"],
        policy["expected_admission_sha256"],
        policy["expected_metadata_sha256"],
        policy["extra_data_upper_bound_bytes"],
        policy["extra_wal_upper_bound_bytes"],
    )
    return BoundCMSRegistrySourceJob(
        request,
        UUID(policy["capture_id"]),
        policy["owner_role"],
        tuple(policy["runtime_roles"]),
        Mock(),
        AsyncMock(),
        RegistryCMSSourceAttempt("run", "run:" + "a" * 32, "2026-10-08T10:00:00+00:00"),
    )


@pytest.mark.asyncio
async def test_historical_work_bypasses_capability_but_retention_requires_it():
    historical = SimpleNamespace(cms_nonprofile_admission={})
    assert await serving._authenticated_source_job(None, object(), historical, object()) is None
    enrolled = SimpleNamespace(cms_nonprofile_admission={"registry_source_retention": {}})
    with pytest.raises(RuntimeError, match="cms_registry_source_runtime_required"):
        await serving._authenticated_source_job(None, object(), enrolled, object())
    with pytest.raises(RuntimeError, match="cms_registry_source_runtime_invalid"):
        await serving._authenticated_source_job({}, object(), enrolled, object())


def _retention_scope_case(monkeypatch, capture_failure):
    """Install ownership assertions at the exact address and Profile boundaries."""
    events = []
    job = _bound_job()
    source_pair, address = object(), object()
    prepared = SimpleNamespace(
        fhir=object(),
        execution=object(),
        fence=object(),
        relation_overrides={},
        overlay_identity=object(),
        nonprofile_admission=SimpleNamespace(registry_source_job=job),
        archive_delta=None,
        metrics={},
        assert_ready=AsyncMock(),
        registry_source_pair=None,
        source_session_factory=None,
    )

    @asynccontextmanager
    async def address_factory(*args, **kwargs):
        events.append("address-ready")
        try:
            yield address
        finally:
            events.append("address-closed")

    async def capture(actual_prepared, actual_factory, request, **options):
        assert actual_prepared is prepared and prepared.address is address
        assert actual_factory is address_factory and request is job.retention_request
        assert options == dict(
            capture_id=job.capture_id,
            owner_role=job.owner_role,
            runtime_roles=job.runtime_roles,
            source_session_factory=job.source_session_factory,
            source_attempt=job.source_attempt,
        )
        events.append("source-retained")
        if capture_failure:
            raise RuntimeError("synthetic retention failure")
        return source_pair

    @asynccontextmanager
    async def profile(*args):
        assert prepared.registry_source_pair is source_pair
        assert prepared.source_session_factory is job.source_session_factory
        events.append("profile-resumed")
        yield object(), {"profile": {"prepared": True}}

    monkeypatch.setattr(retained, "prepare_registry_cms_source_pair", capture)
    monkeypatch.setattr(preparation, "_profile_scope", profile)
    monkeypatch.setattr(preparation, "_emit_prepared_manifest", AsyncMock())
    return prepared, events, address_factory


@pytest.mark.asyncio
@pytest.mark.parametrize("capture_failure", [False, True])
async def test_source_retention_finishes_before_profile_scope(monkeypatch, capture_failure):
    prepared, events, address_factory = _retention_scope_case(monkeypatch, capture_failure)
    operation = preparation._prepare_address_and_profile(
        prepared, run_id="run", control_run_id="run", metrics={}, address_preparation=address_factory
    )
    if capture_failure:
        with pytest.raises(RuntimeError, match="synthetic retention failure"):
            async with operation:
                pytest.fail("failed retention exposed preparation")
        assert events == ["address-ready", "source-retained", "address-closed"]
    else:
        async with operation:
            assert events == ["address-ready", "source-retained", "profile-resumed"]
        assert events[-1] == "address-closed"


@pytest.mark.asyncio
@pytest.mark.parametrize("notify_failure", [False, True])
async def test_receipt_binding_and_notification_share_the_passed_session(monkeypatch, notify_failure):
    job, session, retained_pair, payload = _bound_job(), object(), object(), {}
    prepared = SimpleNamespace(
        nonprofile_admission=SimpleNamespace(registry_source_job=job), registry_source_pair=object()
    )
    events = []

    async def bind(actual_session, pair, *, receipt_id, receipt_payload):
        assert actual_session is session and pair is prepared.registry_source_pair
        assert receipt_id == "receipt" and receipt_payload is payload
        events.append("bound")
        return retained_pair

    async def notify(actual_session, *, receipt_id, pair):
        assert actual_session is session and pair is retained_pair and receipt_id == "receipt"
        events.append("notified")
        if notify_failure:
            raise RuntimeError("synthetic notification failure")

    job.notify_receipt.side_effect = notify
    monkeypatch.setattr(retained, "bind_prepared_registry_cms_source_pair", bind)
    operation = publication._bind_registry_source_receipt(session, prepared, "receipt", payload)
    if notify_failure:
        with pytest.raises(RuntimeError, match="synthetic notification failure"):
            await operation
    else:
        await operation
    assert events == ["bound", "notified"]
    job.notify_receipt.assert_awaited_once()


@pytest.mark.asyncio
async def test_retention_hook_rejects_a_missing_prepared_pair():
    prepared = SimpleNamespace(
        nonprofile_admission=SimpleNamespace(registry_source_job=_bound_job()), registry_source_pair=None
    )
    with pytest.raises(RuntimeError, match="cms_registry_source_runtime_invalid"):
        await publication._bind_registry_source_receipt(object(), prepared, "receipt", {})
