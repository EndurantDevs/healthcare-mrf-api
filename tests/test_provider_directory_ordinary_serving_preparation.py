# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The common owner receives resumed Profile authority with every candidate alive."""

import asyncio
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_preparation as preparation
from process import provider_directory_ordinary_serving_preparation as ordinary
from process import provider_directory_profile_scope_copy as scope
from process import provider_directory_profile_source_inputs as inputs


class PreparationCase:
    """Observe meter, candidate lifetime and scope changes at the existing boundaries."""

    def __init__(self, monkeypatch, *, failure=None):
        self.trace = []
        self.failure = failure
        self.variables_by_name = {
            name: ContextVar(name, default=None)
            for name in (
                "_PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION",
                "_PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION",
                "_PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES",
                "_PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS",
                "_PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE",
            )
        }
        self.capacity = self.variables_by_name["_PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION"]
        self.fence_var = self.variables_by_name["_PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE"]
        self.original, self.resumed, self.paused = object(), object(), object()
        self.full_fence, self.overlay_fence, self.profile_resources = object(), object(), object()
        self.fhir = SimpleNamespace(**self.variables_by_name, _schema=lambda: "synthetic")
        self.fhir._provider_directory_profile_capacity_admission = self.capacity.get
        self.fhir._provider_directory_artifact_resource_types = lambda *args, **kwargs: frozenset({"PractitionerRole"})
        self.fhir._artifact_scope_tokens = self.artifact_tokens
        self.fhir._provider_directory_artifact_bundle_scope = self.bundle
        self.admission = SimpleNamespace(profile_admission=None, pause_profile=self.pause, resume_profile=self.resume)
        self.prepared = preparation.PreparedServingArtifacts(
            self.fhir,
            self.full_fence,
            object(),
            self.admission,
            None,
            None,
            {},
            {},
            None,
        )
        self.options = dict(
            run_id="synthetic",
            control_run_id=None,
            address_preparation=self.address,
            retention_preparation=self.retention,
        )
        self._install(monkeypatch)

    def _install(self, monkeypatch):
        monkeypatch.setattr(
            ordinary,
            "_admit",
            AsyncMock(
                return_value=(
                    SimpleNamespace(overlay_fence=self.overlay_fence),
                    frozenset(),
                    object(),
                )
            ),
        )
        monkeypatch.setattr(preparation, "_profile_scope", self.profile)
        monkeypatch.setattr(preparation, "_full_scope", self.scratch)
        monkeypatch.setattr(
            preparation, "_prepare_full_bundle", AsyncMock(return_value={"address_overlay": {"rows": 1}})
        )
        monkeypatch.setattr(preparation, "_overlay_identity", AsyncMock(return_value=object()))
        monkeypatch.setattr(preparation.PreparedServingArtifacts, "assert_ready", self.ready)
        monkeypatch.setattr(scope, "resource_fence", AsyncMock(return_value=self.profile_resources))
        monkeypatch.setattr(scope, "input_transaction", self.no_transaction)
        monkeypatch.setattr(inputs, "profile_source_input_scope", self.no_transaction)

    async def pause(self, actual):
        assert actual is self.original
        self.trace.append("pause_profile")
        return self.paused

    async def resume(self, actual, selected, types):
        assert actual is self.paused and selected is self.profile_resources
        self._assert_address_meter()
        self.trace.append("resume_profile")
        return self.resumed

    def _assert_address_meter(self):
        assert self.capacity.get() is None and preparation._ACTIVE.get() is self.admission

    @contextmanager
    def artifact_tokens(self, fence, selected, relations_by_name, oids_by_name):
        assert selected is self.profile_resources
        assert relations_by_name == {"resource": "profile_input"} and oids_by_name == {"resource": 17}
        token = self.fence_var.set(fence)
        try:
            yield
        finally:
            self.fence_var.reset(token)

    @asynccontextmanager
    async def profile(self, *args):
        token = self.capacity.set(self.original)
        admission_token = preparation._ACTIVE.set(None)
        relations = self.variables_by_name["_PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES"]
        oids = self.variables_by_name["_PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS"]
        relation_token, oid_token = relations.set({"resource": "profile_input"}), oids.set({"resource": 17})
        self.trace.append("profile_ready")
        try:
            yield SimpleNamespace(stages=(), profile_delta=None), {"profile": {"rows": 2}}
        finally:
            self._assert_address_meter()
            self.trace.append("profile_cleanup")
            self.capacity.reset(token)
            preparation._ACTIVE.reset(admission_token)
            relations.reset(relation_token)
            oids.reset(oid_token)

    @asynccontextmanager
    async def scratch(self, *args):
        self._assert_address_meter()
        token = self.fence_var.set(self.overlay_fence)
        self.trace.append("overlay_inputs")
        try:
            yield {"resource": "overlay_input"}
        finally:
            self._assert_address_meter()
            self.trace.append("overlay_cleanup")
            self.fence_var.reset(token)

    @asynccontextmanager
    async def bundle(self):
        self.trace.append("overlay_ready")
        try:
            yield SimpleNamespace(stages=(), relation_overrides={"overlay": "overlay_candidate"})
        finally:
            self.trace.append("bundle_cleanup")

    @asynccontextmanager
    async def address(self, *args):
        assert self.fence_var.get() is self.overlay_fence and self.capacity.get() is None
        self.trace.append("address_ready")
        try:
            yield object()
        except BaseException as error:
            self.trace.append(type(error).__name__)
            raise
        finally:
            self._assert_address_meter()
            self.trace.append("address_cleanup")

    async def retention(self, prepared):
        assert prepared.address is not None and self.capacity.get() is None
        self.trace.append("retention_logged")
        if self.failure is not None:
            raise self.failure
        return object()

    @asynccontextmanager
    async def no_transaction(self, *args):
        yield

    async def ready(self, *args, **kwargs):
        assert self.capacity.get() is self.resumed and self.fence_var.get() is self.full_fence
        self.trace.append("ready_for_cut")

    def assert_restored(self):
        assert all(variable.get() is None for variable in self.variables_by_name.values())
        assert preparation._ACTIVE.get() is None


@pytest.mark.asyncio
async def test_detached_geometry_cannot_issue_a_preparation_admission(monkeypatch):
    from tests.provider_directory_serving_capacity_signing_test_support import serving_case

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    # A genuine plan's bytes do not turn a detached object into its runtime authority.
    case = serving_case()
    with pytest.raises(RuntimeError, match="ordinary_preparation_invalid"):
        await ordinary._admit(object(), object(), object(), SimpleNamespace(plan=case.plan), lambda: None, lambda: None)


@pytest.mark.asyncio
async def test_resume_precedes_common_cut_and_cleanup_keeps_its_own_meter(monkeypatch):
    case = PreparationCase(monkeypatch)
    async with ordinary.prepare_ordinary_serving_artifacts(case.prepared, **case.options) as prepared:
        assert prepared.retained_generation is not None
        assert case.capacity.get() is case.resumed
        case.trace.append("common_cut")
    assert case.trace == [
        "profile_ready",
        "pause_profile",
        "overlay_inputs",
        "overlay_ready",
        "address_ready",
        "retention_logged",
        "resume_profile",
        "ready_for_cut",
        "common_cut",
        "address_cleanup",
        "bundle_cleanup",
        "overlay_cleanup",
        "profile_cleanup",
    ]
    case.assert_restored()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("retention_failed"), asyncio.CancelledError()])
async def test_failed_retention_drains_candidates_without_resuming_or_publishing(monkeypatch, failure):
    case = PreparationCase(monkeypatch, failure=failure)
    with pytest.raises(type(failure)):
        async with ordinary.prepare_ordinary_serving_artifacts(case.prepared, **case.options):
            pytest.fail("an incomplete candidate cannot publish")
    assert "resume_profile" not in case.trace and "ready_for_cut" not in case.trace
    assert case.trace[-4:] == ["address_cleanup", "bundle_cleanup", "overlay_cleanup", "profile_cleanup"]
    assert type(failure).__name__ in case.trace
    case.assert_restored()


@pytest.mark.asyncio
async def test_publication_failure_reaches_preparation_cleanup(monkeypatch):
    case = PreparationCase(monkeypatch)
    with pytest.raises(ValueError, match="cut_failed"):
        async with ordinary.prepare_ordinary_serving_artifacts(case.prepared, **case.options):
            raise ValueError("cut_failed")
    assert "resume_profile" in case.trace and "ValueError" in case.trace
    case.assert_restored()
