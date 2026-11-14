# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Logical registry and lease checks before original-source acquisition."""

from unittest.mock import AsyncMock

import pytest

from process.custom_import import https_candidate
from process.custom_import.capture_store import CaptureStoreError
from process.custom_import.object_candidate import ObjectCandidateError, run_object_candidate
from tests.test_custom_import_https_candidate import coordinator as coordinator
from tests.test_custom_import_https_candidate import no_network as no_network
from tests.test_custom_import_https_originals import _original_io, _original_request
from tests.test_custom_import_object_candidate import _native_io, _request


@pytest.mark.parametrize("source_kind", ("https", "s3", "upload"))
@pytest.mark.parametrize("denial", ("registry", "lease"))
async def test_denied_logical_authority_never_downloads_signs_or_opens_sources(
    coordinator, monkeypatch, source_kind, denial
):
    if source_kind == "https":
        request, payloads = _original_request("ndjson", "none")
        register, load, _captures = _original_io(monkeypatch, coordinator, request, payloads)
        acquire = coordinator.acquire
        run = https_candidate.run_https_candidate
        failure = https_candidate.HttpsCandidateError
    else:
        request, payloads = _request(source_kind)
        register, load, _captures, acquire = _native_io(monkeypatch, coordinator, request, payloads)
        run = run_object_candidate
        failure = ObjectCandidateError
    mocks = coordinator.mocks_by_name
    if denial == "registry":
        mocks["load_registry"].side_effect = ValueError("synthetic revision mismatch")
        with pytest.raises(failure, match="candidate_failed"):
            await run(coordinator.session_factory, request)
        mocks["claim_execution"].assert_not_awaited()
    else:
        mocks["claim_execution"].return_value = None
        assert (await run(coordinator.session_factory, request)).status == "not_claimed"
    acquire.assert_not_awaited()
    register.assert_not_awaited()
    load.assert_not_awaited()
    mocks["run_claimed_candidate"].assert_not_awaited()
    mocks["finish_execution"].assert_not_awaited()


async def test_bound_raw_resume_never_acquires_fresh_originals(coordinator, monkeypatch):
    request, payloads = _original_request("ndjson", "none")
    register, load, _captures = _original_io(monkeypatch, coordinator, request, payloads)
    assert (
        await https_candidate.run_https_candidate(coordinator.session_factory, request, resume=True)
    ).status == "no_change"
    coordinator.acquire.assert_not_awaited()
    register.assert_not_awaited()
    load.assert_awaited_once()


@pytest.mark.parametrize("source_kind", ("https", "s3", "upload"))
async def test_unavailable_registrar_never_opens_original_source(coordinator, monkeypatch, source_kind):
    if source_kind == "https":
        request, payloads = _original_request("ndjson", "none")
        register, load, _captures = _original_io(monkeypatch, coordinator, request, payloads)
        acquire, run, failure = (
            coordinator.acquire,
            https_candidate.run_https_candidate,
            https_candidate.HttpsCandidateError,
        )
    else:
        request, payloads = _request(source_kind)
        register, load, _captures, acquire = _native_io(monkeypatch, coordinator, request, payloads)
        run, failure = run_object_candidate, ObjectCandidateError
    preflight = AsyncMock(side_effect=CaptureStoreError("synthetic unavailable registrar"))
    monkeypatch.setattr(https_candidate, "require_raw_capture_write_access", preflight)
    with pytest.raises(failure, match="candidate_failed"):
        await run(coordinator.session_factory, request)
    preflight.assert_awaited_once()
    acquire.assert_not_awaited()
    register.assert_not_awaited()
    load.assert_not_awaited()
    coordinator.mocks_by_name["run_claimed_candidate"].assert_not_awaited()
