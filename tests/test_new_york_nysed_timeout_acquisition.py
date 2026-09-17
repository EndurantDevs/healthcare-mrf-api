# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import copy
import json
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from process import new_york_nysed_profile as profile
from process import new_york_nysed_profile_acquisition as acquisition
from process import new_york_nysed_profile_retries as retries
from process import new_york_profile as worker
from process import new_york_profile_store as store
from process.control_cancel import ImportCancelledError
from tests.test_new_york_nysed_profile import PUBLIC_HEADER, SourceResponse, _acquire, _artifact
from tests.test_new_york_nysed_profile import deny_network as deny_network
from tests.test_new_york_nysed_profile import source_session as source_session
from tests.test_new_york_profile_managed import TASK
from tests.test_new_york_profile_managed import managed_case as managed_case


@pytest.fixture
def recovery_clock(monkeypatch):
    clock_by_field = {"now": datetime(2026, 1, 1, tzinfo=timezone.utc), "delays": []}

    async def sleep(delay):
        clock_by_field["delays"].append(delay)
        clock_by_field["now"] += timedelta(seconds=delay)

    monkeypatch.setattr(acquisition, "_now", lambda: clock_by_field["now"].isoformat())
    monkeypatch.setattr(acquisition.asyncio, "sleep", sleep)
    return clock_by_field


def _sequence(session, responses):
    pending = deque(responses)
    get = session.get

    def next_response(url, **options):
        session.response = pending.popleft()
        return get(url, **options)

    session.get = next_response
    return pending


def _timeout():
    return SourceResponse(status=408, raw=b"The source search exceeded its time limit.")


@pytest.mark.parametrize("timeout_count", [1, 2])
@pytest.mark.parametrize("held", [False, True])
async def test_recovery_retains_complete_attempts(source_session, recovery_clock, timeout_count, held):
    session = source_session(_timeout())
    final = SourceResponse(raw=b"", status=204) if held else SourceResponse()
    pending = _sequence(session, [_timeout() for _ in range(timeout_count)] + [final])

    acquired = await _acquire(session)

    assert not pending and session.closed and len(session.requests) == timeout_count + 1
    assert recovery_clock["delays"] == list(retries.TIMEOUT_DELAYS[:timeout_count])
    receipt = _artifact(session, "result.json")
    manifest = _artifact(session, "manifest.json")
    extra_files = retries.timeout_files(receipt, manifest)
    assert set(extra_files) == {retries.timeout_filename(number) for number in range(1, timeout_count + 1)}
    assert {path.name for path in session.destination.iterdir()} == set(worker.NYSED_FILES) | set(extra_files)
    assert all(_artifact(session, name)["status"] == 408 for name in extra_files)
    assert _artifact(session, "response.json")["status"] == (204 if held else 200)
    reader = profile.read_held_acquisition if held else profile.read_acquisition
    assert reader(session.destination, receipt_sha256=acquired["receipt_sha256"]) == acquired
    assert all(PUBLIC_HEADER.encode() not in path.read_bytes() for path in session.destination.iterdir())


async def test_timeout_exhaustion_fails_closed(source_session, recovery_clock):
    session = source_session(_timeout())
    pending = _sequence(session, [_timeout(), _timeout(), _timeout(), SourceResponse()])

    with pytest.raises(ValueError, match="acquisition_failed"):
        await _acquire(session)

    assert len(pending) == 1 and len(session.requests) == 3 and session.closed
    assert recovery_clock["delays"] == [5, 15]
    assert _artifact(session, "response.json")["status"] == 408
    assert _artifact(session, "result.json")["outcome"] == "failed"
    assert {path.name for path in session.destination.iterdir()} == set(worker.NYSED_FILES) | {
        "timeout-1.response.json",
        "timeout-2.response.json",
    }
    with pytest.raises(ValueError, match="receipt_changed_or_failed"):
        profile.read_acquisition(session.destination, receipt_sha256=profile._hash(_artifact(session, "result.json")))


@pytest.mark.parametrize("status", [301, 403, 429, 500, 502, 503, 504])
async def test_other_statuses_are_not_retried(source_session, recovery_clock, status):
    session = source_session(SourceResponse(status=status, raw=b"synthetic error"))
    with pytest.raises(ValueError, match="acquisition_failed"):
        await _acquire(session)
    assert len(session.requests) == 1 and not recovery_clock["delays"]
    assert {path.name for path in session.destination.iterdir()} == set(worker.NYSED_FILES)


@pytest.mark.parametrize(
    "response",
    [
        SourceResponse(status=408, raw=b"x" * (65536 + 1)),
        SourceResponse(status=408, chunks=[b"partial", TimeoutError("synthetic timeout")]),
        SourceResponse(status=408, headers=[("Content-Type", "text/html")]),
        SourceResponse(status=408, raw=PUBLIC_HEADER.encode()),
    ],
)
async def test_unsafe_timeouts_are_not_retried(source_session, recovery_clock, response):
    session = source_session(response)
    with pytest.raises(ValueError, match="acquisition_failed"):
        await _acquire(session)
    assert len(session.requests) == 1 and not recovery_clock["delays"]
    assert _artifact(session, "result.json")["outcome"] == "failed"


async def test_cancelled_backoff_retains_timeout(source_session, monkeypatch, recovery_clock):
    session = source_session(_timeout())

    async def cancel(delay):
        assert delay == 5
        assert _artifact(session, "timeout-1.response.json")["status"] == 408
        assert not (session.destination / "response.json").exists()
        raise asyncio.CancelledError("synthetic cancellation")

    monkeypatch.setattr(acquisition.asyncio, "sleep", cancel)
    with pytest.raises(asyncio.CancelledError):
        await _acquire(session)
    assert len(session.requests) == 1 and session.closed
    assert _artifact(session, "result.json")["outcome"] == "failed"


async def test_unlink_failure_prevents_retry(source_session, monkeypatch, recovery_clock):
    session = source_session(_timeout())
    original = Path.unlink

    def unlink(path, *args, **kwargs):
        if path == session.destination / "response.json":
            raise OSError("synthetic unlink failure")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink)
    with pytest.raises(ValueError, match="acquisition_failed:OSError"):
        await _acquire(session)
    assert len(session.requests) == 1 and not recovery_clock["delays"]
    assert (session.destination / "response.json").read_bytes() == (
        session.destination / "timeout-1.response.json"
    ).read_bytes()


async def test_managed_timeout_preserves_publication_guards(managed_case, recovery_clock):
    state = managed_case([{"license": "111111"}])
    session = state.sessions[1]
    _sequence(session, [_timeout(), session.response])

    metrics = await worker.import_profiles({}, TASK)

    assert metrics["published"] and not state.failed and state.published == state.run["run_id"]
    bundle = json.loads((state.directory / "manifest.json").read_bytes())
    support = bundle["acquisition"]["nysed_support"]["111111"]
    assert len(support["file_sha256"]) == 5
    store._validate_nysed_capture(support, state.run["run_id"], "111111")
    altered = copy.deepcopy(support)
    altered["file_sha256"]["timeout-1.response.json"] = "0" * 64
    with pytest.raises(ValueError):
        store._validate_nysed_capture(altered, state.run["run_id"], "111111")

    altered = copy.deepcopy(support)
    del altered["receipt"]["timeout_retries"]
    altered["receipt_sha256"] = profile._hash(altered["receipt"])
    altered["file_sha256"]["result.json"] = altered["receipt_sha256"]
    with pytest.raises(ValueError):
        store._validate_nysed_capture(altered, state.run["run_id"], "111111")


async def test_managed_cancel_prevents_next_request(managed_case, monkeypatch, recovery_clock):
    state = managed_case([{"license": "111111"}])
    session = state.sessions[1]
    _sequence(session, [_timeout(), session.response])

    async def cancel_during_backoff(delay):
        if delay == 0:
            return
        assert delay == 5
        state.cancel = True

    monkeypatch.setattr(acquisition.asyncio, "sleep", cancel_during_backoff)
    with pytest.raises(ImportCancelledError):
        await worker.import_profiles({}, TASK)

    assert len(session.requests) == 1 and session.closed and state.published is None
    assert state.failed == [(state.run["run_id"], "ImportCancelledError")]
    destination = state.directory / "nysed" / "111111"
    assert json.loads((destination / "timeout-1.response.json").read_bytes())["status"] == 408
    assert json.loads((destination / "result.json").read_bytes())["error_type"] == "ImportCancelledError"
