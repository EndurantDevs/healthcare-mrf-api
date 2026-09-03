# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-download and native-runner proof for hospital-price acquisition."""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from typing import Any

import pytest

from tests.hospital_price_control_support import (
    acquisition_module as _acquisition_module,
)


class _ExpiredPermissionError(PermissionError):
    status = 403


class _ServerError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(str(status))
        self.status = status


def test_content_only_locator_binding_preserves_unknown_location():
    acquisition = _acquisition_module()
    locator_url = "https://hospital.example/cms-hpt.txt"
    selector = "https://files.example/shared.csv"
    hospital_by_field = {
        "hospital_id": "hospital-a",
        "name": "Catalog Sublocation",
        "cms_hpt_url": locator_url,
        "locator_mrf_url": selector,
    }
    locator_result = acquisition.LocatorResult(
        locator_url,
        "locator-a",
        "observation-a",
        (hospital_by_field,),
        (
            acquisition.HospitalHptLocatorRecord(
                "Parent Hospital", f"{selector}?sig=fresh"
            ),
            acquisition.HospitalHptLocatorRecord(
                "Parent Hospital Annex", f"{selector}?sig=fresh"
            ),
        ),
    )

    candidate = acquisition.candidates_from_locators((locator_result,))[0]

    assert candidate.source_url == f"{selector}?sig=fresh"
    assert candidate.locator_name is None


@pytest.mark.parametrize(
    "locator_record_names",
    (("Hospital A",), ("Different Hospital",), ("Hospital A", "Hospital A")),
)
def test_locator_uses_explicit_reviewed_fallback_source(locator_record_names):
    acquisition = _acquisition_module()
    locator_url = "https://hospital.example/cms-hpt.txt"
    is_ambiguous = len(locator_record_names) > 1
    hospital_by_field = {
        "hospital_id": "hospital-a",
        "name": "Hospital A",
        "cms_hpt_url": locator_url,
        "fallback_mrf_url": "https://files.example/current.json?sig=reviewed",
    }
    locator_result = acquisition.LocatorResult(
        locator_url,
        "locator-a",
        "observation-a",
        (hospital_by_field,),
        tuple(
            acquisition.HospitalHptLocatorRecord(
                locator_name, f"https://files.example/{index}.json?sig=stale"
            )
            for index, locator_name in enumerate(locator_record_names)
        ),
    )

    candidate = acquisition.candidates_from_locators((locator_result,))[0]

    assert candidate.source_url == (
        locator_url if is_ambiguous else hospital_by_field["fallback_mrf_url"]
    )
    assert candidate.initial_error_code == (
        "locator_ambiguous" if is_ambiguous else None
    )


def test_fetch_failure_uses_only_an_explicit_reviewed_fallback():
    acquisition = _acquisition_module()
    hospital_by_field = {
        "hospital_id": "a",
        "name": "Hospital A",
        "fallback_mrf_url": "https://files.example/report?facility=a",
    }
    failed_fetch = acquisition.LocatorResult(
        "https://a/cms-hpt.txt", "locator", "fetch-observation",
        (hospital_by_field,), None, "clientresponse", "403", fetch_failed=True,
    )
    invalid_body = acquisition.LocatorResult(
        "https://a/cms-hpt.txt", "locator", "parse-observation",
        (hospital_by_field,), None, "hospitalhptlocator", "invalid",
        fetch_failed=False,
    )

    fallback = acquisition.candidates_from_locators((failed_fetch,))[0]
    rejected = acquisition.candidates_from_locators((invalid_body,))[0]

    assert fallback.source_url == hospital_by_field["fallback_mrf_url"]
    assert fallback.initial_error_code is None
    assert fallback.observation_id == "fetch-observation"
    assert rejected.source_url == invalid_body.url
    assert rejected.initial_error_code == "hospitalhptlocator"


@pytest.mark.parametrize(
    ("response_body_started", "expected_fetch_failure"),
    ((True, False), (False, True), (None, False)),
)
@pytest.mark.asyncio
async def test_locator_fallback_requires_explicit_prebody_failure(
    monkeypatch,
    response_body_started,
    expected_fetch_failure,
):
    acquisition = _acquisition_module()
    locator_url = "https://a/cms-hpt.txt"
    hospital_by_field = {
        "hospital_id": "a",
        "name": "Hospital A",
        "fallback_mrf_url": "https://files.example/report?facility=a",
    }
    partial_error = OSError("partial locator body")
    if response_body_started is not None:
        setattr(
            partial_error,
            "_ptg2_response_body_started",
            response_body_started,
        )

    async def fail_download(*_args, **_kwargs):
        raise partial_error

    async def record_observation(*_args, **_kwargs):
        return None

    monkeypatch.setattr(acquisition, "download_raw_artifact", fail_download)
    monkeypatch.setattr(acquisition, "_record_locator_observation", record_observation)

    locator_result = await acquisition.fetch_locator(
        (locator_url, (hospital_by_field,)),
        object(),
    )
    candidate = acquisition.candidates_from_locators((locator_result,))[0]

    assert locator_result.fetch_failed is expected_fetch_failure
    assert locator_result.error_code == "os"
    assert locator_result.error_detail == "partial locator body"
    assert candidate.source_url == (
        hospital_by_field["fallback_mrf_url"]
        if expected_fetch_failure
        else locator_url
    )
    assert candidate.initial_error_code == (None if expected_fetch_failure else "os")


@pytest.mark.asyncio
async def test_source_download_updates_shared_attempts_and_reports_errors(monkeypatch):
    acquisition = _acquisition_module()
    attempt = acquisition.Attempt("attempt", "a", "Hospital A", "https://a/mrf", 1)
    raw = SimpleNamespace(head=SimpleNamespace(url="https://a/final", status=200))
    download_options: list[dict[str, Any]] = []

    async def download(*_args, **kwargs):
        download_options.append(dict(kwargs))
        return raw

    monkeypatch.setattr(acquisition, "download_raw_artifact", download)
    downloaded_source = await acquisition.download_source(
        ("https://a/mrf", (attempt,)), object(), 1024
    )
    assert downloaded_source.raw is raw
    assert (attempt.final_source_url, attempt.source_http_status) == (
        "https://a/final", 200,
    )
    assert download_options[0]["reuse_raw_artifacts"] is False
    assert download_options[0]["max_bytes"] == 1024
    assert download_options[0]["keep_partial_artifacts"] is False
    assert download_options[0]["user_agent"].startswith("Mozilla/5.0")
    raw.head = None
    unchanged = await acquisition.download_source(
        ("https://a/mrf", (attempt,)), object(), 1024
    )
    assert unchanged.raw is raw
    assert (attempt.final_source_url, attempt.source_http_status) == (None, None)

    async def fail(*_args, **_kwargs):
        raise ValueError("failed")

    monkeypatch.setattr(acquisition, "download_raw_artifact", fail)
    failed = await acquisition.download_source(
        ("https://a/mrf", (attempt,)), object(), 1024
    )
    assert (failed.raw, failed.error_code) == (None, "value")

    async def cancel(*_args, **_kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(acquisition, "download_raw_artifact", cancel)
    with pytest.raises(asyncio.CancelledError):
        await acquisition.download_source(("https://a/mrf", (attempt,)), object(), 1024)


@pytest.mark.parametrize(
    ("first_status", "first_body_started", "fallback_status", "expected_calls"),
    (
        (403, False, None, 2),
        (403, False, 500, 2),
        (403, True, None, 1),
        (500, False, None, 1),
    ),
)
@pytest.mark.asyncio
async def test_source_download_retries_only_prebody_403_with_default_user_agent(
    monkeypatch, first_status, first_body_started, fallback_status, expected_calls
):
    acquisition = _acquisition_module()
    source_url = "https://a/mrf?sig=exact"
    attempt = acquisition.Attempt("attempt", "a", "Hospital A", source_url, 1)
    raw = SimpleNamespace(head=SimpleNamespace(url=source_url, status=200))
    requests: list[tuple[str, dict[str, Any]]] = []

    async def download(url, **kwargs):
        requests.append((url, dict(kwargs)))
        if len(requests) == 1 or fallback_status is not None:
            error = _ServerError(
                first_status if len(requests) == 1 else fallback_status
            )
            setattr(error, "_ptg2_response_body_started", first_body_started)
            raise error
        return raw

    monkeypatch.setattr(acquisition, "download_raw_artifact", download)
    downloaded_source = await acquisition.download_source(
        (source_url, (attempt,)), object(), 1024
    )

    assert [url for url, _kwargs in requests] == [source_url] * expected_calls
    assert requests[0][1]["user_agent"].startswith("Mozilla/5.0")
    if expected_calls == 2:
        assert "user_agent" not in requests[1][1]
    if expected_calls == 2 and fallback_status is None:
        assert downloaded_source.raw is raw
    else:
        assert downloaded_source.raw is None
    if fallback_status is not None:
        assert downloaded_source.auth_refresh_required is True
        assert attempt.source_http_status == 403


@pytest.mark.asyncio
async def test_source_download_tries_runtime_agent_after_two_prebody_403s(monkeypatch):
    acquisition = _acquisition_module()
    source_url = "https://a/mrf?sig=exact"
    attempt = acquisition.Attempt("attempt", "a", "Hospital A", source_url, 1)
    raw = SimpleNamespace(head=SimpleNamespace(url=source_url, status=206))
    requests: list[tuple[str, dict[str, Any]]] = []

    async def download(url, **kwargs):
        requests.append((url, dict(kwargs)))
        if len(requests) < 3:
            error = _ServerError(403)
            setattr(error, "_ptg2_response_body_started", False)
            raise error
        return raw

    monkeypatch.setattr(acquisition, "download_raw_artifact", download)
    downloaded_source = await acquisition.download_source(
        (source_url, (attempt,)), object(), 1024
    )

    assert downloaded_source.raw is raw
    assert [url for url, _kwargs in requests] == [source_url] * 3
    assert requests[0][1]["user_agent"].startswith("Mozilla/5.0")
    assert "user_agent" not in requests[1][1]
    assert requests[2][1]["user_agent"] == "Python/3.12 aiohttp/3.11"


@pytest.mark.asyncio
async def test_source_download_marks_expired_authorization_and_exact_retry(monkeypatch):
    acquisition = _acquisition_module()
    expired_attempt = acquisition.Attempt(
        "expired", "a", "Hospital A", "https://a/mrf?sig=expired", 1
    )

    async def expired(*_args, **_kwargs):
        raise _ExpiredPermissionError("expired")

    monkeypatch.setattr(acquisition, "download_raw_artifact", expired)
    expired_source = await acquisition.download_source(
        (expired_attempt.source_url, (expired_attempt,)), object(), 1024
    )
    assert expired_source.auth_refresh_required is True
    assert (expired_attempt.final_source_url, expired_attempt.source_http_status) == (
        expired_attempt.source_url, 403,
    )

    exact_requests: list[str] = []

    async def exact_expired(url, **_kwargs):
        exact_requests.append(url)
        raise _ExpiredPermissionError("expired")

    monkeypatch.setattr(acquisition, "download_raw_artifact", exact_expired)
    await acquisition.download_source(
        ("https://a/mrf?sig=refreshed", (expired_attempt,)),
        object(), 1024, exact_url_only=True,
    )
    assert exact_requests == ["https://a/mrf?sig=refreshed"]


@pytest.mark.asyncio
async def test_source_download_refreshes_an_authorization_sibling(monkeypatch):
    acquisition = _acquisition_module()
    expired_attempt = acquisition.Attempt(
        "expired", "a", "Hospital A", "https://a/mrf?sig=expired", 1
    )
    mixed_attempt = acquisition.Attempt(
        "mixed", "b", "Hospital B", "https://a/mrf?sig=other", 1
    )

    async def mixed_status(url, **_kwargs):
        raise _ServerError(403 if url == expired_attempt.source_url else 500)

    monkeypatch.setattr(acquisition, "download_raw_artifact", mixed_status)
    mixed_failure = await acquisition.download_source(
        (expired_attempt.source_url, (expired_attempt, mixed_attempt)), object(), 1024
    )
    assert mixed_failure.auth_refresh_required is True
    assert (expired_attempt.source_http_status, mixed_attempt.source_http_status) == (
        403, 500,
    )


@pytest.mark.asyncio
async def test_source_download_tries_distinct_exact_url_variants(monkeypatch):
    acquisition = _acquisition_module()
    stale_attempt = acquisition.Attempt(
        "stale", "a", "Hospital A", "https://a/mrf?sig=stale", 1
    )
    fresh_attempt = acquisition.Attempt(
        "fresh", "b", "Hospital B", "https://a/mrf?sig=fresh", 1
    )
    raw = SimpleNamespace(head=None)
    requested_urls: list[str] = []

    async def stale_then_fresh(url, **_kwargs):
        requested_urls.append(url)
        if url == stale_attempt.source_url:
            raise ValueError("expired")
        return raw

    monkeypatch.setattr(acquisition, "download_raw_artifact", stale_then_fresh)
    recovered = await acquisition.download_source(
        (stale_attempt.source_url, (stale_attempt, fresh_attempt)), object(), 1024
    )
    assert recovered.raw is raw
    assert requested_urls == [stale_attempt.source_url, fresh_attempt.source_url]


@pytest.mark.asyncio
async def test_native_runner_rejects_debug_binary(tmp_path, monkeypatch):
    acquisition = _acquisition_module()
    monkeypatch.setattr(
        acquisition, "_ptg2_rust_scanner_binary",
        lambda: tmp_path / "debug" / "ptg2_scanner",
    )
    monkeypatch.setattr(
        acquisition, "_ptg2_scanner_binary_profile", lambda _path: "debug"
    )

    with pytest.raises(RuntimeError, match="release Rust parser"):
        await acquisition.run_native_parser(
            tmp_path / "input.json", tmp_path / "output", "a" * 64,
            "json", 1, 2048, 1024,
        )


@pytest.mark.asyncio
async def test_native_runner_drains_cleanup_after_repeated_cancel(
    tmp_path, monkeypatch
):
    acquisition = _acquisition_module()
    communicate_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    class Process:
        returncode = None

        async def communicate(self) -> tuple[bytes, bytes]:
            communicate_started.set()
            await asyncio.Future()
            raise AssertionError("cancelled parser communication returned")

    process = Process()

    async def spawn(*_args: Any, **_kwargs: Any) -> Process:
        return process

    async def terminate(_process: Process) -> None:
        cleanup_started.set()
        await allow_cleanup.wait()
        cleanup_finished.set()

    binary = tmp_path / "release" / "ptg2_scanner"
    monkeypatch.setattr(acquisition, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(
        acquisition, "_ptg2_scanner_binary_profile", lambda _path: "release"
    )
    monkeypatch.setattr(acquisition.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(acquisition, "_terminate_asyncio_subprocess_group", terminate)
    operation = asyncio.create_task(acquisition.run_native_parser(
        tmp_path / "input.json", tmp_path / "output", "a" * 64,
        "json", 1, 2048, 1024,
    ))
    await asyncio.wait_for(communicate_started.wait(), timeout=1)

    operation.cancel()
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    operation.cancel()
    await asyncio.sleep(0)
    assert not operation.done()
    allow_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert cleanup_finished.is_set()


@pytest.mark.asyncio
async def test_native_runner_passes_and_validates_exact_output_cap(
    tmp_path, monkeypatch
):
    acquisition = _acquisition_module()
    call_by_name: dict[str, Any] = {}
    expected_receipt = object()

    class Process:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"{}", b""

    async def spawn(*args: Any, **kwargs: Any) -> Process:
        call_by_name["args"] = args
        call_by_name["kwargs"] = kwargs
        return Process()

    def validate(summary_bytes: bytes, **kwargs: Any) -> object:
        call_by_name["summary_bytes"] = summary_bytes
        call_by_name["validation"] = kwargs
        call_by_name["validation_thread"] = threading.get_ident()
        return expected_receipt

    binary = tmp_path / "release" / "ptg2_scanner"
    monkeypatch.setattr(acquisition, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(
        acquisition, "_ptg2_scanner_binary_profile", lambda _path: "release"
    )
    monkeypatch.setattr(acquisition.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(acquisition, "validate_hospital_parser_summary", validate)

    event_loop_thread = threading.get_ident()
    receipt = await acquisition.run_native_parser(
        tmp_path / "input.json", tmp_path / "output", "a" * 64,
        "json", 123, 8192, 4096,
    )

    assert receipt is expected_receipt
    assert call_by_name["args"][-3:] == ("8192", "4096", "packed")
    assert call_by_name["validation"]["max_decompressed_bytes"] == 8192
    assert call_by_name["validation"]["max_output_bytes"] == 4096
    assert call_by_name["validation_thread"] != event_loop_thread
