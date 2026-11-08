# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Offline proxy pacing, response rejection and restart-safe evidence checks."""

import asyncio
import hashlib
import json
import ssl
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import aiohttp
import pytest

from process import vsp_directory_http as http
from process import vsp_directory_store as retained
from process.vsp_directory_contract import API_BASE, SEARCH_URL, VSPDirectoryError, request_key

PROFILE_URL = API_BASE + "provider-web/publicdoctorprofile?officepublicid=101&asofdate=2035-01-01"
PAYLOAD = {"networkNames": ["Choice"], "offset": "1", "limit": "50"}
BODY = b'{"totalCounts":0,"locations":[]}'


class Clock:
    def __init__(self):
        self.now = 0.0
        self.sleeps = []

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        assert seconds >= 0
        self.sleeps.append(seconds)
        self.now += seconds
        await asyncio.sleep(0)


class Binding:
    policy_revision = "policy-example"
    pool_revision = "pool-example"

    def __init__(self):
        self.options = {"proxy": "https://proxy.example:8443", "ssl": ssl.create_default_context()}
        self.selections = []
        self.deferrals = []

    def select(self, url, key):
        self.selections.append((url, key))
        return SimpleNamespace(proxy_id="route-a", request_options=self.options)

    def cooldown_snapshot(self):
        return {"version": 1, "cooldowns": []}

    def defer(self, proxy_id, boundary):
        self.deferrals.append((proxy_id, boundary))


class Response:
    def __init__(self, raw=BODY, *, status=200, url=SEARCH_URL, headers=None, error=None):
        self.raw, self.status, self.url, self.error = raw, status, url, error
        self.headers = {"Content-Type": "application/json", "Content-Length": str(len(raw)), **(headers or {})}
        self.content = self

    async def __aenter__(self):
        if self.error:
            raise self.error
        return self

    async def __aexit__(self, *_args):
        return False

    async def iter_chunked(self, _size):
        yield self.raw[:3]
        yield self.raw[3:]


class Session:
    def __init__(self, clock, responses):
        self.clock, self.responses, self.calls = clock, iter(responses), []

    def request(self, method, url, **options):
        self.calls.append((self.clock(), method, url, options))
        return next(self.responses)


def client(responses, *, max_requests=100):
    clock, binding, pauses = Clock(), Binding(), []
    session = Session(clock, responses)

    async def pause(boundary, cooldowns):
        pauses.append((boundary, cooldowns))

    transport = http.VSPHTTP(session, binding, pause=pause, max_requests=max_requests, clock=clock, sleep=clock.sleep)
    return transport, session, binding, clock, pauses


@pytest.mark.asyncio
async def test_concurrent_search_and_profile_share_proxy_and_pacing():
    transport, session, binding, _, _ = client([Response(), Response(b"[]", url=PROFILE_URL)])
    search, profile = await asyncio.gather(
        transport.request("POST", SEARCH_URL, PAYLOAD), transport.request("GET", PROFILE_URL)
    )
    assert [call[0] for call in session.calls] == [0.0, http.MIN_REQUEST_INTERVAL]
    assert search[0] == {"totalCounts": 0, "locations": []} and profile[0] == []
    for _, _, _, options in session.calls:
        assert options["proxy"] == binding.options["proxy"] and options["ssl"].check_hostname
        assert options["allow_redirects"] is False
        assert options["headers"]["Accept-Encoding"] == "identity"
    assert search[2]["sha256"] == hashlib.sha256(BODY).hexdigest()
    assert search[2]["request_key"] == request_key("POST", SEARCH_URL, PAYLOAD)


@pytest.mark.asyncio
async def test_rate_limit_waits_globally_without_rotating_proxy():
    transport, session, binding, clock, pauses = client(
        [Response(status=429, headers={"Retry-After": "17"}), Response(), Response(b"[]", url=PROFILE_URL)]
    )
    await asyncio.gather(transport.request("POST", SEARCH_URL, PAYLOAD), transport.request("GET", PROFILE_URL))
    assert [call[0] for call in session.calls] == [0.0, 17.0, 18.0]
    assert binding.selections[0] == binding.selections[1] and binding.deferrals == []
    assert clock.sleeps[:2] == [15.0, 2.0] and len(pauses) == 4


@pytest.mark.asyncio
async def test_direct_transport_preserves_tls_pacing_and_publisher_backoff():
    clock, pauses = Clock(), []
    session = Session(
        clock, [Response(status=429, headers={"Retry-After": "17"}), Response(), Response(b"[]", url=PROFILE_URL)]
    )

    async def pause(boundary, cooldowns):
        pauses.append((boundary, cooldowns))

    transport = http.VSPHTTP(session, None, pause=pause, clock=clock, sleep=clock.sleep)
    search, _ = await asyncio.gather(
        transport.request("POST", SEARCH_URL, PAYLOAD), transport.request("GET", PROFILE_URL)
    )
    assert [call[0] for call in session.calls] == [0.0, 17.0, 18.0]
    for _, _, _, options in session.calls:
        assert "proxy" not in options and "proxy_auth" not in options
        assert options["ssl"].check_hostname and options["ssl"].verify_mode == ssl.CERT_REQUIRED
    assert search[2]["proxy_id"] is search[2]["policy_revision"] is search[2]["pool_revision"] is None
    assert len(pauses) == 4 and all(cooldowns is None for _, cooldowns in pauses)


@pytest.mark.asyncio
async def test_direct_connection_failure_retries_without_a_proxy_binding():
    clock = Clock()
    session = Session(clock, [Response(error=aiohttp.ClientConnectionError("synthetic")), Response()])

    async def pause(*_args):
        return None

    transport = http.VSPHTTP(session, None, pause=pause, clock=clock, sleep=clock.sleep)
    await transport.request("POST", SEARCH_URL, PAYLOAD)
    assert [call[0] for call in session.calls] == [0.0, 5.0]


@pytest.mark.asyncio
async def test_connection_failure_defers_only_the_failed_route():
    failure = aiohttp.ClientConnectionError("synthetic transport detail must not escape")
    transport, session, binding, clock, _ = client([Response(error=failure), Response()])
    await transport.request("POST", SEARCH_URL, PAYLOAD)
    assert len(session.calls) == 2 and clock.now >= 5
    assert len(binding.deferrals) == 1 and binding.deferrals[0][0] == "route-a"


@pytest.mark.asyncio
async def test_terminal_transport_error_does_not_expose_exception_detail():
    responses = [Response(error=aiohttp.ClientConnectionError("synthetic private connection detail")) for _ in range(3)]
    transport, session, binding, _, _ = client(responses)
    with pytest.raises(VSPDirectoryError, match="^vsp_http_transport_failed$"):
        await transport.request("POST", SEARCH_URL, PAYLOAD)
    assert len(session.calls) == 3 and len(binding.deferrals) == 2


@pytest.mark.asyncio
async def test_retry_attempts_count_toward_origin_request_budget():
    transport, session, _, _, pauses = client([Response(status=429, headers={"Retry-After": "3"})], max_requests=1)
    with pytest.raises(VSPDirectoryError, match="^vsp_request_budget_exhausted$"):
        await transport.request("POST", SEARCH_URL, PAYLOAD)
    assert len(session.calls) == 1 and len(pauses) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("options", [{"proxy": None}, {"ssl": False}])
async def test_proxy_or_verified_tls_cannot_silently_fall_back(options):
    transport, session, binding, _, _ = client([])
    binding.options.update(options)
    with pytest.raises(VSPDirectoryError, match="^vsp_source_proxy_required$"):
        await transport.request("POST", SEARCH_URL, PAYLOAD)
    assert session.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response,code",
    [
        (Response(status=302), "vsp_http_terminal_status"),
        (Response(status=403), "vsp_http_terminal_status"),
        (Response(url="https://other.example/private"), "vsp_response_url_changed"),
        (Response(b'{"totalCounts":0,"errors":[{"code":"B003"}]}'), "vsp_search_source_error"),
        (Response(b'{"a":1,"a":2}'), "vsp_response_json_invalid"),
        (Response(b'{"a":'), "vsp_response_json_invalid"),
        (Response(b'{"a":NaN}'), "vsp_response_json_invalid"),
        (Response(headers={"Content-Length": str(len(BODY) + 1)}), "vsp_response_truncated"),
        (Response(headers={"Content-Type": "text/html"}), "vsp_response_media_type_invalid"),
        (Response(headers={"Content-Encoding": "gzip"}), "vsp_response_encoding_invalid"),
    ],
)
async def test_invalid_source_response_is_terminal_and_sanitized(response, code):
    transport, session, binding, _, _ = client([response])
    with pytest.raises(VSPDirectoryError) as failure:
        await transport.request("POST", SEARCH_URL, PAYLOAD)
    assert str(failure.value) == code
    assert len(session.calls) == 1 and binding.deferrals == []


@pytest.mark.asyncio
async def test_streaming_byte_bound_is_enforced_without_declared_length(monkeypatch):
    response = Response()
    response.headers.pop("Content-Length")
    monkeypatch.setattr(http, "MAX_RESPONSE_BYTES", 8)
    transport, _, _, _, _ = client([response])
    with pytest.raises(VSPDirectoryError, match="^vsp_response_length_invalid$"):
        await transport.request("POST", SEARCH_URL, PAYLOAD)


def provenance(raw, *, observed="2035-01-01T00:00:00+00:00"):
    return {
        "request_key": request_key("POST", SEARCH_URL, PAYLOAD),
        "method": "POST",
        "url": SEARCH_URL,
        "payload": PAYLOAD,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "byte_count": len(raw),
        "observed_at": observed,
        "proxy_id": "route-a",
    }


def test_restart_reuses_only_the_exact_verified_request(tmp_path):
    store = retained.VSPCaptureStore(tmp_path, "synthetic-run")
    store.retain(BODY, provenance(BODY))
    resumed = retained.VSPCaptureStore(tmp_path, "synthetic-run")
    value, metadata = resumed.lookup("POST", SEARCH_URL, dict(reversed(list(PAYLOAD.items()))))
    assert value == json.loads(BODY) and metadata["sha256"] == hashlib.sha256(BODY).hexdigest()
    assert resumed.lookup("POST", SEARCH_URL, {**PAYLOAD, "networkNames": ["Signature"]}) is None
    raw_path = next((store.path / "raw").rglob("*.json"))
    raw_path.write_bytes(BODY.replace(b"0", b"1"))
    with pytest.raises(VSPDirectoryError, match="^vsp_response_digest_mismatch$"):
        resumed.lookup("POST", SEARCH_URL, PAYLOAD)
    with pytest.raises(VSPDirectoryError, match="^vsp_retained_evidence_changed$"):
        store.retain(BODY, provenance(BODY))


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("sha256", "0" * 64, "vsp_response_digest_mismatch"),
        ("byte_count", len(BODY) + 1, "vsp_response_digest_mismatch"),
        ("request_key", "0" * 64, "vsp_request_identity_mismatch"),
    ],
)
def test_invalid_retained_identity_is_rejected_before_writing(tmp_path, field, value, code):
    store = retained.VSPCaptureStore(tmp_path, "synthetic-run")
    with pytest.raises(VSPDirectoryError, match=f"^{code}$"):
        store.retain(BODY, {**provenance(BODY), field: value})
    assert list(store.requests()) == [] and not (store.path / "raw").exists()


def test_fresh_census_keeps_both_receipts_and_first_cached_observation(tmp_path):
    store = retained.VSPCaptureStore(tmp_path, "synthetic-run")
    later = BODY.replace(b"0", b"1")
    store.retain(BODY, provenance(BODY))
    store.retain(later, provenance(later, observed="2035-01-01T01:00:00+00:00"))
    assert len(list(store.requests())) == 2
    assert {x["sha256"] for x in store.requests()} == {hashlib.sha256(x).hexdigest() for x in (BODY, later)}
    assert store.lookup("POST", SEARCH_URL, PAYLOAD)[0] == json.loads(BODY)


def test_source_lock_rejects_overlapping_captures_and_releases_cleanly(tmp_path):
    owner = retained.VSPCaptureStore(tmp_path, "synthetic-first")
    blocked = retained.VSPCaptureStore(tmp_path, "synthetic-second")
    owner.acquire()
    try:
        with pytest.raises(VSPDirectoryError, match="^vsp_source_capture_already_active$"):
            blocked.acquire()
    finally:
        owner.release()
    blocked.acquire()
    blocked.release()


def test_failed_checkpoint_write_preserves_previous_state_and_cleans_temporary_file(tmp_path, monkeypatch):
    store = retained.VSPCaptureStore(tmp_path, "synthetic-run")
    store.checkpoint("discovery", {"pending": ["tile-a"]})

    def unavailable(*_args):
        raise OSError("synthetic disk failure")

    monkeypatch.setattr(retained.os, "replace", unavailable)
    with pytest.raises(OSError, match="synthetic disk failure"):
        store.checkpoint("discovery", {"pending": []})
    assert store.load_checkpoint("discovery") == {"pending": ["tile-a"]}
    assert list(store.path.glob(".vsp-*")) == []


@pytest.mark.asyncio
async def test_source_pause_survives_restart_and_cannot_be_shortened(tmp_path):
    store = retained.VSPCaptureStore(tmp_path, "synthetic-run")
    now = datetime.now(UTC)
    later = now + timedelta(minutes=2)
    await store.pause(later.isoformat(), {"version": 1, "cooldowns": []})
    await store.pause((now + timedelta(seconds=5)).isoformat(), {"version": 1, "cooldowns": []})
    resumed = retained.VSPCaptureStore(tmp_path, "synthetic-run")
    assert datetime.fromisoformat(resumed.pause_state()["not_before"]) == later
