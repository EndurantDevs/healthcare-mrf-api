# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime as dt
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import mrf_source_discovery as discovery
from tests.test_mrf_discovery_adapter_contracts import _Response, _Session

SOURCE = {"source_id": "source_example", "display_name": "Example Plan"}
URL = "https://adapter.example.invalid/landing"


class _TrackedResponse(_Response):
    """Record response release on both successful and rejected bodies."""

    async def __aexit__(self, *exc):
        self.released = True
        return await super().__aexit__(*exc)


@pytest.mark.parametrize(
    ("chunks", "limit", "expected", "error"),
    [
        ((b'{"name":', b'"Example"}'), 18, {"name": "Example"}, None),
        ((b"{}", b" "), 2, None, "exceeds 2 byte"),
        ((b"[]",), 10, None, "expected JSON object"),
        ((b"not-json",), 10, None, "Expecting value"),
    ],
)
@pytest.mark.asyncio
async def test_streamed_json_response_contract(monkeypatch, chunks, limit, expected, error):
    """Bounded streaming keeps headers and releases malformed responses."""
    response = _TrackedResponse(*chunks)
    session = _Session(response)
    allowed = AsyncMock()
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", allowed)
    request = discovery._fetch_json_with_headers(
        URL, headers={"Accept": "application/json"}, max_bytes=limit, session=session
    )
    if error:
        with pytest.raises(ValueError, match=error):
            await request
    else:
        assert await request == expected
    assert response.released
    assert session.get_calls == [(URL, {"headers": {"Accept": "application/json"}, "allow_redirects": True})]
    assert [call.args[0] for call in allowed.await_args_list] == [URL, response.url]


@pytest.mark.asyncio
async def test_redirect_fence_releases_response(monkeypatch):
    """A disallowed redirect must not yield a discovery payload."""
    response = _TrackedResponse(b'{"accepted":true}')
    allowed = AsyncMock(side_effect=[None, ValueError("blocked redirect")])
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", allowed)
    with pytest.raises(ValueError, match="blocked redirect"):
        await discovery._fetch_json_with_headers(URL, headers={}, max_bytes=100, session=_Session(response))
    assert response.released
    assert allowed.await_count == 2


def _websocket(*frames):
    return SimpleNamespace(
        receive=AsyncMock(side_effect=frames), send_str=AsyncMock(), exception=Mock(return_value="broken transport")
    )


def _text_frame(*messages):
    return SimpleNamespace(
        type=discovery.aiohttp.WSMsgType.TEXT, data="a" + json.dumps([json.dumps(message) for message in messages])
    )


def _sent_messages(websocket):
    return [json.loads(item) for call in websocket.send_str.await_args_list for item in json.loads(call.args[0])]


@pytest.mark.parametrize("frame_type", ["CLOSE", "CLOSED", "CLOSING", "ERROR"])
@pytest.mark.asyncio
async def test_websocket_transport_errors(frame_type):
    """Closed or failed transports fail promptly rather than returning data."""
    websocket = _websocket(SimpleNamespace(type=getattr(discovery.aiohttp.WSMsgType, frame_type)))
    with pytest.raises(ValueError, match="broken transport" if frame_type == "ERROR" else "closed before a response"):
        await discovery._mymedicalshopper_ddp_recv(websocket, timeout_seconds=1)
    assert websocket.receive.await_count == 1


@pytest.mark.asyncio
async def test_websocket_pings_preserve_results():
    """Heartbeat acknowledgments retain optional IDs without losing results."""
    result_by_field = {"msg": "result", "id": "request", "result": {"files": []}}
    websocket = _websocket(
        _text_frame({"msg": "ping"}), _text_frame({"msg": "ping", "id": "heartbeat"}, result_by_field)
    )
    assert await discovery._mymedicalshopper_ddp_recv(websocket, timeout_seconds=1) == [result_by_field]
    assert _sent_messages(websocket) == [{"msg": "pong"}, {"msg": "pong", "id": "heartbeat"}]


@pytest.mark.parametrize("accepted", [True, False])
@pytest.mark.asyncio
async def test_websocket_connection_negotiation(monkeypatch, accepted):
    """Negotiate the supported protocol or report an explicit server rejection."""
    outcome = {"msg": "connected", "session": "example"} if accepted else {"msg": "failed", "version": "unsupported"}
    websocket = _websocket(_text_frame(), _text_frame({"msg": "notice"}), _text_frame(outcome))
    session = SimpleNamespace(ws_connect=AsyncMock(return_value=websocket))
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", AsyncMock())
    request = discovery._mymedicalshopper_ddp_connect(session, URL, timeout_seconds=1)
    if accepted:
        assert await request is websocket
    else:
        with pytest.raises(ValueError, match="connection failed"):
            await request
    assert _sent_messages(websocket) == [{"msg": "connect", "version": "1", "support": ["1", "pre2", "pre1"]}]
    assert session.ws_connect.await_args.kwargs["headers"]["Origin"] == "https://adapter.example.invalid"
    assert session.ws_connect.await_args.args[0].endswith("/websocket")


@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.asyncio
async def test_websocket_method_correlates_response(failed):
    """Ignore unrelated results and keep the requested method's value or error."""
    answer = (
        {"msg": "result", "id": "wanted", "error": "rejected"}
        if failed
        else {"msg": "result", "id": "wanted", "result": ["plan"]}
    )
    websocket = _websocket(_text_frame({"msg": "notice"}, {"msg": "result", "id": "other"}), _text_frame(answer))
    request = discovery._mymedicalshopper_ddp_call(
        websocket, method="plans", params=["example"], request_id="wanted", timeout_seconds=1
    )
    if failed:
        with pytest.raises(ValueError, match="method plans failed: rejected"):
            await request
    else:
        assert await request == ["plan"]
    assert _sent_messages(websocket) == [{"msg": "method", "id": "wanted", "method": "plans", "params": ["example"]}]


@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.asyncio
async def test_websocket_subscription_preserves_events(failed):
    """Keep ordered updates until this subscription is ready or rejected."""
    updates = [{"msg": kind, "id": "plan", "collection": "plans"} for kind in ("added", "changed", "removed")]
    answer = {"msg": "nosub", "id": "wanted", "error": "denied"} if failed else {"msg": "ready", "subs": ["wanted"]}
    websocket = _websocket(
        _text_frame(*updates, {"msg": "ready", "subs": ["other"]}, {"msg": "nosub", "id": "other"}), _text_frame(answer)
    )
    request = discovery._mymedicalshopper_ddp_subscribe_collect(
        websocket, name="plans", params=[], sub_id="wanted", timeout_seconds=1
    )
    if failed:
        with pytest.raises(ValueError, match="subscription plans failed: denied"):
            await request
    else:
        assert await request == updates
    assert _sent_messages(websocket) == [{"msg": "sub", "id": "wanted", "name": "plans", "params": []}]


@pytest.mark.parametrize("progress", [None, "progress-example"])
@pytest.mark.asyncio
async def test_file_probe_pipeline_persists_metadata(monkeypatch, progress):
    """Mixed probe results drain all workers and persist only valid metadata."""
    probe_targets = [
        {
            "mrf_file_id": f"file-{index}",
            "url": f"https://files.example.invalid/{index}.json",
            "file_type": "in-network",
        }
        for index in range(3)
    ]
    checked = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    heads = [
        {"status": "ok", "checked_at": checked, "content_length": 0, "etag": "new"},
        {"status": "http_error", "checked_at": checked, "http_status": 404},
        {"status": "ok", "checked_at": checked},
    ]
    session = _Session(_Response())
    persisted, updates = [], []

    async def store(rows, model, **kwargs):
        assert model is discovery.MRFUrlObservation
        assert kwargs == {"rewrite": True, "use_copy": False}
        persisted.extend(rows)

    async def update_metadata(rows):
        updates.extend(rows)

    loader = AsyncMock(return_value=probe_targets)
    monkeypatch.setattr(discovery, "_load_file_probe_targets", loader)
    monkeypatch.setattr(discovery, "_head_url", AsyncMock(side_effect=heads))
    monkeypatch.setattr(discovery.aiohttp, "ClientSession", Mock(return_value=session))
    monkeypatch.setattr(discovery, "_tcp_connector", Mock(return_value=None))
    monkeypatch.setattr(discovery, "push_objects", store)
    monkeypatch.setattr(discovery, "_update_mrf_file_probe_metadata", update_metadata)
    monkeypatch.setattr(discovery, "WRITE_BATCH_SIZE", 2)
    reporting = Mock()
    monkeypatch.setattr(discovery, "enqueue_live_progress", reporting)
    observations, successes = await discovery._probe_mrf_file_heads(
        file_types=("in-network",), limit=3, run_id="run-example", progress_run_id=progress, concurrency=2
    )
    assert successes == 2 and len(observations) == 3
    assert persisted == observations
    assert updates == [{"mrf_file_id": "file-0", "size_bytes": 0, "etag": "new"}]
    assert [observation_row["status"] for observation_row in observations] == ["ok", "http_error", "ok"]
    assert all(observation_row["metadata_json"]["run_id"] == "run-example" for observation_row in observations)
    assert session.entered and session.exited
    assert reporting.call_count == (3 if progress else 0)
    if progress:
        assert [call.kwargs["done"] for call in reporting.call_args_list] == [1, 2, 3]
    loader.assert_awaited_once_with(("in-network",), 3, entity_types=(), payer_query=None)


@pytest.mark.asyncio
async def test_empty_probe_avoids_http_session(monkeypatch):
    """An empty catalog requires no HTTP connection or observation writes."""
    monkeypatch.setattr(discovery, "_load_file_probe_targets", AsyncMock(return_value=[]))
    session_factory = Mock()
    monkeypatch.setattr(discovery.aiohttp, "ClientSession", session_factory)
    assert await discovery._probe_mrf_file_heads(file_types=(), limit=None, run_id=None, concurrency=0) == ([], 0)
    session_factory.assert_not_called()


@pytest.mark.parametrize("limit", [None, 2])
@pytest.mark.asyncio
async def test_probe_catalog_filters_and_balances(monkeypatch, limit):
    """Entity and payer filters survive host balancing and the global cap."""
    records = [
        (str(index), f"https://{host}.example.invalid/{index}", "in-network", "payer", "Example Plan", "issuer")
        for index, host in enumerate(("a", "a", "b"))
    ]
    query = AsyncMock(return_value=records)
    monkeypatch.setattr(discovery.db, "all", query)
    targets = await discovery._load_file_probe_targets(
        ("in-network",), limit, entity_types=("issuer", "administrator"), payer_query="EXAMPLE"
    )
    assert [target["mrf_file_id"] for target in targets] == (["0", "2"] if limit else ["0", "2", "1"])
    compiled = query.await_args.args[0].compile()
    assert "%example%" in compiled.params.values()
    assert "%issuer%" in compiled.params.values() and "%administrator%" in compiled.params.values()


@pytest.mark.asyncio
async def test_probe_updates_ignore_incomplete_rows(monkeypatch):
    """Missing IDs or empty updates cannot overwrite catalog metadata."""
    session = SimpleNamespace(execute=AsyncMock())
    context = AsyncMock()
    context.__aenter__.return_value = session
    monkeypatch.setattr(discovery.db, "session", Mock(return_value=context))
    await discovery._update_mrf_file_probe_metadata([])
    await discovery._update_mrf_file_probe_metadata(
        [{"etag": "invalid"}, {"mrf_file_id": "empty"}, {"mrf_file_id": "valid", "etag": "new"}]
    )
    assert session.execute.await_count == 1
    assert session.execute.await_args.args[0].compile().params == {"etag": "new", "mrf_file_id_1": "valid"}


@pytest.mark.parametrize("max_targets", [1, 0])
@pytest.mark.asyncio
async def test_azure_listing_merges_duplicate_targets(monkeypatch, max_targets):
    """Multiple listings deduplicate public URLs before applying a global cap."""
    xml = (
        "<EnumerationResults><Blobs>"
        + "".join(
            f"<Blob><Name>{name}_index.json</Name><Url>https://files.example.invalid/{name}_index.json</Url></Blob>"
            for name in ("a", "b")
        )
        + "</Blobs></EnumerationResults>"
    )
    fetch = AsyncMock(return_value=xml)
    monkeypatch.setattr(discovery, "_fetch_text", fetch)
    targets = await discovery._resolve_azure_mrf_listing(
        SOURCE, URL, {"listing_urls": [" ", URL, URL + "?page=2"], "max_targets": max_targets}, None
    )
    assert [target.url for target in targets] == [
        f"https://files.example.invalid/{name}_index.json" for name in (("a",) if max_targets else ("a", "b"))
    ]
    assert fetch.await_count == 2
    assert all(target.metadata["target_file_type"] == "table-of-contents" for target in targets)


@pytest.mark.parametrize(
    "adapter,message",
    [
        ("azure_mrf_listing", "no Azure MRF listing targets found"),
        ("s3_xml_listing", "no S3 XML MRF listing targets found"),
        ("cigna_static_mrf_lookup", "no Cigna MRF lookup URLs found"),
        ("bcbs_asomrf_filelist", "no BCBS ASO filelist URL found"),
        ("hcsc_asomrf_landing", "no HCSC ASO MRF targets found"),
    ],
)
@pytest.mark.asyncio
async def test_empty_adapter_reports_missing_targets(monkeypatch, adapter, message):
    """A successful but empty upstream response must not appear as discovery."""
    payload = "<EnumerationResults><Blobs /></EnumerationResults>" if "listing" in adapter else "<html></html>"
    monkeypatch.setattr(discovery, "_fetch_text", AsyncMock(return_value=payload))
    with pytest.raises(ValueError, match=message):
        await getattr(discovery, "_resolve_" + adapter)(SOURCE, URL, {}, None)


@pytest.mark.asyncio
async def test_hcsc_landing_retains_state_balance(monkeypatch):
    """A failed state page cannot hide healthy states or defeat the global cap."""
    landing = "".join(f'<a href="/{state}/asomrf">{state}</a>' for state in ("failed", "il", "tx"))

    async def fetch_text(url, **_kwargs):
        if url == URL:
            return landing
        if "/failed/" in url:
            raise OSError("unavailable state")
        return '<script>var filelist="/content/dam/bcbs/mrf/si-filelist.json";</script>'

    payload = [
        {"url": f"https://files.example.invalid/{state}{index}_index.json", "state": state}
        for state in ("IL", "TX")
        for index in range(2)
    ]
    monkeypatch.setattr(discovery, "_fetch_text", fetch_text)
    monkeypatch.setattr(discovery, "_fetch_json_value", AsyncMock(return_value=payload))
    targets = await discovery._resolve_hcsc_asomrf_landing(SOURCE, URL, {"max_state_pages": 3, "max_targets": 2}, None)
    assert [target.metadata["state"] for target in targets] == ["IL", "TX"]
    assert [target.url for target in targets] == [
        "https://files.example.invalid/IL0_index.json",
        "https://files.example.invalid/TX0_index.json",
    ]
    assert all(target.metadata["delegated_resolver"] == "bcbs_asomrf_filelist" for target in targets)
    assert all(target.metadata["hcsc_landing_url"] == URL for target in targets)


@pytest.mark.parametrize("max_targets", [1, 10])
@pytest.mark.asyncio
async def test_fchn_search_survives_failed_details(monkeypatch, max_targets):
    """Skip blocked details, deduplicate files, and stop after the target cap."""
    paths = [f"/PayorSearch/Home/PayorDetail/{index}" for index in range(4)]
    landing = "".join(f'<a href="{path}">Plan</a>' for path in paths)

    async def fetch_text(url, **_kwargs):
        if url == URL:
            return landing
        if url.endswith("/0"):
            raise OSError("unavailable detail")
        if url.endswith("/1"):
            return "<title>Just a moment...</title>"
        return '<a href="https://files.example.invalid/in-network-rates.json">In network machine readable file</a>'

    fetch = AsyncMock(side_effect=fetch_text)
    monkeypatch.setattr(discovery, "_fetch_text", fetch)
    targets = await discovery._resolve_fchn_payor_search(SOURCE, URL, {"max_targets": max_targets}, None)
    assert [target.url for target in targets] == ["https://files.example.invalid/in-network-rates.json"]
    assert targets[0].metadata["target_file_type"] == "in-network"
    assert targets[0].metadata["fchn_payor_detail_id"] == "2"
    assert fetch.await_count == (4 if max_targets == 1 else 5)


@pytest.mark.parametrize("payload", [{}, {"mrf": [None, {"files": [None, {}]}]}])
@pytest.mark.asyncio
async def test_cigna_lookup_rejects_empty_files(monkeypatch, payload):
    """Lookup links with unusable file records must report an empty inventory."""
    monkeypatch.setattr(
        discovery, "_fetch_text", AsyncMock(return_value='<a href="/static/mrf/latest.json">Lookup</a>')
    )
    fetch = AsyncMock(return_value=payload)
    monkeypatch.setattr(discovery, "_fetch_json", fetch)
    with pytest.raises(ValueError, match="no Cigna MRF index URLs"):
        await discovery._resolve_cigna_static_mrf_lookup(SOURCE, URL, {}, None)
    fetch.assert_awaited_once_with(
        "https://adapter.example.invalid/static/mrf/latest.json", max_bytes=2 * 1024 * 1024, session=None
    )


@pytest.mark.asyncio
async def test_cigna_lookup_preserves_group_metadata(monkeypatch):
    """Resolve all advertised lookup files with inherited plan identity."""
    monkeypatch.setattr(
        discovery, "_fetch_text", AsyncMock(return_value='<a href="/static/mrf/latest.json">Lookup</a>')
    )
    payload = {
        "mrf": [
            None,
            {
                "reporting_entity_name": "Example Plan",
                "files": [None, {"url": "https://files.example.invalid/example_index.json"}],
            },
        ]
    }
    monkeypatch.setattr(discovery, "_fetch_json", AsyncMock(return_value=payload))
    targets = await discovery._resolve_cigna_static_mrf_lookup(SOURCE, URL, {}, None)
    assert [target.url for target in targets] == ["https://files.example.invalid/example_index.json"]
    assert targets[0].metadata["reporting_entity_name"] == "Example Plan"
    assert targets[0].resolved_from_url == "https://adapter.example.invalid/static/mrf/latest.json"


@pytest.mark.parametrize(
    "status,final_url,error",
    [
        (200, "https://files.example.invalid/example_index.json", None),
        (404, "https://files.example.invalid/example_index.json", "HTTP 404"),
        (200, URL, "did not resolve to a TOC"),
    ],
)
@pytest.mark.asyncio
async def test_keyed_redirect_retains_head_metadata(monkeypatch, status, final_url, error):
    """Only successful TOC redirects retain the upstream header provenance."""
    keyed_url = "https://www.ibx.com/transparency-in-coverage/example?key=synthetic"
    response = _TrackedResponse(status=status)
    response.url = final_url
    response.headers.update({"ETag": "example-etag", "Content-Length": "17"})
    session = SimpleNamespace(head=Mock(return_value=response))
    allowed = AsyncMock()
    monkeypatch.setattr(discovery, "_assert_fetch_url_allowed", allowed)
    request = discovery._resolve_cmstic_keyed_toc_redirect(SOURCE, keyed_url, {}, session)
    if error:
        with pytest.raises(ValueError, match=error):
            await request
    else:
        targets = await request
        assert [target.url for target in targets] == [final_url]
        assert targets[0].metadata["etag"] == "example-etag"
        assert targets[0].metadata["content_length"] == "17"
        assert targets[0].resolved_from_url == keyed_url
    assert response.released
    assert [call.args[0] for call in allowed.await_args_list] == [keyed_url, final_url]


@pytest.mark.parametrize("payload", [{"url": "https://files.example.invalid/example_index.json"}, {}])
@pytest.mark.asyncio
async def test_file_info_deduplicates_brand_inventory(monkeypatch, payload):
    """Brand aliases returning the same file must yield one canonical target."""
    fetch = AsyncMock(return_value=payload)
    monkeypatch.setattr(discovery, "_fetch_json", fetch)
    request = discovery._resolve_cmstic_file_info(
        SOURCE,
        "https://www.ibx.com/transparency-in-coverage",
        {"default_brands_by_host": {"www.ibx.com": ["qcc", "khpe"]}},
        None,
    )
    if payload:
        assert [target.url for target in await request] == [payload["url"]]
    else:
        with pytest.raises(ValueError, match="did not include a TOC"):
            await request
    assert fetch.await_count == 2


@pytest.mark.parametrize(
    "frame", ["h", "o", "garbage", "a[", 'a{"msg":"result"}', 'a["bad-json", "[]", "null", "{\\"msg\\":\\"ready\\"}"]']
)
def test_sockjs_frames_ignore_protocol_noise(frame):
    """Heartbeat and malformed envelopes cannot become application records."""
    expected = [{"msg": "ready"}] if "ready" in frame else []
    assert discovery._mymedicalshopper_sockjs_messages(frame) == expected


@pytest.mark.parametrize(
    "plans,max_plans",
    [([], None), ({"invalid": "shape"}, None), (["plan-a", "plan-b"], 1), (["plan-a", "plan-b"], None)],
)
@pytest.mark.asyncio
async def test_generated_inventory_uses_selected_plans(plans, max_plans):
    """Empty plan lists stop discovery while a cap limits the subsequent request."""
    generated_by_field = {"generatedPlans": [{"planId": "plan-a"}]}
    websocket = _websocket(
        _text_frame({"msg": "result", "id": "mms-plans-example", "result": plans}),
        _text_frame({"msg": "result", "id": "mms-generated-example", "result": generated_by_field}),
    )
    actual = await discovery._mymedicalshopper_generated_for_employer(
        websocket, employer_slug="example", timeout_seconds=1, max_plans=max_plans
    )
    if not isinstance(plans, list) or not plans:
        assert actual == []
        assert websocket.receive.await_count == 1
    else:
        assert actual == generated_by_field
        selected = plans[:max_plans] if max_plans else plans
        assert _sent_messages(websocket)[1]["params"] == [selected]
    assert _sent_messages(websocket)[0]["params"] == [{"employerSlug": "example", "skipPlanDesign": True}]


@pytest.mark.parametrize(
    "generated",
    [
        None,
        [None, {"planId": "example"}],
        {"mrfGeneratedPlans": [False, {"planId": "example"}]},
        {"generatedPlans": [{"planId": "example"}]},
        {"plans": [{"planId": "example"}]},
        {"result": [{"planId": "example"}]},
        {"planId": "example"},
    ],
)
def test_generated_records_normalize_response_shapes(generated):
    """Supported API envelopes retain plan records and discard non-record values."""
    assert discovery._mymedicalshopper_generated_entries(generated) == (
        [] if generated is None else [{"planId": "example"}]
    )


@pytest.mark.parametrize("metadata_url", ["https://files.example.invalid/metadata.json", " "])
@pytest.mark.asyncio
async def test_healthsparq_request_limits_public_fields(monkeypatch, metadata_url):
    """Authenticate once and forward only supported nonempty search fields."""
    login = AsyncMock(return_value={})
    post = AsyncMock(return_value={"url": metadata_url})
    monkeypatch.setattr(discovery, "_fetch_json", login)
    monkeypatch.setattr(discovery, "_post_json", post)
    request = discovery._healthsparq_service_metadata_url(
        URL,
        resolver={"login_path": "login", "mrf_all_path": "mrf-all"},
        params={"brandCode": "example", "insurerCode": "", "searchTerm": "Example Plan", "unrelated": "ignored"},
        session=None,
    )
    if metadata_url.strip():
        resolved, service_url = await request
        assert resolved == metadata_url
        assert service_url == post.await_args.args[0]
    else:
        with pytest.raises(ValueError, match="did not return a metadata URL"):
            await request
    assert login.await_count == 1
    assert post.await_args.args[1] == {"brandCode": "example", "searchTerm": "Example Plan"}


@pytest.mark.asyncio
async def test_filelist_rejects_empty_inventory(monkeypatch):
    """An advertised file list with no usable TOCs must fail explicitly."""
    monkeypatch.setattr(
        discovery,
        "_fetch_text",
        AsyncMock(return_value='<script>var files="/content/dam/bcbs/mrf/si-filelist.json";</script>'),
    )
    monkeypatch.setattr(
        discovery,
        "_fetch_json_value",
        AsyncMock(return_value=[None, {}, {"url": "https://files.example.invalid/in-network.json"}]),
    )
    with pytest.raises(ValueError, match="no BCBS ASO index URLs"):
        await discovery._resolve_bcbs_asomrf_filelist(SOURCE, URL, {}, None)
