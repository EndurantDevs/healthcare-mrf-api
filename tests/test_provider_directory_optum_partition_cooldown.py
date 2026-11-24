"""Same-cursor recovery for mandatory Optum partition pages."""

import asyncio
import importlib
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

importer = importlib.import_module("process.provider_directory_fhir")
BASE = importer.UHC_PROVIDER_DIRECTORY_BASE
START = f"{BASE}/Organization?_count=100&name=AB"
CURSOR = f"{BASE}?_getpages=synthetic%2Bcursor&_getpagesoffset=100"


@pytest.fixture
def chain():
    return importer.PartitionChainRequest(
        source_record={"source_id": "synthetic", "api_base": BASE},
        resource_type="Organization",
        start_url_queue=asyncio.Queue(),
        state=importer.PartitionFetchState(importer.ProviderDirectoryOrganization, []),
        request_url=START,
        fetch_options=importer.PartitionFetchOptions(
            timeout=3,
            run_id=None,
            row_batch_handler=None,
            row_batch_size=25,
            retain_rows=True,
            deadline_at=None,
            max_pages=0,
        ),
    )


def _page(resource_id, next_url=None):
    payload = {"resourceType": "Bundle", "entry": [{"resource": {"resourceType": "Organization", "id": resource_id}}]}
    if next_url:
        payload["link"] = [{"relation": "next", "url": next_url}]
    return 200, payload, None, 1


@pytest.mark.parametrize(
    ("status", "error"),
    [(429, None), (502, None), (503, None), (504, None), (None, "TimeoutError"), (None, "ServerDisconnectedError")],
)
async def test_partition_continuation_recovers(monkeypatch, chain, status, error):
    transient = (status, {importer.SOURCE_RETRY_AFTER_FIELD: "4"} if status else None, error, 1)
    fetch = AsyncMock(side_effect=[_page("first", CURSOR), transient, _page("second")])
    sleep = AsyncMock()
    checkpoint = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)
    monkeypatch.setattr(importer, "_record_partition_checkpoint", checkpoint)

    await importer._fetch_partition_url_chain(chain)

    assert [call.args[1] for call in fetch.await_args_list] == [START, CURSOR, CURSOR]
    sleep.assert_awaited_once_with(4.0 if status else 1.0)
    assert [row["resource_id"] for row in chain.state.retained_resource_rows] == ["first", "second"]
    assert chain.state.page_count == 2 and not chain.state.retry_leaf_urls
    assert checkpoint.await_args.kwargs == {"pages_fetched": 2, "rows_fetched": 2}


@pytest.mark.parametrize(
    ("status", "expected_attempts", "deadline"),
    [(503, 3, False), (500, 1, False), (503, 1, True)],
)
async def test_failed_continuation_retains_leaf(monkeypatch, chain, status, expected_attempts, deadline):
    failure = (status, {importer.SOURCE_RETRY_AFTER_FIELD: "300"}, None, 1)
    fetch = AsyncMock(side_effect=[_page("first", CURSOR), *([failure] * expected_attempts)])
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer.asyncio, "sleep", AsyncMock())
    checkpoint = AsyncMock()
    monkeypatch.setattr(importer, "_record_partition_checkpoint", checkpoint)
    if deadline:
        chain = replace(chain, fetch_options=replace(chain.fetch_options, deadline_at=importer.time.monotonic() + 200))

    await importer._fetch_partition_url_chain(chain)

    assert [call.args[1] for call in fetch.await_args_list] == [START] + [CURSOR] * expected_attempts
    assert chain.state.page_count == 1
    assert chain.state.retry_leaf_urls == {START: f"http_{status}"}
    checkpoint.assert_not_awaited()


@pytest.mark.parametrize("scope", ["start", "residual", "other_source"])
async def test_other_partition_pages_keep_retry_scope(monkeypatch, chain, scope):
    fetch = AsyncMock(return_value=(503, {}, None, 1))
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    request = importer.PartitionPageRequest(
        source_record=chain.source_record if scope != "other_source" else {"api_base": "https://other.example/fhir"},
        state=chain.state,
        resource_type=chain.resource_type,
        start_url_queue=chain.start_url_queue,
        partition_start_url=START,
        current_url=START if scope == "start" else CURSOR,
        is_residual_only=scope == "residual",
        pending_resource_rows=[],
        fetch_options=chain.fetch_options,
    )

    assert await importer._fetch_valid_partition_bundle(request) is None
    fetch.assert_awaited_once()
    assert chain.state.retry_leaf_urls == ({START: "http_503"} if scope == "start" else {})
    assert chain.state.partition_error_count == int(scope == "other_source")
