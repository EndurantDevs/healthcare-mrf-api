# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Product identities, exact cursors and durable acquisition stay independent."""

import copy
import importlib
import json
import ssl
from pathlib import Path
from urllib.parse import urlsplit

import pytest
from aiohttp import web

from api.provider_directory_sources import provider_directory_source_catalog

directory = importlib.import_module("process.provider_directory_fhir")
FIXTURE = json.loads((Path(__file__).parent / "fixtures/provider_directory_capital_products.json").read_text())


@pytest.mark.parametrize("product", FIXTURE)
def test_capital_product_uses_existing_checkpointed_rest_contract(product):
    fixture = FIXTURE[product]
    source_record = next(
        directory._source_row_from_seed(seed_row)
        for seed_row in directory._capital_blue_cross_product_seed_rows()
        if seed_row["api_base"] == fixture["base"]
    )
    assert source_record["source_id"] == fixture["source_id"]
    metadata = source_record["metadata_json"]
    assert metadata["provider_directory_supported_resources"] == list(directory.DEFAULT_RESOURCES)
    assert metadata["provider_directory_expected_nonempty_resources"] == list(
        directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES
    )
    assert directory._resource_acquisition_blocked_reason(source_record) is None
    assert directory._is_source_bulk_export_requested(source_record, False) is False
    assert (
        directory._resource_start_url(source_record, "Location", page_count=1000)
        == fixture["base"] + "/Location?_count=100"
    )
    assert (
        directory._resolved_fhir_next_url(source_record, fixture["base"] + "/Location?_count=100", fixture["next"])
        == fixture["next"]
    )
    scope = directory._pagination_checkpoint_scope_identity(source_record, [source_record["source_id"]])
    assert scope is not None and scope[0] == fixture["base"]
    changed = copy.deepcopy(source_record)
    changed["metadata_json"].pop("provider_directory_pagination_same_collection")
    assert directory._pagination_checkpoint_scope_identity(changed, [source_record["source_id"]]) != scope
    assert directory._source_contract_metadata_keys(
        metadata, artifact=True
    ) != directory._source_contract_metadata_keys(changed["metadata_json"], artifact=True)
    entry = next(
        x for x in provider_directory_source_catalog()["items"] if x["source_ids"] == [source_record["source_id"]]
    )
    assert entry["runnable"] is True and entry["canonical_base"] == fixture["base"]


@pytest.mark.parametrize("product", FIXTURE)
@pytest.mark.parametrize("replacement", ["/r4/other-product/Location", "/r4/public-ma/Practitioner", "/Patient"])
def test_capital_cursor_cannot_cross_collection_or_product(product, replacement):
    fixture = FIXTURE[product]
    source_record = next(
        directory._source_row_from_seed(seed_row)
        for seed_row in directory._capital_blue_cross_product_seed_rows()
        if seed_row["api_base"] == fixture["base"]
    )
    with pytest.raises(ValueError, match="untrusted_pagination_link"):
        directory._resolved_fhir_next_url(
            source_record,
            fixture["base"] + "/Location?_count=100",
            "https://api.capitalbluecrossfhir.com" + replacement + "?_after=synthetic",
        )


def test_capital_legacy_identity_remains_probe_only():
    seed = directory._reviewed_provider_directory_candidate_seed_rows(source_query="Capital Blue Cross")[0]
    source_record = directory._source_row_from_seed(seed)
    assert source_record["source_id"] == "pdfhir_95d5ad93c689d016351dd088"
    assert (
        directory._resource_acquisition_blocked_reason(source_record)
        == directory.PROVIDER_DIRECTORY_PROBE_ONLY_BLOCKED_REASON
    )


def test_same_collection_opt_in_preserves_other_sources_contracts():
    source_by_field = {"api_base": "https://directory.example/fhir", "metadata_json": {}}
    current = source_by_field["api_base"] + "/Location"
    following = source_by_field["api_base"] + "/page?cursor=synthetic%2Bvalue"
    assert directory._resolved_fhir_next_url(source_by_field, current, following) == following
    assert (
        directory._source_contract_metadata_keys({}, artifact=True) == directory.ARTIFACT_SOURCE_CONTRACT_METADATA_KEYS
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("product", FIXTURE)
async def test_capital_async_transport_preserves_issued_query_on_wire(monkeypatch, product):
    observed_paths = []

    async def observe(request):
        observed_paths.append(request.raw_path)
        if "redirect" in request.query:
            return web.Response(status=302, headers={"Location": "/other-product/Location"})
        return web.json_response({"resourceType": "Bundle", "type": "searchset", "total": 0})

    application = web.Application()
    application.router.add_get("/{path:.*}", observe)
    runner = web.AppRunner(application)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        fixture = FIXTURE[product]
        source_record = next(
            directory._source_row_from_seed(seed_row)
            for seed_row in directory._capital_blue_cross_product_seed_rows()
            if seed_row["api_base"] == fixture["base"]
        )
        request_url = fixture["next"].replace("https://api.capitalbluecrossfhir.com", f"http://127.0.0.1:{port}")
        monkeypatch.setattr(directory, "_credential_spec_for_source", lambda _source: {})
        async with directory._source_http_session_scope(force=True, require_verified_tls=True):
            response = await directory._fetch_source_json_unpaced(source_record, request_url, timeout=5)
            redirected = await directory._fetch_source_json_unpaced(
                source_record, request_url + "&redirect=1", timeout=5
            )
        assert response[0] == 200 and response[2] is None
        parsed = urlsplit(request_url)
        assert observed_paths == [parsed.path + "?" + parsed.query, parsed.path + "?" + parsed.query + "&redirect=1"]
        assert redirected[0] == 302
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_same_collection_transport_refuses_missing_anonymous_session(monkeypatch):
    source_record = directory._source_row_from_seed(directory._capital_blue_cross_product_seed_rows()[0])
    monkeypatch.setattr(directory, "_rest_page_prefetch_session", lambda *_args: None)
    token = directory._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.set(None)
    try:
        with pytest.raises(RuntimeError, match="pagination_anonymous_session_required"):
            await directory._fetch_source_json_unpaced(source_record, FIXTURE["Medicare"]["next"], timeout=5)
    finally:
        directory._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.reset(token)


@pytest.mark.asyncio
async def test_same_collection_import_forces_existing_verified_session(monkeypatch):
    source_records = [
        directory._source_row_from_seed(seed) for seed in directory._capital_blue_cross_product_seed_rows()
    ]
    monkeypatch.setenv(directory.SOURCE_HTTP_KEEPALIVE_ENV, "false")
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_TLS_VERIFY", "false")

    async def inspect_import(_source_records, **_options):
        session = directory._SOURCE_HTTP_SESSION.get()
        assert session is not None and not session.closed
        assert session.connector.limit_per_host == 8
        assert session.connector._ssl.check_hostname is True
        assert session.connector._ssl.verify_mode == ssl.CERT_REQUIRED
        return {"Location": 0}

    monkeypatch.setattr(directory, "_import_resources", inspect_import)
    assert await directory._import_resources_with_source_http_session(source_records, resource_scan_concurrency=8) == {
        "Location": 0
    }
