# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Product identities, exact cursors and durable acquisition stay independent."""

import copy
import importlib
import json
import ssl
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
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
    priority = fixture["priority"]
    assert directory._source_resource_fetch_order(source_record, list(directory.DEFAULT_RESOURCES)) == priority
    assert directory._source_resource_fetch_order(source_record, ["Endpoint", priority[0]]) == [priority[0], "Endpoint"]


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
@pytest.mark.parametrize("pinned", [False, True], ids=["common-session", "pinned-session"])
async def test_capital_async_transport_preserves_issued_query_on_wire(monkeypatch, product, pinned):
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
            session = directory._SOURCE_HTTP_SESSION.get()
            assert session.connector._ssl.check_hostname is True
            assert session.connector._ssl.verify_mode == ssl.CERT_REQUIRED
            request_options = []
            original_get = session.get

            def capture_get(url, *args, **options):
                request_options.append(options.copy())
                return original_get(url, *args, **options)

            monkeypatch.setattr(session, "get", capture_get)
            token = directory._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.set(session if pinned else None)
            try:
                response = await directory._fetch_source_json_unpaced(source_record, request_url, timeout=5)
                redirected = await directory._fetch_source_json_unpaced(
                    source_record, request_url + "&redirect=1", timeout=5
                )
            finally:
                directory._SOURCE_HTTP_PINNED_ANONYMOUS_SESSION.reset(token)
        assert response[0] == 200 and response[2] is None
        assert len(request_options) == 2
        assert all(options.get("ssl") is not False for options in request_options)
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


@pytest.mark.asyncio
@pytest.mark.parametrize("product", FIXTURE)
async def test_product_probe_forces_verified_session_with_keepalive_disabled(monkeypatch, product):
    seed_rows = [
        seed
        for seed in directory._capital_blue_cross_product_seed_rows()
        if seed["api_base"] == FIXTURE[product]["base"]
    ]
    monkeypatch.setenv(directory.SOURCE_HTTP_KEEPALIVE_ENV, "false")
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_TLS_VERIFY", "false")
    monkeypatch.setattr(directory, "ensure_database", AsyncMock())
    monkeypatch.setattr(directory, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(directory, "_resolve_seed_db", lambda *_args, **_kwargs: ("fixture.db", None))
    monkeypatch.setattr(directory, "_seed_rows_from_sqlite", lambda *_args, **_kwargs: seed_rows)
    monkeypatch.setattr(directory, "_upsert_rows", AsyncMock(return_value=1))
    monkeypatch.setattr(directory, "_credential_spec_for_source", lambda _source: {})

    async def fetch_metadata(session, url, **options):
        assert session is directory._SOURCE_HTTP_SESSION.get()
        assert session.connector._ssl.check_hostname is True
        assert session.connector._ssl.verify_mode == ssl.CERT_REQUIRED
        assert options["preserve_url_bytes"] is True and options["allow_redirects"] is False
        return 200, {"resourceType": "CapabilityStatement"}, None, 1

    async def probe(source_records, **_options):
        source_record = source_records[0]
        response = await directory._fetch_source_json_unpaced(
            source_record, source_record["api_base"] + "/metadata", timeout=5
        )
        assert response[0] == 200
        return 1, 1, {source_record["source_id"]}

    monkeypatch.setattr(directory, "_fetch_json_with_source_session", fetch_metadata)
    monkeypatch.setattr(directory, "_run_source_probe_batch", probe)
    audit_by_field = await directory.process_data(
        {"context": {}},
        {
            "source_ids": [FIXTURE[product]["source_id"]],
            "seed_db_path": "fixture.db",
            "probe": True,
            "import_resources": False,
            "publish_artifacts": False,
        },
    )
    assert audit_by_field["sources_probed"] == audit_by_field["valid_capability_sources"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("census", [False, True])
async def test_mixed_probe_preserves_each_sources_tls_and_combines_valid_ids(monkeypatch, census):
    strict_source_record = directory._source_row_from_seed(directory._capital_blue_cross_product_seed_rows()[0])
    ordinary_by_field = {
        "source_id": "ordinary",
        "api_base": "https://ordinary.example/fhir",
        "auth_type": "none",
    }
    capped_by_field = {**ordinary_by_field, "source_id": "capped"}
    monkeypatch.setattr(directory.capped_partition, "enabled", lambda _directory, record: record is capped_by_field)
    monkeypatch.setenv(directory.SOURCE_HTTP_KEEPALIVE_ENV, "true")
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_TLS_VERIFY", "false")
    observations = []
    progress = AsyncMock()
    monkeypatch.setattr(directory, "_mark_provider_directory_progress", progress)
    monkeypatch.setattr(directory, "_upsert_rows", AsyncMock())

    async def probe(record, **options):
        session = directory._SOURCE_HTTP_SESSION.get()
        is_verified = session.connector._ssl.verify_mode == ssl.CERT_REQUIRED
        observations.append(record["source_id"])
        assert options == {"timeout": 3, "run_id": "synthetic"}
        assert is_verified == (census or record is not ordinary_by_field)
        return {"status": "valid", "http_status": 200}, {"resourceType": "CapabilityStatement"}

    monkeypatch.setattr(directory, "_probe_source", probe)
    probe_result = await directory.source_selection.probe_sources_with_session(
        directory,
        [strict_source_record, ordinary_by_field, capped_by_field],
        census_contract=object() if census else None,
        proxy_run=SimpleNamespace(bindings={}),
        timeout=3,
        concurrency=8,
        run_id="synthetic",
    )
    assert probe_result == (3, 3, {strict_source_record["source_id"], "ordinary", "capped"})
    assert set(observations) == probe_result[2]
    assert [(call.kwargs["done"], call.kwargs["total"]) for call in progress.await_args_list] == [
        (0, 3),
        (1, 3),
        (2, 3),
        (3, 3),
    ]
    selected_sources, _metrics = directory._select_resource_import_sources(
        [ordinary_by_field], valid_source_ids=probe_result[2], open_only=True, include_auth_required=False
    )
    assert selected_sources == [ordinary_by_field]
    assert directory._SOURCE_HTTP_SESSION.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("same_base", [False, True])
@pytest.mark.parametrize("fail_ordinary", [False, True])
async def test_mixed_acquisition_keeps_tls_slots_and_progress_source_scoped(monkeypatch, fail_ordinary, same_base):
    """One strict group retains shared capacity, independent failures and aggregate progress."""
    strict_source_record = directory._source_row_from_seed(directory._capital_blue_cross_product_seed_rows()[0])
    ordinary_by_field = {
        "source_id": "ordinary",
        "api_base": strict_source_record["api_base"] if same_base else "https://ordinary.example/fhir",
        "auth_type": "none",
    }
    monkeypatch.setenv(directory.SOURCE_HTTP_KEEPALIVE_ENV, "false")
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_TLS_VERIFY", "false")
    monkeypatch.setattr(
        directory, "_prepare_resource_import_source_group", AsyncMock(side_effect=lambda rows, *_a, **_k: (rows, None))
    )
    monkeypatch.setattr(directory, "_update_source_resource_import_metadata", AsyncMock())
    observations = []
    progress = AsyncMock()

    async def fetch(record, resource_type, **_options):
        session = directory._SOURCE_HTTP_SESSION.get()
        is_verified = session.connector._ssl.verify_mode == ssl.CERT_REQUIRED
        assert is_verified == (record["source_id"] == strict_source_record["source_id"])
        assert session.connector.limit_per_host == 8
        slot_map = directory.SOURCE_HTTP_REQUEST_SLOTS.get()
        observations.append((record["source_id"], slot_map, slot_map["total"]))
        if fail_ordinary and not is_verified:
            raise ValueError("synthetic source failure")
        return _empty_location_result()

    monkeypatch.setattr(directory, "_fetch_resource_rows", fetch)
    request = directory._import_resources_with_source_http_session(
        [strict_source_record, ordinary_by_field],
        resources=["Location"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id=None,
        resource_scan_concurrency=8,
        source_concurrency=2,
        progress_callback=progress,
    )
    if fail_ordinary:
        with pytest.raises(RuntimeError, match="provider_directory_source_groups_failed:ordinary"):
            await request
    else:
        assert await request == {"Location": 0}
    assert {entry[0] for entry in observations} == {strict_source_record["source_id"], "ordinary"}
    assert observations[0][1] is observations[1][1] and observations[0][2] is observations[1][2]
    assert progress.await_args.args[:2] == (2, 2)
    assert directory._SOURCE_HTTP_SESSION.get() is None


def test_same_base_aliases_only_share_scans_with_the_same_continuation_policy():
    strict_by_field = directory._source_row_from_seed(directory._capital_blue_cross_product_seed_rows()[0])
    ordinary_by_field = {**strict_by_field, "source_id": "a-ordinary", "metadata_json": {}}
    strict_alias_by_field = {**strict_by_field, "source_id": "z-strict-alias"}
    groups = directory._group_resource_import_sources(
        [ordinary_by_field, strict_by_field, strict_alias_by_field], linked_resource_limit=0
    )
    assert groups == [[ordinary_by_field], [strict_by_field, strict_alias_by_field]]
    assert directory._resource_import_group_key(ordinary_by_field) != directory._resource_import_group_key(
        strict_by_field
    )
    assert (
        directory._provider_directory_api_endpoint_row(ordinary_by_field)["endpoint_id"]
        != (directory._provider_directory_api_endpoint_row(strict_by_field)["endpoint_id"])
    )
    assert directory._resource_endpoint_signature(ordinary_by_field) == tuple(
        (field, directory._clean_text(ordinary_by_field.get(field)) or "")
        for field in sorted(directory.RESOURCE_ENDPOINT_FIELDS.values())
    )


def _empty_location_result():
    return directory.ResourceFetchResult(
        model=directory.ProviderDirectoryLocation,
        rows=[],
        rows_fetched=0,
        rows_written=0,
        pages_fetched=1,
        complete=True,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=False,
    )
