# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Highmark reuses the shared importer with independent tenant scopes."""

import importlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from urllib.parse import parse_qs, urlsplit

import pytest

from api.provider_directory_sources import provider_directory_source_catalog

directory = importlib.import_module("process.provider_directory_fhir")
BASE = "https://apicms.hmhs.com/fhir/v4/providers"
PARTITIONS = {"HM", "HMPAMS", "HMWVMS", "HMDE", "HMWV", "HMNY", "BCBSDE", "BCBSWY"}


@pytest.fixture
def tenant_sources():
    return {
        row["api_base"].rsplit("/", 1)[-1]: directory._source_row_from_seed(row)
        for row in directory.source_selection.configured_seed_rows()
        if row["api_base"] in directory.highmark.API_BASES
    }


def test_all_partitions_have_independent_catalog_checkpoints(tenant_sources):
    catalog_by_base = {
        row["canonical_base"]: row
        for row in provider_directory_source_catalog()["items"]
        if row["entry_id"].startswith("highmark-")
    }
    assert set(tenant_sources) == PARTITIONS
    assert set(catalog_by_base) == {f"{BASE}/{partition}" for partition in PARTITIONS}
    scope_hashes = set()
    for partition, source in tenant_sources.items():
        base = f"{BASE}/{partition}"
        entry = catalog_by_base[base]
        assert entry["source_ids"] == [source["source_id"]]
        assert entry["runnable"] and entry["profile_enabled"]
        assert entry["resources"] == list(directory.DEFAULT_RESOURCES)
        assert source["requires_api_key"] and source["requires_registration"]
        assert source["is_medicaid_mco"] == (partition in {"HMPAMS", "HMWVMS"})
        directory._assert_resource_acquisition_allowed(source, entry["resources"])
        assert directory._source_full_refresh_page_count(source, 100, 0, 0) == 500
        for resource in directory.DEFAULT_RESOURCES:
            assert directory._resource_start_url(source, resource, page_count=1000) == f"{base}/{resource}?_count=500"
        context = directory._pagination_checkpoint_context(
            source, entry["source_ids"], run_id="example-run", retry_of_run_id=None
        )
        assert context is not None and context.canonical_api_base == base
        scope_hashes.add(context.source_scope_hash)
    assert len(scope_hashes) == len(PARTITIONS)


def test_endpoint_identity_survives_key_rotation(monkeypatch, tenant_sources):
    source = tenant_sources["HM"]
    endpoint_id = directory._provider_directory_api_endpoint_row(source)["endpoint_id"]
    for key_value in (None, "synthetic-key", "rotated-synthetic-key"):
        monkeypatch.setattr(
            directory,
            "_credential_spec_for_source",
            lambda _, value=key_value: {"api_key": {"header": "x-hmhs-keyId", "value": value}} if value else {},
        )
        assert directory._provider_directory_api_endpoint_row(source)["endpoint_id"] == endpoint_id


def test_highmark_role_chunks_use_count_only_requests_and_bounded_windows(tenant_sources):
    chunk_config_by_tenant = {}
    for tenant, tenant_source in tenant_sources.items():
        config, _ = directory._last_updated_partition_config(tenant_source, "PractitionerRole")
        assert directory._last_updated_partition_config(tenant_source, "Organization")[0] is None
        if config is not None:
            chunk_config_by_tenant[tenant] = config
    assert set(chunk_config_by_tenant) == {"HM", "HMDE", "HMNY", "HMWV"}

    tenant_source = tenant_sources["HM"]
    config = chunk_config_by_tenant["HM"]
    assert config.census_mode == "unfiltered-leaf-sum-v1"
    assert config.page_count == 500 and 0 < config.ceiling <= 25000
    assert 0 < config.maximum_pages_per_window <= 64
    assert directory._source_resource_timeout(tenant_source, "PractitionerRole", 15) >= 60
    resource_url = directory._resource_start_url(tenant_source, "PractitionerRole", page_count=500)
    begin = datetime(2024, 1, 1, tzinfo=UTC)
    window = directory.TimeWindow("root", begin, begin + timedelta(days=1))

    def query(url):
        return parse_qs(urlsplit(url).query)

    census_query_map = {"_count": ["0"], "_summary": ["count"], "_total": ["accurate"]}
    window_query_map = {"_lastUpdated": ["ge2024-01-01T00:00:00.000000Z", "lt2024-01-02T00:00:00.000000Z"]}
    assert query(directory._last_updated_partition_unfiltered_count_url(resource_url)) == census_query_map
    assert query(directory._last_updated_partition_url(resource_url, window, page_count=500, count_only=True)) == {
        **census_query_map,
        **window_query_map,
    }
    assert query(directory._last_updated_partition_url(resource_url, window, page_count=500, count_only=False)) == {
        **window_query_map,
        "_count": ["500"],
    }


@pytest.mark.asyncio
async def test_highmark_role_chunks_follow_issued_cursor_and_reject_next_window_rows(monkeypatch, tenant_sources):
    tenant_source = tenant_sources["HM"]
    begin = datetime(2024, 1, 1, tzinfo=UTC)
    window = directory.TimeWindow("root", begin, begin + timedelta(days=1))
    first_url = directory._last_updated_partition_url(
        f"{BASE}/HM/PractitionerRole", window, page_count=500, count_only=False
    )
    next_url = f"{BASE}/HM?_getpages=opaque%2btoken%3d&_getpagesoffset=500&_count=500"
    entry_map = {
        "resource": {
            "resourceType": "PractitionerRole",
            "id": "synthetic-role",
            "meta": {"lastUpdated": "2024-01-01T01:00:00+01:00"},
        },
    }
    continuation_bundle_map = {"resourceType": "Bundle", "entry": [entry_map]}
    first_bundle_map = {**continuation_bundle_map, "type": "searchset", "link": [{"relation": "next", "url": next_url}]}
    cross_tenant_bundle_map = {
        **first_bundle_map,
        "link": [{"relation": "next", "url": next_url.replace("/HM?", "/HMNY?")}],
    }
    outside_resource_map = {**entry_map["resource"], "meta": {"lastUpdated": "2024-01-02T00:00:00Z"}}
    outside_bundle_map = {**continuation_bundle_map, "entry": [{"resource": outside_resource_map}]}
    monkeypatch.setattr(
        directory,
        "_fetch_source_json",
        AsyncMock(
            side_effect=[
                (200, bundle_value, None, 1)
                for bundle_value in (
                    first_bundle_map,
                    continuation_bundle_map,
                    cross_tenant_bundle_map,
                    outside_bundle_map,
                )
            ]
        ),
    )

    async def fetch(url):
        return await directory._fetch_last_updated_window_page(
            tenant_source, "PractitionerRole", url, window, timeout=3
        )

    first_page = await fetch(first_url)
    assert first_page.error is None and first_page.next_url == next_url
    final_page = await fetch(first_page.next_url)
    assert final_page.error is None and final_page.next_url is None and final_page.resources == (entry_map["resource"],)
    assert (await fetch(first_url)).error == "untrusted_pagination_link"
    assert (await fetch(next_url)).error == "resource_last_updated_outside_window"


def test_issued_cursor_retains_bytes_and_partition(tenant_sources):
    source = tenant_sources["HM"]
    first = f"{BASE}/HM/PractitionerRole?_count=500"
    following = f"{BASE}/HM?_getpages=opaque%2btoken%3d&_getpagesoffset=500&_count=500"
    assert directory._resolved_fhir_next_url(source, first, following) == following
    last = following.replace("offset=500", "offset=1000")
    assert directory._resolved_fhir_next_url(source, following, last) == last
    assert directory._source_fetch_candidate_urls(following) == [following]
    for invalid in (
        following.replace("/HM?", "/HMPAMS?"),
        following.replace("/HM?", "/HM/Organization?"),
        following.replace("apicms.hmhs.com", "untrusted.example"),
        f"{BASE}/HM?_page=1",
        f"{BASE}/HM?_getpages=x&_getpages=y",
    ):
        with pytest.raises(ValueError, match="untrusted_pagination_link"):
            directory._resolved_fhir_next_url(source, first, invalid)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "next_url",
    [
        f"{BASE}/HMPAMS?_getpages=synthetic-cursor",
        f"{BASE}/HM/Organization?_getpages=synthetic-cursor",
        "https://[invalid/HM?_getpages=synthetic-cursor",
    ],
    ids=["sibling-tenant", "wrong-collection", "malformed-url"],
)
async def test_untrusted_retained_cursor_preserves_checkpoint_without_fetching(monkeypatch, tenant_sources, next_url):
    highmark_source = tenant_sources["HM"]
    context = directory._pagination_checkpoint_context(
        highmark_source, [highmark_source["source_id"]], run_id="retry-example", retry_of_run_id=None
    )
    state = directory.PaginationResumeState(
        next_url=next_url, pages_processed=1, rows_processed=500, recent_url_hashes=(), resumed=True
    )
    load = AsyncMock(return_value=state)
    fetch, save, reset, write = AsyncMock(), AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(directory, "_load_or_initialize_pagination_checkpoint", load)
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    monkeypatch.setattr(directory, "_save_pagination_checkpoint", save)
    monkeypatch.setattr(directory, "_reset_pagination_checkpoint", reset)
    scan_outcome = await directory._fetch_resource_rows(
        highmark_source,
        "InsurancePlan",
        per_resource_limit=0,
        page_limit=0,
        page_count=500,
        timeout=3,
        run_id="retry-example",
        row_batch_handler=write,
        retain_rows=False,
        pagination_checkpoint=context,
    )
    load.assert_awaited_once_with(context, "InsurancePlan", f"{BASE}/HM/InsurancePlan?_count=500")
    assert (
        not scan_outcome.complete
        and scan_outcome.next_url_remaining
        and scan_outcome.error == "untrusted_pagination_link"
    )
    assert (scan_outcome.pages_fetched, scan_outcome.rows_fetched, scan_outcome.rows_written) == (1, 500, 0)
    fetch.assert_not_called()
    save.assert_not_called()
    reset.assert_not_called()
    write.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("entry_kind", "links", "expected_error"),
    [
        ("Organization", [], "highmark_invalid_search_bundle"),
        ("InsurancePlan", [{"relation": "next", "url": ""}], "highmark_invalid_next_link"),
        (
            "InsurancePlan",
            [{"relation": "next", "url": f"{BASE}/HMWV?_getpages=synthetic-cursor"}],
            "untrusted_pagination_link",
        ),
    ],
)
async def test_malformed_search_page_cannot_complete_or_write(
    monkeypatch, tenant_sources, entry_kind, links, expected_error
):
    payload = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [{"resource": {"resourceType": entry_kind, "id": "synthetic-plan"}}],
        "link": links,
    }
    monkeypatch.setattr(directory, "_fetch_source_json", AsyncMock(return_value=(200, payload, None, 1)))
    write = AsyncMock(return_value=1)
    result = await directory._fetch_resource_rows(
        tenant_sources["HM"],
        "InsurancePlan",
        per_resource_limit=0,
        page_limit=0,
        page_count=500,
        timeout=3,
        run_id=None,
        row_batch_handler=write,
    )
    assert result is not None and not result.complete and result.error == expected_error
    assert result.rows_fetched == 0
    write.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_bundle_type_is_only_allowed_on_issued_cursor(monkeypatch, tenant_sources):
    plan_by_field = {"resourceType": "InsurancePlan", "id": "synthetic-plan"}
    base = f"{BASE}/HM"
    cursor = f"{base}?_getpages=opaque%2btoken&_getpagesoffset=1"
    last_page_by_field = {
        "resourceType": "Bundle",
        "entry": [{"resource": {**plan_by_field, "id": "synthetic-plan-2"}}],
    }
    first_page_by_field = {
        "resourceType": "Bundle",
        "type": "searchset",
        "entry": [{"resource": plan_by_field}],
        "link": [{"relation": "next", "url": cursor}],
    }
    fetch = AsyncMock(side_effect=[(200, first_page_by_field, None, 1), (200, last_page_by_field, None, 1)])
    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    scan_outcome = await directory._fetch_resource_rows(
        tenant_sources["HM"],
        "InsurancePlan",
        per_resource_limit=0,
        page_limit=0,
        page_count=500,
        timeout=3,
        run_id=None,
    )
    assert scan_outcome is not None and scan_outcome.complete and scan_outcome.rows_fetched == 2
    assert fetch.await_args.args[1] == cursor
    with pytest.raises(ValueError, match="highmark_invalid_search_bundle"):
        directory.highmark.search_next_link(
            directory, tenant_sources["HM"], last_page_by_field, "InsurancePlan", f"{base}/InsurancePlan"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault", [None, "wrong_tenant_key", "extra_credential", "wrong_header", "missing_key", "wrong_url", "filter"]
)
async def test_window_census_sends_only_the_exact_tenant_key(monkeypatch, tenant_sources, fault):
    tenant_source = tenant_sources["HMDE"]
    resource = "PractitionerRole"
    config, _ = directory._last_updated_partition_config(tenant_source, resource)
    base = tenant_source["api_base"]
    credential_base = f"{BASE}/HM" if fault == "wrong_tenant_key" else base
    key_by_field = {"header": "X-Other-Key" if fault == "wrong_header" else "x-hmhs-keyId", "value": "example-key"}
    credential_map = {} if fault == "missing_key" else {"api_key": key_by_field}
    if fault == "extra_credential":
        credential_map["bearer_token"] = "example-token"
    monkeypatch.setattr(directory, "_load_credentials_config", lambda: {"api_bases": {credential_base: credential_map}})
    monkeypatch.setattr(directory, "_save_last_updated_partition_plan", AsyncMock())
    send = AsyncMock(return_value=(200, {"resourceType": "Bundle", "type": "searchset", "total": 31000}, None, 1))
    monkeypatch.setattr(directory, "_fetch_json_with_source_session", send)
    start_url = directory._resource_start_url(tenant_source, resource, page_count=500)
    if fault == "wrong_url":
        start_url = start_url.replace("/HMDE/", "/HM/")
    if fault == "filter":
        start_url += "&_id=example"
    state = directory.LastUpdatedPartitionState(
        None,
        directory._new_last_updated_partition_plan(config),
        directory.LastUpdatedCompletenessCensus(),
        start_url,
        0,
        0,
        None,
    )
    options = directory.LastUpdatedPartitionFetchOptions(0, 0, 60, None, None, 500, False, None, None, 0, None)
    census_result = await directory.leaf_census.pre_census(
        directory, tenant_source, resource, object, config, state, options
    )
    if fault is not None:
        assert census_result is not None and state.census.failure == "leaf_unfiltered_contract_required"
        send.assert_not_awaited()
        return
    assert census_result is None and state.census.unfiltered_pre == 31000
    assert len(state.plan.pending_count_windows()) == 2
    send.assert_awaited_once()
    url = urlsplit(send.await_args.args[1])
    assert url._replace(query="").geturl() == f"{base}/{resource}"
    assert parse_qs(url.query) == {"_count": ["0"], "_summary": ["count"], "_total": ["accurate"]}
    assert send.await_args.kwargs["extra_headers"] == {"x-hmhs-keyId": "example-key"}
    assert send.await_args.kwargs["allow_redirects"] is False


def _companion_checkpoints(context, resource, fault):
    """Represent the same-root predecessor and its independently bound plan."""
    identity_by_field = {
        "dataset_id": context.dataset_id,
        "source_ids": list(context.source_ids),
        "acquisition_root_run_id": context.acquisition_root_run_id,
        "owner_run_id": "original",
    }
    cursor_base = f"{BASE}/HMDE" if fault == "foreign_cursor" else context.canonical_api_base
    predecessor_by_field = {
        **identity_by_field,
        "state": "superseded",
        "resource_type": resource,
        "next_url": None,
        "completed_at": None,
        "pages_processed": 4,
        "rows_processed": 2000,
        "start_url_hash": "d" * 64,
        "completeness_json": {directory.highmark.RETIRED_CURSOR_KEY: f"{cursor_base}?_getpages=synthetic"},
    }
    if fault == "foreign_predecessor":
        predecessor_by_field["dataset_id"] = "different-dataset"
    return predecessor_by_field, {} if fault == "missing_successor" else identity_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "missing_successor", "foreign_predecessor", "foreign_cursor"])
async def test_retained_companion_uses_admitted_plan_and_preserves_progress(monkeypatch, tenant_sources, fault):
    """Only the verified successor may fetch, with both kinds of progress accounted for."""
    tenant = tenant_sources["HM"]
    base, resource = tenant["api_base"], "InsurancePlan"
    context = directory._pagination_checkpoint_context(
        tenant, [tenant["source_id"]], run_id="current", retry_of_run_id="original", pagination_root_run_id="original"
    )
    context = replace(context, dataset_id="example-dataset", lineage_verified=True)
    assert directory._last_updated_partition_config(tenant, resource) == (None, "resource_not_opted_in")
    predecessor_by_field, successor_by_field = _companion_checkpoints(context, resource, fault)
    checkpoint_fetch = AsyncMock(side_effect=[predecessor_by_field, successor_by_field])
    fetch = AsyncMock(
        return_value=directory._last_updated_partition_failure_result(
            object, None, "example", pages_fetched=5, rows_fetched=500, fetch_diagnostic={"example": True}
        )
    )
    monkeypatch.setattr(directory, "_fetch_pagination_checkpoint", checkpoint_fetch)
    monkeypatch.setattr(directory, "_fetch_last_updated_partition_resource_rows", fetch)

    async def scan():
        return await directory._fetch_resource_rows(
            tenant,
            resource,
            per_resource_limit=0,
            page_limit=0,
            page_count=500,
            timeout=3,
            run_id="current",
            pagination_checkpoint=context,
            deferred_materialization=True,
        )

    if fault:
        with pytest.raises(RuntimeError, match="provider_directory_highmark_companion_"):
            await scan()
        fetch.assert_not_awaited()
        return

    partition_result = await scan()
    fetch.assert_awaited_once()
    config = fetch.await_args.args[3]
    assert config.ceiling == 4000 and config.maximum_pages_per_window == 12
    assert directory._last_updated_window_hard_page_limit(tenant, resource, 4000) == 12
    assert (partition_result.pages_fetched, partition_result.rows_fetched) == (9, 2500)
    assert partition_result.fetch_diagnostic == {"example": True, "retained_ordinary_rows": 2000}
    completed = replace(fetch.return_value, complete=True, rows_fetched=300, error=None)
    assert directory.highmark.with_retained_companion_progress(completed, (4, 2000)).rows_fetched == 2600
    assert directory._compatible_pagination_resume_state(predecessor_by_field, context, "d" * 64) is None
    assert not directory._can_reset_empty_pagination_checkpoint(predecessor_by_field, context)
    assert directory._pagination_checkpoint_scope_identity(tenant, list(context.source_ids)) == (
        base,
        context.source_scope_hash,
    )


@pytest.mark.asyncio
async def test_highmark_key_is_required_and_used_only_in_verified_requests(monkeypatch, tenant_sources):
    selection = directory.source_selection
    source = tenant_sources["HM"]
    cursor = f"{BASE}/HM?_getpages=opaque%2Btoken&_getpagesoffset=500"
    send = AsyncMock(return_value=(200, {"resourceType": "Bundle"}, None, 1))
    monkeypatch.setattr(directory, "_fetch_json_with_source_session", send)
    monkeypatch.setattr(directory, "_credential_spec_for_source", lambda _: {"bearer_token": "unrelated"})
    assert (await selection.fetch_credentialed_rest_json(directory, source, cursor, timeout=5))[2] == (
        "provider_directory_credentials_unavailable"
    )
    send.assert_not_awaited()

    monkeypatch.setattr(
        directory,
        "_credential_spec_for_source",
        lambda _: {"api_key": {"header": "x-hmhs-keyId", "value": "synthetic-key"}, "bearer_token": "unrelated"},
    )
    with pytest.raises(selection.QualificationURLRejected):
        await selection.fetch_credentialed_rest_json(directory, source, cursor.replace("/HM?", "/HMWV?"), timeout=5)
    send.assert_not_awaited()
    assert (await selection.fetch_credentialed_rest_json(directory, source, cursor, timeout=5))[0] == 200
    assert send.await_args.args[1] == cursor
    assert send.await_args.kwargs == {
        "timeout": 5,
        "allow_redirects": False,
        "preserve_url_bytes": True,
        "extra_headers": {"x-hmhs-keyId": "synthetic-key"},
        "allow_gzip": True,
    }
