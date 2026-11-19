# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Highmark reuses the shared importer with independent tenant scopes."""

import importlib
from unittest.mock import AsyncMock

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
