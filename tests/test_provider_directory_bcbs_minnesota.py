# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Counted time chunks and issued caches share the existing import pipeline."""

import datetime
import importlib
import urllib.parse

import pytest

from api.provider_directory_sources import provider_directory_source_catalog
from tests.test_process_provider_directory_fhir_unit import (
    _CensusSequenceResponder,
    _last_updated_partition_test_source,
    _run_last_updated_partition_test_fetch,
    unit_source_factory,
)

directory = importlib.import_module("process.provider_directory_fhir")
BASE = "https://api.bluecrossmn.com/fhir/provider"
SOURCE_ID = "pdfhir_69fc4cc6c4ce23447dc03058"


@pytest.fixture
def carrier_source():
    seed = next(row for row in directory.source_selection.configured_seed_rows() if row["api_base"] == BASE)
    return directory._source_row_from_seed(seed)


def test_catalog_preserves_carrier_identity_and_documented_scope(carrier_source):
    entry = next(row for row in provider_directory_source_catalog()["items"] if row["entry_id"] == "bcbs-minnesota")
    assert carrier_source["source_id"] == SOURCE_ID and entry["source_ids"] == [SOURCE_ID]
    assert carrier_source["canonical_api_base"] == entry["canonical_base"] == BASE
    assert carrier_source["org_name"] == "Blue Cross and Blue Shield of Minnesota"
    assert carrier_source["plan_name"] is None
    assert carrier_source["auth_type"] == "none" and not carrier_source["requires_registration"]
    assert not directory._has_source_declared_credentialed_access(carrier_source)
    assert entry["runnable"] and entry["profile_enabled"]
    assert entry["resources"] == list(directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES)
    assert entry["resource_profile"] == "A7"
    assert set(entry["supported_resources"]) == set(directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES)


@pytest.mark.parametrize(
    "resources",
    [[resource] for resource in directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES]
    + [list(directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES)],
)
def test_acquisition_uses_counted_time_chunks_and_cached_searches(carrier_source, resources):
    metadata = carrier_source["metadata_json"]
    assert metadata["provider_directory_acquisition_enabled"] is True
    assert directory._source_fully_enumerable_resource_types(carrier_source) == set(
        directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES
    )
    directory._assert_resource_acquisition_allowed(carrier_source, resources)
    for resource in directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES:
        count = 0 if resource in directory.bcbs_minnesota.CACHED_RESOURCES else 500
        assert (
            directory._resource_start_url(carrier_source, resource, page_count=1000)
            == f"{BASE}/{resource}?_count={count}"
        )
    assert directory._resource_start_url(carrier_source, "Endpoint", page_count=100) is None
    assert set(directory._last_updated_partition_resource_configs(carrier_source)) == {
        "Practitioner",
        "PractitionerRole",
        "Organization",
        "OrganizationAffiliation",
    }
    assert directory._source_request_interval_seconds(carrier_source) == 0.1
    assert directory._source_resource_scan_concurrency(carrier_source) == 8
    context = directory._pagination_checkpoint_context(
        carrier_source, [SOURCE_ID], run_id="example-import", retry_of_run_id=None
    )
    assert context is not None and context.canonical_api_base == BASE
    assert (
        directory._pagination_checkpoint_strategy_version(
            BASE, directory._last_updated_partition_resource_configs(carrier_source)
        )
        == directory.bcbs_minnesota.STRATEGY_VERSION
    )


@pytest.mark.asyncio
async def test_declared_collection_probe_keeps_the_confirmed_base(monkeypatch, carrier_source):
    requests = []

    async def fetch(_source, url, **_kwargs):
        requests.append(url)
        return 200, {"resourceType": "Bundle", "type": "searchset", "entry": []}, None, 1

    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    monkeypatch.setattr(directory, "_load_credentials_config", lambda: {})
    probe, capability = await directory._probe_source(carrier_source, timeout=5, run_id=None)
    assert probe["status"] == "valid" and probe["api_base"] == BASE
    assert capability is None and probe["resource_probe"]["search_bundle_valid"] is True
    assert requests == [f"{BASE}/Practitioner?_count=1"]


@pytest.mark.parametrize("resource", directory.PUBLIC_DIRECTORY_SEVEN_RESOURCES)
def test_count_zero_requests_keep_strict_census_and_positive_data_pages(resource):
    window = directory.TimeWindow(
        "root", datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC), datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)
    )
    start = f"{BASE}/{resource}?_count=500&_summary=data&_total=estimate"
    for url in (
        directory._last_updated_partition_unfiltered_count_url(start),
        directory._last_updated_partition_url(start, window, page_count=499, count_only=True),
    ):
        query_by_name = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        assert (
            query_by_name["_count"] == ["0"]
            and query_by_name["_summary"] == ["count"]
            and query_by_name["_total"] == ["accurate"]
        )
    data_url = directory._last_updated_partition_url(start, window, page_count=499, count_only=False)
    data_query_by_name = urllib.parse.parse_qs(urllib.parse.urlsplit(data_url).query)
    assert data_query_by_name["_count"] == ["499"]
    assert data_query_by_name["_lastUpdated"] == ["ge2024-01-01T00:00:00.000000Z", "lt2024-01-02T00:00:00.000000Z"]
    assert "_summary" not in data_query_by_name


@pytest.mark.parametrize(
    "base", ["https://example.test/fhir/provider", "https://api.bluecrossmn.com/fhir/provider-directory"]
)
def test_count_zero_does_not_change_other_sources_or_historical_aliases(base):
    window = directory.TimeWindow(
        "root", datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC), datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)
    )
    for url in (
        directory._last_updated_partition_unfiltered_count_url(f"{base}/Practitioner?_count=500"),
        directory._last_updated_partition_url(
            f"{base}/Practitioner?_count=500", window, page_count=499, count_only=True
        ),
    ):
        assert "_count" not in urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)


@pytest.mark.asyncio
async def test_count_zero_completes_shared_partition_proof(monkeypatch, unit_source_factory):
    source = _last_updated_partition_test_source()
    source.update(api_base=BASE, canonical_api_base=BASE)
    responder = _CensusSequenceResponder([1, 1, 1, 1])

    async def fetch(source_record, url, *, timeout):
        query_by_name = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        if query_by_name.get("_summary") == ["count"]:
            assert query_by_name["_count"] == ["0"]
        return await responder._fetch_source_json(source_record, url, timeout=timeout)

    outcome, staged, written = await _run_last_updated_partition_test_fetch(monkeypatch, source, fetch)
    assert outcome.complete and outcome.fetch_diagnostic["verified"] is True
    assert set(staged) == {"prac-1"} and written == []
    assert outcome.rows_fetched == outcome.rows_written == 1
    assert all(
        outcome.fetch_diagnostic[key] == 1
        for key in (
            "unfiltered_pre",
            "ranged_root_pre",
            "unfiltered_post",
            "ranged_root_post",
            "pass1_unique",
            "pass2_unique",
            "staged_candidate_count",
        )
    )


@pytest.mark.parametrize(
    ("timestamp", "error"),
    [
        ("2024-01-01T00:00:00Z", None),
        ("2023-12-31T19:00:00-05:00", None),
        ("2024-01-01T05:30:00+05:30", None),
        ("2024-01-01T23:59:59.999-00:00", None),
        ("2024-01-01T19:00:00-05:00", "resource_last_updated_outside_window"),
        ("2023-12-31T18:59:59-05:00", "resource_last_updated_outside_window"),
        ("2024-01-01T12:00:00", "resource_last_updated_invalid"),
        ("2024-01-01", "resource_last_updated_invalid"),
        (None, "resource_last_updated_invalid"),
        (1, "resource_last_updated_invalid"),
    ],
)
def test_resource_timestamp_offsets_preserve_utc_window_bounds(timestamp, error):
    window = directory.TimeWindow(
        "root", datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC), datetime.datetime(2024, 1, 2, tzinfo=datetime.UTC)
    )
    assert (
        directory._last_updated_partition_resource_window_error({"meta": {"lastUpdated": timestamp}}, window) == error
    )


@pytest.mark.parametrize("resource", sorted(directory.bcbs_minnesota.CACHED_RESOURCES))
@pytest.mark.asyncio
async def test_cached_pages_reuse_guards_when_full_page_next_links_are_absent(monkeypatch, carrier_source, resource):
    requested_urls = []

    async def fetch(_source, url, **_kwargs):
        requested_urls.append(url)
        query_by_name = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        if "_getpages" not in query_by_name:
            assert query_by_name["_count"] == ["0"]
            offset, count = 0, 50
            links = [
                {
                    "relation": "next",
                    "url": BASE + "?_getpages=example-cache-H&_getpagesoffset=50&_count=50&_bundletype=searchset",
                }
            ]
        else:
            assert query_by_name["_getpages"] == ["example-cache-H"] and query_by_name["_count"] == ["500"]
            offset = int(query_by_name["_getpagesoffset"][0])
            count = 500 if offset == 50 else 1
            links = []
        return (
            200,
            {
                "resourceType": "Bundle",
                "type": "searchset",
                "total": 551 if offset == 550 else None,
                "entry": [
                    {"resource": {"resourceType": resource, "id": f"record-{number}"}}
                    for number in range(offset, offset + count)
                ],
                "link": links,
            },
            None,
            1,
        )

    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    fetch_result = await directory._fetch_resource_rows(
        carrier_source, resource, per_resource_limit=0, page_limit=0, page_count=500, timeout=3, run_id=None
    )
    assert fetch_result.complete and fetch_result.rows_fetched == 551 and len(requested_urls) == 3
    assert urllib.parse.parse_qs(urllib.parse.urlsplit(requested_urls[-1]).query)["_getpagesoffset"] == ["550"]


@pytest.mark.parametrize("status", [400, 404, 410])
def test_expired_cached_search_never_resets_retained_progress(status):
    url = BASE + "?_getpages=example-cache-H&_getpagesoffset=500&_count=500"
    resume = directory.PaginationResumeState(
        next_url=url, pages_processed=2, rows_processed=500, recent_url_hashes=(), resumed=True
    )
    assert not directory._should_restart_expired_pagination_checkpoint(
        status_code=status,
        fetch_error=None,
        request_url=url,
        resume_state=resume,
        has_restart_attempted=False,
        allow_retained_pagination_reset=True,
    )


def test_cached_page_guard_and_resume_require_previous_fingerprints(carrier_source):
    entries = [{"resource": {"resourceType": "Location", "id": "example-location"}}]
    identity = directory._synthetic_position_page_identity(carrier_source, entries)
    assert identity is not None
    assert directory._synthetic_position_page_guard_error(identity, identity[0], ()) == "pagination_page_repeated"
    resume = directory.PaginationResumeState(
        next_url=BASE + "?_getpages=example-cache-H",
        pages_processed=1,
        rows_processed=50,
        recent_url_hashes=(),
        resumed=True,
    )
    assert directory._synthetic_position_resume_guard_error(carrier_source, resume) == "pagination_page_guard_missing"


@pytest.mark.parametrize(
    "next_link",
    [
        "https://example.test/fhir?_getpages=example-cache-H&_getpagesoffset=50&_count=50",
        BASE + "?_getpages=example-cache-H&_getpagesoffset=49&_count=50",
        BASE + "?_getpages=example-cache-H&_getpages=other&_getpagesoffset=50&_count=50",
        BASE + "/metadata?_getpages=example-cache-H&_getpagesoffset=50&_count=50",
    ],
)
def test_cached_initial_cursor_cannot_change_origin_offset_or_query(next_link):
    with pytest.raises(ValueError, match="minnesota_cache_"):
        directory.bcbs_minnesota.next_url(BASE + "/Location?_count=0", next_link, "Location", 500, 50)


def test_cached_handle_survives_gateway_decorations_and_short_page_ends():
    current = BASE + "?_getpages=example-cache-H&_getpagesoffset=50&_count=500"
    decorated = BASE + "?_getpages=example-cache-H-H&_getpagesoffset=550-H&_count=500-H"
    next_url = directory.bcbs_minnesota.next_url(current, decorated, "Location", 500, 500)
    query_by_name = urllib.parse.parse_qs(urllib.parse.urlsplit(next_url).query)
    assert query_by_name["_getpages"] == ["example-cache-H"] and query_by_name["_getpagesoffset"] == ["550"]
    assert directory.bcbs_minnesota.next_url(current, None, "Location", 500, 499) is None
    with pytest.raises(ValueError, match="cursor_changed"):
        directory.bcbs_minnesota.next_url(current, decorated.replace("550-H", "551-H"), "Location", 500, 500)


@pytest.mark.parametrize(
    "payload",
    [
        {"resourceType": "Bundle", "total": 3},
        {"resourceType": "Bundle", "type": "searchset", "total": 3, "entry": []},
        {"resourceType": "Bundle", "entry": [{"resource": {"resourceType": "Organization", "id": "example"}}]},
        {"resourceType": "Bundle", "entry": {}},
    ],
)
def test_invalid_cached_pages_fail_before_candidate_writes(payload):
    assert directory.bcbs_minnesota.page_error(payload, "Location", BASE + "/Location?_count=0")


@pytest.mark.parametrize(
    "links",
    [
        None,
        1,
        {},
        ["next"],
        [{"relation": "next", "url": ""}],
        [{"relation": "next", "url": 1}],
        [{"relation": "next", "url": "first"}, {"relation": "next", "url": "second"}],
    ],
)
def test_malformed_cached_continuations_cannot_become_terminal_pages(links):
    payload = {"resourceType": "Bundle", "link": links, "entry": []}
    assert (
        directory.bcbs_minnesota.page_error(
            payload, "Location", BASE + "?_getpages=example-cache-H&_getpagesoffset=50&_count=500"
        )
        == "minnesota_cache_links_invalid"
    )


@pytest.mark.parametrize(
    ("total", "expected"),
    [
        (187, "minnesota_cache_short_page_before_total"),
        (49, "minnesota_cache_total_below_processed"),
        (True, "minnesota_cache_total_invalid"),
        (-1, "minnesota_cache_total_invalid"),
        (None, None),
        (51, None),
    ],
)
def test_cached_short_pages_reject_contradictory_totals(total, expected):
    payload = {
        "resourceType": "Bundle",
        "total": total,
        "entry": [{"resource": {"resourceType": "Location", "id": "example-location"}}],
    }
    assert (
        directory.bcbs_minnesota.page_error(
            payload, "Location", BASE + "?_getpages=example-cache-H&_getpagesoffset=50&_count=500"
        )
        == expected
    )
