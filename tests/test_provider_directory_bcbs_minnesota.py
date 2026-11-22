# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Counted time chunks and issued caches share the existing import pipeline."""

import datetime
import importlib
import urllib.parse
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

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


def test_fresh_minnesota_plan_contains_updates_during_acquisition(carrier_source):
    config, error = directory._last_updated_partition_config(carrier_source, "Practitioner")
    assert error is None
    started = datetime.datetime.now(datetime.UTC)
    default = directory._last_updated_partition_initialization("Practitioner", config)
    minnesota = directory._last_updated_partition_initialization("Practitioner", config, BASE)
    assert minnesota.start_url_hash == default.start_url_hash
    regular_plan = directory._last_updated_partition_plan_from_checkpoint(default.checkpoint_payload, config)
    scoped_plan = directory._last_updated_partition_plan_from_checkpoint(minnesota.checkpoint_payload, config)
    assert regular_plan.windows["root"].end < started + datetime.timedelta(minutes=1)
    assert scoped_plan.windows["root"].end >= started + datetime.timedelta(days=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "count_changed", "wrong_id", "wrong_stamp", "throttled"])
async def test_timed_out_practitioner_verifies_small_retained_window(carrier_source, monkeypatch, outcome):
    start = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    window = directory.TimeWindow("root", start, start + datetime.timedelta(days=1), count=2, passes={1: None})
    start_url = f"{BASE}/Practitioner?_count=500"
    state = SimpleNamespace(context=object(), start_url=start_url, deadline_at=None)
    options = SimpleNamespace(timeout=5, cancel_ctx=None, cancel_task=None)
    range_fetch = directory.LastUpdatedWindowFetch((), 1, False, error="http_504", transient=True)
    range_query = AsyncMock(return_value=range_fetch)
    proof = AsyncMock(return_value={"second": "hash-b", "first": "hash-a"})
    count = AsyncMock(
        return_value=directory.LastUpdatedCountFetch(
            directory.CountObservation.exact(3 if outcome == "count_changed" else 2)
        )
    )

    async def direct_read(_source, url, _prior, **_options):
        resource_id = url.rsplit("/", 1)[-1]
        if outcome == "throttled":
            return (429, None, None, 20), False
        resource_by_field = {
            "id": "unknown" if outcome == "wrong_id" else resource_id,
            "resourceType": "Practitioner",
            "meta": {"lastUpdated": ("2030-01-01T00:00:00Z" if outcome == "wrong_stamp" else start.isoformat())},
        }
        return (200, resource_by_field, None, 20), False

    fallback_fetch = AsyncMock(side_effect=direct_read)
    monkeypatch.setattr(directory, "_fetch_last_updated_partition_window", range_query)
    monkeypatch.setattr(directory, "_load_last_updated_partition_window_fingerprints", proof)
    monkeypatch.setattr(directory, "_fetch_last_updated_partition_count", count)
    monkeypatch.setattr(directory, "_fetch_cooldown_checkpoint_page", fallback_fetch)
    main_url, window_fetch = await directory._fetch_partition_window_for_pass(
        carrier_source, "Practitioner", SimpleNamespace(page_count=500), state, options, window
    )
    assert "_sort=" in main_url
    proof.assert_awaited_once_with(state.context, "Practitioner", "root", 1)
    assert "_summary=count" in count.await_args.args[1]
    assert "_total=accurate" in count.await_args.args[1]
    assert window.passes == {1: None}
    if outcome == "success":
        assert window_fetch.complete and window_fetch.pages_fetched == 4
        assert [resource["id"] for resource in window_fetch.resources] == ["first", "second"]
    else:
        assert not window_fetch.complete and window_fetch.resources == ()
        assert fallback_fetch.await_count == (0 if outcome == "count_changed" else 1)
        assert window_fetch.transient is (outcome == "throttled")


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
                **({"type": "searchset"} if offset == 0 else {}),
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


@pytest.mark.asyncio
async def test_import_uses_the_shared_verified_bounded_session(monkeypatch, carrier_source):
    from process import provider_directory_generic_source_artifacts as artifacts

    assert directory.source_selection.requires_verified_session(directory, carrier_source)
    session_options = []

    @asynccontextmanager
    async def session_scope(**import_options_by_name):
        session_options.append(import_options_by_name)
        yield

    acquire = AsyncMock(return_value={"complete": True})
    monkeypatch.setattr(directory, "_source_http_session_scope", session_scope)
    monkeypatch.setattr(directory, "_import_resources", acquire)
    import_options_by_name = {"resource_scan_concurrency": 8, "source_concurrency": 1}
    outcome = await artifacts.import_resources_with_source_session(
        directory, [carrier_source], None, False, import_options_by_name
    )
    assert outcome == {"complete": True}
    assert session_options == [
        {"force": True, "require_verified_tls": True, "request_concurrency": 8, "source_concurrency": 1}
    ]
    acquire.assert_awaited_once_with([carrier_source], **import_options_by_name)


@pytest.mark.asyncio
async def test_non_search_cached_continuation_cannot_complete_acquisition(monkeypatch, carrier_source):
    requests = []

    async def fetch(_source, url, **_kwargs):
        requests.append(url)
        if len(requests) == 1:
            return (
                200,
                {
                    "resourceType": "Bundle",
                    "type": "searchset",
                    "entry": [
                        {"resource": {"resourceType": "Location", "id": f"location-{number}"}} for number in range(50)
                    ],
                    "link": [
                        {"relation": "next", "url": BASE + "?_getpages=example-cache-H&_getpagesoffset=50&_count=50"}
                    ],
                },
                None,
                1,
            )
        return 200, {"resourceType": "Bundle", "type": "collection", "entry": []}, None, 1

    monkeypatch.setattr(directory, "_fetch_source_json", fetch)
    fetch_result = await directory._fetch_resource_rows(
        carrier_source, "Location", per_resource_limit=0, page_limit=0, page_count=500, timeout=3, run_id=None
    )
    assert not fetch_result.complete and fetch_result.error == "minnesota_cache_non_searchset_bundle"
    assert fetch_result.rows_fetched == 50 and len(requests) == 2


@pytest.mark.parametrize("total", [200, True, -1])
def test_initial_short_page_cannot_hide_a_larger_total(total):
    bundle_payload_by_field = {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": total,
        "entry": [{"resource": {"resourceType": "Location", "id": f"location-{number}"}} for number in range(20)],
    }
    assert directory.bcbs_minnesota.page_error(bundle_payload_by_field, "Location", BASE + "/Location?_count=0")


@pytest.mark.parametrize(
    ("bundle_type", "total", "expected_error"),
    [
        ("collection", None, "minnesota_cache_non_searchset_bundle"),
        ("history", 50, "minnesota_cache_non_searchset_bundle"),
        (None, None, "minnesota_cache_ambiguous_empty_page"),
        (None, 50, None),
        ("searchset", None, None),
    ],
)
def test_empty_cached_pages_require_search_semantics_or_exact_terminal_accounting(bundle_type, total, expected_error):
    bundle_payload_by_field = {"resourceType": "Bundle", "type": bundle_type, "total": total, "entry": []}
    assert (
        directory.bcbs_minnesota.page_error(
            bundle_payload_by_field, "Location", BASE + "?_getpages=example-cache-H&_getpagesoffset=50&_count=500"
        )
        == expected_error
    )
