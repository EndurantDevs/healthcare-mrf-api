# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Capped collection acquisition must cover and reconcile every time chunk."""

import asyncio
import copy
import dataclasses
import json
import urllib.parse

import pytest

from process import provider_directory_capped_partition as capped
from tests.test_process_provider_directory_fhir_unit import (
    _last_updated_partition_test_bundle as bundle,
)
from tests.test_process_provider_directory_fhir_unit import (
    _last_updated_partition_test_resource as resource,
)
from tests.test_process_provider_directory_fhir_unit import (
    _last_updated_partition_test_source as source,
)
from tests.test_process_provider_directory_fhir_unit import (
    _LastUpdatedPartitionFetchHarness,
    _PartitionResumeRecorder,
    importer,
    unit_source_factory,
)
from tests.test_process_provider_directory_fhir_unit import (
    _run_last_updated_partition_test_fetch as run_fetch,
)


def overflow(limit=4):
    return {
        "resourceType": "OperationOutcome",
        "issue": [
            {
                "severity": "error",
                "code": "too-costly",
                "diagnostics": "<HSFHIRErr>SearchTooCostly",
                "details": {"text": f"Search selects more than maximum allowed number of results ({limit})."},
            }
        ],
    }


def capped_source():
    return source(ceiling=2, versioned_search_limit=4, page_count=1, volatile_metadata_paths=[])


class Collection:
    def __init__(self, *, root_timeout=False):
        self.resources = [
            resource(f"record-{n}", last_updated=f"2024-01-0{day}T{hour:02}:00:00Z")
            for n, (day, hour) in enumerate(((1, 1), (1, 7), (1, 13), (2, 1), (2, 7), (2, 13)))
        ]
        self.resources[0].pop("name")
        self.resources[1]["name"] = [{"family": "123"}]
        self.resources[2]["name"] = [{"family": "赵"}]
        self.requests = []
        self.cursors = {}
        self.root_timeout = root_timeout
        self.versioning = "versioned"
        self.tail_count = 0
        self.post_drift = False
        self.count_calls = {}
        self.metadata_reads = 0

    async def fetch(self, _source, url, *, timeout):
        self.requests.append(url)
        if urllib.parse.urlsplit(url).path.endswith("/metadata"):
            self.metadata_reads += 1
            return (
                200,
                {
                    "resourceType": "CapabilityStatement",
                    "fhirVersion": "4.0.1",
                    "rest": [
                        {
                            "mode": "server",
                            "resource": [
                                {
                                    "type": "Practitioner",
                                    "versioning": self.versioning,
                                    "interaction": [{"code": x} for x in ("create", "update", "search-type")],
                                }
                            ],
                        }
                    ],
                },
                None,
                1,
            )
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        bounds = query.get("_lastUpdated", [])
        if len(bounds) == 1:
            return 200, bundle([], total=self.tail_count), None, 1
        if query.get("queryId"):
            selected_resources = self.cursors[query["queryId"][0]]
            return 200, bundle(selected_resources[1:], total=len(selected_resources)), None, 1
        assert len(bounds) == 2
        start = importer.parse_utc_instant(next(x[2:] for x in bounds if x.startswith("ge")))
        end = importer.parse_utc_instant(next(x[2:] for x in bounds if x.startswith("lt")))
        selected_resources = [
            resource_payload
            for resource_payload in self.resources
            if start <= importer.parse_utc_instant(resource_payload["meta"]["lastUpdated"]) < end
        ]
        if query.get("_summary") == ["count"]:
            if len(selected_resources) > 4:
                return (None, None, "TimeoutError: timed out", 1) if self.root_timeout else (413, overflow(), None, 1)
            self.count_calls[tuple(bounds)] = self.count_calls.get(tuple(bounds), 0) + 1
            drift = int(self.post_drift and self.count_calls[tuple(bounds)] > 1)
            return 200, bundle([], total=len(selected_resources) + drift), None, 1
        response_payload = bundle(selected_resources[:1], total=len(selected_resources))
        if len(selected_resources) > 1:
            token = f"cursor+{len(self.cursors)}/="
            self.cursors[token] = selected_resources
            response_payload["link"] = [
                {
                    "relation": "next",
                    "url": _source["api_base"] + "/Practitioner?page=2&queryId=" + urllib.parse.quote(token, safe=""),
                }
            ]
        return 200, response_payload, None, 1


@pytest.mark.parametrize("root_timeout", [False, True])
@pytest.mark.asyncio
async def test_chunks_cover_unnamed_numeric_and_unicode_rows(monkeypatch, unit_source_factory, root_timeout):
    collection = Collection(root_timeout=root_timeout)
    result, staged, _ = await run_fetch(monkeypatch, capped_source(), collection.fetch)
    assert result.complete and result.rows_fetched == result.rows_written == 6
    assert set(staged) == {resource_payload["id"] for resource_payload in collection.resources}
    proof_by_field = result.fetch_diagnostic["capped_search"]
    assert (
        proof_by_field["contract"] == capped.CONTRACT
        and proof_by_field["count_pre"] == proof_by_field["count_post"] == 6
    )
    assert all(proof_by_field[k] == 0 for k in capped.TAIL_FIELDS)
    assert result.fetch_diagnostic["unfiltered_pre"] is None
    assert any("queryId=cursor%2B" in url for url in collection.requests)
    assert all("name=" not in url and "family=" not in url for url in collection.requests)


@pytest.mark.asyncio
async def test_cancelled_first_pass_resumes_without_discarding_proof(monkeypatch, unit_source_factory):
    collection = Collection()
    recorder = _PartitionResumeRecorder()
    fingerprints_by_window_and_pass = {}
    with pytest.raises(asyncio.CancelledError):
        await run_fetch(
            monkeypatch,
            capped_source(),
            collection.fetch,
            save_plan_callback=recorder._save_and_interrupt,
            fingerprints_by_window_and_pass=fingerprints_by_window_and_pass,
        )
    assert recorder.persisted_resume.census.capped["count_pre"] == 6
    retained = copy.deepcopy(fingerprints_by_window_and_pass)
    result, staged, _ = await run_fetch(
        monkeypatch,
        capped_source(),
        collection.fetch,
        partition_resume=recorder.persisted_resume,
        fingerprints_by_window_and_pass=fingerprints_by_window_and_pass,
    )
    assert result.complete and len(staged) == 6
    assert all(fingerprints_by_window_and_pass[k] == value for k, value in retained.items())


@pytest.mark.parametrize(
    "change,error",
    [("tail", "not_exact_zero"), ("version", "versioned_write_contract_missing"), ("drift", "post_census_mismatch")],
)
@pytest.mark.asyncio
async def test_incomplete_universe_or_changed_count_never_completes(monkeypatch, unit_source_factory, change, error):
    collection = Collection()
    if change == "tail":
        collection.tail_count = 1
    if change == "version":
        collection.versioning = "no-version"
    if change == "drift":
        collection.post_drift = True
    result, _staged, written = await run_fetch(monkeypatch, capped_source(), collection.fetch)
    assert not result.complete and error in result.error
    assert written == []


@pytest.mark.parametrize(
    "status,response_payload,error",
    [
        (401, overflow(), None),
        (403, overflow(), None),
        (429, overflow(), None),
        (503, overflow(), None),
        (200, overflow(), None),
        (413, overflow(99), None),
        (413, {"resourceType": "OperationOutcome"}, None),
        (None, None, "TLS certificate failed"),
    ],
)
def test_only_verified_overflows_or_transport_timeouts_split(status, response_payload, error):
    url = source()["api_base"] + "/Practitioner?_summary=count&_lastUpdated=ge2024-01-01&_lastUpdated=lt2024-01-03"
    assert capped.split_observation(importer, capped_source(), url, status, response_payload, error) is None
    assert capped.split_observation(importer, source(), url, 413, overflow(), None) is None


def test_timeout_tail_is_not_a_partition_split_hint():
    url = source()["api_base"] + "/Practitioner?_summary=count&_lastUpdated=lt2024-01-01"
    assert capped.split_observation(importer, capped_source(), url, None, None, "TimeoutError") is None


@pytest.mark.asyncio
async def test_tail_throttle_retains_retry_boundary(monkeypatch, unit_source_factory):
    collection = Collection()

    async def fetch(record, url, *, timeout):
        if "/metadata" not in url:
            return 429, {importer.SOURCE_RETRY_AFTER_FIELD: "60"}, None, 1
        return await collection.fetch(record, url, timeout=timeout)

    result, staged, written = await run_fetch(monkeypatch, capped_source(), fetch)
    assert not result.complete and result.next_url_remaining and result.retry_not_before
    assert staged == {} and written == []


def test_checkpoint_mode_cannot_be_reinterpreted_as_exact_unfiltered_proof():
    config, _ = importer._last_updated_partition_config(capped_source(), "Practitioner")
    plan = importer._new_last_updated_partition_plan(config)
    census = importer.LastUpdatedCompletenessCensus(
        capped={
            "contract": capped.CONTRACT,
            "versioned": True,
            **dict.fromkeys(capped.COUNT_FIELDS),
        }
    )
    response_payload = importer._last_updated_partition_checkpoint_payload(config, plan, census)
    assert (
        importer._last_updated_partition_resume_from_checkpoint(response_payload, config).census.capped == census.capped
    )
    exact, _ = importer._last_updated_partition_config(source(), "Practitioner")
    with pytest.raises(RuntimeError, match="checkpoint_invalid"):
        importer._last_updated_partition_resume_from_checkpoint(response_payload, exact)
    forged = json.loads(response_payload)
    forged["control"]["census"]["unfiltered_pre"] = 6
    with pytest.raises(RuntimeError, match="checkpoint_invalid"):
        importer._last_updated_partition_resume_from_checkpoint(json.dumps(forged), config)


@pytest.mark.parametrize(
    "field,value",
    [
        ("contract", "other"),
        ("versioned", False),
        ("count_pre", True),
        ("count_pre", -1),
        ("count_post", 7),
        ("count_pre", None),
        ("before_start_pre", None),
        ("after_end_post", None),
        ("before_start_post", 1),
        ("count_post", "6"),
    ],
)
def test_census_rejects_forged_or_incomplete_completion(field, value):
    proof_by_field = {
        "contract": capped.CONTRACT,
        "versioned": True,
        **dict.fromkeys(capped.TAIL_FIELDS, 0),
        "count_pre": 6,
        "count_post": 6,
    }
    proof_by_field[field] = value
    with pytest.raises(ValueError):
        capped.complete(proof_by_field)


@pytest.mark.parametrize(
    "endpoint",
    [
        "/Practitioner?name=A",
        "/Practitioner?_lastUpdated=ge2024-01-01",
        "/Practitioner?_count=1&_count=2",
        "/Organization",
        "/Practitioner#fragment",
    ],
)
@pytest.mark.asyncio
async def test_unreviewed_filtered_or_cross_collection_start_fails_before_http(
    monkeypatch, unit_source_factory, endpoint
):
    record = capped_source()
    record["endpoint_practitioner"] = record["api_base"] + endpoint
    collection = Collection()
    result, staged, written = await run_fetch(monkeypatch, record, collection.fetch)
    assert not result.complete and "unfiltered_anonymous_contract_required" in result.error
    assert collection.requests == [] and staged == {} and written == []


@pytest.mark.asyncio
async def test_nonempty_final_tail_retains_candidate_without_completion(monkeypatch, unit_source_factory):
    collection = Collection()

    async def fetch(record, url, *, timeout):
        if "/metadata" in url:
            if collection.metadata_reads == 1:
                collection.tail_count = 1
        return await collection.fetch(record, url, timeout=timeout)

    result, staged, written = await run_fetch(monkeypatch, capped_source(), fetch)
    assert not result.complete and "not_exact_zero" in result.error
    assert len(staged) == 6 and written == []
    assert "_lastUpdated=lt2024" in collection.requests[-1]


def test_tls_error_text_is_not_a_timeout_split_hint():
    url = source()["api_base"] + "/Practitioner?_summary=count&_lastUpdated=ge2024-01-01&_lastUpdated=lt2024-01-03"
    assert capped.split_observation(importer, capped_source(), url, None, None, "SSLError: timeout") is None


@pytest.mark.parametrize("limit", [True, 0, -1, 2, "50000"])
def test_capped_limit_requires_integer_headroom(limit):
    with pytest.raises(ValueError, match="must_exceed"):
        capped.search_limit({"versioned_search_limit": limit}, 2)


@pytest.mark.parametrize("phase", ["before-acquisition", "after-twin-passes"])
@pytest.mark.parametrize("status", [401, 503])
@pytest.mark.asyncio
async def test_metadata_outage_never_promotes_previously_staged_rows(monkeypatch, unit_source_factory, phase, status):
    collection = Collection()

    async def fetch(record, url, *, timeout):
        if url.endswith("/metadata") and (phase == "before-acquisition" or collection.metadata_reads == 1):
            return status, {importer.SOURCE_RETRY_AFTER_FIELD: "60"}, None, 1
        return await collection.fetch(record, url, timeout=timeout)

    result, staged, written = await run_fetch(monkeypatch, capped_source(), fetch)
    assert not result.complete and "versioning_metadata_unavailable" in result.error
    assert len(staged) == (0 if phase == "before-acquisition" else 6)
    assert written == []
    assert result.next_url_remaining is (status == 503)
    assert bool(result.retry_not_before) is (status == 503)


@pytest.mark.parametrize("phase", ["metadata", "tails", "post-census"])
@pytest.mark.asyncio
async def test_deadline_retains_proof_and_resumes_without_restarting_chunks(monkeypatch, unit_source_factory, phase):
    collection = Collection()
    monotonic_values = [0.0]
    persisted_resumes = []
    fingerprints_by_window_and_pass = {}
    directory_source = capped_source()
    config, _ = importer._last_updated_partition_config(directory_source, "Practitioner")
    harness = _LastUpdatedPartitionFetchHarness(directory_source, fingerprints_by_window_and_pass)
    prepare = importer._prepare_partition_state
    monkeypatch.setattr(importer, "_last_updated_partition_monotonic", lambda: monotonic_values[0])

    async def prepared(*args, **kwargs):
        state = await prepare(*args, **kwargs)
        if phase == "metadata":
            monotonic_values[0] = 2.0
        return state

    async def fetch(record, url, *, timeout):
        response = await collection.fetch(record, url, timeout=timeout)
        if phase == "tails" and url.endswith("/metadata"):
            monotonic_values[0] = 2.0
        return response

    async def save(_context, _resource_type, config, plan, **fields):
        resume = importer._last_updated_partition_resume_from_checkpoint(
            importer._last_updated_partition_checkpoint_payload(config, plan, fields["census"]), config
        )
        resume = dataclasses.replace(
            resume, pages_processed=fields["pages_processed"], rows_processed=fields["rows_processed"]
        )
        persisted_resumes.append(resume)
        if phase == "post-census" and plan.status is importer.PlanStatus.SUCCEEDED:
            monotonic_values[0] = 2.0

    monkeypatch.setattr(importer, "_prepare_partition_state", prepared)
    initial = importer.LastUpdatedPartitionResume(importer._new_last_updated_partition_plan(config))
    harness._install_importer_mocks(monkeypatch, fetch, initial, save)
    fetch_result = await harness._fetch_resource_rows(config, deadline_seconds=1)
    assert fetch_result.deadline_reached and fetch_result.next_url_remaining and not fetch_result.complete
    assert harness.written_resource_ids == [] and persisted_resumes
    retained = copy.deepcopy(fingerprints_by_window_and_pass)
    retained_rows = set(harness.staged_resources_by_id)
    assert len(retained_rows) == (6 if phase == "post-census" else 0)
    assert not importer._is_last_updated_partition_census_complete(persisted_resumes[-1].census)
    monotonic_values[0] = 0.0
    monkeypatch.setattr(importer, "_prepare_partition_state", prepare)
    harness._install_importer_mocks(monkeypatch, collection.fetch, persisted_resumes[-1], None)
    fetch_result = await harness._fetch_resource_rows(config)
    assert fetch_result.complete and fetch_result.rows_fetched == fetch_result.rows_written == 6
    assert all(fingerprints_by_window_and_pass[key] == fingerprint for key, fingerprint in retained.items())
    assert len(harness.staged_resources_by_id) == 6
    assert harness.written_resource_ids == []


@pytest.mark.asyncio
async def test_resumed_leaf_sum_cannot_replace_the_frozen_census(monkeypatch, unit_source_factory):
    collection = Collection()
    recorder = _PartitionResumeRecorder()
    fingerprints_by_window_and_pass = {}
    with pytest.raises(asyncio.CancelledError):
        await run_fetch(
            monkeypatch,
            capped_source(),
            collection.fetch,
            save_plan_callback=recorder._save_and_interrupt,
            fingerprints_by_window_and_pass=fingerprints_by_window_and_pass,
        )
    recorder.persisted_resume.census.capped["count_pre"] += 1
    fetch_result, _staged, written = await run_fetch(
        monkeypatch,
        capped_source(),
        collection.fetch,
        partition_resume=recorder.persisted_resume,
        fingerprints_by_window_and_pass=fingerprints_by_window_and_pass,
    )
    assert not fetch_result.complete and "capped_leaf_census_changed" in fetch_result.error
    assert written == []


@pytest.mark.asyncio
async def test_statistics_require_the_complete_capped_proof_without_synthetic_root_totals(
    monkeypatch, unit_source_factory
):
    fetch_result, _staged, _written = await run_fetch(monkeypatch, capped_source(), Collection().fetch)
    stats = importer._empty_resource_stats()
    importer._record_last_updated_partition_stats(stats, fetch_result)
    assert stats["last_updated_capped_sources"] == stats["last_updated_capped_verified_sources"] == 1
    assert stats["last_updated_capped_versioned_sources"] == stats["last_updated_completeness_verified_sources"] == 1
    assert stats["last_updated_capped_count_pre"] == stats["last_updated_capped_count_post"] == 6
    assert all(stats[f"last_updated_capped_{field}"] == 0 for field in capped.TAIL_FIELDS)
    assert stats["last_updated_unfiltered_pre"] == stats["last_updated_unfiltered_post"] == 0
    assert stats["last_updated_ranged_root_pre"] == stats["last_updated_ranged_root_post"] == 0
    assert stats["last_updated_staged_candidate_count"] == 6
    incomplete = copy.deepcopy(fetch_result)
    incomplete.fetch_diagnostic["capped_search"]["count_post"] = None
    incomplete.fetch_diagnostic["capped_search"]["after_end_post"] = None
    stats = importer._empty_resource_stats()
    importer._record_last_updated_partition_stats(stats, incomplete)
    assert stats["last_updated_capped_sources"] == 1
    assert stats["last_updated_completeness_verified_sources"] == stats["last_updated_staged_candidate_count"] == 0
    assert "last_updated_capped_verified_sources" not in stats


@pytest.mark.parametrize("proof", [None, [], {}, {"contract": capped.CONTRACT}])
def test_checkpoint_requires_the_closed_capped_proof_schema(proof):
    config, _ = importer._last_updated_partition_config(capped_source(), "Practitioner")
    plan = importer._new_last_updated_partition_plan(config)
    census = importer.LastUpdatedCompletenessCensus(capped=proof)
    checkpoint = json.loads(importer._last_updated_partition_checkpoint_payload(config, plan, census))
    if proof is None:
        checkpoint["control"]["census"].update(unfiltered_pre=6, ranged_root_pre=6)
    with pytest.raises(RuntimeError, match="checkpoint_invalid"):
        importer._last_updated_partition_resume_from_checkpoint(json.dumps(checkpoint), config)


@pytest.mark.parametrize("details", [None, [], "SearchTooCostly"])
def test_malformed_overflow_details_cannot_authorize_an_uncounted_split(details):
    outcome = overflow()
    outcome["issue"][0]["details"] = details
    url = source()["api_base"] + "/Practitioner?_summary=count&_lastUpdated=ge2024-01-01&_lastUpdated=lt2024-01-03"
    assert capped.split_observation(importer, capped_source(), url, 413, outcome, None) is None
