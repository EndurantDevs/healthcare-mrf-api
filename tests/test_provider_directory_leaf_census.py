# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""An exact leaf census must reconcile the whole endpoint without a root count."""

import asyncio
import copy
import datetime
import json
import types
import urllib.parse
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_leaf_census as leaf
from scripts.research import provider_directory_endpoint_acquisition_reporting as reporting
from scripts.research import provider_directory_endpoint_acquisition_support as support
from tests.test_provider_directory_capped_partition import (
    Collection,
    _LastUpdatedPartitionFetchHarness,
    _PartitionResumeRecorder,
    bundle,
    importer,
    resource,
    run_fetch,
    source,
    unit_source_factory,
)


def leaf_source(**settings):
    return source(
        **{"census_mode": leaf.CONTRACT, "ceiling": 2, "page_count": 1, "volatile_metadata_paths": [], **settings}
    )


@pytest.fixture
def census_clock(monkeypatch):
    clock_by_field = {"now": datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)}

    class Clock(datetime.datetime):
        @classmethod
        def now(cls, _zone):
            return clock_by_field["now"]

    clock_module = types.SimpleNamespace(**vars(datetime))
    clock_module.datetime = Clock
    monkeypatch.setattr(importer, "datetime", clock_module)
    monkeypatch.setattr(leaf, "datetime", clock_module)
    return clock_by_field


def retry_source(mode):
    source_by_field = leaf_source() if mode == "leaf" else source()
    if mode == "opaque":
        source_by_field["metadata_json"]["provider_directory_opaque_cursor_census"] = True
    return source_by_field


class LeafCollection(Collection):
    def __init__(self):
        super().__init__()
        self.unfiltered_reads = 0
        self.unfiltered_drift = False
        self.failure = None

    async def fetch(self, source_record, url, *, timeout):
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        assert not url.endswith("/metadata")
        if query.get("_summary") == ["count"] and not query.get("_lastUpdated"):
            self.requests.append(url)
            self.unfiltered_reads += 1
            drift = int(self.unfiltered_drift and self.unfiltered_reads > 1)
            return 200, bundle([], total=len(self.resources) + drift), None, 1
        if query.get("_summary") == ["count"]:
            assert query["_lastUpdated"] != ["ge2024-01-01T00:00:00Z", "lt2024-01-03T00:00:00Z"]
            if self.failure is not None:
                self.requests.append(url)
                failure, self.failure = self.failure, None
                return failure
        return await super().fetch(source_record, url, timeout=timeout)


@pytest.mark.asyncio
async def test_exact_unfiltered_leaf_and_native_counts_reconcile(monkeypatch, unit_source_factory):
    collection = LeafCollection()
    outcome, staged, written = await run_fetch(monkeypatch, leaf_source(), collection.fetch)
    assert (
        outcome.complete
        and outcome.rows_written == 6
        and set(staged) == {record["id"] for record in collection.resources}
    )
    proof_by_field = outcome.fetch_diagnostic
    assert proof_by_field["verified"] is True
    assert proof_by_field["ranged_root_pre"] is proof_by_field["ranged_root_post"] is None
    assert all(
        proof_by_field[name] == 6
        for name in (
            "unfiltered_pre",
            "unfiltered_post",
            "exact_leaf_count_sum",
            "pass1_unique",
            "pass2_unique",
            "staged_candidate_count",
        )
    )
    assert proof_by_field["leaf_sum_census"] == {
        "contract": leaf.CONTRACT,
        "count_pre": 6,
        "count_post": 6,
        "refinements": {},
    }
    assert collection.unfiltered_reads == 2


@pytest.mark.parametrize("retry_after", ["3600", "Wed, 01 Jan 2025 01:00:00 GMT"])
@pytest.mark.asyncio
async def test_503_saves_boundary_before_any_planned_refinement(
    monkeypatch, unit_source_factory, census_clock, retry_after
):
    collection, recorder = LeafCollection(), _PartitionResumeRecorder()
    collection.failure = (503, {importer.SOURCE_RETRY_AFTER_FIELD: retry_after}, None, 180_000)
    boundary = "2025-01-01T01:00:00Z"
    outcome, staged, _ = await run_fetch(
        monkeypatch, leaf_source(), collection.fetch, save_plan_callback=recorder._save_and_interrupt
    )
    assert not outcome.complete and outcome.retry_not_before == boundary and not staged
    resume = recorder.persisted_resume
    window = resume.plan.windows["root.0"]
    assert window.count is None and window.split_reason is None and "root.0.0" not in resume.plan.windows
    evidence = copy.deepcopy(resume.census.leaf_sum["refinements"]["root.0"])
    assert evidence["error"] == "http_503" and evidence["retry_not_before"] == boundary
    requested_urls = list(collection.requests)
    census_clock["now"] += datetime.timedelta(seconds=3599)
    outcome, _, _ = await run_fetch(monkeypatch, leaf_source(), collection.fetch, partition_resume=resume)
    assert not outcome.complete and outcome.retry_not_before == boundary and collection.requests == requested_urls
    assert resume.plan.windows["root.0"].split_reason is None and "root.0.0" not in resume.plan.windows
    census_clock["now"] += datetime.timedelta(seconds=1)
    outcome, staged, _ = await run_fetch(monkeypatch, leaf_source(), collection.fetch, partition_resume=resume)
    assert outcome.complete and len(staged) == 6
    assert resume.plan.windows["root.0"].split_reason == "planned"
    assert outcome.fetch_diagnostic["leaf_sum_census"]["refinements"]["root.0"] == evidence
    assert collection.requests.count(requested_urls[-1]) == 1


@pytest.mark.parametrize(
    "change,reason", [("post_drift", "post_census_mismatch"), ("unfiltered_drift", "unfiltered_post_mismatch")]
)
@pytest.mark.asyncio
async def test_drift_keeps_staged_rows_without_completing(monkeypatch, unit_source_factory, change, reason):
    collection = LeafCollection()
    setattr(collection, change, True)
    outcome, staged, written = await run_fetch(monkeypatch, leaf_source(), collection.fetch)
    assert not outcome.complete and reason in outcome.error and len(staged) == 6 and written == []


@pytest.mark.asyncio
async def test_outside_rows_cannot_hide_in_leaf_sum(monkeypatch, unit_source_factory):
    collection = LeafCollection()
    collection.resources.append(resource("outside", last_updated="2023-12-31T23:59:59Z"))
    outcome, staged, written = await run_fetch(monkeypatch, leaf_source(), collection.fetch)
    assert not outcome.complete and "leaf_census_pre_sum_mismatch" in outcome.error and not staged and not written


@pytest.mark.parametrize(
    "failure",
    [
        (401, None, None, 1),
        (403, None, None, 1),
        (200, {"resourceType": "OperationOutcome"}, None, 1),
        (200, {"resourceType": "Bundle", "type": "searchset", "total": True}, None, 1),
    ],
)
@pytest.mark.asyncio
async def test_auth_and_invalid_counts_are_terminal(monkeypatch, unit_source_factory, failure):
    collection = LeafCollection()
    collection.failure = failure
    monkeypatch.setattr(importer, "LAST_UPDATED_PARTITION_COUNT_ENVELOPE_RETRY_DELAYS_SECONDS", ())
    outcome, staged, _ = await run_fetch(monkeypatch, leaf_source(), collection.fetch)
    assert not outcome.complete and not outcome.next_url_remaining and not staged
    assert outcome.fetch_diagnostic["leaf_sum_census"]["refinements"] == {}


@pytest.mark.asyncio
async def test_existing_native_proof_remains_a_required_gate(monkeypatch, unit_source_factory):
    original = _LastUpdatedPartitionFetchHarness._proof_counts

    async def bad_native_count(*args, **kwargs):
        proof = await original(*args, **kwargs)
        return importer.replace(proof, pass2_unique=proof.pass2_unique - 1)

    monkeypatch.setattr(_LastUpdatedPartitionFetchHarness, "_proof_counts", bad_native_count)
    outcome, staged, written = await run_fetch(monkeypatch, leaf_source(), LeafCollection().fetch)
    assert not outcome.complete and len(staged) == 6 and not written
    assert "completeness_proof_mismatch" in outcome.error


@pytest.mark.asyncio
async def test_first_pass_resume_preserves_frozen_census(monkeypatch, unit_source_factory):
    collection, recorder = LeafCollection(), _PartitionResumeRecorder()
    fingerprints_by_window = {}
    with pytest.raises(asyncio.CancelledError):
        await run_fetch(
            monkeypatch,
            leaf_source(),
            collection.fetch,
            save_plan_callback=recorder._save_and_interrupt,
            fingerprints_by_window_and_pass=fingerprints_by_window,
        )
    resume = recorder.persisted_resume
    assert resume.census.leaf_sum["count_pre"] == 6
    retained = copy.deepcopy(fingerprints_by_window)
    outcome, _, _ = await run_fetch(
        monkeypatch,
        leaf_source(),
        collection.fetch,
        partition_resume=resume,
        fingerprints_by_window_and_pass=fingerprints_by_window,
    )
    assert outcome.complete and all(fingerprints_by_window[key] == value for key, value in retained.items())
    assert collection.unfiltered_reads == 2


@pytest.mark.parametrize("change", ["mode", "mixed", "contract", "predecessor", "sum", "refinement", "missing"])
@pytest.mark.asyncio
async def test_checkpoint_rejects_mixed_or_forged_evidence(monkeypatch, unit_source_factory, change):
    recorder = _PartitionResumeRecorder()
    with pytest.raises(asyncio.CancelledError):
        await run_fetch(
            monkeypatch, leaf_source(), LeafCollection().fetch, save_plan_callback=recorder._save_and_interrupt
        )
    config, _ = importer._last_updated_partition_config(leaf_source(), "Practitioner")
    checkpoint = json.loads(
        importer._last_updated_partition_checkpoint_payload(
            config, recorder.persisted_resume.plan, recorder.persisted_resume.census
        )
    )
    census_by_field = checkpoint["control"]["census"]
    match change:
        case "mode":
            config = importer.replace(config, census_mode=None)
        case "mixed":
            census_by_field["ranged_root_pre"] = 6
        case "contract":
            census_by_field["leaf_sum"]["contract"] = "unknown"
        case "predecessor":
            census_by_field["unfiltered_pre"] = None
        case "sum":
            census_by_field["leaf_sum"]["count_pre"] = 7
        case "refinement":
            census_by_field["leaf_sum"]["refinements"]["unknown"] = {}
        case "missing":
            del census_by_field["leaf_sum"]
    with pytest.raises(RuntimeError, match="checkpoint_invalid"):
        importer._last_updated_partition_resume_from_checkpoint(json.dumps(checkpoint), config)


@pytest.mark.parametrize(
    "settings",
    [{"census_mode": "unknown"}, {"census_mode": True}, {"census_mode": leaf.CONTRACT, "versioned_search_limit": 20}],
)
def test_mode_is_explicit_and_cannot_mix_capped_contract(settings):
    config, error = importer._last_updated_partition_config(source(**settings), "Practitioner")
    assert config is None and "census_mode_invalid" in error


@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.asyncio
async def test_reporting_requires_truthful_provenance_even_at_zero(monkeypatch, unit_source_factory, empty):
    collection = LeafCollection()
    if empty:
        collection.resources.clear()
    outcome, _, _ = await run_fetch(monkeypatch, leaf_source(), collection.fetch)
    stats = importer._empty_resource_stats()
    importer._record_last_updated_partition_stats(stats, outcome)
    retained = reporting._safe_resource_outcomes({"resource_fetch_stats": {"Practitioner": stats}})["Practitioner"]
    entry_by_field = {"source_ids": ["synthetic"], "leaf_sum_last_updated_resources": ["Practitioner"]}
    assert support._last_updated_completeness_errors(entry_by_field, "Practitioner", retained) == []
    assert retained["last_updated_ranged_root_pre"] == retained["last_updated_ranged_root_post"] == 0
    assert (
        retained["last_updated_leaf_sum_count_pre"]
        == retained["last_updated_leaf_sum_count_post"]
        == len(collection.resources)
    )
    del retained["last_updated_leaf_sum_verified_sources"]
    assert support._last_updated_completeness_errors(entry_by_field, "Practitioner", retained)


@pytest.mark.parametrize("invalid_hash", [False, True])
@pytest.mark.asyncio
async def test_resume_stops_at_minimum_width_or_changed_request(monkeypatch, unit_source_factory, invalid_hash):
    collection, recorder = LeafCollection(), _PartitionResumeRecorder()
    collection.failure = (503, None, None, 1)
    directory_source = leaf_source(minimum_width_seconds=86400)
    monkeypatch.setattr(
        importer, "_last_updated_partition_retry_not_before", lambda *args, **kwargs: "2025-01-01T00:00:00Z"
    )
    await run_fetch(monkeypatch, directory_source, collection.fetch, save_plan_callback=recorder._save_and_interrupt)
    resume = recorder.persisted_resume
    if invalid_hash:
        resume.census.leaf_sum["refinements"]["root.0"]["request_sha256"] = "0" * 64
    requested_urls = list(collection.requests)
    outcome, staged, _ = await run_fetch(
        monkeypatch,
        directory_source,
        collection.fetch,
        partition_resume=resume,
        save_plan_callback=recorder._save_and_interrupt,
    )
    assert not outcome.complete and not outcome.next_url_remaining and not staged
    assert ("leaf_count_request_changed" if invalid_hash else "minimum_width_planned_split") in outcome.error
    assert collection.requests == requested_urls
    assert recorder.persisted_resume.census.leaf_sum["refinements"]["root.0"]["error"] == "http_503"


@pytest.mark.parametrize("query", ["active=true", "_lastUpdated=ge2020-01-01T00:00:00Z", "_id=single"])
@pytest.mark.asyncio
async def test_filtered_collection_cannot_claim_an_unfiltered_census(monkeypatch, unit_source_factory, query):
    collection = LeafCollection()
    directory_source = leaf_source()
    directory_source["endpoint_practitioner"] = directory_source["api_base"] + "/Practitioner?" + query
    outcome, _, _ = await run_fetch(monkeypatch, directory_source, collection.fetch)
    assert not outcome.complete and "leaf_unfiltered_contract_required" in outcome.error
    assert collection.requests == []


def test_organization_reports_projected_candidates_separately():
    stats_by_field = {name: 6 for name in support.LEAF_SUM_LAST_UPDATED_COUNT_METRIC_NAMES}
    stats_by_field.update({name: 1 for name in support.LEAF_SUM_LAST_UPDATED_SOURCE_METRIC_NAMES})
    stats_by_field.update({name: 0 for name in support.LEAF_SUM_LAST_UPDATED_ZERO_METRIC_NAMES})
    stats_by_field["last_updated_staged_candidate_count"] = 4
    entry_by_field = {"source_ids": ["synthetic"], "leaf_sum_last_updated_resources": ["Organization", "Practitioner"]}
    assert support._last_updated_completeness_errors(entry_by_field, "Organization", stats_by_field) == []
    assert support._last_updated_completeness_errors(entry_by_field, "Practitioner", stats_by_field)
    assert (
        stats_by_field["last_updated_staged_candidate_count"] == 4 and stats_by_field["last_updated_pass2_unique"] == 6
    )


@pytest.mark.asyncio
async def test_frozen_count_and_mode_identity_survive_before_acquisition(monkeypatch, unit_source_factory):
    recorder = _PartitionResumeRecorder()
    with pytest.raises(asyncio.CancelledError):
        await run_fetch(
            monkeypatch, leaf_source(), LeafCollection().fetch, save_plan_callback=recorder._save_and_interrupt
        )
    census = recorder.persisted_resume.census
    census.leaf_sum["count_pre"] += 1
    outcome, _, _ = await run_fetch(
        monkeypatch, leaf_source(), LeafCollection().fetch, partition_resume=recorder.persisted_resume
    )
    assert not outcome.complete and "leaf_census_pre_sum_mismatch" in outcome.error
    ordinary, _ = importer._last_updated_partition_config(source(), "Practitioner")
    selected, _ = importer._last_updated_partition_config(leaf_source(), "Practitioner")
    assert "census_mode" not in ordinary.identity() and selected.identity()["census_mode"] == leaf.CONTRACT
    assert importer._last_updated_partition_start_hash(
        "Practitioner", ordinary
    ) != importer._last_updated_partition_start_hash("Practitioner", selected)


@pytest.mark.parametrize(
    "failure", [(None, None, "TimeoutError: timed out", 1), (429, None, None, 1), (502, None, None, 1)]
)
@pytest.mark.asyncio
async def test_other_transients_retry_the_same_uncounted_window(monkeypatch, unit_source_factory, failure):
    collection, recorder = LeafCollection(), _PartitionResumeRecorder()
    collection.failure = failure
    outcome, _, _ = await run_fetch(
        monkeypatch, leaf_source(), collection.fetch, save_plan_callback=recorder._save_and_interrupt
    )
    assert not outcome.complete and outcome.next_url_remaining and outcome.retry_not_before is not None
    assert recorder.persisted_resume.plan.windows["root.0"].split_reason is None
    assert recorder.persisted_resume.census.leaf_sum["refinements"] == {}


@pytest.mark.parametrize("reason", ["http_503", ["planned"], {"reason": "planned"}])
def test_planner_never_accepts_http_failures_as_split_observations(reason):
    with pytest.raises(importer.PartitionPlanError):
        importer.CountObservation.split(reason)


@pytest.mark.parametrize(
    "opaque_flag,enabled,census_mode,expected",
    [
        (True, False, None, None),
        (False, True, leaf.CONTRACT, None),
        (False, False, leaf.CONTRACT, 600.0),
        (False, "true", leaf.CONTRACT, 600.0),
        (False, True, "unknown", 600.0),
        (False, True, None, 600.0),
        ("true", False, None, 600.0),
        (1, False, None, 600.0),
    ],
)
def test_full_retry_after_requires_an_explicit_census(opaque_flag, enabled, census_mode, expected):
    source_by_field = {
        "api_base": "https://directory.example.test/fhir",
        "metadata_json": {
            "provider_directory_opaque_cursor_census": opaque_flag,
            importer.LAST_UPDATED_PARTITION_METADATA_KEY: {
                "enabled": enabled,
                "resources": {"Practitioner": {"census_mode": census_mode}},
            },
        },
    }
    assert importer.source_selection.retry_after_limit(importer, source_by_field) == expected


@pytest.mark.parametrize("retry_after", ["3600", "Wed, 01 Jan 2025 01:00:00 GMT"])
@pytest.mark.parametrize("mode", ["leaf", "opaque", "legacy"])
@pytest.mark.parametrize(
    "fetch_name",
    ["_fetch_last_updated_partition_count", "_fetch_caresource_census_count", "_fetch_current_version_census_count"],
)
@pytest.mark.asyncio
async def test_census_count_and_page_retry_boundaries_retain_source_policy(
    monkeypatch, census_clock, retry_after, mode, fetch_name
):
    source_by_field = retry_source(mode)
    request_url = source_by_field["api_base"] + "/Practitioner?_summary=count"
    response_by_field = {importer.SOURCE_RETRY_AFTER_FIELD: retry_after}
    fetch = AsyncMock(return_value=(503, response_by_field, None, 1))
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    count_result = await getattr(importer, fetch_name)(source_by_field, request_url, timeout=1)
    seconds = 600 if mode == "legacy" and fetch_name != "_fetch_current_version_census_count" else 3600
    expected_boundary = (census_clock["now"] + datetime.timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
    assert count_result.transient and count_result.retry_not_before == expected_boundary
    fetch.assert_awaited_once()
    page_result = importer._last_updated_window_page_failure(source_by_field, request_url, 503, response_by_field, None)
    seconds = 600 if mode == "legacy" else 3600
    expected_boundary = (census_clock["now"] + datetime.timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")
    assert page_result.transient and page_result.retry_not_before == expected_boundary


@pytest.mark.parametrize("retry_after", ["3600", "Wed, 01 Jan 2025 01:00:00 GMT"])
@pytest.mark.parametrize("mode", ["leaf", "opaque", "legacy", "current-version"])
@pytest.mark.asyncio
async def test_source_pacing_retains_full_census_delay_without_sleeping(monkeypatch, census_clock, retry_after, mode):
    source_by_field = retry_source("opaque" if mode == "current-version" else mode)
    if mode == "current-version":
        monkeypatch.setattr(importer, "current_version_census_contract", lambda _source: object())
    source_by_field["_provider_directory_request_pacing_state"] = {}
    source_by_field["metadata_json"]["provider_directory_request_interval_seconds"] = 0
    initial_time = census_clock["now"]
    unpaced = AsyncMock(
        side_effect=[(503, {importer.SOURCE_RETRY_AFTER_FIELD: retry_after}, None, 1), (200, {}, None, 1)]
    )
    waits = []

    async def advance_sleep(seconds):
        if seconds:
            assert unpaced.await_count == 1
            waits.append(seconds)
            census_clock["now"] += datetime.timedelta(seconds=seconds)

    monkeypatch.setattr(importer.time, "monotonic", lambda: (census_clock["now"] - initial_time).total_seconds())
    monkeypatch.setattr(importer.asyncio, "sleep", advance_sleep)
    monkeypatch.setattr(importer, "_fetch_source_json_unpaced", unpaced)
    request_url = source_by_field["api_base"] + "/Practitioner"
    async with importer._source_http_session_scope(force=True):
        await importer._fetch_source_json_once(source_by_field, request_url, timeout=1)
        second_result = await importer._fetch_source_json_once(source_by_field, request_url, timeout=1)
        if mode in {"leaf", "opaque"}:
            assert second_result == (None, None, importer.source_selection.CENSUS_REQUEST_DEFERRED_ERROR, 0)
            unpaced.assert_awaited_once()
            census_clock["now"] += datetime.timedelta(seconds=3600)
            assert (await importer._fetch_source_json_once(source_by_field, request_url, timeout=1))[0] == 200
    assert unpaced.await_count == 2 and waits == ([600] if mode == "legacy" else [])
