# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A gateway-limited leaf reuses exact counting and complete twin-pass proof."""

import asyncio
import dataclasses
import urllib.parse

import pytest

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
    _run_last_updated_partition_test_fetch as run_fetch,
)
from tests.test_process_provider_directory_fhir_unit import (
    importer,
    unit_source_factory,
)


def directory_source(**overrides):
    record = source(volatile_metadata_paths=[], **overrides)
    record.update(api_base=importer.bcbs_minnesota.API_BASE, canonical_api_base=importer.bcbs_minnesota.API_BASE)
    return record


class GatewayLimitedCollection:
    def __init__(self, mode="success"):
        self.mode = mode
        self.rows = [
            resource(f"record-{day}-{hour}", last_updated=f"2024-01-{day:02}T{hour:02}:00:00Z")
            for day in (1, 2)
            for hour in (1, 13)
        ]
        self.data_requests = []
        self.direct_requests = []

    def select(self, bounds):
        if not bounds:
            return self.rows
        start = importer.parse_utc_instant(bounds[0][2:])
        end = importer.parse_utc_instant(bounds[1][2:])
        return [row for row in self.rows if start <= importer.parse_utc_instant(row["meta"]["lastUpdated"]) < end]

    async def fetch(self, _source, url, *, timeout):
        request = urllib.parse.urlsplit(url)
        if "/Practitioner/" in request.path:
            assert self.mode in {"second_pass", "second_pass_direct", "second_pass_changed"} and not request.query
            resource_id = request.path.rsplit("/", 1)[-1]
            self.direct_requests.append(resource_id)
            if self.mode == "second_pass":
                return 504, None, None, 1
            resource_by_field = next(candidate for candidate in self.rows if candidate["id"] == resource_id)
            if self.mode == "second_pass_changed":
                resource_by_field = {**resource_by_field, "name": [{"family": "Changed"}]}
            return 200, resource_by_field, None, 1
        query = urllib.parse.parse_qs(request.query)
        bounds = tuple(query.get("_lastUpdated", []))
        selected_resources = self.select(bounds)
        if query.get("_summary") == ["count"]:
            if _source["api_base"] == importer.bcbs_minnesota.API_BASE:
                assert query["_count"] == ["0"]
            else:
                assert "_count" not in query
            drift = int(self.mode == "child_count_mismatch" and len(selected_resources) == 2 and "01T" in bounds[0])
            return 200, bundle([], total=len(selected_resources) + drift), None, 1
        assert query["_count"] == ["10"]
        self.data_requests.append(bounds)
        occurrence = self.data_requests.count(bounds)
        if len(selected_resources) > 2 and (not self.mode.startswith("second_pass") or occurrence == 2):
            return (503 if self.mode == "unavailable" else 504), None, None, 1
        if self.mode == "truncated":
            selected_resources = selected_resources[:1]
        if self.mode == "changed" and occurrence == 2:
            selected_resources = [{**candidate, "name": [{"family": "Changed"}]} for candidate in selected_resources]
        return 200, bundle(selected_resources, total=len(selected_resources)), None, 1


@pytest.mark.asyncio
async def test_data_timeout_splits_and_completes_original_exact_collection(monkeypatch, unit_source_factory):
    collection = GatewayLimitedCollection()
    saved_plans = []

    async def save(_context, _resource, _config, plan, **_kwargs):
        saved_plans.append(plan.to_dict())

    result, staged, written = await run_fetch(
        monkeypatch, directory_source(), collection.fetch, save_plan_callback=save
    )

    assert result.complete and result.rows_fetched == result.rows_written == 4
    assert set(staged) == {row["id"] for row in collection.rows}
    assert written == []
    assert len(collection.data_requests) == 5
    assert collection.data_requests.count(collection.data_requests[0]) == 1
    assert any(plan["windows"][0]["state"] == "split" for plan in saved_plans)
    assert result.fetch_diagnostic["verified"] is True
    assert result.fetch_diagnostic["unfiltered_pre"] == result.fetch_diagnostic["unfiltered_post"] == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mode", "error", "data_calls"),
    [
        ("child_count_mismatch", "split_count_mismatch", 1),
        ("truncated", "count_mismatch", 2),
        ("changed", "twin_pass_mismatch", 3),
    ],
)
async def test_split_preserves_count_and_content_rejection(monkeypatch, unit_source_factory, mode, error, data_calls):
    collection = GatewayLimitedCollection(mode)
    result, _staged, written = await run_fetch(monkeypatch, directory_source(), collection.fetch)

    assert not result.complete and error in result.error
    assert not written and len(collection.data_requests) == data_calls


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["unavailable", "second_pass", "minimum_width", "other_source"])
async def test_unsuitable_timeout_keeps_existing_retry_and_proof(monkeypatch, unit_source_factory, mode):
    record = directory_source(minimum_width_seconds=172800) if mode == "minimum_width" else directory_source()
    if mode == "other_source":
        record = source()
    collection = GatewayLimitedCollection(mode)
    saved_plans = []

    async def save(_context, _resource, _config, plan, **_kwargs):
        saved_plans.append((plan.failure, plan.windows["root"].state.value, set(plan.windows["root"].passes)))

    result, _staged, written = await run_fetch(monkeypatch, record, collection.fetch, save_plan_callback=save)

    assert not result.complete and result.next_url_remaining
    assert "acquisition_retryable:http_" in result.error
    assert not written and saved_plans[-1] == (None, "ready", {1} if mode == "second_pass" else set())
    assert len(collection.data_requests) == (2 if mode == "second_pass" else 1)
    assert len(collection.direct_requests) == (1 if mode == "second_pass" else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["second_pass_direct", "second_pass_changed"])
async def test_small_second_pass_direct_reads_preserve_the_complete_fingerprint(monkeypatch, unit_source_factory, mode):
    collection = GatewayLimitedCollection(mode)
    result, staged, written = await run_fetch(monkeypatch, directory_source(), collection.fetch)

    assert sorted(collection.direct_requests) == sorted(row["id"] for row in collection.rows)
    assert len(collection.data_requests) == 2 and not written
    if mode == "second_pass_direct":
        assert result.complete and result.rows_fetched == result.rows_written == 4
        assert set(staged) == set(collection.direct_requests)
        assert result.fetch_diagnostic["verified"] is True
    else:
        assert not result.complete and "twin_pass_mismatch" in result.error
        assert not staged


@pytest.mark.asyncio
async def test_cancelled_split_resumes_counting_without_refetching_parent(monkeypatch, unit_source_factory):
    collection = GatewayLimitedCollection()
    record = directory_source()
    retained_by_field = {}

    async def interrupt(_context, _resource, config, plan, **kwargs):
        if plan.status is importer.PlanStatus.COUNTING and plan.windows["root"].state.value == "split":
            retained_by_field["resume"] = dataclasses.replace(
                importer._last_updated_partition_resume_from_checkpoint(
                    importer._last_updated_partition_checkpoint_payload(config, plan, kwargs["census"]), config
                ),
                pages_processed=kwargs["pages_processed"],
                rows_processed=kwargs["rows_processed"],
            )
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_fetch(monkeypatch, record, collection.fetch, save_plan_callback=interrupt)
    assert len(collection.data_requests) == 1

    result, staged, written = await run_fetch(
        monkeypatch, record, collection.fetch, partition_resume=retained_by_field["resume"]
    )
    assert result.complete and set(staged) == {row["id"] for row in collection.rows}
    assert written == []
    assert len(collection.data_requests) == 5
    assert collection.data_requests.count(collection.data_requests[0]) == 1
