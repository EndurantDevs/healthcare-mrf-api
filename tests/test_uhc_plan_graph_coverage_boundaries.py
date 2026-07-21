# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib

import pytest


fhir = importlib.import_module("process.provider_directory_fhir")


def _source(api_base: str | None = None) -> dict[str, str]:
    base = api_base or fhir.UHC_PROVIDER_DIRECTORY_BASE
    return {
        "source_id": "uhc",
        "api_base": base,
        "canonical_api_base": base,
    }


def _snapshot(
    *,
    rows=None,
    error=None,
    deadline_reached=False,
):
    return fhir.UHCPlanGraphSnapshot(
        rows_by_resource=rows or {"InsurancePlan": []},
        pages_by_resource={"InsurancePlan": 1},
        network_count=0,
        error=error,
        deadline_reached=deadline_reached,
    )


def _plan_graph_fetch_options() -> dict[str, object]:
    return {
        "page_count": 100,
        "timeout": 1,
        "run_id": "run",
        "network_reference": None,
    }


def _complete_acquisition():
    return fhir.UHCPlanGraphAcquisition(
        _snapshot(
            rows={
                "InsurancePlan": [{"resource_id": "plan"}],
                "PractitionerRole": [{"resource_id": "role"}],
            }
        ),
        True,
        collection_complete=True,
        stability_verified=True,
    )


def test_uhc_plan_graph_validation_rejects_drift_and_bad_networks():
    assert fhir._uhc_plan_graph_network_predicate_error(
        "PractitionerRole",
        "",
        [],
    ) == "uhc_plan_graph_network_reference_invalid"
    assert fhir._uhc_plan_graph_network_predicate_error(
        "PractitionerRole",
        "Organization/network-1",
        [{"resourceType": "OrganizationAffiliation"}],
    ) == "uhc_plan_graph_resource_type_mismatch"
    assert (
        fhir._uhc_plan_graph_collection_url(
            _source("https://example.test/fhir"),
            "PractitionerRole",
            page_count=100,
            network_reference="Organization/network-1",
        )
        is None
    )

    assert fhir._validated_uhc_plan_graph_total("Location", 2, 3) == (
        2,
        "uhc_plan_graph_location_total_changed",
    )
    assert fhir._validated_uhc_plan_graph_total("Location", 2, 2) == (2, None)
    assert fhir._uhc_plan_graph_terminal_count_error(
        "Location", 1, {"one": {}}
    ) is None
    assert fhir._uhc_plan_graph_terminal_count_error(
        "Location", 2, {"one": {}}
    ) == "uhc_plan_graph_location_row_count_mismatch"


@pytest.mark.asyncio
async def test_uhc_plan_graph_advance_reports_fetch_and_next_link_errors(monkeypatch):
    async def failed_bundle(*_args, **_kwargs):
        return {}, "fetch-failed"

    monkeypatch.setattr(fhir, "_fetch_uhc_plan_graph_bundle", failed_bundle)
    state = fhir.UHCPlanGraphCollectionState()
    assert await fhir._advance_uhc_plan_graph_collection(
        _source(),
        "Location",
        "https://example.test/Location",
        state,
        timeout=1,
        run_id="run",
        network_reference=None,
    ) == (None, "fetch-failed")

    async def valid_bundle(*_args, **_kwargs):
        return {"resourceType": "Bundle", "total": 0}, None

    monkeypatch.setattr(fhir, "_fetch_uhc_plan_graph_bundle", valid_bundle)
    monkeypatch.setattr(fhir, "_merge_uhc_plan_graph_bundle_rows", lambda *_a, **_k: None)
    monkeypatch.setattr(fhir, "_next_link", lambda _bundle: "bad-next")

    def invalid_next(*_args, **_kwargs):
        raise ValueError("unsafe-next")

    monkeypatch.setattr(fhir, "_resolved_fhir_next_url", invalid_next)
    assert await fhir._advance_uhc_plan_graph_collection(
        _source(),
        "Location",
        "https://example.test/Location",
        fhir.UHCPlanGraphCollectionState(),
        timeout=1,
        run_id="run",
        network_reference=None,
    ) == (None, "unsafe-next")


@pytest.mark.asyncio
async def test_uhc_plan_graph_collection_rejects_missing_and_repeated_urls(monkeypatch):
    fetch_options_by_name = _plan_graph_fetch_options()
    monkeypatch.setattr(fhir, "_uhc_plan_graph_collection_url", lambda *_a, **_k: None)
    assert await fhir._fetch_uhc_plan_graph_collection(
        _source(), "Location", deadline_at=None, **fetch_options_by_name
    ) == ([], 0, "uhc_plan_graph_start_url_missing", False)

    start_url = "https://example.test/Location"
    monkeypatch.setattr(
        fhir,
        "_uhc_plan_graph_collection_url",
        lambda *_a, **_k: start_url,
    )
    assert await fhir._fetch_uhc_plan_graph_collection(
        _source(), "Location", deadline_at=0, **fetch_options_by_name
    ) == ([], 0, "deadline_reached", True)

    async def repeat_url(_source, _type, request_url, state, **_kwargs):
        state.pages_fetched += 1
        return request_url, None

    monkeypatch.setattr(fhir, "_advance_uhc_plan_graph_collection", repeat_url)
    repeated = await fhir._fetch_uhc_plan_graph_collection(
        _source(), "Location", deadline_at=None, **fetch_options_by_name
    )
    assert repeated[2] == "pagination_cursor_repeated"


@pytest.mark.asyncio
async def test_uhc_plan_graph_collection_covers_terminal_page_outcomes(monkeypatch):
    fetch_options_by_name = _plan_graph_fetch_options()
    monkeypatch.setattr(
        fhir,
        "_uhc_plan_graph_collection_url",
        lambda *_a, **_k: "https://example.test/Location",
    )

    async def page_error(_source, _type, _url, state, **_kwargs):
        state.pages_fetched += 1
        return None, "page-failed"

    monkeypatch.setattr(fhir, "_advance_uhc_plan_graph_collection", page_error)
    failed = await fhir._fetch_uhc_plan_graph_collection(
        _source(), "Location", deadline_at=None, **fetch_options_by_name
    )
    assert failed[2] == "page-failed"

    async def terminal_mismatch(_source, _type, _url, state, **_kwargs):
        state.pages_fetched += 1
        return None, None

    monkeypatch.setattr(
        fhir,
        "_advance_uhc_plan_graph_collection",
        terminal_mismatch,
    )
    mismatch = await fhir._fetch_uhc_plan_graph_collection(
        _source(), "Location", deadline_at=None, **fetch_options_by_name
    )
    assert mismatch[2] == "uhc_plan_graph_location_row_count_mismatch"

    async def terminal_complete(_source, _type, _url, state, **_kwargs):
        state.pages_fetched += 1
        state.expected_total = 0
        return None, None

    monkeypatch.setattr(fhir, "_advance_uhc_plan_graph_collection", terminal_complete)
    assert await fhir._fetch_uhc_plan_graph_collection(
        _source(), "Location", deadline_at=None, **fetch_options_by_name
    ) == ([], 1, None, False)


@pytest.mark.asyncio
async def test_uhc_plan_graph_bundle_and_linked_reference_errors(monkeypatch):
    async def failed_source_json(*_args, **_kwargs):
        return 503, {}, None, 1

    monkeypatch.setattr(fhir, "_fetch_source_json", failed_source_json)
    assert await fhir._fetch_uhc_plan_graph_bundle(
        _source(), "https://example.test", timeout=1
    ) == ({}, "http_503")

    linked_reference = (
        "Practitioner",
        "p1",
        "Practitioner/p1",
        "practitioner_ref",
    )
    monkeypatch.setattr(fhir, "_linked_resource_refs", lambda _rows: [linked_reference])
    deadline = await fhir._fetch_uhc_plan_graph_linked_rows(
        _source(),
        {},
        timeout=1,
        run_id="run",
        deadline_at=0,
    )
    assert deadline == ({}, "deadline_reached", True)

    async def missing_linked(*_args, **_kwargs):
        return None

    monkeypatch.setattr(fhir, "_fetch_linked_resource_row", missing_linked)
    unresolved = await fhir._fetch_uhc_plan_graph_linked_rows(
        _source(),
        {},
        timeout=1,
        run_id="run",
        deadline_at=None,
    )
    assert unresolved == ({}, "uhc_plan_graph_unresolved_practitioner_reference", False)


def test_uhc_plan_graph_conflict_and_failure_snapshot_edges():
    assert not fhir._has_uhc_plan_graph_row_conflict(
        {"Location": [{"resource_id": None}]}
    )
    assert fhir._has_uhc_plan_graph_row_conflict(
        {
            "Location": [
                {"resource_id": "l1", "name": "first"},
                {"resource_id": "l1", "name": "second"},
            ]
        }
    )

    failed = fhir._uhc_plan_graph_plan_failure_snapshot([], 1, "fetch-failed", True)
    assert failed is not None and failed.error == "fetch-failed"
    empty = fhir._uhc_plan_graph_plan_failure_snapshot([], 1, None, False)
    assert empty is not None and empty.error.endswith("unexpected_empty")
    invalid = fhir._uhc_plan_graph_plan_failure_snapshot(
        [{"resource_id": "plan", "network_refs": []}],
        1,
        None,
        False,
    )
    assert invalid is not None and invalid.error.endswith("reference_invalid")
    assert fhir._uhc_plan_graph_plan_failure_snapshot(
        [{"resource_id": "plan", "network_refs": ["Organization/n1"]}],
        1,
        None,
        False,
    ) is None


@pytest.mark.asyncio
async def test_uhc_plan_graph_network_shards_report_error_and_mutation(monkeypatch):
    async def failed_collection(*_args, **_kwargs):
        return [], 1, "network-failed", True

    monkeypatch.setattr(fhir, "_fetch_uhc_plan_graph_collection", failed_collection)
    failed = await fhir._fetch_uhc_plan_graph_network_shards(
        _source(),
        ["Organization/n1"],
        page_count=100,
        timeout=1,
        run_id="run",
        deadline_at=None,
    )
    assert failed[2:] == ("network-failed", True)

    async def conflicting_collection(
        _source,
        resource_type,
        *,
        network_reference,
        **_kwargs,
    ):
        if resource_type == "OrganizationAffiliation":
            return [], 1, None, False
        return [
            {
                "resource_id": "role-1",
                "network": network_reference,
            }
        ], 1, None, False

    monkeypatch.setattr(
        fhir,
        "_fetch_uhc_plan_graph_collection",
        conflicting_collection,
    )
    conflict = await fhir._fetch_uhc_plan_graph_network_shards(
        _source(),
        ["Organization/n1", "Organization/n2"],
        page_count=100,
        timeout=1,
        run_id="run",
        deadline_at=None,
    )
    assert conflict[2] == "uhc_plan_graph_resource_mutated"


@pytest.mark.asyncio
async def test_uhc_plan_graph_two_pass_acquisition_paths(monkeypatch):
    stable_rows_by_resource = {"InsurancePlan": [{"resource_id": "plan"}]}
    successful = _snapshot(rows=stable_rows_by_resource)

    async def run_with(snapshots):
        iterator = iter(snapshots)

        async def next_snapshot(*_args, **_kwargs):
            return next(iterator)

        monkeypatch.setattr(fhir, "_fetch_uhc_plan_graph_snapshot", next_snapshot)
        return await fhir._acquire_uhc_plan_graph(
            _source(),
            page_count=100,
            timeout=1,
            run_id="run",
            deadline_seconds=0,
        )

    first_failed = await run_with([_snapshot(error="first-failed")])
    assert first_failed.plan_graph_complete is False
    second_failed = await run_with([successful, _snapshot(error="second-failed")])
    assert second_failed.snapshot.error == "second-failed"
    mutated = await run_with(
        [successful, _snapshot(rows={"InsurancePlan": [{"resource_id": "other"}]})]
    )
    assert mutated.snapshot.error == "uhc_plan_graph_snapshot_mutated"
    stable = await run_with([successful, successful])
    assert stable.plan_graph_complete is True
    assert stable.collection_complete is False
    assert stable.stability_verified is True


@pytest.mark.asyncio
async def test_uhc_plan_graph_write_covers_incomplete_and_complete_edges(monkeypatch):
    incomplete = fhir.UHCPlanGraphAcquisition(_snapshot(), False)
    selected_resource_types = ["InsurancePlan", "PractitionerRole"]
    assert await fhir._write_uhc_plan_graph_resources(
        _source(),
        incomplete,
        selected_resource_types,
        run_id="run",
        stale_cleanup=True,
        seen_table=None,
    ) == {"InsurancePlan": 0, "PractitionerRole": 0}

    complete = _complete_acquisition()

    async def count_rows(_model, rows, **_kwargs):
        return len(rows)

    monkeypatch.setattr(fhir, "_upsert_resource_rows", count_rows)
    written = await fhir._write_uhc_plan_graph_resources(
        _source(),
        complete,
        selected_resource_types,
        run_id="run",
        stale_cleanup=True,
        seen_table=None,
    )
    assert written == {"InsurancePlan": 1, "PractitionerRole": 1}


@pytest.mark.asyncio
async def test_uhc_plan_graph_finalize_stats_and_completion_edges(monkeypatch):
    complete = _complete_acquisition()
    selected_resource_types = ["InsurancePlan", "PractitionerRole"]
    deleted_counts = iter((0, 2))

    async def delete_rows(*_args, **_kwargs):
        return next(deleted_counts)

    monkeypatch.setattr(fhir, "_delete_stale_resource_rows", delete_rows)
    stale_counts, ready = await fhir._finalize_uhc_plan_graph_stale_rows(
        _source(),
        complete,
        selected_resource_types,
        run_id="run",
        stale_cleanup=True,
        seen_table=None,
    )
    assert stale_counts == {"PractitionerRole": 2}
    assert ready == {}

    failed_stats = fhir._uhc_plan_graph_stats(
        fhir.UHCPlanGraphAcquisition(_snapshot(error="failed"), False),
        "InsurancePlan",
    )
    assert failed_stats["sources_failed"] == 1
    complete_stats = fhir._uhc_plan_graph_stats(complete, "InsurancePlan")
    assert complete_stats["sources_completed"] == 1
    assert complete_stats["collection_complete_sources"] == 1

    fhir._record_uhc_plan_graph_completion(None, ["uhc"], {})
    completed_sources_by_resource: dict[str, set[str]] = {}
    fhir._record_uhc_plan_graph_completion(
        completed_sources_by_resource,
        ["uhc"],
        {
            "Location": {"complete": False},
            "InsurancePlan": {
                "complete": True,
            },
        },
    )
    assert completed_sources_by_resource == {"InsurancePlan": {"uhc"}}
