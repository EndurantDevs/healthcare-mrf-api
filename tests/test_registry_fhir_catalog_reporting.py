# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure run telemetry checks confer no source or publication authority."""

import asyncio
import json
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import live_progress
from process import registry_fhir_catalog_reporting as reporting
from process import registry_fhir_catalog_sync as sync
from process.control_cancel import ImportCancelledError
from process.registry_fhir_catalog_sources import FHIRCatalogSourceDisposition


def disposition(*, status="accepted", dataset="edition-one", aliases=("alias-a", "alias-b")):
    return FHIRCatalogSourceDisposition(
        "endpoint-one",
        dataset,
        aliases,
        len(aliases),
        status,
        None if status == "accepted" else "current_dataset_missing",
        {"coordinates": {"source_id": aliases[0]}, "profile": "published_normalized"} if status == "accepted" else None,
        None,
    )


def report_with_source():
    report = reporting.FHIRCatalogRunReport()
    report.add_sources((disposition(),), generations=("a" * 64,))
    return report


def test_profile_inventory_aliases_and_retained_editions_stay_distinct_without_private_policy():
    report = reporting.FHIRCatalogRunReport()
    profile = SimpleNamespace(schema_name="example_source", catalog_schema="example_catalog", owner_role="secret-owner")
    index = report.add_profile(profile)
    report.add_sources(
        (disposition(), disposition(status="unavailable", dataset=None)),
        profile_index=index,
        generations=("a" * 64, None),
    )
    report.document["profiles"][index]["complete"] = True
    report.add_sources((disposition(dataset="older-edition"),), generations=("b" * 64,))
    previous_by_field = {"source_vector_json": [{"source_generation": "c" * 64, "source_id": "historical-source"}]}
    report.inventory_ready({"a" * 64: object(), "b" * 64: object()}, previous_by_field)
    rows = report.document["sources"]
    assert len(rows) == 4 and [row["inventory_kind"] for row in rows] == [
        "registered",
        "registered",
        "retained",
        "carried",
    ]
    assert rows[0]["source_aliases"] == ["alias-a", "alias-b"] and rows[0]["aliases_complete"] is True
    assert rows[1]["outcome"] == "unavailable" and rows[1]["reason"] == "current_dataset_missing"
    assert rows[2]["dataset_id"] == "older-edition" and rows[3]["endpoint_id"] is None
    assert report.document["inventory_complete"] is True
    assert report.document["global_source_count"] is report.document["global_network_count"] is None
    encoded = json.dumps(report.document)
    assert "secret-owner" not in encoded and "native_read_profile" not in encoded and "source_uri" not in encoded


def test_alias_overflow_is_explicitly_unknown_and_does_not_invent_missing_aliases():
    report = reporting.FHIRCatalogRunReport()
    source = replace(disposition(status="unavailable"), alias_count=1001, reason="source_alias_bounds")
    report.add_sources((source,), profile_index=0)
    row = report.document["sources"][0]
    assert row["source_aliases"] == ["alias-a", "alias-b"] and row["alias_count"] == 1001
    assert row["aliases_complete"] is False and row["reason"] == "source_alias_bounds"


def test_inventory_and_admitted_aliases_preserve_distinct_canonical_identity():
    source = disposition()
    source.descriptor["source_aliases"] = ["alias-b"]
    source.descriptor["coordinates"]["source_id"] = "alias-b"
    report = reporting.FHIRCatalogRunReport()
    report.add_sources((source,), profile_index=0, generations=("a" * 64,))
    row = report.document["sources"][0]
    assert row["source_id"] == "alias-b"
    assert row["source_aliases"] == ["alias-a", "alias-b"]
    assert row["admitted_source_aliases"] == ["alias-b"]


@pytest.mark.parametrize("state", ["completed", "unchanged"])
def test_replay_and_publication_keep_unknown_counts_null(state):
    report = report_with_source()
    report.inventory_ready({"a" * 64: object()}, None)
    report.finish(state)
    row = report.document["sources"][0]
    assert row["outcome"] == ("published" if state == "completed" else "unchanged")
    assert all(value is None for value in row["counts"].values())
    assert report.document["publication_state"] == ("committed" if state == "completed" else "unchanged")


def test_empty_source_counts_require_projection_and_publication_stays_separate():
    report = report_with_source()
    report.source_started("a" * 64)
    report.source_prepared(
        "a" * 64, {"source_rows": 12, "observation_rows": 4}, {"networks": 0, "created_networks": 0, "relationships": 0}
    )
    row = report.document["sources"][0]
    assert row["outcome"] == "prepared" and row["counts"]["networks"] == 0
    assert row["count_scope"] == "source_organization_projection" and row["catalog_network_count"] is None
    report.failed(RuntimeError("private connection string must not be copied"))
    assert row["outcome"] == "prepared" and report.document["run_state"] == "failed"
    assert "private connection" not in json.dumps(report.document)


@pytest.mark.parametrize(
    "error",
    [RuntimeError("secret"), asyncio.CancelledError("secret"), ImportCancelledError("secret"), TimeoutError("secret")],
)
def test_failures_preserve_completed_prefix_and_do_not_assert_publication_failure(error):
    report = report_with_source()
    report.add_sources((disposition(dataset="edition-two"),), generations=("b" * 64,))
    report.source_prepared("a" * 64, {}, {"networks": 2})
    report.source_started("b" * 64)
    report.publication_started()
    report.failed(error)
    first, second = report.document["sources"]
    assert first["outcome"] == "prepared" and first["counts"]["networks"] == 2
    assert second["outcome"] == (
        "canceled" if isinstance(error, (asyncio.CancelledError, ImportCancelledError)) else "failed"
    )
    assert report.document["publication_state"] == "unknown"
    assert "secret" not in json.dumps(report.document)


@pytest.mark.parametrize("counter", [-1, True, 2**63, "1"])
def test_invalid_measured_counters_refuse_before_publication(counter):
    report = report_with_source()
    with pytest.raises(ValueError, match="counter_invalid"):
        report.source_prepared("a" * 64, {}, {"networks": counter})
    assert report.document["publication_state"] == "not_attempted"


def test_exact_report_bound_accepts_fit_and_refuses_next_source_without_trimming(monkeypatch):
    expected = report_with_source()
    monkeypatch.setattr(reporting, "DEFAULT_BYTES", expected._reserved_bytes)
    actual = report_with_source()
    assert actual.document == expected.document
    retained = json.dumps(actual.document["sources"])
    with pytest.raises(ValueError, match="source_report_bounds"):
        actual.add_sources((disposition(dataset="edition-two"),), generations=("b" * 64,))
    assert json.dumps(actual.document["sources"]) == retained
    assert actual.document["inventory_complete"] is False
    assert actual.document["report_error"] == "source_report_bounds"
    assert len(reporting.fhir_catalog_json_bytes(actual.document)) <= reporting.DEFAULT_BYTES


def test_unavailable_inventory_has_no_new_thousand_source_cap():
    report = reporting.FHIRCatalogRunReport()
    report.add_sources(
        tuple(disposition(status="unavailable", dataset=str(index)) for index in range(1001)), profile_index=0
    )
    assert len(report.document["sources"]) == 1001
    assert len(reporting.fhir_catalog_json_bytes(report.document)) <= reporting.DEFAULT_BYTES


def test_report_context_shares_control_audit_and_resets_after_failure():
    context_by_field = {"audit": {"existing": "preserved"}}
    with pytest.raises(RuntimeError):
        with reporting.fhir_catalog_run_reporting(context_by_field) as report:
            with reporting.fhir_catalog_run_reporting() as nested:
                assert report is nested
            assert context_by_field["audit"]["fhir_catalog_sync"] is report.document
            raise RuntimeError("secret")
    assert context_by_field["audit"]["existing"] == "preserved"
    assert context_by_field["audit"]["fhir_catalog_sync"]["run_state"] == "failed"
    assert reporting.current_fhir_catalog_report() is None


def test_parent_cancellation_finalizes_shared_audit_synchronously_and_resets_token():
    context_by_field = {}
    with pytest.raises(asyncio.CancelledError):
        with reporting.fhir_catalog_run_reporting(context_by_field) as report:
            report.add_sources((disposition(),), generations=("a" * 64,))
            report.source_started("a" * 64)
            raise asyncio.CancelledError()
    audit = context_by_field["audit"]["fhir_catalog_sync"]
    assert audit["run_state"] == audit["sources"][0]["outcome"] == "canceled"
    assert reporting.current_fhir_catalog_report() is None


def test_progress_uses_existing_attempt_context_once_per_source_boundary(monkeypatch):
    events = []
    monkeypatch.setattr(reporting, "current_live_progress_context", lambda: {"run_id": "synthetic-run"})
    monkeypatch.setattr(reporting, "enqueue_live_progress", lambda **payload: events.append(payload))
    report = report_with_source()
    report.inventory_ready({"a" * 64: object()}, None)
    report.source_started("a" * 64)
    report.source_prepared("a" * 64, {}, {"networks": 1000})
    assert len(events) == 3
    assert [event["done"] for event in events] == [0, 0, 1]
    assert all(event["total"] == 1 and event["unit"] == "source" for event in events)
    assert all("secret" not in json.dumps(event) and "networks" not in event for event in events)


@pytest.mark.asyncio
@pytest.mark.parametrize("fail", [False, True])
async def test_discovery_records_every_disposition_and_only_complete_profile_eof(monkeypatch, fail):
    events = []
    monkeypatch.setattr(reporting, "current_live_progress_context", lambda: {"run_id": "synthetic-run"})
    monkeypatch.setattr(reporting, "enqueue_live_progress", lambda **payload: events.append(payload))

    async def pages(*_args, **_kwargs):
        assert len(events) == 1 and events[0]["phase"] == "discovery"
        assert events[0]["denominator_state"] == "unknown" and events[0]["total"] is None
        yield (disposition(), disposition(status="unavailable", dataset=None))
        assert events[-1]["counters"]["sources_inspected"] == 2
        if fail:
            raise RuntimeError("private SQL error")

    monkeypatch.setattr(sync, "iter_fhir_catalog_sources", pages)
    monkeypatch.setattr(sync, "fhir_catalog_source_generation", lambda _descriptor: "a" * 64)
    grant = SimpleNamespace(
        source_profiles=(SimpleNamespace(schema_name="example", catalog_schema="example"),), observation_contract=None
    )
    with reporting.fhir_catalog_run_reporting() as report:
        if fail:
            with pytest.raises(RuntimeError, match="private SQL"):
                await sync._discover_catalog_sources(object(), grant)
        else:
            accepted, unavailable = await sync._discover_catalog_sources(object(), grant)
            assert len(accepted) == len(unavailable) == 1
        assert len(report.document["sources"]) == 2
        assert report.document["profiles"][0]["complete"] is not fail
        assert report.document["inventory_complete"] is False
        assert len(events) == 2


def merged_catalog_progress(previous_by_field, event_by_field, *, seconds):
    """Apply the actual progress writer's merge at a deterministic observation time."""
    observed_at = datetime(2026, 1, 1) + timedelta(seconds=seconds)
    return live_progress._merged_live_progress_candidate(
        run_id="synthetic-catalog",
        context={},
        progress_by_field=event_by_field,
        observed_at=observed_at.isoformat(),
        now=observed_at,
        previous=previous_by_field,
    )


def test_discovery_progress_survives_heartbeat_without_inventing_a_total(monkeypatch):
    """Keep unknown work, writer-owned ordering and passive observations distinct."""
    events = []
    monkeypatch.setattr(reporting, "current_live_progress_context", lambda: {"run_id": "synthetic-catalog"})
    monkeypatch.setattr(reporting, "enqueue_live_progress", lambda **payload: events.append(payload))
    clock = iter((2.0, 4.0, 6.0))
    monkeypatch.setattr(reporting, "monotonic", lambda: next(clock))
    report = reporting.FHIRCatalogRunReport()
    report.progress("discovery")
    report.observation_progress("edition-one", 2)
    report.observation_progress("edition-two", 3)
    live_progress._reset_attempt_sequences("synthetic-catalog")
    progress_by_field = {
        "attempt_id": "synthetic-catalog",
        "unit": "run",
        "done": 0,
        "total": 1,
        "pct": 0,
        "source": "engine-heartbeat",
        "progress_seq": 40,
    }
    for index, event in enumerate(events):
        progress_by_field = merged_catalog_progress(progress_by_field, event, seconds=index)
        assert "total" not in progress_by_field and "pct" not in progress_by_field
    assert progress_by_field["counters"]["resource_rows_scanned"] == 5
    assert progress_by_field["progress_seq"] == 43
    report.observation_progress("edition-three", 4)
    heartbeat_by_field = merged_catalog_progress(
        progress_by_field,
        {
            "source": "engine-heartbeat",
            "phase": "control_start running",
            "message": "running",
            "unit": "run",
            "done": 0,
            "total": 1,
            "pct": 0,
        },
        seconds=20,
    )
    assert heartbeat_by_field["phase"] == "discovery"
    assert heartbeat_by_field["message"] == progress_by_field["message"]
    assert heartbeat_by_field["observed_at"] != progress_by_field["observed_at"]
    assert heartbeat_by_field["progressed_at"] == progress_by_field["progressed_at"]
    assert heartbeat_by_field["progress_seq"] == progress_by_field["progress_seq"]
    assert "total" not in heartbeat_by_field and "pct" not in heartbeat_by_field
    next_progress_by_field = merged_catalog_progress(heartbeat_by_field, events[-1], seconds=21)
    assert next_progress_by_field["counters"]["resource_rows_scanned"] == 9
    assert next_progress_by_field["progress_seq"] == 44


@pytest.mark.asyncio
@pytest.mark.parametrize("cached", [False, True])
async def test_source_projection_reports_identity_and_measured_counts_without_new_reads(monkeypatch, cached):
    source = disposition()
    stage = AsyncMock(return_value={"source_rows": 10, "observation_rows": 5})
    projection = AsyncMock(return_value={"networks": 3, "created_networks": 1, "relationships": 6})
    monkeypatch.setattr(sync, "stage_fhir_catalog_source", stage)
    monkeypatch.setattr(sync, "project_fhir_catalog_source", projection)
    previous = {"source_vector_json": [{"source_generation": "a" * 64}]} if cached else None
    with reporting.fhir_catalog_run_reporting() as report:
        report.add_sources((source,), generations=("a" * 64,))
        await sync._project_current_sources(
            SimpleNamespace(grant=SimpleNamespace(control_schema="example"), loader_role="example"),
            object(),
            object(),
            object(),
            object(),
            {},
            previous,
            {"a" * 64: source},
        )
        row = report.document["sources"][0]
        assert row["counts"]["networks"] == 3 and row["counts"]["created_networks"] == 1
        assert row["counts"]["source_rows"] == (None if cached else 10)
        assert row["outcome"] == "prepared"
        assert stage.await_count == int(not cached) and projection.await_count == 1


@pytest.mark.asyncio
async def test_reporting_overflow_stops_real_preparation_before_registry_or_candidate_writes(monkeypatch):
    profile = SimpleNamespace(schema_name="example", catalog_schema="example")
    expected = reporting.FHIRCatalogRunReport()
    expected.add_profile(profile)
    expected.add_sources((disposition(status="unavailable"),), profile_index=0)
    monkeypatch.setattr(reporting, "DEFAULT_BYTES", expected._reserved_bytes)

    @asynccontextmanager
    async def scoped(value=None):
        yield value

    async def pages(*_args, **_kwargs):
        yield (disposition(status="unavailable"), disposition(status="unavailable", dataset="edition-two"))

    source_connection = SimpleNamespace(execute=AsyncMock(), transaction=lambda **_kwargs: scoped())
    loader = SimpleNamespace(transaction=lambda **_kwargs: scoped())
    pool = SimpleNamespace(acquire=lambda: scoped(source_connection))
    grant = SimpleNamespace(source_profiles=(profile,), observation_contract=None)
    head, build = AsyncMock(), AsyncMock()
    monkeypatch.setattr(sync, "iter_fhir_catalog_sources", pages)
    monkeypatch.setattr(sync, "fhir_catalog_reference_workspace", lambda _connection: scoped(object()))
    monkeypatch.setattr(sync, "fhir_catalog_published_reads", lambda _connection: nullcontext())
    monkeypatch.setattr(sync, "_catalog_registry_head", head)
    monkeypatch.setattr(sync, "_build_catalog_candidate", build)
    staged_by_kind = {}
    with reporting.fhir_catalog_run_reporting() as report:
        with pytest.raises(ValueError, match="source_report_bounds"):
            await sync._prepare_fhir_catalog_snapshot(
                SimpleNamespace(grant=grant), loader, pool, object(), staged_by_kind
            )
        assert len(report.document["sources"]) == 1 and report.document["inventory_complete"] is False
        assert report.document["report_error"] == "source_report_bounds"
    head.assert_not_awaited()
    build.assert_not_awaited()
    assert staged_by_kind == {}
