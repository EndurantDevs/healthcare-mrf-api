# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import contextlib
import csv
import importlib
import io
import urllib.parse
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import Column, MetaData, String, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.models import (
    ProviderDirectoryAPIEndpoint,
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryPractitioner,
    ProviderDirectorySource,
)


fhir = importlib.import_module("process.provider_directory_fhir")


def _scan_options(**overrides):
    options_by_name = {
        "per_resource_limit": 0,
        "page_limit": 0,
        "page_count": 10,
        "timeout": 1,
        "run_id": "run-reader",
    }
    options_by_name.update(overrides)
    return fhir.ScanPractitionerRoleFetchOptions(**options_by_name)


@contextlib.asynccontextmanager
async def _async_context(value):
    yield value


def test_reverse_lookup_and_start_url_empty_boundaries():
    source_by_field = {
        "endpoint_practitioner": "Practitioner",
        "metadata_json": {"provider_directory_resource_page_count_cap": {}},
    }

    assert (
        fhir._metadata_resource_page_count_cap(source_by_field, "Practitioner") is None
    )
    assert (
        fhir._resource_start_url(source_by_field, "Practitioner", page_count=10) is None
    )
    assert fhir._practitioner_role_reverse_lookup_params({}) == {}
    assert (
        fhir._scan_practitioner_role_reverse_lookup_url(
            {}, "practitioner", "Practitioner", "p1", page_count=10
        )
        is None
    )


@pytest.mark.asyncio
async def test_reverse_lookup_checkpoint_short_circuits(monkeypatch):
    status = AsyncMock(return_value=3)
    monkeypatch.setattr(fhir.db, "status", status)
    deleted = await fhir._clear_reverse_lookup_checkpoints(
        {"api_base": "https://example.test/fhir"}, run_id="run-reader"
    )
    options = _scan_options(
        resume_completed_seeds=True,
        source={"api_base": "https://example.test/fhir"},
    )

    assert deleted == 3
    assert await fhir._mark_checkpointed_reverse_lookup_roles_seen(options) == 0
    assert fhir._postal_checkpoint_seen_params({}, [], None) is None
    assert await fhir._mark_postal_checkpointed_roles_seen({}, [], None, None) == 0


@pytest.mark.asyncio
async def test_scan_seed_iterators_cover_empty_and_skipped_rows(monkeypatch):
    options = _scan_options(source={})
    assert [
        seed async for seed in fhir._iter_scan_practitioner_role_seed_rows({}, options)
    ] == []

    monkeypatch.setattr(
        fhir,
        "_practitioner_role_reverse_lookup_resources",
        lambda _source: ("Unknown",),
    )
    skipped = _scan_options(
        source={}, seed_stage_table="stage", seed_source_ids=("source",)
    )
    assert [seed async for seed in fhir._scan_role_stage_seeds(skipped)] == []

    monkeypatch.setattr(
        fhir,
        "_practitioner_role_reverse_lookup_resources",
        lambda _source: ("Practitioner",),
    )
    monkeypatch.setattr(
        fhir,
        "_practitioner_role_reverse_lookup_params",
        lambda _source: {"Practitioner": "practitioner"},
    )
    monkeypatch.setattr(fhir.db, "all", AsyncMock(side_effect=[[(None,)], []]))
    paged = _scan_options(
        source={},
        seed_stage_table="stage",
        seed_source_ids=("source",),
        seed_page_size=1,
    )
    assert [seed async for seed in fhir._scan_role_stage_seeds(paged)] == []


def test_reference_and_metadata_reader_boundaries(monkeypatch):
    monkeypatch.setattr(fhir, "_candidate_base_urls", lambda _source: ["relative"])

    assert fhir._insurance_plan_network_references({"plan": [None]}) == []
    assert fhir._json_object("{") == {}
    assert fhir._source_hosts({}) == set()
    assert not fhir._is_url_within_api_base(
        "https://example.test:bad/fhir", "https://example.test/fhir"
    )
    assert fhir._references([{}]) == []
    assert fhir._organization_reference_identity("Organization/bad$id") is None


def test_reviewed_telehealth_and_date_edge_values():
    reviewed_url = next(iter(fhir.REVIEWED_TELEHEALTH_EXTENSION_URLS))
    delivery_url = next(iter(fhir.PLAN_NET_DELIVERY_METHOD_EXTENSION_URLS))
    resource_by_field = {
        "extension": [
            {"url": reviewed_url, "valueBoolean": "yes"},
            {"url": delivery_url, "extension": [{"url": "other"}]},
        ]
    }

    assert fhir._reviewed_telehealth(resource_by_field) == []
    assert fhir._full_fhir_date("2026-02-31") is None


def test_alohr_scalar_boundaries(monkeypatch):
    assert fhir._alohr_resource_id("prefix")
    assert fhir._alohr_npi("not-a-number") is None

    def fail_int(_value):
        raise RuntimeError("int failed")

    monkeypatch.setattr(fhir, "int", fail_int, raising=False)
    assert fhir._alohr_npi("123") is None
    assert fhir._alohr_telecom([{}], "") == []
    assert fhir._alohr_specialty_codings(None, "Specialty") == [
        {"display": "Specialty"}
    ]


@pytest.mark.asyncio
async def test_artifact_lock_release_and_simple_publishers(monkeypatch):
    monkeypatch.setattr(fhir.db, "status", AsyncMock(return_value=1))
    await fhir.publish_provider_directory_address_corroboration_view("mrf")

    monkeypatch.setattr(
        fhir, "provider_directory_address_corroboration_sql", lambda _s: "bad"
    )
    with pytest.raises(ValueError, match="view AS marker"):
        fhir.provider_directory_address_corroboration_select_sql("mrf")

    connection = SimpleNamespace(
        execute=AsyncMock(side_effect=RuntimeError("lost")),
        invalidate=AsyncMock(),
        close=AsyncMock(),
    )
    await fhir._release_provider_directory_artifact_build_lock(connection, "lock")
    connection.invalidate.assert_awaited_once()
    connection.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_address_artifact_small_helpers(monkeypatch):
    monkeypatch.setattr(fhir.db, "status", AsyncMock())
    metrics_by_field = {"rows": 2}
    assert (
        await fhir._address_corroboration_network_metrics(
            "mrf",
            [],
            refresh_network_catalog=False,
            metrics_override=metrics_by_field,
        )
        is metrics_by_field
    )
    ensure = AsyncMock(return_value={"rows": 3})
    monkeypatch.setattr(
        fhir, "_ensure_provider_directory_network_catalog_populated", ensure
    )
    assert await fhir._address_corroboration_network_metrics(
        "mrf", [], refresh_network_catalog=False, metrics_override=None
    ) == {"rows": 3}

    prepare = AsyncMock()
    monkeypatch.setattr(fhir, "_prepare_provider_directory_artifact_stage", prepare)
    metrics, stage = await fhir._prepared_address_corroboration_result(
        "mrf", "stage", {"rows": 1}, fhir.ProviderDirectoryArtifactBuildFence(None)
    )
    assert metrics == {"rows": 1}
    assert stage.stage_table == "stage"
    await fhir._best_effort_drop_address_corroboration_stage('"mrf"."stage"')


@pytest.mark.asyncio
async def test_location_address_key_stuck_cursor_stops(monkeypatch):
    monkeypatch.setattr(
        fhir, "_has_address_canon_functions", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(fhir, "_is_table_present", AsyncMock(return_value=False))
    monkeypatch.setattr(
        fhir.db,
        "first",
        AsyncMock(
            side_effect=[
                {
                    "candidate_rows": 1,
                    "updated_rows": 1,
                    "last_source_id": "s",
                    "last_resource_id": "r",
                },
                {
                    "candidate_rows": 1,
                    "updated_rows": 0,
                    "last_source_id": "s",
                    "last_resource_id": "r",
                },
            ]
        ),
    )

    assert await fhir.publish_provider_directory_location_address_keys("mrf") == 1


def test_update_expression_edge_columns():
    canonical = ProviderDirectoryCanonicalResource.__table__
    canonical_stmt = pg_insert(canonical)
    endpoint = ProviderDirectoryAPIEndpoint.__table__
    endpoint_stmt = pg_insert(endpoint)
    source = ProviderDirectorySource.__table__
    source_stmt = pg_insert(source)

    assert (
        fhir._effective_update_expression(
            canonical, canonical_stmt, "first_seen_run_id"
        )
        is not None
    )
    assert (
        fhir._effective_update_expression(endpoint, endpoint_stmt, "created_at")
        is not None
    )
    assert (
        fhir._effective_update_expression(source, source_stmt, "last_probe_status_code")
        is not None
    )
    assert "COALESCE" in fhir._effective_update_sql(
        endpoint, "created_at", target_prefix="target", incoming_prefix="incoming"
    )


def test_canonical_row_builder_empty_boundaries():
    assert (
        fhir._canonical_resource_rows(object, [], canonical_api_base=None, run_id=None)
        == []
    )
    assert (
        fhir._canonical_resource_rows(
            ProviderDirectoryPractitioner,
            [{}],
            canonical_api_base="https://example.test/fhir",
            run_id=None,
        )
        == []
    )
    assert (
        fhir._endpoint_dataset_resource_rows(
            ProviderDirectoryPractitioner, [{}], dataset_id="dataset"
        )
        == []
    )
    assert (
        fhir._source_resource_edge_rows(
            object, [], canonical_api_base=None, source_ids=[], run_id=None
        )
        == []
    )


@pytest.mark.asyncio
async def test_upsert_value_and_copy_boundaries(monkeypatch):
    metadata = MetaData()
    table = Table(
        "reader_tiny",
        metadata,
        Column("id", String, primary_key=True),
        Column("value", String),
    )
    model = SimpleNamespace(__table__=table)
    statement = pg_insert(table)
    assert fhir._upsert_changed_row_predicate(table, statement, ["id"], ["id"]) is None
    assert (
        await fhir._upsert_rows_values(model, [], ["id"], ["id"], skip_unchanged=False)
        == 0
    )

    session = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(fhir.db, "session", lambda: _async_context(session))
    assert (
        await fhir._upsert_rows_values(
            model,
            [{"id": "1", "value": "v"}],
            ["id", "value"],
            ["id"],
            skip_unchanged=False,
        )
        == 1
    )

    connection = SimpleNamespace(
        status=AsyncMock(),
        raw_connection=SimpleNamespace(driver_connection=SimpleNamespace()),
    )
    monkeypatch.setattr(fhir.db, "acquire", lambda: _async_context(connection))
    with pytest.raises(NotImplementedError, match="copy_records_to_table"):
        await fhir._copy_upsert_rows(
            model, [{"id": "1"}], ["id"], ["id"], skip_unchanged=False
        )


@pytest.mark.asyncio
async def test_seen_copy_and_contact_backfill_boundaries(monkeypatch):
    connection = SimpleNamespace(
        status=AsyncMock(),
        raw_connection=SimpleNamespace(driver_connection=SimpleNamespace()),
    )
    monkeypatch.setattr(fhir.db, "acquire", lambda: _async_context(connection))
    with pytest.raises(NotImplementedError, match="copy_records_to_table"):
        await fhir._copy_mark_resource_rows_seen("Practitioner", [("s", "r")], "run")

    monkeypatch.setattr(fhir.db, "status", AsyncMock(return_value=2))
    assert await fhir._clear_resource_rows_seen("run") == 2
    monkeypatch.setattr(fhir, "_ensure_provider_directory_tables", AsyncMock())
    monkeypatch.setattr(
        fhir, "provider_directory_location_contact_backfill_sql", lambda _s: "SQL"
    )
    assert await fhir.backfill_provider_directory_location_contacts() == {
        "location_contact_rows_updated": 2
    }


def _cms_reader_csv() -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["", "Provider Directory Endpoint Information", "", "", ""])
    writer.writerow(
        [
            "State",
            "Provider Directory Production Base URL",
            "Status (Drop Down List)",
            "FHIR Capability Statement Link",
            "Is the API Public? (Y/N - Drop Down List)",
        ]
    )
    writer.writerow(
        [
            "Reader",
            "https://bad.test/Practitioner",
            "Active",
            "https://bad.test/metadata",
            "Yes",
        ]
    )
    return output.getvalue()


def test_catalog_parser_skip_boundaries(monkeypatch):
    parser = fhir._HtmlLinkParser()
    parser.feed("<a>missing href</a>")
    assert parser.links == []
    assert (
        fhir._cms_sma_selected_api_base(
            "https://one.test/fhir", ["https://two.test/fhir"]
        )
        == "https://one.test/fhir"
    )

    monkeypatch.setattr(
        fhir, "_provider_directory_base_from_metadata_url", lambda _url: None
    )
    monkeypatch.setattr(
        fhir, "_provider_directory_base_from_catalog_url", lambda _url: None
    )
    assert fhir._cms_sma_endpoint_directory_seed_rows_from_csv(_cms_reader_csv()) == []
    assert (
        fhir._amerihealth_caritas_seed_rows_from_catalog_html(
            "<table><tr><td>code</td><td>Plan</td></tr></table>"
        )
        == []
    )


@pytest.mark.asyncio
async def test_fetch_loop_and_cooldown_boundaries(monkeypatch):
    fetch_result = (500, None, None, 1)
    monkeypatch.setattr(fhir, "_source_fetch_retry_attempts", lambda: 1)
    monkeypatch.setattr(
        fhir,
        "_fetch_source_json_attempt",
        AsyncMock(return_value=(fetch_result, False, False, False)),
    )
    fetched, smaller, retries = await fhir._fetch_source_json_candidate(
        {}, "https://x.test", timeout=1, is_last_candidate=True
    )
    assert fetched == fetch_result
    assert not smaller and retries == 0

    monkeypatch.setattr(fhir, "_source_fetch_candidate_urls", lambda _url: [])
    monkeypatch.setattr(fhir, "_record_terminal_source_fetch_diagnostic", Mock())
    assert (await fhir._fetch_source_json({}, "https://x.test", timeout=1))[
        2
    ] == "request_not_attempted"

    monkeypatch.setattr(fhir.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(
        fhir,
        "_fetch_cooldown_checkpoint_page",
        AsyncMock(return_value=((423, None, None, 0), True)),
    )
    cooldown = await fhir._retry_rest_pagination_after_cooldown(
        {}, "https://x.test", (423, None, None, 0), timeout=1, deadline_at=None
    )
    assert cooldown.deadline_blocked


@pytest.mark.asyncio
async def test_cooldown_deadline_boundaries(monkeypatch):
    monkeypatch.setattr(fhir.time, "monotonic", lambda: 10.0)
    prior = (423, None, None, 0)
    assert await fhir._fetch_cooldown_checkpoint_page(
        {}, "https://x.test", prior, timeout=1, deadline_at=9.0
    ) == (prior, True)

    monkeypatch.setattr(fhir.time, "monotonic", lambda: 0.0)

    async def fail_wait(awaitable, **_kwargs):
        awaitable.close()
        raise TimeoutError

    monkeypatch.setattr(fhir.asyncio, "wait_for", fail_wait)
    result, blocked = await fhir._fetch_cooldown_checkpoint_page(
        {}, "https://x.test", prior, timeout=1, deadline_at=1.0
    )
    assert blocked and result[0] is None


@pytest.mark.asyncio
async def test_empty_probe_batch_skips_all_flushes(monkeypatch):
    monkeypatch.setattr(fhir, "_mark_provider_directory_progress", AsyncMock())
    monkeypatch.setattr(fhir, "_upsert_rows", AsyncMock())
    assert await fhir._run_source_probe_batch(
        [], timeout=1, concurrency=1, run_id=None
    ) == (
        0,
        0,
        set(),
    )


def test_bundle_and_pagination_reader_boundaries():
    assert not fhir._is_bundle_payload(None)
    assert fhir._bundle_entries(None) == []
    assert fhir._next_link(None) is None
    assert fhir._pagination_host_key("https://example.test:bad") is None
    assert fhir._pagination_allowed_hosts(
        {
            "metadata_json": {
                "provider_directory_pagination_allowed_hosts": ["http://bad"]
            }
        },
        "https://good.test/fhir",
    ) == {"good.test:443"}

    missing_cursor = urllib.parse.urlsplit("https://idaho.test/fhir?_count=10")
    bad_count = urllib.parse.urlsplit("https://idaho.test/fhir?ct=x&_count=bad")
    assert not fhir._is_idaho_medicaid_query_valid(missing_cursor, "Practitioner")
    assert not fhir._is_idaho_medicaid_query_valid(bad_count, "Practitioner")
    assert fhir._url_https_port(urllib.parse.urlsplit("https://example.test:bad")) == 0
