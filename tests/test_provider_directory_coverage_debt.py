# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""High-value branch coverage for durable Provider Directory helpers."""

from __future__ import annotations

import asyncio
import csv
import importlib
import io
import sqlite3
from unittest.mock import AsyncMock

import pytest

importer = importlib.import_module("process.provider_directory_fhir")


def _reverse_request(
    *,
    cancel: bool = False,
    resume: bool = False,
) -> importer._PractitionerRoleReverseLookupRequest:
    options = importer.ScanPractitionerRoleFetchOptions(
        per_resource_limit=0,
        page_limit=0,
        page_count=20,
        timeout=1,
        run_id="run-coverage",
        cancel_ctx={} if cancel else None,
        cancel_task={} if cancel else None,
        resume_completed_seeds=resume,
    )
    return importer._PractitionerRoleReverseLookupRequest(
        source={
            "source_id": "source-coverage",
            "canonical_api_base": "https://coverage.test/fhir",
        },
        rows_by_resource={},
        options=options,
        resource_timeout=1,
        deadline_at=None,
    )


def _reverse_state(
    *,
    page_limit: int = 0,
) -> importer._PractitionerRoleReverseLookupState:
    return importer._PractitionerRoleReverseLookupState(
        result_model=object,
        retained_resource_rows=[],
        streamed_rows=importer._StreamedResourceRowBuffer(
            object,
            AsyncMock(return_value=1),
            1,
            [],
        ),
        retain_rows=True,
        page_limit=page_limit,
    )


@pytest.mark.asyncio
async def test_reverse_lookup_producer_and_worker_cover_live_seed_paths(
    monkeypatch,
):
    async def two_seeds(*_args, **_kwargs):
        yield ("practitioner", "Practitioner", "one")
        yield ("organization", "Organization", "two")

    deadline_checks = iter((False, True))
    monkeypatch.setattr(
        importer,
        "_iter_scan_practitioner_role_seed_rows",
        two_seeds,
    )
    monkeypatch.setattr(
        importer,
        "_is_lookup_deadline_elapsed",
        lambda _deadline: next(deadline_checks),
    )
    cancel_check = AsyncMock()
    monkeypatch.setattr(importer, "raise_if_cancelled", cancel_check)
    state = _reverse_state()
    request = _reverse_request(cancel=True)
    seed_queue = asyncio.Queue()

    await state.produce_seed_rows(request, seed_queue, worker_count=1)

    assert state.seed_count == 1
    assert state.is_deadline_reached is True
    assert await seed_queue.get() == ("practitioner", "Practitioner", "one")
    assert await seed_queue.get() is None
    cancel_check.assert_awaited()

    worker_queue = asyncio.Queue()
    await worker_queue.put(("practitioner", "Practitioner", "one"))
    await worker_queue.put(None)
    fetch_seed_pages = AsyncMock()
    monkeypatch.setattr(state, "fetch_seed_pages", fetch_seed_pages)
    await state.fetch_seed_rows(request, worker_queue)
    fetch_seed_pages.assert_awaited_once()


def _configure_reverse_lookup_fetch(monkeypatch, fetch_result):
    monkeypatch.setattr(
        importer,
        "_scan_practitioner_role_reverse_lookup_url",
        lambda *_args, **_kwargs: "https://coverage.test/fhir/PractitionerRole",
    )
    monkeypatch.setattr(
        importer,
        "_is_lookup_deadline_elapsed",
        lambda _deadline: False,
    )
    monkeypatch.setattr(
        importer,
        "_fetch_source_json",
        AsyncMock(return_value=fetch_result),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fetch_result", "expected_error"),
    [
        ((503, {}, None, 1), "http_503"),
        ((200, {}, "socket_closed", 1), "socket_closed"),
        ((200, {"resourceType": "OperationOutcome"}, None, 1), "non_bundle_payload"),
    ],
)
async def test_reverse_lookup_fetch_rejects_bad_responses(
    monkeypatch,
    fetch_result,
    expected_error,
):
    _configure_reverse_lookup_fetch(monkeypatch, fetch_result)
    state = _reverse_state()

    await state.fetch_seed_pages(
        _reverse_request(),
        ("practitioner", "Practitioner", "one"),
    )

    assert state.reverse_error_count == 1
    assert expected_error in (state.error_message or "")


@pytest.mark.asyncio
async def test_reverse_lookup_fetch_covers_deadline_and_page_limits(monkeypatch):
    _configure_reverse_lookup_fetch(
        monkeypatch,
        (200, {"resourceType": "Bundle", "entry": []}, None, 1),
    )
    state = _reverse_state()
    monkeypatch.setattr(
        importer,
        "_is_lookup_deadline_elapsed",
        lambda _deadline: True,
    )
    await state.fetch_seed_pages(
        _reverse_request(),
        ("practitioner", "Practitioner", "one"),
    )
    assert state.is_deadline_reached is True

    state = _reverse_state()
    state.is_hard_page_limit_reached = True
    await state.fetch_seed_pages(
        _reverse_request(),
        ("practitioner", "Practitioner", "one"),
    )
    assert importer._fetch_source_json.await_count == 0


@pytest.mark.asyncio
async def test_reverse_lookup_fetch_covers_invalid_next_and_completion(
    monkeypatch,
):
    bundle_map = {"resourceType": "Bundle", "entry": []}
    _configure_reverse_lookup_fetch(monkeypatch, (200, bundle_map, None, 1))
    monkeypatch.setattr(
        importer,
        "_parse_practitioner_role_reverse_lookup_rows",
        lambda *_args, **_kwargs: [{"resource_id": "role-one"}],
    )
    monkeypatch.setattr(importer, "_next_link", lambda _bundle: "bad-next")
    monkeypatch.setattr(
        importer,
        "_resolved_fhir_next_url",
        lambda *_args: (_ for _ in ()).throw(ValueError("unsafe_next")),
    )
    state = _reverse_state()
    await state.fetch_seed_pages(
        _reverse_request(),
        ("practitioner", "Practitioner", "one"),
    )
    assert "unsafe_next" in (state.error_message or "")
    assert state.fetched_count == 1

    monkeypatch.setattr(importer, "_next_link", lambda _bundle: None)
    monkeypatch.setattr(importer, "_resolved_fhir_next_url", lambda *_args: None)
    upsert_rows = AsyncMock()
    monkeypatch.setattr(importer, "_upsert_rows", upsert_rows)
    state = _reverse_state()
    await state.fetch_seed_pages(
        _reverse_request(resume=True),
        ("practitioner", "Practitioner", "one"),
    )
    await state.flush_completed_seed_rows()
    upsert_rows.assert_awaited_once()

    state = _reverse_state(page_limit=1)
    await state.fetch_seed_pages(
        _reverse_request(),
        ("practitioner", "Practitioner", "one"),
    )
    assert state.is_hard_page_limit_reached is True


@pytest.mark.parametrize(
    ("function", "value"),
    [
        (importer._provider_api_base_from_url, None),
        (importer._provider_directory_base_from_metadata_url, None),
        (importer._provider_directory_base_from_metadata_url, "relative/metadata"),
        (importer._provider_directory_base_from_metadata_url, "https://example.test/fhir"),
        (importer._provider_directory_base_from_metadata_url, "https://example.test/metadata"),
        (importer._resource_type_from_provider_directory_url, None),
        (importer._resource_type_from_provider_directory_url, "https://example.test"),
        (importer._provider_directory_base_from_catalog_url, None),
    ],
)
def test_catalog_url_helpers_reject_non_bases(function, value):
    assert function(value) is None


def test_catalog_url_helpers_cover_unrelated_hosts_and_missing_cells():
    assert importer._is_cms_sma_base_pair_related(None, None) is False
    assert importer._is_cms_sma_base_pair_related(
        "https://one.test/fhir",
        "https://two.test/fhir",
    ) is False
    assert importer._cms_sma_column_index(["one"], {"missing"}) is None
    assert importer._cms_sma_cell([], None) is None
    assert importer._cms_sma_cell([], 2) is None


def _cms_edge_csv(rows: list[list[str]]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        ["", "Provider Directory Endpoint Information", "", "", ""]
    )
    writer.writerow(
        [
            "State",
            "Provider Directory Production Base URL",
            "Status (Drop Down List)",
            "FHIR Capability Statement Link",
            "Is the API Public? (Y/N - Drop Down List)",
        ]
    )
    writer.writerows(rows)
    return output.getvalue()


def test_cms_sma_parser_covers_skips_and_capability_only_fallback():
    missing_headers = "ignored\nignored\n"
    assert importer._cms_sma_endpoint_directory_seed_rows_from_csv(
        missing_headers
    ) == []
    csv_text = _cms_edge_csv(
        [
            ["", "", "Active", "", "Yes"],
            ["Skip", "", "Retired", "", "Yes"],
            [
                "Fallback",
                "not-a-url",
                "Active",
                "https://fallback.test/fhir/metadata",
                "Yes",
            ],
        ]
    )

    rows = importer._cms_sma_endpoint_directory_seed_rows_from_csv(csv_text)

    assert [row["org_name"] for row in rows] == ["State of Fallback"]
    assert rows[0]["api_base"] == "https://fallback.test/fhir"


def test_capability_splash_and_coding_helpers_cover_invalid_items():
    assert importer._capability_resource_types(
        {
            "rest": [
                "invalid",
                {
                    "resource": [
                        "invalid",
                        {"type": " "},
                        {"type": "Practitioner"},
                    ]
                },
            ]
        }
    ) == {"Practitioner"}
    source_url = "https://example.test/developers"
    assert importer._external_splash_target_url(None, source_url=source_url) is None
    assert importer._external_splash_target_url(
        "?splash=https%3A%2F%2Ftarget.test%2Ffhir",
        source_url=source_url,
    ) == "https://target.test/fhir"
    assert importer._external_splash_target_url(
        "/plain",
        source_url=source_url,
    ) == "https://example.test/plain"
    assert importer._codings("invalid") == []
    assert importer._codings(
        {"coding": ["invalid", {"system": None}], "text": "fallback"}
    ) == [{"text": "fallback"}]


@pytest.mark.asyncio
async def test_role_lookup_task_runner_cancels_both_failure_directions():
    class ProducerFailureState:
        async def fetch_seed_rows(self, _request, _queue):
            await asyncio.Future()

        async def produce_seed_rows(self, _request, _queue, _concurrency):
            raise RuntimeError("producer failed")

        async def flush_completed_seed_rows(self):
            return None

    with pytest.raises(RuntimeError, match="producer failed"):
        await importer._run_role_lookup_tasks(
            ProducerFailureState(),
            _reverse_request(),
            1,
        )

    class WorkerFailureState:
        async def fetch_seed_rows(self, _request, _queue):
            raise RuntimeError("worker failed")

        async def produce_seed_rows(self, _request, _queue, _concurrency):
            await asyncio.Future()

        async def flush_completed_seed_rows(self):
            return None

    with pytest.raises(RuntimeError, match="worker failed"):
        await importer._run_role_lookup_tasks(
            WorkerFailureState(),
            _reverse_request(),
            1,
        )


@pytest.mark.asyncio
async def test_role_lookup_task_runner_allows_zero_workers():
    state = _reverse_state()
    state.produce_seed_rows = AsyncMock()
    state.flush_completed_seed_rows = AsyncMock()

    await importer._run_role_lookup_tasks(state, _reverse_request(), 0)

    state.produce_seed_rows.assert_awaited_once()
    state.flush_completed_seed_rows.assert_awaited_once()


def test_seed_rows_from_sqlite_covers_query_and_limit(tmp_path):
    database_path = tmp_path / "provider-directory.db"
    connection = sqlite3.connect(database_path)
    try:
        connection.execute(
            "CREATE TABLE payers (id INTEGER, org_name TEXT, plan_name TEXT)"
        )
        connection.executemany(
            "INSERT INTO payers VALUES (?, ?, ?)",
            [(1, "First", "Alpha"), (2, "Second", "Beta")],
        )
        connection.commit()
    finally:
        connection.close()

    assert len(importer._seed_rows_from_sqlite(database_path)) == 2
    assert importer._seed_rows_from_sqlite(
        database_path,
        limit=1,
        source_query="second",
    )[0]["id"] == 2


@pytest.mark.asyncio
async def test_canonical_backfill_covers_unknown_and_known_resources(
    monkeypatch,
):
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_tables",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_selected_resources",
        lambda _resources: ["Unknown", "Practitioner"],
    )
    monkeypatch.setattr(
        importer,
        "_canonical_backfill_resource_sql",
        lambda *_args: ("canonical", "edges"),
    )
    monkeypatch.setattr(
        importer.db,
        "status",
        AsyncMock(side_effect=["INSERT 0 3", "INSERT 0 4"]),
    )

    summary = await importer.backfill_provider_directory_canonical_resources()

    assert summary["canonical_rows"] == 3
    assert summary["source_edge_rows"] == 4
    assert set(summary["resources"]) == {"Practitioner"}


def test_retest_loader_and_metadata_merge_cover_rejected_shapes(tmp_path):
    invalid_json_path = tmp_path / "invalid.json"
    invalid_json_path.write_text("not-json", encoding="utf-8")
    with pytest.raises(RuntimeError, match="could not be read"):
        importer._seed_rows_from_retest_results(invalid_json_path)

    invalid_shape_path = tmp_path / "shape.json"
    invalid_shape_path.write_text('{"results": {}}', encoding="utf-8")
    with pytest.raises(RuntimeError, match="must be a JSON object/list"):
        importer._seed_rows_from_retest_results(invalid_shape_path)

    rows_path = tmp_path / "rows.json"
    rows_path.write_text(
        '[null, {"classification": "valid", "org_name": "Missing API"}]',
        encoding="utf-8",
    )
    assert importer._seed_rows_from_retest_results(rows_path) == []

    metadata_by_name = {"values": "invalid"}
    importer._append_unique_metadata_value(metadata_by_name, "values", None)
    importer._append_unique_metadata_value(metadata_by_name, "values", "one")
    importer._append_unique_metadata_value(metadata_by_name, "values", "one")
    assert metadata_by_name == {"values": ["one"]}

    target_map = {
        "source_id": "retained",
        "org_name": "Retained",
        "api_base": "https://retained.test/fhir",
        "metadata_json": "invalid",
    }
    skipped_map = {
        "api_base": "https://old.test/fhir",
        "metadata_json": {"provider_directory_override": "manual"},
    }
    importer._merge_skipped_source_row_metadata(target_map, skipped_map)
    assert target_map["metadata_json"]["provider_directory_merged_overrides"] == [
        "manual"
    ]
