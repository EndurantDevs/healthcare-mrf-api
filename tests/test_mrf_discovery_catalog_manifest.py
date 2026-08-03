# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest
from sqlalchemy.dialects import postgresql

from api import mrf_discovery_catalog_manifest as manifest
from api.mrf_discovery_catalog_paging import (
    MAX_FILE_PAGE_PLAN_REFERENCES,
    bounded_file_windows,
    parse_file_cursor,
)
from db.models import MRFFile, db
from process import mrf_discovery_catalog_manifest as manifest_refresh


def _file_row(file_index: int, plan_reference_count: int) -> dict[str, object]:
    return {
        "mrf_file_id": f"file_{file_index:04d}",
        "metadata_json": {"plan_info": [{}] * plan_reference_count},
        "plan_ids": [],
        "plan_names": [],
        "market_types": [],
    }


def _page_total_from_production_pager(
    plan_reference_counts: list[int],
    *,
    page_limit: int,
) -> int:
    """Count requests using the same windows and cursors as the reader."""

    if not plan_reference_counts:
        return 1
    file_rows = [
        _file_row(file_index, plan_reference_count)
        for file_index, plan_reference_count in enumerate(plan_reference_counts)
    ]
    file_index_by_id = {
        str(file_row["mrf_file_id"]): file_index
        for file_index, file_row in enumerate(file_rows)
    }
    cursor: str | None = None
    page_total = 0
    while True:
        cursor_file_id, cursor_plan_offset = parse_file_cursor(cursor)
        start_index = 0
        if cursor_file_id:
            start_index = file_index_by_id[cursor_file_id]
            if not cursor_plan_offset:
                start_index += 1
        page_rows = file_rows[start_index:start_index + page_limit + 1]
        page_windows, cursor = bounded_file_windows(
            page_rows,
            limit=page_limit,
            cursor_plan_offset=cursor_plan_offset,
            plan_reference_limit=MAX_FILE_PAGE_PLAN_REFERENCES,
        )
        assert page_windows
        page_total += 1
        if cursor is None:
            return page_total


def _manifest_metadata(
    *,
    source_version: str = "run_example",
    plan_reference_counts: list[int] | None = None,
) -> dict[str, object]:
    accumulator = manifest._SourceManifestAccumulator.create("source_example")
    for file_index, plan_reference_count in enumerate(plan_reference_counts or []):
        accumulator.add_file(f"file_{file_index:04d}", plan_reference_count)
    return {
        "discovery_run_id": source_version,
        manifest.CATALOG_PAGING_MANIFEST_METADATA_KEY: accumulator.manifest(
            source_version
        ),
    }


def test_catalog_page_totals_match_the_production_pager_for_all_supported_limits():
    """One scalar counter must match real dual-bound response traversal."""

    plan_reference_counts = [0, 3, 10_000, 10_005, 1, 0]

    assert manifest.catalog_page_totals(plan_reference_counts) == {
        page_limit: _page_total_from_production_pager(
            plan_reference_counts,
            page_limit=page_limit,
        )
        for page_limit in manifest.CATALOG_PAGING_MANIFEST_PAGE_LIMITS
    }


def test_catalog_page_totals_counts_one_terminal_page_for_an_empty_source():
    assert manifest.catalog_page_totals([]) == {
        page_limit: 1
        for page_limit in manifest.CATALOG_PAGING_MANIFEST_PAGE_LIMITS
    }


def test_catalog_paging_manifest_uses_supported_public_schema_and_matches_version():
    source_metadata = _manifest_metadata(plan_reference_counts=[4, 10_001])

    response_payload = manifest.catalog_paging_manifest_for_file_page(
        source_metadata,
        page_limit=250,
    )

    assert response_payload is not None
    assert response_payload["file_pages_total"] == 2
    assert response_payload["page_limit"] == 250
    assert response_payload["plan_reference_limit"] == 10_000
    assert response_payload["source_version"] == "run_example"
    assert response_payload["scope_revision"] == 1
    assert response_payload["manifest_revision"] == 1
    assert len(response_payload["manifest_digest"]) == 64
    assert "source_discovery_run_id" not in response_payload


def test_catalog_paging_manifest_omits_stale_or_unsupported_payloads():
    source_metadata = _manifest_metadata()
    cache_payload = source_metadata[manifest.CATALOG_PAGING_MANIFEST_METADATA_KEY]
    assert isinstance(cache_payload, dict)

    cache_payload["source_version"] = "run_stale"
    assert (
        manifest.catalog_paging_manifest_for_file_page(
            source_metadata,
            page_limit=100,
        )
        is None
    )

    cache_payload["source_version"] = "run_example"
    cache_payload["manifest_revision"] = "1"
    assert (
        manifest.catalog_paging_manifest_for_file_page(
            source_metadata,
            page_limit=100,
        )
        is None
    )

    cache_payload["manifest_revision"] = 1
    cache_payload["scope_revision"] = 0
    assert (
        manifest.catalog_paging_manifest_for_file_page(
            source_metadata,
            page_limit=100,
        )
        is None
    )


def test_catalog_paging_manifest_allows_its_digest_to_be_omitted():
    source_metadata = _manifest_metadata()
    cache_payload = source_metadata[manifest.CATALOG_PAGING_MANIFEST_METADATA_KEY]
    assert isinstance(cache_payload, dict)
    cache_payload.pop("manifest_digest")

    response_payload = manifest.catalog_paging_manifest_for_file_page(
        source_metadata,
        page_limit=500,
    )

    assert response_payload is not None
    assert "manifest_digest" not in response_payload


def test_plan_reference_count_query_uses_native_postgresql_json_operators():
    statement = db.select(
        manifest_refresh._plan_reference_count_expression(MRFFile.__table__).label(
            "plan_reference_count"
        )
    )

    compiled_sql = str(statement.compile(dialect=postgresql.dialect()))

    assert "json_typeof" in compiled_sql
    assert "json_array_length" in compiled_sql
    assert "#>> '{}'" in compiled_sql
    assert "JSONB" not in compiled_sql.upper()


class _ScalarStreamStatement:
    """Minimal ordered-statement double for manifest-stream behavior tests."""

    def __init__(self, file_rows, *, pause_after_rows: int | None = None):
        self.file_rows = file_rows
        self.pause_after_rows = pause_after_rows
        self.execution_options_dict = {}

    def select_from(self, *_args):
        return self

    def where(self, *_args):
        return self

    def order_by(self, *_args):
        return self

    def execution_options(self, **execution_options):
        self.execution_options_dict = execution_options
        return self

    async def iterate(self):
        for row_index, file_row in enumerate(self.file_rows):
            yield file_row
            if self.pause_after_rows == row_index + 1:
                await asyncio.sleep(1)


@pytest.mark.asyncio
async def test_manifest_stream_skips_only_oversized_source_and_continues(monkeypatch):
    statement = _ScalarStreamStatement(
        [
            {
                "source_id": "source_oversized",
                "mrf_file_id": "file_001",
                "plan_reference_count": 1,
            },
            {
                "source_id": "source_oversized",
                "mrf_file_id": "file_002",
                "plan_reference_count": 1,
            },
            {
                "source_id": "source_bounded",
                "mrf_file_id": "file_003",
                "plan_reference_count": 0,
            },
        ]
    )
    monkeypatch.setattr(manifest_refresh.db, "select", lambda *_columns: statement)

    stream_result = await manifest_refresh._stream_source_manifests(
        ("source_bounded", "source_oversized"),
        max_file_rows_per_source=1,
        deadline_at=None,
    )

    assert stream_result.is_stream_complete is True
    assert stream_result.oversized_source_count == 1
    assert stream_result.omitted_source_ids == {"source_oversized"}
    assert set(stream_result.manifest_by_source_id) == {"source_bounded"}
    assert statement.execution_options_dict == {
        "yield_per": manifest_refresh.CATALOG_PAGING_MANIFEST_STREAM_BATCH_SIZE
    }


@pytest.mark.asyncio
async def test_manifest_stream_keeps_completed_sources_when_deadline_expires(monkeypatch):
    statement = _ScalarStreamStatement(
        [
            {
                "source_id": "source_complete",
                "mrf_file_id": "file_001",
                "plan_reference_count": 1,
            },
            {
                "source_id": "source_active",
                "mrf_file_id": "file_002",
                "plan_reference_count": 1,
            },
        ],
        pause_after_rows=2,
    )
    monkeypatch.setattr(manifest_refresh.db, "select", lambda *_columns: statement)

    stream_result = await manifest_refresh._stream_source_manifests(
        ("source_active", "source_complete"),
        max_file_rows_per_source=10,
        deadline_at=asyncio.get_running_loop().time() + 0.01,
    )

    assert stream_result.is_stream_complete is False
    assert stream_result.has_scan_timed_out is True
    assert set(stream_result.manifest_by_source_id) == {"source_complete"}


class _ManifestUpdateCapture:
    """Capture the finalizer's bulk metadata update parameters."""

    def __init__(self):
        self.execute_calls = []

    async def execute(self, statement, parameters):
        self.execute_calls.append((statement, parameters))


@asynccontextmanager
async def _manifest_update_session(update_capture):
    yield update_capture


async def _run_finalizer(
    monkeypatch,
    stream_result,
    source_metadata_by_id,
    source_ids,
) -> tuple[int, _ManifestUpdateCapture]:
    update_capture = _ManifestUpdateCapture()

    async def fake_stream(*_args, **_kwargs):
        return stream_result

    async def fake_source_metadata(_source_ids):
        return source_metadata_by_id

    monkeypatch.setattr(manifest_refresh, "_stream_source_manifests", fake_stream)
    monkeypatch.setattr(manifest_refresh, "_source_metadata_by_id", fake_source_metadata)
    monkeypatch.setattr(
        manifest_refresh.db,
        "session",
        lambda: _manifest_update_session(update_capture),
    )
    published_count = await manifest_refresh._refresh_catalog_paging_manifests(
        source_ids,
        source_version="run_example",
        max_file_rows_per_source=10,
        deadline_at=None,
    )
    return published_count, update_capture


def _parameters_by_source_id(update_capture) -> dict[str, object]:
    [(_statement, update_parameters)] = update_capture.execute_calls
    return {
        parameters["manifest_source_id"]: parameters
        for parameters in update_parameters
    }


@pytest.mark.asyncio
async def test_finalizer_publishes_zero_file_source_as_one_terminal_page(monkeypatch):
    stream_result = manifest_refresh._ManifestStreamResult(
        manifest_by_source_id={},
        is_stream_complete=True,
        has_scan_timed_out=False,
        oversized_source_count=0,
        omitted_source_ids=set(),
    )
    published_count, update_capture = await _run_finalizer(
        monkeypatch,
        stream_result,
        {"source_empty": {"discovery_run_id": "run_example"}},
        ("source_empty",),
    )

    assert published_count == 1
    parameters = _parameters_by_source_id(update_capture)["source_empty"]
    cache_payload = parameters["manifest_metadata_json"][
        manifest.CATALOG_PAGING_MANIFEST_METADATA_KEY
    ]
    assert cache_payload["file_count"] == 0
    assert cache_payload["page_totals"] == {"100": 1, "250": 1, "500": 1}


@pytest.mark.asyncio
async def test_finalizer_skips_oversized_source_and_publishes_bounded_sources(monkeypatch):
    bounded_accumulator = manifest._SourceManifestAccumulator.create("source_bounded")
    bounded_accumulator.add_file("file_001", 1)
    stream_result = manifest_refresh._ManifestStreamResult(
        manifest_by_source_id={"source_bounded": bounded_accumulator},
        is_stream_complete=True,
        has_scan_timed_out=False,
        oversized_source_count=1,
        omitted_source_ids={"source_oversized"},
    )
    published_count, update_capture = await _run_finalizer(
        monkeypatch,
        stream_result,
        {
            "source_oversized": {"discovery_run_id": "run_example"},
            "source_bounded": {"discovery_run_id": "run_example"},
            "source_empty": {"discovery_run_id": "run_example"},
        },
        ("source_oversized", "source_bounded", "source_empty"),
    )

    assert published_count == 2
    parameters_by_source_id = _parameters_by_source_id(update_capture)
    assert set(parameters_by_source_id) == {"source_bounded", "source_empty"}
    cache_by_source_id = {
        source_id: parameters["manifest_metadata_json"][
            manifest.CATALOG_PAGING_MANIFEST_METADATA_KEY
        ]
        for source_id, parameters in parameters_by_source_id.items()
    }
    assert cache_by_source_id["source_bounded"]["file_count"] == 1
    assert cache_by_source_id["source_empty"]["page_totals"] == {
        "100": 1,
        "250": 1,
        "500": 1,
    }
