# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from api import mrf_discovery_catalog as catalog
from api import mrf_discovery_catalog_manifest as paging_manifest


def _cached_source_metadata(
    *,
    source_version: str = "run_example",
    plan_count: int | None = None,
) -> dict[str, object]:
    plan_reference_counts = [] if plan_count is None else [plan_count]
    page_totals_by_limit = paging_manifest.catalog_page_totals(
        plan_reference_counts
    )
    return {
        "discovery_run_id": source_version,
        paging_manifest.CATALOG_PAGING_MANIFEST_METADATA_KEY: {
            "contract": paging_manifest.CATALOG_PAGING_MANIFEST_CONTRACT,
            "source_version": source_version,
            "scope_revision": paging_manifest.CATALOG_PAGING_MANIFEST_SCOPE_REVISION,
            "manifest_revision": paging_manifest.CATALOG_PAGING_MANIFEST_REVISION,
            "plan_reference_limit": 10_000,
            "file_count": len(plan_reference_counts),
            "page_totals": {
                str(page_limit): page_total
                for page_limit, page_total in page_totals_by_limit.items()
            },
        },
    }


def _file_row(source_metadata: dict[str, object]) -> dict[str, object]:
    return {
        "mrf_file_id": "file_001",
        "source_id": "source_example",
        "file_type": "in-network",
        "url": "https://example.test/file_001.json.gz",
        "plan_ids": ["plan-001"],
        "plan_names": ["Example Plan"],
        "market_types": ["group"],
        "metadata_json": {},
        "source_display_name": "Example Payer",
        "source_metadata_json": source_metadata,
    }


@pytest.mark.asyncio
async def test_file_page_exposes_only_a_matching_cached_paging_manifest(monkeypatch):
    source_metadata = _cached_source_metadata(plan_count=1)
    file_row = _file_row(source_metadata)

    async def fake_file_rows(_statement, **_paging_options):
        return [file_row]

    monkeypatch.setattr(
        catalog,
        "_bounded_stream_file_query_rows",
        fake_file_rows,
    )

    matching_page = await catalog.list_discovery_source_files_page(
        "source_example",
        limit=250,
    )
    source_metadata["discovery_run_id"] = "run_stale"
    stale_page = await catalog.list_discovery_source_files_page(
        "source_example",
        limit=250,
    )

    assert matching_page["paging_manifest"]["file_pages_total"] == 1
    assert matching_page["paging_manifest"]["source_version"] == "run_example"
    assert "paging_manifest" not in stale_page


@pytest.mark.asyncio
async def test_empty_file_page_reads_cached_terminal_total_without_counting(monkeypatch):
    source_metadata = _cached_source_metadata()
    source_statements = []

    async def fake_file_rows(_statement, **_paging_options):
        return []

    async def fake_first(statement):
        source_statements.append(statement)
        return {"metadata_json": source_metadata}

    monkeypatch.setattr(
        catalog,
        "_bounded_stream_file_query_rows",
        fake_file_rows,
    )
    monkeypatch.setattr(catalog.db, "first", fake_first)

    page = await catalog.list_discovery_source_files_page(
        "source_example",
        limit=100,
    )

    assert page["items"] == []
    assert page["next_cursor"] is None
    assert page["paging_manifest"]["file_pages_total"] == 1
    assert len(source_statements) == 1
    assert "count(" not in str(source_statements[0]).lower()


def test_source_item_hides_private_paging_manifest_metadata():
    source_metadata = _cached_source_metadata()
    source_item = catalog._source_item(
        {
            "source_id": "source_example",
            "metadata_json": source_metadata,
            "payer_aliases": [],
        }
    )

    assert paging_manifest.CATALOG_PAGING_MANIFEST_METADATA_KEY not in source_item[
        "metadata"
    ]
    assert paging_manifest.CATALOG_PAGING_MANIFEST_METADATA_KEY in source_metadata
