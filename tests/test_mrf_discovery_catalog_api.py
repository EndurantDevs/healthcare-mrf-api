# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import types

import pytest
from sanic.exceptions import BadRequest

from api import control
from api import mrf_discovery_catalog as catalog


def _request(**kwargs):
    kwargs.setdefault("headers", {"Authorization": "Bearer secret"})
    kwargs.setdefault("args", {})
    return types.SimpleNamespace(**kwargs)


def test_page_limit_validates_and_clamps_values():
    assert catalog.page_limit(None, default=100, maximum=250) == 100
    assert catalog.page_limit("500", default=100, maximum=250) == 250
    with pytest.raises(ValueError, match="greater than zero"):
        catalog.page_limit("0", default=100, maximum=250)
    with pytest.raises(ValueError, match="integer"):
        catalog.page_limit("many", default=100, maximum=250)


def _dense_file_row(file_id: str, plan_count: int) -> dict[str, object]:
    """Build one stored file row with a controllable plan-reference count."""

    return {
        "mrf_file_id": file_id,
        "source_id": "source_example",
        "file_type": "in-network",
        "url": f"https://example.test/{file_id}.json.gz",
        "plan_ids": [f"plan-{index}" for index in range(plan_count)],
        "plan_names": [f"Example Plan {index}" for index in range(plan_count)],
        "market_types": ["group"] * plan_count,
        "metadata_json": {},
        "source_display_name": "Example Payer",
        "source_metadata_json": {},
    }


def test_file_page_splits_one_plan_dense_file_with_composite_cursor():
    """Split one file's plan identities without skipping or duplicating them."""

    dense_file_rows = [_dense_file_row("file_dense", 5)]

    first_items, first_cursor = catalog._bounded_file_items(
        dense_file_rows,
        limit=10,
        cursor_plan_offset=0,
        plan_reference_limit=3,
    )
    second_items, second_cursor = catalog._bounded_file_items(
        dense_file_rows,
        limit=10,
        cursor_plan_offset=3,
        plan_reference_limit=3,
    )

    assert [plan["plan_id"] for plan in first_items[0]["plan_info"]] == [
        "plan-0",
        "plan-1",
        "plan-2",
    ]
    assert first_items[0]["plan_chunk_offset"] == 0
    assert first_items[0]["plan_chunk_total"] == 5
    assert "plan_ids" not in first_items[0]
    assert "plan_names" not in first_items[0]
    assert "market_types" not in first_items[0]
    assert first_cursor == "file_dense~plan~3"
    assert [plan["plan_id"] for plan in second_items[0]["plan_info"]] == [
        "plan-3",
        "plan-4",
    ]
    assert second_items[0]["plan_chunk_offset"] == 3
    assert second_cursor is None


def test_file_page_plan_budget_continues_inside_next_file():
    """Use the remaining budget before continuing within the next file."""

    rows = [
        _dense_file_row("file_a", 2),
        _dense_file_row("file_b", 3),
    ]

    page_items, next_cursor = catalog._bounded_file_items(
        rows,
        limit=10,
        cursor_plan_offset=0,
        plan_reference_limit=3,
    )

    assert [len(item["plan_info"]) for item in page_items] == [2, 1]
    assert next_cursor == "file_b~plan~1"


def test_file_page_preserves_ambiguous_plan_identity_hints_across_chunks():
    """Annotate split plans using ambiguity from the complete stored file."""

    dense_row = _dense_file_row("file_dense", 4)
    dense_row["metadata_json"] = {
        "plan_info": [
            {
                "plan_id": "123456789",
                "plan_id_type": "ein",
                "plan_market_type": "group",
                "plan_name": f"Example Plan {index}",
            }
            for index in range(4)
        ]
    }
    first_items, first_cursor = catalog._bounded_file_items(
        [dense_row],
        limit=10,
        cursor_plan_offset=0,
        plan_reference_limit=2,
    )
    second_items, second_cursor = catalog._bounded_file_items(
        [dense_row],
        limit=10,
        cursor_plan_offset=2,
        plan_reference_limit=2,
    )

    first_hints = [
        plan["plan_identity_hint"] for plan in first_items[0]["plan_info"]
    ]
    second_hints = [
        plan["plan_identity_hint"] for plan in second_items[0]["plan_info"]
    ]
    assert first_hints == ["plan_name:Example Plan 0", "plan_name:Example Plan 1"]
    assert second_hints == ["plan_name:Example Plan 2", "plan_name:Example Plan 3"]
    assert first_cursor == "file_dense~plan~2"
    assert second_cursor is None


def test_file_plan_cursor_accepts_legacy_and_rejects_invalid_offsets():
    """Keep legacy cursors valid while rejecting malformed plan offsets."""

    assert catalog._parse_file_cursor("file_001") == ("file_001", 0)
    assert catalog._parse_file_cursor("file_001~plan~4") == ("file_001", 4)
    with pytest.raises(ValueError, match="integer"):
        catalog._parse_file_cursor("file_001~plan~many")
    with pytest.raises(ValueError, match="greater than zero"):
        catalog._parse_file_cursor("file_001~plan~0")


@pytest.mark.asyncio
async def test_file_page_resumes_composite_plan_cursor(monkeypatch):
    """Resume the same file at the next plan slice through the public reader."""

    dense_row = _dense_file_row("file_dense", 5)

    async def fake_all(_statement):
        return [dense_row]

    monkeypatch.setattr(catalog.db, "all", fake_all)
    monkeypatch.setattr(catalog, "MAX_FILE_PAGE_PLAN_REFERENCES", 3)

    first_page = await catalog.list_discovery_source_files_page(
        "source_example",
        limit=10,
    )
    second_page = await catalog.list_discovery_source_files_page(
        "source_example",
        cursor=first_page["next_cursor"],
        limit=10,
    )

    assert [
        plan["plan_id"] for plan in first_page["items"][0]["plan_info"]
    ] == ["plan-0", "plan-1", "plan-2"]
    assert [
        plan["plan_id"] for plan in second_page["items"][0]["plan_info"]
    ] == ["plan-3", "plan-4"]
    assert second_page["next_cursor"] is None


@pytest.mark.asyncio
async def test_source_page_is_cursor_bounded_and_merges_payer_identity(monkeypatch):
    query_calls = []

    async def fake_all(statement):
        query_calls.append(statement)
        return [
            {
                "source_id": "source_001",
                "source_key": "example-one",
                "display_name": "Example One",
                "index_url": "https://example.test/one-index.json",
                "metadata_json": {"aliases": ["Example Primary"]},
                "payer_name": "Example Payer",
                "payer_aliases": ["Example Alias"],
                "payer_states": ["CA"],
                "payer_eins": [],
                "payer_metadata_json": {"benefit_lines": ["medical"]},
            },
            {
                "source_id": "source_002",
                "source_key": "example-two",
                "display_name": "Example Two",
                "index_url": "https://example.test/two-index.json",
                "metadata_json": {},
                "payer_aliases": [],
            },
            {
                "source_id": "source_003",
                "source_key": "example-three",
                "display_name": "Example Three",
                "index_url": "https://example.test/three-index.json",
                "metadata_json": {},
                "payer_aliases": [],
            },
        ]

    monkeypatch.setattr(catalog.db, "all", fake_all)

    page = await catalog.list_discovery_sources_page(
        cursor="source_000",
        limit=2,
        query="example",
    )

    assert len(query_calls) == 1
    assert [source_item["source_id"] for source_item in page["items"]] == [
        "source_001",
        "source_002",
    ]
    assert page["next_cursor"] == "source_002"
    assert page["items"][0]["metadata"]["aliases"] == [
        "Example Primary",
        "Example Alias",
    ]
    assert (
        page["items"][0]["metadata"]["engine_source_catalog_id"]
        == "source_001"
    )
    assert page["items"][0]["payer"]["canonical_name"] == "Example Payer"


@pytest.mark.asyncio
async def test_source_page_filters_exact_discovery_run(monkeypatch):
    source_statements = []

    async def capture_statement(source_statement):
        source_statements.append(source_statement)
        return []

    monkeypatch.setattr(catalog.db, "all", capture_statement)

    await catalog.list_discovery_sources_page(
        limit=2,
        discovery_run_id="run_example",
    )

    assert len(source_statements) == 1
    compiled_query = source_statements[0].compile()
    compiled_value_set = set(compiled_query.params.values())
    assert "discovery_run_id" in compiled_value_set
    assert "run_example" in compiled_value_set


@pytest.mark.asyncio
async def test_file_page_normalizes_label_only_plan_identity(monkeypatch):
    async def fake_all(_statement):
        return [
            {
                "mrf_file_id": "file_001",
                "source_id": "source_001",
                "file_type": "in-network",
                "url": "https://example.test/rates.json.gz",
                "canonical_url": "https://example.test/rates.json.gz",
                "from_index_url": "https://example.test/index.json",
                "description": "Example plan rates",
                "network_name": "Example Network",
                "plan_ids": [],
                "plan_names": ["Example Group Plan"],
                "market_types": ["group"],
                "metadata_json": {},
                "source_display_name": "Example Payer",
                "source_index_url": "https://example.test/index.json",
                "source_metadata_json": {},
            }
        ]

    monkeypatch.setattr(catalog.db, "all", fake_all)

    first_page = await catalog.list_discovery_source_files_page(
        "source_001", limit=10
    )
    repeated_page = await catalog.list_discovery_source_files_page(
        "source_001", limit=10
    )

    [file_item] = first_page["items"]
    [repeated_file_item] = repeated_page["items"]
    [plan] = file_item["plan_info"]
    assert file_item["domain"] == "in-network"
    assert file_item["reporting_entity_name"] == "Example Payer"
    assert file_item["source_index_url"] == "https://example.test/index.json"
    assert plan["plan_name"] == "Example Group Plan"
    assert plan["plan_market_type"] == "group"
    assert plan["plan_id_type"] == "source_file_context_hash"
    assert plan["plan_id"] == repeated_file_item["plan_info"][0]["plan_id"]


@pytest.mark.asyncio
async def test_file_page_prefers_stored_plan_metadata(monkeypatch):
    async def fake_all(_statement):
        return [
            {
                "mrf_file_id": "file_001",
                "source_id": "source_001",
                "file_type": "allowed-amounts",
                "url": "https://example.test/allowed.json.gz",
                "metadata_json": {
                    "domain": "allowed-amounts",
                    "reporting_entity_name": "Example Reporting Entity",
                    "plan_info": [
                        {
                            "plan_id": "PLAN-001",
                            "plan_id_type": "group_number",
                            "plan_market_type": "group",
                            "plan_name": "Example PPO",
                        }
                    ],
                },
                "source_display_name": "Example Source",
                "source_metadata_json": {},
            }
        ]

    monkeypatch.setattr(catalog.db, "all", fake_all)

    page = await catalog.list_discovery_source_files_page("source_001", limit=10)

    [file_item] = page["items"]
    assert file_item["reporting_entity_name"] == "Example Reporting Entity"
    assert file_item["plan_info"] == [
        {
            "plan_id": "PLAN-001",
            "plan_id_type": "group_number",
            "plan_market_type": "group",
            "plan_name": "Example PPO",
        }
    ]
    assert "plan_info" not in file_item["metadata"]


@pytest.mark.asyncio
async def test_control_source_endpoint_passes_cursor_query_and_limit(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    captured_kwargs_dict = {}

    async def fake_page(**kwargs):
        captured_kwargs_dict.update(kwargs)
        return {"items": [], "next_cursor": None}

    monkeypatch.setattr(control, "list_discovery_sources_page", fake_page)

    result = await control.control_mrf_discovery_sources(
        _request(
            args={
                "cursor": "source_010",
                "limit": "25",
                "q": "Example",
                "run_id": "run_example",
            }
        )
    )

    assert json.loads(result.body) == {"items": [], "next_cursor": None}
    assert captured_kwargs_dict == {
        "cursor": "source_010",
        "limit": 25,
        "query": "Example",
        "discovery_run_id": "run_example",
    }


@pytest.mark.asyncio
async def test_control_file_endpoint_rejects_invalid_limit(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")

    with pytest.raises(BadRequest, match="limit must be an integer"):
        await control.control_mrf_discovery_source_files(
            _request(args={"limit": "all"}),
            "source_001",
        )
