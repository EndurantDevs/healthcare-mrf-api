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
