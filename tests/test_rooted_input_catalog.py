# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog component checks reject unsafe transport and unreviewed guard definitions."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_rooted_graph_input_fence as fence

SCHEMA = "snapshot_schema"


def test_sql_consumers_keep_the_same_fence_projection():
    query = fence.input_fence_query(SCHEMA, "$1", "$2::oid[]", include_bodies=True)
    projected = fence.root_input_fence_sql(SCHEMA, "$1", "$2::jsonb")
    assert query == projected
    assert "to_jsonb(h)" in query and "to_jsonb(p)" in query
    assert "candidate_storage_json" in query and "published_at" in query
    assert all(name not in query for name in ("pg_proc", "pg_trigger", "pg_relation_filenode", "::oid"))
    protected = fence.root_input_fence_sql(SCHEMA, "$1", "$2::jsonb", variant="rooted_combined")
    assert '"snapshot_schema"."provider_directory_rooted_graph_dataset"' in protected
    assert "WHERE h.dataset_id=$1" in protected
    with pytest.raises(ValueError, match="rooted_variant_invalid"):
        fence.root_input_fence_sql(SCHEMA, "$1", "$2::jsonb", variant="unknown")


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", [False, True])
async def test_capture_preserves_the_complete_logical_predecessor(encoded):
    document_by_field = {
        "dataset_id": "edition",
        "variant": "uhc_flex_practitioner",
        "header": {"dataset_hash": "a" * 64},
        "parent": {"dataset_hash": "b" * 64},
    }
    database = SimpleNamespace(
        scalar=AsyncMock(return_value=json.dumps(document_by_field) if encoded else document_by_field)
    )
    captured = await fence.capture_input_fence(database, SCHEMA, {}, "edition")
    assert json.loads(captured) == document_by_field
    assert captured == json.dumps(document_by_field, sort_keys=True, separators=(",", ":"))
    database.scalar.assert_awaited_once_with(fence.root_input_fence_sql(SCHEMA, ":dataset_id"), dataset_id="edition")


@pytest.mark.asyncio
@pytest.mark.parametrize("document", [None, [], False, "metadata"])
async def test_capture_rejects_nonmapping_predecessor_data(document):
    database = SimpleNamespace(scalar=AsyncMock(return_value=json.dumps(document)))
    with pytest.raises(RuntimeError, match="predecessor_invalid"):
        await fence.capture_input_fence(database, SCHEMA, {}, "edition")
