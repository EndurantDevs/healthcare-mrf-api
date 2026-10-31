# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded six-target COPY mechanics; no protected publication authority."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_prepared_payload_publication as payloads


def test_fixed_layouts_exclude_generated_copy_fields_but_compare_them():
    root = payloads.ROOTS[1]
    columns, json_columns = payloads._columns(root)
    assert "plan_active" not in columns and "plan_identifier" not in columns
    assert {"plan_active", "plan_identifier"} < set(payloads._columns(root, generated=True)[0])
    assert json_columns == {"payload_json"}
    assert payloads._columns(payloads.storage.WITNESS)[1] == {"raw_payload_json"}
    assert len(payloads._columns(payloads.storage.RELATIONSHIP)[0]) == 16


def test_graph_keyset_keeps_native_integer_order_and_server_byte_bound():
    query = payloads._page(payloads.storage.RELATIONSHIP, '"private"."graph"')
    assert '"parent_ordinal",' in query and '"parent_ordinal" COLLATE' not in query
    assert "$6::integer,$7::integer" in query
    assert "LIMIT $8" in query and "copy_row_bytes<=$9" in query
    assert "CASE WHEN copy_row_bytes<=$9" in query
    assert "12+12" in query
    assert "CURSOR" not in query and "OFFSET" not in query
    assert "COLLATE" not in query and "::boolean OR" not in query
    first = payloads._page(payloads.storage.RELATIONSHIP, '"private"."graph"', first=True)
    assert "LIMIT $2" in first and "copy_row_bytes<=$3" in first
    assert "$4" not in first and ")>(" not in first


@pytest.mark.parametrize("root", [payloads.storage.RESOURCE, payloads.storage.WITNESS])
def test_json_read_is_encoded_for_native_copy_and_full_comparison(root):
    query = payloads._page(root, '"private"."frozen"')
    column = "payload_json" if root == payloads.storage.RESOURCE else "raw_payload_json"
    assert f'"{column}"::text AS "{column}"' in query
    assert "dataset_id=$1" in query


def table_workspace(rows):
    driver = SimpleNamespace(fetch=AsyncMock(side_effect=rows))
    return SimpleNamespace(
        connection=driver,
        session=object(),
        policy=SimpleNamespace(schema="private_input"),
        catalog_schema="accepted_catalog",
        identity={"dataset_id": "dataset"},
        verify_current=AsyncMock(),
    )


@pytest.mark.parametrize("damage", ["row_size", "page_size", "page_count"])
async def test_oversized_first_record_or_page_refuses_before_copy(monkeypatch, damage):
    root = payloads.ROOTS[2]
    row_by_field = {"dataset_id": "dataset", "network_resource_id": "network", "insurance_plan_resource_id": "plan"}
    row_by_field["copy_row_bytes"] = 20
    monkeypatch.setattr(payloads, "COPY_MAX_BYTES", 50)
    monkeypatch.setattr(payloads, "COPY_MAX_ROWS", 2)
    rows = [row_by_field]
    if damage == "row_size":
        row_by_field["copy_row_bytes"] = 30
    else:
        rows = [dict(row_by_field)] * (3 if damage == "page_count" else 2)
    workspace = table_workspace([rows])
    copy = AsyncMock()
    monkeypatch.setattr(payloads, "copy_identity_rows_native", copy)
    with pytest.raises(ValueError, match="copy_page_unbounded"):
        await payloads._copy_table(workspace, root, {"name": "prepared_network"})
    copy.assert_not_awaited()


async def test_copy_uses_original_publisher_and_native_coordinates(monkeypatch):
    root = payloads.ROOTS[2]
    row_by_field = {
        "dataset_id": "dataset",
        "network_resource_id": "network",
        "insurance_plan_resource_id": "plan",
        "copy_row_bytes": 50,
    }
    workspace = table_workspace([[row_by_field], []])
    copy = AsyncMock(return_value=1)
    monkeypatch.setattr(payloads, "copy_identity_rows_native", copy)
    assert await payloads._copy_table(workspace, root, {"name": "prepared_network"}) == 1
    copy.assert_awaited_once_with(
        workspace.connection,
        workspace.session,
        "accepted_catalog",
        root,
        payloads._columns(root)[0],
        [("dataset", "network", "plan")],
    )
    assert workspace.connection.fetch.await_args_list[1].args[2:5] == ("dataset", "network", "plan")


async def test_same_key_cannot_loop_after_partial_copy(monkeypatch):
    root = payloads.ROOTS[2]
    row_by_field = {
        "dataset_id": "dataset",
        "network_resource_id": "network",
        "insurance_plan_resource_id": "plan",
        "copy_row_bytes": 50,
    }
    workspace = table_workspace([[row_by_field], [row_by_field]])
    monkeypatch.setattr(payloads, "copy_identity_rows_native", AsyncMock(return_value=1))
    with pytest.raises(ValueError, match="copy_key_not_advanced"):
        await payloads._copy_table(workspace, root, {"name": "prepared_network"})


async def test_keyset_progress_does_not_reimpose_python_order_over_native_collation(monkeypatch):
    root = payloads.ROOTS[2]
    rows = [
        {
            "dataset_id": "dataset",
            "network_resource_id": key,
            "insurance_plan_resource_id": "plan",
            "copy_row_bytes": 50,
        }
        for key in ("a", "B")
    ]
    workspace = table_workspace([[rows[0]], [rows[1]], []])
    monkeypatch.setattr(payloads, "copy_identity_rows_native", AsyncMock(return_value=1))
    assert "B" < "a"
    assert await payloads._copy_table(workspace, root, {"name": "prepared_network"}) == 2


async def test_untyped_scope_refuses_before_native_observation():
    with pytest.raises(ValueError, match="scope_required"):
        await payloads.copy_prepared_cms_payloads(SimpleNamespace(), SimpleNamespace())
