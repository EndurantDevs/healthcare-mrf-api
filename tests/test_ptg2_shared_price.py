from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_price as shared_price
from process.ptg_parts.ptg2_serving_binary_v3 import (
    decode_price_memberships,
    encode_price_memberships,
)


@pytest.mark.asyncio
async def test_strict_price_stage_deduplicates_identical_cross_file_atoms(monkeypatch):
    scalar = AsyncMock(side_effect=[3, 2, False])
    status = AsyncMock()
    monkeypatch.setattr(shared_price.db, "scalar", scalar)
    monkeypatch.setattr(shared_price.db, "status", status)

    metrics = await shared_price._normalize_strict_v3_price_atom_stage(
        schema_name="mrf",
        price_atom_table="price_atoms",
    )

    assert metrics == {
        "rows_before": 3,
        "rows_after": 2,
        "duplicate_rows_removed": 1,
        "conflicting_ids": 0,
    }
    scalar_sql = [call.args[0] for call in scalar.await_args_list]
    assert "IS DISTINCT FROM" in scalar_sql[2]
    status_sql = [call.args[0] for call in status.await_args_list]
    assert any("SELECT DISTINCT ON (price_atom_global_id_128)" in sql for sql in status_sql)
    assert any("CREATE UNIQUE INDEX" in sql for sql in status_sql)
    assert any("RENAME TO \"price_atoms\"" in sql for sql in status_sql)


@pytest.mark.asyncio
async def test_strict_price_stage_rejects_same_id_with_different_payload(monkeypatch):
    scalar = AsyncMock(side_effect=[2, 1, True])
    status = AsyncMock()
    monkeypatch.setattr(shared_price.db, "scalar", scalar)
    monkeypatch.setattr(shared_price.db, "status", status)

    with pytest.raises(RuntimeError, match="conflicting source payloads"):
        await shared_price._normalize_strict_v3_price_atom_stage(
            schema_name="mrf",
            price_atom_table="price_atoms",
        )

    assert not any(
        "RENAME TO \"price_atoms\"" in call.args[0]
        for call in status.await_args_list
    )


@pytest.mark.asyncio
async def test_strict_price_stage_rejects_empty_dictionary(monkeypatch):
    monkeypatch.setattr(shared_price.db, "scalar", AsyncMock(side_effect=[0, 0]))
    monkeypatch.setattr(shared_price.db, "status", AsyncMock())

    with pytest.raises(RuntimeError, match="empty price atom dictionary"):
        await shared_price._normalize_strict_v3_price_atom_stage(
            schema_name="mrf",
            price_atom_table="price_atoms",
        )


def test_strict_price_memberships_preserve_duplicate_source_occurrences():
    sql = shared_price._v3_price_membership_sql(
        qualified_price_set_atom_table="mrf.price_set_atoms",
        qualified_price_key_map="mrf.price_keys",
        qualified_atom_key_map="mrf.atom_keys",
    )
    assert "SELECT DISTINCT" not in sql

    payload = encode_price_memberships(((3, (1, 1, 257)),), 24)
    assert decode_price_memberships(payload) == {3: (1, 1, 257)}


@pytest.mark.asyncio
async def test_price_keys_use_exact_minimum_rate_order(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(shared_price.db, "status", status)
    monkeypatch.setattr(
        shared_price,
        "_validate_v3_dense_map",
        AsyncMock(return_value={"row_count": 2, "minimum_key": 0, "maximum_key": 1}),
    )

    await shared_price._create_v3_price_key_stage(
        schema_name="mrf",
        price_atom_table="price_atoms",
        price_set_atom_table="price_set_atoms",
        stage_table="price_keys",
    )

    sql = "\n".join(call.args[0] for call in status.await_args_list)
    assert "MIN(NULLIF(BTRIM(atom.negotiated_rate), '')::numeric)" in sql
    assert "ORDER BY minimum_negotiated_rate ASC NULLS LAST" in sql
    assert "price_set_global_id_128" in sql
    assert "CREATE UNIQUE INDEX" in sql
    assert "(price_key)" in sql


@pytest.mark.asyncio
async def test_price_prepare_failure_removes_partial_key_stages(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(shared_price.db, "status", status)
    monkeypatch.setattr(
        shared_price,
        "_normalize_strict_v3_price_atom_stage",
        AsyncMock(side_effect=RuntimeError("broken stage")),
    )

    with pytest.raises(RuntimeError, match="broken stage"):
        await shared_price.prepare_shared_price_artifacts(
            schema_name="mrf",
            manifest_stage_table="manifest_stage",
        )

    cleanup_sql = status.await_args_list[-1].args[0]
    assert "v3_price_key" in cleanup_sql
    assert "v3_atom_key" in cleanup_sql
    assert "v3_price_attr" in cleanup_sql
