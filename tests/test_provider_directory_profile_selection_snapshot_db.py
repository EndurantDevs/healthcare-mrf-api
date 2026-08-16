# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused database boundaries for the locked Profile selection snapshot."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile as profile_artifact
from process import provider_directory_profile_selection_snapshot as snapshot
from tests.test_provider_directory_profile_selection_attestation import _catalog
from tests.test_provider_directory_profile_selection_coverage import (
    _dataset_row,
    _source_row,
)


@pytest.mark.asyncio
async def test_snapshot_database_queries_lock_all_selection_relations(monkeypatch):
    status = AsyncMock()
    all_rows = AsyncMock(
        side_effect=[
            [SimpleNamespace(_mapping={"source_id": "a"})],
            [{"dataset_id": "d"}],
        ]
    )
    monkeypatch.setattr(snapshot.db, "status", status)
    monkeypatch.setattr(snapshot.db, "all", all_rows)

    await snapshot._lock_profile_selection_tables()
    assert await snapshot._selection_source_rows() == [{"source_id": "a"}]
    assert await snapshot._selection_dataset_rows() == [{"dataset_id": "d"}]
    assert status.await_count == 4
    advisory_call = status.await_args_list[0]
    assert "pg_advisory_xact_lock" in advisory_call.args[0]
    assert advisory_call.kwargs["lock_identity"].endswith(
        "pdfhir_1ceb7c0986c320b7eb924881"
    )
    relation_lock_sql = status.await_args_list[-1].args[0]
    assert (
        relation_lock_sql.index("provider_directory_dataset_insurance_plan")
        < relation_lock_sql.index("provider_directory_dataset_network_plan")
        < relation_lock_sql.index("provider_directory_dataset_affiliation_organization")
    )
    assert all_rows.await_count == 2


@pytest.mark.asyncio
async def test_snapshot_current_selection_honors_optional_lock(monkeypatch):
    lock = AsyncMock()
    monkeypatch.setattr(snapshot, "_lock_profile_selection_tables", lock)
    monkeypatch.setattr(
        snapshot,
        "_selection_source_rows",
        AsyncMock(return_value=[_source_row(), *_variant_registry_rows()]),
    )
    monkeypatch.setattr(
        snapshot,
        "_selection_dataset_rows",
        AsyncMock(return_value=[_dataset_row()]),
    )
    unlocked = await snapshot._compute_current_selection(
        _catalog(),
        node_id="dev-node",
        lock_selection=False,
    )
    locked = await snapshot._compute_current_selection(
        _catalog(),
        node_id="dev-node",
        lock_selection=True,
    )
    assert unlocked == locked
    lock.assert_awaited_once_with()


def _variant_registry_rows() -> list[dict[str, object]]:
    return [
        {
            "source_id": source_id,
            "endpoint_id": endpoint_id,
            "canonical_api_base": "https://synthetic.invalid/R4",
            "org_name": "Synthetic dataset variant",
            "plan_name": None,
        }
        for source_id, endpoint_id in (
            profile_artifact.configured_dataset_scoped_profile_endpoints()
        )
    ]
