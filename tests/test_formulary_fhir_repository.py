# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from process.formulary_fhir import repository as repository_module
from process.formulary_fhir.repository import FHIRFormularyRepository


def _coverage_row(
    alias: str,
    *,
    dataset_alias_version_id: str | None,
) -> dict:
    return {
        "public_id": "fhir_abcdefghijklmnopqrstuvwxyz",
        "canonical_identity": "https://fhir.example.invalid/r4/List/list-a",
        "content_hash": "c" * 64,
        "metadata_json": {
            "source_plan_identifiers": ["CURRENT-ALIAS"],
        },
        "alias_id": f"alias-{alias}",
        "dataset_alias_version_id": dataset_alias_version_id,
        "source_plan_identifier": alias,
        "expected_count": 1 if dataset_alias_version_id else None,
        "membership_count": 1 if dataset_alias_version_id else None,
        "membership_hash": "m" * 64 if dataset_alias_version_id else None,
    }


@pytest.mark.asyncio
async def test_verification_ignores_removed_historical_aliases(monkeypatch):
    rows = [
        _coverage_row(
            "CURRENT-ALIAS",
            dataset_alias_version_id="alias-version-current",
        ),
        _coverage_row("REMOVED-ALIAS", dataset_alias_version_id=None),
    ]
    all_rows = AsyncMock(return_value=rows)
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(repository_module.db, "all", all_rows)
    monkeypatch.setattr(repository_module.db, "status", status)

    summary = await FHIRFormularyRepository().verify_dataset("candidate")

    assert summary["list_count"] == 1
    assert summary["alias_count"] == 1
    assert summary["medication_membership_count"] == 1
    assert "fhir_formulary_coverage_plan_version" in all_rows.await_args.args[0]
    assert status.await_count == 1


@pytest.mark.asyncio
async def test_verification_rejects_missing_current_alias(monkeypatch):
    rows = [_coverage_row("CURRENT-ALIAS", dataset_alias_version_id=None)]
    monkeypatch.setattr(
        repository_module.db,
        "all",
        AsyncMock(return_value=rows),
    )
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(repository_module.db, "status", status)

    with pytest.raises(RuntimeError, match="List-to-alias coverage"):
        await FHIRFormularyRepository().verify_dataset("candidate")

    status.assert_not_awaited()
