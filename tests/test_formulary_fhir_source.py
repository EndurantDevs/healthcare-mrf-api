# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Tests for exact, redacted FHIR formulary source binding."""

from __future__ import annotations

from copy import deepcopy

import pytest

from process.formulary_fhir.source import load_enabled_source
from process.formulary_fhir.source import require_source_unchanged
from process.formulary_fhir.types import FHIRSourceConfigurationError


RUNTIME_CONFIG = {
    "timeout_seconds": 30,
    "max_attempts": 2,
    "page_size": 50,
    "max_pages": 100,
    "max_total_resources": 5_000,
    "max_response_bytes": 1_048_576,
}


def _source_row() -> dict[str, object]:
    return {
        "source_id": "source-alpha",
        "canonical_base": "https://synthetic.invalid/fhir",
        "enabled": True,
        "runtime_config_json": deepcopy(RUNTIME_CONFIG),
        "metadata_json": {"mode": "manual"},
    }


class _Database:
    def __init__(self, rows: list[dict[str, object] | None]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, dict[str, object]]] = []

    async def first(self, statement: str, **params: object):
        self.calls.append((statement, params))
        return self.rows.pop(0) if self.rows else None


@pytest.mark.asyncio
async def test_load_enabled_source_binds_sql_and_redacts_configuration():
    database = _Database([_source_row()])

    binding = await load_enabled_source("source-alpha", database=database)

    statement, params_by_name = database.calls[0]
    assert ":source_id" in statement
    assert "source-alpha" not in statement
    assert params_by_name == {"source_id": "source-alpha"}
    assert binding.config.canonical_base == "https://synthetic.invalid/fhir"
    assert len(binding.configuration_hash) == 64
    assert "synthetic.invalid" not in repr(binding)
    assert "max_attempts" not in repr(binding)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_by_field",
    [
        None,
        {**_source_row(), "enabled": False},
        {**_source_row(), "runtime_config_json": {"page_size": 50}},
        {**_source_row(), "metadata_json": []},
        {**_source_row(), "source_id": "source-beta"},
    ],
)
async def test_load_enabled_source_sanitizes_invalid_rows(source_by_field):
    database = _Database([source_by_field])

    with pytest.raises(FHIRSourceConfigurationError) as error:
        await load_enabled_source("source-alpha", database=database)

    assert "synthetic.invalid" not in str(error.value)
    assert "source-beta" not in str(error.value)


@pytest.mark.asyncio
async def test_source_configuration_hash_is_canonical_and_unchanged():
    first_row = _source_row()
    reordered_row = _source_row()
    reordered_row["metadata_json"] = {"second": 2, "first": 1}
    first_row["metadata_json"] = {"first": 1, "second": 2}
    database = _Database([first_row, reordered_row])

    binding = await load_enabled_source("source-alpha", database=database)
    await require_source_unchanged(binding, database=database)


@pytest.mark.asyncio
async def test_source_configuration_drift_fails_closed():
    changed_row = _source_row()
    changed_row["metadata_json"] = {"mode": "changed"}
    database = _Database([_source_row(), changed_row])
    binding = await load_enabled_source("source-alpha", database=database)

    with pytest.raises(FHIRSourceConfigurationError, match="changed"):
        await require_source_unchanged(binding, database=database)
