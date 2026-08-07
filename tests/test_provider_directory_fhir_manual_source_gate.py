# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Dormancy proof for manually admitted Provider Directory sources."""

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock, Mock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _source_record(source_id: str, *, manual_only: bool) -> dict[str, object]:
    return {
        "source_id": source_id,
        "api_base": f"https://{source_id}.example.test/fhir",
        "canonical_api_base": f"https://{source_id}.example.test/fhir",
        "auth_type": "none",
        "last_validated_status": "valid",
        "metadata_json": {
            "provider_directory_manual_only": manual_only,
        },
    }


def test_manual_source_is_excluded_before_probe_transport():
    manual_source = _source_record("manual-source", manual_only=True)
    regular_source = _source_record("regular-source", manual_only=False)

    assert importer._source_rows_allowed_for_probe(
        [manual_source, regular_source]
    ) == [regular_source]


def test_exact_source_id_cannot_bypass_manual_source_gate():
    manual_source = _source_record("manual-source", manual_only=True)

    selected_sources, metrics_by_name = importer._select_resource_import_sources(
        [manual_source],
        valid_source_ids={"manual-source"},
        open_only=True,
        include_auth_required=False,
        checkpoint_retry_source_ids={"manual-source"},
    )

    assert selected_sources == []
    assert metrics_by_name["source_import_skipped_manual_only"] == 1
    assert metrics_by_name["source_import_sources_selected"] == 0


def test_regular_source_selection_behavior_is_unchanged():
    regular_source = _source_record("regular-source", manual_only=False)

    selected_sources, metrics_by_name = importer._select_resource_import_sources(
        [regular_source],
        valid_source_ids={"regular-source"},
        open_only=True,
        include_auth_required=False,
    )

    assert selected_sources == [regular_source]
    assert metrics_by_name["source_import_skipped_manual_only"] == 0
    assert metrics_by_name["source_import_sources_selected"] == 1


@pytest.mark.asyncio
async def test_census_request_rejects_before_database_or_transport(monkeypatch):
    cancellation_mock = AsyncMock()
    database_mock = AsyncMock()
    table_setup_mock = AsyncMock()
    catalog_mock = Mock()
    probe_mock = AsyncMock()
    import_mock = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_raise_if_resource_import_cancelled",
        cancellation_mock,
    )
    monkeypatch.setattr(importer, "ensure_database", database_mock)
    monkeypatch.setattr(
        importer,
        "_ensure_provider_directory_tables",
        table_setup_mock,
    )
    monkeypatch.setattr(importer, "_resolve_seed_db", catalog_mock)
    monkeypatch.setattr(importer, "_run_source_probe_batch", probe_mock)
    monkeypatch.setattr(importer, "_import_resources", import_mock)

    with pytest.raises(RuntimeError, match="census_not_activated"):
        await importer.process_provider_directory_fhir_data(
            {"context": {}},
            {
                "provider_directory_acquisition_strategy": (
                    "cutoff-bounded-current-version-census"
                ),
                "provider_directory_census_cutoff": (
                    "2026-08-01T12:00:00.000000Z"
                ),
                "source_ids": ["manual-source"],
                "resources": ["Organization"],
                "import_resources": True,
            },
        )

    cancellation_mock.assert_not_awaited()
    database_mock.assert_not_awaited()
    table_setup_mock.assert_not_awaited()
    catalog_mock.assert_not_called()
    probe_mock.assert_not_awaited()
    import_mock.assert_not_awaited()
