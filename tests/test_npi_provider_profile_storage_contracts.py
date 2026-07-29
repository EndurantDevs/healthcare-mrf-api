from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from api import provider_profile as profile_api
from api.endpoint import npi as npi_module
from api.provider_profile_display import display_value


VALID_NPI = "1000000004"


class _Result:
    def __init__(self, *, scalar_value=None, rows=None):
        self._scalar_value = scalar_value
        self._rows = list(rows or [])

    def scalar(self):
        return self._scalar_value

    def all(self):
        return self._rows


class _Row:
    def __init__(self, **mapping):
        self._mapping = mapping


def test_profile_row_decoding_rejects_malformed_values_and_normalizes_time():
    assert npi_module._provider_directory_profile_json("{malformed") is None
    assert npi_module._provider_directory_profile_json(["not", "a", "map"]) is None
    assert npi_module._serialize_utc_rfc3339_datetime(None) is None
    assert (
        npi_module._serialize_utc_rfc3339_datetime("2026-07-27T12:30:00z")
        == "2026-07-27T12:30:00Z"
    )
    assert (
        npi_module._serialize_utc_rfc3339_datetime("2026-07-27T12:30:00")
        == "2026-07-27T12:30:00Z"
    )
    with pytest.raises(TypeError, match="published_at must be"):
        npi_module._serialize_utc_rfc3339_datetime(123)


def test_profile_payload_ignores_malformed_optional_evidence():
    payload = npi_module._provider_directory_profile_payload(
        {
            "profile_json": {"categories": {}},
            "evidence_json": '["not", "a", "mapping"]',
            "generation_id": "generation-1",
            "published_at": None,
        },
        include_evidence=True,
    )

    assert payload == {
        "profile": {
            "categories": {},
            "generation_id": "generation-1",
            "published_at": None,
        }
    }


@pytest.mark.asyncio
async def test_profile_table_and_column_capability_queries_are_cached(
    monkeypatch,
):
    execute = AsyncMock(
        side_effect=[
            _Result(scalar_value="mrf.provider_directory_profile"),
            _Result(rows=[("generation_id",), (None,), (), ("profile_json",)]),
        ]
    )
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)
    monkeypatch.setattr(
        npi_module,
        "_PROVIDER_DIRECTORY_PROFILE_TABLES_SEEN",
        set(),
    )
    monkeypatch.setattr(npi_module, "_TABLE_COLUMNS_CACHE", {})
    monkeypatch.setattr(npi_module, "ENABLE_NPI_SCHEMA_CACHE", True)

    assert await npi_module._is_provider_directory_profile_table_available(
        "mrf.provider_directory_profile"
    )
    assert await npi_module._is_provider_directory_profile_table_available(
        "mrf.provider_directory_profile"
    )
    assert await npi_module._table_columns("provider_directory_profile") == {
        "generation_id",
        "profile_json",
    }
    assert await npi_module._table_columns("provider_directory_profile") == {
        "generation_id",
        "profile_json",
    }
    assert execute.await_count == 2


@pytest.mark.asyncio
async def test_profile_fetch_skips_malformed_rows_and_accepts_mapping_rows(
    monkeypatch,
):
    source_rows = [
        _Row(
            npi=int(VALID_NPI),
            profile_json="{malformed",
            generation_id="bad-row",
            published_at=None,
        ),
        {
            "npi": 1588616783,
            "profile_json": {"categories": {}},
            "generation_id": "valid-row",
            "published_at": datetime(2026, 7, 27, 12, 0),
        },
    ]
    execute = AsyncMock(return_value=_Result(rows=source_rows))
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)
    monkeypatch.setattr(
        npi_module,
        "_is_provider_directory_profile_table_available",
        AsyncMock(return_value=True),
    )

    profiles = await npi_module._fetch_provider_directory_profile_map(
        [VALID_NPI, 1588616783],
    )

    assert list(profiles) == [1588616783]
    assert profiles[1588616783]["profile"]["generation_id"] == "valid-row"
    assert profiles[1588616783]["profile"]["published_at"] == (
        "2026-07-27T12:00:00Z"
    )
    assert "evidence_json" not in str(execute.await_args.args[0])


def _filtered_evidence_composer_inputs():
    """Build the source payloads for returned-state evidence filtering."""
    provider_profile_by_key = {
        "categories": {
            "identity": {
                "items": [
                    {
                        "type": "license",
                        "value": {"number": "ME1"},
                        "source_record_id": "state-keep",
                        "source_record_ids": ["state-also-keep"],
                        "source_kinds": ["state_regulator"],
                    },
                    {
                        "type": "name",
                        "value": {"text": "Alex Example"},
                        "source_kinds": ["provider_directory_fhir"],
                    },
                ]
            }
        }
    }
    state_projection_by_key = {
        "evidence": {
            "records": [
                {"source_record_id": "state-keep"},
                {"source_record_id": "state-also-keep"},
                {"source_record_id": "state-drop"},
                None,
            ]
        }
    }
    fhir_evidence_by_key = {
        "facts": {
            "name": {
                "items": [
                    {"value": {"text": "Alex Example"}},
                    {"value": {"text": "Different Provider"}},
                    None,
                ],
                "total": 2,
                "truncated": True,
            },
            "service": {
                "items": [{"value": {"display": "Unreturned service"}}],
            },
        }
    }

    return provider_profile_by_key, state_projection_by_key, fhir_evidence_by_key


def test_evidence_composer_filters_to_returned_state_and_fhir_facts():
    """Verify evidence composer filters to returned state and fhir facts."""
    (
        provider_profile_by_key,
        state_projection_by_key,
        fhir_evidence_by_key,
    ) = _filtered_evidence_composer_inputs()
    evidence = profile_api.compose_provider_profile_evidence(
        state_projection=state_projection_by_key,
        fhir_evidence=fhir_evidence_by_key,
        provider_profile=provider_profile_by_key,
        page_category="identity",
    )

    assert evidence["sources"]["state_regulator"]["records"] == [
        {"source_record_id": "state-keep"},
        {"source_record_id": "state-also-keep"},
    ]
    assert evidence["sources"]["provider_directory_fhir"]["facts"] == {
        "name": {
            "items": [{"value": {"text": "Alex Example"}}],
            "total": 1,
            "truncated": False,
        }
    }


def test_evidence_composer_preserves_source_payloads_without_profile_filter():
    state_evidence_by_key = {
        "records": [
            {"source_record_id": "state-1"},
            {"source_record_id": "state-2"},
        ]
    }
    fhir_evidence_by_key = {
        "facts": {
            "name": {
                "items": [
                    {"value": {"text": "Alex Example"}},
                    {"value": {"text": "Different Provider"}},
                ]
            }
        }
    }

    evidence = profile_api.compose_provider_profile_evidence(
        state_projection={"evidence": state_evidence_by_key},
        fhir_evidence=fhir_evidence_by_key,
        provider_profile=None,
    )

    assert evidence["sources"]["state_regulator"] == state_evidence_by_key
    assert evidence["sources"]["provider_directory_fhir"] == fhir_evidence_by_key


def test_profile_composer_handles_nonmapping_display_and_unkeyed_state_fact():
    assert display_value("provider_detail", ["unstructured", "value"]) == (
        "unstructured; value"
    )
    profile = profile_api.compose_provider_profile(
        int(VALID_NPI),
        state_projection={
            "generation_id": "state",
            "profile": {
                "categories": {
                    "identity": {
                        "availability": "available",
                        "items": [
                            {
                                "type": "name",
                                "display": "Alex Example",
                                "value": {"text": "Alex Example"},
                                "source_kinds": [],
                            }
                        ],
                    }
                }
            },
        },
        fhir_profile={
            "generation_id": "fhir",
            "facts": {
                "name": {
                    "items": [
                        {
                            "value": {"text": "Alex Example"},
                            "source_ids": ["fhir-source"],
                        }
                    ]
                }
            },
        },
        requested_categories=["identity"],
    )

    profile_item = profile["categories"]["identity"]["items"][0]
    assert profile_item["source_kinds"] == ["provider_directory_fhir"]
    assert profile_item["assertion_count"] == 2
