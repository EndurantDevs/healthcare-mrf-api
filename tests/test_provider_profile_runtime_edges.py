from __future__ import annotations

import importlib
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderProfileProjection

profile_api = importlib.import_module("api.provider_profile")


class _Row:
    def __init__(self, **mapping):
        self._mapping = mapping


@pytest.mark.asyncio
async def test_state_projection_lookup_distinguishes_absent_table_row_and_payload(
    monkeypatch,
):
    database = type(
        "ProjectionDb",
        (),
        {
            "scalar": AsyncMock(side_effect=[None, "mrf.provider_profile_projection", "mrf.provider_profile_projection"]),
            "first": AsyncMock(
                side_effect=[
                    None,
                    _Row(
                        profile_json={"npi": 1000000004},
                        evidence_json={"records": []},
                        generation_id="a" * 32,
                        published_at=datetime(2026, 7, 27, tzinfo=UTC),
                    ),
                ]
            ),
        },
    )()
    monkeypatch.setattr(profile_api, "db", database)

    assert await profile_api.fetch_state_profile_projection(1000000004) is None
    assert await profile_api.fetch_state_profile_projection(1000000004) is None
    operation_result = await profile_api.fetch_state_profile_projection(1000000004)

    assert operation_result == {
        "profile": {"npi": 1000000004},
        "evidence": {"records": []},
        "generation_id": "a" * 32,
        "published_at": datetime(2026, 7, 27, tzinfo=UTC),
    }
    assert database.first.await_count == 2


@pytest.mark.asyncio
async def test_state_projection_lookup_rejects_unsafe_schema(monkeypatch):
    monkeypatch.setattr(
        ProviderProfileProjection.__table__,
        "schema",
        "unsafe-schema",
    )

    with pytest.raises(RuntimeError, match="schema_invalid"):
        await profile_api.fetch_state_profile_projection(1000000004)


def test_composer_handles_empty_and_non_display_values():
    assert profile_api.compose_provider_profile(
        1000000004,
        state_projection=None,
        fhir_profile=None,
    ) is None
    assert profile_api._display_value("plain text") == "plain text"
    assert profile_api._display_value({"other": 7}) == '{"other":7}'


def test_composer_tolerates_unmaterialized_fhir_groups_and_items():
    profile = profile_api.compose_provider_profile(
        1000000004,
        state_projection=None,
        fhir_profile={
            "generation_id": "fhir-generation",
            "facts": {
                "name": {"items": "not-a-list"},
                "service": {"items": [None]},
                "role": "not-a-group",
            },
            "sources": [None],
        },
    )

    assert profile is not None
    assert profile["categories"]["identity"]["items"] == []
    assert profile["categories"]["services"]["items"] == []
    assert profile["sources"] == []

    no_fact_mapping = profile_api.compose_provider_profile(
        1000000004,
        state_projection=None,
        fhir_profile={"facts": ["not", "a", "mapping"]},
    )
    assert no_fact_mapping is not None
    assert all(
        category["availability"] == "unavailable"
        for category in no_fact_mapping["categories"].values()
    )


def test_composer_merges_repeated_fhir_support_without_double_counting():
    state_item_by_key = {
        "type": "name",
        "display": "Alex Example",
        "value": {"text": "Alex Example"},
        "source_record_id": "state-record",
        "source_record_ids": ["state-record"],
        "source_kinds": ["provider_directory_fhir"],
        "assertions": [
            {
                "source_kind": "provider_directory_fhir",
                "assertion_type": "provider_directory_reported",
                "verification_status": "payer_directory_source",
            }
        ],
        "assertion_count": 2,
        "sensitive": False,
        "public_default": True,
    }
    profile = profile_api.compose_provider_profile(
        1000000004,
        state_projection={
            "generation_id": "state-generation",
            "profile": {
                "categories": {
                    "identity": {
                        "availability": "available",
                        "items": [state_item_by_key],
                    }
                },
                "sources": [],
            },
        },
        fhir_profile={
            "generation_id": "fhir-generation",
            "facts": {
                "name": {
                    "items": [
                        None,
                        {
                            "value": {"text": "Alex Example"},
                            "source_ids": ["fhir-source-1"],
                            "source_count": 3,
                        },
                    ]
                }
            },
        },
        requested_categories=["identity"],
    )

    assert profile is not None
    profile_item = profile["categories"]["identity"]["items"][0]
    assert profile_item["assertion_count"] == 3
    assert profile_item["source_ids"] == ["fhir-source-1"]


def test_evidence_composer_handles_non_mapping_payloads_and_empty_sources():
    assert profile_api.compose_provider_profile_evidence(
        state_projection=None,
        fhir_evidence=None,
    ) is None
    evidence = profile_api.compose_provider_profile_evidence(
        state_projection={"evidence": ["not-a-mapping"]},
        fhir_evidence={"facts": ["not-a-mapping"]},
        provider_profile={"categories": ["not-a-mapping"]},
    )

    assert evidence == {
        "schema_version": profile_api.PROFILE_SCHEMA_VERSION,
        "sources": {
            "provider_directory_fhir": {
                "facts": ["not-a-mapping"],
            }
        },
    }
