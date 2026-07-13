# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import types
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module


class _Result:
    def __init__(self, *, scalar_value=None, rows=None):
        self.scalar_value = scalar_value
        self.rows = list(rows or [])

    def scalar(self):
        return self.scalar_value

    def all(self):
        return self.rows


def _build_profile_dict() -> dict:
    """Return a compact profile fixture with derived profile facts."""
    return {
        "schema_version": 1,
        "facts": {
            "age": {"items": [{"value": {"years": 56}}]},
            "years_of_practice": {
                "items": [
                    {
                        "value": {
                            "years": 25,
                            "estimated": True,
                            "basis": (
                                "FHIR Practitioner.qualification.period.start"
                            ),
                        }
                    }
                ]
            },
        },
    }


def _build_evidence_dict(source_id: str) -> dict:
    """Return an evidence fixture for one source."""
    return {
        "schema_version": 1,
        "facts": {
            "age": {"items": [{"evidence": [{"source_id": source_id}]}]},
            "years_of_practice": {
                "items": [{"evidence": [{"source_id": source_id}]}]
            },
        },
    }


def _build_profile_payload_by_kind(source_id: str) -> dict:
    """Return the profile map payload consumed by the NPI route."""
    profile_dict = _build_profile_dict()
    profile_dict["generation_id"] = "generation_1"
    evidence_dict = _build_evidence_dict(source_id)
    evidence_dict["generation_id"] = "generation_1"
    return {"profile": profile_dict, "evidence": evidence_dict}


def _patch_npi_detail_dependencies(monkeypatch, profile_payload_by_kind) -> None:
    """Install stable NPI-detail dependencies for the route test."""
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_profile_map",
        AsyncMock(return_value={1588616783: profile_payload_by_kind}),
    )
    monkeypatch.setattr(
        npi_module,
        "_build_npi_details",
        AsyncMock(
            return_value={
                "npi": 1588616783,
                "taxonomy_list": [],
                "taxonomy_group_list": [],
                "do_business_as": [],
                "address_list": [],
            }
        ),
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_other_names",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_detail",
        AsyncMock(
            return_value={
                "summary": None,
                "enrollments": {},
                "ffs_visibility": {},
            }
        ),
    )
    monkeypatch.setattr(
        npi_module,
        "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS",
        0.0,
    )


@pytest.mark.asyncio
async def test_profile_fetch_is_safe_before_first_artifact_publication(
    monkeypatch,
):
    execute = AsyncMock(return_value=_Result(scalar_value=None))
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)
    monkeypatch.setattr(
        npi_module,
        "_PROVIDER_DIRECTORY_PROFILE_TABLES_SEEN",
        set(),
    )

    assert await npi_module._fetch_provider_directory_profile_map(
        [1588616783]
    ) == {}
    assert execute.await_count == 1
    assert "to_regclass" in str(execute.await_args.args[0])


@pytest.mark.asyncio
async def test_profile_fetch_returns_compact_and_optional_evidence(monkeypatch):
    """Fetch compact facts and opt-in source evidence in one indexed query."""
    profile_dict = _build_profile_dict()
    evidence_dict = _build_evidence_dict("s1")
    execute = AsyncMock(
        side_effect=[
            _Result(scalar_value="mrf.provider_directory_profile"),
            _Result(
                rows=[
                    {
                        "npi": 1588616783,
                        "profile_json": json.dumps(profile_dict),
                        "evidence_json": evidence_dict,
                        "generation_id": "generation_1",
                        "published_at": "2026-07-13 20:00:00",
                    }
                ]
            ),
        ]
    )
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)
    monkeypatch.setattr(
        npi_module,
        "_PROVIDER_DIRECTORY_PROFILE_TABLES_SEEN",
        set(),
    )

    profiles_by_npi = await npi_module._fetch_provider_directory_profile_map(
        [None, "invalid", "1588616783", 1588616783],
        include_evidence=True,
    )

    assert profiles_by_npi[1588616783]["profile"]["generation_id"] == "generation_1"
    assert profiles_by_npi[1588616783]["profile"]["facts"]["age"]["items"][0][
        "value"
    ]["years"] == 56
    assert profiles_by_npi[1588616783]["evidence"]["facts"]["age"]["items"][0][
        "evidence"
    ][0]["source_id"] == "s1"
    profile_query = execute.await_args_list[1]
    assert "evidence_json" in str(profile_query.args[0])
    assert profile_query.kwargs["params"] == {"npis": [1588616783]}


def test_npi_cache_key_tracks_profile_generation_and_visibility():
    common_options_by_name = {
        "npi": 1588616783,
        "view": "full",
        "include_chain": False,
        "extra_info": False,
        "sync_geocode": False,
        "lookup_stored_geocode": False,
    }

    first_generation = npi_module._npi_detail_cache_key(
        **common_options_by_name,
        include_profile=True,
        profile_generation="generation_1",
    )
    second_generation = npi_module._npi_detail_cache_key(
        **common_options_by_name,
        include_profile=True,
        profile_generation="generation_2",
    )
    profile_disabled = npi_module._npi_detail_cache_key(
        **common_options_by_name,
        include_profile=False,
        profile_generation=None,
    )

    assert first_generation != second_generation
    assert first_generation != profile_disabled


@pytest.mark.asyncio
async def test_npi_detail_exposes_profile_and_evidence(monkeypatch):
    """Expose compact profile facts and opt-in evidence through NPI detail."""
    profile_payload_by_kind = _build_profile_payload_by_kind(
        "pdfhir_source_a"
    )
    _patch_npi_detail_dependencies(monkeypatch, profile_payload_by_kind)
    request = types.SimpleNamespace(
        args={
            "include_evidence": "1",
            "sync_geocode": "0",
            "lookup_stored_geocode": "0",
        },
        app=types.SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )

    response = await npi_module.get_npi(request, "1588616783")
    response_payload = json.loads(response.body)

    assert response_payload["provider_directory_profile"]["facts"][
        "years_of_practice"
    ]["items"][0]["value"]["years"] == 25
    assert response_payload["provider_directory_profile_evidence"]["facts"][
        "years_of_practice"
    ]["items"][0]["evidence"][0]["source_id"] == "pdfhir_source_a"
