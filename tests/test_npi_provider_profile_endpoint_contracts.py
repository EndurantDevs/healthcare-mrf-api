from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from api.endpoint import npi as npi_module


VALID_NPI = "1000000004"
CURRENT_GENERATION = "a" * 64


def _request(**args):
    return SimpleNamespace(args=args)


def _json_body(result):
    return json.loads(result.body)


def _install_route_dependencies(
    monkeypatch,
    *,
    state_projection=None,
    fhir_profile_map=None,
    composed_profile=None,
    composed_evidence=None,
):
    state_fetch = AsyncMock(return_value=state_projection)
    fhir_fetch = AsyncMock(return_value=fhir_profile_map or {})
    compose_profile = MagicMock(return_value=composed_profile)
    compose_evidence = MagicMock(return_value=composed_evidence)
    monkeypatch.setattr(
        npi_module,
        "fetch_state_profile_projection",
        state_fetch,
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_profile_map",
        fhir_fetch,
    )
    monkeypatch.setattr(
        npi_module,
        "compose_provider_profile",
        compose_profile,
    )
    monkeypatch.setattr(
        npi_module,
        "compose_provider_profile_evidence",
        compose_evidence,
    )
    return state_fetch, fhir_fetch, compose_profile, compose_evidence


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("npi", "args", "error"),
    [
        ("not-an-npi", {}, "invalid_npi"),
        (
            VALID_NPI,
            {"generation_id": "not-a-generation"},
            "invalid_profile_generation_id",
        ),
        (
            VALID_NPI,
            {"category": "identity", "categories": "services"},
            "conflicting_profile_parameters",
        ),
        (VALID_NPI, {"limit": "10"}, "profile_category_required"),
        (
            VALID_NPI,
            {"generation_id": CURRENT_GENERATION},
            "profile_category_required",
        ),
        (
            VALID_NPI,
            {"categories": "identity,unknown,not_standard"},
            "invalid_profile_categories",
        ),
    ],
)
async def test_provider_profile_route_rejects_invalid_contracts_before_queries(
    monkeypatch,
    npi,
    args,
    error,
):
    state_fetch, fhir_fetch, _, _ = _install_route_dependencies(monkeypatch)

    result = await npi_module.get_provider_profile(_request(**args), npi)

    assert result.status == 400
    assert _json_body(result)["error"] == error
    state_fetch.assert_not_awaited()
    fhir_fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("args", "message"),
    [
        (
            {"category": "identity", "limit": "zero"},
            "Parameter 'limit' must be an integer",
        ),
        (
            {"category": "identity", "offset": "-1"},
            "Parameter 'offset' must be between 0 and 1000000",
        ),
        (
            {"category": "x" * 65},
            "Parameter 'category' is too long",
        ),
    ],
)
async def test_provider_profile_route_uses_shared_bounded_query_validation(
    monkeypatch,
    args,
    message,
):
    state_fetch, fhir_fetch, _, _ = _install_route_dependencies(monkeypatch)

    with pytest.raises(npi_module.sanic.exceptions.InvalidUsage, match=message):
        await npi_module.get_provider_profile(_request(**args), VALID_NPI)

    state_fetch.assert_not_awaited()
    fhir_fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_provider_profile_route_returns_not_found_for_empty_source_mix(
    monkeypatch,
):
    state_fetch, fhir_fetch, compose_profile, compose_evidence = (
        _install_route_dependencies(monkeypatch)
    )

    result = await npi_module.get_provider_profile(_request(), VALID_NPI)

    assert result.status == 404
    assert _json_body(result) == {
        "error": "provider_profile_not_found",
        "message": "No reviewed provider profile facts are available for this NPI.",
        "npi": int(VALID_NPI),
    }
    state_fetch.assert_awaited_once_with(int(VALID_NPI))
    fhir_fetch.assert_awaited_once_with(
        [int(VALID_NPI)],
        include_evidence=False,
    )
    assert compose_profile.call_args.kwargs["requested_categories"] == list(
        npi_module.STANDARD_CATEGORIES
    )
    compose_evidence.assert_not_called()


@pytest.mark.asyncio
async def test_provider_profile_route_rejects_changed_paging_generation(
    monkeypatch,
):
    profile_by_key = {
        "generation_id": CURRENT_GENERATION,
        "categories": {"identity": {"items": []}},
    }
    _, _, _, compose_evidence = _install_route_dependencies(
        monkeypatch,
        composed_profile=profile_by_key,
    )
    requested_generation = "b" * 64

    operation_result = await npi_module.get_provider_profile(
        _request(
            category="identity",
            generation_id=requested_generation,
            limit="5",
            offset="10",
        ),
        VALID_NPI,
    )

    assert operation_result.status == 409
    assert _json_body(operation_result) == {
        "error": "provider_profile_generation_changed",
        "message": "The provider profile changed; restart category pagination.",
        "requested_generation_id": requested_generation,
        "current_generation_id": CURRENT_GENERATION,
    }
    compose_evidence.assert_not_called()


@pytest.mark.asyncio
async def test_provider_profile_route_passes_paging_visibility_and_evidence(
    monkeypatch,
):
    """Verify provider profile route passes paging visibility and evidence."""
    state_projection_by_key = {"generation_id": "state-generation"}
    fhir_record_by_key = {
        "profile": {"generation_id": "fhir-generation"},
        "evidence": {"facts": {"name": {"items": []}}},
    }
    profile_by_key = {
        "generation_id": CURRENT_GENERATION,
        "categories": {"identity": {"items": []}},
    }
    evidence_by_key = {"schema_version": 1, "sources": {"state_regulator": {}}}
    state_fetch, fhir_fetch, compose_profile, compose_evidence = (
        _install_route_dependencies(
            monkeypatch,
            state_projection=state_projection_by_key,
            fhir_profile_map={int(VALID_NPI): fhir_record_by_key},
            composed_profile=profile_by_key,
            composed_evidence=evidence_by_key,
        )
    )

    operation_result = await npi_module.get_provider_profile(
        _request(
            include_evidence="yes",
            include_sensitive="on",
            category="identity",
            generation_id=CURRENT_GENERATION.upper(),
            limit="7",
            offset="2",
        ),
        VALID_NPI,
    )

    assert operation_result.status == 200
    assert _json_body(operation_result) == {
        "npi": int(VALID_NPI),
        "provider_profile": profile_by_key,
        "provider_profile_evidence": evidence_by_key,
    }
    state_fetch.assert_awaited_once_with(int(VALID_NPI))
    fhir_fetch.assert_awaited_once_with([int(VALID_NPI)], include_evidence=True)
    compose_profile.assert_called_once_with(
        int(VALID_NPI),
        state_projection=state_projection_by_key,
        fhir_profile=fhir_record_by_key["profile"],
        requested_categories=["identity"],
        include_sensitive=True,
        page_category="identity",
        page_limit=7,
        page_offset=2,
    )
    compose_evidence.assert_called_once_with(
        state_projection=state_projection_by_key,
        fhir_evidence=fhir_record_by_key["evidence"],
        provider_profile=profile_by_key,
        page_category="identity",
    )


@pytest.mark.asyncio
async def test_provider_profile_route_omits_empty_evidence_for_state_only_profile(
    monkeypatch,
):
    profile_by_key = {
        "generation_id": CURRENT_GENERATION,
        "categories": {
            "identity": {"items": []},
            "services": {"items": []},
        },
    }
    _, fhir_fetch, compose_profile, compose_evidence = (
        _install_route_dependencies(
            monkeypatch,
            state_projection={"generation_id": "state-generation"},
            composed_profile=profile_by_key,
        )
    )

    operation_result = await npi_module.get_provider_profile(
        _request(
            include_evidence="1",
            categories="identity, services",
        ),
        VALID_NPI,
    )

    assert operation_result.status == 200
    assert _json_body(operation_result) == {
        "npi": int(VALID_NPI),
        "provider_profile": profile_by_key,
    }
    fhir_fetch.assert_awaited_once_with(
        [int(VALID_NPI)],
        include_evidence=True,
    )
    assert compose_profile.call_args.kwargs["requested_categories"] == [
        "identity",
        "services",
    ]
    compose_evidence.assert_called_once()


@pytest.mark.asyncio
async def test_provider_profile_route_returns_selected_categories_without_evidence(
    monkeypatch,
):
    profile_by_key = {
        "generation_id": CURRENT_GENERATION,
        "categories": {
            "identity": {"items": []},
            "services": {"items": []},
        },
    }
    _, fhir_fetch, compose_profile, compose_evidence = (
        _install_route_dependencies(
            monkeypatch,
            composed_profile=profile_by_key,
        )
    )

    operation_result = await npi_module.get_provider_profile(
        _request(categories="identity, services"),
        VALID_NPI,
    )

    assert operation_result.status == 200
    assert _json_body(operation_result) == {
        "npi": int(VALID_NPI),
        "provider_profile": profile_by_key,
    }
    fhir_fetch.assert_awaited_once_with(
        [int(VALID_NPI)],
        include_evidence=False,
    )
    assert compose_profile.call_args.kwargs["requested_categories"] == [
        "identity",
        "services",
    ]
    compose_evidence.assert_not_called()
