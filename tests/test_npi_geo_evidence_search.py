# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import types

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module


SOURCE_ID = "pdfhir_nebraska"
SOURCE_RECORD_ID = (
    "provider_directory_fhir:organization_address:"
    f"{SOURCE_ID}:organization-1:0"
)
ALIAS_SOURCE_ID = "pdfhir_nebraska_alias"
ALIAS_SOURCE_RECORD_ID = (
    "provider_directory_fhir:organization_affiliation:"
    f"{ALIAS_SOURCE_ID}:affiliation-1:0"
)
STALE_SOURCE_RECORD_ID = (
    "provider_directory_fhir:organization_address:pdfhir_stale:organization-1:0"
)
ADDRESS_KEY = "47ab23c8-74f7-41eb-8ffb-e8b9f6214594"


def _geo_candidate_params() -> dict:
    return {
        "address_site_key": None,
        "address_key": None,
        "lat": 41.09967,
        "long": -101.67646,
        "radius_miles": 0.1,
        "phone_digits": None,
        "entity_type_code": None,
        "taxonomy_exact": (),
        "taxonomy_prefixes": (),
        "provider_type": None,
        "specialty_filter": None,
        "include_sources": True,
        "include_evidence": False,
        "limit": 5,
    }


def _request(args):
    return types.SimpleNamespace(
        args=args,
        ctx=types.SimpleNamespace(sa_session=None),
    )


def test_geo_candidate_evidence_query_is_exact_and_current():
    """Geo corroboration must use exact keys from one current published run."""
    sql = npi_module._current_provider_directory_geo_evidence_sql()

    assert "unnest(" in sql
    assert "CAST(:candidate_npis AS bigint[])" in sql
    assert "CAST(:candidate_address_keys AS uuid[])" in sql
    assert "overlay.npi = requested.npi" in sql
    assert "overlay.address_key = requested.address_key" in sql
    assert "dataset.is_current IS TRUE" in sql
    assert "current_endpoint_counts AS MATERIALIZED" in sql
    assert "HAVING COUNT(*) = 1" in sql
    assert "current_datasets AS MATERIALIZED" in sql
    assert "dataset.status = 'published'" in sql
    assert "dataset.published_at IS NOT NULL" in sql
    assert "dataset.superseded_at IS NULL" in sql
    assert "current_run.run_id = overlay.last_seen_run_id" in sql
    assert "provider_directory_dataset_resource AS dataset_resource" in sql
    assert "dataset_resource.dataset_id = current_run.dataset_id" in sql
    assert "dataset_resource.resource_type = overlay.resource_type" in sql
    assert "dataset_resource.resource_id = overlay.resource_id" in sql
    assert "COUNT(DISTINCT current_run.dataset_id)::integer" in sql
    assert "AS provider_directory_source_count" in sql


def test_geo_candidate_query_does_not_rank_by_serving_source_count():
    """Geo preselection cannot rank with stale serving provenance counts."""
    query, _query_params = npi_module._match_candidate_query(
        _geo_candidate_params(),
        "mrf.entity_address_unified",
    )

    assert "source_count DESC NULLS LAST" not in str(query)


@pytest.fixture
def geo_evidence_fakes(monkeypatch):
    """Install deterministic query and source-detail fakes."""
    captured_by_name = {}

    async def fake_execute(statement, **keyword_args):
        captured_by_name.update(
            sql=str(statement),
            session=keyword_args.get("session"),
            params=keyword_args.get("params"),
        )
        evidence_row_by_name = {
            "npi": 1003968710,
            "address_key": ADDRESS_KEY,
            "source_record_ids": [SOURCE_RECORD_ID, ALIAS_SOURCE_RECORD_ID],
            "provider_directory_source_count": 1,
        }
        return types.SimpleNamespace(all=lambda: [evidence_row_by_name])

    async def fake_source_details(source_id_list, *, session=None):
        assert source_id_list == [SOURCE_ID, ALIAS_SOURCE_ID]
        assert session == "session"
        return {
            SOURCE_ID: {
                "source_id": SOURCE_ID,
                "endpoint_id": "endpoint-nebraska",
                "canonical_api_base": "https://nebraska.example/fhir",
                "org_name": "State of Nebraska",
                "plan_name": "Medicaid FFS",
            },
            ALIAS_SOURCE_ID: {
                "source_id": ALIAS_SOURCE_ID,
                "endpoint_id": "endpoint-nebraska",
                "canonical_api_base": "https://nebraska.example/fhir",
                "org_name": "Nebraska Alias",
                "plan_name": None,
            },
        }

    monkeypatch.setattr(npi_module, "_execute_stmt", fake_execute)
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_source_detail_map",
        fake_source_details,
    )
    return captured_by_name


@pytest.mark.asyncio
async def test_geo_candidate_attaches_current_overlay_provenance(geo_evidence_fakes):
    """A canonical base row retains exact current directory provenance."""
    candidate_row_list = [
        {
            "npi": 1003968710,
            "address_key": ADDRESS_KEY,
            "source_record_ids": [
                "nppes:1003968710:practice",
                STALE_SOURCE_RECORD_ID,
            ],
            "address_sources": ["nppes", "provider_directory_fhir"],
            "provider_directory_sources": [{"source_ids": ["pdfhir_stale"]}],
            "source_count": 2,
            "independent_source_count": 2,
            "multi_source_confirmed": True,
        }
    ]

    await npi_module._attach_match_candidate_source_details(
        candidate_row_list,
        _geo_candidate_params(),
        session="session",
    )

    assert geo_evidence_fakes["session"] == "session"
    assert geo_evidence_fakes["params"] == {
        "candidate_npis": [1003968710],
        "candidate_address_keys": [ADDRESS_KEY],
    }
    assert "current_provider_directory_runs AS MATERIALIZED" in geo_evidence_fakes[
        "sql"
    ]
    assert candidate_row_list[0]["source_record_ids"] == [
        "nppes:1003968710:practice",
        SOURCE_RECORD_ID,
        ALIAS_SOURCE_RECORD_ID,
    ]
    assert candidate_row_list[0]["address_sources"] == [
        "nppes",
        "provider_directory_fhir",
    ]
    assert candidate_row_list[0]["source_count"] == 2
    assert candidate_row_list[0]["independent_source_count"] == 2
    assert candidate_row_list[0]["multi_source_confirmed"] is True
    assert candidate_row_list[0]["provider_directory_source_count"] == 1
    source_summary = candidate_row_list[0]["provider_directory_sources"][0]
    assert source_summary["source_ids"] == [SOURCE_ID, ALIAS_SOURCE_ID]
    assert len(candidate_row_list[0]["provider_directory_sources"]) == 1


@pytest.mark.asyncio
async def test_geo_candidate_rejects_stale_serving_evidence(monkeypatch):
    """No exact current membership means no FHIR match from serving residue."""
    async def fake_execute(*_args, **_kwargs):
        return types.SimpleNamespace(all=list)

    async def unexpected_source_details(*_args, **_kwargs):
        raise AssertionError("stale source details must not be fetched")

    monkeypatch.setattr(npi_module, "_execute_stmt", fake_execute)
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_source_detail_map",
        unexpected_source_details,
    )
    candidate_row_by_name = {
        "npi": 1003968710,
        "address_key": ADDRESS_KEY,
        "source_record_ids": ["nppes:1003968710:practice", STALE_SOURCE_RECORD_ID],
        "address_sources": ["nppes", "provider_directory_fhir"],
        "provider_directory_sources": [{"source_ids": ["pdfhir_stale"]}],
        "geo_distance_miles": 0.0,
        "source_count": 2,
        "independent_source_count": 2,
        "multi_source_confirmed": True,
    }

    candidate_params_by_name = _geo_candidate_params()
    candidate_params_by_name["include_sources"] = False
    await npi_module._attach_match_candidate_source_details(
        [candidate_row_by_name],
        candidate_params_by_name,
    )
    candidate_output_map = npi_module._match_candidate_output(
        candidate_row_by_name,
        candidate_params_by_name,
        None,
    )

    assert candidate_row_by_name["source_record_ids"] == [
        "nppes:1003968710:practice"
    ]
    assert candidate_row_by_name["address_sources"] == ["nppes"]
    assert candidate_row_by_name["source_count"] == 1
    assert candidate_row_by_name["independent_source_count"] == 1
    assert candidate_row_by_name["multi_source_confirmed"] is False
    assert candidate_row_by_name["provider_directory_source_count"] == 0
    assert "provider_directory_sources" not in candidate_row_by_name
    assert candidate_output_map["sources"]["fhir"]["matched"] is False
    assert candidate_output_map["sources"]["fhir"]["source_count"] == 0


@pytest.mark.asyncio
async def test_geo_candidate_ranking_uses_current_source_count(monkeypatch):
    """Only exact current corroboration contributes to the final FHIR rank."""
    async def fake_execute(*_args, **_kwargs):
        current_evidence_map = {
            "npi": 1003968711,
            "address_key": ADDRESS_KEY,
            "source_record_ids": [SOURCE_RECORD_ID, ALIAS_SOURCE_RECORD_ID],
            "provider_directory_source_count": 1,
        }
        return types.SimpleNamespace(all=lambda: [current_evidence_map])

    monkeypatch.setattr(npi_module, "_execute_stmt", fake_execute)
    candidate_rows = [
        {
            "npi": 1003968710,
            "address_key": "57ab23c8-74f7-41eb-8ffb-e8b9f6214594",
            "source_record_ids": [STALE_SOURCE_RECORD_ID],
            "address_sources": ["provider_directory_fhir"],
            "source_count": 99,
            "geo_distance_miles": 0.0,
            "taxonomy_list": [],
        },
        {
            "npi": 1003968711,
            "address_key": ADDRESS_KEY,
            "source_record_ids": [STALE_SOURCE_RECORD_ID],
            "address_sources": ["provider_directory_fhir"],
            "source_count": 50,
            "geo_distance_miles": 0.0,
            "taxonomy_list": [],
        },
    ]
    candidate_params = _geo_candidate_params()
    candidate_params["include_sources"] = False
    candidate_params["limit"] = 2

    await npi_module._attach_match_candidate_source_details(
        candidate_rows,
        candidate_params,
    )
    ranked_candidates = npi_module._rank_match_candidate_outputs(
        candidate_rows,
        candidate_params,
        {},
    )

    assert [
        candidate_row["source_count"] for candidate_row in candidate_rows
    ] == [0, 1]
    assert [
        candidate_row["provider_directory_source_count"]
        for candidate_row in candidate_rows
    ] == [0, 1]
    assert ranked_candidates[0]["npi"] == 1003968711
    assert ranked_candidates[0]["sources"]["fhir"]["source_count"] == 1
    assert ranked_candidates[1]["sources"]["fhir"]["source_count"] == 0


@pytest.mark.asyncio
async def test_geo_evidence_query_caps_candidate_pairs(monkeypatch):
    """Corroboration parameters stay bounded if upstream returns excess rows."""
    captured_by_name = {}

    async def fake_execute(_statement, **keyword_args):
        captured_by_name.update(keyword_args.get("params") or {})
        return types.SimpleNamespace(all=list)

    monkeypatch.setattr(npi_module, "_execute_stmt", fake_execute)
    candidate_row_list = [
        {
            "npi": 1000000000 + candidate_index,
            "address_key": f"00000000-0000-0000-0000-{candidate_index:012d}",
        }
        for candidate_index in range(
            npi_module._MATCH_CANDIDATES_MAX_INTERNAL_ROWS + 1
        )
    ]

    await npi_module._attach_geo_candidate_record_ids(
        candidate_row_list,
        _geo_candidate_params(),
    )

    expected_limit = npi_module._MATCH_CANDIDATES_MAX_INTERNAL_ROWS
    assert len(captured_by_name["candidate_npis"]) == expected_limit
    assert len(captured_by_name["candidate_address_keys"]) == expected_limit


async def _one_geo_candidate(_params, *, session=None):
    return [{"npi": 1003968710, "address_key": ADDRESS_KEY}]


async def _no_candidate_sources(*_args, **_kwargs):
    return None


@pytest.mark.asyncio
async def test_match_candidates_returns_503_on_source_lookup_timeout(monkeypatch):
    """The added lookup cannot exceed the endpoint timeout budget."""
    async def slow_source_lookup(*_args, **_kwargs):
        await asyncio.sleep(1)

    monkeypatch.setattr(npi_module, "_fetch_match_candidate_rows", _one_geo_candidate)
    monkeypatch.setattr(
        npi_module,
        "_attach_match_candidate_source_details",
        slow_source_lookup,
    )
    monkeypatch.setattr(npi_module, "_MATCH_CANDIDATES_TIMEOUT_SECONDS", 0.001)

    with pytest.raises(sanic.exceptions.ServiceUnavailable, match="source lookup exceeded"):
        await npi_module.match_candidates(
            _request({"lat": "41.09967", "long": "-101.67646", "include_sources": "true"})
        )


@pytest.mark.asyncio
async def test_match_candidates_returns_503_on_source_lookup_db_failure(monkeypatch):
    """A corroboration DB error fails closed instead of serving stale evidence."""
    async def failing_source_lookup(*_args, **_kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(npi_module, "_fetch_match_candidate_rows", _one_geo_candidate)
    monkeypatch.setattr(
        npi_module,
        "_attach_match_candidate_source_details",
        failing_source_lookup,
    )

    with pytest.raises(sanic.exceptions.ServiceUnavailable, match="temporarily unavailable"):
        await npi_module.match_candidates(
            _request({"lat": "41.09967", "long": "-101.67646", "include_sources": "true"})
        )


@pytest.mark.asyncio
async def test_match_candidates_enrichment_uses_remaining_deadline(monkeypatch):
    """Later enrichment receives the original endpoint deadline, not a reset."""
    async def slow_candidate_lookup(*_args, **_kwargs):
        await asyncio.sleep(0.12)
        return await _one_geo_candidate({}, session=None)

    async def slow_enrichment(*_args, **_kwargs):
        await asyncio.sleep(0.12)
        return {}

    monkeypatch.setattr(
        npi_module,
        "_fetch_match_candidate_rows",
        slow_candidate_lookup,
    )
    monkeypatch.setattr(
        npi_module,
        "_attach_match_candidate_source_details",
        _no_candidate_sources,
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_summary_map",
        slow_enrichment,
    )
    monkeypatch.setattr(npi_module, "_MATCH_CANDIDATES_TIMEOUT_SECONDS", 0.2)

    with pytest.raises(
        sanic.exceptions.ServiceUnavailable,
        match="enrichment lookup exceeded",
    ):
        await npi_module.match_candidates(
            _request({"lat": "41.09967", "long": "-101.67646"})
        )


@pytest.mark.asyncio
async def test_match_candidates_returns_503_on_enrichment_timeout(monkeypatch):
    """A slow enrichment query cannot outlive the endpoint deadline."""
    async def slow_enrichment(*_args, **_kwargs):
        await asyncio.sleep(1)

    monkeypatch.setattr(npi_module, "_fetch_match_candidate_rows", _one_geo_candidate)
    monkeypatch.setattr(
        npi_module,
        "_attach_match_candidate_source_details",
        _no_candidate_sources,
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_summary_map",
        slow_enrichment,
    )
    monkeypatch.setattr(npi_module, "_MATCH_CANDIDATES_TIMEOUT_SECONDS", 0.001)

    with pytest.raises(
        sanic.exceptions.ServiceUnavailable,
        match="enrichment lookup exceeded",
    ):
        await npi_module.match_candidates(
            _request({"lat": "41.09967", "long": "-101.67646"})
        )


@pytest.mark.asyncio
async def test_match_candidates_returns_503_on_enrichment_db_failure(monkeypatch):
    """An enrichment DB failure is translated without exposing raw details."""
    async def failing_enrichment(*_args, **_kwargs):
        raise RuntimeError("database connection secret")

    monkeypatch.setattr(npi_module, "_fetch_match_candidate_rows", _one_geo_candidate)
    monkeypatch.setattr(
        npi_module,
        "_attach_match_candidate_source_details",
        _no_candidate_sources,
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_summary_map",
        failing_enrichment,
    )

    with pytest.raises(sanic.exceptions.ServiceUnavailable) as exc_info:
        await npi_module.match_candidates(
            _request({"lat": "41.09967", "long": "-101.67646"})
        )

    assert "enrichment lookup is temporarily unavailable" in str(exc_info.value)
    assert "database connection secret" not in str(exc_info.value)
