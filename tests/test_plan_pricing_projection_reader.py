# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-path and migration contracts for the plan-pricing projection."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import SimpleNamespace

import orjson
import pytest

from api import plan_pricing_projection as projection

from .test_plan_pricing_projection import PROJECTION_ID, _selection, _Session


@pytest.mark.asyncio
async def test_provider_projection_keeps_one_address_per_npi_and_zip():
    provider_rows = [
        {
            "npi": 1234567890,
            "provider_name": "Synthetic Provider",
            "entity_type_code": 1,
            "credential": "MD",
            "taxonomy_codes": ["207Q00000X"],
            "classifications": ["Family Medicine"],
            "primary_specialty": "Family Medicine",
            "city": "Example One",
            "state": "IL",
            "zip5": "60601",
        },
        {
            "npi": 1234567890,
            "provider_name": "Synthetic Provider",
            "entity_type_code": 1,
            "credential": "MD",
            "taxonomy_codes": ["207Q00000X"],
            "classifications": ["Family Medicine"],
            "primary_specialty": "Family Medicine",
            "city": "Example Two",
            "state": "IL",
            "zip5": "60602",
        },
    ]
    session = _Session(provider_rows)

    providers_by_npi = await projection._projection_provider_rows_for_npis(
        session, [1234567890]
    )

    assert [
        provider_row["zip5"] for provider_row in providers_by_npi[1234567890]
    ] == ["60601", "60602"]
    assert "PARTITION BY addr.npi, COALESCE" in session.statements[0][0]


@pytest.mark.asyncio
async def test_zip_radius_resolves_its_centroid_inside_the_projection_query():
    session = _Session(["62401", "62402"])

    cells = await projection._geo_cells(
        session,
        {"zip5": "62401", "zip_radius_miles": 25},
        result_type="provider_cards",
    )

    assert cells == ["62401", "62402"]
    statement, params = session.statements[0]
    assert "WHERE zip_code = :zip5" in statement
    assert "CROSS JOIN LATERAL" in statement
    assert params["zip5"] == "62401"
    assert "latitude" not in params

    assert await projection._geo_cells(
        _Session([]),
        {"zip5": "62401", "zip_radius_miles": 25},
        result_type="provider_cards",
    ) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("args", "expected_result_type", "fragment"),
    (
        (
            {
                "view": "card",
                "include_providers": "true",
                "code_system": "CPT",
                "code": "27447",
                "zip5": "62401",
                "zip_radius_miles": 0,
            },
            "provider_cards",
            b'{"npi":1234567890,"provider_name":"Synthetic Provider"}',
        ),
        (
            {
                "view": "card",
                "include_providers": "false",
                "code_system": "CPT",
                "code": "27447",
                "zip5": "62401",
                "zip_radius_miles": 0,
            },
            "rate_aggregates",
            b'{"geo_cell":"62401","provider_count":1,"rate_count":2}',
        ),
    ),
)
async def test_projection_reader_embeds_pre_rendered_fragments(
    args,
    expected_result_type,
    fragment,
):
    session = _Session([(memoryview(fragment), 1)])
    response = await projection.search_plan_pricing_projection(
        session,
        _selection(),
        args,
        SimpleNamespace(limit=25, offset=0, page=1),
    )
    wire_response = orjson.loads(orjson.dumps(response))

    assert wire_response["result_type"] == expected_result_type
    assert wire_response["items"] == [orjson.loads(fragment)]
    assert wire_response["query"]["projection_contract"] == (
        projection.PROJECTION_CONTRACT
    )
    assert wire_response["query"]["include_providers"] is (
        expected_result_type == "provider_cards"
    )
    assert wire_response["query"]["view"] == args["view"]
    assert PROJECTION_ID in session.statements[0][1].values()
    if expected_result_type == "provider_cards":
        assert "PARTITION BY item.npi" in session.statements[0][0]


@pytest.mark.asyncio
async def test_projection_reader_preserves_total_past_the_last_page():
    response = await projection.search_plan_pricing_projection(
        _Session([(None, 2)]),
        _selection(),
        {
            "view": "card",
            "code_system": "CPT",
            "code": "27447",
            "zip5": "62401",
            "zip_radius_miles": 0,
        },
        SimpleNamespace(limit=25, offset=50, page=3),
    )

    assert response["result_state"] == "matched"
    assert response["items"] == []
    assert response["pagination"] == {
        "total": 2,
        "total_is_exact": True,
        "total_lower_bound": 2,
        "limit": 25,
        "offset": 50,
        "page": 3,
        "has_more": False,
    }


@pytest.mark.asyncio
async def test_projection_reader_uses_existing_code_system_aliases():
    session = _Session([])

    await projection.search_plan_pricing_projection(
        session,
        _selection(),
        {
            "view": "card",
            "code_system": "MS-DRG",
            "code": "20",
            "zip5": "62401",
            "zip_radius_miles": 0,
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )

    params = session.statements[0][1]
    assert params["code_system"] == "MS_DRG"
    assert params["code"] == "020"


@pytest.mark.asyncio
@pytest.mark.parametrize("requested_system", ["CPT", "HCPCS"])
async def test_projection_reader_uses_shared_numeric_cpt_hcpcs_identity(
    requested_system,
):
    fragment = b'{"npi":1234567890,"minimum_negotiated_rate":44}'
    session = _Session([(memoryview(fragment), 1)])

    response = await projection.search_plan_pricing_projection(
        session,
        _selection(),
        {
            "view": "card",
            "code_system": requested_system,
            "code": "27447",
            "zip5": "62401",
            "zip_radius_miles": 0,
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )

    assert response["pagination"]["total"] == 1
    params = session.statements[0][1]
    assert params["code_system"] == "CPT"
    assert params["code"] == "27447"


@pytest.mark.asyncio
async def test_projection_reader_reads_hcpcs_card_fragment():
    fragment = b'{"npi":1234567890,"minimum_negotiated_rate":44}'
    session = _Session([(memoryview(fragment), 1)])

    response = await projection.search_plan_pricing_projection(
        session,
        _selection(),
        {
            "view": "card",
            "code_system": "HCPCS",
            "code": "G0439",
            "zip5": "62401",
            "zip_radius_miles": 0,
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )

    assert orjson.loads(orjson.dumps(response))["items"] == [
        {"npi": 1234567890, "minimum_negotiated_rate": 44}
    ]
    params = session.statements[0][1]
    assert params["code_system"] == "HCPCS"
    assert params["code"] == "G0439"


@pytest.mark.asyncio
async def test_card_rejects_filters_that_projection_cannot_preserve():
    with pytest.raises(
        projection.PlanPricingProjectionUnsupported,
        match="classification",
    ):
        await projection.search_plan_pricing_projection(
            _Session([]),
            _selection(),
            {
                "view": "card",
                "code_system": "CPT",
                "code": "27447",
                "zip5": "62401",
                "classification": "Family Medicine",
            },
            SimpleNamespace(limit=25, offset=0, page=1),
        )


@pytest.mark.asyncio
async def test_card_aggregate_uses_full_fallback_for_unprojected_filters():
    session = _Session([])

    response = await projection.search_plan_pricing_projection(
        session,
        _selection(),
        {
            "view": "card",
            "include_providers": "false",
            "code_system": "CPT",
            "code": "99213",
            "zip5": "62401",
            "classification": "Family Medicine",
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )

    assert response is None
    assert session.statements == []


def test_projection_ignores_explicit_false_diagnostic_flags():
    assert projection._unsupported_projection_fields(
        {
            "include_sources": "false",
            "include_evidence": "0",
            "include_details": False,
            "include_debug": "off",
        }
    ) == ()


def test_projection_rejects_unverified_locations_and_non_cost_ordering():
    assert projection._unsupported_projection_fields(
        {"include_unverified_addresses": "true"}
    ) == ("include_unverified_addresses",)
    assert projection._unsupported_projection_fields(
        {"order_by": "distance"}
    ) == ("order_by",)


@pytest.mark.asyncio
async def test_full_false_never_uses_the_projection_reader():
    session = _Session([])

    response = await projection.search_plan_pricing_projection(
        session,
        _selection(),
        {
            "view": "full",
            "include_providers": "false",
            "code_system": "CPT",
            "code": "27447",
            "state": "IL",
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )

    assert response is None
    assert session.statements == []


@pytest.mark.asyncio
async def test_projection_distinguishes_no_rates_from_no_geography():
    response = await projection.search_plan_pricing_projection(
        _Session([]),
        _selection(),
        {
            "view": "card",
            "code_system": "CPT",
            "code": "27447",
            "zip5": "62401",
            "zip_radius_miles": 0,
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )

    assert response["result_state"] == "no_matching_rates"
    assert response["items"] == []


def test_projection_migration_keeps_fragments_and_aggregates_in_one_build(
    monkeypatch,
):
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260825150000_plan_pricing_card_projection.py"
    )
    module_spec = importlib.util.spec_from_file_location(
        "plan_pricing_card_projection_migration",
        migration_path,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    statements = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "projection_test")
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    statement = " ".join(" ".join(statements).split())
    assert migration.down_revision == "20260825120000_ptg_v4_finalizer_map_pack"
    assert '"projection_test"."plan_pricing_projection_candidate"' in statement
    assert '"projection_test"."plan_pricing_card"' in statement
    assert '"projection_test"."plan_pricing_cell_aggregate"' in statement
    assert "fragment bytea NOT NULL" in statement
    assert "median_negotiated_rate numeric NOT NULL" in statement
    assert "contract_version = 'plan_pricing_card_v2'" in statement
    assert "IF to_regclass" in statement
    assert "plan_pricing_geo_zip_coordinates_idx" in statement
    assert "(latitude, longitude, zip_code)" in statement
    assert "ready plan-pricing projections are immutable" in statement
    assert "receipt counts do not match rows" in statement
    assert "SELECT state INTO parent_state" in statement
    assert "FOR UPDATE" in statement
    assert "BEFORE TRUNCATE" in statement
