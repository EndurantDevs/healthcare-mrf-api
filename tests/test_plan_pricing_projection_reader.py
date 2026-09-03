# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-path and migration contracts for the plan-pricing projection."""

from __future__ import annotations

import importlib.util
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace

import orjson
import pytest

from api import plan_pricing_projection as projection
from api import plan_pricing_projection_source as projection_source

from .test_plan_pricing_projection import PROJECTION_ID, _selection, _Session


def _projection_migration():
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
    return migration


@pytest.mark.asyncio
async def test_provider_projection_rejects_row_overflow_before_grouping(
    monkeypatch,
):
    monkeypatch.setattr(projection_source, "MAX_PROVIDER_ROWS_PER_BATCH", 1)
    provider_row_by_field = {
        "npi": 1234567890,
        "zip5": "60601",
        "state": "IL",
    }

    with pytest.raises(ValueError, match="provider-row bound exceeded"):
        await projection._projection_provider_rows_for_npis(
            _Session([provider_row_by_field, provider_row_by_field]),
            [1234567890],
        )


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
        (
            {
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
        projection.LEGACY_PROJECTION_CONTRACT
    )
    assert wire_response["query"]["include_providers"] is (
        expected_result_type == "provider_cards"
    )
    assert wire_response["query"]["view"] == str(
        args.get("view") or "full"
    )
    selection = _selection()
    assert wire_response["plan_version_id"] == selection.plan_version_id
    assert wire_response["serving_revision_id"] == (
        selection.serving_revision_id
    )
    assert wire_response["serving_revision_published_at"] == (
        selection.serving_revision_published_at
    )
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
    migration = _projection_migration()
    statements = []
    created_indexes = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "projection_test")
    monkeypatch.setattr(migration.op, "execute", statements.append)
    monkeypatch.setattr(
        migration.op,
        "get_context",
        lambda: SimpleNamespace(as_sql=False, autocommit_block=nullcontext),
    )
    monkeypatch.setattr(
        migration,
        "_zip_table_has_index_columns",
        lambda _schema: True,
    )
    monkeypatch.setattr(migration, "_zip_index_record", lambda _schema: None)
    monkeypatch.setattr(
        migration.op,
        "create_index",
        lambda *positional, **keyword: created_indexes.append(
            (positional, keyword)
        ),
    )

    migration.upgrade()

    statement = " ".join(" ".join(statements).split())
    assert migration.down_revision == "20260826090000_hospital_price_packed_blocks"
    assert '"projection_test"."plan_pricing_projection_candidate"' in statement
    assert '"projection_test"."plan_pricing_card"' in statement
    assert '"projection_test"."plan_pricing_cell_aggregate"' in statement
    assert "fragment bytea NOT NULL" in statement
    assert "median_negotiated_rate numeric NOT NULL" in statement
    assert "contract_version = 'plan_pricing_card_v2'" in statement
    assert "plan_pricing_cell_aggregate_lookup_idx" not in statement
    assert created_indexes == [
        (
            (
                "plan_pricing_geo_zip_coordinates_idx",
                "geo_zip_lookup",
                ["latitude", "longitude", "zip_code"],
            ),
            {
                "schema": "projection_test",
                "if_not_exists": True,
                "postgresql_concurrently": True,
            },
        )
    ]
    assert "ready plan-pricing projections are immutable" in statement
    assert "receipt counts do not match rows" in statement
    assert "SELECT state INTO parent_state" in statement
    assert "FOR UPDATE" in statement
    assert "BEFORE TRUNCATE" in statement


def test_projection_migration_skips_zip_index_without_required_columns(
    monkeypatch,
):
    migration = _projection_migration()
    created_indexes = []
    monkeypatch.setattr(migration.op, "execute", lambda _statement: None)
    monkeypatch.setattr(
        migration.op,
        "get_context",
        lambda: SimpleNamespace(as_sql=False, autocommit_block=nullcontext),
    )
    monkeypatch.setattr(
        migration,
        "_zip_table_has_index_columns",
        lambda _schema: False,
    )
    monkeypatch.setattr(migration, "_zip_index_record", lambda _schema: None)
    monkeypatch.setattr(
        migration.op,
        "create_index",
        lambda *positional, **keyword: created_indexes.append(
            (positional, keyword)
        ),
    )

    migration.upgrade()

    assert created_indexes == []
