# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named provider queries preserve bounded pages, geo mapping and complete price data."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_em_distance as distance
from api import plan_pricing_em_managed as managed
from api import provider_allowed_amount_policy as allowed
from api import ptg2_geo_policy as geo_policy
from api import ptg2_geo_projection as geo_projection
from api.network_address_scope import scoped_address_relation_sql
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from process import network_em_provider_inputs as inputs
from tests import test_network_em_api_metadata as metadata
from tests import test_network_em_provider_inputs as fixture
from tests.test_provider_group_plan_network_read import held_scope


async def _input_session(monkeypatch):
    driver, manifest, _actual, resolver = await metadata._input_reader(monkeypatch)
    connection = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=driver)))
    session = SimpleNamespace(info={}, get_transaction=lambda: object(), connection=AsyncMock(return_value=connection))
    return session, manifest, resolver


async def test_session_inputs_read_complete_named_provider_recipe(monkeypatch):
    session, manifest, resolver = await _input_session(monkeypatch)
    actual = await inputs.resolve_em_provider_inputs_in_session(session, manifest, "control")
    assert actual == fixture.family()
    resolver.assert_awaited_once_with(
        session.connection.return_value.get_raw_connection.return_value.driver_connection,
        generation_id=manifest.generation_id,
        control_schema="control",
        api_read=True,
    )
    assert len(actual.relations) == 9 and all(
        set(relation.__dataclass_fields__) == {"role", "schema_name", "table_name", "generation_id"}
        for relation in actual.relations
    )
    assert session.info == {}


async def test_session_input_cancellation_is_preserved(monkeypatch):
    session, manifest, resolver = await _input_session(monkeypatch)
    resolver.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await inputs.resolve_em_provider_inputs_in_session(session, manifest, "control")
    assert session.info == {}


class _PageResult:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return SimpleNamespace(all=lambda: self.rows)


@pytest.mark.parametrize("projection_ready", [False, True])
async def test_progressive_windows_require_ready_projection_and_complete_page(monkeypatch, projection_ready):
    session, manifest, _resolver = await _input_session(monkeypatch)
    await inputs.resolve_em_provider_inputs_in_session(session, manifest, "control")
    pages = []
    page_parameter_rows = []

    async def execute(_statement, parameters):
        page_parameter_rows.append(parameters)
        rows = [{"projection_ready": projection_ready, "candidate_count": 128 if not pages else 1, "unique_count": 1}]
        pages.append(rows)
        return _PageResult(rows)

    session.execute = execute
    page = await distance._progressive_page_rows(
        session,
        SimpleNamespace(serving_revision_id="example", binding_set_digest="a" * 64),
        {"radius_miles": "5"},
        SimpleNamespace(limit=2, offset=0),
        ("60611", None, None),
        3,
    )
    if projection_ready:
        assert page == (pages[-1], True, False, 1) and len(pages) == 2
        assert [parameters["candidate_limit"] for parameters in page_parameter_rows] == [128, 256]
    else:
        assert page is None and len(pages) == 1
    assert all(parameters["page_limit"] == 3 and parameters["code_bit"] == 8 for parameters in page_parameter_rows)


@pytest.mark.parametrize("damage", [None, "count", "bytes", "missing", "duplicate", "json", "incomplete"])
async def test_native_completion_checks_bounded_payload_and_exact_provider_identity(monkeypatch, damage):
    session, manifest, _resolver = await _input_session(monkeypatch)
    held = await inputs.resolve_em_provider_inputs_in_session(session, manifest, "control")
    rate_rows = [{"npi": 1003000000, "minimum_rates": ["10"] * 6, "maximum_rates": ["20"] * 6, "rate_counts": [2] * 6}]
    if damage == "missing":
        rate_rows[0]["npi"] = 1003000001
    if damage == "duplicate":
        rate_rows *= 2
    encoded = json.dumps(rate_rows)
    envelope_by_field = {"count": len(rate_rows), "bytes": len(encoded), "rows_json": encoded}
    if damage == "count":
        envelope_by_field["count"] = 5001
    if damage == "bytes":
        envelope_by_field["bytes"] = 8 * 1024 * 1024 + 1
    if damage == "json":
        envelope_by_field["rows_json"] = "{"
    if damage == "incomplete":
        envelope_by_field["count"] += 1

    async def fetchrow(query, *parameters):
        assert query.startswith("WITH source_rows AS MATERIALIZED (")
        assert parameters == ([1003000000], [3, 11], 3)
        assert held.relation("npi") in query and '"held_rates"."rates"' in query
        return envelope_by_field

    driver = session.connection.return_value.get_raw_connection.return_value.driver_connection
    monkeypatch.setattr(driver, "fetchrow", fetchrow)
    provider_rows = [{"npi": 1003000000, "distance_miles": 1.5}]
    if damage:
        with pytest.raises(PlanPricingProjectionUnavailable):
            await managed._complete_page(session, held, '"held_rates"."rates"', provider_rows, (3, 11), 3)
    else:
        cards = await managed._complete_page(session, held, '"held_rates"."rates"', provider_rows, (3, 11), 3)
        assert cards[0]["minimum_negotiated_rate"] == 10 and cards[0]["distance_miles"] == 1.5


def test_managed_and_allowed_geo_emitters_use_six_verified_heaps():
    held = fixture.family()
    bindings = held.geo_bindings("mrf")
    with held_scope() as manifest:
        page = managed._page_sql(held, '"held_rates"."rates"')
        joins = allowed.allowed_geo_joins(held, "mrf")
        evidence = allowed.allowed_geo_evidence(held, "mrf")
        for binding in bindings.values():
            relation = f'"{binding["schema_name"]}"."{binding["table_name"]}"'
            for sql in (page, joins + evidence):
                assert f"FROM {relation} " not in sql and f"JOIN {relation} " not in sql
                assert f"FROM ONLY {relation} " in sql or f"JOIN ONLY {relation} " in sql
        assert f"FROM {scoped_address_relation_sql('mrf.entity_address_unified')} addr" in page
        assert f'FROM "{manifest.schema_name}"."entity_address_unified" AS geo_nppes_anchor' in evidence
        assert "ONLY (" not in page + joins + evidence
        assert "rate.binding_ordinal=ANY(CAST(:allowed_binding_ordinals AS integer[]))" in page
        assert 'FROM ONLY "held_rates"."rates" rate' in page
        raw_evidence = geo_projection.legacy_evidence_source_id_sql(
            "addr", schema_name="mrf", dependency_bindings=bindings, unified_schema_name=manifest.schema_name
        )
        assert evidence.replace("FROM ONLY ", "FROM ").replace("JOIN ONLY ", "JOIN ") == f"({raw_evidence})<>0"
    raw_joins = (
        geo_policy.provider_address_identity_reference_joins_sql(
            "addr",
            schema_name="mrf",
            geo_zip_alias="allowed_zip_ref",
            zip_state_alias="allowed_state_ref",
            dependency_bindings=bindings,
        )
        + "\nLEFT "
        + geo_policy.provider_address_point_reference_join_sql(
            "addr", zcta_alias="allowed_zcta_ref", schema_name="mrf", dependency_bindings=bindings
        )
    )
    assert joins.replace("JOIN ONLY ", "JOIN ") == raw_joins
    assert inputs.em_provider_geo_reads_sql(joins, held, "mrf") == joins
    signature = geo_projection.projection_dependency_bindings_match_sql("mrf", bindings)
    assert "to_regclass" in signature
    assert inputs.em_provider_geo_reads_sql(signature, held, "mrf") == signature
    completion = managed._completion_sql(held, '"held_rates"."rates"')
    for role in ("npi", "npi_taxonomy", "nucc_taxonomy"):
        assert "ONLY " + held.relation(role) in completion
        assert "FROM " + held.relation(role) + " " not in completion
        assert "JOIN " + held.relation(role) + " " not in completion
    assert 'FROM ONLY "held_rates"."rates" rate' in completion


@pytest.mark.parametrize("damage", ["type", "family", "binding", "sql", "schema"])
def test_direct_geo_renderer_rejects_invalid_input_shapes(damage):
    held, sql, schema = fixture.family(), "SELECT 1", "mrf"
    if damage == "type":
        held = SimpleNamespace()
    elif damage == "family":
        object.__setattr__(held, "relations", held.relations[:-1])
    elif damage == "binding":
        object.__setattr__(held.relations[1], "table_name", "invalid.name")
    elif damage == "sql":
        sql = None
    else:
        schema = "invalid.schema"
    with pytest.raises(ValueError):
        inputs.em_provider_geo_reads_sql(sql, held, schema)
