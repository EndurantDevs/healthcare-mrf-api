# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact imported-field composition checks for nearby-provider pages."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from functools import partial
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters
from sqlalchemy import Integer, literal, select, text
from sqlalchemy.exc import DBAPIError

from api import provider_geo_sql as geo_sql
from api import provider_list_sql as provider_list_sql
from api.custom_import_provider_sql import ProviderImportQuery, compile_npi_entity_relation
from api.endpoint import npi as npi_module
from process.custom_import.read_core import PreparedNpiEntityRelation, ReadOrderTerm
from tests.custom_import_postgres_support import transaction_session

_FIRST_ADDRESS_KEY = "00000000-0000-0000-0000-000000000001"
_SECOND_ADDRESS_KEY = "00000000-0000-0000-0000-000000000002"
_THIRD_ADDRESS_KEY = "00000000-0000-0000-0000-000000000003"
_FOURTH_ADDRESS_KEY = "00000000-0000-0000-0000-000000000004"
_FIFTH_ADDRESS_KEY = "00000000-0000-0000-0000-000000000005"


def _context(*, direction: str | None = "asc", require_match: bool = False) -> ProviderImportQuery:
    columns = [literal("1000000002").label("entity_value")]
    order_terms: tuple[ReadOrderTerm, ...] = ()
    if direction is not None:
        columns.append(literal(None, type_=Integer()).label("sort_0"))
        order_terms = (ReadOrderTerm("synthetic_rank", direction, "last"),)
    relation = select(*columns)
    if direction is not None:
        relation = relation.union_all(
            select(literal("1000000003").label("entity_value"), literal(7, type_=Integer()).label("sort_0")),
            select(literal("1000000004").label("entity_value"), literal(2, type_=Integer()).label("sort_0")),
        )
    prepared = PreparedNpiEntityRelation(relation, order_terms, "a" * 64, "b" * 64)
    return ProviderImportQuery(prepared, compile_npi_entity_relation(relation), require_match)


def _statements(
    *,
    direction: str | None = "asc",
    require_match: bool = False,
    anchor: tuple[str, str] | None = None,
    native_parameters: dict[str, object] | None = None,
):
    return geo_sql.build_imported_geo_statements(
        _context(direction=direction, require_match=require_match),
        _imported_geo_query(anchor=anchor, native_parameters=native_parameters),
    )


def _imported_geo_query(
    *,
    anchor: tuple[str, str] | None = None,
    native_parameters: dict[str, object] | None = None,
    address_table_sql: str = "mrf.npi_address",
) -> geo_sql.ImportedGeoQuery:
    return geo_sql.ImportedGeoQuery(
        nearby=geo_sql.NearbySqlQuery(
            taxonomy_conditions="1=1",
            extra_clause="",
            ilike_clause="",
            use_taxonomy_filter=False,
            primary_only=False,
            address_table_sql=address_table_sql,
            geo_precision_clause="",
            geo_type_clause="AND (a.type = 'primary' OR a.type = 'secondary')",
        ),
        native_parameters=(
            {"in_long": 0.0, "in_lat": 0.0, "radius": 10.0} if native_parameters is None else native_parameters
        ),
        limit=2,
        cursor_anchor=anchor,
    )


@pytest.mark.parametrize("direction", ("asc", "desc"))
def test_imported_geo_builder_dedupes_before_ordering_and_uses_typed_binds(direction):
    native_parameters_by_name = {"in_long": 0.0, "in_lat": 0.0, "radius": 10.0}
    context = _context(direction=direction)
    statements = geo_sql.build_imported_geo_statements(
        context,
        _imported_geo_query(native_parameters=native_parameters_by_name),
    )

    count_sql = str(statements.count)
    page_sql = str(statements.page)
    assert native_parameters_by_name == {"in_long": 0.0, "in_lat": 0.0, "radius": 10.0}
    assert "SELECT DISTINCT ON (d.npi, a.address_key)" in count_sql
    assert "custom_import_provider_relation AS" in count_sql
    assert "LEFT JOIN custom_import_provider_relation AS imported" in page_sql
    assert page_sql.index("LEFT JOIN custom_import_provider_relation AS imported") < page_sql.index("LIMIT")
    assert "ROW_NUMBER() OVER" in page_sql
    assert f"selected_geo.sort_0 {direction.upper()} NULLS LAST" in page_sql
    assert "(selected_geo._custom_import_entity_value IS NULL) ASC" in page_sql
    assert "cursor_distance_meters ASC, selected_geo.npi_code ASC, selected_geo.address_key ASC" in page_sql
    assert "candidate_limit" not in page_sql
    assert "LIMIT" not in count_sql
    assert "a.*, d.*" not in count_sql and "ST_Distance(" not in count_sql
    assert page_sql.index("LIMIT") < page_sql.index("hydrated_geo AS MATERIALIZED") < page_sql.index("a.*, d.*")
    assert page_sql.index("hydrated_geo AS MATERIALIZED") < page_sql.index("ST_Distance(")
    assert set(context.compiled.values) <= set(statements.parameters)
    assert set(context.compiled.values) <= set(statements.page._bindparams)


@pytest.mark.parametrize("direction", (None, "asc", "desc"))
def test_imported_geo_filter_uses_one_correlated_membership_predicate(direction):
    statements = _statements(direction=direction, require_match=True)
    count_sql = str(statements.count)
    page_sql = str(statements.page)
    membership = "EXISTS (SELECT 1 FROM custom_import_provider_relation AS imported"

    assert count_sql.count(membership) == 1
    assert page_sql.count(membership) == 1
    assert "WHERE imported.entity_value = d.npi::text" in page_sql
    assert ("LEFT JOIN custom_import_provider_relation AS imported" in page_sql) is (direction is not None)


@pytest.mark.parametrize("direction", ("asc", "desc"))
def test_imported_geo_optional_order_keeps_unmatched_native_providers(direction):
    page_sql = str(_statements(direction=direction).page)
    assert "EXISTS (SELECT 1 FROM custom_import_provider_relation AS imported" not in page_sql
    assert "LEFT JOIN custom_import_provider_relation AS imported" in page_sql
    assert "WHERE imported.entity_value IS NOT NULL" not in page_sql


@pytest.mark.parametrize("direction", ("asc", "desc"))
def test_imported_geo_cursor_reconstructs_anchor_with_null_aware_keyset(direction):
    statements = _statements(
        direction=direction,
        anchor=("1000000003", _THIRD_ADDRESS_KEY),
    )
    assert statements.anchor is not None
    anchor_sql = str(statements.anchor)
    page_sql = str(statements.page)

    assert "cursor_anchor_rows AS MATERIALIZED" in anchor_sql
    assert "WHERE npi_code::text = :__custom_import_geo_cursor_npi" in anchor_sql
    assert "address_key = CAST(:__custom_import_geo_cursor_address_key AS uuid)" in anchor_sql
    assert "CROSS JOIN cursor_anchor" in page_sql
    assert "IS NOT DISTINCT FROM" in page_sql
    comparison = ">" if direction == "asc" else "<"
    assert f"selected_geo.sort_0 {comparison} cursor_anchor.sort_0" in page_sql
    assert statements.parameters["__custom_import_geo_cursor_npi"] == "1000000003"
    assert statements.parameters["__custom_import_geo_cursor_address_key"] == _THIRD_ADDRESS_KEY


def test_imported_geo_builder_rejects_bad_anchor_and_parameter_collision_without_mutation():
    native_parameters_by_name = {
        "in_long": 0.0,
        "in_lat": 0.0,
        "radius": 10.0,
        "__custom_import_geo_page_limit": 1,
    }

    with pytest.raises(ValueError, match="parameters collide"):
        _statements(native_parameters=native_parameters_by_name)
    assert native_parameters_by_name["__custom_import_geo_page_limit"] == 1
    with pytest.raises(ValueError, match="cursor anchor is invalid"):
        _statements(anchor=("1000000003", "not-an-address"))


class _RecordingGeoProxy:
    def __init__(self, _database, session, raw_connection) -> None:
        assert raw_connection is None
        self._session = session

    async def all(self, statement, **parameters):
        sql = str(statement)
        self._session.calls.append((sql, dict(parameters)))
        if "SELECT COUNT(*) AS anchor_count" in sql:
            return [SimpleNamespace(_mapping={"anchor_count": getattr(self._session, "anchor_count", 1)})]
        if "SELECT COUNT(*) AS total_count" in sql:
            return [SimpleNamespace(_mapping={"total_count": 3})]
        if "page_geo AS MATERIALIZED" in sql:
            rows = [
                _geo_row(1000000004, _FOURTH_ADDRESS_KEY, 10.0),
                _geo_row(1000000003, _THIRD_ADDRESS_KEY, 20.0),
                _geo_row(1000000002, _SECOND_ADDRESS_KEY, 30.0),
            ]
            for row in rows:
                row._mapping.update(_geo_total=3, _geo_anchor_count=getattr(self._session, "anchor_count", 1))
            return rows
        raise AssertionError(f"unexpected query: {sql}")


def _geo_row(npi: int, address_key: str, distance: float):
    return SimpleNamespace(
        _mapping={
            "npi_code": npi,
            "npi": npi,
            "address_key": address_key,
            "type": "primary",
            "distance": round(distance / 1609.34, 2),
            "cursor_distance_meters": distance,
            "entity_type_code": 1,
            "provider_first_name": "Synthetic",
            "provider_last_name": "Provider",
            "city_name": "Example City",
            "state_name": "CA",
            "postal_code": "90001",
        }
    )


async def _legacy_address_table(required_columns, *, session=None, address_table_sql="mrf.npi_address"):
    assert required_columns == npi_module._public_address_serving_column_keys()
    assert session is not None
    return address_table_sql


async def _empty_plan_scope(session, release_id):
    assert session is not None and release_id is None
    return None, {}


@pytest.mark.asyncio
@pytest.mark.parametrize("view", ("", "card"))
async def test_imported_geo_handler_uses_one_session_and_keeps_private_next_anchor(monkeypatch, view):
    session = SimpleNamespace(calls=[])
    cursor_calls = []

    async def prepare_cursor(**kwargs):
        cursor_calls.append(kwargs)
        return None

    monkeypatch.setattr(provider_list_sql, "ConnectionProxy", _RecordingGeoProxy)
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", _legacy_address_table)
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", _empty_plan_scope)
    monkeypatch.setattr(
        npi_module.db,
        "acquire",
        lambda: (_ for _ in ()).throw(AssertionError("native pool used")),
    )
    native_args = RequestParameters(
        {
            "lat": ["0"],
            "long": ["0"],
            "include_total": ["true"],
            "limit": ["2"],
            "view": [view],
        }
    )
    reply = await npi_module.get_near_npi(
        SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session)),
        native_args=native_args,
        import_context=_context(),
        prepare_cursor=prepare_cursor,
    )
    response_by_key = json.loads(reply.body)

    assert [call["session"] for call in cursor_calls] == [session]
    assert cursor_calls[0]["query_parameters"]["in_lat"] == 0.0
    assert cursor_calls[0]["query_parameters"]["in_long"] == 0.0
    assert cursor_calls[0]["limit"] == 2
    assert len(session.calls) == 1 and "AS _geo_total" in session.calls[0][0]
    assert response_by_key["total_count"] == 3
    assert response_by_key["next_cursor"] is None
    assert response_by_key["_custom_import_next_anchor"] == ["1000000003", _THIRD_ADDRESS_KEY]
    assert response_by_key["has_more"] is True
    assert len(response_by_key["items"]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("anchor_count", (0, 2))
async def test_imported_geo_handler_rejects_missing_or_nonunique_cursor_anchor(
    monkeypatch,
    anchor_count,
):
    session = SimpleNamespace(calls=[], anchor_count=anchor_count)

    async def prepare_cursor(**_kwargs):
        return "1000000009", _FIRST_ADDRESS_KEY

    monkeypatch.setattr(provider_list_sql, "ConnectionProxy", _RecordingGeoProxy)
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", _legacy_address_table)
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", _empty_plan_scope)
    monkeypatch.setattr(
        npi_module.db,
        "acquire",
        lambda: (_ for _ in ()).throw(AssertionError("native pool used")),
    )
    with pytest.raises(InvalidUsage, match="anchor is unavailable"):
        await npi_module.get_near_npi(
            SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session)),
            native_args=RequestParameters({"lat": ["0"], "long": ["0"], "include_total": ["true"]}),
            import_context=_context(),
            prepare_cursor=prepare_cursor,
        )
    assert len(session.calls) == 1 and "AS _geo_anchor_count" in session.calls[0][0]
    assert "page_geo AS MATERIALIZED" in session.calls[0][0]


@pytest.mark.asyncio
async def test_imported_geo_normalizes_before_cursor_even_when_code_resolution_is_empty(monkeypatch):
    session = SimpleNamespace(calls=[])
    cursor_state_by_key = {"was_prepared": False}

    async def prepare_cursor(**_kwargs):
        cursor_state_by_key["was_prepared"] = True
        raise InvalidUsage("custom-import geo cursor is invalid")

    async def empty_codes(*_args, **_kwargs):
        return [], []

    async def filter_year(*_args, **_kwargs):
        return 2024, "synthetic"

    async def capabilities(*_args, **_kwargs):
        return {
            "npi_procedures_array_available": True,
            "npi_medications_array_available": True,
            "pricing_provider_procedure_available": False,
            "pricing_provider_prescription_available": False,
        }

    monkeypatch.setattr(npi_module, "_address_serving_table_sql", _legacy_address_table)
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", _empty_plan_scope)
    monkeypatch.setattr(npi_module, "_resolve_internal_filter_codes", empty_codes)
    monkeypatch.setattr(npi_module, "_resolve_filter_year", filter_year)
    monkeypatch.setattr(npi_module, "_resolve_npi_filter_capabilities", capabilities)
    with pytest.raises(InvalidUsage, match="cursor is invalid"):
        await npi_module.get_near_npi(
            SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session)),
            native_args=RequestParameters(
                {
                    "lat": ["0"],
                    "long": ["0"],
                    "include_total": ["true"],
                    "procedure_codes": ["1001"],
                }
            ),
            import_context=_context(),
            prepare_cursor=prepare_cursor,
        )
    assert cursor_state_by_key["was_prepared"] is True


class _PostgresGeoProxy:
    def __init__(self, _database, session, raw_connection) -> None:
        assert raw_connection is None
        self._session = session

    async def all(self, statement, **parameters):
        self._session.info.setdefault("geo_calls", []).append((str(statement), dict(parameters)))
        return (await self._session.execute(statement, parameters)).all()


async def _seed_geo_tables(session, *, address_table_sql: str = "mrf.npi_address") -> None:
    await session.execute(
        text(
            "SELECT set_config('search_path', quote_ident(n.nspname) || ', public', true) "
            "FROM pg_extension AS e JOIN pg_namespace AS n ON n.oid = e.extnamespace WHERE e.extname = 'postgis'"
        )
    )
    await session.execute(text("CREATE SCHEMA mrf"))
    await session.execute(
        text("CREATE TABLE mrf.npi (npi bigint PRIMARY KEY, provider_first_name text DEFAULT 'Synthetic')")
    )
    is_unified = address_table_sql.endswith(".entity_address_unified")
    extra_columns = (
        ", location_key text NOT NULL, address_sources text[] NOT NULL, address_precision text DEFAULT 'rooftop'"
        if is_unified
        else ""
    )
    primary_key = "location_key" if is_unified else "checksum, npi, type"
    await session.execute(
        text(
            f"CREATE TABLE {address_table_sql} ("
            "npi bigint NOT NULL, type text NOT NULL, checksum bigint NOT NULL, lat double precision, "
            "long double precision, address_key uuid, first_line text, telephone_number text, "
            f"taxonomy_array integer[] NOT NULL DEFAULT '{{0}}'{extra_columns}, PRIMARY KEY ({primary_key}))"
        )
    )
    await session.execute(text("CREATE TABLE mrf.npi_taxonomy (npi bigint, healthcare_provider_taxonomy_code text)"))
    await session.execute(text("CREATE TABLE mrf.nucc_taxonomy (code text, display_name text, int_code integer)"))
    await session.execute(
        text("INSERT INTO mrf.npi (npi) VALUES (1000000001), (1000000002), (1000000003), (1000000004), (1000000005)")
    )
    await _seed_geo_addresses(session, address_table_sql, is_unified=is_unified)
    await session.execute(text("INSERT INTO mrf.npi_taxonomy VALUES (1000000004, '111'), (1000000004, '222')"))
    await session.execute(text("INSERT INTO mrf.nucc_taxonomy VALUES ('111', 'One', 1), ('222', 'Two', 2)"))


async def _seed_geo_addresses(session, address_table_sql: str, *, is_unified: bool) -> None:
    address_rows = [
        (1000000001, "primary", 1, 0, _FIRST_ADDRESS_KEY),
        (1000000002, "primary", 1, 0, _SECOND_ADDRESS_KEY),
        (1000000003, "primary", 3, 0, _THIRD_ADDRESS_KEY),
        (1000000003, "secondary", 6, 0, _FIFTH_ADDRESS_KEY),
        (1000000004, "primary", 4, 0.02, _FOURTH_ADDRESS_KEY),
        (1000000004, "secondary", 4, 0, _FOURTH_ADDRESS_KEY),
        (1000000005, "primary", 5, 0, _FIFTH_ADDRESS_KEY),
        (1000000005, "secondary", 5, 0, _FIFTH_ADDRESS_KEY),
        (1000000004, "secondary", 444, 0 if is_unified else 0.04, _FOURTH_ADDRESS_KEY),
    ]
    address_parameters = [
        {
            "npi": npi,
            "type": address_type,
            "checksum": checksum,
            "lat": latitude,
            "address_key": address_key,
            "first_line": f"{checksum} {address_type} Example Street",
            "location_key": f"location-{ordinal:02}",
        }
        for ordinal, (npi, address_type, checksum, latitude, address_key) in enumerate(address_rows)
    ]
    extra_columns = ", location_key, address_sources" if is_unified else ""
    extra_values = ", :location_key, ARRAY['synthetic']" if is_unified else ""
    await session.execute(
        text(
            f"INSERT INTO {address_table_sql} "
            f"(npi, type, checksum, lat, long, address_key, first_line, telephone_number{extra_columns}) "
            f"VALUES (:npi, :type, :checksum, :lat, 0, CAST(:address_key AS uuid), :first_line, '555-0100'{extra_values})"
        ),
        address_parameters,
    )


def _postgres_request(session, *, cursor_anchor=None):
    async def prepare_cursor(**_kwargs):
        return cursor_anchor

    return (
        SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session)),
        prepare_cursor,
    )


async def _read_postgres_geo_page(
    session,
    direction: str,
    cursor_anchor: tuple[str, str] | None = None,
    *,
    require_match: bool = False,
) -> dict[str, Any]:
    request, prepare_cursor = _postgres_request(
        session,
        cursor_anchor=cursor_anchor,
    )
    reply = await npi_module.get_near_npi(
        request,
        native_args=RequestParameters(
            {
                "lat": ["0"],
                "long": ["0"],
                "include_total": ["true"],
                "limit": ["2"],
            }
        ),
        import_context=_context(direction=direction, require_match=require_match),
        prepare_cursor=prepare_cursor,
    )
    return json.loads(reply.body)


def _provider_identities(page_body: Mapping[str, Any]) -> list[tuple[int, str]]:
    return [(provider["npi"], provider["address_key"]) for provider in page_body["items"]]


@pytest.mark.asyncio
@pytest.mark.parametrize("address_table_sql", ("mrf.npi_address", "mrf.entity_address_unified"))
@pytest.mark.parametrize(
    ("direction", "expected_first", "expected_second"),
    (
        (
            "asc",
            [(1000000004, _FOURTH_ADDRESS_KEY), (1000000003, _THIRD_ADDRESS_KEY)],
            [(1000000003, _FIFTH_ADDRESS_KEY), (1000000002, _SECOND_ADDRESS_KEY)],
        ),
        (
            "desc",
            [(1000000003, _THIRD_ADDRESS_KEY), (1000000003, _FIFTH_ADDRESS_KEY)],
            [(1000000004, _FOURTH_ADDRESS_KEY), (1000000002, _SECOND_ADDRESS_KEY)],
        ),
    ),
)
async def test_postgres_imported_geo_handler_executes_count_page_and_cursor(
    monkeypatch,
    direction,
    expected_first,
    expected_second,
    address_table_sql,
):
    async with transaction_session() as session:
        try:
            await session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        except DBAPIError:
            pytest.skip("PostGIS is unavailable for the native geo execution proof")
        await _seed_geo_tables(session, address_table_sql=address_table_sql)
        monkeypatch.setattr(provider_list_sql, "ConnectionProxy", _PostgresGeoProxy)
        monkeypatch.setattr(
            npi_module,
            "_address_serving_table_sql",
            partial(_legacy_address_table, address_table_sql=address_table_sql),
        )
        monkeypatch.setattr(npi_module, "_plan_release_npi_scope", _empty_plan_scope)
        monkeypatch.setattr(
            npi_module.db,
            "acquire",
            lambda: (_ for _ in ()).throw(AssertionError("native pool used")),
        )
        first_body = await _read_postgres_geo_page(session, direction)
        second_body = await _read_postgres_geo_page(
            session,
            cursor_anchor=tuple(first_body["_custom_import_next_anchor"]),
            direction=direction,
        )
        third_body = await _read_postgres_geo_page(
            session,
            cursor_anchor=tuple(second_body["_custom_import_next_anchor"]),
            direction=direction,
        )
        empty_body = await _read_postgres_geo_page(
            session,
            direction,
            cursor_anchor=(str(_provider_identities(third_body)[-1][0]), _provider_identities(third_body)[-1][1]),
        )
        calls = session.info["geo_calls"]

    assert first_body["total_count"] == 6
    assert _provider_identities(first_body) == expected_first
    assert _provider_identities(second_body) == expected_second
    assert second_body["total_count"] == 6
    assert _provider_identities(third_body) == [
        (1000000001, _FIRST_ADDRESS_KEY),
        (1000000005, _FIFTH_ADDRESS_KEY),
    ]
    combined_providers = [*first_body["items"], *second_body["items"]]
    assert next(provider["type"] for provider in combined_providers if provider["npi"] == 1000000004) == "secondary"
    assert third_body["items"][1]["type"] == "primary"
    assert empty_body["total_count"] == 6 and empty_body["items"] == []
    assert any("page_geo AS MATERIALIZED" in sql for sql, _ in calls)
    assert len(calls) == 4 and all("AS _geo_total" in sql for sql, _ in calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("address_table_sql", ("mrf.npi_address", "mrf.entity_address_unified"))
@pytest.mark.parametrize(
    ("direction", "expected_first", "expected_second"),
    (
        (
            "asc",
            [(1000000004, _FOURTH_ADDRESS_KEY), (1000000003, _THIRD_ADDRESS_KEY)],
            [(1000000003, _FIFTH_ADDRESS_KEY), (1000000002, _SECOND_ADDRESS_KEY)],
        ),
        (
            "desc",
            [(1000000003, _THIRD_ADDRESS_KEY), (1000000003, _FIFTH_ADDRESS_KEY)],
            [(1000000004, _FOURTH_ADDRESS_KEY), (1000000002, _SECOND_ADDRESS_KEY)],
        ),
    ),
)
async def test_postgres_required_geo_order_keeps_null_score_and_exact_address_pages(
    monkeypatch, direction, expected_first, expected_second, address_table_sql
):
    async with transaction_session() as session:
        try:
            await session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        except DBAPIError:
            pytest.skip("PostGIS is unavailable for the native geo execution proof")
        await _seed_geo_tables(session, address_table_sql=address_table_sql)
        monkeypatch.setattr(provider_list_sql, "ConnectionProxy", _PostgresGeoProxy)
        monkeypatch.setattr(
            npi_module,
            "_address_serving_table_sql",
            partial(_legacy_address_table, address_table_sql=address_table_sql),
        )
        monkeypatch.setattr(npi_module, "_plan_release_npi_scope", _empty_plan_scope)
        monkeypatch.setattr(npi_module.db, "acquire", lambda: (_ for _ in ()).throw(AssertionError("native pool used")))
        first_body = await _read_postgres_geo_page(session, direction, require_match=True)
        second_body = await _read_postgres_geo_page(
            session, direction, tuple(first_body["_custom_import_next_anchor"]), require_match=True
        )
        last_provider = second_body["items"][-1]
        exhausted_body = await _read_postgres_geo_page(
            session, direction, (str(last_provider["npi"]), last_provider["address_key"]), require_match=True
        )
        calls = session.info["geo_calls"]
    assert _provider_identities(first_body) == expected_first
    assert _provider_identities(second_body) == expected_second
    assert first_body["has_more"] is True and second_body["has_more"] is False
    assert exhausted_body["items"] == [] and exhausted_body["has_more"] is False
    assert all(page["total_count"] == 4 for page in (first_body, second_body, exhausted_body))
    providers = [*first_body["items"], *second_body["items"]]
    assert next(provider["type"] for provider in providers if provider["npi"] == 1000000004) == "secondary"
    assert providers[-1]["npi"] == 1000000002
    assert len(calls) == 3 and all("AS _geo_total" in sql for sql, _ in calls)


def _wide_geo_page_statement(context, address_table_sql: str, direction: str):
    sql = (Path(__file__).parent / "fixtures" / "custom_import_provider_geo_wide_page.sql").read_text()
    return text(
        sql.format(
            address_table=address_table_sql,
            address_tiebreaker=geo_sql.nearby_row_tiebreaker(address_table_sql),
            direction=direction.upper(),
            membership_clause=(
                "\n       AND EXISTS (SELECT 1 FROM custom_import_provider_relation AS imported "
                "WHERE imported.entity_value = d.npi::text)"
                if context.require_match
                else ""
            ),
            require_match_clause="\n     WHERE imported.entity_value IS NOT NULL" if context.require_match else "",
        )
    ).bindparams(*context.compiled.typed_binds)


def _ordered_geo_payload_rows(result):
    return [
        tuple(row)
        for row in sorted(
            result.all(),
            key=lambda row: (
                row._mapping["_geo_page_position"] or 0,
                row._mapping["healthcare_provider_taxonomy_code"] or "",
            ),
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("address_table_sql", ("mrf.npi_address", "mrf.entity_address_unified"))
@pytest.mark.parametrize("direction", ("asc", "desc"))
@pytest.mark.parametrize("require_match", (False, True))
async def test_postgres_imported_geo_page_preserves_wide_payload(address_table_sql, direction, require_match):
    async with transaction_session() as session:
        try:
            await session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        except DBAPIError:
            pytest.skip("PostGIS is unavailable for the native geo execution proof")
        await _seed_geo_tables(session, address_table_sql=address_table_sql)
        context = _context(direction=direction, require_match=require_match)
        query = _imported_geo_query(address_table_sql=address_table_sql)
        for limit in (2, 50):
            statements = geo_sql.build_imported_geo_statements(context, replace(query, limit=limit))
            wide_result = await session.execute(
                _wide_geo_page_statement(context, address_table_sql, direction), statements.parameters
            )
            hydrated_result = await session.execute(statements.page, statements.parameters)
            assert list(hydrated_result.keys()) == list(wide_result.keys())
            assert _ordered_geo_payload_rows(hydrated_result) == _ordered_geo_payload_rows(wide_result)


@pytest.mark.asyncio
@pytest.mark.parametrize("address_table_sql", ("mrf.npi_address", "mrf.entity_address_unified"))
async def test_postgres_imported_geo_without_order_preserves_empty_total_and_hydration(address_table_sql):
    async with transaction_session() as session:
        try:
            await session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        except DBAPIError:
            pytest.skip("PostGIS is unavailable for the native geo execution proof")
        await _seed_geo_tables(session, address_table_sql=address_table_sql)
        context = _context(direction=None, require_match=True)
        query = _imported_geo_query(address_table_sql=address_table_sql)
        statements = geo_sql.build_imported_geo_statements(context, query)
        rows = (await session.execute(statements.page, statements.parameters)).all()
        assert len(rows) == 1 and rows[0]._mapping["npi_code"] == 1000000002
        assert rows[0]._mapping["first_line"] == "1 primary Example Street"
        assert rows[0]._mapping["_geo_total"] == 1
        exhausted = geo_sql.build_imported_geo_statements(
            context, replace(query, cursor_anchor=("1000000002", _SECOND_ADDRESS_KEY))
        )
        empty_rows = (await session.execute(exhausted.page, exhausted.parameters)).all()
        assert len(empty_rows) == 1 and empty_rows[0]._mapping["npi_code"] is None
        assert empty_rows[0]._mapping["_geo_total"] == 1 and empty_rows[0]._mapping["_geo_anchor_count"] == 1
        absent = geo_sql.build_imported_geo_statements(
            context, replace(query, native_parameters={"in_long": 90.0, "in_lat": 0.0, "radius": 1.0})
        )
        absent_rows = (await session.execute(absent.page, absent.parameters)).all()
        assert len(absent_rows) == 1 and absent_rows[0]._mapping["npi_code"] is None
        assert absent_rows[0]._mapping["_geo_total"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("address_table_sql", ("mrf.npi_address", "mrf.entity_address_unified"))
async def test_postgres_imported_geo_hydrates_mixed_sort_fields_across_null_cursor(address_table_sql):
    relation_queries = [
        select(
            literal(npi).label("entity_value"),
            literal(first_rank, type_=Integer()).label("sort_0"),
            literal(second_rank, type_=Integer()).label("sort_1"),
        )
        for npi, first_rank, second_rank in (("1000000002", None, 9), ("1000000003", 7, 2), ("1000000004", 7, 8))
    ]
    relation = relation_queries[0].union_all(*relation_queries[1:])
    order_terms = (
        ReadOrderTerm("synthetic_rank", "asc", "last"),
        ReadOrderTerm("synthetic_secondary_rank", "desc", "last"),
    )
    prepared = PreparedNpiEntityRelation(relation, order_terms, "a" * 64, "b" * 64)
    context = ProviderImportQuery(prepared, compile_npi_entity_relation(relation), True)
    async with transaction_session() as session:
        try:
            await session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        except DBAPIError:
            pytest.skip("PostGIS is unavailable for the native geo execution proof")
        await _seed_geo_tables(session, address_table_sql=address_table_sql)
        query = _imported_geo_query(address_table_sql=address_table_sql)
        first = geo_sql.build_imported_geo_statements(context, query)
        first_rows = (await session.execute(first.page, first.parameters)).all()
        assert list(
            dict.fromkeys(
                (geo_row._mapping["npi_code"], str(geo_row._mapping["address_key"])) for geo_row in first_rows
            )
        ) == [
            (1000000004, _FOURTH_ADDRESS_KEY),
            (1000000003, _THIRD_ADDRESS_KEY),
            (1000000003, _FIFTH_ADDRESS_KEY),
        ]
        assert [geo_row._mapping["sort_1"] for geo_row in first_rows] == [8, 8, 2, 2]
        second = geo_sql.build_imported_geo_statements(
            context, replace(query, cursor_anchor=("1000000003", _THIRD_ADDRESS_KEY))
        )
        second_rows = (await session.execute(second.page, second.parameters)).all()
        assert [(geo_row._mapping["npi_code"], str(geo_row._mapping["address_key"])) for geo_row in second_rows] == [
            (1000000003, _FIFTH_ADDRESS_KEY),
            (1000000002, _SECOND_ADDRESS_KEY),
        ]
        assert second_rows[-1]._mapping["sort_0"] is None and second_rows[-1]._mapping["sort_1"] == 9
        assert all(
            geo_row._mapping["_geo_total"] == 4 and geo_row._mapping["_geo_anchor_count"] == 1
            for geo_row in second_rows
        )
