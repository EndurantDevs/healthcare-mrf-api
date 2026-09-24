# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Provider-list composition stays bounded inside the signed transaction."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest
from sanic.exceptions import InvalidUsage
from sanic.request.parameters import RequestParameters
from sqlalchemy import Integer, literal, select, text

from api import provider_list_sql as provider_list_sql_module
from api.custom_import_provider_sql import ProviderImportQuery, compile_npi_entity_relation
from api.endpoint import npi as npi_module
from process.custom_import.read_core import PreparedNpiEntityRelation, ReadOrderTerm
from tests.custom_import_postgres_support import transaction_session


class _TransactionConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any], Any]] = []

    async def all(self, statement, **parameters):
        sql = str(statement)
        self.calls.append((sql, dict(parameters), statement))
        if "SELECT COUNT(DISTINCT" in sql:
            return [(501,)]
        if "page_npis AS" in sql:
            return [
                SimpleNamespace(
                    _mapping={
                        "npi_code": 1000000002,
                        "npi": 1000000002,
                        "entity_type_code": 2,
                        "provider_organization_name": "Synthetic Provider",
                        "type": "primary",
                        "first_line": "1 Example Road",
                        "city_name": "Example City",
                        "state_name": "CA",
                        "postal_code": "90001",
                        "country_code": "US",
                        "telephone_number": "2125550100",
                        "address_key": "10000000-0000-0000-0000-000000000002",
                        "provider_address_total": 1,
                    }
                )
            ]
        if "SELECT taxonomy.*" in sql:
            return []
        raise AssertionError(f"unexpected statement: {sql}")


class _Session:
    def __init__(self, connection: _TransactionConnection) -> None:
        self._connection = connection
        self.proxy_databases: list[Any] = []
        self.proxy_inputs: list[Any] = []
        self.events: list[str] = []

    async def all(self, statement, **parameters):
        return await self._connection.all(statement, **parameters)


class _TrackingConnectionProxy:
    def __init__(self, database, connection: _Session, raw_connection) -> None:
        assert raw_connection is None
        self._connection = connection
        connection.proxy_databases.append(database)
        connection.proxy_inputs.append(connection)

    async def all(self, statement, **parameters):
        sql = str(statement)
        if "SELECT COUNT(DISTINCT" in sql:
            self._connection.events.append("count")
        elif "page_npis AS" in sql:
            self._connection.events.append("page")
        elif "SELECT taxonomy.*" in sql:
            self._connection.events.append("taxonomy")
        return await self._connection.all(statement, **parameters)


def _import_context(*, direction: str | None, require_match: bool) -> ProviderImportQuery:
    columns = [literal("1000000002").label("entity_value")]
    order_terms: tuple[ReadOrderTerm, ...] = ()
    if direction is not None:
        columns.append(literal(7).label("sort_0"))
        order_terms = (ReadOrderTerm("synthetic_rank", direction, "last"),)
    statement = select(*columns)
    prepared = PreparedNpiEntityRelation(
        statement,
        order_terms,
        "a" * 64,
        "b" * 64,
    )
    return ProviderImportQuery(
        prepared,
        compile_npi_entity_relation(statement),
        require_match,
    )


async def _legacy_address_table(required_columns, *, session=None):
    assert required_columns == npi_module._public_address_serving_column_keys()
    assert session is not None
    return "mrf.npi_address"


async def _empty_plan_scope(session, release_id):
    assert session is not None and release_id is None
    return None, {}


async def _mark_active_statuses(
    locations,
    *,
    session=None,
    use_request_session=False,
    fail_closed=False,
):
    assert session is not None and use_request_session is True and fail_closed is True
    session.events.append("status")
    for location in locations:
        location["location_status"] = "active"


async def _empty_enrichment(npis, *, include_chain=False, session=None):
    assert npis == [1000000002]
    assert include_chain is False and session is not None
    session.events.append("enrichment")
    return {}


def _configure_imported_page_dependencies(monkeypatch) -> None:
    monkeypatch.setattr(
        provider_list_sql_module,
        "ConnectionProxy",
        _TrackingConnectionProxy,
    )
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", _legacy_address_table)
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", _empty_plan_scope)
    monkeypatch.setattr(npi_module, "_apply_location_statuses", _mark_active_statuses)
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", _empty_enrichment)
    monkeypatch.setattr(
        npi_module,
        "db",
        SimpleNamespace(acquire=lambda: (_ for _ in ()).throw(AssertionError("native pool used"))),
    )


async def _run_imported_page(
    monkeypatch,
    *,
    direction: str | None,
    require_match: bool,
) -> tuple[dict[str, Any], _TransactionConnection, _Session, ProviderImportQuery]:
    connection = _TransactionConnection()
    session = _Session(connection)
    context = _import_context(direction=direction, require_match=require_match)
    _configure_imported_page_dependencies(monkeypatch)
    request = SimpleNamespace(
        args={"q": "ignored-by-signed-request"},
        ctx=SimpleNamespace(sa_session=session),
    )
    native_args = RequestParameters(
        {
            "include_total": ["true"],
            "limit": ["1"],
            "start": ["0"],
            "phone": ["2125550100"],
        }
    )

    reply = await npi_module.list_providers(
        request,
        native_args=native_args,
        import_context=context,
    )

    assert session.events == ["count", "page", "taxonomy", "status", "enrichment"]
    return json.loads(reply.body), connection, session, context


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ("asc", "desc"))
@pytest.mark.parametrize("require_match", (False, True))
async def test_imported_order_page_keeps_native_rows_and_uses_one_transaction(
    monkeypatch,
    direction,
    require_match,
):
    body, connection, session, context = await _run_imported_page(
        monkeypatch,
        direction=direction,
        require_match=require_match,
    )

    assert body["total"] == 501
    assert body["total"] > npi_module.MAX_PROVIDER_LIST_PHONE_CANDIDATES
    assert body["total_source"] == "computed"
    assert [provider_row["npi"] for provider_row in body["rows"]] == [1000000002]
    assert session.proxy_databases == [npi_module.db, npi_module.db, npi_module.db]
    assert session.proxy_inputs == [session, session, session]

    count_sql, count_parameters, count_statement = connection.calls[0]
    page_sql, page_parameters, page_statement = connection.calls[1]
    assert "custom_import_provider_relation AS" in count_sql
    assert "LEFT JOIN custom_import_provider_relation AS imported" in page_sql
    assert page_sql.index("LEFT JOIN custom_import_provider_relation AS imported") < page_sql.index("LIMIT :limit")
    assert ("WHERE imported.entity_value IS NOT NULL" in page_sql) is require_match
    assert "ROW_NUMBER() OVER" in page_sql
    assert "_provider_page_position" in page_sql
    assert "candidate_limit" not in count_parameters
    assert "candidate_limit" not in page_parameters
    assert "regexp_replace(COALESCE(c.telephone_number" in page_sql
    assert f"imported.sort_0 {direction.upper()} NULLS LAST" in page_sql
    assert "(imported.entity_value IS NULL) ASC" in page_sql
    assert "ORDER BY sub_s._provider_page_position ASC, sub_s.npi_code ASC" in page_sql
    assert set(context.compiled.values) <= set(count_parameters)
    assert set(context.compiled.values) <= set(page_parameters)
    for statement in (count_statement, page_statement):
        assert set(context.compiled.values) <= set(statement._bindparams)


@pytest.mark.asyncio
async def test_imported_filter_page_uses_one_correlated_membership_predicate(
    monkeypatch,
):
    body, connection, _, context = await _run_imported_page(
        monkeypatch,
        direction=None,
        require_match=True,
    )

    assert body["total"] == 501
    count_sql = connection.calls[0][0]
    page_sql = connection.calls[1][0]
    predicate = "EXISTS (SELECT 1 FROM custom_import_provider_relation AS imported"
    assert count_sql.count(predicate) == 1
    assert page_sql.count(predicate) == 1
    assert "LEFT JOIN custom_import_provider_relation AS imported" not in page_sql
    assert set(context.compiled.values) <= set(connection.calls[0][1])
    assert set(context.compiled.values) <= set(connection.calls[1][1])


@pytest.mark.asyncio
async def test_imported_arguments_stay_paired_and_invalid_plan_network_is_bad_request():
    request = SimpleNamespace(args={})
    context = _import_context(direction="asc", require_match=False)

    with pytest.raises(InvalidUsage, match="arguments are invalid"):
        await npi_module.list_providers(
            request,
            native_args=RequestParameters({}),
        )
    with pytest.raises(InvalidUsage, match="arguments are invalid"):
        await npi_module.list_providers(request, import_context=context)
    with pytest.raises(InvalidUsage, match="exact totals"):
        await npi_module.list_providers(
            request,
            native_args=RequestParameters({"include_total": ["false"]}),
            import_context=context,
        )
    with pytest.raises(InvalidUsage, match="imported ordering"):
        await npi_module.list_providers(
            request,
            native_args=RequestParameters({"include_total": ["true"]}),
            import_context=_import_context(direction=None, require_match=False),
        )
    with pytest.raises(InvalidUsage, match="plan_network must contain integers"):
        await npi_module.list_providers(
            request,
            native_args=RequestParameters({"plan_network": ["3,not-an-integer"]}),
            import_context=context,
        )


def test_extract_name_filters_uses_the_supplied_native_arguments():
    request = SimpleNamespace(args={"name_like": "ignored"})
    native_args = RequestParameters({"name_like": ["Synthetic", "EXAMPLE"]})

    assert npi_module._extract_name_filters(request, args=native_args) == [
        "synthetic",
        "example",
    ]


def test_extract_name_filters_retains_single_name_when_getall_fails():
    class FailingMultiValueArgs:
        def getall(self, _key):
            raise ValueError("invalid multi-value input")

        def get(self, _key):
            return "Synthetic"

    assert npi_module._extract_name_filters(SimpleNamespace(args={}), args=FailingMultiValueArgs()) == ["synthetic"]


def _postgres_order_contexts(direction: str) -> tuple[ProviderImportQuery, ProviderImportQuery]:
    relation_statement = select(
        literal("1000000002").label("entity_value"),
        literal(None, type_=Integer()).label("sort_0"),
    ).union_all(
        select(
            literal("1000000003").label("entity_value"),
            literal(7, type_=Integer()).label("sort_0"),
        ),
        select(
            literal("1000000004").label("entity_value"),
            literal(2, type_=Integer()).label("sort_0"),
        ),
    )
    prepared = PreparedNpiEntityRelation(
        relation_statement,
        (ReadOrderTerm("synthetic_rank", direction, "last"),),
        "a" * 64,
        "b" * 64,
    )
    ordered_context = ProviderImportQuery(
        prepared,
        compile_npi_entity_relation(relation_statement),
        False,
    )
    matching_context = ProviderImportQuery(
        prepared,
        ordered_context.compiled,
        True,
    )
    return ordered_context, matching_context


def _postgres_page_statement(ordered_context: ProviderImportQuery):
    relation_cte = npi_module._provider_import_relation_cte(ordered_context)
    order_sql = npi_module._provider_import_order_clause(
        ordered_context,
        "native.npi",
    )
    page_statement = npi_module._provider_list_statement(
        f"""
        WITH {relation_cte},
        native(npi) AS (
            VALUES
                (1000000001::bigint),
                (1000000002::bigint),
                (1000000003::bigint),
                (1000000004::bigint)
        ),
        page_npis AS (
            SELECT native.npi,
                   ROW_NUMBER() OVER (ORDER BY {order_sql}) AS page_position
              FROM native
         LEFT JOIN custom_import_provider_relation AS imported
                ON imported.entity_value = native.npi::text
             ORDER BY {order_sql}
             LIMIT :limit OFFSET :start
        )
        SELECT npi FROM page_npis ORDER BY page_position
        """,
        ordered_context,
    )
    return page_statement


def _postgres_matching_count_statement(matching_context: ProviderImportQuery):
    relation_cte = npi_module._provider_import_relation_cte(matching_context)
    return npi_module._provider_list_statement(
        f"""
        WITH {relation_cte},
        native(npi) AS (
            SELECT 1000000000 + value
              FROM generate_series(1, 501) AS generated(value)
        )
        SELECT COUNT(DISTINCT native.npi)
          FROM native
         WHERE {npi_module._provider_import_membership_clause(matching_context, "native.npi")}
        """,
        matching_context,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("direction", "expected_page"),
    (
        ("asc", [1000000004, 1000000003, 1000000002, 1000000001]),
        ("desc", [1000000003, 1000000004, 1000000002, 1000000001]),
    ),
)
async def test_postgres_imported_order_keeps_matched_null_before_absent_native(
    direction,
    expected_page,
):
    ordered_context, matching_context = _postgres_order_contexts(direction)
    page_statement = _postgres_page_statement(ordered_context)
    count_statement = _postgres_matching_count_statement(matching_context)

    async with transaction_session() as session:
        page = (
            (
                await session.execute(
                    page_statement,
                    npi_module._provider_list_parameters(
                        {"limit": 4, "start": 0},
                        ordered_context,
                    ),
                )
            )
            .scalars()
            .all()
        )
        matching_total = await session.scalar(
            count_statement,
            npi_module._provider_list_parameters({}, matching_context),
        )

    assert page == expected_page
    assert matching_total == 3


class _PostgresConnectionProxy:
    def __init__(self, _database, connection, raw_connection) -> None:
        assert raw_connection is None
        self._connection = connection

    async def all(self, statement, **parameters):
        calls = self._connection.info.setdefault("provider_list_calls", [])
        calls.append((str(statement), dict(parameters)))
        execution_result = await self._connection.execute(statement, parameters)
        return execution_result.all()


async def _seed_postgres_provider_list_tables(session) -> None:
    await session.execute(text("CREATE SCHEMA mrf"))
    await session.execute(text("CREATE TABLE mrf.npi (npi bigint PRIMARY KEY)"))
    await session.execute(
        text(
            "CREATE TABLE mrf.npi_address ("
            "npi bigint NOT NULL, type text NOT NULL, lat double precision, "
            "long double precision, first_line text, date_added timestamptz, "
            "checksum text, address_key uuid)"
        )
    )
    await session.execute(
        text("CREATE TABLE mrf.npi_taxonomy (npi bigint, healthcare_provider_taxonomy_code text, checksum text)")
    )
    await session.execute(text("CREATE TABLE mrf.nucc_taxonomy (code text, display_name text)"))
    await session.execute(
        text("INSERT INTO mrf.npi (npi) VALUES (1000000001), (1000000002), (1000000003), (1000000004)")
    )
    await session.execute(
        text(
            "INSERT INTO mrf.npi_address "
            "(npi, type, lat, long, first_line, date_added, checksum) VALUES "
            "(1000000001, 'primary', 1, 1, 'One Road', now(), 'one'), "
            "(1000000002, 'primary', 1, 1, 'Two Road', now(), 'two'), "
            "(1000000003, 'primary', 1, 1, 'Three Road', now(), 'three'), "
            "(1000000004, 'primary', 1, 1, 'Four Road', now(), 'four')"
        )
    )


_POSTGRES_PHONE_PROVIDER_TABLE_STATEMENTS = (
    "CREATE SCHEMA mrf",
    "CREATE TABLE mrf.npi (npi bigint PRIMARY KEY)",
    """
    CREATE TABLE mrf.entity_address_unified (
        checksum bigint, npi bigint, inferred_npi bigint, type text,
        first_line text, second_line text, city_name text, state_name text,
        state_code text, postal_code text, country_code text,
        telephone_number text, phone_number text, formatted_address text,
        location_key text, address_key uuid, premise_key uuid,
        address_precision text, lat double precision, long double precision,
        source_count integer, independent_source_count integer,
        multi_source_confirmed boolean, address_sources varchar[],
        source_record_ids varchar[], updated_at timestamptz,
        last_seen_at timestamptz, date_added date, entity_name text
    )
    """,
    """
    CREATE TABLE mrf.provider_directory_source (
        source_id varchar, endpoint_id varchar
    )
    """,
    """
    CREATE TABLE mrf.provider_directory_endpoint_dataset (
        dataset_id varchar, endpoint_id varchar, acquisition_root_run_id varchar,
        import_run_id varchar, is_current boolean, status varchar,
        published_at timestamptz, superseded_at timestamptz
    )
    """,
    """
    CREATE TABLE mrf.provider_directory_dataset_resource (
        dataset_id varchar, resource_type varchar, resource_id varchar
    )
    """,
    """
    CREATE TABLE mrf.provider_directory_address_overlay (
        npi bigint, address_key uuid, source_id varchar,
        last_seen_run_id varchar, source_record_id varchar,
        resource_type varchar, resource_id varchar, phone_number varchar
    )
    """,
    """
    CREATE TABLE mrf.npi_taxonomy (
        npi bigint, healthcare_provider_taxonomy_code text, checksum text
    )
    """,
    "CREATE TABLE mrf.nucc_taxonomy (code text, display_name text)",
)


_POSTGRES_PHONE_PROVIDER_SEED_STATEMENTS = (
    """
    INSERT INTO mrf.npi (npi)
    SELECT 1000000000 + value FROM generate_series(1, 501) AS generated(value)
    UNION ALL SELECT 1000000602
    """,
    """
    INSERT INTO mrf.entity_address_unified (
        checksum, npi, type, first_line, telephone_number, phone_number,
        location_key, address_key, address_precision, lat, long,
        source_count, independent_source_count, multi_source_confirmed,
        address_sources, source_record_ids, updated_at, last_seen_at,
        date_added, entity_name
    )
    SELECT value, 1000000000 + value, 'primary', 'Synthetic Road',
           '2125550100', '2125550100', 'direct-' || value,
           ('10000000-0000-0000-0000-' || lpad(value::text, 12, '0'))::uuid,
           'street', 1, 1, 1, 1, true, ARRAY[]::varchar[],
           ARRAY[]::varchar[], now(), now(), current_date, 'Synthetic Provider'
      FROM generate_series(1, 501) AS generated(value)
    """,
    """
    INSERT INTO mrf.entity_address_unified (
        checksum, npi, type, first_line, telephone_number, phone_number,
        location_key, address_key, address_precision, lat, long,
        source_count, independent_source_count, multi_source_confirmed,
        address_sources, source_record_ids, updated_at, last_seen_at,
        date_added, entity_name
    )
    VALUES (
        602, 1000000602, 'primary', 'Overlay Road', '0000000000', '0000000000',
        'overlay-only', '20000000-0000-0000-0000-000000000001', 'street',
        1, 1, 1, 1, true, ARRAY[]::varchar[], ARRAY[]::varchar[], now(), now(),
        current_date, 'Overlay Provider'
    )
    """,
    "INSERT INTO mrf.provider_directory_source VALUES ('source-a', 'endpoint-a')",
    """
    INSERT INTO mrf.provider_directory_endpoint_dataset VALUES (
        'dataset-a', 'endpoint-a', 'run-a', NULL, true, 'published', now(), NULL
    )
    """,
    """
    INSERT INTO mrf.provider_directory_dataset_resource VALUES (
        'dataset-a', 'PractitionerRole', 'role-a'
    )
    """,
    """
    INSERT INTO mrf.provider_directory_address_overlay VALUES (
        1000000602, '20000000-0000-0000-0000-000000000001', 'source-a',
        'run-a', 'record-a', 'PractitionerRole', 'role-a', '2125550100'
    )
    """,
)


async def _seed_postgres_phone_provider_list_tables(session) -> None:
    for table_statement in _POSTGRES_PHONE_PROVIDER_TABLE_STATEMENTS:
        await session.execute(text(table_statement))
    for seed_statement in _POSTGRES_PHONE_PROVIDER_SEED_STATEMENTS:
        await session.execute(text(seed_statement))


async def _unified_address_table(required_columns, *, session=None):
    assert required_columns == npi_module._public_address_serving_column_keys()
    assert session is not None
    return "mrf.entity_address_unified"


def _postgres_phone_filter_context() -> ProviderImportQuery:
    relation_statement = select(literal("1000000501").label("entity_value")).union_all(
        select(literal("1000000602").label("entity_value"))
    )
    prepared = PreparedNpiEntityRelation(
        relation_statement,
        (),
        "a" * 64,
        "b" * 64,
    )
    return ProviderImportQuery(
        prepared,
        compile_npi_entity_relation(relation_statement),
        True,
    )


async def _noop_location_statuses(*_args, **_kwargs) -> None:
    return None


async def _empty_postgres_enrichment(*_args, **_kwargs) -> dict[int, dict[str, Any]]:
    return {}


def _configure_postgres_provider_list(monkeypatch) -> None:
    monkeypatch.setattr(
        provider_list_sql_module,
        "ConnectionProxy",
        _PostgresConnectionProxy,
    )
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", _legacy_address_table)
    monkeypatch.setattr(npi_module, "_plan_release_npi_scope", _empty_plan_scope)
    monkeypatch.setattr(npi_module, "_apply_location_statuses", _noop_location_statuses)
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_summary_map",
        _empty_postgres_enrichment,
    )
    monkeypatch.setattr(
        npi_module,
        "_npi_serving_columns",
        lambda: (npi_module.NPIData.__table__.c.npi,),
    )
    monkeypatch.setattr(
        npi_module.db,
        "acquire",
        lambda: (_ for _ in ()).throw(AssertionError("native pool used")),
    )


@pytest.mark.asyncio
async def test_postgres_generated_provider_list_count_page_and_lateral_hydration(
    monkeypatch,
):
    ordered_context, _ = _postgres_order_contexts("asc")
    async with transaction_session() as session:
        await _seed_postgres_provider_list_tables(session)
        _configure_postgres_provider_list(monkeypatch)
        response = await npi_module.list_providers(
            SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session)),
            native_args=RequestParameters({"include_total": ["true"], "limit": ["4"], "start": ["0"]}),
            import_context=ordered_context,
        )
        calls = session.info["provider_list_calls"]

    response_body = json.loads(response.body)
    count_sql, count_parameters = next(call for call in calls if "SELECT COUNT(DISTINCT" in call[0])
    page_sql, page_parameters = next(call for call in calls if "page_npis AS" in call[0])
    assert response_body["total"] == 4
    assert [provider_row["npi"] for provider_row in response_body["rows"]] == [
        1000000004,
        1000000003,
        1000000002,
        1000000001,
    ]
    assert "JOIN LATERAL" in page_sql
    assert set(ordered_context.compiled.values) <= set(count_parameters)
    assert set(ordered_context.compiled.values) <= set(page_parameters)


@pytest.mark.asyncio
async def test_postgres_imported_phone_keeps_overlay_and_rows_past_candidate_limit(
    monkeypatch,
):
    phone_filter_context = _postgres_phone_filter_context()
    async with transaction_session() as session:
        await _seed_postgres_phone_provider_list_tables(session)
        _configure_postgres_provider_list(monkeypatch)
        monkeypatch.setattr(
            npi_module,
            "_address_serving_table_sql",
            _unified_address_table,
        )
        response = await npi_module.list_providers(
            SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session)),
            native_args=RequestParameters(
                {
                    "include_total": ["true"],
                    "limit": ["3"],
                    "start": ["0"],
                    "phone": ["2125550100"],
                }
            ),
            import_context=phone_filter_context,
        )
        calls = session.info["provider_list_calls"]

    response_body = json.loads(response.body)
    count_sql, count_parameters = next(call for call in calls if "SELECT COUNT(DISTINCT" in call[0])
    page_sql, page_parameters = next(call for call in calls if "page_npis AS" in call[0])
    assert response_body["total"] == 2
    assert [provider_row["npi"] for provider_row in response_body["rows"]] == [
        1000000501,
        1000000602,
    ]
    for sql, parameters in ((count_sql, count_parameters), (page_sql, page_parameters)):
        assert "provider_directory_address_overlay AS overlay" in sql
        assert "LIMIT :candidate_limit" not in sql
        assert "candidate_limit" not in parameters
