# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Imported scores select complete native plan entries before page hydration."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import literal, select, text, union_all
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from api import custom_import_plan_pricing as imported
from api import custom_import_plan_sql as plan_sql
from api import custom_import_plan_staging as plan_staging
from api import ptg2_code_scope as code_scope
from api import ptg2_serving as serving
from api.custom_import_provider_service_sql import ProviderServiceImportQuery
from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable, PlanPricingProjectionUnsupported
from api.plan_release_serving import PlanReleaseServingSelection, PlanReleaseSnapshotBinding
from process.custom_import.read_core import PreparedNpiEntityRelation, ReadOrderTerm
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.custom_import_postgres_support import _database_url


def _install_code_lookup_scope(monkeypatch):
    monkeypatch.setattr(
        serving,
        "_shared_v3_code_scope_sql",
        lambda *_args, **_kwargs: (
            "CROSS JOIN (SELECT 'synthetic-plan' AS plan_id, 'group' AS plan_market_type) logical_scope",
            [],
            {},
            "code_metadata.code_key",
        ),
    )
    monkeypatch.setattr(serving, "_shared_v3_code_table", lambda: "pg_temp.sealed_code_fixture")


@pytest.mark.asyncio
@pytest.mark.parametrize("system,code", [("ICD10CM", "."), ("ICD10PCS", "..")])
async def test_empty_canonical_code_returns_without_database_lookup(monkeypatch, system, code):
    _install_code_lookup_scope(monkeypatch)
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("empty code must not query")))
    assert (
        await code_scope.load_sealed_code_rows(
            session,
            SimpleNamespace(shared_snapshot_key=1, uses_shared_blocks=True),
            {"code_system": system, "code": code},
        )
        == []
    )
    session.execute.assert_not_awaited()


def _selection(bindings=1):
    frozen_bindings = tuple(
        PlanReleaseSnapshotBinding(index, f"snapshot-{index}", f"source-{index}", "plan-1", "group", "in_network", True)
        for index in range(bindings)
    )
    native_tables = SimpleNamespace(
        network_names=[], shared_snapshot_key=1, provider_shard_span=1024, uses_shared_blocks=True
    )
    return PlanReleaseServingSelection(
        "serving-1",
        "release-1",
        "plan-identity-1",
        "version-1",
        "2026-01",
        "published",
        "a" * 64,
        frozen_bindings,
        _validated_serving_tables=tuple((binding.snapshot_id, native_tables) for binding in frozen_bindings),
    )


def _import_context(*, ordered=True, require_match=True):
    terms = (ReadOrderTerm("quality", "desc", "last"),) if ordered else ()
    statements = []
    for npi, score in ((1000000001, 1), (1000000002, 5), (1000000003, 3)):
        columns = [literal(str(npi)).label("entity_value")]
        if ordered:
            columns.append(literal(score).label("sort_0"))
        statements.append(select(*columns))
    imported_relation = union_all(*statements).subquery()
    statement = select(*imported_relation.c)
    prepared = PreparedNpiEntityRelation(statement, terms, "a" * 64, "b" * 64)
    return ProviderServiceImportQuery(prepared, require_match)


def _rate(source_key, price_key, set_key=7):
    return dict(
        source_key=source_key,
        price_key=price_key,
        _ptg_provider_set_key=set_key,
        provider_set_global_id_128=f"{set_key:032x}",
        price_set_global_id_128=f"{price_key:032x}",
        serving_content_hash_128=f"{price_key:032x}",
        provider_count=3,
        plan_id="plan-1",
        plan_market_type="group",
        reported_code_system="CPT",
        reported_code="27447",
        negotiation_arrangement="ffs",
        billing_code_type_version="2026",
        source_procedure_name="Synthetic procedure",
        source_procedure_description="Synthetic description",
        network_names=[],
    )


def _native_fixture(monkeypatch):
    rates = [_rate(1, 11), _rate(1, 12), _rate(2, 13)]
    monkeypatch.setattr(imported, "load_sealed_code_rows", AsyncMock(return_value=[dict(code_key=1)]))
    monkeypatch.setattr(serving, "_manifest_provider_predicates", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(serving, "_ptg2_npi_scope_table", lambda _tables: "pg_temp.native_npi_fixture")
    forward_read = AsyncMock(return_value=rates)
    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", forward_read)
    monkeypatch.setattr(serving, "_hydrate_provider_set_network_names", AsyncMock())
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_selected_npis",
        AsyncMock(side_effect=lambda _session, _tables, npis: {npi: (f"{7:032x}",) for npi in npis}),
    )
    monkeypatch.setattr(serving, "_provider_set_keys_for_ids", AsyncMock(return_value={f"{7:032x}": 7}))

    async def read_prices(_session, _tables, price_keys):
        return {
            price_key: [
                dict(
                    negotiated_rate=Decimal(price_key),
                    billing_class="professional",
                    negotiated_type="negotiated",
                    service_code=["11"],
                    billing_code_modifier=[],
                )
            ]
            for price_key in price_keys
        }

    prices = AsyncMock(side_effect=read_prices)
    monkeypatch.setattr(serving, "_version_three_prices_by_key", prices)

    async def read_providers(_session, *, npis, **_kwargs):
        return [
            dict(
                npi=npi,
                provider_name="Synthetic Provider",
                location_hash="location-1",
                provider_sex_code="F",
                address_payload="{}",
            )
            for npi in npis
        ]

    providers = AsyncMock(side_effect=read_providers)
    monkeypatch.setattr(serving, "_enriched_provider_rows_for_npis", providers)
    monkeypatch.setattr(serving, "_procedure_details_for_rows", AsyncMock(return_value={}))
    monkeypatch.setattr(serving, "_version_three_explicit_npi_graph_scope", AsyncMock(return_value=None))
    monkeypatch.setattr(serving, "_billing_associations_for_exact_npi_request", AsyncMock(return_value={}))
    return forward_read, prices, providers


@asynccontextmanager
async def _plan_session(driver="asyncpg"):
    engine = create_async_engine(_database_url().set(drivername=f"postgresql+{driver}"))
    try:
        async with AsyncSession(engine) as session:
            async with session.begin():
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                await session.execute(
                    text("""CREATE TEMP TABLE native_npi_fixture (
                snapshot_key bigint, npi bigint
            ) ON COMMIT DROP""")
                )
                await session.execute(
                    text("""INSERT INTO native_npi_fixture VALUES
                (1, 1000000001), (1, 1000000002), (1, 1000000003)
            """)
                )
                await plan_sql.prepare_plan_query_tables(session)
                await session.execute(text("SET TRANSACTION READ ONLY"))
                yield session
                assert await session.scalar(text("SHOW transaction_read_only")) == "on"
                assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
            owned_tables = [
                "native_npi_fixture",
                plan_sql.IMPORTED,
                plan_sql.CANDIDATES,
                plan_sql.MEMBERSHIPS,
                plan_sql.OCCURRENCES,
                plan_sql.PRICES,
                plan_sql.SELECTED,
            ]
            assert (
                await session.scalar(
                    text("""SELECT COUNT(*) FROM pg_class WHERE relnamespace = pg_my_temp_schema()
                     AND relname = ANY(CAST(:owned_tables AS text[]))"""),
                    {"owned_tables": owned_tables},
                )
                == 0
            )
    finally:
        await engine.dispose()


def test_native_candidates_keep_pinned_scope_and_indexed_imported_membership(monkeypatch):
    scope = imported._NativeScope(
        _selection().in_network_bindings[0], SimpleNamespace(shared_snapshot_key=1, uses_shared_blocks=True), {}, None
    )
    monkeypatch.setattr(serving, "_manifest_provider_predicates", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(serving, "_ptg2_npi_scope_table", lambda _tables: "synthetic.native_npi_scope")
    query = _import_context()
    sql, parameters_by_name = imported._candidate_query(scope, query, serving)
    assert "imported.npi IS NOT NULL" in sql
    assert "native.snapshot_key = :shared_snapshot_key" in sql
    assert "LIMIT" not in sql and "OFFSET" not in sql
    assert "eligible_native AS MATERIALIZED" in sql
    assert f"LEFT JOIN pg_temp.{plan_sql.IMPORTED} imported ON imported.npi = native.npi" in sql
    assert parameters_by_name["shared_snapshot_key"] == 1


def _typed_import_context(first_value, second_value, *, ordered=True, nulls="last"):
    source_relation = union_all(
        select(literal("1000000001").label("entity_value"), literal(first_value).label("sort_0")),
        select(literal("1000000002").label("entity_value"), literal(second_value).label("sort_0")),
    ).subquery()
    terms = (ReadOrderTerm("metric", "asc", nulls),) if ordered else ()
    columns = [source_relation.c.entity_value]
    if ordered:
        columns.append(source_relation.c.sort_0)
    prepared = PreparedNpiEntityRelation(select(*columns), terms, "a" * 64, "b" * 64)
    return ProviderServiceImportQuery(prepared, False)


@pytest.mark.parametrize("binary_type", [bytes, bytearray, memoryview])
def test_occurrence_copy_keeps_native_binary_identifiers(binary_type):
    scope = imported._NativeScope(_selection().in_network_bindings[0], None, {}, None)
    serving_by_field = _rate(1, 11)
    serving_by_field["provider_set_global_id_128"] = binary_type(bytes.fromhex(f"{7:032x}"))
    occurrence_record = plan_staging._occurrence_record(scope, 1, serving_by_field, serving)
    assert json.loads(occurrence_record[-1])["provider_set_global_id_128"] == f"{7:032x}"
    with pytest.raises(TypeError, match="unsupported JSON metadata"):
        plan_staging._occurrence_record(scope, 1, {**serving_by_field, "unexpected": object()}, serving)


@pytest.mark.asyncio
async def test_plan_missing_validated_native_binding_fails_closed(monkeypatch):
    forward_read, prices, providers = _native_fixture(monkeypatch)
    selection = replace(_selection(), _validated_serving_tables=())
    with pytest.raises(PlanPricingProjectionUnavailable, match="validated serving tables"):
        await imported.search_imported_plan_providers(
            object(),
            dict(code="27447", code_system="CPT"),
            SimpleNamespace(offset=0, limit=2, page=1),
            selection,
            _import_context(),
        )
    forward_read.assert_not_awaited()
    prices.assert_not_awaited()
    providers.assert_not_awaited()


def _install_location_fixture(monkeypatch, is_complete):
    location_context = SimpleNamespace(parameter_map={"fixture_radius": 2.0})
    location_query = AsyncMock(return_value=location_context)
    monkeypatch.setattr(serving, "_membership_location_query", location_query)
    monkeypatch.setattr(
        serving,
        "_unpaged_membership_location_sql",
        lambda _context: (
            """
        SELECT * FROM (VALUES
            (1000000001::bigint, 1.5::double precision, 'location-1'),
            (1000000002::bigint, 0.5::double precision, 'location-2'),
            (1000000003::bigint, 4.0::double precision, 'location-3')
        ) witness(npi, distance_miles, location_hash) WHERE distance_miles <= :fixture_radius
    """
        ),
    )

    async def read_locations(_session, _tables, _args, *, candidate_npis, **_kwargs):
        return (
            [
                dict(
                    npi=npi,
                    distance_miles=1.5 if npi == 1000000001 else 0.5,
                    location_hash=f"location-{npi - 1000000000}",
                    address_payload=json.dumps(
                        {"first_line": "1 Synthetic Road", "city": "Sample", "state": "TX", "zip5": "12345"}
                    ),
                )
                for npi in candidate_npis
            ]
            if is_complete
            else []
        )

    monkeypatch.setattr(serving, "_membership_location_rows", AsyncMock(side_effect=read_locations))
    return location_query


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "request_overrides,selection,import_context,error_type",
    [
        ({}, None, _import_context(), PlanPricingProjectionUnavailable),
        ({}, _selection(), object(), PlanPricingProjectionUnavailable),
        ({"q": "procedure"}, _selection(), _import_context(), PlanPricingProjectionUnsupported),
        ({"view": "card"}, _selection(), _import_context(), PlanPricingProjectionUnsupported),
        ({"include_providers": "false"}, _selection(), _import_context(), PlanPricingProjectionUnsupported),
    ],
)
async def test_plan_request_rejects_unsupported_scope(
    monkeypatch, request_overrides, selection, import_context, error_type
):
    scope_reader = AsyncMock(side_effect=AssertionError("invalid request must not reach native scope"))
    monkeypatch.setattr(imported, "_native_scopes", scope_reader)
    with pytest.raises(error_type):
        await imported.search_imported_plan_providers(
            object(),
            {"code": "27447", "code_system": "CPT", **request_overrides},
            SimpleNamespace(offset=0, limit=2, page=1),
            selection,
            import_context,
        )
    scope_reader.assert_not_awaited()


@pytest.mark.asyncio
async def test_plan_location_requires_exact_relation(monkeypatch):
    location_reader = AsyncMock(return_value=None)
    monkeypatch.setattr(serving, "_membership_location_query", location_reader)
    with pytest.raises(PlanPricingProjectionUnavailable, match="exact address relation"):
        await imported._native_scopes(object(), _selection(), {"zip5": "12345"}, serving)
    assert location_reader.await_args.kwargs["offset"] == 1


@pytest.mark.asyncio
async def test_plan_dispatch_requires_pinned_release(monkeypatch):
    snapshot_reader = AsyncMock(side_effect=AssertionError("imported query must not fall back"))
    monkeypatch.setattr(serving, "_search_manifest_serving_table", snapshot_reader)
    with pytest.raises(PlanPricingProjectionUnavailable, match="pinned release"):
        await serving.search_current_ptg2_index(
            object(), {}, SimpleNamespace(offset=0, limit=2, page=1), import_context=_import_context()
        )
    snapshot_reader.assert_not_awaited()


def test_plan_rejects_duplicate_identity(monkeypatch):
    shape_response = AsyncMock(side_effect=AssertionError("duplicate page must not be shaped"))
    monkeypatch.setattr(serving, "_shape_ptg2_response", shape_response)
    scope = imported._NativeScope(_selection().in_network_bindings[0], None, {}, None)
    provider_by_field = {"npi": 1000000001, **_rate(1, 11)}
    entry = imported._NativeEntry(scope, {"npi": 1000000001}, ())
    with pytest.raises(PTG2ManifestArtifactError, match="repeated a native entry"):
        imported._plan_response(
            _selection(),
            {},
            SimpleNamespace(offset=0, limit=2, page=1),
            2,
            [entry, entry],
            [provider_by_field, provider_by_field],
            serving,
        )
    shape_response.assert_not_called()


def _copy_session(driver, events, written_rows, failure=None):
    async def write_row(copy_row):
        if failure is not None:
            raise failure
        written_rows.append(copy_row)

    @asynccontextmanager
    async def copy_cursor():
        events.append("cursor_enter")
        try:
            yield SimpleNamespace(copy=copy_stream)
        finally:
            events.append("cursor_exit")

    @asynccontextmanager
    async def copy_stream(statement):
        events.append(statement)
        try:
            yield SimpleNamespace(write_row=write_row)
        finally:
            events.append("copy_exit")

    connection = SimpleNamespace(
        dialect=SimpleNamespace(driver=driver, identifier_preparer=dialect().identifier_preparer),
        get_raw_connection=AsyncMock(
            return_value=SimpleNamespace(driver_connection=SimpleNamespace(cursor=copy_cursor))
        ),
    )
    return SimpleNamespace(connection=AsyncMock(return_value=connection))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError("copy failure"), asyncio.CancelledError()])
async def test_psycopg_copy_preserves_cleanup(failure):
    events, written_rows = [], []
    session = _copy_session("psycopg", events, written_rows, failure)
    copy_rows = [(1, Decimal("2.000000000001")), (2, None)]
    if failure is None:
        await plan_staging.copy_plan_rows(session, "synthetic_copy", ("id", "rate value"), iter(copy_rows))
        assert written_rows == copy_rows
    else:
        with pytest.raises(type(failure)):
            await plan_staging.copy_plan_rows(session, "synthetic_copy", ("id", "rate value"), iter(copy_rows))
        assert written_rows == []
    assert events == [
        "cursor_enter",
        'COPY pg_temp.synthetic_copy (id, "rate value") FROM STDIN',
        "copy_exit",
        "cursor_exit",
    ]


@pytest.mark.asyncio
async def test_plan_copy_rejects_unsupported_driver():
    events, written_rows = [], []
    session = _copy_session("unsupported", events, written_rows)
    with pytest.raises(PTG2ManifestArtifactError, match="COPY driver is unavailable"):
        await plan_staging.copy_plan_rows(session, "synthetic_copy", ("id",), iter([(1,)]))
    assert events == written_rows == []
