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
from sqlalchemy import BigInteger, String, cast, literal, select, text, union_all
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
    monkeypatch.setattr(serving, "_shared_v3_code_table", lambda _tables=None: "pg_temp.sealed_code_fixture")


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


def test_native_score_union_isolates_binding_parameters(monkeypatch):
    monkeypatch.setattr(serving, "_manifest_provider_predicates", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(serving, "_ptg2_npi_scope_table", lambda _tables: "synthetic.native_npi_scope")
    monkeypatch.setattr(
        serving,
        "_unpaged_membership_location_sql",
        lambda _context: "SELECT CAST(:location_npi AS bigint) AS npi, :radius AS distance_miles, '' AS location_hash",
    )
    scopes = [
        imported._NativeScope(
            binding,
            SimpleNamespace(shared_snapshot_key=index + 1, uses_shared_blocks=True),
            {"npi": str(9000000001 + index)},
            SimpleNamespace(
                parameter_map={"location_npi": 9000000001 + index, "radius": index + 0.5, "limit": 25, "offset": 5000}
            ),
        )
        for index, binding in enumerate(_selection(2).in_network_bindings)
    ]
    statement = imported._native_score_scope([(scope, [dict(code_key=1)]) for scope in scopes], serving)
    compiled = statement.compile(dialect=dialect())
    assert isinstance(statement.selected_columns.npi.type, BigInteger)
    assert str(compiled).startswith("SELECT DISTINCT") and "UNION ALL" in str(compiled)
    assert "LIMIT" not in str(compiled) and "OFFSET" not in str(compiled)
    assert not any(name.startswith(("limit", "offset")) for name in compiled.params)
    for name, expected in (
        ("shared_snapshot_key", {1, 2}),
        ("provider_npi", {9000000001, 9000000002}),
        ("location_npi", {9000000001, 9000000002}),
        ("radius", {0.5, 1.5}),
    ):
        assert {
            parameter_value
            for parameter_name, parameter_value in compiled.params.items()
            if parameter_name.startswith(name + "_")
        } == expected
    assert all(isinstance(bind.type, BigInteger) for bind in compiled.binds.values() if "provider_npi" in bind.key)


@pytest.mark.parametrize("selector", ("none", "npi", "provider", "location"))
def test_native_score_scope_uses_actual_native_selectivity(monkeypatch, selector):
    monkeypatch.setattr(
        serving,
        "_manifest_provider_predicates",
        lambda *_args, **_kwargs: ["native.npi > 0"] if selector == "provider" else [],
    )
    monkeypatch.setattr(serving, "_ptg2_npi_scope_table", lambda _tables: "synthetic.native_npi_scope")
    monkeypatch.setattr(
        serving,
        "_unpaged_membership_location_sql",
        lambda _context: "SELECT 1000000001 AS npi, 0 AS distance_miles, '' AS location_hash",
    )
    scope = imported._NativeScope(
        _selection().in_network_bindings[0],
        SimpleNamespace(shared_snapshot_key=1, uses_shared_blocks=True),
        {"npi": "1000000001"} if selector == "npi" else {},
        SimpleNamespace(parameter_map={}) if selector == "location" else None,
    )
    assert (imported._native_score_scope([(scope, [dict(code_key=1)])], serving) is None) == (selector == "none")
    assert imported._native_score_scope([], serving) is None


def _install_score_scope_flow(monkeypatch, scopes, code_rows):
    monkeypatch.setattr(imported, "_native_scopes", AsyncMock(return_value=scopes))
    monkeypatch.setattr(imported, "load_sealed_code_rows", AsyncMock(side_effect=code_rows))
    monkeypatch.setattr(serving, "_manifest_provider_predicates", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(serving, "_ptg2_npi_scope_table", lambda _tables: "synthetic.native_npi_scope")
    monkeypatch.setattr(imported, "_stage_scope", AsyncMock())
    monkeypatch.setattr(imported, "_select_native_entries", AsyncMock(return_value=(0, [])))
    monkeypatch.setattr(imported, "_hydrate_selected_entries", AsyncMock(return_value=[]))
    return SimpleNamespace(execute=AsyncMock())


@pytest.mark.asyncio
async def test_native_plan_scope_precedes_score_compile(monkeypatch):
    scopes = [
        imported._NativeScope(
            binding,
            SimpleNamespace(shared_snapshot_key=index + 1, uses_shared_blocks=True),
            {"npi": str(1000000001 + index)} if index == 0 else {},
            None,
        )
        for index, binding in enumerate(_selection(3).in_network_bindings)
    ]
    session = _install_score_scope_flow(monkeypatch, scopes, [[dict(code_key=1)], [], [dict(code_key=3)]])
    query = _import_context()
    events, prepared_scoped = [], []
    compiler = imported.compile_npi_entity_relation

    async def scope_scores(native_scope):
        events.append("scope")
        compiled = native_scope.compile(dialect=dialect())
        assert {
            parameter_value
            for parameter_name, parameter_value in compiled.params.items()
            if parameter_name.startswith("shared_snapshot_key_")
        } == {1, 3}
        assert "UNION ALL" in str(compiled) and "LIMIT" not in str(compiled) and "OFFSET" not in str(compiled)
        native = native_scope.subquery()
        prepared = replace(
            query.prepared,
            statement=query.prepared.statement.where(
                query.prepared.statement.selected_columns.entity_value.in_(select(cast(native.c.npi, String)))
            ),
        )
        prepared_scoped.append(prepared)
        return prepared

    def compile_scores(statement):
        events.append("compile")
        assert statement is prepared_scoped[0].statement
        return compiler(statement)

    async def stage_scores(_session, scoped, compiled):
        events.append("stage")
        assert scoped.prepared is prepared_scoped[0]
        assert "native_plan_score_scope" in compiled.sql

    scope_callback = AsyncMock(side_effect=scope_scores)
    monkeypatch.setattr(imported, "compile_npi_entity_relation", compile_scores)
    monkeypatch.setattr(imported, "_stage_imported_scores", AsyncMock(side_effect=stage_scores))
    response = await imported.search_imported_plan_providers(
        session,
        {},
        SimpleNamespace(offset=0, limit=2, page=1),
        _selection(3),
        replace(query, native_scope_preparer=scope_callback),
    )
    assert events == ["scope", "compile", "stage"]
    scope_callback.assert_awaited_once()
    assert [call.args[1] for call in imported._stage_scope.await_args_list] == [scopes[0], scopes[2]]
    assert all(call.args[3].prepared is prepared_scoped[0] for call in imported._stage_scope.await_args_list)
    assert response["pagination"]["total"] == 0 and response["pagination"]["total_is_exact"] is True


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("no_selectivity", "no_callback", "no_code"))
async def test_native_plan_preserves_score_first_fallback(monkeypatch, case):
    scope = imported._NativeScope(
        _selection().in_network_bindings[0],
        SimpleNamespace(shared_snapshot_key=1, uses_shared_blocks=True),
        {"npi": "1000000001"} if case != "no_selectivity" else {},
        None,
    )
    session = _install_score_scope_flow(monkeypatch, [scope], [[] if case == "no_code" else [dict(code_key=1)]])
    query = _import_context()
    callback = AsyncMock(side_effect=AssertionError("fallback must not scope scores"))
    query = replace(query, native_scope_preparer=None if case == "no_callback" else callback)
    compile_scores = imported.compile_npi_entity_relation
    compiler = AsyncMock() if case == "no_code" else None
    events = []

    def record_compile(statement):
        events.append("compile")
        assert statement is query.prepared.statement
        return compile_scores(statement)

    monkeypatch.setattr(imported, "compile_npi_entity_relation", compiler or record_compile)
    monkeypatch.setattr(
        imported, "_stage_imported_scores", AsyncMock(side_effect=lambda *_args: events.append("stage"))
    )
    await imported.search_imported_plan_providers(
        session, {}, SimpleNamespace(offset=0, limit=2, page=1), _selection(), query
    )
    callback.assert_not_awaited()
    if case == "no_code":
        compiler.assert_not_called()
        imported._stage_imported_scores.assert_not_awaited()
        imported._stage_scope.assert_not_awaited()
        assert events == []
    else:
        assert events == ["compile", "stage"]


@pytest.mark.asyncio
async def test_native_plan_scope_failure_stops_before_staging(monkeypatch):
    scope = imported._NativeScope(
        _selection().in_network_bindings[0],
        SimpleNamespace(shared_snapshot_key=1, uses_shared_blocks=True),
        {"npi": "1000000001"},
        None,
    )
    session = _install_score_scope_flow(monkeypatch, [scope], [[dict(code_key=1)]])
    callback = AsyncMock(side_effect=PlanPricingProjectionUnavailable("native scope unavailable"))
    compiler = AsyncMock()
    monkeypatch.setattr(imported, "compile_npi_entity_relation", compiler)
    monkeypatch.setattr(imported, "_stage_imported_scores", AsyncMock())
    with pytest.raises(PlanPricingProjectionUnavailable, match="native scope unavailable"):
        await imported.search_imported_plan_providers(
            session,
            {},
            SimpleNamespace(offset=0, limit=2, page=1),
            _selection(),
            replace(_import_context(), native_scope_preparer=callback),
        )
    callback.assert_awaited_once()
    compiler.assert_not_called()
    imported._stage_imported_scores.assert_not_awaited()
    imported._stage_scope.assert_not_awaited()
    session.execute.assert_not_awaited()


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
async def test_occurrence_batches_span_sparse_shards_without_losing_repeated_rates(monkeypatch):
    monkeypatch.setattr(plan_staging, "_PROVIDER_KEY_BATCH_SIZE", 2)
    scope = imported._NativeScope(
        _selection().in_network_bindings[0], _selection()._validated_serving_tables[0][1], {}, None
    )
    provider_keys = (0, 7, 1024, 8192, 1048576)
    batches = []

    async def forward(_session, _tables, **options):
        batch = options["provider_set_keys"]
        batches.append(batch)
        assert options["limit"] is None and options["offset"] == 0
        return [_rate(1, key + 1, key) for key in batch for _ in range(2)]

    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", forward)
    monkeypatch.setattr(serving, "_hydrate_provider_set_network_names", AsyncMock())
    async with _plan_session() as session:
        await plan_staging.copy_plan_rows(
            session,
            plan_sql.MEMBERSHIPS,
            ("binding_ordinal", "npi", "provider_set_key", "membership_ordinal"),
            [(0, npi, key, ordinal) for npi in (1000000001, 1000000002) for ordinal, key in enumerate(provider_keys)],
        )
        await plan_staging.stage_native_occurrences(session, scope, [dict(code_key=1)], serving)
        occurrences = (
            await session.execute(
                text(
                    f"SELECT occurrence_id, provider_set_key FROM pg_temp.{plan_sql.OCCURRENCES} ORDER BY occurrence_id"
                )
            )
        ).all()
    assert batches == [(0, 7), (1024, 8192), (1048576,)]
    assert [tuple(occurrence) for occurrence in occurrences] == list(
        enumerate(key for key in provider_keys for _ in range(2))
    )


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


@pytest.mark.asyncio
async def test_source_admission_precedes_membership_and_keeps_original_ordinal(monkeypatch):
    selection = _selection(2)
    events = []

    async def admit(session, selected):
        events.append("admission")
        assert selected is selection
        return (selection.in_network_bindings[1],)

    async def location(session, tables, args, **options):
        events.append("membership")
        assert args["snapshot_id"] == "snapshot-1" and options["offset"] == 1
        return SimpleNamespace(parameter_map={})

    monkeypatch.setattr(imported, "admitted_plan_release_bindings", admit)
    monkeypatch.setattr(serving, "_membership_location_query", location)
    scopes = await imported._native_scopes(object(), selection, {"zip5": "12345"}, serving)
    assert events == ["admission", "membership"]
    assert len(scopes) == 1 and scopes[0].binding is selection.in_network_bindings[1]
    assert scopes[0].binding.binding_ordinal == 1 and len(selection.bindings) == 2


@pytest.mark.asyncio
async def test_source_admission_failure_stops_before_native_code_or_scores(monkeypatch):
    admission = AsyncMock(side_effect=PlanPricingProjectionUnavailable("managed plan source admission unavailable"))
    code = AsyncMock(side_effect=AssertionError("denied source must not read codes"))
    monkeypatch.setattr(imported, "admitted_plan_release_bindings", admission)
    monkeypatch.setattr(imported, "load_sealed_code_rows", code)
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("denied source must not stage")))
    with pytest.raises(PlanPricingProjectionUnavailable, match="source admission"):
        await imported.search_imported_plan_providers(
            session, {}, SimpleNamespace(offset=3, limit=2, page=2), _selection(), _import_context()
        )
    admission.assert_awaited_once()
    code.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("require_match", [False, True])
async def test_empty_admission_never_falls_back_to_original_bindings_or_scores(monkeypatch, require_match):
    actual_native_scopes = imported._native_scopes
    session = _install_score_scope_flow(monkeypatch, [], [])
    monkeypatch.setattr(imported, "_native_scopes", actual_native_scopes)
    admission = AsyncMock(return_value=())
    monkeypatch.setattr(imported, "admitted_plan_release_bindings", admission)
    scores = AsyncMock(side_effect=AssertionError("empty admission must not score"))
    monkeypatch.setattr(imported, "_stage_imported_scores", scores)
    context = replace(_import_context(require_match=require_match), native_scope_preparer=scores)
    response = await imported.search_imported_plan_providers(
        session, {}, SimpleNamespace(offset=3, limit=2, page=2), _selection(2), context
    )
    assert response["pagination"]["total"] == 0 and response["pagination"]["total_is_exact"] is True
    imported.load_sealed_code_rows.assert_not_awaited()
    imported._stage_scope.assert_not_awaited()
    scores.assert_not_awaited()
    assert imported._select_native_entries.await_args.args[1] == []
    assert imported._select_native_entries.await_args.args[4].offset == 3


def _contributor_selection():
    selection = _selection(3)
    bindings = tuple(
        replace(binding, plan_id=f"plan-meta-{index}", binding_ordinal=7 * index + 3)
        for index, binding in enumerate(selection.bindings)
    )
    return replace(selection, bindings=bindings)


@pytest.mark.parametrize(
    "flags", [{}, {"include_sources": "true"}, {"include_details": "true"}, {"include_debug": "true"}]
)
@pytest.mark.parametrize("positions,offset,total", [((), 0, 0), ((1,), 0, 0), ((0, 2), 7, 3), ((0, 1, 2), 7, 3)])
def test_imported_metadata_uses_all_admitted_contributors_on_empty_pages(flags, positions, offset, total):
    selection = _contributor_selection()
    bindings = selection.bindings
    contributing_bindings = tuple(bindings[position] for position in positions)
    response = imported._plan_response(
        selection,
        flags,
        SimpleNamespace(limit=2, offset=offset, page=4),
        total,
        [],
        [],
        serving,
        contributing_bindings=contributing_bindings,
    )
    snapshots = [binding.snapshot_id for binding in contributing_bindings]
    assert response["in_network_snapshot_ids"] == response["resolved_snapshot_ids"] == snapshots
    assert response["query"]["in_network_snapshot_ids"] == response["query"]["resolved_snapshot_ids"] == snapshots
    assert response["query"]["plan_ids"] == [binding.plan_id for binding in contributing_bindings]
    assert response["query"]["snapshots"] == [
        {
            "source_key": binding.source_key,
            "snapshot_id": binding.snapshot_id,
            "plan_id": binding.plan_id,
            "plan_market_type": binding.plan_market_type,
        }
        for binding in contributing_bindings
    ]
    encoded = json.dumps(response)
    for index, binding in enumerate(bindings):
        if index not in positions:
            assert all(
                identity not in encoded for identity in (binding.source_key, binding.plan_id, binding.snapshot_id)
            )
    assert response["items"] == [] and response[imported.NATIVE_ENTRY_IDS] == []
    assert response["pagination"] == dict(
        total=total, total_is_exact=True, limit=2, offset=offset, page=4, has_more=False
    )
    assert response["query"]["status"] == ("matched" if total else "no_match")
    assert response["binding_set_digest"] == selection.binding_set_digest
    assert selection.bindings is bindings and len(selection.network_tables_by_snapshot()) == 3
    assert selection.response_metadata()["in_network_snapshot_ids"] == ["snapshot-0", "snapshot-1", "snapshot-2"]


@pytest.mark.parametrize("include_sources", ["false", "true"])
def test_contributor_metadata_preserves_native_entry_identity_and_total(include_sources):
    selection = _contributor_selection()
    binding = selection.bindings[1]
    scope = imported._NativeScope(binding, None, {}, None)
    entry = imported._NativeEntry(scope, {"npi": 1000000001}, ())
    provider_by_field = {"npi": 1000000001, **_rate(binding.source_key, 11), "plan_id": binding.plan_id}
    query_by_field = {"include_sources": include_sources}
    pagination = SimpleNamespace(limit=2, offset=0, page=1)
    original = imported._plan_response(
        selection, query_by_field, pagination, 3, [entry], [dict(provider_by_field)], serving
    )
    admitted = imported._plan_response(
        selection,
        query_by_field,
        pagination,
        3,
        [entry],
        [dict(provider_by_field)],
        serving,
        contributing_bindings=(binding,),
    )
    assert admitted[imported.NATIVE_ENTRY_IDS] == original[imported.NATIVE_ENTRY_IDS]
    assert admitted["items"] == original["items"] and admitted["pagination"] == original["pagination"]
    assert admitted["in_network_snapshot_ids"] == [binding.snapshot_id]
    assert admitted["query"]["plan_ids"] == [binding.plan_id]
    assert admitted["binding_set_digest"] == original["binding_set_digest"] == selection.binding_set_digest


@pytest.mark.asyncio
@pytest.mark.parametrize("positions,total,offset", [((), 0, 0), ((1,), 3, 7), ((0, 2), 3, 7), ((0, 1, 2), 3, 7)])
async def test_imported_search_passes_admitted_scopes_to_empty_page_metadata(monkeypatch, positions, total, offset):
    selection = _contributor_selection()
    scopes = [imported._NativeScope(selection.bindings[position], None, {}, None) for position in positions]
    session = _install_score_scope_flow(monkeypatch, scopes, [[dict(code_key=1)] for _scope in scopes])
    monkeypatch.setattr(imported, "_stage_imported_scores", AsyncMock())
    imported._select_native_entries.return_value = (total, [])
    response = await imported.search_imported_plan_providers(
        session,
        {"include_sources": "true"},
        SimpleNamespace(limit=2, offset=offset, page=4),
        selection,
        _import_context(),
    )
    expected_snapshots = [scope.binding.snapshot_id for scope in scopes]
    assert response["in_network_snapshot_ids"] == response["query"]["resolved_snapshot_ids"] == expected_snapshots
    assert response["query"]["plan_ids"] == [scope.binding.plan_id for scope in scopes]
    assert response["pagination"]["total"] == total and response["items"] == []
    assert len(selection.bindings) == 3 and len(selection.network_tables_by_snapshot()) == 3
    assert response["binding_set_digest"] == selection.binding_set_digest


def test_complete_contributors_preserve_unscoped_response_bytes():
    selection = _contributor_selection()
    query_by_field = {"include_sources": "true"}
    pagination = SimpleNamespace(limit=2, offset=0, page=1)
    original = imported._plan_response(selection, query_by_field, pagination, 0, [], [], serving)
    explicit = imported._plan_response(
        selection, query_by_field, pagination, 0, [], [], serving, contributing_bindings=selection.in_network_bindings
    )
    assert json.dumps(explicit, separators=(",", ":")) == json.dumps(original, separators=(",", ":"))
