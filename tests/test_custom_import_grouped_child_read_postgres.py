# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native selected-family correlation, child ordering, and complete hydration."""

from __future__ import annotations

import json
from dataclasses import replace
from decimal import Decimal

import pytest
from sanic import response
from sqlalchemy import BigInteger, Numeric, column, select, text, values
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from api import custom_import_provider_geo as geo_http
from api import custom_import_provider_http as provider_http
from api import custom_import_read_http as transport
from api import provider_geo_sql, provider_list_sql
from api.custom_import_provider_sql import ProviderImportQuery, compile_npi_entity_relation
from process.custom_import import grouped_query, grouped_read, read_core
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.read_core import (
    ExtensionReadAuthorization,
    ExtensionReadScope,
    PinnedReadTarget,
    ReadFilter,
    ReadOrderTerm,
    SearchRequest,
)
from process.custom_import.runner import run_candidate
from tests import custom_import_grouped_child_support as fixture
from tests import test_custom_import_grouped_score_query_postgres as scores
from tests import test_custom_import_provider_http as provider_fixture
from tests import test_custom_import_provider_hydration_postgres as native
from tests import test_custom_import_read_core_postgres as read_fixture
from tests import test_custom_import_read_http as http_fixture
from tests import test_custom_import_runner_postgres as runner_fixture
from tests.custom_import_postgres_support import (
    _database_url,
    _quoted_publication_schema,
    isolated_publication_case,
)

_PANEL = ReadFilter("segment", "eq", "segment_a")
_KEY = ReadFilter("service_code", "eq", "chosen")
_ABSENT = "1000000038"


@pytest.fixture
async def values_connection():
    engine = create_async_engine(_database_url())
    try:
        async with engine.connect() as connection:
            yield connection
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguous", (False, True))
async def test_stored_child_ambiguity_cannot_be_hidden_by_a_later_metric_filter(ambiguous, values_connection):
    rows = [(1, 2, 3, Decimal("1"))]
    if ambiguous:
        rows.append((1, 2, 4, Decimal("9")))
    children = (
        values(
            column("family_revision_id", BigInteger),
            column("root_record_id", BigInteger),
            column("child_revision_id", BigInteger),
            column("amount", Numeric),
        )
        .data(rows)
        .cte("synthetic_stored_children")
    )
    validated = grouped_query._validated_stored_children(children)
    statement = select(validated.c.child_revision_id).where(
        validated.c.validated_child_id.is_not(None),
        validated.c.amount < 2,
    )
    if ambiguous:
        with pytest.raises(DBAPIError, match="more than one row"):
            await values_connection.execute(statement)
    else:
        assert (await values_connection.execute(statement)).scalar_one() == 3


@pytest.fixture(autouse=True)
def _child_definition(monkeypatch):
    monkeypatch.setattr(native.fixture, "definition", fixture.definition)


def _children():
    return [
        {
            "rate_npi": npi,
            "rate_period": year,
            "rate_segment": segment,
            "service_code": key,
            "amount": None if amount is None else Decimal(amount),
            "quality": Decimal(quality),
        }
        for npi, year, segment, key, amount, quality in (
            (native._A, 2024, "segment_a", "chosen", "2", "9"),
            (native._A, 2024, "segment_a", "sibling", "9", "2"),
            (native._A, 2024, "segment_a", "both", "7", "7"),
            (native._A, 2024, "segment_a", "both_again", "8", "8"),
            (native._A, 2024, "segment_a", "nullable", "2", "3"),
            (native._A, 2024, "segment_b", "chosen", "99", "99"),
            (native._A, 2023, "segment_a", "chosen", "99", "99"),
            (native._B, 2025, "segment_a", "chosen", "5", "4"),
            (native._B, 2025, "segment_a", "nullable", None, "4"),
            (native._B, 2024, "segment_b", "chosen", "88", "88"),
            (native._C, 2025, "segment_a", "chosen", "99", "99"),
        )
    ]


def _ordered_query(key="chosen", direction="asc", **changes):
    return fixture.query(
        context_filters=(_PANEL, ReadFilter("service_code", "eq", key)),
        order_terms=(ReadOrderTerm("amount", direction, "last"),),
        require_match=False,
        **changes,
    )


@pytest.mark.asyncio
async def test_one_child_must_satisfy_every_predicate_in_the_selected_family():
    async with native._case(children=_children()) as (case, target), case.sessions() as session:
        split = fixture.query(
            context_filters=(_PANEL,), filters=(ReadFilter("amount", "gt", "8"), ReadFilter("quality", "gt", "8"))
        )
        assert (await native._relation(session, target, split))[1] == []
        same = replace(split, filters=(ReadFilter("amount", "gt", "6"), ReadFilter("quality", "gt", "6")))
        assert [row.entity_value for row in (await native._relation(session, target, same))[1]] == [native._A]
        # Two qualifying siblings still produce one native provider.
        assert (await native._relation(session, target, replace(same, context_filters=(_PANEL, _KEY))))[1] == []
        chosen = replace(same, context_filters=(_PANEL, ReadFilter("service_code", "eq", "both")))
        assert [row.entity_value for row in (await native._relation(session, target, chosen))[1]] == [native._A]
        root_order = replace(same, order_terms=(ReadOrderTerm("score", "desc", "last"),))
        prepared, root_rows = await native._relation(session, target, root_order)
        assert [tuple(row) for row in root_rows] == [(native._A, Decimal("10"))]
        for is_geo in (False, True):
            count, page = await _native_rows(session, prepared, is_geo=is_geo, require_match=True)
            assert count == (2 if is_geo else 1) and page[0]["npi_code"] == native._A


@pytest.mark.asyncio
async def test_year_and_panel_are_selected_before_child_metrics_without_fallback():
    async with native._case(children=_children()) as (case, target), case.sessions() as session:
        query = fixture.query(context_filters=(_PANEL, _KEY), filters=(ReadFilter("amount", "gt", "90"),))
        assert (await native._relation(session, target, query))[1] == []
        older = replace(query, context_filters=(_PANEL, _KEY, ReadFilter("period", "eq", 2023)))
        assert [row.entity_value for row in (await native._relation(session, target, older))[1]] == [native._A]
        panel_b = replace(query, context_filters=(ReadFilter("segment", "eq", "segment_b"), _KEY))
        assert [row.entity_value for row in (await native._relation(session, target, panel_b))[1]] == [native._A]
        missing_year = replace(query, context_filters=(_PANEL, _KEY, ReadFilter("period", "eq", 2022)))
        assert (await native._relation(session, target, missing_year))[1] == []


@pytest.mark.asyncio
async def test_child_eligibility_keeps_complete_sibling_families_and_page_bound(monkeypatch):
    async with native._case(children=_children()) as (case, target), case.sessions() as session:
        query = fixture.query(
            context_filters=(_PANEL, ReadFilter("service_code", "eq", "both")), family_entitlement="full_family"
        )
        page = await native._page(session, target, query)
        assert set(page) == {native._A}
        selected = page[native._A]
        assert selected.selection_value == 2024
        assert [group for group, _detail in selected.families] == ["segment_a", "segment_b"]
        keys = [
            next(field.value for field in child.fields if field.field_id == "service_code")
            for _group, detail in selected.families
            for child in detail.children
        ]
        assert sorted(keys) == ["both", "both_again", "chosen", "chosen", "nullable", "sibling"]
        monkeypatch.setattr(read_core, "MAX_DETAIL_CHILDREN", 5)
        with pytest.raises(read_core.CustomImportReadUnavailableError, match="child limit"):
            await native._page(session, target, query)


def _native_ctes(context, *, is_geo):
    rows = [(native._A, 1), (native._B, 2), (native._C, 3), (_ABSENT, 4)]
    if is_geo:
        rows.extend([(native._A, 5), (native._A, 1)])
    values = ", ".join(f"('{npi}', '00000000-0000-4000-8000-{ordinal:012d}'::uuid, 1.0)" for npi, ordinal in rows)
    native_cte = f"native_input(npi_code, address_key, cursor_distance_meters) AS (VALUES {values})"
    relation_cte = f"custom_import_provider_relation AS ({context.compiled.sql})"
    if is_geo:
        eligible = "eligible_geo AS (SELECT DISTINCT * FROM native_input)"
        return f"{relation_cte}, {native_cte}, {eligible}, {provider_geo_sql._selected_geo_cte(context)}"
    membership = provider_list_sql._provider_import_membership_clause(context, "native_input.npi_code")
    selected = "selected_list AS (SELECT native_input.*, imported.* FROM native_input LEFT JOIN custom_import_provider_relation AS imported ON imported.entity_value = native_input.npi_code"
    return f"{relation_cte}, {native_cte}, {selected}{' WHERE ' + membership if membership else ''})"


async def _native_rows(session, prepared, *, is_geo, require_match=False, anchor=None, offset=0):
    context = ProviderImportQuery(prepared, compile_npi_entity_relation(prepared.statement), require_match)
    ctes = _native_ctes(context, is_geo=is_geo)
    if is_geo:
        order = provider_geo_sql._order_clause(context, "selected_geo")
        clause = ""
        if anchor is not None:
            ctes += ", cursor_anchor AS (SELECT * FROM selected_geo WHERE npi_code = :anchor_npi AND address_key = CAST(:anchor_address AS uuid))"
            clause = " CROSS JOIN cursor_anchor" + provider_geo_sql._keyset_clause(
                context, "selected_geo", "cursor_anchor"
            )
        sql = f"SELECT selected_geo.* FROM selected_geo{clause} ORDER BY {order} LIMIT 1"
        count_sql = "SELECT count(*) FROM selected_geo"
    else:
        order = provider_list_sql._provider_import_order_clause(context, "imported.npi_code")
        sql = f"SELECT imported.* FROM selected_list AS imported ORDER BY {order} LIMIT 1 OFFSET :page_offset"
        count_sql = "SELECT count(*) FROM selected_list"
    schema = _quoted_publication_schema(session.get_bind().get_execution_options()["schema_translate_map"]["mrf"])
    parameters_by_name = {
        "page_offset": offset,
        "anchor_npi": anchor[0] if anchor else None,
        "anchor_address": anchor[1] if anchor else None,
    }
    count = await session.scalar(
        text(f"WITH {ctes} {count_sql}".replace("mrf.", f"{schema}.")).bindparams(*context.compiled.typed_binds),
        parameters_by_name,
    )
    page_result = await session.execute(
        text(f"WITH {ctes} {sql}".replace("mrf.", f"{schema}.")).bindparams(*context.compiled.typed_binds),
        parameters_by_name,
    )
    return count, page_result.mappings().all()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_geo", [False, True])
@pytest.mark.parametrize("direction", ["asc", "desc"])
async def test_native_exact_counts_and_stable_child_order_pagination(is_geo, direction):
    async with native._case(children=_children()) as (case, target), case.sessions() as session:
        prepared, rows = await native._relation(session, target, _ordered_query(direction=direction))
        assert set(tuple(row) for row in rows) == {(native._A, Decimal("2")), (native._B, Decimal("5"))}
        received, anchor = [], None
        total = 3 if is_geo else 2
        for offset in range(total):
            count, page = await _native_rows(session, prepared, is_geo=is_geo, anchor=anchor, offset=offset)
            assert count == total and len(page) == 1
            received.append((page[0]["npi_code"], str(page[0]["address_key"])))
            anchor = received[-1]
        assert len(set(received)) == total
        expected = [native._A] * (2 if is_geo else 1) + [native._B]
        if direction == "desc":
            expected = [native._B] + [native._A] * (2 if is_geo else 1)
        assert [npi for npi, _address in received] == expected
        assert (await _native_rows(session, prepared, is_geo=is_geo, anchor=anchor, offset=total))[1] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("is_geo", [False, True])
@pytest.mark.parametrize("direction", ["asc", "desc"])
async def test_present_null_sorts_last_and_missing_child_excludes_native_rows(is_geo, direction):
    async with native._case(children=_children()) as (case, target), case.sessions() as session:
        query = _ordered_query("nullable", direction, family_entitlement="full_family")
        prepared, rows = await native._relation(session, target, query)
        assert set(tuple(row) for row in rows) == {(native._A, Decimal("2")), (native._B, None)}
        first_count, first = await _native_rows(session, prepared, is_geo=is_geo)
        assert first_count == (3 if is_geo else 2) and first[0]["npi_code"] == native._A
        anchor = None
        selected_npis = []
        for offset in range(first_count):
            _count, page = await _native_rows(session, prepared, is_geo=is_geo, anchor=anchor, offset=offset)
            selected_npis.append(page[0]["npi_code"])
            anchor = (page[0]["npi_code"], str(page[0]["address_key"]))
        assert selected_npis == [native._A] * (2 if is_geo else 1) + [native._B]
        missing = _ordered_query("both", direction, family_entitlement="full_family")
        missing_prepared, missing_rows = await native._relation(session, target, missing)
        assert len(missing_rows) == 1 and missing_rows[0].entity_value == native._A
        assert (await _native_rows(session, missing_prepared, is_geo=is_geo))[0] == (2 if is_geo else 1)
        assert set(await native._page(session, target, missing)) == {native._A}


@pytest.mark.asyncio
async def test_signed_detail_verifies_child_capability_and_full_family(monkeypatch):
    http_fixture._install_keyring(monkeypatch)
    async with native._case(children=_children()) as (case, target):
        document_map = {
            "target": transport._target_document(native._external_target(target)),
            "entity": {"adapter_id": "npi", "value": native._A},
            "family_entitlement": "full_family",
            "context": [{"field_id": "service_code", "operator": "eq", "value": "chosen"}],
            "grouped_entity_selection": native.fixture.selection_document(),
            "grouped_child_query": fixture.child_descriptor(),
        }
        for mode, status in [("valid", 200), ("missing", 400), ("stale", 400), ("absent", 404)]:
            body_map = {**document_map}
            if mode == "missing":
                body_map.pop("grouped_child_query")
            if mode == "stale":
                body_map["grouped_child_query"] = {**fixture.child_descriptor(), "key_field_id": "amount"}
            if mode == "absent":
                body_map["context"] = [{"field_id": "service_code", "operator": "eq", "value": "absent"}]
            body = transport._canonical_json_bytes(body_map)
            headers = http_fixture._resigned_headers(
                body=body, path=transport.CUSTOM_IMPORT_DETAIL_PATH, target=document_map["target"]
            )
            request = http_fixture._Request(body, headers, path=transport.CUSTOM_IMPORT_DETAIL_PATH)
            async with case.sessions() as session:
                reply = await transport.serve_custom_import_detail(request, session)
            assert reply.status == status, (mode, reply.body)
            if mode == "valid":
                assert len(json.loads(reply.body)["families"]) == 2


@pytest.mark.asyncio
async def test_child_descriptor_cannot_be_added_after_transport_signing(monkeypatch):
    http_fixture._install_keyring(monkeypatch)
    async with native._case(children=_children()) as (case, target), case.sessions() as session:
        path = provider_http.CUSTOM_IMPORT_PROVIDERS_PATH
        request = native._page_request(target, path, full_family=True)
        changed_map = {**json.loads(request.body), "grouped_child_query": fixture.child_descriptor()}
        tampered = http_fixture._Request(transport._canonical_json_bytes(changed_map), request.headers, path=path)
        assert (await provider_http.serve_custom_import_providers(tampered, session)).status == 404
        assert not session.in_transaction()


@pytest.mark.asyncio
async def test_hydration_rejects_changed_child_query_identity():
    async with native._case(children=_children()) as (case, target), case.sessions() as session:
        prepared, _rows = await native._relation(session, target, _ordered_query())
        with pytest.raises(read_core.CustomImportReadUnavailableError):
            await native._service(target).hydrate_npi_page(
                session,
                authorization=native._AUTHORIZATION,
                pinned_target=target,
                prepared=prepared,
                entity_values=(native._A,),
                query=_ordered_query("sibling"),
            )


async def _signed_page_reply(session, prepared, *, is_geo):
    native_rows, anchor = [], None
    count, _first = await _native_rows(session, prepared, is_geo=is_geo)
    for offset in range(count):
        _count, page = await _native_rows(session, prepared, is_geo=is_geo, anchor=anchor, offset=offset)
        anchor = (page[0]["npi_code"], str(page[0]["address_key"]))
        native_rows.append({"npi": anchor[0], "address_key": anchor[1]})
    if not is_geo:
        return response.json({"rows": native_rows, "total": count})
    return response.json(
        {
            "items": native_rows,
            "total_count": count,
            "next_cursor": None,
            "has_more": False,
            "result_identity": ["npi", "address_key"],
            "_custom_import_next_anchor": None,
        }
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("is_geo", [False, True])
async def test_signed_child_pages_require_selected_episode_and_keep_complete_families(monkeypatch, is_geo):
    http_fixture._install_keyring(monkeypatch)
    module = geo_http if is_geo else provider_http
    path = geo_http.CUSTOM_IMPORT_PROVIDER_GEO_PATH if is_geo else provider_http.CUSTOM_IMPORT_PROVIDERS_PATH
    endpoint = geo_http.serve_custom_import_provider_geo if is_geo else provider_http.serve_custom_import_providers
    async with native._case(children=_children()) as (case, pinned_target), case.sessions() as session:

        async def native_page(request, *, native_args, import_context, prepare_cursor=None):
            if prepare_cursor is not None:
                await prepare_cursor(session=session, coordinates=[40, -73])
            return await _signed_page_reply(session, import_context.prepared, is_geo=is_geo)

        monkeypatch.setattr(module, "_geo_page" if is_geo else "_provider_page", native_page)
        baseline = native._page_request(pinned_target, path, full_family=True)
        body_map = json.loads(baseline.body)
        body_map["grouped_child_query"] = fixture.child_descriptor()
        body_map["context"].append({"field_id": "service_code", "operator": "eq", "value": "both"})
        body_map["order"] = [{"field_id": "amount", "direction": "desc"}]
        body = transport._canonical_json_bytes(body_map)
        headers = http_fixture._resigned_provider_headers(body=body, path=path, target=body_map["target"])
        reply = await endpoint(http_fixture._Request(body, headers, path=path), session)
        assert reply.status == 200, reply.body
        result_map = json.loads(reply.body)
        assert result_map["total_count" if is_geo else "total"] == (2 if is_geo else 1)
        families = []
        for provider_row in result_map["items" if is_geo else "rows"]:
            if provider_row["npi"] == native._A:
                families.append(provider_row["custom_import"])
                assert provider_row["custom_import"]["projection"] == "full_family"
                assert len(provider_row["custom_import"]["families"]) == 2
            else:
                assert provider_row["custom_import"] is None
        assert len(families) == (2 if is_geo else 1) and families[0] == families[-1]


def _numeric_definition():
    document = fixture.definition_document()
    next(field for field in document["schema"]["root"]["fields"] if field["id"] == "score")["type"] = "integer"
    return CustomImportDefinition.from_mapping(document)


def _numeric_query(target, field, operator, lexeme, *, context=None):
    external_target = transport._target_document(native._external_target(target))
    body = provider_fixture._body(
        target=external_target,
        context=context or [{"field_id": "panel_alias", "operator": "eq", "value": "segment_a"}],
        filters=[{"field_id": field, "operator": operator, "value": {"decimal": lexeme}}],
        order=None,
        require_match=True,
        grouped_entity_selection=native.fixture.selection_document(),
        grouped_child_query=fixture.child_descriptor(),
        family_entitlement="full_family",
    )
    parsed = provider_http._parse_provider_request(body)
    path = provider_http.CUSTOM_IMPORT_PROVIDERS_PATH
    transport._verify_provider_transport(
        headers=http_fixture._resigned_provider_headers(body=body, path=path, target=external_target),
        body=body,
        request=parsed,
        trusted_now=http_fixture._NOW,
        keyring=transport._load_keyring(http_fixture._keyring_document()),
        path=path,
    )
    return provider_http._provider_relation_query(parsed)


@pytest.mark.asyncio
async def test_signed_numeric_operands_compare_natively_without_changing_imported_types(monkeypatch):
    roots = [{**root_map, "score": int(root_map["score"])} for root_map in native._roots()]
    roots[0]["score"], roots[4]["score"] = 2, 3
    roots[1]["score"], roots[5]["score"] = -(2**63), 2**63 - 1
    monkeypatch.setattr(native.fixture, "definition", _numeric_definition)
    monkeypatch.setattr(native, "_roots", lambda: roots)
    child_rows = [
        {
            "rate_npi": npi,
            "rate_period": year,
            "rate_segment": "segment_a",
            "service_code": "chosen",
            "amount": Decimal(amount),
        }
        for npi, year, amount in [(native._A, 2024, "0.85"), (native._B, 2025, "0.86")]
    ]
    async with native._case(children=child_rows) as (case, pinned_target), case.sessions() as session:
        for field, operator, lexeme, expected in [
            ("metric_alias", "gt", "2.5", {native._B}),
            ("metric_alias", "lt", "2.5", {native._A}),
            ("metric_alias", "eq", "2.5", set()),
            ("metric_alias", "eq", "2.0", {native._A}),
            ("cost_alias", "gt", "0.85", {native._B}),
            ("cost_alias", "lt", "0.85", set()),
            ("cost_alias", "eq", "0.85", {native._A}),
            ("cost_alias", "gt", "0.849999999999", {native._A, native._B}),
        ]:
            query = _numeric_query(pinned_target, field, operator, lexeme)
            _prepared, entity_rows = await native._relation(session, pinned_target, query)
            assert {entity_row.entity_value for entity_row in entity_rows} == expected
        page = await native._page(session, pinned_target, _numeric_query(pinned_target, "metric_alias", "gt", "2.5"))
        score = next(
            projected_field
            for _group, family in page[native._B].families
            for projected_field in family.root_fields
            if projected_field.field_id == "score"
        )
        assert score.field_type == "integer" and type(score.value) is int and score.value == 3
        amount = next(
            projected_field
            for _group, family in page[native._B].families
            for child in family.children
            for projected_field in child.fields
            if projected_field.field_id == "amount"
        )
        assert amount.field_type == "decimal" and type(amount.value) is Decimal and amount.value == Decimal("0.86")
        await _assert_integer_edges(session, pinned_target)


async def _assert_integer_edges(session, target):
    selectors = [
        {"field_id": "segment", "operator": "eq", "value": "segment_b"},
        {"field_id": "period", "operator": "eq", "value": 2024},
    ]
    for operator, lexeme, expected in [
        ("gt", "9223372036854775806.5", {native._B}),
        ("eq", "9223372036854775806.5", set()),
        ("gt", "9223372036854775807.5", set()),
        ("lt", "-9223372036854775807.5", {native._A}),
    ]:
        query = _numeric_query(target, "score", operator, lexeme, context=selectors)
        _prepared, rows = await native._relation(session, target, query)
        assert {row.entity_value for row in rows} == expected
    selectors[1]["value"] = {"decimal": "2024.5"}
    with pytest.raises(read_core.CustomImportReadRequestError):
        await native._relation(session, target, _numeric_query(target, "score", "gt", "2.5", context=selectors))


def _values(fields):
    return {field.field_id: field.value for field in fields}


async def _grouped_target(case):
    seed = await runner_fixture._seed_case(case, "grouped_read", read_fixture._ranked_family_definition())
    execution, token = await runner_fixture._new_execution(case, seed, "grouped_read")
    roots = [
        runner_fixture._root_with_rank(npi, name, rank)
        for npi in ("1234567893", "1234567802")
        for name, rank in (("Before", 1), ("Selected", 2))
    ]
    child_records = [
        runner_fixture._rate_with_rank(npi, code, Decimal(amount), rank)
        for npi in ("1234567893", "1234567802")
        for code, amount, rank in (("A", "10", 1), ("B", "20", 1), ("A", "4", 2), ("B", "8", 2))
    ]
    run_result = await run_candidate(
        case.sessions, runner_fixture._request(seed, execution, token, roots, child_records)
    )
    assert run_result.status == "activated" and run_result.accepted_family_count == 4
    assert run_result.rejection_count == 0
    return PinnedReadTarget(
        dataset_id=seed.dataset_id,
        generation_id=run_result.generation_id,
        definition_revision_id=seed.definition_revision_id,
        schema_revision_id=seed.schema_revision_id,
        profile_id="default",
    )


@pytest.mark.asyncio
async def test_grouped_reads_filter_selected_children_before_exact_count_and_pages():
    async with isolated_publication_case() as case:
        pinned_target = await _grouped_target(case)
        service = read_fixture._service()
        authorization = ExtensionReadAuthorization("synthetic-grouped-read")
        filters = (ReadFilter("service_code", "eq", "A"), ReadFilter("amount", "lt", "5"))
        async with case.sessions() as session:
            first = await service.search(
                session,
                authorization=authorization,
                request=SearchRequest(target=pinned_target, filters=filters, page_size=1),
            )
            assert first.total == 2 and len(first.items) == 1 and first.next_cursor
            second = await service.search(
                session,
                authorization=authorization,
                request=SearchRequest(target=pinned_target, filters=filters, page_size=1, cursor=first.next_cursor),
            )
            assert second.total == 2 and len(second.items) == 1 and second.next_cursor is None
            assert first.items[0].winner != second.items[0].winner
            assert {_values(page_item.root_fields)["npi"] for page_item in (*first.items, *second.items)} == {
                "1234567893",
                "1234567802",
            }
            for page_item in (*first.items, *second.items):
                assert _values(page_item.root_fields)["display_name"] == "Selected"
                assert _values(page_item.context_fields) == {"service_code": "A", "amount": Decimal("4")}
                detail = await service.root_detail(
                    session, authorization=authorization, target=pinned_target, winner=page_item.winner
                )
                assert len(detail.children) == 2
                assert {
                    _values(child.fields)["service_code"]: _values(child.fields)["amount"] for child in detail.children
                } == {"A": Decimal("4"), "B": Decimal("8")}


@pytest.mark.asyncio
async def test_grouped_reads_never_use_siblings_or_older_family_to_satisfy_metrics():
    async with isolated_publication_case() as case:
        pinned_target = await _grouped_target(case)
        service = read_fixture._service()
        authorization = ExtensionReadAuthorization("synthetic-grouped-boundary")
        async with case.sessions() as session:
            rejected = await service.search(
                session,
                authorization=authorization,
                request=SearchRequest(
                    target=pinned_target,
                    filters=(ReadFilter("service_code", "eq", "A"), ReadFilter("amount", "gt", "5")),
                    page_size=1,
                ),
            )
            assert (rejected.total, rejected.items, rejected.next_cursor) == (0, (), None)
            accepted = await service.search(
                session,
                authorization=authorization,
                request=SearchRequest(
                    target=pinned_target,
                    filters=(ReadFilter("service_code", "eq", "B"), ReadFilter("amount", "gt", "5")),
                ),
            )
            assert accepted.total == len(accepted.items) == 2
            assert {_values(page_item.context_fields)["amount"] for page_item in accepted.items} == {Decimal("8")}
            assert {_values(page_item.root_fields)["display_name"] for page_item in accepted.items} == {"Selected"}


@pytest.mark.asyncio
async def test_four_scores_preserve_groups_and_complete_families(monkeypatch):
    monkeypatch.setattr(native.fixture, "definition", scores._definition)
    roots = native._roots()
    monkeypatch.setattr(native, "_roots", lambda: [dict(row, weight=2) for row in roots])
    filters = tuple(
        ReadFilter(field, "gt", 0) for field in ("root_cost", "root_quality", "child_cost", "child_quality")
    )
    order_terms = tuple(
        ReadOrderTerm(field, "asc", "last") for field in ("root_cost", "root_quality", "child_cost", "child_quality")
    )
    query = fixture.query(
        context_filters=(_KEY,),
        filters=filters,
        order_terms=order_terms,
        family_entitlement="full_family",
    )
    async with native._case(children=_children()) as (case, target), case.sessions() as session:
        prepared, rows = await native._relation(session, target, query)
        assert set(row.entity_value for row in rows) == {native._A, native._B}
        selected_provider = next(row for row in rows if row.entity_value == native._A)
        assert tuple(selected_provider)[1:] == (Decimal("15"), 2, Decimal("90.916666666667"), Decimal("9"))
        raw_panel = replace(query, context_filters=(_KEY, _PANEL))
        assert [tuple(row) for row in (await native._relation(session, target, raw_panel))[1]] == [
            tuple(row) for row in rows
        ]
        families = await native._page(session, target, query)
        assert [group for group, _family in families[native._A].families] == ["segment_a", "segment_b"]
        assert len(families[native._A].families[0][1].children) == 5
        assert prepared.effective_require_match is True


@pytest.mark.asyncio
async def test_reduced_scores_preserve_panel_membership_nulls_and_mixed_raw_metrics(monkeypatch):
    monkeypatch.setattr(native.fixture, "definition", scores._definition)
    roots = native._roots()
    monkeypatch.setattr(native, "_roots", lambda: [dict(source_row, weight=2) for source_row in roots])
    query = fixture.query(
        context_filters=(_KEY, ReadFilter("segment", "eq", "segment_b")),
        order_terms=(ReadOrderTerm("root_cost", "asc", "last"),),
        require_match=False,
    )
    async with native._case(children=_children()) as (case, pinned_target), case.sessions() as session:
        prepared, selected_rows = await native._relation(session, pinned_target, query)
        assert [tuple(selected_row) for selected_row in selected_rows] == [(native._A, Decimal("15"))]
        assert "FROM derived_score_values" in str(prepared.statement)
        nullable = replace(
            query,
            context_filters=(ReadFilter("service_code", "eq", "nullable"),),
            order_terms=(ReadOrderTerm("child_cost", "asc", "last"),),
        )
        selected_rows = (await native._relation(session, pinned_target, nullable))[1]
        assert {tuple(selected_row) for selected_row in selected_rows} == {(native._A, Decimal("2")), (native._B, None)}
        positive = replace(nullable, filters=(ReadFilter("child_cost", "gt", 0),), require_match=True)
        assert [
            selected_row.entity_value for selected_row in (await native._relation(session, pinned_target, positive))[1]
        ] == [native._A]
        mixed = replace(
            query,
            context_filters=(_KEY, _PANEL),
            filters=(ReadFilter("root_cost", "gt", 0), ReadFilter("amount", "gt", 1)),
            order_terms=(ReadOrderTerm("root_cost", "asc", "last"), ReadOrderTerm("quality", "desc", "last")),
            require_match=True,
        )
        prepared, selected_rows = await native._relation(session, pinned_target, mixed)
        assert "JOIN derived_score_values" in str(prepared.statement)
        assert next(
            tuple(selected_row)[1:] for selected_row in selected_rows if selected_row.entity_value == native._A
        ) == (Decimal("15"), Decimal("9"))
        assert set(selected_row.entity_value for selected_row in selected_rows) == {native._A, native._B}


@pytest.mark.asyncio
async def test_missing_group_child_preserves_root_reduction_and_same_family_membership(monkeypatch):
    monkeypatch.setattr(native.fixture, "definition", scores._definition)
    roots = native._roots()
    monkeypatch.setattr(native, "_roots", lambda: [dict(source_row, weight=2) for source_row in roots])
    child_rows = [
        child_row
        for child_row in _children()
        if child_row["service_code"] != "chosen"
        or (child_row["rate_npi"], child_row["rate_period"], child_row["rate_segment"])
        == (native._A, 2024, "segment_a")
    ]
    fields = ("root_cost", "root_quality", "child_cost", "child_quality")
    query = fixture.query(
        context_filters=(_KEY,),
        filters=tuple(ReadFilter(field, "gt", 0) for field in fields),
        order_terms=tuple(ReadOrderTerm(field, "asc", "last") for field in fields),
    )
    async with native._case(children=child_rows) as (case, pinned_target), case.sessions() as session:
        prepared, selected_rows = await native._relation(session, pinned_target, query)
        assert "complete_score_child_keys AS MATERIALIZED" not in str(prepared.statement)
        assert [tuple(selected_row) for selected_row in selected_rows] == [
            (native._A, Decimal("15"), 2, Decimal("2"), Decimal("9"))
        ]
        absent_group = replace(query, context_filters=(_KEY, ReadFilter("segment", "eq", "segment_b")))
        assert (await native._relation(session, pinned_target, absent_group))[1] == []
        absent_key = replace(query, context_filters=(ReadFilter("service_code", "eq", "absent"),))
        assert (await native._relation(session, pinned_target, absent_key))[1] == []
        mixed = replace(
            query,
            context_filters=(_KEY, _PANEL),
            filters=(ReadFilter("root_cost", "gt", 0), ReadFilter("amount", "gt", 1)),
            order_terms=(ReadOrderTerm("root_cost", "asc", "last"), ReadOrderTerm("amount", "desc", "last")),
        )
        assert [tuple(selected_row) for selected_row in (await native._relation(session, pinned_target, mixed))[1]] == [
            (native._A, Decimal("15"), Decimal("2"))
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguous", (False, True))
async def test_root_preserving_child_guard_keeps_zero_children_and_rejects_hidden_ambiguity(
    ambiguous, values_connection
):
    child_rows = [(1, 2, 3, Decimal("1")), (2, 4, None, None)]
    if ambiguous:
        child_rows.append((1, 2, 5, Decimal("9")))
    children = (
        values(
            column("family_revision_id", BigInteger),
            column("root_record_id", BigInteger),
            column("child_revision_id", BigInteger),
            column("amount", Numeric),
        )
        .data(child_rows)
        .cte("synthetic_nullable_children")
    )
    validated = grouped_query._validated_stored_children(children, preserve_roots=True)
    statement = select(validated.c.child_revision_id, validated.c.validated_child_id).where(
        (validated.c.amount < 2) | validated.c.amount.is_(None)
    )
    if ambiguous:
        with pytest.raises(DBAPIError, match="more than one row"):
            await values_connection.execute(statement)
    else:
        assert set((await values_connection.execute(statement)).all()) == {(3, 3), (None, None)}


@pytest.mark.asyncio
@pytest.mark.parametrize("ambiguous", (False, True))
async def test_materialized_complete_child_identity_retains_native_ambiguity_failure(ambiguous, values_connection):
    child_rows = ((1, 2, 3), (1, 2, 4)) if ambiguous else ((1, 2, 3),)
    children = (
        values(*(column(name, BigInteger) for name in ("family_revision_id", "root_record_id", "child_revision_id")))
        .data(child_rows)
        .cte("synthetic_children")
    )
    selected = select(children.c.family_revision_id, children.c.root_record_id).distinct().cte("selected_families")
    identities, ownership, child_id = grouped_query._complete_child_identity(children, selected)
    statement = select(child_id).select_from(selected).outerjoin(identities, ownership)
    if ambiguous:
        with pytest.raises(DBAPIError, match="more than one row"):
            await values_connection.execute(statement)
    else:
        assert (await values_connection.execute(statement)).scalar_one() == 3


def test_episode_alias_promotes_membership_but_root_order_retains_native_nulls():
    child_query = fixture.query(
        context_filters=(ReadFilter("panel_alias", "eq", "segment_a"), ReadFilter("child_alias", "eq", "chosen")),
        order_terms=(ReadOrderTerm("cost_alias", "asc", "last"),),
        require_match=False,
    )
    context, scope = fixture.context(), ExtensionReadScope("synthetic:grouped")
    assert grouped_read.prepare_relation(context, child_query, scope).effective_require_match is True
    root_query = replace(child_query, context_filters=(_PANEL,), order_terms=(ReadOrderTerm("score", "asc", "last"),))
    assert grouped_read.prepare_relation(context, root_query, scope).effective_require_match is False


def test_derived_child_metrics_require_the_complete_key_even_without_order():
    context = replace(fixture.context(), definition=scores._definition())
    with pytest.raises(read_core.CustomImportReadRequestError, match="complete-key"):
        grouped_read.prepare_relation(
            context, fixture.query(filters=(ReadFilter("child_cost", "gt", "1"),)), ExtensionReadScope("synthetic")
        )


def _renamed_identifiers(document, identifiers):
    if type(document) is str:
        return identifiers.get(document, document)
    if type(document) is list:
        return [_renamed_identifiers(value, identifiers) for value in document]
    if type(document) is dict:
        return {key: _renamed_identifiers(value, identifiers) for key, value in document.items()}
    return document


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identifiers",
    (
        {
            "weight": "state_score",
            "amount": "value_score",
            "quality": "state_value_score",
            "root_quality": "entity_value",
        },
        {
            name: "a" * 62 + suffix
            for name, suffix in zip(
                ("score", "weight", "amount", "quality", "root_cost", "child_cost"), "abcdef", strict=True
            )
        },
    ),
)
async def test_configured_field_identifiers_cannot_collide_with_internal_sql_columns(monkeypatch, identifiers):
    document = _renamed_identifiers(json.loads(scores._definition().canonical), identifiers)
    definition = CustomImportDefinition.from_mapping(document)
    monkeypatch.setattr(native.fixture, "definition", lambda: definition)
    roots = [
        {identifiers.get(key, key): field_value for key, field_value in dict(source_row, weight=2).items()}
        for source_row in native._roots()
    ]
    child_rows = [
        {identifiers.get(key, key): field_value for key, field_value in source_row.items()}
        for source_row in _children()
    ]
    monkeypatch.setattr(native, "_roots", lambda: roots)
    fields = tuple(
        identifiers.get(field, field) for field in ("root_cost", "root_quality", "child_cost", "child_quality")
    )
    query = fixture.query(
        context_filters=(_KEY,),
        filters=tuple(ReadFilter(field, "gt", 0) for field in fields),
        order_terms=tuple(ReadOrderTerm(field, "asc", "last") for field in fields),
        family_entitlement="full_family",
    )
    async with native._case(children=child_rows) as (case, pinned_target), case.sessions() as session:
        eligible_rows = (await native._relation(session, pinned_target, query))[1]
        assert set(provider_row.entity_value for provider_row in eligible_rows) == {native._A, native._B}
        selected_provider = next(
            provider_row for provider_row in eligible_rows if provider_row.entity_value == native._A
        )
        assert tuple(selected_provider)[1:] == (Decimal("15"), 2, Decimal("90.916666666667"), Decimal("9"))
        raw = replace(
            query,
            context_filters=(_KEY, _PANEL),
            filters=(ReadFilter(identifiers.get("score", "score"), "gt", 0),),
            order_terms=(ReadOrderTerm(identifiers.get("amount", "amount"), "asc", "last"),),
        )
        assert (await native._relation(session, pinned_target, raw))[1]
        assert len((await native._page(session, pinned_target, query))[native._A].families[0][1].children) == 5
