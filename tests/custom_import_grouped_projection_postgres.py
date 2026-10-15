# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Execute complete projected and legacy relations over finalized native families."""

import json
from dataclasses import replace
from decimal import Decimal

import pytest
from sqlalchemy import BigInteger, LargeBinary, Numeric, Text, and_, column, select, values
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import aliased

from process.custom_import import grouped_query, grouped_read, read_core
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.storage_layout import SNAPSHOT_MODELS
from tests import custom_import_grouped_child_support as fixture
from tests import test_custom_import_grouped_child_read_postgres as child_native
from tests import test_custom_import_grouped_score_query_postgres as scores
from tests import test_custom_import_provider_hydration_postgres as native
from tests.test_custom_import_projection_read_context import projection_helper as projection_helper

_KEY = read_core.ReadFilter("service_code", "eq", "chosen")
_PANEL = read_core.ReadFilter("segment", "eq", "segment_a")
_SCOPE = read_core.ExtensionReadScope("synthetic")
_ROOT_SCORES = (
    (native._A, 2024, Decimal("15"), 2),
    (native._A, 2023, Decimal("100"), 2),
    (native._B, 2025, None, 0),
    (native._B, 2024, Decimal("90"), 2),
    (native._C, 2025, Decimal("999"), 2),
)
_CHILD_SCORES = (
    (native._A, 2024, "chosen", "2", "9"),
    (native._A, 2024, "sibling", "99", "99"),
    (native._A, 2024, "other_group", "99", "99"),
    (native._A, 2024, "nullable", "2", "3"),
    (native._A, 2023, "older_only", "99", "9"),
    (native._B, 2025, "chosen", "5", "4"),
    (native._B, 2025, "nullable", None, "4"),
    (native._B, 2024, "chosen", "88", "88"),
    (native._C, 2025, "chosen", "99", "99"),
)


def _definition():
    document = json.loads(scores._definition().canonical)
    document["query"]["filterable_fields"] = ["root_cost", "child_cost", "score", "amount"]
    document["query"]["aliases"].update({"output_cost": "root_cost", "output_child": "child_cost"})
    return CustomImportDefinition.from_mapping(document)


@pytest.fixture
async def projection_case(monkeypatch, projection_helper):
    projection_helper.read_projection_tables.return_value = None
    monkeypatch.setattr(native.fixture, "definition", _definition)
    roots = [dict(row, weight=0 if (row["npi"], row["period"]) == (native._B, 2025) else 2) for row in native._roots()]
    child_rows = [
        {
            "rate_npi": npi,
            "rate_period": period,
            "rate_segment": "segment_b" if key == "other_group" or (npi, period) == (native._B, 2024) else "segment_a",
            "service_code": key,
            "amount": None if amount is None else Decimal(amount),
            "quality": Decimal(quality),
        }
        for npi, period, key, amount, quality in _CHILD_SCORES
    ]
    async with native._case(roots=roots, children=child_rows) as (case, target), case.sessions() as session:
        context = await read_core._load_read_context(session, target)
        yield session, replace(context, projection_tables=None), await _projected_context(session, context)


def _projection_tables(bindings_by_npi, keys_by_code, *, omit_scope=None):
    root_rows = [
        (bindings_by_npi[npi], period, cost, quality)
        for npi, period, cost, quality in _ROOT_SCORES
        if omit_scope != "root" or (npi, period) != (native._A, 2024)
    ]
    child_rows = [
        (bindings_by_npi[npi], period, *keys_by_code[key], None if cost is None else Decimal(cost), Decimal(quality))
        for npi, period, key, cost, quality in _CHILD_SCORES
        if omit_scope != "child" or (npi, period, key) != (native._A, 2024, "chosen")
    ]
    return {
        "custom_import_derived_root": values(
            column("entity_binding_id", BigInteger),
            column("selected_value", BigInteger),
            column("field_1", Numeric(31, 12)),
            column("field_2", BigInteger),
        )
        .data(root_rows)
        .cte("synthetic_projected_roots"),
        "custom_import_derived_child": values(
            column("entity_binding_id", BigInteger),
            column("selected_value", BigInteger),
            column("child_key_sha256", LargeBinary(32)),
            column("canonical_child_key", Text(collation="C")),
            column("field_3", Numeric(31, 12)),
            column("field_4", Numeric(31, 12)),
        )
        .data(child_rows)
        .cte("synthetic_projected_children"),
    }


async def _projection_inputs(session, context):
    entity = context.model(read_core.CustomImportEntityBinding)
    bindings_by_npi = dict(
        (
            await session.execute(
                select(entity.canonical_value, entity.entity_binding_id).where(
                    entity.dataset_id == context.target.dataset_id
                )
            )
        ).all()
    )
    revision = context.model(read_core.CustomImportChildRevision)
    scalar = context.model(read_core.CustomImportChildScalar)
    identities = ("child_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "collection_slot")
    key_rows = (
        await session.execute(
            select(
                scalar.string_value,
                revision.child_key_sha256,
                revision.canonical_child_key,
            )
            .select_from(revision)
            .join(scalar, and_(*(getattr(scalar, name) == getattr(revision, name) for name in identities)))
            .where(
                revision.dataset_id == context.target.dataset_id,
                revision.schema_revision_id == context.target.schema_revision_id,
                revision.collection_slot == context.collection_slots_by_name["rates"],
                scalar.field_slot == context.definition.fields_by_id["service_code"].field_slot,
            )
            .distinct()
        )
    ).all()
    keys_by_code = {code: (key_hash, canonical_key) for code, key_hash, canonical_key in key_rows}
    assert len(keys_by_code) == len(key_rows)
    return bindings_by_npi, keys_by_code


async def _projected_context(session, context, *, omit_scope=None):
    bindings_by_npi, keys_by_code = await _projection_inputs(session, context)
    return replace(context, projection_tables=_projection_tables(bindings_by_npi, keys_by_code, omit_scope=omit_scope))


def _prepare(context, **changes_map):
    return grouped_read.prepare_relation(context, fixture.query(**changes_map), _SCOPE)


async def _assert_rows(session, contexts, expected, **changes_map):
    for context in contexts:
        prepared = _prepare(context, **changes_map)
        rows = [tuple(row) for row in (await session.execute(prepared.statement)).all()]
        assert sorted(rows) == sorted(expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ("root", "child"))
async def test_nullable_projection_preserves_membership(scope, projection_case):
    session, legacy, projected = projection_case
    metric = "output_cost" if scope == "root" else "output_child"
    selectors = () if scope == "root" else (read_core.ReadFilter("service_code", "eq", "nullable"),)
    changes_map = dict(context_filters=selectors, order_terms=(read_core.ReadOrderTerm(metric, "asc", "last"),))
    await _assert_rows(
        session,
        (legacy, projected),
        [(native._A, Decimal("15" if scope == "root" else "2")), (native._B, None)],
        **changes_map,
    )
    await _assert_rows(
        session, (legacy, projected), [], filters=(read_core.ReadFilter(metric, "gt", 10000),), **changes_map
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ("root", "child"))
@pytest.mark.parametrize("entity_values", (None, (native._A,)))
async def test_missing_projection_fails_before_filter(scope, entity_values, projection_case):
    session, legacy, projected = projection_case
    metric = "output_cost" if scope == "root" else "output_child"
    selectors = () if scope == "root" else (_KEY,)
    projected = await _projected_context(session, projected, omit_scope=scope)
    changes_map = dict(
        context_filters=selectors, filters=(read_core.ReadFilter(metric, "gt", 10000),), entity_values=entity_values
    )
    await _assert_rows(session, (legacy,), [], **changes_map)
    with pytest.raises(DBAPIError) as raised:
        async with session.begin_nested():
            await session.execute(_prepare(projected, **changes_map).statement)
    assert raised.value.orig.sqlstate == "21000"


@pytest.mark.asyncio
async def test_mixed_filters_keep_exact_context(projection_case):
    session, legacy, projected = projection_case
    changes_map = dict(
        context_filters=(_KEY, _PANEL),
        order_terms=(
            read_core.ReadOrderTerm("output_cost", "asc", "last"),
            read_core.ReadOrderTerm("output_child", "asc", "last"),
        ),
    )
    await _assert_rows(
        session,
        (legacy, projected),
        [],
        filters=(read_core.ReadFilter("child_cost", "gt", 0), read_core.ReadFilter("amount", "gt", 50)),
        **changes_map,
    )
    await _assert_rows(
        session,
        (legacy, projected),
        [(native._A, Decimal("15"), Decimal("2"))],
        filters=(
            read_core.ReadFilter("child_cost", "gt", 0),
            read_core.ReadFilter("amount", "gt", 1),
            read_core.ReadFilter("score", "lt", 15),
        ),
        **changes_map,
    )


@pytest.mark.asyncio
async def test_root_rollup_keeps_siblings_and_year(projection_case):
    session, legacy, projected = projection_case
    changes_map = dict(order_terms=(read_core.ReadOrderTerm("root_cost", "asc", "last"),))
    await _assert_rows(
        session,
        (legacy, projected),
        [(native._A, Decimal("15")), (native._B, None)],
        context_filters=(_KEY,),
        **changes_map,
    )
    await _assert_rows(
        session,
        (legacy, projected),
        [],
        context_filters=(_KEY, read_core.ReadFilter("segment", "eq", "segment_b")),
        **changes_map,
    )
    older = (read_core.ReadFilter("service_code", "eq", "older_only"),)
    for entity_values in (None, (native._A,)):
        await _assert_rows(
            session, (legacy, projected), [], context_filters=older, entity_values=entity_values, **changes_map
        )
        await _assert_rows(
            session,
            (legacy, projected),
            [(native._A, Decimal("100"))],
            context_filters=older + (read_core.ReadFilter("period", "eq", 2023),),
            entity_values=entity_values,
            **changes_map,
        )

    for selectors, metric, expected_rows in (
        ((), "root_cost", []),
        ((read_core.ReadFilter("period", "eq", 2026),), "root_cost", []),
        ((read_core.ReadFilter("period", "eq", 2025),), "root_cost", [(native._C, Decimal("999"))]),
        ((_KEY,), "child_cost", []),
        ((_KEY, read_core.ReadFilter("period", "eq", 2025)), "child_cost", [(native._C, Decimal("99"))]),
    ):
        await _assert_rows(
            session,
            (legacy, projected),
            expected_rows,
            context_filters=selectors,
            entity_values=(native._C,),
            order_terms=(read_core.ReadOrderTerm(metric, "asc", "last"),),
        )


@pytest.mark.asyncio
async def test_native_count_order_page_parity(projection_case):
    session, legacy, projected = projection_case
    for metric, selectors in (("root_cost", ()), ("child_cost", (_KEY,))):
        for direction in ("asc", "desc"):
            prepared_relations = [
                _prepare(
                    context,
                    context_filters=selectors,
                    order_terms=(read_core.ReadOrderTerm(metric, direction, "last"),),
                    require_match=False,
                )
                for context in (legacy, projected)
            ]
            for is_geo in (False, True):
                expected_total = (5 if is_geo else 4) if not selectors else (3 if is_geo else 2)
                pages = [
                    await _native_pages(session, prepared, is_geo=is_geo, expected_total=expected_total)
                    for prepared in prepared_relations
                ]
                assert pages[0] == pages[1]
                positive_order = (
                    [native._A, native._B] if metric == "root_cost" or direction == "asc" else [native._B, native._A]
                )
                expected_npis = [npi for npi in positive_order for _ in range(2 if is_geo and npi == native._A else 1)]
                if not selectors:
                    expected_npis += [native._C, child_native._ABSENT]
                assert [identity[0] for identity, _score in pages[0]] == expected_npis


async def _native_pages(session, prepared, *, is_geo, expected_total):
    items, anchor = [], None
    for offset in range(expected_total + 1):
        total, page = await child_native._native_rows(session, prepared, is_geo=is_geo, anchor=anchor, offset=offset)
        assert total == expected_total and len(page) == (offset < expected_total)
        if page:
            anchor = (page[0]["npi_code"], str(page[0]["address_key"]))
            items.append((anchor, page[0]["sort_0"]))
    assert len({identity for identity, _score in items}) == expected_total
    return items


@pytest.mark.asyncio
@pytest.mark.parametrize("entity_values", (None, (native._A,)))
async def test_duplicate_members_fail_before_metric(entity_values, projection_case):
    session, legacy, projected = projection_case
    plan = grouped_read.normalize_plan(legacy, fixture.query(context_filters=(_KEY,)), _SCOPE)
    selected = grouped_query.selected_identities(legacy, plan, (native._A,))
    members, _member, _revision = grouped_query._child_statement(
        legacy, selected, grouped_query._keyed_children(legacy, plan.context_filters)
    )
    child_id = (await session.execute(members)).one().child_revision_id
    member = legacy.model(read_core.CustomImportFamilyChild)
    duplicated = (
        select(member)
        .union_all(select(member).where(member.child_revision_id == child_id))
        .subquery("duplicated_complete_members")
    )
    for context in (legacy, projected):
        models_by_canonical = {model: context.model(model) for model in SNAPSHOT_MODELS}
        models_by_canonical[read_core.CustomImportFamilyChild] = aliased(
            read_core.CustomImportFamilyChild, duplicated, adapt_on_names=True
        )
        context = replace(context, storage_models=models_by_canonical)
        with pytest.raises(DBAPIError) as raised:
            async with session.begin_nested():
                await session.execute(
                    _prepare(
                        context,
                        context_filters=(_KEY,),
                        filters=(read_core.ReadFilter("root_cost", "gt", 10000),),
                        entity_values=entity_values,
                    ).statement
                )
        assert raised.value.orig.sqlstate == "21000"
