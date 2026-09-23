# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Unit contracts for the provider-query custom-import relation seam."""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects import postgresql

from process.custom_import import read_core
from process.custom_import.definition import CustomImportDefinition, load_json_definition
from process.custom_import.read_core import (
    CustomImportReadAuthorizationError,
    CustomImportReadRequestError,
    CustomImportReadService,
    ExtensionReadAuthorization,
    ExtensionReadScope,
    NpiEntityRelationQuery,
    PinnedReadTarget,
    ReadFilter,
    ReadOrderTerm,
)


def _target() -> PinnedReadTarget:
    return PinnedReadTarget(
        dataset_id=11,
        generation_id=12,
        definition_revision_id=13,
        schema_revision_id=14,
        profile_id="default",
    )


def _context(
    *,
    aliases: bool = False,
    context_dimensions: tuple[str, ...] | None = None,
    sortable_fields: tuple[str, ...] | None = None,
):
    raw = load_json_definition((Path(__file__).with_name("fixtures") / "custom_import/v1_valid.json").read_text())
    if aliases:
        raw["query"].update(
            {
                "aliases": {"metric": "amount", "service": "service_code"},
                "sortable_fields": ["amount"],
            }
        )
    if context_dimensions is not None:
        raw["selection_profiles"][0]["context_dimensions"] = list(context_dimensions)
    if sortable_fields is not None:
        raw["query"]["sortable_fields"] = list(sortable_fields)
    definition = CustomImportDefinition.from_mapping(raw)
    return read_core._ReadContext(_target(), definition, 1, 1, {"rates": 1}, {1: "rates"})


class _Allow:
    def authorize(self, authorization, *, target):
        del authorization, target
        return ExtensionReadScope("synthetic:provider-query")


class _CacheSpy:
    def __init__(self) -> None:
        self.get_calls = 0
        self.set_calls = 0

    async def get(self, key):
        del key
        self.get_calls += 1
        return None

    async def set(self, key, value, *, expires_at):
        del key, value, expires_at
        self.set_calls += 1


@pytest.mark.asyncio
async def test_provider_query_authorizes_before_any_storage_work():
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unexpected storage work")))
    service = CustomImportReadService(authorizer=None, cursor_secret=b"s" * 32)

    with pytest.raises(CustomImportReadAuthorizationError, match="^extension read is not authorized$"):
        await service.prepare_npi_entity_relation(
            session,
            authorization=ExtensionReadAuthorization("synthetic-token"),
            target=_target(),
        )

    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_provider_query_boundary_rejects_unbounded_shapes_before_context_loading(monkeypatch):
    service = CustomImportReadService(authorizer=_Allow(), cursor_secret=b"s" * 32)
    load_context = AsyncMock()
    monkeypatch.setattr(read_core, "_load_read_context", load_context)
    over_limit_filters = tuple(ReadFilter("npi", "eq", "1234567893") for _ in range(read_core.MAX_FILTER_TERMS + 1))
    over_limit_order_terms = tuple(ReadOrderTerm("amount", "asc", "last") for _ in range(read_core.MAX_ORDER_TERMS + 1))

    for filters, context_filters, order_terms in (
        ([], None, None),
        (over_limit_filters, None, None),
        ((), over_limit_filters, None),
        ((ReadFilter("npi", "eq", "1234567893"),) * 2, (ReadFilter("service", "eq", "99213"),) * 2, None),
        ((), None, []),
        ((), None, over_limit_order_terms),
    ):
        with pytest.raises(CustomImportReadRequestError):
            await service.prepare_npi_entity_relation(
                object(),
                authorization=ExtensionReadAuthorization("synthetic-token"),
                target=_target(),
                query=NpiEntityRelationQuery(
                    filters=filters,
                    context_filters=context_filters,
                    order_terms=order_terms,
                ),
            )

    load_context.assert_not_awaited()


def test_provider_query_normalizes_aliases_before_context_binding_and_fingerprinting():
    context = _context(aliases=True)
    aliased_filters = read_core._normalized_filters((ReadFilter("service", "eq", "99213"),), context)
    canonical_filters = read_core._normalized_filters((ReadFilter("service_code", "eq", "99213"),), context)
    aliased_order = read_core._normalize_query_order_terms(
        (ReadOrderTerm("metric", "desc", "last"),),
        context,
        explicit=True,
    )
    canonical_order = read_core._normalize_query_order_terms(
        (ReadOrderTerm("amount", "desc", "last"),),
        context,
        explicit=True,
    )

    assert aliased_order == canonical_order == (ReadOrderTerm("amount", "desc", "last"),)
    assert read_core._npi_entity_relation_fingerprint(context.target, aliased_filters, aliased_order) == (
        read_core._npi_entity_relation_fingerprint(context.target, canonical_filters, canonical_order)
    )
    assert len(read_core._npi_entity_relation_fingerprint(context.target, aliased_filters, aliased_order)) == 64


def test_provider_v2_query_normalizes_roles_before_binding_and_fingerprinting():
    context = _context(aliases=True)
    context_filters, metric_filters, order_terms = read_core._normalized_npi_query(
        context,
        (ReadFilter("service", "eq", "99213"),),
        (ReadFilter("metric", "gt", "5"),),
        (ReadOrderTerm("metric", "desc", "last"),),
    )

    assert context_filters[0].field.field_id == "service_code"
    assert metric_filters[0].field.field_id == "amount"
    assert order_terms == (ReadOrderTerm("amount", "desc", "last"),)
    repeated = read_core._normalized_filters((ReadFilter("service", "eq", "99213"),), context)
    assert read_core._npi_entity_relation_fingerprint(
        context.target, (), order_terms, context_filters=repeated
    ) != read_core._npi_entity_relation_fingerprint(context.target, repeated, order_terms)


@pytest.mark.parametrize(
    ("context_filters", "filters"),
    (
        ((ReadFilter("metric", "eq", "5"),), ()),
        ((ReadFilter("service", "neq", "99213"),), ()),
        ((), (ReadFilter("service", "eq", "99213"),)),
        ((), (ReadFilter("metric", "gte", "5"),)),
    ),
)
def test_provider_v2_query_rejects_crossed_context_and_metric_roles(context_filters, filters):
    with pytest.raises(CustomImportReadRequestError):
        read_core._normalized_npi_query(_context(aliases=True), context_filters, filters, None)


def test_provider_v2_relation_applies_metric_predicates_after_context_winner_selection(monkeypatch):
    context = _context(aliases=True)
    context_filters = read_core._normalized_filters((ReadFilter("service", "eq", "99213"),), context)
    metric_filters = read_core._normalized_filters((ReadFilter("metric", "gt", "5"),), context)
    selected = Mock()
    joined = Mock()
    bound = Mock()
    metric_bound = Mock()
    selected.join.return_value = joined
    joined.where.return_value = bound
    bound.where.return_value = metric_bound
    observed_context_filters = []
    observed_metric_filters = []

    def selected_winners(_context, received_context_filters):
        observed_context_filters.append(received_context_filters)
        return selected

    def metric_predicate(received_metric_filter, _context):
        observed_metric_filters.append(received_metric_filter)
        return object()

    monkeypatch.setattr(read_core, "_filtered_winner_statement", selected_winners)
    monkeypatch.setattr(read_core, "_predicate_condition", metric_predicate)

    assert (
        read_core._filtered_npi_winner_statement(context, metric_filters, context_filters=context_filters)
        is metric_bound
    )
    assert observed_context_filters == [context_filters]
    assert observed_metric_filters == list(metric_filters)
    assert bound.where.call_count == len(metric_filters)


@pytest.mark.asyncio
async def test_provider_v2_optional_membership_keeps_multi_context_filter_only_queries(monkeypatch):
    @asynccontextmanager
    async def bounded(session, *, timeout_ms):
        del session, timeout_ms
        yield

    statement = Mock()
    monkeypatch.setattr(read_core, "_bounded_read_window", bounded)
    monkeypatch.setattr(read_core, "_load_read_context", AsyncMock(return_value=_context(aliases=True)))
    monkeypatch.setattr(read_core, "_npi_entity_relation_statement", statement)
    service = CustomImportReadService(authorizer=_Allow())
    query = NpiEntityRelationQuery(
        context_filters=(ReadFilter("service", "eq", "99213"),),
        require_match=False,
    )

    await service.prepare_npi_entity_relation(
        object(),
        authorization=ExtensionReadAuthorization("synthetic-token"),
        target=_target(),
        query=query,
    )
    statement.assert_called_once()
    with pytest.raises(CustomImportReadRequestError, match="metric predicates require imported membership"):
        await service.prepare_npi_entity_relation(
            object(),
            authorization=ExtensionReadAuthorization("synthetic-token"),
            target=_target(),
            query=NpiEntityRelationQuery(
                context_filters=query.context_filters,
                filters=(ReadFilter("metric", "gt", "5"),),
                require_match=False,
            ),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filters", "require_match", "rejected"),
    [
        ((), False, False),
        ((ReadFilter("service", "eq", "99213"),), False, False),
        ((ReadFilter("service_code", "eq", "99213"),), False, False),
        ((ReadFilter("metric", "eq", "5"),), False, True),
        ((ReadFilter("service", "eq", "99213"), ReadFilter("metric", "eq", "5")), False, True),
        ((ReadFilter("amount", "gt", "5"),), False, True),
        ((ReadFilter("service", "is_null"),), False, True),
        ((ReadFilter("metric", "eq", "5"),), True, False),
    ],
)
async def test_provider_query_optional_membership_only_accepts_declared_context(
    monkeypatch, filters, require_match, rejected
):
    @asynccontextmanager
    async def bounded(session, *, timeout_ms):
        yield

    statement = Mock()
    monkeypatch.setattr(read_core, "_bounded_read_window", bounded)
    monkeypatch.setattr(read_core, "_load_read_context", AsyncMock(return_value=_context(aliases=True)))
    monkeypatch.setattr(read_core, "_npi_entity_relation_statement", statement)
    service = CustomImportReadService(authorizer=_Allow())
    request_arguments_by_name = dict(
        authorization=ExtensionReadAuthorization("synthetic-token"),
        target=_target(),
        query=NpiEntityRelationQuery(filters=filters, require_match=require_match),
    )
    if rejected:
        with pytest.raises(CustomImportReadRequestError, match="metric predicates require imported membership"):
            await service.prepare_npi_entity_relation(object(), **request_arguments_by_name)
        statement.assert_not_called()
    else:
        await service.prepare_npi_entity_relation(object(), **request_arguments_by_name)
        statement.assert_called_once()


def test_provider_query_order_requires_each_exact_context_dimension():
    context = _context(aliases=True)
    order_terms = read_core._normalize_query_order_terms(
        (ReadOrderTerm("metric", "asc", "last"),),
        context,
        explicit=True,
    )
    with pytest.raises(CustomImportReadRequestError, match="^context_required$"):
        read_core._require_order_context_filters(order_terms, (), context)
    with pytest.raises(CustomImportReadRequestError, match="^context_required$"):
        read_core._require_order_context_filters(
            order_terms,
            read_core._normalized_filters((ReadFilter("service", "is_null"),), context),
            context,
        )
    with pytest.raises(CustomImportReadRequestError, match="^context_required$"):
        read_core._require_order_context_filters(
            order_terms,
            read_core._normalized_filters(
                (ReadFilter("service", "eq", "99213"), ReadFilter("service_code", "eq", "99214")),
                context,
            ),
            context,
        )

    no_dimension_context = _context(aliases=True, context_dimensions=())
    no_dimension_order = read_core._normalize_query_order_terms(
        (ReadOrderTerm("metric", "asc", "last"),),
        no_dimension_context,
        explicit=True,
    )
    read_core._require_order_context_filters(no_dimension_order, (), no_dimension_context)


def test_provider_query_filter_only_can_require_one_exact_child_context():
    context = _context(aliases=True)
    with pytest.raises(CustomImportReadRequestError, match="^context_required$"):
        read_core._require_order_context_filters((), (), context, require_exact_context=True)
    filters = read_core._normalized_filters((ReadFilter("service", "eq", "99213"),), context)
    read_core._require_order_context_filters((), filters, context, require_exact_context=True)


def test_provider_query_relation_projects_only_requested_typed_sort_columns_without_limit():
    context = _context(aliases=True)
    filters = read_core._normalized_filters((ReadFilter("service", "eq", "99213"),), context)
    order_terms = read_core._normalize_query_order_terms(
        (ReadOrderTerm("metric", "desc", "last"),),
        context,
        explicit=True,
    )
    statement = read_core._npi_entity_relation_statement(context, filters, order_terms)
    compiled = str(statement.compile(dialect=postgresql.dialect()))

    assert tuple(statement.selected_columns.keys()) == ("entity_value", "sort_0")
    assert statement.selected_columns.sort_0.type.python_type.__name__ == "Decimal"
    assert "custom_import_entity_binding.adapter_id" in compiled
    assert "LIMIT" not in compiled
    assert "ORDER BY" not in compiled


@pytest.mark.asyncio
async def test_provider_query_none_order_avoids_defaults_and_caches(monkeypatch):
    context = _context()

    @asynccontextmanager
    async def unbounded_window(session, *, timeout_ms):
        del session, timeout_ms
        yield

    load_context = AsyncMock(return_value=context)
    monkeypatch.setattr(read_core, "_bounded_read_window", unbounded_window)
    monkeypatch.setattr(read_core, "_load_read_context", load_context)
    cache = _CacheSpy()
    service = CustomImportReadService(authorizer=_Allow(), cache=cache, cursor_secret=b"s" * 32)
    session = object()

    prepared = await service.prepare_npi_entity_relation(
        session,
        authorization=ExtensionReadAuthorization("synthetic-token"),
        target=_target(),
    )

    assert prepared.normalized_order_terms == ()
    assert tuple(prepared.statement.selected_columns.keys()) == ("entity_value",)
    assert prepared.authorization_scope_sha256 == read_core._scope_digest(
        ExtensionReadScope("synthetic:provider-query")
    )
    assert (cache.get_calls, cache.set_calls) == (0, 0)
    load_context.assert_awaited_once_with(session, _target())

    with pytest.raises(CustomImportReadRequestError, match="^context_required$"):
        await service.prepare_npi_entity_relation(
            session,
            authorization=ExtensionReadAuthorization("synthetic-token"),
            target=_target(),
            query=NpiEntityRelationQuery(order_terms=(ReadOrderTerm("amount", "asc", "last"),)),
        )


@pytest.mark.asyncio
async def test_provider_v2_root_rollup_sort_keeps_the_child_context_guard(monkeypatch):
    """A child-profile root sort cannot fan out across unselected child winners."""

    @asynccontextmanager
    async def unbounded_window(session, *, timeout_ms):
        del session, timeout_ms
        yield

    context = _context(sortable_fields=("npi",))
    monkeypatch.setattr(read_core, "_bounded_read_window", unbounded_window)
    monkeypatch.setattr(read_core, "_load_read_context", AsyncMock(return_value=context))
    service = CustomImportReadService(authorizer=_Allow())

    with pytest.raises(CustomImportReadRequestError, match="^context_required$"):
        await service.prepare_npi_entity_relation(
            object(),
            authorization=ExtensionReadAuthorization("synthetic-token"),
            target=_target(),
            query=NpiEntityRelationQuery(
                context_filters=(),
                order_terms=(ReadOrderTerm("npi", "asc", "last"),),
            ),
        )
