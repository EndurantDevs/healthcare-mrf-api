# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pinned hot-table routing, canonical control reads, and fail-closed resolution."""

import re
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import func, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import visitors
from sqlalchemy.sql.selectable import Exists

from db.models import custom_import as models
from process.custom_import import grouped_read, query_projection_evidence, read_core, read_identity
from process.custom_import.family_block_layout import ROLE_SPECS
from process.custom_import.storage_layout import SNAPSHOT_MODELS, snapshot_models, snapshot_schema, snapshot_tables
from tests import custom_import_grouped_child_support as grouped_child
from tests import test_custom_import_grouped_read as grouped_rows
from tests import test_custom_import_provider_query as provider

_SCOPE = read_core.ExtensionReadScope("synthetic:snapshot-read")


def _storage(family_id):
    return None if family_id is None else snapshot_models(family_id)


def _assert_routing(statement, family_id):
    sql = str(statement.compile(dialect=postgresql.dialect()))
    schema = "mrf" if family_id is None else snapshot_schema(family_id)
    for model in SNAPSHOT_MODELS:
        if family_id is not None:
            assert f"mrf.{model.__tablename__}." not in sql
            assert f"mrf.{model.__tablename__} " not in sql
    for name in ("custom_import_generation", "custom_import_generation_seal"):
        assert f"ci_snapshot_17.{name}." not in sql
        assert f"ci_snapshot_18.{name}." not in sql
    assert f"{schema}.custom_import_" in sql
    return sql


def _assert_finality_gates(statement, target):
    gates = [node.element.element for node in visitors.iterate(statement) if isinstance(node, Exists)]
    for model in (models.CustomImportGeneration, models.CustomImportGenerationSeal):
        matching_gates = [gate for gate in gates if gate.get_final_froms() == [model.__table__]]
        assert len(matching_gates) == 1
        gate = matching_gates[0]
        assert not gate._auto_correlate and not gate._correlate
        expected_predicates = tuple(
            getattr(model, name) == getattr(target, name)
            for name in ("dataset_id", "generation_id", "definition_revision_id", "schema_revision_id")
        )
        if model is models.CustomImportGenerationSeal:
            expected_predicates += (model.seal_contract == "custom-import-generation-seal/v1",)
        assert len(gate._where_criteria) == len(expected_predicates)
        assert all(actual.compare(required) for actual, required in zip(gate._where_criteria, expected_predicates))


@pytest.mark.parametrize("family_id", [None, 17, 18])
@pytest.mark.parametrize("child_context", [False, True])
def test_winner_search_relation_and_order_share_one_storage_binding(family_id, child_context):
    context = replace(provider._context(), storage_models=_storage(family_id), profile_context_slot=int(child_context))
    filters = [read_core.ReadFilter("display_name", "eq", "Synthetic")]
    if child_context:
        filters.append(read_core.ReadFilter("amount", "gt", "1"))
    normalized = read_core._normalized_filters(tuple(filters), context)
    terms = (read_core.ReadOrderTerm("amount" if child_context else "display_name", "asc", "last"),)
    statement = read_core._filtered_winner_statement(context, normalized).order_by(
        *read_core._winner_order_terms(context, terms)
    )
    sql = _assert_routing(statement, family_id)
    schema = "mrf" if family_id is None else snapshot_schema(family_id)
    _assert_finality_gates(statement, context.target)
    assert "JOIN mrf.custom_import_generation ON" not in sql
    assert "JOIN mrf.custom_import_generation_seal ON" not in sql
    assert (
        f"{schema}.custom_import_root_scalar.root_record_id = {schema}.custom_import_family_revision.root_record_id"
        in sql
    )
    assert f"{schema}.custom_import_winner.context_child_revision_id IS NULL" in sql or child_context
    assert (
        f"{schema}.custom_import_family_revision.entity_binding_id = {schema}.custom_import_winner.entity_binding_id"
        in sql
    )
    _assert_routing(select(func.count()).select_from(statement.subquery()), family_id)
    relation = read_core._npi_entity_relation_statement(context, normalized, terms)
    sql = _assert_routing(relation, family_id)
    assert f"{schema}.custom_import_entity_binding.canonical_value AS entity_value" in sql
    assert f"{schema}.custom_import_entity_binding.dataset_id = {schema}.custom_import_winner.dataset_id" in sql
    if child_context:
        assert (
            f"{schema}.custom_import_child_scalar.child_revision_id = {schema}.custom_import_winner.context_child_revision_id"
            in sql
        )
        assert (
            f"{schema}.custom_import_family_child.child_revision_id = {schema}.custom_import_winner.context_child_revision_id"
            in sql
        )


@pytest.mark.parametrize("family_id", [None, 17, 18])
@pytest.mark.parametrize("period", [None, 2024])
def test_grouped_helper_child_predicates_and_order_keep_the_pinned_family(family_id, period):
    context = replace(grouped_child.context(), storage_models=_storage(family_id))
    selectors = (
        read_core.ReadFilter("segment", "eq", "segment_a"),
        read_core.ReadFilter("service_code", "eq", "chosen"),
    )
    if period is not None:
        selectors += (read_core.ReadFilter("period", "eq", period),)
    query = grouped_child.query(
        context_filters=selectors,
        filters=(read_core.ReadFilter("amount", "gt", "1"), read_core.ReadFilter("quality", "lt", "3")),
        order_terms=(read_core.ReadOrderTerm("amount", "desc", "last"),),
    )
    prepared = grouped_read.prepare_relation(context, query, _SCOPE)
    sql = _assert_routing(prepared.statement, family_id)
    schema = "mrf" if family_id is None else snapshot_schema(family_id)
    assert "selected_entity_value AS MATERIALIZED" not in sql
    assert (
        f"WHERE selected_entity_value.entity_binding_id = {schema}.custom_import_winner.entity_binding_id" in sql
    ) is (period is None)
    plan = grouped_read.normalize_plan(context, query, _SCOPE)
    detail_sql = _assert_routing(grouped_read.selected_family_statement(context, plan), family_id)
    assert "MATERIALIZED" not in detail_sql
    assert ("AS selected_entity_value" in detail_sql) is (period is None)
    _assert_joined_child_ownership(sql, schema)
    _assert_shared_child_projection(prepared.statement, context, schema)
    assert "LIMIT" not in sql


def _assert_joined_child_ownership(sql, schema):
    """Keep selected child keys, revisions, and complete identities on one family/root."""
    member = f"{schema}.custom_import_family_child"
    revision = f"{schema}.custom_import_child_revision"
    assert (
        f"{schema}.custom_import_family_revision.family_revision_id = {schema}.custom_import_winner.family_revision_id"
        in sql
    )
    assert (
        f"{schema}.custom_import_family_revision.root_record_id = {schema}.custom_import_generation_family.root_record_id"
        in sql
    )
    for identity in ("child_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "collection_slot"):
        assert f"complete_score_child_keys.{identity} = {member}.{identity}" in sql
        assert f"{revision}.{identity} = {member}.{identity}" in sql
    for identity in ("family_revision_id", "root_record_id"):
        assert f"matched_root_score_values.{identity} = {member}.{identity}" in sql
        assert f"selected_score_children.{identity} = counted_score_children.{identity}" in sql
    assert "complete_score_child_keys AS MATERIALIZED" in sql
    assert "matched_root_score_values AS MATERIALIZED" in sql
    assert "validated_score_children AS MATERIALIZED" in sql
    assert (
        "count(*) OVER (PARTITION BY selected_score_children.family_revision_id, selected_score_children.root_record_id)"
        in sql
    )
    assert "validated_score_children.validated_child_id IS NOT NULL" in sql
    assert "complete_score_child_identities" not in sql and "selected_child_counts" not in sql
    assert "JOIN selected_score_children" not in sql
    assert f"FROM {member} JOIN complete_score_child_keys" in sql
    assert f"FROM {schema}.custom_import_child_scalar \nWHERE" not in sql
    assert f"FROM {schema}.custom_import_child_scalar, {schema}.custom_import_family_revision" not in sql


def _assert_shared_child_projection(statement, context, schema):
    """Use one owned typed child projection for each metric and its ordering value."""
    compiled = statement.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    member = f"{schema}.custom_import_family_child"
    for field_id in ("amount", "quality"):
        field = context.definition.fields_by_id[field_id]
        scalar = f"score_child_{field.field_slot}"
        assert sql.count(f"LEFT OUTER JOIN {schema}.custom_import_child_scalar AS {scalar} ON") == 1
        assert f"{scalar}.child_revision_id = {schema}.custom_import_child_revision.child_revision_id" in sql
        for identity in ("dataset_id", "schema_revision_id", "root_record_id", "collection_slot"):
            assert f"{scalar}.{identity} = {member}.{identity}" in sql
        parameter = re.search(rf"{scalar}\.field_slot = %\((\w+)\)s", sql)
        assert parameter and compiled.params[parameter.group(1)] == field.field_slot
    ordered_value = statement.selected_columns["sort_0"].element
    assert ordered_value.table.name == "validated_score_children"
    assert f"validated_score_children.{ordered_value.name} > " in sql
    for parameter, expected in (
        ("dataset_id_", context.target.dataset_id),
        ("schema_revision_id_", context.target.schema_revision_id),
    ):
        values = [value for name, value in compiled.params.items() if name.startswith(parameter)]
        assert values and all(value == expected for value in values)


@pytest.mark.asyncio
@pytest.mark.parametrize("family_id", [None, 17, 18])
async def test_bounded_family_hydration_routes_all_three_queries_and_preserves_rows(family_id):
    context = replace(grouped_child.context(), storage_models=_storage(family_id))
    rows, session = grouped_rows._complete_family_case((2, 1))
    details = await grouped_read._hydrate_complete_families(session, context, rows, _SCOPE)
    assert [detail.winner.family_revision_id for detail in details] == [2, 1]
    assert [[child.child_revision_id for child in detail.children] for detail in details] == [[1011], [1001, 1002]]
    assert [[child.fields[1].state for child in detail.children] for detail in details] == [
        ["null"],
        ["null", "missing"],
    ]
    assert session.execute.await_count == 3
    for call in session.execute.await_args_list:
        _assert_routing(call.args[0], family_id)


@pytest.mark.asyncio
async def test_single_family_provider_page_distinct_and_ties_use_snapshot_aliases(monkeypatch):
    context = replace(provider._context(), storage_models=_storage(17))
    query = read_core.NpiEntityRelationQuery()
    prepared = read_core.PreparedNpiEntityRelation(
        read_core._npi_entity_relation_statement(context, (), ()),
        (),
        read_core._npi_entity_relation_fingerprint(context.target, (), ()),
        read_core._scope_digest(_SCOPE),
    )
    selected = (object(), object(), object(), object())
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: [(*selected, "1234567893")])))
    projected = object()
    monkeypatch.setattr(read_core, "_hydrate_provider_page_items", AsyncMock(return_value=(projected,)))
    monkeypatch.setattr(read_core, "verify_published_generation", AsyncMock())
    service = read_core.CustomImportReadService(authorizer=provider._Allow())
    result = await service._hydrate_single_family_page(session, context, query, prepared, ("1234567893",), _SCOPE)
    assert result == {"1234567893": projected}
    sql = _assert_routing(session.execute.await_args.args[0], 17)
    assert "DISTINCT ON (ci_snapshot_17.custom_import_entity_binding.canonical_value)" in sql
    assert "ORDER BY ci_snapshot_17.custom_import_entity_binding.canonical_value" in sql
    assert "ci_snapshot_17.custom_import_winner.context_key_sha256 ASC" in sql
    read_core._hydrate_provider_page_items.assert_awaited_once_with(
        session, context, (selected,), read_core._scope_digest(_SCOPE)
    )


def _resolver_session(value, schema="mrf"):
    options_by_name = {"schema_translate_map": {"mrf": schema}}
    transactions = [SimpleNamespace(is_active=True), None, SimpleNamespace(is_active=True), None]
    connection = SimpleNamespace(
        sync_connection=SimpleNamespace(get_execution_options=lambda: options_by_name),
        dialect=postgresql.dialect(),
        get_transaction=lambda: transactions[2],
        get_nested_transaction=lambda: transactions[3],
    )
    return SimpleNamespace(
        connection=AsyncMock(return_value=connection),
        execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: value, one=lambda: value)),
        get_transaction=lambda: transactions[0],
        get_nested_transaction=lambda: transactions[1],
        transactions=transactions,
    )


def _block_role_oids():
    return [[slot, 1000 + slot] for slot, _ in ROLE_SPECS.values()]


@pytest.mark.asyncio
@pytest.mark.parametrize("family_id", [None, 17, 2**63 - 1])
async def test_resolver_uses_exact_typed_target_and_trusted_control_schema(family_id):
    session = _resolver_session(family_id, 'control"namespace')
    target = provider._target()
    assert await read_identity.resolve_generation_snapshot(session, target) == family_id
    statement, parameters = session.execute.await_args.args
    assert str(statement).startswith('SELECT "control""namespace".resolve_custom_import_generation_snapshot(')
    assert str(statement).count(" AS bigint)") == 4
    assert "pg_catalog" not in str(statement) and "MATERIALIZED" not in str(statement)
    assert parameters == {
        "generation_id": target.generation_id,
        "dataset_id": target.dataset_id,
        "definition_revision_id": target.definition_revision_id,
        "schema_revision_id": target.schema_revision_id,
    }
    assert session.connection.await_count == 1 and session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("family_id,legacy_snapshot", [(None, False), (17, False), (17, True), (2**63 - 1, True)])
async def test_context_binding_observes_complete_legacy_layout_after_one_native_pin(family_id, legacy_snapshot):
    role_oids = [] if family_id is None or legacy_snapshot else _block_role_oids()
    session = _resolver_session((family_id, legacy_snapshot, role_oids), 'control"namespace')
    rejection_target = provider._target()

    assert await read_identity.resolve_generation_snapshot_binding(session, rejection_target) == (
        read_identity.GenerationSnapshotBinding(family_id, legacy_snapshot, tuple(map(tuple, role_oids)))
    )

    statement, parameters = session.execute.await_args.args
    sql = str(statement)
    assert sql.lstrip().startswith("WITH pinned AS MATERIALIZED (SELECT")
    assert sql.count('"control""namespace".resolve_custom_import_generation_snapshot(') == 1
    assert sql.count(" AS bigint)") == 4
    assert "FROM pinned\n" in sql and "CROSS JOIN LATERAL" in sql
    assert "n.nspname = 'ci_snapshot_' || pinned.family_id::text" in sql
    assert "c.relname = ANY(CAST(:legacy_names AS text[]))" in sql
    assert "c.relkind = 'r' AND c.relpersistence = 'p'" in sql
    assert "census.legacy_count = cardinality(CAST(:legacy_names AS text[]))" in sql
    assert "block_count" not in sql
    assert "expected.role_slot, c.oid::bigint" in sql and "ORDER BY expected.role_slot" in sql
    assert "c.relname = expected.table_name" in sql
    assert "pinned.family_id IS NOT NULL" in sql
    assert "source_bulk_landing" not in sql and "legacy_copy_origin" not in sql
    names = parameters.pop("legacy_names")
    assert names == [model.__tablename__ for model in SNAPSHOT_MODELS]
    assert len(names) == len(set(names)) == 15
    assert parameters.pop("block_slots") == [slot for slot, _ in ROLE_SPECS.values()]
    assert parameters.pop("block_names") == [name for _, name in ROLE_SPECS.values()]
    assert parameters == {
        "generation_id": rejection_target.generation_id,
        "dataset_id": rejection_target.dataset_id,
        "definition_revision_id": rejection_target.definition_revision_id,
        "schema_revision_id": rejection_target.schema_revision_id,
    }
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("family_id", [False, 0, -1, 2**63, "17", 17.0])
async def test_context_binding_rejects_invalid_identity_even_when_layout_is_unresolved(family_id):
    session = _resolver_session((family_id, False, []))
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="snapshot identity"):
        await read_identity.resolve_generation_snapshot_binding(session, provider._target())
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("family_id,legacy_snapshot", [(17, None), (17, 0), (17, 1), (17, "false"), (None, True)])
async def test_context_binding_rejects_malformed_layout_evidence(family_id, legacy_snapshot):
    session = _resolver_session((family_id, legacy_snapshot, []))
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="layout evidence"):
        await read_identity.resolve_generation_snapshot_binding(session, provider._target())
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "duplicate_slot", "duplicate_oid", "slot", "oid", "boolean", "shape"])
async def test_context_binding_rejects_incomplete_or_malformed_observed_block_oids(change):
    rows = _block_role_oids()
    match change:
        case "missing":
            rows.pop()
        case "duplicate_slot":
            rows[-1][0] = rows[0][0]
        case "duplicate_oid":
            rows[-1][1] = rows[0][1]
        case "slot":
            rows[-1][0] = 14
        case "oid":
            rows[-1][1] = 0
        case "boolean":
            rows[0][0] = True
        case _:
            rows[-1] = [108]
    session = _resolver_session((17, False, rows))
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="role evidence"):
        await read_identity.resolve_generation_snapshot_binding(session, provider._target())
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("family_id,legacy_snapshot", [(None, False), (17, True)])
async def test_nonblock_pin_cannot_carry_a_block_oid_receipt(family_id, legacy_snapshot):
    session = _resolver_session((family_id, legacy_snapshot, _block_role_oids()))
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="role evidence"):
        await read_identity.resolve_generation_snapshot_binding(session, provider._target())


@pytest.mark.asyncio
async def test_fresh_canonical_null_pin_starts_and_binds_the_current_transaction():
    session = _resolver_session((None, False, []))
    connection = session.connection.return_value
    session.transactions[:] = [None, None, None, None]

    async def acquire():
        if session.transactions[0] is None:
            session.transactions[0] = SimpleNamespace(is_active=True)
            session.transactions[2] = SimpleNamespace(is_active=True)
        return connection

    session.connection.side_effect = acquire
    pin = await read_identity.resolve_generation_snapshot_binding(session, provider._target())
    assert pin == read_identity.GenerationSnapshotBinding(None, False)
    assert pin._session is session
    assert all(left is right for left, right in zip(pin._transactions, session.transactions, strict=True))
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("transaction_slot", range(4))
async def test_pin_cannot_cross_a_transaction_or_savepoint_change_during_native_resolution(transaction_slot):
    session = _resolver_session((17, False, _block_role_oids()))
    result = session.execute.return_value

    async def changed_transaction(*_):
        session.transactions[transaction_slot] = SimpleNamespace(is_active=True)
        return result

    session.execute.side_effect = changed_transaction
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="pin transaction"):
        await read_identity.resolve_generation_snapshot_binding(session, provider._target())


@pytest.mark.asyncio
@pytest.mark.parametrize("family_id", [False, 0, -1, 2**63, "17", 17.0])
async def test_resolver_rejects_invalid_native_id_without_a_legacy_fallback(family_id):
    session = _resolver_session(family_id)
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="snapshot identity"):
        await read_identity.resolve_generation_snapshot(session, provider._target())
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resolve", [read_identity.resolve_generation_snapshot, read_identity.resolve_generation_snapshot_binding]
)
async def test_resolver_requires_explicit_control_schema_and_propagates_sql_errors(resolve):
    session = _resolver_session(17, None)
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="explicit model schema"):
        await resolve(session, provider._target())
    session.execute.assert_not_awaited()
    for sqlstate in ("42883", "42501", "23514"):
        failure = DBAPIError("resolver", {}, SimpleNamespace(sqlstate=sqlstate))
        session = _resolver_session(17)
        session.execute.side_effect = failure
        with pytest.raises(DBAPIError) as caught:
            await resolve(session, provider._target())
        assert caught.value is failure
        assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "resolve", [read_identity.resolve_generation_snapshot, read_identity.resolve_generation_snapshot_binding]
)
async def test_retired_snapshot_remains_unavailable_without_layout_fallback(resolve):
    driver = Exception()
    driver.sqlstate = "55000"
    driver.message = "custom_import_snapshot_retired"
    failure = DBAPIError("resolver", {}, driver)
    session = _resolver_session(17)
    session.execute.side_effect = failure
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="storage is retired") as caught:
        await resolve(session, provider._target())
    assert caught.value.__cause__ is failure
    assert session.execute.await_count == 1


def _context_metadata(monkeypatch):
    context = provider._context()
    monkeypatch.setattr(read_core, "_eligible_definition_rows", AsyncMock(return_value=(None, None)))
    monkeypatch.setattr(read_core, "verified_definition", Mock(return_value=context.definition))
    monkeypatch.setattr(read_core, "_collection_slots", AsyncMock(return_value={"rates": 1}))
    monkeypatch.setattr(read_core, "_exact_profile", AsyncMock(return_value=None))
    monkeypatch.setattr(read_core, "_verified_profile", Mock(return_value=(1, 1)))
    monkeypatch.setattr(read_core, "_verified_field_rows", AsyncMock())
    monkeypatch.setattr(query_projection_evidence, "read_projection_tables", AsyncMock(return_value=None))
    return context


@pytest.mark.asyncio
async def test_context_resolves_fresh_but_existing_context_binding_stays_pinned(monkeypatch):
    context = _context_metadata(monkeypatch)
    session = _resolver_session((17, True, []))
    first = await read_core._load_read_context(session, context.target)
    session.execute.return_value = SimpleNamespace(one=lambda: (18, True, []))
    second = await read_core._load_read_context(session, context.target)
    assert first.storage_models is snapshot_models(17) and second.storage_models is snapshot_models(18)
    assert first.model(models.CustomImportWinner) is not second.model(models.CustomImportWinner)
    assert session.execute.await_count == 2
    _assert_routing(read_core._winner_statement(first), 17)
    _assert_routing(read_core._winner_statement(second), 18)
    assert models.CustomImportWinner.__table__.schema == "mrf"


def test_context_keeps_exact_alias_objects_across_metadata_cache_eviction():
    context = replace(grouped_child.context(), storage_models=snapshot_models(17))
    winner = context.model(models.CustomImportWinner)
    statement = read_core._winner_statement(context)
    before = str(statement.order_by(*read_core._winner_order_terms(context, ())).compile(dialect=postgresql.dialect()))
    snapshot_models.cache_clear()
    snapshot_tables.cache_clear()
    assert snapshot_models(17)[models.CustomImportWinner] is not winner
    assert context.model(models.CustomImportWinner) is winner
    helper = replace(context, target=replace(context.target, profile_id="latest_period"), profile_slot=2)
    assert helper.storage_models is context.storage_models
    after = str(statement.order_by(*read_core._winner_order_terms(context, ())).compile(dialect=postgresql.dialect()))
    assert before == after
    _assert_routing(grouped_read.prepare_relation(context, grouped_child.query(), _SCOPE).statement, 17)


@pytest.mark.asyncio
@pytest.mark.parametrize("resolver_failure", [False, True])
async def test_authorization_and_snapshot_validation_precede_cached_search(monkeypatch, resolver_failure):
    context = _context_metadata(monkeypatch)
    events = []

    class Authorizer:
        def authorize(self, authorization, *, target):
            events.append("authorize")
            return _SCOPE

    @asynccontextmanager
    async def window(session, *, timeout_ms):
        yield

    monkeypatch.setattr(read_core, "_bounded_read_window", window)
    monkeypatch.setattr(read_core, "verify_published_generation", AsyncMock())
    request = read_core.SearchRequest(context.target)
    plan = read_core._normalize_search_plan(request, context)
    cached = read_core.SearchPage(context.target, 0, (), None, 1100, plan.fingerprint, read_core._scope_digest(_SCOPE))

    async def cache_get(key):
        query_projection_evidence.read_projection_tables.assert_awaited_once_with(
            session, context.target, context.definition, snapshot_family_id=17, family_block_binding=None
        )
        events.append("cache")
        return cached

    cache = SimpleNamespace(get=AsyncMock(side_effect=cache_get), set=AsyncMock())
    service = read_core.CustomImportReadService(
        authorizer=Authorizer(), cache=cache, cursor_secret=b"s" * 32, now=lambda: 1000
    )
    session = _resolver_session(17)

    async def resolve(statement, parameters):
        events.append("resolve")
        if resolver_failure:
            raise DBAPIError("resolver", {}, SimpleNamespace(sqlstate="42883"))
        return SimpleNamespace(one=lambda: (17, True, []))

    session.execute.side_effect = resolve
    if resolver_failure:
        with pytest.raises(DBAPIError):
            await service.search(
                session, authorization=read_core.ExtensionReadAuthorization("synthetic"), request=request
            )
        assert events == ["authorize", "resolve"]
        cache.get.assert_not_awaited()
    else:
        assert (
            await service.search(
                session, authorization=read_core.ExtensionReadAuthorization("synthetic"), request=request
            )
            is cached
        )
        assert events == ["authorize", "resolve", "cache"]
        read_core.verify_published_generation.assert_awaited_once_with(session, context.target)
    cache.set.assert_not_awaited()
