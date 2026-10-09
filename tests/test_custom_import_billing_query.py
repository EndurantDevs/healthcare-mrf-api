# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configured billing membership preserves exact values and complete identity scope."""

from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import DateTime, Numeric, String, literal, select, text, union_all
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import BindParameter

from api import billing_search_pagination as pagination
from api import custom_import_billing_query as composition
from api.billing_search_cursor import BillingSearchCursorError
from api.ptg2_billing_search_contract import BillingSearchServingUnavailableError
from process.custom_import.read_contracts import PinnedReadTarget
from process.custom_import.read_core import PreparedNpiEntityRelation, ReadOrderTerm
from tests.billing_search_page_support import NPI_VALUES, candidate
from tests.custom_import_postgres_support import _database_url
from tests.test_billing_search_pagination import (
    KEYRING,
    REQUEST_TIME,
    _authorization_context,
    _pin,
    _request,
    _wire_token,
)

_ACCESS_STATE = "d" * 64
_TARGET = PinnedReadTarget(1, 2, 3, 4, "synthetic_profile")


def _prepared(*, direction="desc", nulls="last"):
    rows = (
        (str(NPI_VALUES[0]), Decimal("4.000000000002")),
        (str(NPI_VALUES[1]), None),
        (str(NPI_VALUES[2]), Decimal("4.000000000001")),
        ("0" + str(NPI_VALUES[0]), Decimal("999")),
        ("malformed", Decimal("999")),
    )
    source = union_all(
        *(
            select(
                literal(entity, type_=String).label("entity_value"),
                literal(score, type_=Numeric(30, 12)).label("sort_0"),
            )
            for entity, score in rows
        )
    ).subquery("synthetic_scores")
    terms = () if direction is None else (ReadOrderTerm("metric", direction, nulls),)
    columns = [source.c.entity_value] + ([source.c.sort_0] if terms else [])
    return PreparedNpiEntityRelation(select(*columns), terms, "a" * 64, "b" * 64)


def _context(*, require_match=True, direction="desc", nulls="last"):
    return composition._new_billing_import_query(
        _prepared(direction=direction, nulls=nulls), require_match, _TARGET, _ACCESS_STATE
    )


def test_statement_is_unpaged_and_uses_exact_typed_native_scope():
    context = _context()
    candidates = tuple(candidate(npi=npi) for npi in NPI_VALUES)
    statement = composition._candidate_statement(candidates, context)
    sql = str(statement)
    assert "unnest" in sql and "WITH ORDINALITY" in sql and "LIMIT" not in sql
    assert "CAST(billing_candidates.npi AS VARCHAR)" in sql
    assert "DESC NULLS LAST" in sql
    assert "billing_candidate_entities" in statement.compile().params


@pytest.mark.parametrize("mutation", ("where", "bind"))
def test_original_select_mutation_cannot_change_frozen_execution(mutation):
    context = _context()
    candidates = (candidate(),)
    before = composition._candidate_statement(candidates, context).compile()
    if mutation == "where":
        object.__setattr__(context.prepared.statement, "_where_criteria", (literal(False),))
    else:
        binding = next(
            element for element in visitors.iterate(context.prepared.statement) if isinstance(element, BindParameter)
        )
        binding.value = "changed"
    composition.validate_billing_import_query(context, endpoint_access_state_sha256=_ACCESS_STATE)
    after = composition._candidate_statement(candidates, context).compile()
    assert str(after) == str(before) and after.params == before.params


def test_prepared_relation_compiles_once_and_validation_reuses_frozen_values(monkeypatch):
    calls = []
    compiler = composition.compile_npi_entity_relation

    def counted(statement):
        calls.append(statement)
        return compiler(statement)

    monkeypatch.setattr(composition, "compile_npi_entity_relation", counted)
    context = _context()
    for _ordinal in range(3):
        composition.validate_billing_import_query(context, endpoint_access_state_sha256=_ACCESS_STATE)
        composition._candidate_statement((candidate(),), context)
    assert calls == [context.prepared.statement]


@pytest.mark.parametrize("first_requires_match", (False, True))
def test_cursor_cannot_change_effective_required_or_optional_membership(first_requires_match):
    first_scope = _context(require_match=first_requires_match).import_scope
    binding = pagination.build_billing_search_cursor_binding(
        _request(),
        _authorization_context(),
        _pin(),
        trusted_now=REQUEST_TIME,
        import_scope=first_scope,
    )
    marker = candidate().sort_key
    sealed = pagination.seal_billing_search_page_cursor(marker, keyring=KEYRING, binding=binding)
    resumed = _request(cursor=_wire_token(sealed, binding))
    unchanged_binding = pagination.build_billing_search_cursor_binding(
        resumed,
        _authorization_context(),
        _pin(),
        trusted_now=REQUEST_TIME,
        import_scope=_context(require_match=first_requires_match).import_scope,
    )
    assert pagination.open_billing_search_page_cursor(resumed, keyring=KEYRING, binding=unchanged_binding) == marker
    changed_binding = pagination.build_billing_search_cursor_binding(
        resumed,
        _authorization_context(),
        _pin(),
        trusted_now=REQUEST_TIME,
        import_scope=_context(require_match=not first_requires_match).import_scope,
    )
    with pytest.raises(BillingSearchCursorError):
        pagination.open_billing_search_page_cursor(resumed, keyring=KEYRING, binding=changed_binding)


@pytest.mark.parametrize("mutation", ("sql", "bind", "order", "signature", "access"))
def test_frozen_execution_and_request_proof_tampering_is_rejected(mutation):
    context = _context()
    if mutation == "sql":
        object.__setattr__(context.compiled, "sql", "SELECT 'changed'")
    elif mutation == "bind":
        context.compiled.typed_binds[0].value = "changed"
    elif mutation == "order":
        object.__setattr__(context.prepared.normalized_order_terms[0], "direction", "asc")
    elif mutation == "signature":
        context = replace(context, _seal=b"invalid")
    expected_access = "e" * 64 if mutation == "access" else _ACCESS_STATE
    with pytest.raises(BillingSearchServingUnavailableError):
        composition.validate_billing_import_query(context, endpoint_access_state_sha256=expected_access)


@pytest.mark.parametrize("binding_flag", ("callable", "expanding", "literal_execute"))
def test_mutated_binding_execution_flags_fail_closed(binding_flag):
    context = _context()
    replacement = (lambda: "changed") if binding_flag == "callable" else True
    setattr(context.compiled.typed_binds[0], binding_flag, replacement)
    with pytest.raises(BillingSearchServingUnavailableError):
        composition.validate_billing_import_query(context, endpoint_access_state_sha256=_ACCESS_STATE)


def test_mutated_output_decimal_processing_type_fails_closed():
    context = _context()
    dict(context.output_types)["sort_0"].asdecimal = False
    with pytest.raises(BillingSearchServingUnavailableError):
        composition.validate_billing_import_query(context, endpoint_access_state_sha256=_ACCESS_STATE)


def test_mutated_datetime_bind_changes_driver_cast_but_cannot_reuse_seal():
    statement = select(
        literal(str(NPI_VALUES[0]), type_=String).label("entity_value"),
        literal(datetime(2031, 1, 2, tzinfo=timezone.utc), type_=DateTime(timezone=True)).label("sort_0"),
    )
    prepared = PreparedNpiEntityRelation(statement, (ReadOrderTerm("metric", "desc", "last"),), "a" * 64, "b" * 64)
    context = composition._new_billing_import_query(prepared, True, _TARGET, _ACCESS_STATE)
    candidates = (candidate(),)
    initial_sql = str(composition._candidate_statement(candidates, context).compile(dialect=PGDialect_asyncpg()))
    timestamp_binding = next(binding for binding in context.compiled.typed_binds if isinstance(binding.type, DateTime))
    timestamp_binding.type.timezone = False
    mutated_sql = str(composition._candidate_statement(candidates, context).compile(dialect=PGDialect_asyncpg()))
    assert "TIMESTAMP WITH TIME ZONE" in initial_sql
    assert "TIMESTAMP WITHOUT TIME ZONE" in mutated_sql
    with pytest.raises(BillingSearchServingUnavailableError):
        composition.validate_billing_import_query(context, endpoint_access_state_sha256=_ACCESS_STATE)


@asynccontextmanager
async def _readonly_sql_session():
    """Run literal-only SQL in the existing isolated test database, then rollback."""

    engine = create_async_engine(
        _database_url(),
        connect_args={
            "server_settings": {
                "application_name": "synthetic_billing_query_tests",
                "statement_timeout": "10000",
            }
        },
    )
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            session = AsyncSession(bind=connection, join_transaction_mode="create_savepoint")
            try:
                await connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
                yield session
            finally:
                await session.close()
                await transaction.rollback()
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "direction,nulls,require_match,expected_npis",
    (
        ("desc", "last", True, (NPI_VALUES[0], NPI_VALUES[2], NPI_VALUES[1])),
        ("asc", "last", True, (NPI_VALUES[2], NPI_VALUES[0], NPI_VALUES[1])),
        ("desc", "first", True, (NPI_VALUES[1], NPI_VALUES[0], NPI_VALUES[2])),
        (None, "last", True, NPI_VALUES[:3]),
        ("desc", "last", False, (NPI_VALUES[0], NPI_VALUES[2], NPI_VALUES[1], *NPI_VALUES[3:])),
    ),
)
@pytest.mark.asyncio
async def test_postgres_order_preserves_decimal_nulls_missing_and_exact_identity(
    direction, nulls, require_match, expected_npis
):
    context = _context(direction=direction, nulls=nulls, require_match=require_match)
    candidates = tuple(candidate(npi=npi) for npi in NPI_VALUES)
    async with _readonly_sql_session() as session:
        ordered, proof = await composition.compose_billing_candidates(
            session, candidates, context, endpoint_access_state_sha256=_ACCESS_STATE
        )
    assert tuple(provider.address.npi for provider in ordered) == expected_npis
    assert proof.candidate_keys == tuple(provider.sort_key for provider in ordered)


@pytest.mark.asyncio
async def test_postgres_full_scope_exceeds_generic_page_cap_and_retains_repeated_npis():
    context = _context()
    candidates = tuple(candidate(binding_ordinal=ordinal) for ordinal in range(257))
    async with _readonly_sql_session() as session:
        ordered, proof = await composition.compose_billing_candidates(
            session, candidates, context, endpoint_access_state_sha256=_ACCESS_STATE
        )
    assert ordered == candidates and len(proof.candidate_keys) == 257


@pytest.mark.asyncio
async def test_postgres_original_mutable_bind_does_not_change_frozen_membership():
    context = _context()
    candidates = tuple(candidate(npi=npi) for npi in NPI_VALUES)
    for element in visitors.iterate(context.prepared.statement):
        if isinstance(element, BindParameter) and element.value == str(NPI_VALUES[0]):
            element.value = "different"
    async with _readonly_sql_session() as session:
        ordered, _proof = await composition.compose_billing_candidates(
            session, candidates, context, endpoint_access_state_sha256=_ACCESS_STATE
        )
    assert tuple(provider.address.npi for provider in ordered) == (NPI_VALUES[0], NPI_VALUES[2], NPI_VALUES[1])
