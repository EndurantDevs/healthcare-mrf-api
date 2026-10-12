# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Billing composition executes configured reducers and hydrates whole families."""

from decimal import Decimal

import pytest
from sqlalchemy import text

from api import custom_import_billing_query as composition
from api.custom_import_provider_http import _ProviderScopePreparer
from api.custom_import_provider_sql import CompiledNpiEntityRelation
from process.custom_import.read_core import ReadFilter, ReadOrderTerm
from tests import custom_import_grouped_child_support as grouped
from tests import test_custom_import_grouped_child_read_postgres as children
from tests import test_custom_import_grouped_score_query_postgres as scores
from tests import test_custom_import_provider_hydration_postgres as native
from tests.billing_search_page_support import candidate
from tests.custom_import_postgres_support import _quoted_publication_schema


def _bind_isolated_schema(monkeypatch, session):
    """Compile the same query against this fixture's exact physical namespace."""

    namespace = session.get_bind().get_execution_options()["schema_translate_map"]["mrf"]
    quoted = _quoted_publication_schema(namespace)
    compiler = composition.compile_npi_entity_relation

    def scoped(statement):
        compiled = compiler(statement)
        return CompiledNpiEntityRelation(
            compiled.sql.replace("mrf.", f"{quoted}."), compiled.values, compiled.typed_binds
        )

    monkeypatch.setattr(composition, "compile_npi_entity_relation", scoped)


async def _scoped_billing_context(session, reader, pinned_target, query, prepared, candidates):
    """Use the serving callback with genuine retained read authority and all native NPIs."""

    context = composition._new_billing_import_query(prepared, True, pinned_target, "d" * 64)
    scope_preparer = _ProviderScopePreparer(session, reader, native._AUTHORIZATION, pinned_target, query, prepared)

    async def prepare_native_scope(native_scope):
        assert native_scope.compile().params["billing_candidate_npis"] == tuple(
            provider.address.npi for provider in candidates
        )
        scoped = await scope_preparer.prepare(native_scope)
        return composition._new_billing_import_query(scoped, True, pinned_target, "d" * 64)

    scoped_context = await composition.scope_billing_import_query(
        candidates, context, native_scope_preparer=prepare_native_scope, endpoint_access_state_sha256="d" * 64
    )
    assert scoped_context.import_scope == context.import_scope
    assert scoped_context.prepared is scope_preparer.prepared and scoped_context.prepared is not prepared
    return scoped_context


def _native_billing_candidates(include_second_match):
    """Include a score-absent provider and optionally the other imported match."""

    npis = (native._A, native._C, "1000000038") + ((native._B,) if include_second_match else ())
    return tuple(sorted((candidate(npi=int(npi)) for npi in npis), key=lambda provider: provider.sort_key))


@pytest.mark.asyncio
@pytest.mark.parametrize("scope_contains_second_match", (False, True))
async def test_postgres_billing_executes_configured_reducers_and_preserves_full_family(
    monkeypatch, scope_contains_second_match
):
    """Restrict configured score evaluation to native eligibility and hydrate whole winners."""

    monkeypatch.setattr(native.fixture, "definition", scores._definition)
    roots = native._roots()
    monkeypatch.setattr(native, "_roots", lambda: [dict(root_row, weight=2) for root_row in roots])
    query = grouped.query(
        context_filters=(ReadFilter("service_code", "eq", "chosen"),),
        filters=(ReadFilter("root_cost", "gte", 15), ReadFilter("child_quality", "gt", 0)),
        order_terms=(ReadOrderTerm("root_cost", "asc", "last"), ReadOrderTerm("child_cost", "desc", "last")),
        family_entitlement="full_family",
    )
    async with (
        native._case(children=children._children()) as (case, pinned_target),
        case.sessions() as session,
        session.begin(),
    ):
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        reader = native._service(pinned_target)
        prepared = await reader.prepare_npi_entity_relation(
            session,
            authorization=native._AUTHORIZATION,
            target=pinned_target,
            query=query,
        )
        _bind_isolated_schema(monkeypatch, session)
        candidates = _native_billing_candidates(scope_contains_second_match)

        scoped_context = await _scoped_billing_context(session, reader, pinned_target, query, prepared, candidates)
        ordered_candidates, order = await composition.compose_billing_candidates(
            session,
            candidates,
            scoped_context,
            endpoint_access_state_sha256="d" * 64,
        )
        entity_values = tuple(str(provider.address.npi) for provider in ordered_candidates)
        assert entity_values == ((native._A, native._B) if scope_contains_second_match else (native._A,))
        assert order.candidate_keys == tuple(provider.sort_key for provider in ordered_candidates)
        hydrated_by_npi = await reader.hydrate_npi_page(
            session,
            authorization=native._AUTHORIZATION,
            pinned_target=pinned_target,
            query=query,
            prepared=scoped_context.prepared,
            entity_values=entity_values,
        )
        selected = hydrated_by_npi[native._A]
        assert selected.selection_value == 2024
        assert tuple(group for group, _family in selected.families) == ("segment_a", "segment_b")
        assert sum(len(family.children) for _group, family in selected.families) == 6
        scoped_score_rows = (await session.execute(scoped_context.prepared.statement)).all()
        assert {score_row.entity_value for score_row in scoped_score_rows} == set(entity_values)
        assert next(tuple(score_row)[1:] for score_row in scoped_score_rows if score_row.entity_value == native._A) == (
            Decimal("15"),
            Decimal("90.916666666667"),
        )
