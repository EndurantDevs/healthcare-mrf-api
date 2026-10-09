# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Billing composition executes configured reducers and hydrates whole families."""

from decimal import Decimal

import pytest

from api import custom_import_billing_query as composition
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


@pytest.mark.asyncio
async def test_postgres_billing_executes_configured_reducers_and_preserves_full_family(monkeypatch):
    monkeypatch.setattr(native.fixture, "definition", scores._definition)
    roots = native._roots()
    monkeypatch.setattr(native, "_roots", lambda: [dict(root_row, weight=2) for root_row in roots])
    query = grouped.query(
        context_filters=(ReadFilter("service_code", "eq", "chosen"),),
        filters=(ReadFilter("root_cost", "gte", 15), ReadFilter("child_quality", "gt", 0)),
        order_terms=(ReadOrderTerm("root_cost", "asc", "last"), ReadOrderTerm("child_cost", "desc", "last")),
        family_entitlement="full_family",
    )
    async with native._case(children=children._children()) as (case, pinned_target), case.sessions() as session:
        reader = native._service(pinned_target)
        prepared = await reader.prepare_npi_entity_relation(
            session,
            authorization=native._AUTHORIZATION,
            target=pinned_target,
            query=query,
        )
        _bind_isolated_schema(monkeypatch, session)
        context = composition._new_billing_import_query(prepared, True, pinned_target, "d" * 64)
        candidates = tuple(
            sorted(
                (candidate(npi=int(npi)) for npi in (native._A, native._B, native._C, "1000000038")),
                key=lambda provider: provider.sort_key,
            )
        )
        ordered_candidates, order = await composition.compose_billing_candidates(
            session,
            candidates,
            context,
            endpoint_access_state_sha256="d" * 64,
        )
        entity_values = tuple(str(provider.address.npi) for provider in ordered_candidates)
        assert entity_values == (native._A, native._B)
        assert order.candidate_keys == tuple(provider.sort_key for provider in ordered_candidates)
        hydrated_by_npi = await reader.hydrate_npi_page(
            session,
            authorization=native._AUTHORIZATION,
            pinned_target=pinned_target,
            query=query,
            prepared=prepared,
            entity_values=entity_values,
        )
        selected = hydrated_by_npi[native._A]
        assert selected.selection_value == 2024
        assert tuple(group for group, _family in selected.families) == ("segment_a", "segment_b")
        assert sum(len(family.children) for _group, family in selected.families) == 6
        assert next(
            tuple(score_row)[1:]
            for score_row in (await session.execute(prepared.statement)).all()
            if score_row.entity_value == native._A
        ) == (
            Decimal("15"),
            Decimal("90.916666666667"),
        )
