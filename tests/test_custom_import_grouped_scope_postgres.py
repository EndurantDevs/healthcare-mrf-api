# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native membership keeps independent family selection and complete hydration."""

from dataclasses import replace

import pytest
from sqlalchemy import String, column, select, text, values

from process.custom_import.read_core import ReadFilter
from tests import custom_import_grouped_support as fixture
from tests import test_custom_import_provider_hydration_postgres as native


@pytest.mark.asyncio
@pytest.mark.parametrize("period", (None, 2023))
async def test_correlated_helper_preserves_native_scope_and_independent_period(period):
    selectors = (ReadFilter("segment", "eq", "segment_a"),)
    if period is not None:
        selectors += (ReadFilter("period", "eq", period),)
    query = fixture.query(context_filters=selectors, filters=(ReadFilter("score", "gte", 20),))
    async with native._case() as (case, pinned_target), case.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        service = native._service(pinned_target)
        _, unscoped_rows = await native._relation(session, pinned_target, query)
        expected_entities = {native._B} if period is None else {native._A}
        assert {provider_row.entity_value for provider_row in unscoped_rows} == expected_entities
        complete_families = await native._page(session, pinned_target, query)
        for entity_scope in ((native._A,), (native._B,), (native._A, native._B, native._B), (native._C,), ()):
            native_rows = (
                values(column("npi", String))
                .data([(entity_value,) for entity_value in entity_scope] or [(None,)])
                .cte("native_scope")
            )
            prepared = await service.prepare_npi_entity_relation(
                session, authorization=native._AUTHORIZATION, target=pinned_target, query=query
            )
            scoped = await service.scope_npi_entity_relation(
                session,
                authorization=native._AUTHORIZATION,
                pinned_target=pinned_target,
                prepared=prepared,
                query=query,
                native_entity_scope=select(native_rows.c.npi).where(native_rows.c.npi.is_not(None)),
            )
            scoped_rows = (await session.execute(scoped.statement)).all()
            matching_entities = expected_entities & set(entity_scope)
            assert {provider_row.entity_value for provider_row in scoped_rows} == matching_entities
            assert len(scoped_rows) == len(matching_entities)
            assert scoped.query_fingerprint == prepared.query_fingerprint
            hydrated = await service.hydrate_npi_page(
                session,
                authorization=native._AUTHORIZATION,
                pinned_target=pinned_target,
                prepared=scoped,
                query=query,
                entity_values=tuple(dict.fromkeys(entity_scope)),
            )
            assert hydrated == {entity: complete_families[entity] for entity in matching_entities}
        narrowed = replace(query, entity_values=(native._A,))
        assert {
            provider_row.entity_value for provider_row in (await native._relation(session, pinned_target, narrowed))[1]
        } == (expected_entities & {native._A})
