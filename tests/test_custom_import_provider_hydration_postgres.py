# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Same-family, same-context PostgreSQL proof for native-page hydration."""

import pytest

from process.custom_import.read_core import ExtensionReadAuthorization, NpiEntityRelationQuery, ReadFilter
from tests import test_custom_import_provider_query_postgres as npi_fixture
from tests import test_custom_import_read_core_postgres as read_fixture
from tests.custom_import_postgres_support import digest, transaction_session


@pytest.mark.asyncio
@pytest.mark.parametrize(("child_index", "state"), ((0, "missing"), (1, "null")))
async def test_native_page_hydration_uses_only_matching_context_and_family(monkeypatch, child_index, state):
    npi_fixture._seed_npi_bindings(monkeypatch)
    npi_fixture._seed_selected_contexts(monkeypatch, (0, 1))
    async with transaction_session() as session, session.begin():
        fixture = await npi_fixture._seed_npi_fixture(session)
        service = read_fixture._service()
        filters = (ReadFilter("synthetic_alpha", "eq", ("high-1", "low-1")[child_index]),)
        query_map = {
            "authorization": ExtensionReadAuthorization("synthetic"),
            "query": NpiEntityRelationQuery(filters=filters),
        }
        prepared = await service.prepare_npi_entity_relation(session, target=fixture.target, **query_map)
        result = await service.hydrate_npi_page(
            session,
            pinned_target=fixture.target,
            **query_map,
            prepared=prepared,
            entity_values=(npi_fixture._NPI, npi_fixture._ABSENT_NPI),
        )

        assert set(result) == {npi_fixture._NPI}
        selected_item = result[npi_fixture._NPI]
        assert selected_item.winner.family_revision_id == fixture.selected_family.family_revision_id
        assert selected_item.context_child_revision_id == fixture.selected_family.child_revision_ids[child_index]
        assert next(value for value in selected_item.context_fields if value.field_id == "amount").state == state
        assert selected_item.root_fields[0].value == "synthetic-root"


@pytest.mark.asyncio
async def test_filter_only_multi_context_hydration_is_deterministic_and_bounded(monkeypatch):
    npi_fixture._seed_npi_bindings(monkeypatch)
    npi_fixture._seed_selected_contexts(monkeypatch, (0, 1))
    async with transaction_session() as session, session.begin():
        fixture = await npi_fixture._seed_npi_fixture(session)
        service = read_fixture._service()
        query_map = {
            "authorization": ExtensionReadAuthorization("synthetic"),
            "query": NpiEntityRelationQuery(context_filters=()),
        }
        prepared = await service.prepare_npi_entity_relation(session, target=fixture.target, **query_map)
        entities = tuple(str(1000000000 + index) for index in range(200))
        result = await service.hydrate_npi_page(
            session, pinned_target=fixture.target, **query_map, prepared=prepared, entity_values=entities
        )
        expected_index = min((0, 1), key=lambda index: digest(f"provider-query-context:{index}"))

        assert set(result) == {npi_fixture._NPI}
        assert (
            result[npi_fixture._NPI].context_child_revision_id
            == fixture.selected_family.child_revision_ids[expected_index]
        )
        assert (
            await service.hydrate_npi_page(
                session, pinned_target=fixture.target, **query_map, prepared=prepared, entity_values=()
            )
            == {}
        )
