# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native exact office discovery; installed service and HTTP remain separate gates."""

import json
from uuid import uuid4

import pytest
from sqlalchemy import text

from process import registry_ptg_office_capture as capture
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget, RegistryPTGGraphReadError
from process.registry_ptg_office_choices import read_registry_ptg_office_choices
from process.registry_ptg_office_preparation_producer import produce_reviewed_office_rows
from tests import test_registry_ptg_office_capture_postgres as native
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_registry_ptg_office_capture_postgres import serving_schema as serving_schema
from tests.test_registry_ptg_office_preparation_postgres import _approved_site


@pytest.fixture
async def choices_db(custom_db, tmp_path, monkeypatch):
    """Provide a real retained name before the existing fixture publishes it."""
    original = native._source_addresses

    async def source_addresses(*args, **kwargs):
        provider_id, table = await original(*args, **kwargs)
        await args[0].connection.execute(f"UPDATE {table} SET entity_name='Example Provider'")
        return provider_id, table

    monkeypatch.setattr(native, "_source_addresses", source_addresses)
    fixture = native.complete_office_db.__wrapped__(custom_db, tmp_path, monkeypatch)
    try:
        yield await anext(fixture)
    finally:
        await fixture.aclose()


@pytest.mark.asyncio
async def test_native_choices_return_only_one_reviewed_office_and_prepare_same_occurrence(choices_db, custom_db):
    fixture = choices_db
    site, binding, approval = await _approved_site(fixture, custom_db)
    scope_by_field = {**fixture.scope, "approved_revision": approval["approved_revision"]}
    command_by_field = {"exact_native_source_scope": scope_by_field["scope_id"]}
    selector_by_field = {name: scope_by_field[name] for name in ("scope_id", "client_id")}
    selector_by_field |= {
        "scope_approval_sha256": scope_by_field["approval_sha256"],
        "retained_generation_id": fixture.serving.generation_id,
        "after": None,
        "limit": 20,
    }
    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
        reply_by_field = await read_registry_ptg_office_choices(
            session,
            fixture.context,
            scope_by_field,
            command_by_field,
            selector_by_field,
            RegistryPTGGraphReadBudget(64 * 1024 * 1024),
        )
        assert len(reply_by_field["items"]) == 1 and reply_by_field["next_cursor"] is None
        choice_by_field = reply_by_field["items"][0]
        assert choice_by_field["selection"]["binding_id"] == binding["record_id"]
        assert choice_by_field["selection"]["binding_revision"] == binding["revision"]
        assert choice_by_field["provider"] == {"npi": site["provider_id"], "display_name": "Example Provider"}
        assert choice_by_field["address"]["location_id"] == site["location_id"]
        assert choice_by_field["address"]["first_line"] == "123 Example Street"
        assert choice_by_field["address"]["city_name"] == "Sample City"
        assert reply_by_field["source_command_sha256"] == capture._digest(command_by_field)
        assert (
            await fixture.connection.fetchval(
                f'SELECT count(*) FROM "{fixture.serving.schema_name}".provider_location_binding'
            )
            == 2
        )
        prepare_by_field = {
            **selector_by_field,
            "capture_id": str(uuid4()),
            "reason": "Reviewed the exact displayed office",
            "idempotency_key": uuid4().hex,
            "selections": [choice_by_field["selection"]],
        }
        raw, _, count = await produce_reviewed_office_rows(
            session, fixture.context, scope_by_field, prepare_by_field, RegistryPTGGraphReadBudget(64 * 1024 * 1024)
        )
        assert count == 1 and json.loads(raw)[0]["location_id"] == site["location_id"]


@pytest.mark.asyncio
async def test_native_choices_refuse_tiny_graph_budget_without_returning_partial_labels(choices_db, custom_db):
    fixture = choices_db
    _, _, approval = await _approved_site(fixture, custom_db)
    scope_by_field = {**fixture.scope, "approved_revision": approval["approved_revision"]}
    selector_by_field = {name: scope_by_field[name] for name in ("scope_id", "client_id")}
    selector_by_field |= {
        "scope_approval_sha256": scope_by_field["approval_sha256"],
        "retained_generation_id": fixture.serving.generation_id,
        "after": None,
        "limit": 20,
    }
    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await session.execute(text("SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"))
        with pytest.raises(RegistryPTGGraphReadError, match="budget"):
            await read_registry_ptg_office_choices(
                session,
                fixture.context,
                scope_by_field,
                {"native": "scope"},
                selector_by_field,
                RegistryPTGGraphReadBudget(1),
            )
        assert (await session.execute(text("SELECT 1"))).scalar_one() == 1
