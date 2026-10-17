# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Both real writers, retained corrections and approval enforce tenant access after replay."""

import json
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from process.company_network_link_store import apply_company_network_link_batch
from process.registry_approval_store import approve_registry_records
from process.registry_benefit_plan_metadata import RegistryBenefitPlanAccessDenied
from process.registry_manual_undo import RegistryManualUndoCommand, prepare_registry_manual_undo
from process.registry_record_store import RegistryAddressUnavailable
from tests.test_company_network_link_store_postgres import _assertion, _batch, _seed, _state
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _command
from tests.test_registry_benefit_plan_writers_postgres import _PLAN, _selected_record, _service, _write
from tests.test_registry_company_links_postgres import company_links_app as company_links_app

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("mode", ["denied", "wrong_client"])
async def test_native_batch_denial_is_atomic_and_exact_replay_precedes_access(serving_schema, mode):
    connection, schema, _ = serving_schema
    companies, _, networks = await _seed(connection, schema)
    command = _batch(companies, networks)
    command = replace(
        command,
        targets=tuple(
            replace(
                selected_target,
                network_assertions=[_assertion(selected_target.company_id, networks[0], benefit_plan_ids=[_PLAN])],
            )
            for selected_target in command.targets
        ),
    )
    actor = _actor()
    before = await _state(connection, schema)
    service_state_dict = {"status": "active", "access": "denied"}
    if mode == "wrong_client":
        service_state_dict["access"] = "allowed"
        service_state_dict["response_client_id"] = "foreign-client"
    async with _service(service_state_dict) as client:
        async with connection.transaction():
            with pytest.raises(RegistryBenefitPlanAccessDenied if mode == "denied" else RegistryAddressUnavailable):
                await apply_company_network_link_batch(
                    connection, command, actor, control_schema=schema, plan_metadata_client=client
                )
        assert await _state(connection, schema) == before
        service_state_dict.pop("response_client_id", None)
        service_state_dict["access"] = "allowed"
        async with connection.transaction():
            receipt = await apply_company_network_link_batch(
                connection, command, actor, control_schema=schema, plan_metadata_client=client
            )
        request_count = len(service_state_dict["requests"]) + len(service_state_dict["access_requests"])
        service_state_dict["status"] = service_state_dict["access"] = "unavailable"
        async with connection.transaction():
            assert await apply_company_network_link_batch(connection, command, actor, control_schema=schema) == receipt
        assert request_count == len(service_state_dict["requests"]) + len(service_state_dict["access_requests"])


async def test_retained_correction_undo_and_approval_revalidate_current_access(serving_schema):
    connection, schema, engine = serving_schema
    actor = _actor()
    company, networks, command = await _selected_record(serving_schema, actor)
    service_state_dict = {"status": "active"}
    async with _service(service_state_dict) as client:
        original = await _write(serving_schema, command, actor, plan_metadata_client=client)
        service_state_dict["access"] = "denied"
        correction = replace(
            command,
            operation="correct",
            expected_revision=1,
            idempotency_key=uuid4().hex,
            fields={"network_ids": command.fields["network_ids"], "group_id": None},
        )
        with pytest.raises(RegistryBenefitPlanAccessDenied):
            await _write(serving_schema, correction, actor, plan_metadata_client=client)
        approval = await _command(connection, schema, company, *networks, original)
        async with connection.transaction():
            with pytest.raises(RegistryBenefitPlanAccessDenied):
                await approve_registry_records(
                    connection, approval, actor, control_schema=schema, plan_metadata_client=client
                )
        assert await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control') == 0
        cleared = replace(correction, fields=correction.fields | {"network_assertions": []})
        await _write(serving_schema, cleared, actor)
        undo = RegistryManualUndoCommand(
            "company_links", command.record_id, 2, 1, "Restore reviewed benefit", uuid4().hex
        )
        async with async_sessionmaker(engine)() as session, session.begin():
            prepared = await prepare_registry_manual_undo(session, undo, actor, schema=schema)
        with pytest.raises(RegistryBenefitPlanAccessDenied):
            await _write(
                serving_schema,
                prepared.command,
                actor,
                plan_metadata_client=client,
                undo_provenance=prepared.provenance,
            )
        service_state_dict["status"] = service_state_dict["access"] = "unavailable"
        assert await _write(serving_schema, command, actor) == original
        assert all(
            json.loads(request.content)["client_id"] == actor.client_id
            for request in service_state_dict["access_requests"]
        )


async def test_administrator_retains_existing_scope_without_tenant_entitlement(serving_schema):
    actor = replace(_actor(), kind="platform_admin", client_id="system")
    _, _, command = await _selected_record(serving_schema, actor)
    state_by_name = {"status": "active", "access": "denied"}
    async with _service(state_by_name) as client:
        assert (await _write(serving_schema, command, actor, plan_metadata_client=client))["revision"] == 1
    assert len(state_by_name["requests"]) == 1 and not state_by_name.get("access_requests")


async def test_actual_edit_denial_is_403_and_exact_replay_remains_http_free(company_links_app):
    from tests.test_registry_benefit_plan_callers_postgres import _body
    from tests.test_registry_company_links_postgres import _AUTH

    app, fixture = company_links_app
    actor = _actor()
    _, _, command = await _selected_record(fixture, actor)
    state_by_name = {"status": "active", "access": "denied"}
    async with _service(state_by_name) as client:
        app.ctx.registry_plan_metadata_client = client
        path = "/api/v1/registry/manage/company_links"
        _, denied = await app.asgi_client.post(path, json=_body(command, actor), headers=_AUTH)
        assert denied.status == 403 and denied.json == {"error": {"code": "registry_access_denied"}}
        state_by_name["access"] = "allowed"
        _, saved = await app.asgi_client.post(path, json=_body(command, actor), headers=_AUTH)
        assert saved.status == 200
        app.ctx.registry_plan_metadata_client = None
        _, replay = await app.asgi_client.post(path, json=_body(command, actor), headers=_AUTH)
        assert replay.status == 200 and replay.json == saved.json
