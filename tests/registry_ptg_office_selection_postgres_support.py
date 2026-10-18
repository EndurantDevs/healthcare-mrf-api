"""Use the actual restricted D04 batch writer and explicit approval for office selection."""

import json
from uuid import UUID, uuid4

from sqlalchemy import text

from process.network_source_binding_store import (
    COPY_COLUMNS,
    NetworkSourceBindingBatchCommand,
    apply_network_source_binding_batch,
)
from process.registry_record_store import RegistryActor
from process.registry_source_recipe_store import canonical_registry_source_recipes, registry_source_recipes_sha256
from tests.test_registry_approval_store_postgres import _approve, _command


async def draft_office_recipe_selection(
    native, recipe, *, clear=False, recipe_digest=None, omit=False, operation="rebind", network_id=None, extra_rows=()
):
    service = native.service.preparation.scope_service
    actor = RegistryActor("platform_admin", UUID(native.request["actor"]["user_id"]), "system")
    async with native.service.management_sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        current = json.loads(
            await driver.fetchval(
                f'SELECT record_json::text FROM "{native.custom.control_schema}".registry_approved_record '
                "WHERE approved_revision=(SELECT approved_revision FROM "
                f"\"{native.custom.control_schema}\".registry_revision_control WHERE id=1) AND record_kind='network_binding'"
            )
        )
        binding_by_field = {name: current[name] for name in COPY_COLUMNS if name in current and name != "binding_key"}
        binding_by_field.update(
            operation=operation,
            expected_revision=current["revision"],
            expected_network_id=current["network_id"],
            office_recipe_json={
                "capture_id": recipe.source_pin.capture_id,
                "recipe_sha256": recipe_digest
                or registry_source_recipes_sha256(canonical_registry_source_recipes((recipe,))),
            },
        )
        if network_id is not None:
            binding_by_field["network_id"] = network_id
        if omit:
            binding_by_field.pop("office_recipe_json", None)
        if clear:
            binding_by_field["office_recipe_json"] = None
        written = await apply_network_source_binding_batch(
            driver,
            NetworkSourceBindingBatchCommand(
                json.dumps([*extra_rows, binding_by_field]).encode(), "Review this exact office recipe", uuid4().hex
            ),
            actor,
            control_schema=native.custom.control_schema,
            published_plan_store=service.store,
        )
    return written, actor


async def approve_office_recipe_selection(native, recipe):
    written, actor = await draft_office_recipe_selection(native, recipe)
    approved = await _approve(
        native.custom.connection,
        native.custom.control_schema,
        await _command(native.custom.connection, native.custom.control_schema, *written["records"]),
        actor,
    )
    return approved["approved_revision"]
