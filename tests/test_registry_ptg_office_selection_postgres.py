"""Real D04 batch/approval and paired-office authority on the existing native fixture."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text

from process import registry_ptg_office_selection as selection
from process import registry_ptg_source_consumer as consumer
from process.network_approved_membership_source import pin_approved_membership_source
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_office_selection import require_selected_ptg_office_recipes
from tests.registry_ptg_office_selection_postgres_support import draft_office_recipe_selection
from tests.test_registry_approval_store_postgres import _approve, _command
from tests.test_registry_ptg_office_coordinator_postgres import (
    coordinator_db,
    custom_db,
    office_db,
    office_publisher,
    serving_schema,
)
from tests.test_registry_publication_ptg_runtime_postgres import _review_offices


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_selector", [{}, {"office_recipe_json": None}])
async def test_legacy_deltas_do_not_consume_the_office_selection_limit(serving_schema, monkeypatch, legacy_selector):
    from process import registry_ptg_office_selection_replay as replay
    from tests.test_registry_ptg_office_selection import _pair

    connection, _, _ = serving_schema
    binding, _, recipe = _pair()
    references = AsyncMock(side_effect=[(recipe,), ()])
    monkeypatch.setattr(selection, "_read_references", references)
    monkeypatch.setattr(replay, "refuse_replaced_capture_union", AsyncMock())
    async with connection.transaction():
        await connection.execute("""CREATE TEMP TABLE registry_approved_record (
          approved_revision bigint,record_kind text,record_key text,record_json jsonb) ON COMMIT DROP""")
        await connection.execute(
            """INSERT INTO registry_approved_record
          SELECT revision,'network_binding','legacy-'||unit,
            jsonb_build_object('source_system','ptg','network_id',revision,'archived',false)||$1::jsonb
          FROM generate_series(1,101) unit CROSS JOIN generate_series(1,2) revision""",
            json.dumps(legacy_selector),
        )
        assert await selection.resolve_ptg_office_recipe_selection(
            connection, (recipe,), 2, SimpleNamespace(approved_custom_revision=1), control_schema="pg_temp"
        ) == (recipe,)
        references.assert_not_awaited()
        await connection.executemany(
            "INSERT INTO registry_approved_record VALUES($1,'network_binding','office',$2::jsonb)",
            [(revision, json.dumps({**binding, "archived": revision == 2})) for revision in (1, 2)],
        )
        assert (
            await selection.resolve_ptg_office_recipe_selection(
                connection,
                (recipe,),
                2,
                SimpleNamespace(approved_custom_revision=1),
                control_schema="pg_temp",
                office_authority=(object(), object(), object()),
            )
            == ()
        )
        assert references.await_args_list[0].args[2] == [{**binding, "archived": False}]
        await connection.execute(
            """INSERT INTO registry_approved_record
          SELECT 2,'network_binding','new-office-'||unit,$1::jsonb FROM generate_series(1,101) unit""",
            json.dumps({**binding, "archived": False}),
        )
        with pytest.raises(ValueError, match="too_many_changes"):
            await selection.resolve_ptg_office_recipe_selection(
                connection, (), 2, SimpleNamespace(approved_custom_revision=1), control_schema="pg_temp"
            )


async def _selected(native, recipes):
    async with native.service.management_sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        revision = await driver.fetchval(
            f'SELECT approved_revision FROM "{native.custom.control_schema}".registry_revision_control'
        )
        approved = await pin_approved_membership_source(
            driver, approved_revision=revision, control_schema=native.custom.control_schema
        )
        await require_selected_ptg_office_recipes(
            driver,
            recipes,
            approved,
            control_schema=native.custom.control_schema,
            office_authority=(
                session,
                native.service.preparation.scope_service.store,
                RegistryPTGGraphReadBudget(16 * 1024 * 1024),
            ),
        )


@pytest.mark.asyncio
async def test_reviewed_pair_requires_current_selection_and_draft_is_not_approval(coordinator_db):
    native = coordinator_db
    _, recipes, _ = await _review_offices(native, select=False)
    with pytest.raises(ValueError, match="unselected"):
        await _selected(native, recipes)
    written, actor = await draft_office_recipe_selection(native, recipes[0])
    with pytest.raises(ValueError, match="unselected"):
        await _selected(native, recipes)
    await _approve(
        native.custom.connection,
        native.custom.control_schema,
        await _command(native.custom.connection, native.custom.control_schema, *written["records"]),
        actor,
    )
    await _selected(native, recipes)
    # Immutable source receipts are untouched by choosing one reviewed recipe.
    assert (
        await native.custom.connection.fetchval(
            f'SELECT count(*) FROM "{native.custom.control_schema}".registry_ptg_office_hold WHERE capture_id=$1',
            __import__("uuid").UUID(recipes[0].source_pin.capture_id),
        )
        == 1
    )

    before = await native.custom.connection.fetchval(
        f'SELECT draft_revision FROM "{native.custom.control_schema}".registry_revision_control'
    )
    with pytest.raises(ValueError, match="changed"):
        await draft_office_recipe_selection(native, recipes[0], recipe_digest="f" * 64)
    assert (
        await native.custom.connection.fetchval(
            f'SELECT draft_revision FROM "{native.custom.control_schema}".registry_revision_control'
        )
        == before
    )
    clearing, actor = await draft_office_recipe_selection(native, recipes[0], clear=True)
    await _selected(native, recipes)
    await _approve(
        native.custom.connection,
        native.custom.control_schema,
        await _command(native.custom.connection, native.custom.control_schema, *clearing["records"]),
        actor,
    )
    with pytest.raises(ValueError, match="unselected"):
        await _selected(native, recipes)


async def _binding_state(native):
    schema = native.custom.control_schema
    connection = native.custom.connection
    head = await connection.fetchval(f'SELECT to_jsonb(binding)::text FROM "{schema}".registry_network_binding binding')
    history = await connection.fetchval(
        f'SELECT jsonb_agg(to_jsonb(history) ORDER BY record_kind,record_key,revision)::text FROM "{schema}".registry_record_history history'
    )
    draft = await connection.fetchval(f'SELECT draft_revision FROM "{schema}".registry_revision_control')
    return head, history, draft


async def _approve_written(native, written, actor):
    await _approve(
        native.custom.connection,
        native.custom.control_schema,
        await _command(native.custom.connection, native.custom.control_schema, *written["records"]),
        actor,
    )


async def _refuse_inherited_selector_tamper(native, recipe, selected):
    import json

    original = await _binding_state(native)
    await native.custom.connection.execute(
        f'UPDATE "{native.custom.control_schema}".registry_network_binding SET office_recipe_json=$1::jsonb',
        json.dumps({**selected, "recipe_sha256": "f" * 64}),
    )
    try:
        tampered = await _binding_state(native)
        with pytest.raises(ValueError, match="selection_changed"):
            await draft_office_recipe_selection(native, recipe, omit=True)
        assert await _binding_state(native) == tampered
    finally:
        await native.custom.connection.execute(
            f'UPDATE "{native.custom.control_schema}".registry_network_binding SET office_recipe_json=$1::jsonb',
            json.dumps(selected),
        )
    assert await _binding_state(native) == original


async def _preserve_omission_history(native, recipes, before_head):
    import json

    written, actor = await draft_office_recipe_selection(native, recipes[0], omit=True)
    after_head = json.loads((await _binding_state(native))[0])
    assert after_head["office_recipe_json"] == before_head["office_recipe_json"]
    assert after_head["revision"] == before_head["revision"] + 1
    latest = await native.custom.connection.fetchval(
        f'SELECT record_json::text FROM "{native.custom.control_schema}".registry_record_history '
        "WHERE record_kind='network_binding' ORDER BY revision DESC LIMIT 1"
    )
    assert json.loads(latest)["office_recipe_json"] == before_head["office_recipe_json"]
    await _approve_written(native, written, actor)
    await _selected(native, recipes)


async def _refuse_mixed_scope_batches(native, recipe):
    from tests.test_network_source_binding_store_postgres import _row, _seed

    other_network = (await _seed(native.custom.connection, native.custom.control_schema, 1))[0]
    before = await _binding_state(native)
    with pytest.raises(ValueError, match="changed"):
        await draft_office_recipe_selection(native, recipe, omit=True, network_id=other_network)
    assert await _binding_state(native) == before
    with pytest.raises(ValueError, match="changed"):
        await draft_office_recipe_selection(
            native,
            recipe,
            omit=True,
            network_id=other_network,
            extra_rows=(_row(other_network, source_key="separate-legacy-binding"),),
        )
    assert await _binding_state(native) == before
    with pytest.raises(ValueError, match="changed"):
        await draft_office_recipe_selection(native, recipe, recipe_digest="f" * 64)
    assert await _binding_state(native) == before
    with pytest.raises(ValueError, match="input_invalid"):
        await draft_office_recipe_selection(
            native,
            recipe,
            recipe_digest="F" * 64,
            extra_rows=(_row(other_network, source_key="malformed-mixed-binding"),),
        )
    assert await _binding_state(native) == before


async def _close_reopen_clear_selection(native, recipes, selected):
    import json

    recipe = recipes[0]
    closing, actor = await draft_office_recipe_selection(native, recipe, omit=True, operation="close")
    assert json.loads((await _binding_state(native))[0])["office_recipe_json"] == selected
    await _approve_written(native, closing, actor)
    with pytest.raises(ValueError, match="unselected"):
        await _selected(native, recipes)
    reopened, actor = await draft_office_recipe_selection(native, recipe, omit=True)
    await _approve_written(native, reopened, actor)
    await _selected(native, recipes)
    clearing, actor = await draft_office_recipe_selection(native, recipe, clear=True)
    head = json.loads((await _binding_state(native))[0])
    assert head["office_recipe_json"] is None
    assert await native.custom.connection.fetchval(
        f'SELECT office_recipe_json IS NULL FROM "{native.custom.control_schema}".registry_network_binding'
    )
    await _selected(native, recipes)
    await _approve_written(native, clearing, actor)
    with pytest.raises(ValueError, match="unselected"):
        await _selected(native, recipes)


@pytest.mark.asyncio
async def test_omission_preserves_selected_authority_and_effective_scope_is_atomic(coordinator_db):
    import json

    native = coordinator_db
    _, recipes, _ = await _review_offices(native)
    before_head = json.loads((await _binding_state(native))[0])
    selected = before_head["office_recipe_json"]
    await _refuse_inherited_selector_tamper(native, recipes[0], selected)
    await _preserve_omission_history(native, recipes, before_head)
    await _refuse_mixed_scope_batches(native, recipes[0])
    await _close_reopen_clear_selection(native, recipes, selected)


@pytest.mark.asyncio
async def test_committed_office_event_discovery_keeps_selection_separate_and_reader_original(coordinator_db):
    from process.registry_ptg_source_notifications import (
        qualify_registry_ptg_source_notifications,
        read_registry_ptg_source_notification_headers,
    )

    native = coordinator_db
    source_service, recipes, _ = await _review_offices(native, select=False)
    store = source_service.store
    reader_role = source_service.reader_sessions.kw["bind"].url.username
    async with source_service.reader_sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        before_by_name = dict(
            await driver.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
        )
        identity = await qualify_registry_ptg_source_notifications(driver, store, reader_role=reader_role)
        operation_result = await read_registry_ptg_source_notification_headers(
            driver, store, reader_role=reader_role, identity=identity, limit=1
        )
        pin = recipes[0].source_pin
        assert operation_result["headers"] == [
            {
                "capture_id": pin.capture_id,
                "approval_sha256": pin.approval_sha256,
                "hold_sha256": pin.hold_sha256,
            }
        ]
        assert operation_result["complete"] is True
        assert (
            dict(await driver.fetchrow("SELECT session_user::text AS session,current_user::text AS current"))
            == before_by_name
        )
        assert before_by_name == {"session": reader_role, "current": reader_role}
        assert await driver.fetchval("SHOW transaction_read_only") == "on"
        with pytest.raises(ValueError):
            await qualify_registry_ptg_source_notifications(driver, store, reader_role=store.approval_role)
        assert await qualify_registry_ptg_source_notifications(driver, store, reader_role=reader_role) == identity
    # A committed accepted event is discoverable before explicit binding selection.
    with pytest.raises(ValueError, match="unselected"):
        await _selected(native, recipes)
    async with source_service.reader_sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        with pytest.raises(ValueError):
            await qualify_registry_ptg_source_notifications(driver, store, reader_role=reader_role)


async def _refuse_and_rollback_source_event(native, recipe, arguments_by_name, monkeypatch):
    """Prove unselected refusal and rollback after the real native publication call."""
    from process import registry_ptg_source_consumer as consumer
    from tests.registry_ptg_office_selection_postgres_support import approve_office_recipe_selection
    from tests.test_registry_publication_ptg_runtime_postgres import _retained_pin

    before = await _retained_pin(native)
    assert await consumer.consume_registry_ptg_source_notification_once(**arguments_by_name) == {
        "state": "review_required"
    }
    assert await _retained_pin(native) == before
    approved_revision = await approve_office_recipe_selection(native, recipe)
    controls = await native.custom.connection.fetchrow(
        f'SELECT draft_revision,approved_revision FROM "{native.custom.control_schema}".registry_revision_control WHERE id=1'
    )
    original_publish = consumer.publish_network_candidate

    async def fail_after_real_publish(*args, **kwargs):
        await original_publish(*args, **kwargs)
        raise RuntimeError("synthetic final transaction fault")

    with monkeypatch.context() as fault:
        fault.setattr(consumer, "publish_network_candidate", fail_after_real_publish)
        with pytest.raises(RuntimeError, match="synthetic final transaction fault"):
            await consumer.consume_registry_ptg_source_notification_once(**arguments_by_name)
    assert await _retained_pin(native) == before
    assert (
        await native.custom.connection.fetchval(
            f'SELECT count(*) FROM "{native.custom.control_schema}".network_membership_candidate '
            "WHERE validation_json ? 'ptg_source_notification'"
        )
        == 0
    )
    return before, approved_revision, controls


async def _complete_and_replay_source_event(native, arguments_by_name, connection, before, approved_revision, controls):
    """Prove exact completion, checkpoint identity, approval preservation and lost-ack replay."""
    import json
    from uuid import UUID

    from process import registry_ptg_source_consumer as consumer
    from tests.test_registry_publication_ptg_runtime_postgres import _retained_pin

    assert await consumer.consume_registry_ptg_source_notification_once(**arguments_by_name) == {"state": "completed"}
    published = await _retained_pin(native)
    assert published.generation_id != before.generation_id and published.approved_custom_revision == approved_revision
    assert await connection.fetchval(f'SELECT count(*) FROM "{published.schema_name}".network_membership_source') == 1
    assert await consumer.consume_registry_ptg_source_notification_once(**arguments_by_name) == {"state": "replayed"}
    assert await _retained_pin(native) == published
    checkpoint = await native.custom.connection.fetchval(
        f"SELECT validation_json->'ptg_source_notification' FROM \"{native.custom.control_schema}\".network_membership_candidate WHERE candidate_id=$1",
        UUID(published.candidate_id),
    )
    checkpoint = json.loads(checkpoint) if type(checkpoint) is str else checkpoint
    assert checkpoint["header"] == arguments_by_name["event"]
    assert (
        await native.custom.connection.fetchrow(
            f'SELECT draft_revision,approved_revision FROM "{native.custom.control_schema}".registry_revision_control WHERE id=1'
        )
        == controls
    )


def _source_event_header(recipe):
    """Use only the accepted capture and its exact approval and hold identities."""
    return {name: getattr(recipe.source_pin, name) for name in ("capture_id", "approval_sha256", "hold_sha256")}


def _event_worker_roles(tmp_path):
    """Register the exact task-owned roles before any native resource creation."""
    from tests.test_registry_ptg_office_coordinator_postgres import _registered

    execution_role, owner_role = "ptg_event_execution_" + uuid4().hex, "ptg_event_owner_" + uuid4().hex
    password = uuid4().hex
    registered_roles = [owner_role, execution_role]
    _registered(
        tmp_path / "ptg-event-worker-cleanup.json", {"roles": registered_roles, "schemas": [], "phase": "registered"}
    )
    return execution_role, owner_role, password, registered_roles


@pytest.mark.asyncio
async def test_accepted_ptg_event_refuses_unselected_then_rolls_back_retries_and_replays(
    coordinator_db, tmp_path, monkeypatch
):
    """Keep native unselected refusal, post-publish rollback, exact retry and replay intact."""
    from process.registry_ptg_source_notifications import qualify_registry_ptg_source_notifications
    from tests.test_registry_publication_ptg_runtime_postgres import _cleanup_worker, _worker

    native = coordinator_db
    service, recipes, _ = await _review_offices(native, select=False)
    event_by_name = _source_event_header(recipes[0])
    execution_role, owner_role, password, registered_roles = _event_worker_roles(tmp_path)
    resources = SimpleNamespace(created=[], engine=None, connection=None, pool=None)
    try:
        for role in registered_roles:
            assert not await native.custom.connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role
            )
            resources.created.append(role)
            await native.custom.connection.execute(
                'CREATE ROLE "'
                + role
                + '" '
                + (f"LOGIN PASSWORD '{password}'" if role == execution_role else "NOLOGIN")
            )
        resources.engine, runtime, resources.connection = await _worker(native, execution_role, owner_role, password)
        reader_role = service.reader_sessions.kw["bind"].url.username
        resources.pool = await asyncpg.create_pool(
            service.reader_sessions.kw["bind"].url.set(drivername="postgresql").render_as_string(hide_password=False),
            min_size=1,
            max_size=1,
        )
        async with resources.pool.acquire() as reader, reader.transaction(isolation="repeatable_read", readonly=True):
            identity = await qualify_registry_ptg_source_notifications(reader, service.store, reader_role=reader_role)
            assert await reader.fetchrow("SELECT current_user::text,session_user::text") == (reader_role, reader_role)
        config = consumer.RegistryPTGSourceConsumerConfig(
            native.custom.control_schema,
            execution_role,
            owner_role,
            reader_roles=(native.custom.roles["reader"], native.roles_by_kind["publisher"]),
            composition_runtime=runtime,
        )
        arguments_by_name = dict(
            connection=resources.connection,
            notification_pool=resources.pool,
            store=service.store,
            reader_role=reader_role,
            identity=identity,
            event=event_by_name,
            config=config,
        )
        before, approved_revision, controls = await _refuse_and_rollback_source_event(
            native, recipes[0], arguments_by_name, monkeypatch
        )
        await _complete_and_replay_source_event(
            native, arguments_by_name, resources.connection, before, approved_revision, controls
        )
    finally:
        if resources.pool is not None:
            await resources.pool.close()
        await _cleanup_worker(native, resources.connection, resources.engine, resources.created)
