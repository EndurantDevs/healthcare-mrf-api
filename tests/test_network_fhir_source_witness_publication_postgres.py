# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact approved Source captures through real queue, independent custody and publication."""

import asyncio
import json
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process import network_fhir_source_witness_capture as capture_module
from process import registry_source_recipe_composition as recipes
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_candidate_custody import seal_network_candidate_custody
from process.network_fhir_retained_capture import require_retained_fhir_capture
from process.network_fhir_retained_capture_contract import registry_location_id
from process.network_fhir_source_witness_contract import (
    SourceWitnessCaptureAnchor,
    SourceWitnessContractError,
    canonical_timestamp,
    capture_digest,
)
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_candidate_composition import (
    RegistryAddressArtifactCustody,
    RegistryCompositionAddressSources,
    prepare_registry_composition_address_artifact,
)
from process.registry_publication_consumer import RegistryPublicationConsumerConfig, consume_registry_publication_once
from process.registry_publication_queue import (
    RegistryReactivationCommand,
    enqueue_registry_reactivation,
    enqueue_registry_source_rollback_preview,
    enqueue_registry_source_rollback_publication,
)
from process.registry_publication_worker_permissions import verify_registry_publication_worker_permissions
from process.registry_record_store import RegistryActor
from tests.source_witness_capture_postgres_support import (
    capture_arguments,
    capture_retained_source_for_roles,
    source_scope,
    source_witness_case,
)
from tests.test_network_custom_address_source_postgres import custom_db
from tests.test_network_fhir_retained_capture_postgres import _approved_capture_recipe
from tests.test_network_fhir_source_witness_capture_postgres import _capture
from tests.test_network_fhir_source_witness_membership_postgres import (
    _approve_mapping,
    _registry_read_bootstrap,
    _source_recipe,
)
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft
from tests.test_registry_generic_fhir_reuse_postgres import _native_generic_offices
from tests.test_registry_publication_consumer_postgres import (
    _consumer as _existing_consumer,
)
from tests.test_registry_publication_consumer_postgres import (
    _enqueue_ordinary,
    _head,
    _retained_recipe_custodian,
    _status,
)
from tests.test_registry_publication_worker_permissions_postgres import _login, _options, _worker_roles
from tests.test_verified_office_source_witnesses_v2_postgres import _registry_on_source

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _consumer(worker, **options_by_name):
    async with _existing_consumer(worker, **options_by_name) as (arguments_by_name, calls):
        config = arguments_by_name["config"]
        readers = tuple(sorted({*config.reader_roles, worker.roles_by_kind["api"]}))
        arguments_by_name["config"] = replace(config, reader_roles=readers)
        assert arguments_by_name["config"].custody_roles == config.custody_roles
        assert tuple(sorted(set(config.loader_roles + readers))) == worker.address_reader_roles
        yield arguments_by_name, calls


@asynccontextmanager
async def _single_worker_install(monkeypatch, worker):
    from tests import test_registry_publication_consumer_postgres as consumer_fixture

    original_install = consumer_fixture._install
    counts = SimpleNamespace(installations=0, verifications=0)

    async def install_or_verify(supplied):
        assert supplied is worker
        if not counts.installations:
            result = await original_install(worker)
            counts.installations += 1
            return result
        async with _login(worker, "execution") as connection:
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                result = await verify_registry_publication_worker_permissions(
                    connection, **_options(worker), role=worker.roles_by_kind["execution"]
                )
        counts.verifications += 1
        return result

    with monkeypatch.context() as patch:
        patch.setattr(consumer_fixture, "_install", install_or_verify)
        yield counts
    assert counts.installations == 1 and counts.verifications > 1


async def _remove_address_inputs(connection, schemas):
    for schema in reversed(schemas):
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None


def _actor_censuses(worker, operator, source_reader, custodian_role):
    config = RegistryPublicationConsumerConfig(
        **_options(worker),
        reader_roles=tuple(sorted((worker.fixture.roles["reader"], worker.roles_by_kind["api"]))),
    )
    source_roles = tuple(
        sorted(
            {
                *worker.roles_by_kind.values(),
                *config.custody_roles,
                custodian_role,
                operator.owner_role,
                source_reader,
            }
        )
    )
    return config.custody_roles, source_roles, tuple(sorted(set(config.loader_roles + config.reader_roles)))


def _publication_callbacks(context):
    worker, operator = context.worker, context.operator
    custody = RegistryAddressArtifactCustody(
        operator.owner_role,
        context.candidate_roles,
        worker.fixture.control_schema,
        address_reader_roles=context.address_reader_roles,
    )

    async def prepare_address_sources(**arguments_by_name):
        sources = replace(arguments_by_name["address_sources"], source_witness_runtime=context.runtime)
        arguments_by_name = {
            **arguments_by_name,
            "request_id": UUID(str(arguments_by_name["request_id"])),
            "address_sources": sources,
        }
        async with operator.connection.transaction(isolation="repeatable_read"):
            artifact = await prepare_registry_composition_address_artifact(
                operator.connection, custody=custody, **arguments_by_name
            )
        assert artifact is None and sources.compact
        return sources

    async def seal_candidate(target):
        state = await operator.connection.fetchval(
            f'SELECT state FROM "{worker.fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(target.candidate_id),
        )
        assert state == "ready"
        context.ready_targets.append(target)

        async def authorize(connection, selected):
            assert connection is operator.connection and selected == target
            return await connection.fetchval("SELECT current_user=session_user")

        result = await seal_network_candidate_custody(
            operator.connection,
            target,
            control_schema=worker.fixture.control_schema,
            runtime_roles=context.candidate_roles,
            authorize=authorize,
            source_witness_runtime=context.runtime,
        )
        context.sealed_targets.append(target)
        return result

    return {"seal_candidate": seal_candidate, "prepare_address_sources": prepare_address_sources}


async def _source_capture_for_workers(
    case, worker, operator, candidate_roles, source_roles, address_reader_roles, serving, stack
):
    address_schemas = []
    stack.push_async_callback(_remove_address_inputs, worker.fixture.connection, address_schemas)
    address_fixture = SimpleNamespace(role_by_kind=case.fixture.role_by_kind, captures=address_schemas)
    address = await _native_generic_offices(worker.fixture, address_fixture, serving, address_reader_roles, stack)
    arguments_by_name = {**capture_arguments(case), "runtime_roles": source_roles}
    capture = await _capture(case, capture_module, arguments_by_name)
    anchor = case.anchors_by_id[capture.capture_id]
    recipe = _source_recipe(capture, anchor)
    approved, network_id = await _approve_mapping(serving, recipe, capture, organization="network-one")
    runtime = recipes.SourceWitnessRuntime(
        case.source_export, (case.source_profile,), case.fixture.role_by_kind["owner"], source_roles
    )
    assert capture.owner_role == runtime.capture_owner_role and capture.runtime_roles == runtime.runtime_roles
    raw_writer = await case.writer.fetchval("SELECT session_user")
    assert raw_writer not in source_roles and raw_writer != await operator.connection.fetchval("SELECT session_user")
    return SimpleNamespace(
        case=case,
        worker=worker,
        operator=operator,
        candidate_roles=candidate_roles,
        address_reader_roles=address_reader_roles,
        runtime=runtime,
        capture=capture,
        recipe=recipe,
        approved=approved,
        network_id=network_id,
        address=address,
        ready_targets=[],
        sealed_targets=[],
    )


@asynccontextmanager
async def _publication_case(monkeypatch):
    async with source_witness_case(monkeypatch) as case, AsyncExitStack() as stack:
        serving = await stack.enter_async_context(_registry_on_source(case.fixture, monkeypatch))
        custom = await stack.enter_async_context(aclosing(custom_db.__wrapped__(serving)))
        fixture = await anext(custom)
        for extension in ("intarray", "btree_gin", "postgis"):
            await fixture.connection.execute('CREATE EXTENSION IF NOT EXISTS "' + extension + '"')
        worker = await stack.enter_async_context(_worker_roles(fixture, SimpleNamespace(actor=_actor())))
        installation = await stack.enter_async_context(_single_worker_install(monkeypatch, worker))
        operator = await stack.enter_async_context(_retained_recipe_custodian(worker))
        await _registry_read_bootstrap(case, serving)
        custodian_role = await operator.connection.fetchval("SELECT session_user")
        candidate_roles, source_roles, address_reader_roles = _actor_censuses(
            worker, operator, case.fixture.component.reader, custodian_role
        )
        worker.address_reader_roles = address_reader_roles
        context = await _source_capture_for_workers(
            case, worker, operator, candidate_roles, source_roles, address_reader_roles, serving, stack
        )
        context.installation = installation
        context.serving = serving
        worker.publication_custody_by_name = _publication_callbacks(context)
        yield context


def _initial_sources(context):
    return RegistryCompositionAddressSources(
        context.address,
        compact=True,
        initial_source_recipes=(context.recipe,),
        source_witness_runtime=context.runtime,
    )


async def _require_recipe(context, connection, *, recipe=None, runtime=None, use_default_runtime=True):
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        approved = await pin_approved_membership_source(
            connection,
            approved_revision=context.approved.approved_revision,
            control_schema=context.worker.fixture.control_schema,
        )
        return await recipes.require_registry_source_recipe_custody(
            connection,
            context.recipe if recipe is None else recipe,
            approved,
            context.worker.fixture.control_schema,
            retained_fhir_reads={context.capture.capture_sha256: object()},
            source_witness_runtime=context.runtime if use_default_runtime else runtime,
        )


async def _assert_runtime_and_approval_guards(context):
    capture, runtime = context.capture, context.runtime
    custodian_role = await context.operator.connection.fetchval("SELECT session_user")
    assert {context.worker.roles_by_kind["queue"], custodian_role} <= set(runtime.runtime_roles)
    async with _login(context.worker, "execution") as connection:
        with pytest.raises(
            recipes.RegistrySourceCompositionError,
            match="^registry_source_composition_source_witness_custody_unavailable$",
        ):
            await _require_recipe(context, connection, runtime=None, use_default_runtime=False)
        for invalid in (
            lambda: replace(
                runtime,
                runtime_roles=tuple(
                    role for role in runtime.runtime_roles if role != context.worker.roles_by_kind["queue"]
                ),
            ),
            lambda: replace(
                runtime, runtime_roles=tuple(role for role in runtime.runtime_roles if role != custodian_role)
            ),
            lambda: replace(runtime, capture_owner_role=context.worker.roles_by_kind["physical"]),
            lambda: replace(
                runtime, source_profiles=(replace(context.case.source_profile, schema_name="other_source"),)
            ),
            lambda: replace(
                runtime,
                source_profiles=(
                    replace(context.case.source_profile, owner_role=context.worker.fixture.roles["owner"]),
                ),
            ),
        ):
            with pytest.raises(ValueError):
                await _require_recipe(context, connection, runtime=invalid(), use_default_runtime=False)
        raw_writer = await context.case.writer.fetchval("SELECT session_user")
        assert raw_writer not in runtime.runtime_roles
        with pytest.raises(recipes.RegistrySourceCompositionError, match="runtime_census_differs"):
            await _require_recipe(
                context,
                connection,
                runtime=replace(runtime, capture_owner_role=raw_writer),
                use_default_runtime=False,
            )
        wrong_capture = replace(
            capture,
            observed_at=canonical_timestamp(datetime.fromisoformat(capture.observed_at) + timedelta(microseconds=1)),
        )
        wrong_capture = replace(wrong_capture, capture_sha256=capture_digest(wrong_capture))
        anchor = SourceWitnessCaptureAnchor(
            wrong_capture.capture_id, wrong_capture.release_id, wrong_capture.observed_at
        )
        with pytest.raises(recipes.RegistrySourceCompositionError, match="approval_unavailable"):
            await _require_recipe(context, connection, recipe=_source_recipe(wrong_capture, anchor))
        read = await _require_recipe(context, connection)
        with pytest.raises(SourceWitnessContractError):
            await read.verify(connection, capture)


async def _assert_original_source_identity_guards(context):
    origin, capture = context.case.constructed_origin, context.capture
    anchor = context.case.anchors_by_id[capture.capture_id]
    identity = capture.source_database
    cases = [
        (
            replace(
                origin,
                source_id="unselected-source",
                source_aliases=tuple(sorted({*origin.source_aliases, "unselected-source"})),
            ),
            context.case.source_profile,
            identity,
        ),
        (replace(origin, header_oid=origin.header_oid + 1), context.case.source_profile, identity),
        (replace(origin, resource_leaf_oid=origin.resource_leaf_oid + 1), context.case.source_profile, identity),
        (origin, context.case.source_profile, replace(identity, database_oid=identity.database_oid + 1)),
        (
            origin,
            context.case.source_profile,
            replace(identity, system_identifier=str(int(identity.system_identifier) + 1)),
        ),
        (
            replace(origin, schema_name="unselected_source"),
            replace(context.case.source_profile, schema_name="unselected_source"),
            identity,
        ),
    ]
    async with _login(context.worker, "execution") as connection:
        for reviewed_origin, profile, expected_identity in cases:
            replays_before = len(context.replay_actors)
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                with pytest.raises(ValueError) as refused:
                    await capture_module._verify_source_rows(
                        context.runtime.source_export,
                        connection,
                        source_profile=profile,
                        reviewed_origin=reviewed_origin,
                        expected_anchor=anchor,
                        expected_identity=expected_identity,
                    )
                assert "_verify_source_rows" in _traceback_functions(refused.value)
            assert len(context.replay_actors) == replays_before
    assert await _head(context.worker) is None


def _traceback_functions(error):
    names = []
    trace = error.__traceback__
    while trace is not None:
        names.append(trace.tb_frame.f_code.co_name)
        trace = trace.tb_next
    return names


async def _change_capture_row(context, *, derived, restore=None):
    writer, capture = context.case.writer, context.capture
    async with writer.transaction(isolation="repeatable_read"):
        await writer.execute(f'SET LOCAL ROLE "{capture.owner_role}"')
        if derived:
            relation = f'"{capture.schema_name}".provider_directory_entity_source_binding'
            original = await writer.fetchval(
                f"SELECT site_id FROM {relation} WHERE resource_type='Location' AND resource_id='office-one'"
            )
            await writer.execute(
                f"UPDATE {relation} SET site_id=$1 WHERE resource_type='Location' AND resource_id='office-one'",
                uuid4() if restore is None else restore,
            )
        else:
            relation = f'"{capture.schema_name}".provider_directory_dataset_resource'
            original = await writer.fetchval(
                f"SELECT payload_json::text FROM {relation} WHERE resource_type='Location' AND resource_id='office-one'"
            )
            changed_by_field = json.loads(original)
            changed_by_field["first_line"] = "456 Different Example St"
            await writer.execute(
                f"UPDATE {relation} SET payload_json=$1::jsonb "
                "WHERE resource_type='Location' AND resource_id='office-one'",
                json.dumps(changed_by_field) if restore is None else restore,
            )
        return original


async def _assert_native_capture_tamper_refuses(context):
    for derived in (False, True):
        original = await _change_capture_row(context, derived=derived)
        try:
            with pytest.raises(SourceWitnessContractError) as refused:
                await _require_recipe(context, context.operator.connection)
            assert "_verify_source_rows" in _traceback_functions(refused.value)
            assert await _head(context.worker) is None
        finally:
            await _change_capture_row(context, derived=derived, restore=original)
        await _require_recipe(context, context.operator.connection)


async def _assert_source_replay_lifecycle_guards(context, monkeypatch):
    async def partial(factory, connection, *, source_profile, reviewed_origin, expected_anchor, expected_identity):
        async with factory(
            source_profile=source_profile,
            source_id=reviewed_origin.source_id,
            endpoint_id=reviewed_origin.endpoint_id,
            dataset_id=reviewed_origin.dataset_id,
        ) as export:
            async with aclosing(export["resources"]()) as pages:
                assert await anext(pages)
            return expected_identity

    async with _login(context.worker, "execution") as connection:
        with monkeypatch.context() as patch:
            patch.setattr(capture_module, "_verify_source_rows", partial)
            with pytest.raises(ValueError, match="catalog_source_stream_incomplete"):
                await _require_recipe(context, connection)
        await _assert_replay_exit_failure(context, connection, monkeypatch)
    assert await _head(context.worker) is None


async def _assert_replay_exit_failure(context, connection, monkeypatch):
    original_identity, original_finish = capture_module._export_identity, capture_module._finish_source_replay
    observed_by_field = {}

    def observe_export(export, reviewed, expected_identity=None):
        observed_by_field["export"] = export
        return original_identity(export, reviewed, expected_identity)

    async def close_after_eof(reader, replay):
        result = await original_finish(reader, replay)
        scope = await source_scope(observed_by_field["export"])
        assert not scope.streams_by_token
        observed_by_field["closed_after_eof"] = True
        await scope.connection.close()
        return result

    with monkeypatch.context() as patch:
        patch.setattr(capture_module, "_export_identity", observe_export)
        patch.setattr(capture_module, "_finish_source_replay", close_after_eof)
        with pytest.raises(asyncpg.InterfaceError, match="connection is closed"):
            await _require_recipe(context, connection)
    assert observed_by_field["closed_after_eof"]


async def _manifest(context, *, generation=None):
    async with _login(context.worker, "execution") as connection:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            return await resolve_network_serving_manifest(
                connection,
                generation_id=generation,
                control_schema=context.worker.fixture.control_schema,
            )


async def _assert_serving_parity(context, manifest, connection):
    from api.network_compact_address import compact_address_native_sql

    relation, parameters = compact_address_native_sql(manifest.compact_layout, first_parameter=1)
    offices = [
        tuple(row)
        for row in await connection.fetch(
            f"SELECT entity_id,second_line,canonical_network_ids FROM {relation} office ORDER BY second_line",
            *parameters,
        )
    ]
    assert offices == [("1234567893", "Suite 1", [context.network_id]), ("1234567893", "Suite 2", [])]
    memberships = await connection.fetch(
        f'SELECT network_id,provider_system,provider_id,location_id FROM "{manifest.schema_name}".network_membership'
    )
    assert len(memberships) == 1
    assert tuple(memberships[0])[:3] == (context.network_id, "npi", "1234567893")
    assert memberships[0][3] == registry_location_id(context.case.origin.source_id, "office-one")
    assert memberships[0][3] != registry_location_id(context.case.origin.source_id, "office-two")
    provider_only = await connection.fetch(
        f'SELECT network_id,provider_system,provider_id FROM "{manifest.schema_name}".network_provider_only_evidence'
    )
    assert [tuple(row) for row in provider_only] == [(context.network_id, "npi", "1000000004")]
    assert all(row[2] != "1000000004" for row in memberships)
    return offices, [tuple(row) for row in memberships], [tuple(row) for row in provider_only]


async def _assert_authority_and_missing_runtime(context):
    async def refuse(_request, _count):
        return 403

    for missing_runtime in (False, True):
        async with _consumer(context.worker, authority_hook=None if missing_runtime else refuse) as (arguments, _):
            if missing_runtime:
                original = context.runtime
                context.runtime = None
            try:
                arguments["initial_address_sources"] = _initial_sources(context)
                queued = await _enqueue_ordinary(context.worker)
                with pytest.raises(ValueError, match="registry_publication_request_refused") as rejected_error:
                    await consume_registry_publication_once(**arguments)
                if missing_runtime:
                    assert (
                        str(rejected_error.value.__context__)
                        == "registry_source_composition_source_witness_custody_unavailable"
                    )
                assert (await _status(context.worker, queued))["state"] == "rejected"
                assert await _head(context.worker) is None
            finally:
                if missing_runtime:
                    context.runtime = original


async def _assert_stale_approval_after_seal(context):
    callbacks = context.worker.publication_custody_by_name
    original_seal = callbacks["seal_candidate"]

    async def change_approval_after_seal(target):
        result = await original_seal(target)
        fixture = context.worker.fixture
        actor = context.worker.seed.actor
        record = await _draft(fixture.engine, fixture.control_schema, _create(), actor)
        await _approve(
            fixture.connection,
            fixture.control_schema,
            await _command(fixture.connection, fixture.control_schema, record),
            actor,
        )
        return result

    callbacks["seal_candidate"] = change_approval_after_seal
    try:
        async with _consumer(context.worker) as (arguments, _):
            arguments["initial_address_sources"] = _initial_sources(context)
            queued = await _enqueue_ordinary(context.worker)
            sealed_before = len(context.sealed_targets)
            with pytest.raises(ValueError, match="registry_publication_request_refused"):
                await consume_registry_publication_once(**arguments)
            assert len(context.sealed_targets) == sealed_before + 1
            assert (await _status(context.worker, queued))["state"] == "rejected"
            assert await _head(context.worker) is None
    finally:
        callbacks["seal_candidate"] = original_seal


async def _consume_expected_success(context, arguments_by_name, queued):
    try:
        return await consume_registry_publication_once(**arguments_by_name)
    except Exception as error:
        admin = context.worker.fixture.connection
        if not admin.is_in_transaction():
            try:
                status = await _status(context.worker, queued)
            except Exception as status_error:
                error.add_note("Separate request-status read failed: " + type(status_error).__name__)
            else:
                error.add_note("Separate request status: " + json.dumps(status, sort_keys=True))
        if error.__context__ is not None:
            raise error from error.__context__
        raise


async def _publish_once(context, *, head=0, sources=None):
    async with _consumer(context.worker) as (arguments, calls):
        arguments["initial_address_sources"] = sources or (_initial_sources(context) if not head else None)
        queued = await _enqueue_ordinary(context.worker, head)
        completed = await _consume_expected_success(context, arguments, queued)
        assert completed["request_id"] == queued["request_id"]
        assert (await _status(context.worker, queued))["state"] == "completed" and len(calls) >= 4
        assert await _consume_expected_success(context, arguments, queued) is None
    manifest = await _manifest(context)
    assert manifest.generation_id == completed["result"]["serving_generation"] > head
    assert context.ready_targets[-1] == context.sealed_targets[-1]
    return manifest


async def _assert_lease_and_cancel(context, monkeypatch, fault):
    entered, hold = asyncio.Event(), asyncio.Event()

    async def pause_authority(_request, _count):
        entered.set()
        await hold.wait()

    from process import registry_publication_consumer as consumer

    with monkeypatch.context() as patch:
        patch.setattr(consumer, "_HEARTBEAT_SECONDS", 0.05)
        async with _consumer(context.worker, authority_hook=pause_authority) as (arguments, _):
            arguments["initial_address_sources"] = _initial_sources(context)
            queued = await _enqueue_ordinary(context.worker)
            pending = asyncio.create_task(consume_registry_publication_once(**arguments))
            try:
                await asyncio.wait_for(entered.wait(), 10)
                if fault == "cancel":
                    pending.cancel()
                else:
                    await context.worker.fixture.connection.execute(
                        f'UPDATE "{context.worker.fixture.control_schema}".registry_publication_request '
                        "SET lease_id=$1 WHERE request_id=$2",
                        uuid4(),
                        UUID(queued["request_id"]),
                    )
                with pytest.raises(asyncio.CancelledError if fault == "cancel" else ValueError):
                    await asyncio.wait_for(pending, 10)
                assert await _head(context.worker) is None
                assert (await _status(context.worker, queued))["state"] == "running"
            finally:
                if not pending.done():
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                # Resume this exact interrupted request through the ordinary authority refusal path.
                await context.worker.fixture.connection.execute(
                    f'UPDATE "{context.worker.fixture.control_schema}".registry_publication_request '
                    "SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE request_id=$1",
                    UUID(queued["request_id"]),
                )

    async def deny(_request, _count):
        return 403

    async with _consumer(context.worker, authority_hook=deny) as (arguments, _):
        arguments["initial_address_sources"] = _initial_sources(context)
        with pytest.raises(ValueError, match="registry_publication_request_refused"):
            await consume_registry_publication_once(**arguments)
    assert (await _status(context.worker, queued))["state"] == "rejected"


async def _assert_stale_head_refusal(context, head):
    from process.registry_approval_store import RegistryApprovalConflict

    with pytest.raises(RegistryApprovalConflict, match="registry_queue_revision_conflict"):
        await _enqueue_ordinary(context.worker, head - 1)
    assert await _head(context.worker) == head


def _exact_request_arguments(arguments, queued):
    return {**arguments, "config": replace(arguments["config"], only_request_id=UUID(queued["request_id"]))}


async def _reactivate_generation(context, target, incumbent):
    schema = context.worker.fixture.control_schema
    revision = await context.worker.fixture.connection.fetchval(
        f'SELECT approved_revision FROM "{schema}".registry_revision_control WHERE id=1'
    )
    assert target.approved_custom_revision == incumbent.approved_custom_revision == revision
    actor = RegistryActor("platform_admin", uuid4(), "system")
    command = RegistryReactivationCommand(
        target.generation_id, incumbent.generation_id, revision, "Restore reviewed exact capture", uuid4().hex
    )
    async with _login(context.worker, "api") as connection, connection.transaction():
        queued = await enqueue_registry_reactivation(
            connection, command, actor, session_token_sha256="a" * 64, control_schema=schema
        )
    async with _consumer(context.worker) as (arguments, _):
        result = await _consume_expected_success(context, _exact_request_arguments(arguments, queued), queued)
    assert result["request_id"] == queued["request_id"] and result["result"]["operation"] == "reactivate_generation"
    assert await _head(context.worker) == target.generation_id


async def _prepare_source_rollback(context, source_manifest, incumbent, *, require_witness_replay=True):
    schema = context.worker.fixture.control_schema
    actor = RegistryActor("platform_admin", uuid4(), "system")
    command_by_name = dict(
        operation="prepare_source_rollback",
        target_generation=source_manifest.generation_id,
        expected_serving_generation=incumbent.generation_id,
        expected_approved_revision=incumbent.approved_custom_revision,
        reason="Review retained source under current approval",
        idempotency_key=uuid4().hex,
    )
    async with _login(context.worker, "api") as connection, connection.transaction():
        queued = await enqueue_registry_source_rollback_preview(
            connection, command_by_name, actor, session_token_sha256="a" * 64, control_schema=schema
        )
    replays_before = len(context.replay_actors)
    sealed_before = len(context.sealed_targets)
    async with _consumer(context.worker) as (arguments, _):
        completed = await _consume_expected_success(context, _exact_request_arguments(arguments, queued), queued)
    assert completed["request_id"] == queued["request_id"]
    assert completed["result"]["operation"] == "prepare_source_rollback"
    assert completed["result"]["source_generation_id"] == source_manifest.generation_id
    assert completed["result"]["approved_custom_revision"] == incumbent.approved_custom_revision
    assert completed["result"]["validation"]["state"] == "ready"
    assert len(context.sealed_targets) == sealed_before + 1
    actors = context.replay_actors[replays_before:]
    if require_witness_replay:
        assert context.worker.roles_by_kind["execution"] in actors
        assert await context.operator.connection.fetchval("SELECT session_user") in actors
    assert await _head(context.worker) == incumbent.generation_id
    return completed, actor


async def _enqueue_reviewed_rollback(context, prepared):
    preview, actor = prepared
    command_by_name = dict(
        operation="publish_source_rollback",
        preview_request_id=preview["request_id"],
        candidate_sha256=preview["result"]["candidate_sha256"],
        expected_serving_generation=preview["result"]["expected_serving_generation"],
        expected_approved_revision=preview["result"]["approved_custom_revision"],
        reason="Publish reviewed retained source",
        idempotency_key=uuid4().hex,
    )
    async with _login(context.worker, "api") as connection, connection.transaction():
        return await enqueue_registry_source_rollback_publication(
            connection,
            command_by_name,
            actor,
            session_token_sha256="a" * 64,
            control_schema=context.worker.fixture.control_schema,
        )


async def _publish_source_rollback(context, target, incumbent, *, require_witness_replay=True):
    prepared = await _prepare_source_rollback(context, target, incumbent, require_witness_replay=require_witness_replay)
    queued = await _enqueue_reviewed_rollback(context, prepared)
    async with _consumer(context.worker) as (arguments, _):
        result = await _consume_expected_success(context, _exact_request_arguments(arguments, queued), queued)
    assert result["request_id"] == queued["request_id"] and result["result"]["operation"] == "publish_source_rollback"
    manifest = await _manifest(context)
    assert manifest.generation_id == result["result"]["generation_id"] > incumbent.generation_id
    assert manifest.approved_custom_revision == incumbent.approved_custom_revision
    return manifest


async def _ready_custody_snapshot(context, candidate_id):
    schema = context.worker.fixture.control_schema
    row = await context.worker.fixture.connection.fetchrow(
        f"SELECT candidate.state,custody.seal_sha256,custody.receipt_json::text "
        f'FROM "{schema}".network_membership_candidate candidate '
        f'JOIN "{schema}".network_candidate_custody custody USING(candidate_id) WHERE candidate_id=$1',
        UUID(candidate_id),
    )
    assert row is not None and row["state"] == "ready"
    assert (
        await context.worker.fixture.connection.fetchval(
            f'SELECT count(*) FROM "{schema}".network_serving_manifest WHERE candidate_id=$1', UUID(candidate_id)
        )
        == 0
    )
    return tuple(row)


def _require_completed_head_race(error):
    cause = error.__cause__ or error.__context__
    is_expected_refusal = (
        isinstance(cause, RegistryApprovalConflict)
        and str(cause) == "registry_source_rollback_revision_conflict"
        and "complete_queued_registry_source_rollback_publication" in _traceback_functions(cause)
    )
    if not is_expected_refusal:
        if cause is not None:
            raise error from cause
        raise error


async def _assert_head_change_after_seal(context, target, incumbent):
    prepared = await _prepare_source_rollback(context, target, incumbent)
    candidate_id = prepared[0]["result"]["candidate_id"]
    original = await _ready_custody_snapshot(context, candidate_id)
    queued = await _enqueue_reviewed_rollback(context, prepared)
    await _reactivate_generation(context, target, incumbent)
    async with _consumer(context.worker) as (arguments, _):
        with pytest.raises(ValueError, match="registry_publication_request_refused") as refused:
            await _consume_expected_success(context, _exact_request_arguments(arguments, queued), queued)
    _require_completed_head_race(refused.value)
    assert (await _status(context.worker, queued))["state"] == "rejected"
    assert await _head(context.worker) == target.generation_id
    assert await _ready_custody_snapshot(context, candidate_id) == original


async def _provider_only_api_page(context, manifest, connection):
    from api.network_address_scope import network_address_read_scope
    from api.network_provider_only_read import read_network_provider_only_page
    from api.network_provider_read import NetworkProviderReadUnavailable

    replays_before = len(context.replay_actors)
    with network_address_read_scope(manifest, (context.network_id,)) as scope:
        try:
            page = await read_network_provider_only_page(
                connection, scope, limit=1, control_schema=context.worker.fixture.control_schema
            )
            empty = await read_network_provider_only_page(
                connection, scope, limit=1, offset=1, control_schema=context.worker.fixture.control_schema
            )
        except NetworkProviderReadUnavailable as error:
            if error.__context__ is not None:
                raise error from error.__context__
            raise
    assert len(context.replay_actors) == replays_before
    assert page["generation_id"] == manifest.generation_id and page["total_count"] == 1
    assert page["has_more"] is False
    assert [(row["provider_system"], row["provider_id"], row["npi"]) for row in page["providers"]] == [
        ("npi", "1000000004", 1000000004)
    ]
    memberships = page["providers"][0]["network_memberships"]
    assert [membership["network_id"] for membership in memberships] == [context.network_id]
    assert all(membership["evidence_ids"] for membership in memberships)
    assert empty["total_count"] == 1 and empty["providers"] == [] and empty["has_more"] is False
    return page


async def _assert_provider_only_api_rollback(context, first, second):
    schema = context.worker.fixture.control_schema
    async with _login(context.worker, "api") as reader:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            selected = await resolve_network_serving_manifest(reader, control_schema=schema, api_read=True)
            assert selected == second
            original = await _provider_only_api_page(context, selected, reader)
            restored = await _publish_source_rollback(context, first, second)
            assert await _provider_only_api_page(context, selected, reader) == original
            assert await resolve_network_serving_manifest(reader, control_schema=schema, api_read=True) == second
    async with _login(context.worker, "api") as reader:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            assert await resolve_network_serving_manifest(reader, control_schema=schema, api_read=True) == restored
            pinned = await _provider_only_api_page(context, restored, reader)
            await _assert_head_change_after_seal(context, second, restored)
            assert await _provider_only_api_page(context, restored, reader) == pinned
            assert await resolve_network_serving_manifest(reader, control_schema=schema, api_read=True) == restored
    async with _login(context.worker, "api") as reader:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            assert await resolve_network_serving_manifest(reader, control_schema=schema, api_read=True) == second
            page = await _provider_only_api_page(context, second, reader)
            assert {**page, "generation_id": second.generation_id} == original


async def _assert_publications_keep_pinned_readers(context):
    first = await _publish_once(context)
    async with _login(context.worker, "api") as reader:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await _provider_only_api_page(context, first, reader)
    async with _login(context.worker, "execution") as old_reader:
        async with old_reader.transaction(isolation="repeatable_read", readonly=True):
            assert (
                await resolve_network_serving_manifest(old_reader, control_schema=context.worker.fixture.control_schema)
                == first
            )
            original = await _assert_serving_parity(context, first, old_reader)
            second = await _publish_once(context, head=first.generation_id)
            assert await _assert_serving_parity(context, first, old_reader) == original
            assert (
                await resolve_network_serving_manifest(old_reader, control_schema=context.worker.fixture.control_schema)
                == first
            )
    async with _login(context.worker, "execution") as reader:
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            assert await _assert_serving_parity(context, second, reader) == original
    await _assert_stale_head_refusal(context, second.generation_id)
    await _assert_provider_only_api_rollback(context, first, second)


async def test_source_witness_exact_capture_consumer_publication(monkeypatch, record_property):
    original_replay = capture_module._verify_source_rows
    replay_actors = []

    async def observe_completed_replay(factory, connection, **arguments):
        result = await original_replay(factory, connection, **arguments)
        replay_actors.append(await connection.fetchval("SELECT session_user"))
        return result

    monkeypatch.setattr(capture_module, "_verify_source_rows", observe_completed_replay)
    async with _publication_case(monkeypatch) as context:
        context.replay_actors = replay_actors
        assert asdict(context.case.constructed_origin) == asdict(context.case.origin)
        old_reader_wire = context.case.old_capture.as_dict()
        await _assert_runtime_and_approval_guards(context)
        await _assert_original_source_identity_guards(context)
        await _assert_native_capture_tamper_refuses(context)
        await _assert_source_replay_lifecycle_guards(context, monkeypatch)
        await _assert_authority_and_missing_runtime(context)
        for fault in ("cancel", "lease"):
            await _assert_lease_and_cancel(context, monkeypatch, fault)
        await _assert_stale_approval_after_seal(context)
        await _assert_publications_keep_pinned_readers(context)
        assert context.worker.roles_by_kind["execution"] in replay_actors
        assert await context.operator.connection.fetchval("SELECT session_user") in replay_actors
        assert context.case.old_capture.as_dict() == old_reader_wire
        async with context.case.reader.transaction(isolation="repeatable_read", readonly=True):
            assert (
                await require_retained_fhir_capture(context.case.reader, context.case.old_capture)
                == context.case.old_capture
            )
        record_property("component", "source_witness_exact_capture_publication")
        record_property("rows", str(sum(count for _, count in context.case.origin.resource_counts)))
        record_property("membership_rows", "1")
        record_property("provider_only_rows", "1")
        record_property("worker_permission_installations", str(context.installation.installations))
        record_property(
            "provider_only_api",
            "actual API login; pinned pages through publication and rollback; zero fresh Source replay",
        )
        record_property(
            "authority",
            "protected approved full capture SHA; independent original Source replay and real queue/K custody",
        )
        record_property(
            "limits",
            "LOCAL exact capture and provider-only API read parity; "
            "Source successor refresh disabled; no runtime/performance claim",
        )


async def _membership_snapshot(connection, manifest):
    membership = await connection.fetch(
        f"SELECT network_id,provider_system,provider_id,location_id::text,evidence_id "
        f'FROM "{manifest.schema_name}".network_membership '
        "ORDER BY network_id,provider_system,provider_id,location_id,evidence_id"
    )
    provider_only = await connection.fetch(
        f"SELECT network_id,provider_system,provider_id,evidence_id "
        f'FROM "{manifest.schema_name}".network_provider_only_evidence '
        "ORDER BY network_id,provider_system,provider_id,evidence_id"
    )
    return tuple(map(tuple, membership)), tuple(map(tuple, provider_only))


async def test_source_witness_introduction_preserves_retained_generation(monkeypatch, record_property):
    """Retain exact old facts while the queued addition and rollback preserve pinned readers."""
    async with _publication_case(monkeypatch) as context:
        context.replay_actors = []
        retained_capture = await capture_retained_source_for_roles(context.case, context.runtime.runtime_roles)
        retained, context.approved, retained_network = await _approved_capture_recipe(
            context.case.fixture, context.serving, retained_capture
        )
        assert retained_network != context.network_id
        initial = RegistryCompositionAddressSources(
            context.address,
            compact=True,
            initial_source_recipes=(retained,),
            source_witness_runtime=context.runtime,
        )
        first = await _publish_once(context, sources=initial)
        schema = context.worker.fixture.control_schema
        replacement = replace(initial, initial_source_recipes=(), replacement_source_recipes=(retained, context.recipe))
        assert replacement.base_source == initial.base_source and replacement.npi_source == initial.npi_source
        async with _login(context.worker, "execution") as reader:
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                assert await resolve_network_serving_manifest(reader, control_schema=schema) == first
                original = await _membership_snapshot(reader, first)
                assert {member[0] for member in original[0]} == {retained_network}
                second = await _publish_once(context, head=first.generation_id, sources=replacement)
                assert await _membership_snapshot(reader, first) == original
                assert await resolve_network_serving_manifest(reader, control_schema=schema) == first
        async with _login(context.worker, "execution") as reader:
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                combined = await _membership_snapshot(reader, second)
                assert tuple(member for member in combined[0] if member[0] == retained_network) == original[0]
                assert tuple(member for member in combined[1] if member[0] == retained_network) == original[1]
                introduced_members = tuple(member for member in combined[0] if member[0] == context.network_id)
                assert len(introduced_members) == 1
                assert introduced_members[0][1:4] == (
                    "npi",
                    "1234567893",
                    str(registry_location_id(context.case.origin.source_id, "office-one")),
                )
                assert {member[0] for member in combined[1]} == {retained_network, context.network_id}
                restored = await _publish_source_rollback(context, first, second, require_witness_replay=False)
                assert await _membership_snapshot(reader, second) == combined
                assert await resolve_network_serving_manifest(reader, control_schema=schema) == second
        async with _login(context.worker, "execution") as reader:
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                assert await resolve_network_serving_manifest(reader, control_schema=schema) == restored
                assert await _membership_snapshot(reader, restored) == original
        record_property("component", "retained_generation_source_witness_introduction")
        record_property("generations", json.dumps([first.generation_id, second.generation_id, restored.generation_id]))
        record_property("membership_counts", json.dumps([len(original[0]), len(combined[0]), len(original[0])]))
        record_property("provider_only_counts", json.dumps([len(original[1]), len(combined[1]), len(original[1])]))
        record_property("pinned_reader_publication_and_rollback", "unchanged")
        record_property(
            "limits",
            "Local queued G1-to-G2 adoption and reviewed source rollback; same physical Source, distinct recipes; no automatic refresh",
        )
