# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine accepted-source head fences and native protected-reader qualification."""

import asyncio
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from dataclasses import asdict
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_membership_refresh_sources as sources
from process.registry_company_approval_fence import registry_company_approval_transaction
from process.registry_refresh_anchor_contract import RegistryRefreshAnchorUnavailable
from tests import test_network_serving_schema_postgres as registry
from tests.registry_aca_refresh_anchor_postgres_support import accepted_aca_anchor
from tests.test_network_fhir_membership_source_postgres import _reviewed_binding
from tests.test_registry_aca_refresh_anchors_postgres import _approved
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft

pytestmark = pytest.mark.asyncio


async def _factory(stack, url):
    engine = create_async_engine(url, pool_size=1, max_overflow=0, hide_parameters=True)
    stack.push_async_callback(engine.dispose)
    return async_sessionmaker(engine, expire_on_commit=False)


async def _remove_fence_role(case, role):
    async with case.admin() as session, session.begin():
        await session.execute(text(f'DROP OWNED BY "{role}" CASCADE'))
        await session.execute(text(f'DROP ROLE "{role}"'))
        assert not await session.scalar(text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=:role)"), {"role": role})


async def _reader_grants(case, schema):
    async with case.admin() as session, session.begin():
        await session.execute(
            text(f'GRANT USAGE ON SCHEMA "{schema}","{case.names.source_schema}" TO "{case.names.reader}"')
        )
        await session.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{case.names.reader}"'))
        for table in ("mrf_publication_receipt", "reference_family_result_generation"):
            await session.execute(
                text(f'GRANT SELECT ON "{case.names.source_schema}".{table} TO "{case.names.reader}"')
            )


async def _native_fence_factory(case, stack):
    role = "aca_head_fence_" + uuid4().hex
    async with case.admin() as session, session.begin():
        await session.execute(text(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{case.names.password}'"))
        await session.execute(text(f'GRANT "{case.names.retention_owner}","{case.names.control_owner}" TO "{role}"'))
        await session.execute(text(f'GRANT USAGE ON SCHEMA "{case.names.source_schema}" TO "{role}"'))
        await session.execute(
            text(f'GRANT SELECT,MAINTAIN ON ALL TABLES IN SCHEMA "{case.names.source_schema}" TO "{role}"')
        )
    stack.push_async_callback(_remove_fence_role, case, role)
    return await _factory(stack, case.admin.kw["bind"].url.set(username=role, password=case.names.password))


def _enrollment(binding, recipe):
    return [
        {
            "binding_id": binding.binding_id,
            "binding_revision": binding.binding_revision,
            "network_id": binding.network_id,
            "continuity_policy": "retained_successor",
            "source_scope": {
                "source_system": "aca",
                "source_id": recipe.binding_coordinates.source_id,
                "dataset_schema": recipe.binding_coordinates.dataset_schema,
                "producer_id": recipe.binding_coordinates.producer_id,
                "source_key": binding.record_json["source_key"],
                "approved_binding_sha256": binding.approved_binding_sha256,
                "capture_generation": recipe.source_pin.generation_id,
            },
        }
    ]


@asynccontextmanager
async def _aca_context(case, monkeypatch):
    from process.network_cms_registry_address_capture import native_driver
    from process.registry_refresh_anchor_service import read_approved_refresh_bindings

    engine = case.admin.kw["bind"]
    monkeypatch.setenv(
        "NETWORK_REGISTRY_TEST_DSN", engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    async with aclosing(registry.serving_schema.__wrapped__()) as generator, AsyncExitStack() as stack:
        connection, schema, registry_engine = await anext(generator)
        fields, approved = await _approved(connection, schema, registry_engine, case)
        await _reader_grants(case, schema)
        outer = await _factory(stack, case.sessions.kw["bind"].url)
        child = await _factory(stack, case.sessions.kw["bind"].url)
        fence = await _native_fence_factory(case, stack)
        readers = sources.RegistryMembershipRefreshSourceReaders(
            schema,
            child,
            aca_store=case.store,
            aca_source_schema=case.names.source_schema,
            head_fences=sources.RegistryMembershipRefreshHeadFences(aca_sessions=fence),
        )
        async with registry_company_approval_transaction(outer, control_schema=schema) as session:
            preliminary_scopes = [
                {
                    "binding_id": fields["binding_id"],
                    "binding_revision": 1,
                    "network_id": fields["network_id"],
                    "continuity_policy": "retained_successor",
                    "source_scope": {
                        "source_system": "aca",
                        "source_id": fields["source_id"],
                        "dataset_schema": fields["dataset_schema"],
                        "producer_id": fields["producer_id"],
                        "source_key": fields["source_key"],
                        "approved_binding_sha256": "0" * 64,
                        "capture_generation": "0" * 64,
                    },
                }
            ]
            bindings = await read_approved_refresh_bindings(
                await native_driver(session), approved, preliminary_scopes, control_schema=schema
            )
        yield outer, readers, schema, approved, _enrollment(bindings[0], case.recipe)


async def _assert_head_fenced(case):
    async with case.admin() as blocked, blocked.begin():
        with pytest.raises(DBAPIError, match="could not obtain lock"):
            async with blocked.begin_nested():
                await blocked.execute(
                    text(
                        f'LOCK TABLE "{case.names.source_schema}".mrf_publication_receipt IN ROW EXCLUSIVE MODE NOWAIT'
                    )
                )


async def test_aca_current_head_fence_uses_genuine_publisher_and_keeps_readers_closed(monkeypatch):
    from dataclasses import replace

    async with accepted_aca_anchor(monkeypatch) as case, _aca_context(case, monkeypatch) as context:
        outer, readers, schema, approved, enrollment = context
        async with registry_company_approval_transaction(outer, control_schema=schema) as session:
            with pytest.raises(RegistryRefreshAnchorUnavailable):
                await sources.resolve_registry_membership_refresh_sources(
                    session,
                    enrollment,
                    approved_revision=approved.approved_revision,
                    readers=replace(readers, head_fences=sources.RegistryMembershipRefreshHeadFences()),
                )
        async with registry_company_approval_transaction(outer, control_schema=schema) as session:
            async with sources.pin_registry_membership_refresh_sources(
                session, enrollment, approved_revision=approved.approved_revision, readers=readers
            ) as resolved:
                assert resolved.recipes == (case.recipe,)
                assert (
                    resolved.accepted_sources[0]["bindings_sha256"] == case.recipe.source_pin.validation.manifest_sha256
                )
                await _assert_head_fenced(case)
        async with case.sessions() as reader, reader.begin():
            await reader.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            assert (await case.store.accepted(reader, case.recipe.source_pin.prepared.ownership.dataset_id))[
                "recipe"
            ] == case.recipe
            assert not await reader.scalar(
                text("SELECT has_table_privilege(current_user,:relation,'MAINTAIN')"),
                {"relation": f'"{case.store.control_schema}".registry_aca_source_notification'},
            )


@pytest.mark.parametrize("failure", (RuntimeError, asyncio.CancelledError))
async def test_source_pin_failure_releases_native_heads_and_preserves_error(monkeypatch, failure):
    async with accepted_aca_anchor(monkeypatch) as case, _aca_context(case, monkeypatch) as context:
        outer, readers, schema, approved, enrollment = context
        original = failure("consumer failed")
        with pytest.raises(failure) as caught:
            async with registry_company_approval_transaction(outer, control_schema=schema) as session:
                async with sources.pin_registry_membership_refresh_sources(
                    session, enrollment, approved_revision=approved.approved_revision, readers=readers
                ):
                    raise original
        assert caught.value is original
        async with case.admin() as unlocked, unlocked.begin():
            assert await unlocked.scalar(
                text("SELECT pg_try_advisory_xact_lock(hashtext('address_numeric_grid_alias_v1'))")
            )
            await unlocked.execute(
                text(f'LOCK TABLE "{case.names.source_schema}".mrf_publication_receipt IN ROW EXCLUSIVE MODE NOWAIT')
            )
        assert readers.source_reader_sessions.kw["bind"].pool.checkedout() == 0
        assert readers.aca_head_fence_sessions.kw["bind"].pool.checkedout() == 0


async def _add_same_capture_binding(case, schema):
    from process.network_approved_membership_source import pin_approved_membership_source
    from process.registry_refresh_anchor_service import read_approved_refresh_bindings

    engine = case.admin.kw["bind"]
    actor = _actor()
    network = await _draft(engine, schema, _create("network"), actor)
    connection = await asyncpg.connect(engine.url.set(drivername="postgresql").render_as_string(hide_password=False))
    try:
        binding_by_field = {
            "binding_id": str(uuid4()),
            **asdict(case.recipe.binding_coordinates),
            "source_key": "12345VA0000002",
            "source_scope_json": {
                "issuer_id": "12345",
                "state": "VA",
                "plan_id": "12345VA0000002",
                "plan_year": 2026,
                "checksum_network": 43,
            },
            "network_id": network["record_id"],
            "evidence_id": "second-reviewed-network",
            "evidence_sha256": "d" * 64,
            "operation": "bind",
            "expected_revision": 0,
            "expected_network_id": None,
        }
        binding = (await _reviewed_binding(connection, schema, actor, [binding_by_field]))["records"][0]
        approval = await _approve(connection, schema, await _command(connection, schema, network, binding), actor)
        async with connection.transaction(isolation="repeatable_read"):
            approved = await pin_approved_membership_source(
                connection, approved_revision=approval["approved_revision"], control_schema=schema
            )
            preliminary_scopes = [
                {
                    "binding_id": binding_by_field["binding_id"],
                    "binding_revision": 1,
                    "network_id": binding_by_field["network_id"],
                    "continuity_policy": "retained_successor",
                    "source_scope": {
                        "source_system": "aca",
                        "source_id": binding_by_field["source_id"],
                        "dataset_schema": binding_by_field["dataset_schema"],
                        "producer_id": binding_by_field["producer_id"],
                        "source_key": binding_by_field["source_key"],
                        "approved_binding_sha256": "0" * 64,
                        "capture_generation": case.recipe.source_pin.generation_id,
                    },
                }
            ]
            bindings = await read_approved_refresh_bindings(
                connection, approved, preliminary_scopes, control_schema=schema
            )
        return approved, _enrollment(bindings[0], case.recipe)[0]
    finally:
        await connection.close()


async def test_same_capture_requires_complete_explicit_enrollment(monkeypatch):
    from process.registry_refresh_anchor_contract import RegistryRefreshAnchorMismatch

    async with accepted_aca_anchor(monkeypatch) as case, _aca_context(case, monkeypatch) as context:
        outer, readers, schema, _old, enrollment = context
        approved, additional = await _add_same_capture_binding(case, schema)
        async with registry_company_approval_transaction(outer, control_schema=schema) as session:
            with pytest.raises(RegistryRefreshAnchorMismatch, match="capture_enrollment_incomplete"):
                await sources.observe_registry_membership_refresh_sources(
                    session, enrollment, approved_revision=approved.approved_revision, readers=readers
                )
        complete = sorted([*enrollment, additional], key=lambda scope: scope["binding_id"])
        async with registry_company_approval_transaction(outer, control_schema=schema) as session:
            resolved = await sources.resolve_registry_membership_refresh_sources(
                session, complete, approved_revision=approved.approved_revision, readers=readers
            )
            assert resolved.recipes == (case.recipe,)
            assert len(resolved.accepted_sources) == 1
