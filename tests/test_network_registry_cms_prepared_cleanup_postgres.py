# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native custody component proof; terminal-owner authorization remains external."""

import json
from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import text

from process import network_registry_cms_prepared_cleanup as cleanup
from process import network_registry_cms_prepared_pair as retention
from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_serving_receipt as serving
from process.network_cms_registry_source_pair import (
    _json,
    capture_registry_cms_source_pair,
    require_registry_cms_source_pair,
)
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from tests import cms_npd_admission_postgres_support as support
from tests.provider_directory_cms_capacity_test_support import cms_execution
from tests.test_network_cms_registry_source_pair_postgres import _publish_pair
from tests.test_network_registry_cms_prepared_pair_postgres import _prepared_input, _publish_stages
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template


@pytest_asyncio.fixture
async def prepared_capture(monkeypatch, tmp_path, request):
    """Register exact role cleanup before creating a real retained preparation."""
    directory, acquired = support.retained_release(tmp_path)
    roles = tuple("cleanup_" + uuid4().hex for _ in range(2))
    capture_id = uuid4()
    journal = tmp_path / "prepared-cleanup-roles.json"
    journal.write_text(json.dumps({"phase": "registered", "roles": roles}))
    async with support.admission_database(monkeypatch) as database:
        try:
            async with database.engine.begin() as connection:
                for role in roles:
                    await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
            with support.release_probe_client(directory) as client:
                initial = await cms._run_acquired({"context": {}}, {}, "cleanup-component", directory, acquired, client)
            has_physical_layout = getattr(request, "param", None) == "finalized_v2"
            execution = cms_execution(day=initial["registry_source_admission"]["semantic_projection_as_of"])
            proof = await _publish_pair(
                database,
                initial,
                monkeypatch,
                physical=has_physical_layout,
                selection_proof_id=execution.attestation.proof_id,
            )
            if has_physical_layout:
                retained = None
                pair = await capture_registry_cms_source_pair(
                    database.session_factory,
                    proof,
                    capture_id=capture_id,
                    owner_role=roles[0],
                    runtime_roles=(roles[1],),
                )
            else:
                prepared, factory, retention_request = await _prepared_input(
                    database, proof, capture_id, roles, monkeypatch
                )
                attempt = (
                    RegistryCMSSourceAttempt(
                        factory.run_id, factory.run_id + ":" + "a" * 32, "2026-10-08T10:00:00+00:00"
                    )
                    if getattr(request, "param", None) == "tagged"
                    else None
                )
                retained = await retention.prepare_registry_cms_source_pair(
                    prepared,
                    factory,
                    retention_request,
                    capture_id=capture_id,
                    owner_role=roles[0],
                    runtime_roles=(roles[1],),
                    source_attempt=attempt,
                )
                pair = None
            yield SimpleNamespace(database=database, retained=retained, pair=pair, roles=roles, capture_id=capture_id)
        finally:
            await _remove_roles(database, roles)
            journal.write_text(json.dumps({"phase": "cleanup_verified", "roles": roles}))


async def _remove_roles(database, roles):
    async with database.engine.begin() as connection:
        for role in roles:
            if await connection.scalar(text("SELECT 1 FROM pg_roles WHERE rolname=:role"), {"role": role}):
                await connection.execute(text(f'DROP OWNED BY "{role}" CASCADE'))
                await connection.execute(text(f'DROP ROLE "{role}"'))
            assert not await connection.scalar(text("SELECT 1 FROM pg_roles WHERE rolname=:role"), {"role": role})


async def _repeatable_read(session):
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))


async def _probe(session, capture):
    return await cleanup.probe_registry_cms_source_capture(
        session, capture_id=capture.capture_id, owner_role=capture.roles[0], runtime_roles=(capture.roles[1],)
    )


async def _present(session, capture):
    for schema in (
        capture.retained.address_ownership.schema_name,
        capture.retained.recipe.source_pin.retained_epoch.schema_name,
    ):
        assert await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema}) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_capture", [None, "tagged"], indirect=True)
async def test_native_prepared_decode_and_exact_removal(prepared_capture):
    capture = prepared_capture
    original = capture.retained
    comment = retention._prepared_comment_prefix(original) + _json(original.as_dict())
    assert cleanup.decode_prepared_registry_cms_source_pair(comment) == original
    _reject_documents(original)
    async with capture.database.session_factory() as session, session.begin():
        await _repeatable_read(session)
        assert await _probe(session, capture) == original
        assert await cleanup.cleanup_prepared_registry_cms_source_pair(session, original) == "removed"
        assert await _probe(session, capture) is None
    async with capture.database.session_factory() as session, session.begin():
        await _repeatable_read(session)
        assert await _probe(session, capture) is None
        assert await cleanup.cleanup_prepared_registry_cms_source_pair(session, original) == "absent"
        assert await session.scalar(text("SELECT count(*) FROM mrf.entity_address_unified")) == 2


def _reject_documents(original):
    """Reject closed-wire, nested identity, duplicate-key and byte-bound drift."""
    document = json.loads(_json(original.as_dict()))
    prefix = retention._prepared_comment_prefix(original)
    other_prefix = retention._PREPARED_COMMENT_V2 if original.source_attempt is None else retention._PREPARED_COMMENT
    with pytest.raises(ValueError, match="prepared_capture_invalid"):
        cleanup.decode_prepared_registry_cms_source_pair(other_prefix + _json(document))
    paths = (
        (("extra",), True),
        (("lease_digest",), "invalid"),
        (("runtime_roles",), [original.owner_role]),
        (("address_stages", 0, 2), True),
        (("address_ownership", "schema_oid"), 0),
        (("address_ownership", "schema_oid"), 2**32),
        (("address_ownership", "relation_oids", 0, "oid"), 2**32),
        (("recipe", "source_pin", "retained_epoch", "epoch_id"), str(uuid4())),
        (("request", "source_pin", "resource_table_oid"), True),
        (("request", "extra_data_upper_bound_bytes"), False),
        (("request", "binding_coordinates", "extra"), True),
    )
    for path, invalid_value in paths:
        altered = deepcopy(document)
        nested_document = altered
        for key in path[:-1]:
            nested_document = nested_document[key]
        nested_document[path[-1]] = invalid_value
        with pytest.raises(ValueError, match="prepared_capture_invalid"):
            cleanup.decode_prepared_registry_cms_source_pair(prefix + _json(altered))
    for body in ('{"recipe":{},"recipe":{}}', "NaN", "[" * 1000, " " * 65537):
        with pytest.raises(ValueError, match="prepared_capture_invalid"):
            cleanup.decode_prepared_registry_cms_source_pair(prefix + body)


@pytest.mark.asyncio
async def test_native_catalog_identity_preserves_search_path_and_rejects_index_loss(prepared_capture):
    """Reader visibility changes do not change identity or hide actual index changes."""
    capture = prepared_capture
    address_schema = capture.retained.address_ownership.schema_name
    async with capture.database.session_factory() as session, session.begin():
        await _repeatable_read(session)
        for search_path in ("pg_catalog", "public,pg_catalog", f'"{address_schema}",public,pg_catalog'):
            await session.execute(text("SELECT set_config('search_path',:path,true)"), {"path": search_path})
            assert await _probe(session, capture) == capture.retained
            assert await session.scalar(text("SELECT current_setting('search_path')")) == search_path
        with pytest.raises(ValueError, match="registry_cms_prepared_pair_invalid"):
            async with session.begin_nested():
                await session.execute(
                    text(f'DROP INDEX "{address_schema}".entity_address_unified_canonical_network_ids_idx')
                )
                await retention._require_prepared_pair(session, capture.retained)
        assert await _probe(session, capture) == capture.retained


@pytest.mark.asyncio
async def test_native_finalized_pair_is_preserved(prepared_capture):
    capture = prepared_capture
    async with capture.database.session_factory() as session, session.begin():
        await _repeatable_read(session)
        predecessor = await serving.read_current_receipt(session, "mrf")
        receipt_id, payload = await _publish_stages(session, capture.retained, predecessor)
        pair = await retention.bind_prepared_registry_cms_source_pair(
            session, capture.retained, receipt_id=receipt_id, receipt_payload=payload
        )
    async with capture.database.session_factory() as session, session.begin():
        await _repeatable_read(session)
        assert await _probe(session, capture) == pair
        assert await cleanup.cleanup_prepared_registry_cms_source_pair(session, capture.retained) == "finalized"
        assert await require_registry_cms_source_pair(session, pair) == pair


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared_capture", ["finalized_v2"], indirect=True)
async def test_native_finalized_v2_probe_preserves_witness(prepared_capture):
    """Validate a genuine existing v2 capture without interpreting it as prepared."""
    capture = prepared_capture
    pair = capture.pair
    assert pair.witness_json is not None
    async with capture.database.session_factory() as session, session.begin():
        await _repeatable_read(session)
        assert await _probe(session, capture) == pair
        assert await require_registry_cms_source_pair(session, pair) == pair


@pytest.mark.asyncio
@pytest.mark.parametrize("tamper", ["oid", "comment", "address_acl", "raw_acl"])
async def test_native_tampering_refuses_removal(prepared_capture, tamper):
    capture = prepared_capture
    address = capture.retained.address_ownership.schema_name
    raw = capture.retained.recipe.source_pin.retained_epoch.schema_name
    async with capture.database.session_factory() as session, session.begin():
        if tamper in {"oid", "comment"}:
            document = json.loads(_json(capture.retained.as_dict()))
            if tamper == "oid":
                document["address_ownership"]["schema_oid"] += 1
            else:
                document["lease_digest"] = "0" * 64
            comment = (retention._PREPARED_COMMENT + _json(document)).replace("'", "''")
            await (await cleanup.native_driver(session)).execute(f"COMMENT ON SCHEMA \"{address}\" IS '{comment}'")
        else:
            schema, table = (
                (address, "entity_address_unified")
                if tamper == "address_acl"
                else (raw, "provider_directory_dataset_resource")
            )
            await session.execute(text(f'GRANT UPDATE ON "{schema}"."{table}" TO "{capture.roles[1]}"'))
    async with capture.database.session_factory() as session, session.begin():
        await _repeatable_read(session)
        with pytest.raises(ValueError):
            await cleanup.cleanup_prepared_registry_cms_source_pair(session, capture.retained)
        await _present(session, capture)


@pytest.mark.asyncio
@pytest.mark.parametrize("dependency", ["external_view", "unexpected_type"])
async def test_native_dependency_refusal_rolls_back_both_drops(prepared_capture, dependency):
    capture = prepared_capture
    raw = capture.retained.recipe.source_pin.retained_epoch.schema_name
    async with capture.database.session_factory() as session, session.begin():
        if dependency == "external_view":
            await session.execute(
                text(
                    f'CREATE VIEW mrf.retained_dependency AS SELECT * FROM "{raw}".provider_directory_dataset_resource'
                )
            )
        else:
            await session.execute(text(f"CREATE TYPE \"{raw}\".unexpected_type AS ENUM ('synthetic')"))
    async with capture.database.session_factory() as session, session.begin():
        await _repeatable_read(session)
        with pytest.raises(asyncpg.DependentObjectsStillExistError):
            await cleanup.cleanup_prepared_registry_cms_source_pair(session, capture.retained)
        assert await _probe(session, capture) == capture.retained
    async with capture.database.session_factory() as session:
        await _present(session, capture)


@pytest.mark.asyncio
async def test_native_cleanup_requires_caller_isolation_and_uncontended_locks(prepared_capture):
    capture = prepared_capture
    async with capture.database.session_factory() as session:
        with pytest.raises(ValueError, match="requires_transaction"):
            await cleanup.cleanup_prepared_registry_cms_source_pair(session, capture.retained)
        async with session.begin():
            with pytest.raises(ValueError, match="repeatable-read"):
                await cleanup.cleanup_prepared_registry_cms_source_pair(session, capture.retained)
    address = capture.retained.address_ownership.schema_name
    async with capture.database.session_factory() as holder, holder.begin():
        await holder.execute(text(f'LOCK TABLE "{address}".entity_address_unified IN ACCESS SHARE MODE'))
        async with capture.database.session_factory() as session, session.begin():
            await _repeatable_read(session)
            with pytest.raises(asyncpg.LockNotAvailableError):
                await cleanup.cleanup_prepared_registry_cms_source_pair(session, capture.retained)
            await _present(session, capture)


@pytest.mark.asyncio
async def test_native_probe_refuses_partial_namespace_and_wrong_custody(prepared_capture):
    capture = prepared_capture
    async with capture.database.session_factory() as session, session.begin():
        await _repeatable_read(session)
        with pytest.raises(ValueError):
            await cleanup.probe_registry_cms_source_capture(
                session, capture_id=capture.capture_id, owner_role=capture.roles[1], runtime_roles=(capture.roles[0],)
            )
        await _present(session, capture)
    raw = capture.retained.recipe.source_pin.retained_epoch.schema_name
    async with capture.database.session_factory() as session, session.begin():
        await session.execute(text(f'ALTER SCHEMA "{raw}" RENAME TO "unexpected_epoch_{capture.capture_id.hex}"'))
    async with capture.database.session_factory() as session, session.begin():
        await _repeatable_read(session)
        with pytest.raises(ValueError, match="prepared_capture_invalid"):
            await _probe(session, capture)
