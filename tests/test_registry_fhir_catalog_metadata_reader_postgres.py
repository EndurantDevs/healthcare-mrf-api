# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Separate metadata execution must be a reader before immutable sealing."""

import hashlib
import json
from dataclasses import replace

import asyncpg
import pytest

from db.plan_release_storage import storage_identifier
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_fhir_catalog_custody import PHYSICAL_TABLES
from process.registry_fhir_catalog_read import FHIRCatalogQuery, read_fhir_catalog
from process.registry_network_catalog_read import RegistryNetworkCatalogError
from tests.fhir_catalog_registry_reader_support import configured_catalog_reader
from tests.fhir_catalog_sync_integration_support import (
    adopt_existing_network,
    import_source,
    native_catalog_case,
    run_catalog_sync,
)


def _decoded(value):
    return json.loads(value) if isinstance(value, str) else value


async def _sync_catalog(case, config, *, withhold_reader=None):
    async with configured_catalog_reader(
        case.settings,
        case.reader_url.render_as_string(hide_password=False),
        case.reader_url.username,
        case.reader_url.password,
    ):
        return await run_catalog_sync(case, config, withhold_reader=withhold_reader)


async def _schema_state(connection, schema):
    """Compare logical relation names and full row contents without physical metadata."""
    rows = await connection.fetch(
        "SELECT c.relname,c.relkind::text AS relkind FROM pg_class c "
        "JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relkind IN ('r','p','i','I','S') ORDER BY c.relname",
        schema,
    )
    content_digests = []
    for row in rows:
        if row["relkind"] not in {"r", "p"}:
            continue
        value = await connection.fetchval(
            "SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb)::text FROM "
            + storage_identifier(schema)
            + "."
            + storage_identifier(row["relname"])
            + " t"
        )
        content_digests.append((row["relname"], hashlib.sha256(value.encode()).hexdigest()))
    return [tuple(row) for row in rows], content_digests


async def _snapshots(case):
    records = await case.fixture.connection.fetch(
        f'SELECT to_jsonb(s) AS row FROM "{case.fixture.control_schema}".registry_fhir_catalog_snapshot s '
        "ORDER BY metadata_generation"
    )
    return {_decoded(row["row"])["metadata_generation"]: _decoded(row["row"]) for row in records}


async def _head(case):
    return tuple(
        await case.fixture.connection.fetchrow(
            f'SELECT generation_id,metadata_generation FROM "{case.fixture.control_schema}".network_serving_control WHERE id=1'
        )
    )


def _sqlstates(error):
    seen, states = set(), set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, asyncpg.PostgresError):
            states.add(error.sqlstate)
        error = error.__cause__ or error.__context__
    return states


def _metadata_reader_identity(case):
    """Require the distinct metadata execution actor before publishing."""
    config = case.config
    execution = config.consumer.execution_role
    assert execution == config.execution_url.username
    assert execution != case.worker.roles_by_kind["execution"]
    assert execution in config.reader_roles
    assert (
        len(
            {
                config.loader_url.username,
                config.sealer_url.username,
                config.lifecycle_url.username,
                execution,
                config.source_url.username,
            }
        )
        == 5
    )
    return config, execution


async def _metadata_reader_refusal(case, config, execution, connection, original_head, old):
    """Refuse the missing reader and preserve the incumbent after failed sealing."""
    bad = replace(
        config,
        reader_roles=tuple(role for role in config.reader_roles if role != execution),
        grant=replace(config.grant, version=config.grant.version + 1),
    )
    with pytest.raises(RuntimeError, match="registry_publication_consumer_unavailable") as refused:
        await _sync_catalog(case, bad, withhold_reader=execution)
    assert "42501" in _sqlstates(refused.value)
    assert await _head(case) == original_head
    snapshots = await _snapshots(case)
    assert snapshots[original_head[1]] == old and len(snapshots) == 2
    failed = next(row for generation, row in snapshots.items() if generation != original_head[1])
    assert failed["published_at"] is None and failed["standing_grant_sha256"] == bad.grant.sha256
    failed_physical = await _schema_state(connection, failed["schema_name"])
    login = await asyncpg.connect(
        config.execution_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    try:
        with pytest.raises(asyncpg.InsufficientPrivilegeError) as direct:
            await login.fetchval('SELECT count(*) FROM "' + failed["schema_name"] + '".registry_fhir_catalog_network')
        assert direct.value.sqlstate == "42501"
    finally:
        await login.close()
    return bad, snapshots, failed, failed_physical


async def _restore_metadata_reader(case, config, bad, execution, initial):
    """Restore only the reader and grant version before normal publication."""
    fixed = replace(
        bad,
        reader_roles=tuple(sorted((*bad.reader_roles, execution))),
        grant=replace(bad.grant, version=bad.grant.version + 1),
    )
    assert fixed == replace(
        bad, reader_roles=config.reader_roles, grant=replace(bad.grant, version=bad.grant.version + 1)
    )
    assert {
        field_name: field_value for field_name, field_value in fixed.grant.document().items() if field_name != "version"
    } == {
        field_name: field_value for field_name, field_value in bad.grant.document().items() if field_name != "version"
    }
    sync_receipt = await _sync_catalog(case, fixed)
    assert sync_receipt["state"] == "completed" and sync_receipt["accepted_sources"] == initial["accepted_sources"]
    assert sync_receipt["result"]["memberships_reused"] and sync_receipt["result"]["addresses_reused"]
    assert (
        sync_receipt["result"]["canonical_entities_deleted"]
        == sync_receipt["result"]["canonical_entities_archived"]
        == 0
    )
    return fixed


async def _metadata_reader_login(config, fresh):
    """Read every actual named catalog table through the metadata execution login."""
    login = await asyncpg.connect(
        config.execution_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    try:
        for table in PHYSICAL_TABLES:
            assert (
                await login.fetchval(
                    "SELECT count(*) FROM " + storage_identifier(fresh["schema_name"]) + "." + storage_identifier(table)
                )
                >= 0
            )
        assert (
            await login.fetchval('SELECT count(*) FROM "' + fresh["schema_name"] + '".registry_fhir_catalog_network')
            > 0
        )
    finally:
        await login.close()


def _record_metadata_reader_evidence(record_property):
    """Record the unchanged native reader acceptance properties."""
    record_property(
        "metadata_reader_native",
        json.dumps(
            {
                "excluded_execution_refused_42501": True,
                "independent_execution_login": True,
                "same_scope_version_only_successor": True,
                "ordinary_queue_named_publication": True,
                "new_metadata_generation": True,
                "old_published_and_failed_snapshots_unchanged": True,
                "membership_generation_and_rows_unchanged": True,
                "source_relation_names_and_data_unchanged": True,
                "candidate_all_tables_select": True,
                "idempotent_replay": True,
            },
            sort_keys=True,
        ),
    )


async def _publish_without_api_reader(case, config, api, execution, incumbent, connection):
    """Publish without API read access and prove its authenticated refusal."""
    missing_api = replace(
        config,
        reader_roles=tuple(role for role in config.reader_roles if role != api),
        preparation_roles={
            **config.preparation_roles,
            "application_roles": [role for role in config.preparation_roles["application_roles"] if role != api],
        },
        grant=replace(config.grant, version=2),
    )
    assert execution in missing_api.reader_roles
    first = await _sync_catalog(case, missing_api)
    assert first["state"] == "completed" and first["accepted_sources"] > 0
    original_head = await _head(case)
    assert original_head[0] == incumbent.generation_id
    old = (await _snapshots(case))[original_head[1]]
    assert old["published_at"] is not None
    old_physical = await _schema_state(connection, old["schema_name"])
    await _catalog_as_actor(case, case.reader_url, old, readable=False)
    assert await _head(case) == original_head
    return missing_api, first, original_head, old, old_physical


async def _restore_api_reader(case, config, api, execution, missing_api, first, original_head, old):
    """Publish the reader-only successor while preserving prior snapshots."""
    fixed = replace(
        missing_api,
        reader_roles=tuple(sorted((*missing_api.reader_roles, api))),
        preparation_roles=config.preparation_roles,
        grant=replace(missing_api.grant, version=3),
    )
    assert fixed == replace(
        missing_api,
        reader_roles=config.reader_roles,
        preparation_roles=config.preparation_roles,
        grant=replace(missing_api.grant, version=3),
    )
    assert {
        field_name: field_value for field_name, field_value in fixed.grant.document().items() if field_name != "version"
    } == {
        field_name: field_value
        for field_name, field_value in missing_api.grant.document().items()
        if field_name != "version"
    }
    sync_receipt = await _sync_catalog(case, fixed)
    assert sync_receipt["state"] == "completed" and sync_receipt["accepted_sources"] == first["accepted_sources"]
    assert sync_receipt["result"]["memberships_reused"] and sync_receipt["result"]["addresses_reused"]
    assert (
        sync_receipt["result"]["canonical_entities_deleted"]
        == sync_receipt["result"]["canonical_entities_archived"]
        == 0
    )
    head = await _head(case)
    assert head[0] == original_head[0] and head[1] != original_head[1]
    snapshots = await _snapshots(case)
    assert len(snapshots) == 2 and snapshots[original_head[1]] == old
    fresh = snapshots[head[1]]
    assert fresh["published_at"] is not None and fresh["standing_grant_sha256"] == fixed.grant.sha256
    assert fresh["schema_name"] != old["schema_name"]
    assert {api, execution} <= set(fixed.preparation_roles["application_roles"])
    assert fresh["source_vector_json"] == old["source_vector_json"]
    return fixed, fresh


@pytest.mark.asyncio
async def test_metadata_execution_reader_successor_preserves_incumbent(monkeypatch, tmp_path, record_property):
    """Preserve the incumbent through reader refusal, successor publication and replay."""
    async with native_catalog_case(monkeypatch, tmp_path) as case:
        await import_source(case, tmp_path / "source", retain_old=True, revision="one")
        await adopt_existing_network(case)
        config, execution = _metadata_reader_identity(case)
        connection = case.fixture.connection
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            incumbent = await resolve_network_serving_manifest(connection, control_schema=case.fixture.control_schema)
            source_before = await _schema_state(connection, "mrf")
            membership_before = await _schema_state(connection, incumbent.schema_name)
        initial = await _sync_catalog(case, config)
        assert initial["state"] == "completed" and initial["accepted_sources"] > 0
        assert initial["result"]["memberships_reused"] and initial["result"]["addresses_reused"]
        original_head = await _head(case)
        assert original_head[0] == incumbent.generation_id
        old = (await _snapshots(case))[original_head[1]]
        old_physical = await _schema_state(connection, old["schema_name"])

        bad, snapshots, failed, failed_physical = await _metadata_reader_refusal(
            case, config, execution, connection, original_head, old
        )

        fixed = await _restore_metadata_reader(case, config, bad, execution, initial)
        head = await _head(case)
        assert head[0] == original_head[0] and head[1] not in snapshots
        final = await _snapshots(case)
        assert len(final) == 3 and all(final[key] == previous_snapshot for key, previous_snapshot in snapshots.items())
        fresh = final[head[1]]
        assert fresh["published_at"] is not None and fresh["standing_grant_sha256"] == fixed.grant.sha256
        assert fresh["schema_name"] not in {old["schema_name"], failed["schema_name"]}
        assert fresh["source_vector_json"] == failed["source_vector_json"] == old["source_vector_json"]
        await _metadata_reader_login(config, fresh)
        assert await _schema_state(connection, old["schema_name"]) == old_physical
        assert await _schema_state(connection, failed["schema_name"]) == failed_physical
        assert await _schema_state(connection, incumbent.schema_name) == membership_before
        assert await _schema_state(connection, "mrf") == source_before
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            assert (
                await resolve_network_serving_manifest(connection, control_schema=case.fixture.control_schema)
                == incumbent
            )
        assert (await _sync_catalog(case, fixed))["state"] == "unchanged"
        _record_metadata_reader_evidence(record_property)


async def _catalog_as_actor(case, url, snapshot, *, readable):
    login = await asyncpg.connect(url.set(drivername="postgresql").render_as_string(hide_password=False))
    try:
        async with login.transaction(isolation="repeatable_read", readonly=True):
            if not readable:
                with pytest.raises(RegistryNetworkCatalogError) as refused:
                    await read_fhir_catalog(
                        login, FHIRCatalogQuery(), excluded_network_ids=(), control_schema=case.fixture.control_schema
                    )
                assert "42501" in _sqlstates(refused.value)
                return None
            for table in PHYSICAL_TABLES:
                assert (
                    await login.fetchval(
                        "SELECT count(*) FROM "
                        + storage_identifier(snapshot["schema_name"])
                        + "."
                        + storage_identifier(table)
                    )
                    >= 0
                )
            return await read_fhir_catalog(
                login, FHIRCatalogQuery(), excluded_network_ids=(), control_schema=case.fixture.control_schema
            )
    finally:
        await login.close()


@pytest.mark.asyncio
async def test_api_reader_successor_preserves_incumbent(monkeypatch, tmp_path, record_property):
    """Preserve the incumbent through reader refusal, successor publication and replay."""
    async with native_catalog_case(monkeypatch, tmp_path) as case:
        await import_source(case, tmp_path / "source", retain_old=True, revision="one")
        await adopt_existing_network(case)
        config = case.config
        api, execution = config.consumer.api_role, config.consumer.execution_role
        assert api == case.reader_url.username and execution == config.execution_url.username
        assert api != execution and {api, execution} <= set(config.reader_roles)
        assert execution != case.worker.roles_by_kind["execution"]
        connection = case.fixture.connection
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            incumbent = await resolve_network_serving_manifest(connection, control_schema=case.fixture.control_schema)
            source_before = await _schema_state(connection, "mrf")
            membership_before = await _schema_state(connection, incumbent.schema_name)
        missing_api, first, original_head, old, old_physical = await _publish_without_api_reader(
            case, config, api, execution, incumbent, connection
        )

        fixed, fresh = await _restore_api_reader(case, config, api, execution, missing_api, first, original_head, old)
        page = await _catalog_as_actor(case, case.reader_url, fresh, readable=True)
        execution_page = await _catalog_as_actor(case, config.execution_url, fresh, readable=True)
        assert page == execution_page and page["total"] > 0 and page["items"]
        assert page["metadata_generation"] == fresh["metadata_generation"]
        assert page["generation"] == str(incumbent.generation_id)
        assert await _schema_state(connection, old["schema_name"]) == old_physical
        assert await _schema_state(connection, incumbent.schema_name) == membership_before
        assert await _schema_state(connection, "mrf") == source_before
        assert (await _sync_catalog(case, fixed))["state"] == "unchanged"
        record_property(
            "api_reader_native",
            json.dumps(
                {
                    "published_without_api_reader": True,
                    "api_login_read_fhir_catalog_refused_42501": True,
                    "api_and_metadata_execution_own_logins_read_catalog": True,
                    "both_readers_select_actual_named_data": True,
                    "reader_list_and_version_2_to_3_only": True,
                    "ordinary_queue_named_publication": True,
                    "new_metadata_generation": True,
                    "old_snapshot_names_and_data_unchanged": True,
                    "membership_generation_and_rows_unchanged": True,
                    "source_relation_names_and_data_unchanged": True,
                    "idempotent_replay": True,
                },
                sort_keys=True,
            ),
        )
