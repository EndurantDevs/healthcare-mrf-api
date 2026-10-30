# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Accepted ACA refresh crosses real composition, custody and atomic publication."""

import asyncio
import json
from time import perf_counter
from uuid import UUID

import pytest
from sqlalchemy import text

from api.network_address_scope import network_address_read_scope
from api.network_compact_address import compact_address_native_sql
from api.network_provider_read import read_network_provider_page
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_cms_registry_address_capture import native_driver
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_company_approval_fence import registry_company_approval_transaction
from process.registry_initial_source_composition import verify_initial_source_office_receipt
from process.registry_membership_refresh_authority import COMMIT_AUTHORITY_PATH
from process.registry_membership_refresh_contract import accepted_membership_source_sha256
from process.registry_membership_refresh_queue import validated_membership_refresh_request_status
from process.registry_refresh_anchor_service import read_approved_refresh_bindings
from process.registry_source_inventory_reference import verify_source_recipe_metadata
from tests.registry_membership_refresh_execution_support import refresh_execution
from tests.test_network_fhir_membership_source_postgres import _reviewed_binding
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft
from tests.test_registry_membership_refresh_sources_postgres import _enrollment

pytestmark = pytest.mark.asyncio


async def _counts(fixture):
    schema = '"' + fixture.control_schema + '"'
    return tuple(
        await fixture.connection.fetchrow(
            f"SELECT (SELECT count(*) FROM {schema}.registry_publication_request),"
            f"(SELECT count(*) FROM {schema}.network_membership_candidate),"
            f"(SELECT count(*) FROM {schema}.network_serving_manifest),"
            f"(SELECT count(*) FROM {schema}.network_candidate_custody)"
        )
    )


async def _heads(fixture):
    return tuple(
        await fixture.connection.fetchrow(
            f'SELECT draft_revision,approved_revision FROM "{fixture.control_schema}".registry_revision_control'
        )
    )


async def _storage_rows(fixture, namespace):
    """Include MVCC identity so an otherwise invisible no-op UPDATE also fails parity."""
    tables = await fixture.connection.fetch(
        "SELECT relname FROM pg_class WHERE relnamespace=to_regnamespace($1) AND relkind='r' ORDER BY relname",
        namespace,
    )
    return {
        table["relname"]: await fixture.connection.fetch(
            f'SELECT xmin::text,ctid::text,to_jsonb(row)::text FROM "{namespace}"."{table["relname"]}" row '
            "ORDER BY to_jsonb(row)::text"
        )
        for table in tables
    }


async def _read(fixture, connection, generation=None, *, network_id=None):
    manifest = await resolve_network_serving_manifest(
        connection, generation_id=generation, control_schema=fixture.control_schema
    )
    network = fixture.enrollment[0]["network_id"] if network_id is None else network_id
    with network_address_read_scope(manifest, (network,)) as scope:
        providers = await read_network_provider_page(connection, scope, control_schema=fixture.control_schema)
    relation, parameters = compact_address_native_sql(
        manifest.compact_layout, first_parameter=1, network_ids=(network,)
    )
    near = await connection.fetch(
        f"SELECT location_key,canonical_network_ids FROM {relation} office "
        "WHERE ST_DWithin(ST_SetSRID(ST_MakePoint(long,lat),4326)::geography,"
        "ST_SetSRID(ST_MakePoint(-118,34),4326)::geography,1000)",
        *parameters,
    )
    assert providers["total_count"] == 1
    assert providers["providers"][0]["provider_id"] == "1000000491"
    assert len(providers["providers"][0]["locations"]) == 1
    assert [dict(row) for row in near] == [{"location_key": "a" * 64, "canonical_network_ids": [network]}]
    return manifest, providers, near


async def _approve_manual_change(fixture):
    actor = _actor()
    group = await _draft(fixture.engine, fixture.control_schema, _create("group"), actor)
    return await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, group),
        actor,
    )


def _binding_coordinates_and_evidence(record_by_field):
    return {
        name: record_by_field[name]
        for name in (
            "binding_id",
            "source_system",
            "source_id",
            "dataset_schema",
            "dataset_id",
            "producer_id",
            "edition_id",
            "source_key",
            "source_scope_json",
            "evidence_id",
            "evidence_sha256",
        )
    }


async def _rebind_approved_network(fixture):
    """Change the actual approved mapping; enrollment exposes only native row identity."""
    actor = _actor()
    network = await _draft(fixture.engine, fixture.control_schema, _create("network"), actor)
    original = fixture.enrollment[0]
    record_by_field = json.loads(
        await fixture.connection.fetchval(
            f'SELECT record_json FROM "{fixture.control_schema}".registry_approved_record '
            "WHERE approved_revision=$1 AND record_kind='network_binding' AND record_key=$2",
            fixture.approved.approved_revision,
            original["binding_id"],
        )
    )
    batch = await _reviewed_binding(
        fixture.connection,
        fixture.control_schema,
        actor,
        [
            {
                **_binding_coordinates_and_evidence(record_by_field),
                "network_id": network["record_id"],
                "operation": "rebind",
                "expected_revision": original["binding_revision"],
                "expected_network_id": original["network_id"],
            }
        ],
    )
    approved = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, network, batch["records"][0]),
        actor,
    )
    requested_scopes = [
        {**original, "binding_revision": original["binding_revision"] + 1, "network_id": network["record_id"]}
    ]
    async with registry_company_approval_transaction(fixture.outer, control_schema=fixture.control_schema) as session:
        driver = await native_driver(session)
        fixture.approved = await pin_approved_membership_source(
            driver,
            approved_revision=approved["approved_revision"],
            control_schema=fixture.control_schema,
        )
        bindings = await read_approved_refresh_bindings(
            driver, fixture.approved, requested_scopes, control_schema=fixture.control_schema
        )
    fixture.enrollment = _enrollment(bindings[0], fixture.case.recipe)
    return original["network_id"]


async def _assert_separate_custody(fixture, candidate_id):
    row = await fixture.connection.fetchrow(
        f"SELECT candidate.*,namespace.nspowner::regrole::text AS owner "
        f'FROM "{fixture.control_schema}".network_membership_candidate candidate '
        "JOIN pg_namespace namespace ON namespace.nspname=candidate.schema_name WHERE candidate_id=$1",
        UUID(candidate_id),
    )
    assert row["owner"] == fixture.roles_by_kind["custody"]
    candidate_by_field = dict(row)
    root = verify_source_recipe_metadata(candidate_by_field).inventory.root
    assert (root.entry_count, root.contribution_count, root.derivation_count) == (1, 0, 1)
    office = verify_initial_source_office_receipt(candidate_by_field)
    assert (office["revision"], office["office_rows"]) == (4, 1)
    assert not await fixture.connection.fetchval(
        "SELECT pg_has_role($1::name,$2::name,'SET')", fixture.roles_by_kind["execution"], row["owner"]
    )


async def test_accepted_aca_refresh_publication_replay_and_pinned_second_generation(monkeypatch, record_property):
    async with refresh_execution(monkeypatch) as fixture:
        observed_at = perf_counter()
        desired = await fixture.bundle.inputs_service()
        record_property("initial_getter_seconds", perf_counter() - observed_at)
        assert desired["ready_address_generation"] == desired["address_generation"]
        assert desired["accepted_sources"][0]["capture_generation"] == fixture.case.recipe.source_pin.generation_id
        assert desired["expected_serving_generation"] == 0
        command = fixture.command(desired)
        before = await _heads(fixture)
        queued = await fixture.enqueue(command)
        assert await _heads(fixture) == before
        completed = await fixture.consume(queued["request_id"])
        assert completed["state"] == "completed" and completed["result"]["approved_revision"] == before[1]
        assert fixture.calls.count(COMMIT_AUTHORITY_PATH) == 1
        assert await _heads(fixture) == before
        assert await _counts(fixture) == (1, 1, 1, 1)
        await _assert_separate_custody(fixture, completed["result"]["candidate_id"])
        request = await fixture.connection.fetchrow(
            f'SELECT * FROM "{fixture.control_schema}".registry_publication_request WHERE request_id=$1',
            UUID(queued["request_id"]),
        )
        assert validated_membership_refresh_request_status(request) == command
        await _assert_replay_and_second_generation(fixture, queued, command, record_property)


async def _assert_replay_and_second_generation(fixture, queued, command, record_property):
    async with fixture.case.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        reader = await native_driver(session)
        first, first_providers, first_near = await _read(fixture, reader)
        observed_at = perf_counter()
        unchanged = await fixture.bundle.inputs_service()
        record_property("unchanged_getter_seconds", perf_counter() - observed_at)
        assert unchanged["published_inputs_digest"] == command["idempotency_key"].split(":", 1)[1]
        assert unchanged["refresh_request"]["request_id"] == queued["request_id"]
        assert unchanged["expected_serving_generation"] == 0
        counts = await _counts(fixture)
        membership_before = await _storage_rows(fixture, first.schema_name)
        assert await fixture.enqueue(command) == {**queued, "state": "completed", "replayed": True}
        assert await fixture.consume(queued["request_id"]) is None
        assert await _counts(fixture) == counts
        assert await _storage_rows(fixture, first.schema_name) == membership_before
        original_network = await _rebind_approved_network(fixture)
        changed = await fixture.bundle.inputs_service()
        assert changed["expected_serving_generation"] == first.generation_id
        assert changed["approved_revision"] > command["expected_approved_revision"]
        second = await fixture.enqueue(fixture.command(changed))
        second_result = await fixture.consume(second["request_id"])
        assert second_result["result"]["previous_generation_id"] == first.generation_id
        assert second_result["result"]["generation_id"] != first.generation_id
        still_first, providers, near = await _read(fixture, reader, network_id=original_network)
        assert still_first == first and providers == first_providers and near == first_near
    async with fixture.api.transaction(isolation="repeatable_read", readonly=True):
        latest, _, _ = await _read(fixture, fixture.api)
        retained, _, _ = await _read(fixture, fixture.api, first.generation_id, network_id=original_network)
        with network_address_read_scope(latest, (original_network,)) as scope:
            removed = await read_network_provider_page(fixture.api, scope, control_schema=fixture.control_schema)
        assert removed["total_count"] == 0
    assert latest.generation_id == second_result["result"]["generation_id"] > retained.generation_id
    assert await _counts(fixture) == (2, 2, 2, 2)


async def _damage_after_seal(fixture, damage, target):
    schema = '"' + fixture.control_schema + '"'
    if damage == "recipe":
        await fixture.connection.execute(
            f"UPDATE {schema}.network_membership_candidate SET source_recipes_json='[]'::jsonb WHERE candidate_id=$1",
            UUID(target.candidate_id),
        )
    elif damage == "head":
        await _approve_manual_change(fixture)
    elif damage == "lease":
        await fixture.connection.execute(
            f"UPDATE {schema}.registry_publication_request SET lease_expires_at=now()-interval '1 second' WHERE state='running'"
        )


@pytest.mark.parametrize("damage", ("authority", "vector", "recipe", "head", "lease", "cancel"))
async def test_native_refresh_refusal_never_partially_publishes(monkeypatch, damage):
    async with refresh_execution(monkeypatch) as fixture:
        desired = await fixture.bundle.inputs_service()
        command = fixture.command(desired)
        if damage == "vector":
            command["accepted_sources"] = [{**command["accepted_sources"][0], "capture_generation": "0" * 64}]
            command["accepted_source_vector_sha256"] = accepted_membership_source_sha256(command["accepted_sources"])
        queued = await fixture.enqueue(command)
        fixture.denied_commit = damage == "authority"
        fixture.cancel_commit = damage == "cancel"
        if damage in {"recipe", "head", "lease"}:

            async def after_seal(target):
                await _damage_after_seal(fixture, damage, target)

            fixture.after_seal = after_seal
        expected = {"lease": RegistryApprovalConflict, "cancel": asyncio.CancelledError}.get(
            damage, (ValueError, RuntimeError)
        )
        with pytest.raises(expected):
            await fixture.consume(queued["request_id"])
        state, result_json = await fixture.connection.fetchrow(
            f'SELECT state,result_json FROM "{fixture.control_schema}".registry_publication_request WHERE request_id=$1',
            UUID(queued["request_id"]),
        )
        assert state in {"running", "rejected"}
        assert json.loads(result_json or "null") == (
            {"error": {"code": "registry_publication_rejected"}} if state == "rejected" else None
        )
        assert (
            await fixture.connection.fetchval(
                f'SELECT generation_id FROM "{fixture.control_schema}".network_serving_control'
            )
            is None
        )
        assert (await _counts(fixture))[2] == 0
        assert (await _counts(fixture))[1] == (0 if damage == "vector" else 1)
        assert fixture.calls.count(COMMIT_AUTHORITY_PATH) == (1 if damage in {"authority", "cancel"} else 0)
        async with fixture.connection.transaction():
            await fixture.connection.execute(
                f'LOCK TABLE "{fixture.case.names.source_schema}".mrf_publication_receipt IN ROW EXCLUSIVE MODE NOWAIT'
            )
        resources = fixture.bundle.inputs_service.resources
        assert all(
            factory.kw["bind"].pool.checkedout() == 0
            for factory in (
                resources.fence_sessions,
                resources.source_sessions,
                resources.address_sessions,
                resources.readers.aca_head_fence_sessions,
            )
        )
