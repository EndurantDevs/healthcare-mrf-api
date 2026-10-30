# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A real admitted import keeps the searchable catalog and retained serving data."""

import hashlib
import json
import os
import subprocess
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import UUID, uuid4, uuid5

import asyncpg
import pytest
from aiohttp import web
from sanic import Blueprint, Sanic
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from api.endpoint import network_catalog
from api.network_address_scope import network_address_read_scope
from api.network_provider_read import read_network_provider_page
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_catalog_policy import POLICY_V2
from process.registry_fhir_catalog_identity import ALIAS_SCOPE
from process.registry_fhir_catalog_worker import run_fhir_catalog_sync
from tests import cms_npd_admission_postgres_support as imported
from tests import cms_registry_complete_publication_postgres_support as native
from tests.fhir_catalog_serving_rollover_support import (
    OLD_PROVIDER,
    approve_edition,
    directory_reads,
    grant_pair_read,
    publish_directory,
    separate_source_provider,
)
from tests.fhir_catalog_sync_integration_support import (
    adopt_existing_network,
    import_source,
    native_catalog_case,
    source_resources,
)
from tests.test_network_registry_cms_prepared_pair_postgres import (
    _origin_native_acl_refusals,
    _origin_pair_read,
    _origin_set_authority_refusals,
    _origin_source_authority,
    _origin_wider_reads_refused,
)
from tests.test_registry_approval_store_postgres import _approve, _command, _create, _draft
from tests.test_registry_fhir_catalog_enrichment import COMPANY, GROUP
from tests.test_registry_publication_worker_permissions_postgres import _login as worker_login


@asynccontextmanager
async def core_server(case):
    app = Sanic("catalog_integration_" + uuid4().hex)
    app.blueprint(Blueprint.group([network_catalog.blueprint], version_prefix="/api/v"))
    network_catalog.register_network_catalog_private_responses(app)
    engine = create_async_engine(case.reader_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    @app.middleware("request")
    async def open_session(request):
        request.ctx.sa_session = sessions()

    @app.middleware("response")
    async def close_session(request, _response):
        if hasattr(request.ctx, "sa_session"):
            await request.ctx.sa_session.close()

    async def forward(request):
        _, response = await app.asgi_client.post(request.path, data=await request.read(), headers=request.headers)
        return web.Response(status=response.status, body=response.body, headers=dict(response.headers))

    bridge = web.Application()
    bridge.router.add_post("/{path:.*}", forward)
    runner = web.AppRunner(bridge)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        yield app, "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
    finally:
        await runner.cleanup()
        await engine.dispose()


def policy(query):
    policy_by_field = {
        "contract_version": POLICY_V2,
        "client_id": "client_42",
        "policy_revision": 9,
        "organization_visibility_default": "system_wide",
        "excluded_network_ids": [],
        "excluded_company_ids": [],
        "excluded_group_ids": [],
    }
    policy_by_field["access_scope_sha256"] = hashlib.sha256(
        json.dumps(
            policy_by_field,
            sort_keys=True,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return {**policy_by_field, "policy_revision": "9", "query": query}


async def gateway_probe(url, network_id, status):
    command = os.getenv("NETWORK_CATALOG_GATEWAY_TEST_COMMAND")
    if command is None:
        return
    import asyncio

    environment_by_name = {
        **os.environ,
        "NETWORK_CATALOG_TEST_CORE_URL": url,
        "NETWORK_CATALOG_TEST_NETWORK_ID": str(network_id),
        "NETWORK_CATALOG_TEST_STATUS": status,
    }
    result = await asyncio.to_thread(
        subprocess.run, json.loads(command), env=environment_by_name, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stdout + result.stderr


async def serving_page(case, network_id):
    connection = await asyncpg.connect(
        case.reader_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            manifest = await resolve_network_serving_manifest(connection, control_schema=case.fixture.control_schema)
            with network_address_read_scope(manifest, (network_id,)) as scope:
                return await read_network_provider_page(connection, scope, control_schema=case.fixture.control_schema)
    finally:
        await connection.close()


def test_source_editions_witness_old_membership_before_disappearance():
    first = source_resources(imported._resource_rows, "one", retain_old=True)
    second = source_resources(imported._resource_rows, "two", retain_old=False)
    assert first["Endpoint"] == second["Endpoint"]
    assert {row["id"] for row in first["Organization"]} == {"insurer-1", "network-new", "network-old"}
    assert {row["id"] for row in second["Organization"]} == {"insurer-1", "network-auto", "network-new"}
    assert {plan["id"] for plan in first["InsurancePlan"]} == {"plan-network-new", "plan-network-old"}
    automatic_plan = next(plan for plan in second["InsurancePlan"] if plan["id"] == "plan-network-auto")
    assert automatic_plan["name"] == "Gold network-auto"
    assert automatic_plan["ownedBy"] == {"reference": "Organization/insurer-1"}
    assert automatic_plan["network"] == [{"reference": "Organization/network-auto"}]
    first_memberships = {
        (role["network"][0]["reference"], role["location"][0]["reference"]) for role in first["PractitionerRole"]
    }
    assert first_memberships == {
        ("Organization/network-old", "Location/site-2"),
        ("Organization/network-new", "Location/site-3"),
    }
    assert {role["network"][0]["reference"] for role in second["PractitionerRole"]} == {"Organization/network-new"}
    normalized_rows = [
        imported.fhir._parse_practitioner_role_resource(role, {"source_id": "synthetic-source"})[1]
        for role in first["PractitionerRole"]
    ]
    assert [row["network_refs"] for row in normalized_rows] == [
        ["Organization/network-old"],
        ["Organization/network-new"],
    ]
    assert all(row["active"] is False for row in first["OrganizationAffiliation"])
    assert first["Location"] == second["Location"] and len(second["Location"]) == 2


def test_source_editions_keep_disappeared_provider_out_of_the_new_input(monkeypatch):
    separate_source_provider(monkeypatch)
    from tests import fhir_catalog_sync_integration_support as integration

    first = integration.source_resources(imported._resource_rows, "one", retain_old=True)
    second = integration.source_resources(imported._resource_rows, "two", retain_old=False)
    assert first["Endpoint"] == second["Endpoint"]
    assert first["PractitionerRole"][0]["practitioner"]["reference"] == "Practitioner/" + OLD_PROVIDER
    assert {row["id"] for row in first["Practitioner"]} == {"1234567893", OLD_PROVIDER}
    assert {row["id"] for row in second["Practitioner"]} == {"1234567893"}


async def _first_import(case, tmp_path):
    new_network = await _draft(case.fixture.engine, case.fixture.control_schema, _create("network"), case.seed.actor)
    await _approve(
        case.fixture.connection,
        case.fixture.control_schema,
        await _command(case.fixture.connection, case.fixture.control_schema, new_network),
        case.seed.actor,
    )
    admission = await import_source(case, tmp_path / "first", retain_old=True, revision="one")
    await adopt_existing_network(case)
    case.old_id, case.new_id = int(case.seed.network["record_id"]), int(new_network["record_id"])
    proof, _ = await native.committed_publication_proof(case.administrator, admission)
    return proof


def _observe_origin_metadata_cut(monkeypatch):
    """Observe the real initial grant and its savepoint rollback on the original admin."""
    import asyncio

    from process import provider_directory_cms_receipt_custody as custody
    from tests import nppes_candidate_postgres_support as fixture

    original = fixture.provision_cms_origin_metadata_readers
    assert original is custody.provision_cms_origin_metadata_readers
    cuts = []

    async def observe(connection, source_policy, **options):
        assert not cuts and connection.is_in_transaction()
        task = asyncio.current_task()
        actor_sql = "SELECT session_user::text,current_user::text,pg_backend_pid()"
        actor = await connection.fetchrow(actor_sql)
        receipt_options_by_name = {name: options[name] for name in ("migration_role", "metadata_read_roles")}
        before = await custody.capture_cms_serving_receipt_custody(connection, source_policy, **receipt_options_by_name)
        recipient_oids = {oid for _name, oid in before["metadata_readers"]["roles"]}
        assert not any(edge[1] in recipient_oids for edge in before["catalog"]["column_acl"])
        rows_sql = "SELECT * FROM mrf.provider_directory_cms_serving_receipt ORDER BY receipt_id"
        receipt_rows = await connection.fetch(rows_sql)
        savepoint = connection.transaction()
        await savepoint.start()
        try:
            rolled_back = await original(connection, source_policy, **options)
            custody.require_cms_serving_receipt_read_delta(
                before, rolled_back, source_policy, metadata_read_roles=options["metadata_read_roles"]
            )
            assert await connection.fetch(rows_sql) == receipt_rows
        finally:
            await savepoint.rollback()
        assert (
            await custody.capture_cms_serving_receipt_custody(connection, source_policy, **receipt_options_by_name)
            == before
        )
        assert await connection.fetch(rows_sql) == receipt_rows
        after = await original(connection, source_policy, **options)
        assert after == rolled_back and await connection.fetch(rows_sql) == receipt_rows
        assert after["catalog"]["column_acl"] != before["catalog"]["column_acl"]
        custody.require_cms_serving_receipt_read_delta(
            before, after, source_policy, metadata_read_roles=options["metadata_read_roles"]
        )
        assert asyncio.current_task() is task and await connection.fetchrow(actor_sql) == actor
        cuts.append(
            {
                "policy": source_policy,
                "actor": dict(actor),
                "options": receipt_options_by_name,
                "before": before,
                "after": after,
            }
        )
        return after

    monkeypatch.setattr(fixture, "provision_cms_origin_metadata_readers", observe)
    return cuts


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [False, True])
async def test_origin_metadata_cut_observer_rolls_back_before_final_call(monkeypatch, failure):
    """Only observer orchestration is doubled; this test establishes no native authority."""
    from process import provider_directory_cms_receipt_custody as custody
    from tests import nppes_candidate_postgres_support as fixture

    before_by_field = {"metadata_readers": {"roles": [["fixture_reader", 42]]}, "catalog": {"column_acl": []}}
    after_by_field = {"catalog": {"column_acl": [[1, 42, 99, "SELECT", False]]}}
    grant = AsyncMock(side_effect=RuntimeError("grant refused") if failure else None, return_value=after_by_field)
    capture = AsyncMock(return_value=before_by_field)
    savepoint = SimpleNamespace(start=AsyncMock(), rollback=AsyncMock())
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=lambda: savepoint,
        fetchrow=AsyncMock(return_value={"session_user": "fixture_admin", "current_user": "fixture_admin"}),
        fetch=AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(fixture, "provision_cms_origin_metadata_readers", grant)
    monkeypatch.setattr(custody, "provision_cms_origin_metadata_readers", grant)
    monkeypatch.setattr(custody, "capture_cms_serving_receipt_custody", capture)
    monkeypatch.setattr(custody, "require_cms_serving_receipt_read_delta", Mock())
    cuts = _observe_origin_metadata_cut(monkeypatch)
    options_by_name = {"migration_role": "fixture_migrator", "metadata_read_roles": ("fixture_reader",)}
    policy = object()
    if failure:
        with pytest.raises(RuntimeError, match="grant refused"):
            await fixture.provision_cms_origin_metadata_readers(connection, policy, **options_by_name)
        assert cuts == [] and grant.await_count == 1
    else:
        assert (
            await fixture.provision_cms_origin_metadata_readers(connection, policy, **options_by_name) is after_by_field
        )
        assert len(cuts) == 1 and cuts[0]["before"] is before_by_field and cuts[0]["after"] is after_by_field
        assert grant.await_count == 2 and capture.await_count == 2
    savepoint.rollback.assert_awaited_once()
    assert all(call.args == (connection, policy) and call.kwargs == options_by_name for call in grant.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [asyncpg.InsufficientPrivilegeError, asyncpg.UndefinedTableError])
async def test_wider_read_refusal_uses_existing_source_payload(error):
    """A missing relation must fail the negative test rather than count as a denial."""
    connection = SimpleNamespace(execute=AsyncMock(side_effect=error), transaction=MagicMock())
    if error is asyncpg.UndefinedTableError:
        with pytest.raises(asyncpg.UndefinedTableError):
            await _origin_wider_reads_refused(connection, "fixture_owner")
        return
    await _origin_wider_reads_refused(connection, "fixture_owner")
    assert connection.execute.await_count == 5
    assert connection.execute.await_args_list[2].args == ("SELECT * FROM mrf.provider_directory_dataset_resource",)


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_metadata_actor_context_preserves_optional_lifetimes(monkeypatch, enabled):
    """Check context ordering only; no identity, grant or native authority is synthesized."""
    from tests import fhir_catalog_sync_integration_support as fixture

    events = []
    connection = object()

    @asynccontextmanager
    async def actor(original, kind):
        assert original is connection
        events.append("enter-" + kind)
        try:
            yield kind
        finally:
            events.append("exit-" + kind)

    monkeypatch.setattr(fixture, "_restricted_artifact_roles", lambda original: actor(original, "artifact"))
    monkeypatch.setattr(fixture, "_worker_execution_identity", lambda original: actor(original, "execution"))
    async with fixture._catalog_metadata_actors(connection, enabled) as actors:
        assert (actors.artifact, actors.execution) == (("artifact", "execution") if enabled else (None, None))
        assert events == (["enter-artifact", "enter-execution"] if enabled else [])
    assert events == (["enter-artifact", "enter-execution", "exit-execution", "exit-artifact"] if enabled else [])


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "dependency", "artifact"])
async def test_initial_address_preparation_uses_original_custodian_and_registered_cleanup(monkeypatch, failure):
    """Verify fixture ordering only; doubled I/O cannot establish native custody."""
    from process.network_address_projection import PinnedAddressSource
    from tests import fhir_catalog_sync_integration_support as fixture

    request = uuid4()
    roles = ("fixture_execution", "fixture_reader")
    custodian = SimpleNamespace(transaction=MagicMock())
    case = SimpleNamespace(
        fixture=SimpleNamespace(
            base=object(), roles={"reader": roles[1]}, control_schema="fixture_control", composition_ids=[]
        ),
        address_custody=SimpleNamespace(custodian=custodian, roles={"custody": "fixture_custody"}),
        seed=SimpleNamespace(revision=7),
    )
    dependency = SimpleNamespace(
        address_source=PinnedAddressSource("network_composition_fixture", "entity_address_unified", "a" * 64)
    )
    artifact = object()
    prepare_dependency = AsyncMock(
        return_value=dependency, side_effect=RuntimeError("dependency") if failure == "dependency" else None
    )
    prepare_artifact = AsyncMock(
        return_value=artifact, side_effect=RuntimeError("artifact") if failure == "artifact" else None
    )
    pin = AsyncMock(return_value=SimpleNamespace(generation_id="b" * 64))
    monkeypatch.setattr(fixture, "prepare_custom_address_source", prepare_dependency)
    monkeypatch.setattr(fixture, "pin_approved_membership_source", pin)
    monkeypatch.setattr(fixture, "prepare_registry_composition_address_artifact", prepare_artifact)
    if failure:
        with pytest.raises(RuntimeError, match=failure):
            await fixture._initial_address_artifact(case, request, roles)
    else:
        assert await fixture._initial_address_artifact(case, request, roles) is artifact
    assert case.fixture.composition_ids[0] == UUID(prepare_dependency.await_args.kwargs["composition_id"])
    assert prepare_dependency.await_args.args == (custodian, case.fixture.base)
    assert prepare_dependency.await_args.kwargs["runtime_roles"] == (roles[1],)
    custodian.transaction.assert_called_once_with(isolation="repeatable_read")
    if failure == "dependency":
        pin.assert_not_awaited()
        prepare_artifact.assert_not_awaited()
        return
    options_by_name = prepare_artifact.await_args.kwargs
    assert prepare_artifact.await_args.args == (custodian,)
    assert options_by_name["request_id"] == request and options_by_name["expected_head"] == 0
    assert options_by_name["source_manifest"] is None and options_by_name["approved_revision"] == 7
    assert options_by_name["address_sources"].base_source is dependency.address_source
    assert options_by_name["custody"].runtime_roles == roles
    assert options_by_name["custody"].owner_role == "fixture_custody"
    assert case.fixture.composition_ids[1] == uuid5(request, "address:" + "b" * 64)


def _bootstrap_case():
    """Use distinct synthetic fixture actors without a database identity or authority."""
    return SimpleNamespace(
        fixture=SimpleNamespace(
            connection=object(), roles={"reader": "fixture_reader"}, control_schema="fixture_control"
        ),
        worker=SimpleNamespace(
            roles_by_kind={kind: "fixture_" + kind for kind in ("queue", "physical", "execution", "api", "lifecycle")}
        ),
        address_custody=SimpleNamespace(
            roles={"execution": "fixture_artifact_execution", "custodian": "fixture_custodian"}
        ),
        metadata_roles={
            kind: "fixture_catalog_" + kind for kind in ("owner", "loader", "sealer", "lifecycle", "execution")
        },
        runtime_roles=(
            "fixture_api",
            "fixture_artifact_execution",
            "fixture_catalog_execution",
            "fixture_catalog_lifecycle",
            "fixture_catalog_loader",
            "fixture_catalog_owner",
            "fixture_catalog_sealer",
            "fixture_execution",
            "fixture_lifecycle",
            "fixture_physical",
            "fixture_reader",
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("serving_custody,failure", [(False, False), (True, False), (True, True)])
async def test_initial_manifest_seals_before_publication_and_preserves_default(monkeypatch, serving_custody, failure):
    """Exercise callback wiring and refusal propagation; no seal or Reader is fabricated."""
    from tests import fhir_catalog_sync_integration_support as fixture

    events = []
    case = _bootstrap_case()
    writer, artifact, manifest = object(), object(), object()
    prepare_artifact = AsyncMock(return_value=artifact)
    qualify = AsyncMock(return_value=("exact_candidate", "exact_addresses"))

    @asynccontextmanager
    async def login(worker, kind):
        assert worker is case.worker and kind == "execution"
        events.append("login")
        try:
            yield writer
        finally:
            events.append("logout")

    async def seal(original, target, **options_by_name):
        assert original is case.fixture.connection and target == "exact_candidate"
        assert options_by_name["runtime_roles"] == prepare_artifact.await_args.args[2]
        assert await options_by_name["authorize"](original, target)
        assert not await options_by_name["authorize"](original, "different_candidate")
        events.append("seal")
        if failure:
            raise RuntimeError("seal refused")

    async def publish(execution, target, addresses, **options_by_name):
        assert (execution, target, addresses) == (writer, "exact_candidate", "exact_addresses")
        events.append("prepared")
        if options_by_name["seal_candidate"] is not None:
            await options_by_name["seal_candidate"](target)
        events.append("published")
        return manifest

    monkeypatch.setattr(fixture, "_login", login)
    monkeypatch.setattr(fixture, "_initial_address_artifact", prepare_artifact)
    monkeypatch.setattr(fixture, "_native_qualified_candidate", qualify)
    monkeypatch.setattr(fixture, "seal_network_candidate_custody", seal)
    monkeypatch.setattr(fixture, "prepare_and_publish_network_candidate", publish)
    if failure:
        with pytest.raises(RuntimeError, match="seal refused"):
            await fixture._publish_initial_manifest(case, serving_custody=serving_custody)
        assert events == ["login", "prepared", "seal", "logout"]
    else:
        assert await fixture._publish_initial_manifest(case, serving_custody=serving_custody) is manifest
        assert events == ["login", "prepared", *(["seal"] if serving_custody else []), "published", "logout"]
    if not serving_custody:
        prepare_artifact.assert_not_awaited()
        assert qualify.await_args.kwargs == {}
        return
    assert prepare_artifact.await_args.args[2] == case.runtime_roles
    assert qualify.await_args.kwargs == dict(
        request_id=prepare_artifact.await_args.args[1],
        runtime_roles=("fixture_reader",),
        prepared_address_artifact=artifact,
    )


async def _first_pair_origin_metadata(case, pair, cuts):
    """Verify only this genuine witnessed pair with the fixture's original E/K logins."""
    roles = (
        case.address_custody.roles["execution"],
        case.address_custody.roles["custodian"],
        case.metadata_roles["loader"],
    )
    metadata_read_roles = tuple(sorted((*roles[:2], case.execution_actor.role)))
    expected_oid_by_role = {**case.artifact_actors.role_oids, case.execution_actor.role: case.execution_actor.oid}
    assert len(cuts) == 1 and cuts[0]["policy"] is case.component.policy
    assert cuts[0]["options"]["metadata_read_roles"] == metadata_read_roles
    assert cuts[0]["after"]["metadata_readers"]["roles"] == [
        [role, expected_oid_by_role[role]] for role in metadata_read_roles
    ]
    assert pair.witness_json is not None and pair.address_receipt.tables[0].row_count > 0
    before = await _origin_source_authority(case.administrator)
    for role, connection in zip(
        roles[:2], (case.address_custody.execution, case.address_custody.custodian), strict=True
    ):
        assert await connection.fetchval("SELECT session_user=current_user AND current_user::text=$1", role)
        await _origin_pair_read(connection, pair, granted=True)
        await _origin_wider_reads_refused(connection, case.component.owner)
    async with worker_login(case.worker, "execution") as execution:
        assert (
            await execution.fetchval("SELECT oid FROM pg_roles WHERE rolname=current_user") == case.execution_actor.oid
        )
        await _origin_pair_read(execution, pair, granted=True)
        await _origin_wider_reads_refused(execution, case.component.owner)
    assert case.config.loader_url.username == roles[2]
    undeclared = await asyncpg.connect(
        case.config.loader_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    try:
        assert await undeclared.fetchval("SELECT session_user=current_user AND current_user::text=$1", roles[2])
        await _origin_pair_read(undeclared, pair, granted=False)
        await _origin_wider_reads_refused(undeclared, case.component.owner)
    finally:
        await undeclared.close()
    await _origin_native_acl_refusals(case.administrator, roles, metadata_read_roles=metadata_read_roles)
    await _origin_set_authority_refusals(case.administrator, roles, metadata_read_roles=metadata_read_roles)
    after = await _origin_source_authority(case.administrator)
    assert after == before
    case.origin_metadata_custody = {"initial_grant": cuts[0], "source_before": before, "source_after": after}


async def _first_directory(case, pair, origin_cuts):
    await grant_pair_read(case, pair)
    await _first_pair_origin_metadata(case, pair, origin_cuts)
    approved = await approve_edition(case, pair, {"network-old": case.old_id, "network-new": case.new_id})
    async with case.fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        predecessor = await resolve_network_serving_manifest(
            case.fixture.connection, control_schema=case.fixture.control_schema
        )
    case.first_manifest = await publish_directory(case, pair, approved, predecessor)
    _, case.old_provider, case.old_directory_reads = await directory_reads(case, case.old_id)
    assert case.old_provider["locations"][0]["second_line"] == "Suite 2"
    assert OLD_PROVIDER in {provider["provider_id"] for provider in case.old_directory_reads["page"]["providers"]}
    case.first_pair = pair


async def _read_catalog(app):
    query_by_field = {"view": "fhir-plans-v1", "company_id": COMPANY, "plan_search": "Gold"}
    _, response = await app.asgi_client.post(
        "/api/v1/registry/serving/catalog",
        json=policy(query_by_field),
        headers={"Authorization": "Bearer synthetic-catalog-control-token"},
    )
    assert response.status == 200, response.json
    return response.json


async def _initial_catalog(case, app, url):
    assert (await run_fhir_catalog_sync(case.config))["state"] == "completed"
    initial = await _read_catalog(app)
    assert initial["total"] == 2
    case.catalog_ids = {entry["network_id"] for entry in initial["items"]}
    assert case.catalog_ids == {case.old_id, case.new_id}
    case.old_catalog = next(entry for entry in initial["items"] if entry["network_id"] == case.old_id)
    assert case.old_catalog["directory_available"] is True
    assert any(
        edge["group"] and edge["group"]["group_id"] == GROUP
        for edge in case.old_catalog["matching_relationships"]["items"]
    )
    await gateway_probe(url, case.old_id, "current")
    assert (await run_fhir_catalog_sync(case.config))["state"] == "unchanged"
    assert await _read_catalog(app) == initial


def _directory_content(responses_by_kind):
    from copy import deepcopy

    content_by_kind = deepcopy(responses_by_kind)
    for name in ("page", "detail", "office"):
        content_by_kind[name].pop("generation_id")
    return content_by_kind


async def _successor_input(case, pair):
    assert pair.recipe.source_pin.endpoint_id == case.first_pair.recipe.source_pin.endpoint_id
    assert pair.recipe.source_pin.dataset_id != case.first_pair.recipe.source_pin.dataset_id
    organizations = await case.fixture.connection.fetch(
        f'SELECT resource_id FROM "{pair.recipe.source_pin.read_schema_name}".provider_directory_dataset_resource '
        "WHERE dataset_id=$1 AND resource_type='Organization' ORDER BY resource_id",
        pair.recipe.source_pin.dataset_id,
    )
    assert [organization["resource_id"] for organization in organizations] == [
        "insurer-1",
        "network-auto",
        "network-new",
    ]
    assert not await case.fixture.connection.fetchval(
        f'SELECT EXISTS(SELECT FROM "{pair.address_source.schema_name}".entity_address_unified '
        "WHERE entity_type='npi' AND entity_id=$1)",
        OLD_PROVIDER,
    )


async def _successor_directory(case, pair):
    await _successor_input(case, pair)
    await grant_pair_read(case, pair)
    case.second_approved = await approve_edition(case, pair, {"network-new": case.new_id})
    case.second_manifest = await publish_directory(case, pair, case.second_approved, case.first_manifest)
    # These actual serving paths must work before catalog sync can react.
    current, _, after = await directory_reads(case, case.old_id, provider=case.old_provider)
    assert current.generation_id == case.second_manifest.generation_id != case.first_manifest.generation_id
    assert _directory_content(after) == _directory_content(case.old_directory_reads)
    current_providers = await serving_page(case, case.new_id)
    assert {provider["provider_id"] for provider in current_providers["providers"]} == {"1234567893"}
    assert {
        location["second_line"] for provider in current_providers["providers"] for location in provider["locations"]
    } == {"Suite 2", "Suite 3"}
    case.pre_auto_ids = await _canonical_ids(case)
    case.auto_dataset_id = pair.recipe.source_pin.dataset_id
    namespace = case.fixture.control_schema
    assert await case.fixture.connection.fetchval(
        f'SELECT NOT EXISTS(SELECT FROM "{namespace}".registry_network_binding '
        "WHERE source_system='fhir' AND source_key='network-auto') "
        f'AND NOT EXISTS(SELECT FROM "{namespace}".network_registry_alias '
        "WHERE source_system='fhir' AND alias_type='fhir_organization' AND alias_value='network-auto' AND scope_key=$1)",
        ALIAS_SCOPE,
    )


async def _canonical_ids(case):
    identities = await case.fixture.connection.fetch(
        f'SELECT network_id FROM "{case.fixture.control_schema}".network_registry_identity'
    )
    return {identity["network_id"] for identity in identities}


def _automatic_network_witness(catalog):
    witnesses = [
        (network, binding)
        for network in catalog["items"]
        for binding in network["source_bindings"]["items"]
        if binding["source_system"] == "fhir"
        and binding["source_key"] == "network-auto"
        and binding["source_scope"]["organization_id"] == "network-auto"
    ]
    assert len(witnesses) == 1
    return witnesses[0]


@pytest.mark.parametrize("scope_evidence", [{"source_generation": "a" * 64}, {"capture_sha256": "b" * 64}])
def test_automatic_network_witness_requires_unique_source_identity(scope_evidence):
    binding_by_field = {
        "source_system": "fhir",
        "source_key": "network-auto",
        "source_scope": {"organization_id": "network-auto", **scope_evidence},
    }
    automatic_by_field = {
        "network_id": 7,
        "display_name": "Unrelated label",
        "source_bindings": {"items": [binding_by_field]},
    }
    same_name_by_field = {
        "network_id": 8,
        "display_name": "network-auto",
        "source_bindings": {"items": [{**binding_by_field, "source_key": "network-other"}]},
    }
    assert _automatic_network_witness({"items": [same_name_by_field, automatic_by_field]}) == (
        automatic_by_field,
        binding_by_field,
    )
    for items in ([same_name_by_field], [automatic_by_field, automatic_by_field]):
        with pytest.raises(AssertionError):
            _automatic_network_witness({"items": items})


async def _assert_automatic_allocation(case, catalog):
    automatic_network, automatic_binding = _automatic_network_witness(catalog)
    automatic_id = automatic_network["network_id"]
    assert type(automatic_id) is int and 0 < automatic_id <= 2147483647
    assert automatic_id not in case.pre_auto_ids
    assert automatic_id == getattr(case, "auto_id", automatic_id)
    case.auto_id = automatic_id
    assert automatic_network["directory_available"] is False and automatic_network["archived"] is False
    assert automatic_binding["dataset_id"] == case.auto_dataset_id
    assert len(catalog["items"]) == catalog["total"] == 3
    assert {entry["network_id"] for entry in catalog["items"]} == case.catalog_ids | {automatic_id}
    assert await _canonical_ids(case) == case.pre_auto_ids | {automatic_id}
    namespace = case.fixture.control_schema
    assert await case.fixture.connection.fetchval(
        f'SELECT EXISTS(SELECT FROM "{namespace}".network_registry_alias '
        "WHERE source_system='fhir' AND source_id=$1 AND alias_type='fhir_organization' "
        "AND alias_value='network-auto' AND network_id=$2 AND scope_key=$3) "
        f'AND NOT EXISTS(SELECT FROM "{namespace}".network_registry_record WHERE network_id=$2) '
        f'AND NOT EXISTS(SELECT FROM "{namespace}".registry_network_binding '
        "WHERE source_system='fhir' AND source_key='network-auto')",
        automatic_binding["source_id"],
        automatic_id,
        ALIAS_SCOPE,
    )
    descriptor = await _automatic_source_descriptor(case, catalog, automatic_id, automatic_binding["binding_id"])
    assert descriptor["profile"] == "published_normalized"
    assert automatic_binding["status"] == "current"
    assert descriptor["endpoint_id"] == case.first_pair.recipe.source_pin.endpoint_id
    assert descriptor["coordinates"]["dataset_id"] == case.auto_dataset_id
    assert descriptor["coordinates"]["source_id"] == automatic_binding["source_id"]


async def _automatic_source_descriptor(case, catalog, network_id, binding_id):
    snapshot_schema = await case.fixture.connection.fetchval(
        f'SELECT schema_name FROM "{case.fixture.control_schema}".registry_fhir_catalog_snapshot '
        "WHERE metadata_generation=$1",
        catalog["metadata_generation"],
    )
    sources = await case.fixture.connection.fetch(
        f"SELECT DISTINCT source.source_generation,source.profile,source.descriptor_json::text AS descriptor "
        f'FROM "{snapshot_schema}".registry_fhir_catalog_binding binding '
        f'JOIN "{snapshot_schema}".registry_fhir_catalog_relationship edge '
        "USING(metadata_generation,network_id,binding_id,source_id) "
        f'JOIN "{snapshot_schema}".registry_fhir_catalog_source source '
        "ON source.source_generation=edge.source_generation AND source.source_id=binding.source_id "
        "WHERE binding.metadata_generation=$1 AND binding.network_id=$2 AND binding.binding_id=$3 "
        "AND binding.document_json->>'source_key'='network-auto' "
        "AND binding.document_json->'source_scope'->>'organization_id'='network-auto'",
        catalog["metadata_generation"],
        network_id,
        UUID(binding_id),
    )
    assert len(sources) == 1
    descriptor = json.loads(sources[0]["descriptor"])
    assert sources[0]["profile"] == descriptor["profile"]
    return descriptor


async def _last_good_catalog(case, app, url, manifest):
    assert (await run_fhir_catalog_sync(case.config))["state"] == "completed"
    catalog = await _read_catalog(app)
    await _assert_automatic_allocation(case, catalog)
    retained = next(entry for entry in catalog["items"] if entry["network_id"] == case.old_id)
    assert retained["directory_available"] is True and retained["archived"] is False
    assert retained["source_bindings"]["items"][0]["status"] == "last_good"
    assert retained["matching_relationships"] == case.old_catalog["matching_relationships"]
    assert catalog["generation"] == str(manifest.generation_id)
    await gateway_probe(url, case.old_id, "last_good")
    return catalog


async def _ordinary_rebuild(case, pair, app, url):
    case.rebuilt_manifest = await publish_directory(
        case, pair, case.second_approved, case.second_manifest, ordinary=True
    )
    rebuilt_pin, _, rebuilt_reads = await directory_reads(case, case.old_id, provider=case.old_provider)
    assert rebuilt_pin.generation_id == case.rebuilt_manifest.generation_id != case.second_manifest.generation_id
    assert _directory_content(rebuilt_reads) == _directory_content(case.old_directory_reads)
    rebuilt_catalog = await _last_good_catalog(case, app, url, case.rebuilt_manifest)
    assert (await run_fhir_catalog_sync(case.config))["state"] == "unchanged"
    assert await _read_catalog(app) == rebuilt_catalog
    assert await _canonical_ids(case) == case.pre_auto_ids | {case.auto_id}


async def _disappearance_and_rebuild(case, tmp_path, app, url):
    admission = await import_source(case, tmp_path / "second", retain_old=False, revision="two")
    proof, _ = await native.committed_publication_proof(case.administrator, admission)
    async with native.retained_pair(case.administrator, proof) as pair:
        await _successor_directory(case, pair)
        await _last_good_catalog(case, app, url, case.second_manifest)
        await _ordinary_rebuild(case, pair, app, url)
        (tmp_path / "serving-generations.json").write_text(
            json.dumps(
                {
                    "first": {
                        "generation": case.first_manifest.generation_id,
                        "manifest": case.first_manifest.manifest_sha256,
                    },
                    "second": {
                        "generation": case.second_manifest.generation_id,
                        "manifest": case.second_manifest.manifest_sha256,
                    },
                    "rebuild": {
                        "generation": case.rebuilt_manifest.generation_id,
                        "manifest": case.rebuilt_manifest.manifest_sha256,
                    },
                    "endpoint": case.first_pair.recipe.source_pin.endpoint_id,
                    "first_dataset": case.first_pair.recipe.source_pin.dataset_id,
                    "second_dataset": pair.recipe.source_pin.dataset_id,
                    "automatic_network": {"organization_id": "network-auto", "network_id": case.auto_id},
                },
                sort_keys=True,
            )
        )


@pytest.mark.asyncio
async def test_import_sync_query_replay_and_disappearance(monkeypatch, tmp_path):
    """One genuine E1/E2 sequence proves serving, catalog, gateway and repeat carry."""
    if not os.getenv("NETWORK_REGISTRY_TEST_DSN"):
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must select an owned PostgreSQL fixture")
    pytest.importorskip("ptg2_address_canon")
    separate_source_provider(monkeypatch)
    origin_cuts = _observe_origin_metadata_cut(monkeypatch)
    async with native_catalog_case(monkeypatch, tmp_path, serving_custody=True) as case:
        monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", case.fixture.control_schema)
        monkeypatch.setenv("HLTHPRT_REGISTRY_CONTROL_API_TOKEN", "synthetic-catalog-control-token")
        try:
            proof = await _first_import(case, tmp_path)
            async with native.retained_pair(case.administrator, proof, expected_address_rows=3) as pair:
                await _first_directory(case, pair, origin_cuts)
                async with core_server(case) as (app, url):
                    await _initial_catalog(case, app, url)
                    await _disappearance_and_rebuild(case, tmp_path, app, url)
        finally:
            for schema in getattr(case, "directory_candidates", ()):
                await case.fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
