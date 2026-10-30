# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected managed profiles through genuine ordinary Reader snapshots."""

import json
import secrets
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sanic import Blueprint, Sanic
from sqlalchemy import MetaData, event, inspect, text
from sqlalchemy.schema import CreateColumn

from api import provider_profile, provider_profile_cms, provider_profile_states
from api.endpoint import network_catalog as endpoint
from api.endpoint import npi as provider
from db.connection import Database
from db.models import NPIData, NPIDataOtherIdentifier, NPIDataTaxonomy, NPIDataTaxonomyGroup, system
from process.provider_directory_cms_serving_receipt import read_serving_receipt
from tests.test_network_catalog_routes import envelope, v2_envelope
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template
from tests.test_provider_profile_network_facts_postgres import published_profile_source as published_profile_source

pytestmark = pytest.mark.asyncio
_PATH = "/api/v1/registry/serving/providers/profile"
_AUTH = {"Authorization": "Bearer synthetic-profile-control"}
_NPI = "1000000491"


async def _current_profile_row_types(fixture):
    """Complete older fixture row types without replacing receipt-bound relations."""
    models = [
        getattr(system, "ProviderDirectory" + name)
        for name in (
            "Practitioner",
            "PractitionerRole",
            "Organization",
            "OrganizationAffiliation",
            "HealthcareService",
            "Endpoint",
        )
    ]
    added_columns_by_table = {}
    async with fixture.engine.begin() as connection:
        receipt = await read_serving_receipt(connection, "mrf")
        assert receipt is not None
        for model in models:
            table = model.__table__
            existing = await connection.run_sync(
                lambda sync, table=table: inspect(sync).get_columns(table.name, schema="mrf")
            )
            names = {column["name"] for column in existing}
            relation_oid = await connection.scalar(
                text("SELECT to_regclass(:table)::oid"), {"table": "mrf." + table.name}
            )
            added_columns_by_table[table.name] = []
            for column in table.columns:
                if column.name not in names:
                    definition = str(CreateColumn(column).compile(dialect=connection.dialect))
                    await connection.execute(text(f'ALTER TABLE "mrf"."{table.name}" ADD COLUMN {definition}'))
                    added_columns_by_table[table.name].append(column.name)
            assert (
                await connection.scalar(text("SELECT to_regclass(:table)::oid"), {"table": "mrf." + table.name})
                == relation_oid
            )
        assert await read_serving_receipt(connection, "mrf") == receipt
    return added_columns_by_table


async def _current_detail_identity(fixture):
    """Create independent current NPI inputs without replacing sealed CMS relations."""
    metadata = MetaData()
    for model in (NPIData, NPIDataTaxonomy, NPIDataTaxonomyGroup, NPIDataOtherIdentifier):
        model.__table__.to_metadata(metadata, schema="mrf")
    async with fixture.engine.begin() as connection:
        receipt = await read_serving_receipt(connection, "mrf")
        await connection.run_sync(metadata.create_all)
        await connection.execute(
            text(
                "INSERT INTO mrf.npi(npi,provider_first_name,provider_last_name,entity_type_code) "
                "VALUES(1000000491,'Taylor','Example',1),(1000000005,'Known','No Office',1)"
            )
        )
        await connection.execute(
            text(
                "INSERT INTO mrf.npi_taxonomy(npi,checksum,healthcare_provider_taxonomy_code) "
                "VALUES(1000000491,1,'207Q00000X')"
            )
        )
        for table in metadata.tables.values():
            await connection.execute(text(f'GRANT SELECT ON "mrf"."{table.name}" TO "{fixture.roles["reader"]}"'))
        assert await read_serving_receipt(connection, "mrf") == receipt


def _reader_settings(fixture, role, password):
    url = fixture.engine.url
    return {
        "DRIVER": "asyncpg",
        "HOST": url.host,
        "PORT": str(url.port),
        "DATABASE": url.database,
        "READER_USER": role,
        "READER_PASSWORD": password,
        "READER_POOL_MIN_SIZE": "1",
        "READER_POOL_MAX_SIZE": "1",
        "SCHEMA": "mrf",
    }


def _observe_reader_transactions(database, fixture, statements, transactions):
    """Probe only verified reads; setup probes would establish a snapshot before its locks."""
    setup_statements = []

    @event.listens_for(database._reader_database.engine.sync_engine, "after_cursor_execute")
    def record_reader(connection, cursor, statement, parameters, context, executemany):
        if not database.has_reader_session():
            setup_statements.append(statement)
            return
        probe = connection.connection.cursor()
        try:
            probe.execute(
                "SELECT session_user,current_user,current_setting('transaction_read_only'),current_setting('transaction_isolation'),pg_backend_pid(),"
                f"pg_has_role(current_user,'{fixture.roles['owner']}','SET'),"
                "(SELECT rolsuper FROM pg_roles WHERE rolname=current_user),"
                "(SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid() "
                "AND locktype='virtualxid' AND granted)"
            )
            identity_values = tuple(probe.fetchone())
            statements.append((statement.split()[0], identity_values[:7]))
            transactions.append(identity_values[7])
        finally:
            probe.close()

    return setup_statements


@pytest.fixture
async def profile_transport(published_profile_source, monkeypatch):
    """Configure the real database Reader and preserve the native CMS authorities."""
    _, fixture, _, manifest, network, sites = published_profile_source
    added_columns_by_table = await _current_profile_row_types(fixture)
    await _current_detail_identity(fixture)
    role = fixture.roles["reader"]
    password = secrets.token_urlsafe(24)
    database = Database()
    database._database_override = fixture.engine.url.database
    statements, writer_statements, transactions = [], [], []
    try:
        await fixture.connection.execute(f"ALTER ROLE \"{role}\" LOGIN PASSWORD '{password}'")
        for name, setting in _reader_settings(fixture, role, password).items():
            monkeypatch.setenv("HLTHPRT_DB_" + name, setting)
        monkeypatch.setenv("DB_SCHEMA", "mrf")
        monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-profile-control")
        monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.control_schema)
        for module in (provider, provider_profile, provider_profile_cms, provider_profile_states):
            monkeypatch.setattr(module, "db", database)
        await database._connect_reader()

        setup_statements = _observe_reader_transactions(database, fixture, statements, transactions)

        @event.listens_for(fixture.engine.sync_engine, "before_cursor_execute")
        def record_writer(connection, cursor, statement, parameters, context, executemany):
            writer_statements.append(statement)

        app = Sanic("native_provider_profile_" + uuid4().hex)
        app.blueprint(Blueprint.group([endpoint.blueprint], version_prefix="/api/v"))
        endpoint.register_network_catalog_private_responses(app)
        yield SimpleNamespace(
            app=app,
            database=database,
            added_columns_by_table=added_columns_by_table,
            fixture=fixture,
            manifest=manifest,
            network=network,
            sites=sites,
            role=role,
            statements=statements,
            setup_statements=setup_statements,
            writer_statements=writer_statements,
            transactions=transactions,
        )
    finally:
        try:
            if database._reader_database is not None:
                await database._reader_database.disconnect()
        finally:
            await fixture.connection.execute(f'ALTER ROLE "{role}" NOLOGIN PASSWORD NULL')


async def _post(context, document, *, headers=None, path=_PATH):
    transaction_start = len(context.transactions)
    _, reply = await context.app.asgi_client.post(
        path, data=json.dumps(document).encode(), headers=_AUTH if headers is None else headers
    )
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert not context.writer_statements and context.database.engine is None
    observed_transactions = context.transactions[transaction_start:]
    assert not observed_transactions or (None not in observed_transactions and len(set(observed_transactions)) == 1)
    return reply


async def test_native_profile_post_exact_office_default_denial_and_policy_generation(profile_transport, tmp_path):
    context = profile_transport
    document = v2_envelope({"npi": _NPI, "include_evidence": "true"})
    visible = await _post(context, document)
    (tmp_path / "visible.json").write_text(
        json.dumps(
            {"status": visible.status, "body": visible.json, "fixture_added_columns": context.added_columns_by_table}
        )
    )
    assert visible.status == 200, visible.json
    assert visible.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert visible.headers["X-Network-Generation"] == str(context.manifest.generation_id)
    rendered = json.dumps(visible.json)
    assert "site-1" in rendered and "site-2" not in rendered and "plan-1" in rendered
    roles = [
        public_fact["value"]
        for public_fact in visible.json["provider_profile"]["categories"]["services"]["items"]
        if public_fact["type"] == "role_context"
    ]
    assert len(roles) == 2
    assert all(
        role["location_refs"] == ["Location/site-1"] and role["network_refs"] == ["Organization/network-1"]
        for role in roles
    )
    assert {tuple(role["insurance_plan_refs"]) for role in roles} == {(), ("InsurancePlan/plan-1",)}
    denied_document = v2_envelope({"npi": _NPI, "include_evidence": "true"}, excluded_network_ids=[context.network])
    denied = await _post(context, denied_document)
    assert denied.status == 200, denied.json
    assert "plan-1" not in json.dumps(denied.json) and "role-1" not in json.dumps(denied.json)
    for category in ("certifications", "professional_experience"):
        assert visible.json["provider_profile"]["categories"][category]["items"]
        assert (
            denied.json["provider_profile"]["categories"][category]
            == visible.json["provider_profile"]["categories"][category]
        )
    old_generation = visible.json["provider_profile"]["generation_id"]
    continued_query_by_field = {"npi": _NPI, "category": "certifications", "generation_id": old_generation}
    continued = await _post(context, v2_envelope(continued_query_by_field))
    assert continued.status == 200, continued.json
    changed = await _post(context, v2_envelope(continued_query_by_field, policy_revision="1"))
    assert changed.status == 409 and changed.json["error"] == "provider_profile_generation_changed"
    changed_policy = v2_envelope(continued_query_by_field, policy_revision="1")
    assert changed.headers["X-Network-Access-Scope"] == changed_policy["access_scope_sha256"]
    assert "X-Network-Generation" not in changed.headers
    unknown = await _post(context, v2_envelope({"npi": "1000000004"}))
    assert unknown.status == 404 and unknown.json["error"] == "provider_profile_not_found"
    assert "X-Network-Generation" not in unknown.headers
    assert context.statements and all(
        identity[:4] == (context.role, context.role, "on", "repeatable read") for _, identity in context.statements
    )
    assert all(identity[5:] == (False, False) for _, identity in context.statements)
    assert any(statement.startswith("LOCK TABLE ") for statement in context.setup_statements)
    assert len({identity[4] for _, identity in context.statements}) == 1


async def test_native_profile_auth_and_closed_query_refuse_before_reader(profile_transport):
    context = profile_transport
    document = v2_envelope({"npi": _NPI})
    assert (await _post(context, document, headers={"Authorization": "Bearer wrong"})).status == 403
    assert (await _post(context, v2_envelope({"npi": _NPI, "network_ids": [context.network]}))).status == 400
    assert (await _post(context, document | {"access_scope_sha256": "0" * 64})).status == 400
    assert (await _post(context, envelope({"npi": _NPI}))).status == 400
    assert not context.statements and not context.setup_statements


_DETAIL_PATH = "/api/v1/registry/serving/providers/detail"


def _assert_unproved_detail_denial(http_response):
    assert http_response.status == 404 and "data" not in http_response.json
    assert http_response.json == {"error": {"code": "network_catalog_not_found"}}
    assert (
        "X-Network-Generation" not in http_response.headers and "X-Network-Manifest-SHA256" not in http_response.headers
    )


@pytest.mark.parametrize("published_profile_source", [True], indirect=True)
async def test_native_detail_preserves_public_contacts_and_exact_office_membership(profile_transport, tmp_path):
    """Retain public contacts and verify denied selectors cannot mint an absence proof."""
    context = profile_transport
    query_by_field = {
        "npi": _NPI,
        "include": ["taxonomy", "plans"],
        "include_sources": "true",
        "include_evidence": "true",
    }
    assert await context.fixture.connection.fetchval(
        f"SELECT count(*)=2 AND bool_and(row_origin='base' AND address_source_mask=0) "
        f'FROM "{context.manifest.schema_name}".entity_address_unified'
    )
    document = v2_envelope(query_by_field)
    visible = await _post(context, document, path=_DETAIL_PATH)
    (tmp_path / "detail-visible.json").write_text(json.dumps({"status": visible.status, "body": visible.json}))
    assert visible.status == 200, visible.json
    assert visible.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert visible.headers["X-Network-Generation"] == str(context.manifest.generation_id)
    assert visible.json["manifest_sha256"] == context.manifest.manifest_sha256
    detail = visible.json["data"]["npi"]
    assert detail["provider_first_name"] == "Taylor"
    assert visible.json["data"]["taxonomy"] == detail["taxonomy_list"]
    assert detail["taxonomy_list"][0]["healthcare_provider_taxonomy_code"] == "207Q00000X"
    assert visible.json["data"]["plans_by_npi"] == {"npi_data": [], "plan_data": [], "issuer_data": []}
    assert len(detail["address_list"]) == 2
    assert all(address["telephone_number"] == "555-0100" for address in detail["address_list"])
    assert sum(bool(address["network_memberships"]) for address in detail["address_list"]) == 1
    membership = next(
        address["network_memberships"][0] for address in detail["address_list"] if address["network_memberships"]
    )
    assert membership["network_id"] == context.network and membership["provider_id"] == _NPI
    assert membership["location_id"] == str(context.sites[0]) and membership["location_id"] != str(context.sites[1])
    assert membership["evidence_ids"]
    hidden_document = v2_envelope(query_by_field, excluded_network_ids=[context.network])
    hidden = await _post(context, hidden_document, path=_DETAIL_PATH)
    assert hidden.status == 200, hidden.json
    hidden_rendered = json.dumps(hidden.json)
    assert all(identifier not in hidden_rendered for identifier in ("plan-1", "role-1", "network-1"))
    assert hidden.json["data"]["npi"]["provider_first_name"] == "Taylor"
    assert len(hidden.json["data"]["npi"]["address_list"]) == 2
    assert all(
        address["telephone_number"] == "555-0100" and address["network_memberships"] == []
        for address in hidden.json["data"]["npi"]["address_list"]
    )
    selected_query = query_by_field | {"selector": {"namespace": "canonical", "values": [context.network]}}
    selected = await _post(context, v2_envelope(selected_query), path=_DETAIL_PATH)
    assert selected.status == 200, selected.json
    assert len(selected.json["data"]["npi"]["address_list"]) == 1
    assert selected.json["data"]["npi"]["address_list"][0]["network_memberships"] == [membership]
    denied = await _post(
        context, v2_envelope(selected_query, excluded_network_ids=[context.network]), path=_DETAIL_PATH
    )
    _assert_unproved_detail_denial(denied)
    assert context.statements and all(
        identity[:4] == (context.role, context.role, "on", "repeatable read") for _, identity in context.statements
    )
    assert all(identity[5:] == (False, False) for _, identity in context.statements)
    assert len({identity[4] for _, identity in context.statements}) == 1


async def test_native_detail_known_zero_office_missing_identity_and_auth_before_reader(profile_transport, tmp_path):
    context = profile_transport
    document = v2_envelope({"npi": _NPI})
    assert (await _post(context, document, headers={"Authorization": "Bearer wrong"}, path=_DETAIL_PATH)).status == 403
    assert (await _post(context, document | {"access_scope_sha256": "0" * 64}, path=_DETAIL_PATH)).status == 400
    assert (await _post(context, envelope({"npi": _NPI}), path=_DETAIL_PATH)).status == 400
    assert not context.statements and not context.setup_statements
    known = await _post(context, v2_envelope({"npi": "1000000005"}), path=_DETAIL_PATH)
    assert known.status == 200, known.json
    assert known.json["data"]["npi"]["address_list"] == []
    unknown = await _post(context, v2_envelope({"npi": "0000000000"}), path=_DETAIL_PATH)
    assert unknown.status == 404 and "data" not in unknown.json
    assert unknown.json == {
        "client_id": document["client_id"],
        "policy_revision": document["policy_revision"],
        "generation": str(context.manifest.generation_id),
        "approved_custom_revision": str(context.manifest.approved_custom_revision),
        "manifest_sha256": context.manifest.manifest_sha256,
        "npi": "0000000000",
        "access_scope_sha256": v2_envelope({"npi": "0000000000"})["access_scope_sha256"],
        "error": {"code": "provider_detail_not_found"},
    }
    assert unknown.headers["X-Network-Generation"] == str(context.manifest.generation_id)
    assert unknown.headers["X-Network-Access-Scope"] == unknown.json["access_scope_sha256"]
    assert unknown.headers["X-Network-Manifest-SHA256"] == context.manifest.manifest_sha256
    (tmp_path / "absence-proof.json").write_text(
        json.dumps(
            {
                "status": unknown.status,
                "body": unknown.json,
                "headers": {
                    name: unknown.headers[name]
                    for name in (
                        "Cache-Control",
                        "X-Network-Generation",
                        "X-Network-Access-Scope",
                        "X-Network-Manifest-SHA256",
                    )
                },
                "actual_manifest": {
                    "generation": context.manifest.generation_id,
                    "approved_custom_revision": context.manifest.approved_custom_revision,
                    "manifest_sha256": context.manifest.manifest_sha256,
                },
                "reader_transactions": context.statements,
                "reader_setup_statements": context.setup_statements,
            }
        )
    )


_SOURCE_PATH = "/api/v1/registry/serving/directory/entities"
_SOURCE_KEY = bytes(range(32))


def _source_resource_cases(original, revision):
    """Declare mixed and unreviewed associations before acquisition and custody."""
    from copy import deepcopy

    rows = original(revision)
    for number in (2, 3):
        rows["Organization"].append(
            {"id": f"network-{number}", "name": f"Source Network {number}", "type": [{"text": "ntwk"}]}
        )
        plan = deepcopy(rows["InsurancePlan"][0])
        plan.update(id=f"plan-{number}", name=f"Source Plan {number}")
        networks = (1, 2) if number == 2 else (3,)
        plan["network"] = [{"reference": f"Organization/network-{entry}"} for entry in networks]
        plan["plan"][0]["network"] = list(plan["network"])
        plan["coverage"][0]["network"] = list(plan["network"])
        plan["identifier"] = [{"system": "https://example.test/plan", "value": f"source-plan-{number}"}]
        rows["InsurancePlan"].append(plan)
    rows["InsurancePlan"][-1]["network"].append({"reference": "Organization/unproved-network"})
    return rows


@pytest.fixture
def source_preparation(monkeypatch):
    """Extend only dependent tests before source hashes and review publication."""
    from tests import cms_npd_admission_postgres_support as support
    from tests import test_network_cms_registry_source_pair_postgres as pairs
    from tests import test_provider_profile_network_facts_postgres as facts

    prepare = pairs._retained_office_resources
    review = facts._reviewed_cms_binding

    def resources(settings):
        prepare(settings)
        original = support._resource_rows
        settings.setattr(support, "_resource_rows", lambda revision: _source_resource_cases(original, revision))

    async def reviewed(connection, schema, engine, pin, coordinates):
        from process.network_approved_membership_source import pin_approved_membership_source
        from tests.test_network_fhir_membership_source_postgres import _reviewed_binding
        from tests.test_registry_approval_store_postgres import _approve, _command

        actor, networks, first, _approved = await review(connection, schema, engine, pin, coordinates)
        legacy = await connection.fetchval(
            "SELECT network_id FROM mrf.provider_directory_insurance_network_source_binding "
            "WHERE source_id='cms-npd' AND resource_type='Organization' AND resource_id='network-2'"
        )
        second = first | {
            "binding_id": str(uuid4()),
            "source_key": "network-2",
            "network_id": networks[1]["record_id"],
            "source_scope_json": {
                "organization_id": "network-2",
                "legacy_uuid": str(legacy),
                "alias_scope": pin.alias_scope,
            },
        }
        record = (await _reviewed_binding(connection, schema, actor, [second]))["records"][0]
        approval = await _approve(connection, schema, await _command(connection, schema, *networks, record), actor)
        async with connection.transaction(isolation="repeatable_read"):
            approved = await pin_approved_membership_source(
                connection, approved_revision=approval["approved_revision"], control_schema=schema
            )
        return actor, networks, first, approved

    monkeypatch.setattr(pairs, "_retained_office_resources", resources)
    monkeypatch.setattr(facts, "_reviewed_cms_binding", reviewed)


async def _source_reviewed_payer(held):
    """Bind an existing payer through genuine reviewed CMS source evidence."""
    from datetime import datetime, timezone

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from process.provider_directory_mrf_payer_binding import (
        ReviewedCMSPayerDecision,
        record_reviewed_cms_payer_decision,
    )

    async with async_sessionmaker(held.fixture.engine)() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO mrf.mrf_payer(payer_id,canonical_name,lifecycle) "
                "VALUES('source-payer','Example Source Payer','active')"
            )
        )
        digest = await session.scalar(
            text(
                "SELECT payload_sha256 FROM mrf.provider_directory_entity_release_evidence "
                "WHERE resource_id='insurer-1' AND release_id=:release"
            ),
            {"release": held.source_pin.release_id},
        )
        await record_reviewed_cms_payer_decision(
            session,
            ReviewedCMSPayerDecision(
                uuid4(),
                "bind",
                "cms-npd",
                "insurer-1",
                held.source_pin.release_id,
                digest,
                "source-payer",
                None,
                "source-review",
                "f" * 64,
                "source-reviewer",
                datetime.now(timezone.utc),
            ),
        )


@pytest.fixture
async def source_transport(source_preparation, profile_transport, published_profile_source, monkeypatch):
    """Observe delegated real pins and bind the actual request's prior session."""
    import base64
    from contextlib import contextmanager

    from api import provider_directory_managed_read as managed
    from api.provider_directory_entities_contract import CURSOR_KEY_ENV
    from process import registry_network_catalog_read as catalog

    held = profile_transport
    held.source_pin = published_profile_source[2].recipe.source_pin
    await _source_reviewed_payer(held)
    held.writer_statements.clear()
    held.calls, held.requests, held.source_sql, held.driver_proofs = [], [], [], []
    held.prior = object()
    original_pin, original_custody = catalog._pin, catalog.resolve_network_serving_manifest
    original_reader = managed._request_reader

    async def pin(*args, **kwargs):
        held.calls.append("pin")
        driver = args[0]
        held.active_driver = driver
        proof = await driver.fetchrow(
            "SELECT session_user,current_user,current_setting('transaction_read_only'),"
            "current_setting('transaction_isolation'),pg_backend_pid(),"
            "(SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid() "
            "AND locktype='virtualxid' AND granted),current_setting('statement_timeout'),"
            "current_setting('lock_timeout'),current_setting('work_mem')"
        )
        held.driver_proofs.append(tuple(proof))
        return await original_pin(*args, **kwargs)

    async def custody(*args, **kwargs):
        held.calls.append("custody")
        assert args[0] is held.active_driver
        return await original_custody(*args, **kwargs)

    @contextmanager
    def request_reader(request, session):
        assert request.ctx.sa_session is held.prior
        with original_reader(request, session):
            assert request.ctx.sa_session is session and session.info["api_reader_verified"]
            yield
        assert request.ctx.sa_session is held.prior

    @held.app.middleware("request")
    async def prior_session(request):
        request.ctx.sa_session = held.prior
        held.requests.append(request)

    @event.listens_for(held.database._reader_database.engine.sync_engine, "after_cursor_execute")
    def observe_sql(connection, cursor, statement, parameters, context, executemany):
        if len(held.source_sql) < 5000:
            held.source_sql.append(statement)

    monkeypatch.setattr(catalog, "_pin", pin)
    monkeypatch.setattr(catalog, "resolve_network_serving_manifest", custody)
    monkeypatch.setattr(managed, "_request_reader", request_reader)
    monkeypatch.setenv(CURSOR_KEY_ENV, base64.urlsafe_b64encode(_SOURCE_KEY).decode().rstrip("="))
    yield held


def _source_query(kind="networks", **changes):
    return {"kind": kind, "source_id": "cms-npd", "shape": "entities", "limit": 1} | changes


async def _source_post(held, document, *, status=200, headers=None):
    from api.network_address_scope import current_network_address_scope

    calls, statements = len(held.calls), len(held.statements)
    setup_start = len(held.setup_statements)
    reply = await _post(held, document, headers=headers, path=_SOURCE_PATH)
    assert reply.status == status, reply.json
    assert current_network_address_scope() is None
    assert all(request.ctx.sa_session is held.prior for request in held.requests)
    observed = held.statements[statements:]
    if status == 403:
        assert not observed and not held.calls[calls:] and not held.setup_statements[setup_start:]
    else:
        assert held.calls[calls:] == ["pin", "custody"]
        identities = [identity for _statement, identity in observed]
        assert identities and len({identity[4] for identity in identities}) == 1
        assert all(identity[:4] == (held.role, held.role, "on", "repeatable read") for identity in identities)
        assert all(identity[5:] == (False, False) for identity in identities)
        proof = held.driver_proofs[-1]
        assert proof[:4] == (held.role, held.role, "on", "repeatable read")
        assert proof[4] == identities[0][4] and proof[5] == held.transactions[-1]
        timeout_text = proof[6]
        timeout_ms = float(timeout_text[:-2]) if timeout_text.endswith("ms") else float(timeout_text[:-1]) * 1000
        assert 0 < timeout_ms <= 2000
        assert proof[7:] == ("250ms", "4MB")
    if status == 200:
        if document["query"]["source_id"] == "cms-npd":
            assert reply.headers["X-CMS-Retained-Publication"] == "committed/v1"
        else:
            assert "X-CMS-Retained-Publication" not in reply.headers
        assert reply.json["manifest_sha256"] and reply.json["approved_custom_revision"]
        assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    return reply


async def _source_walk(held, query, **policy):
    items, cursors, generation = [], [], None
    for _page in range(30):
        reply = await _source_post(held, v2_envelope(query, **policy))
        page = reply.json["data"]
        assert len(page["items"]) <= query["limit"]
        assert generation is None or page["generation_id"] == generation
        generation = page["generation_id"]
        items.extend(page["items"])
        if page["next_cursor"] is None:
            return items, cursors, generation
        assert page["next_cursor"] not in cursors
        cursors.append(page["next_cursor"])
        if (
            query["source_id"] == "cms-npd"
            and query["shape"] == "relationships"
            and query["kind"] not in {"networks", "payers"}
        ):
            descriptor = query | {
                "network_generation": query.get("network_generation", str(held.manifest.generation_id))
            }
            contents, _ = _source_cursor_contents(v2_envelope(descriptor, **policy), generation, page["next_cursor"])
            assert contents[0] == len(items)
        query = query | {"generation_id": generation, "cursor": page["next_cursor"]}
    pytest.fail("Source continuation exceeded the synthetic fixture bound")


async def _source_ids(held, resource_type, id_column):
    return {
        row["resource_id"]: str(row["identity"])
        for row in await held.fixture.connection.fetch(
            f"SELECT resource_id,{id_column} AS identity FROM "
            + (
                "mrf.provider_directory_insurance_network_source_binding"
                if resource_type == "Organization"
                else "mrf.provider_directory_resource_identity"
            )
            + " WHERE source_id='cms-npd' AND resource_type=$1",
            resource_type,
        )
    }


async def _source_association_proof(held):
    """Retain the actual admitted and retained plan identity and network evidence."""
    namespace = '"' + held.source_pin.read_schema_name + '"'
    connection = held.fixture.connection
    resources = await connection.fetch(
        f"SELECT resource_id,payload_json FROM {namespace}.provider_directory_dataset_resource "
        "WHERE dataset_id=$1 AND resource_type='InsurancePlan' ORDER BY resource_id",
        held.source_pin.dataset_id,
    )
    evidence = await connection.fetch(
        f"SELECT insurance_plan_resource_id,network_resource_id,plan_payload_sha256 "
        f"FROM {namespace}.provider_directory_insurance_network_plan_evidence ORDER BY insurance_plan_resource_id,network_resource_id"
    )
    identities = await connection.fetch(
        "SELECT resource_id,entity_id FROM mrf.provider_directory_resource_identity "
        "WHERE source_id='cms-npd' AND resource_type='InsurancePlan' ORDER BY resource_id"
    )
    return {
        "retained_resources": [dict(item) for item in resources],
        "network_plan_evidence": [dict(item) for item in evidence],
        "identities": [dict(item) for item in identities],
    }


async def test_native_managed_source_default_paging_and_public_facets(source_transport, tmp_path):
    held = source_transport
    network_ids = await _source_ids(held, "Organization", "network_id")
    plans = await _source_ids(held, "InsurancePlan", "entity_id")
    policy_by_field = {"excluded_network_ids": [held.network]}
    networks, _, _ = await _source_walk(held, _source_query(), **policy_by_field)
    assert {source_item["id"] for source_item in networks} == {network_ids["network-2"], network_ids["network-3"]}
    plan_rows, _, _ = await _source_walk(held, _source_query("plans"), **policy_by_field)
    (tmp_path / "source-association-proof.json").write_text(
        json.dumps(await _source_association_proof(held), default=str)
    )
    assert {source_item["id"] for source_item in plan_rows} == {plans["plan-2"], plans["plan-3"]}
    for kind in ("organizations", "payers", "sites", "practitioner-roles"):
        public, _, _ = await _source_walk(held, _source_query(kind))
        excluded, _, _ = await _source_walk(held, _source_query(kind), **policy_by_field)
        independent, _, _ = await _source_walk(
            held,
            _source_query(kind),
            **policy_by_field,
            excluded_company_ids=[str(uuid4())],
            excluded_group_ids=[str(uuid4())],
        )
        assert public and {source_item["id"] for source_item in public} == {
            source_item["id"] for source_item in excluded
        }
        assert {source_item["id"] for source_item in independent} == {source_item["id"] for source_item in public}
        if kind == "sites":
            assert str(held.sites[1]) in {source_item["id"] for source_item in excluded}
        if kind == "practitioner-roles":
            for role in excluded:
                relations, _, _ = await _source_walk(
                    held, _source_query(kind, shape="relationships", entity_id=role["id"]), **policy_by_field
                )
                assert str(held.sites[1]) in {source_item["target_id"] for source_item in relations}
                assert network_ids["network-1"] not in {source_item["target_id"] for source_item in relations}
                assert any(source_item.get("provider_npi") == _NPI for source_item in relations)
    unproved, _, _ = await _source_walk(
        held, _source_query("plans", shape="relationships", entity_id=plans["plan-3"]), **policy_by_field
    )
    assert any(source_item["status"] == "unresolved" and source_item["target_id"] is None for source_item in unproved)
    assert any("identity_count" in sql and "EXCEPT" in sql and "LIMIT" in sql for sql in held.source_sql)
    assert any("row_number() OVER" in sql and "EXCEPT" in sql for sql in held.source_sql)


def _source_cursor_contents(document, generation, cursor):
    import base64
    from dataclasses import replace

    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    from api.provider_directory_managed_read import parse_managed_directory_query

    query = replace(
        parse_managed_directory_query(document["query"]),
        excluded_network_ids=tuple(document["excluded_network_ids"]),
        network_generation=int(document["query"]["network_generation"]),
        access_scope_sha256=document["access_scope_sha256"],
    )
    scope = json.dumps(query.scope(generation), separators=(",", ":")).encode()
    decoded = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
    aad = b"provider-directory.cursor.v1\0" + scope
    return json.loads(AESGCM(_SOURCE_KEY).decrypt(decoded[:12], decoded[12:], aad)), aad


async def test_native_managed_source_retained_cursor_policy_and_expiry(source_transport):
    import base64
    import time

    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    held = source_transport
    query = _source_query(network_generation=str(held.manifest.generation_id - 1))
    document = v2_envelope(query)
    first = await _source_post(held, document)
    assert first.headers["X-CMS-Retained-Publication"] == "committed/v1"
    assert first.json["generation"] == query["network_generation"]
    page = first.json["data"]
    assert page["generation_id"] != first.json["generation"] and page["next_cursor"]
    contents, aad = _source_cursor_contents(document, page["generation_id"], page["next_cursor"])
    assert int(time.time()) < contents[1] <= int(time.time()) + 900
    continued = query | {"generation_id": page["generation_id"], "cursor": page["next_cursor"]}
    second = await _source_post(held, v2_envelope(continued))
    assert second.json["data"]["next_cursor"]
    next_contents, _ = _source_cursor_contents(document, page["generation_id"], second.json["data"]["next_cursor"])
    assert next_contents[1] == contents[1]
    for changed in (
        v2_envelope(continued, policy_revision="9"),
        v2_envelope(continued | {"kind": "plans"}),
        v2_envelope(continued | {"generation_id": "gen_changed"}),
        v2_envelope(continued | {"network_generation": str(held.manifest.generation_id)}),
    ):
        failure = await _source_post(held, changed, status=409)
        assert failure.json == {"error": {"code": "network_catalog_source_changed"}}
    nonce = b"expired-test"
    nonce = nonce + b"!" * (12 - len(nonce))
    expired = AESGCM(_SOURCE_KEY).encrypt(nonce, json.dumps([contents[0], int(time.time()) - 1]).encode(), aad)
    token = base64.urlsafe_b64encode(nonce + expired).decode().rstrip("=")
    await _source_post(held, v2_envelope(continued | {"cursor": token}), status=409)
    for shape in ("entity", "relationships"):
        missing = await _source_post(held, v2_envelope(_source_query(shape=shape, entity_id=str(uuid4()))), status=404)
        assert missing.json == {"error": {"code": "provider_directory_entity_not_found"}}
    await _source_post(held, document, status=403, headers={"Authorization": "Bearer invalid-control"})


@pytest.fixture
async def source_doctors(source_transport, monkeypatch):
    from tests.provider_directory_entities_postgres_support import _create_tables, _seed_groups

    held = source_transport
    schema = "source_doctors_" + uuid4().hex
    try:
        async with held.fixture.engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.execute(text(f'SET LOCAL search_path TO "{schema}"'))
            await _create_tables(connection)
            await _seed_groups(connection)
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{held.role}"'))
            await connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{held.role}"'))
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        monkeypatch.setenv("DB_SCHEMA", schema)
        held.writer_statements.clear()
        yield held, schema
    finally:
        async with held.fixture.engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


async def test_native_managed_source_doctors_public_policy_and_snapshot(source_doctors):
    from tests.provider_directory_entities_postgres_support import GROUP_A, GROUP_B, SITE_A, SITE_B, SITE_C, SITE_D

    held, schema = source_doctors
    networks = await held.fixture.connection.fetch(
        f'SELECT record_key::integer AS identity FROM "{held.fixture.control_schema}".registry_approved_record '
        "WHERE approved_revision=$1 AND record_kind='network' AND record_json->'archived'='false'::jsonb",
        held.manifest.approved_custom_revision,
    )
    exclusions = sorted(source_item["identity"] for source_item in networks)
    for kind, expected in (("medical-groups", {GROUP_A, GROUP_B}), ("sites", {SITE_A, SITE_B, SITE_C, SITE_D})):
        query = _source_query(kind, source_id="cms-doctors", network_generation=str(held.manifest.generation_id))
        public, _, generation = await _source_walk(held, query)
        visible, cursors, denied_generation = await _source_walk(held, query, excluded_network_ids=exclusions)
        assert (
            {source_item["id"] for source_item in public} == {source_item["id"] for source_item in visible} == expected
        )
        assert generation != denied_generation and cursors
        document = v2_envelope(query, excluded_network_ids=exclusions)
        contents, _ = _source_cursor_contents(document, denied_generation, cursors[0])
        assert contents[1] > 0
        relations, _, _ = await _source_walk(
            held, query | {"shape": "relationships", "entity_id": visible[0]["id"]}, excluded_network_ids=exclusions
        )
        assert relations and all(source_item["provider_npi"] for source_item in relations)
    async with held.fixture.engine.begin() as connection:
        await connection.execute(
            text(f'UPDATE "{schema}".reference_family_result_generation SET local_generation=local_generation+1')
        )
    held.writer_statements.clear()
    await _source_post(
        held, v2_envelope(query | {"generation_id": denied_generation}, excluded_network_ids=exclusions), status=409
    )
    async with held.fixture.engine.begin() as connection:
        await connection.execute(
            text(f"DELETE FROM \"{schema}\".reference_family_result_generation WHERE importer_id='cms-doctors'")
        )
    held.writer_statements.clear()
    failure = await _source_post(held, v2_envelope(query, excluded_network_ids=exclusions), status=503)
    assert failure.json == {"error": {"code": "network_catalog_unavailable"}}


async def _native_prepared_plan_request(database, held, failure):
    from api import custom_import_plan_sql as plan
    from api.provider_detail_network_read import managed_provider_network_read_scope
    from api.provider_profile_network_facts import read_cms_profile_plan_inputs
    from api.provider_profile_snapshot import provider_profile_read_snapshot
    from db.connection import current_session

    async def setup(session):
        assert not session.info.get("api_reader_verified") and not database.has_reader_session()
        await plan.prepare_plan_query_tables(session)

    async with database.reader_session(temporary_setup=setup) as session:
        assert session.info["api_reader_verified"] and current_session() is session
        async with provider_profile_read_snapshot(database, "mrf", include_detail=True) as snapshot:
            assert snapshot is session
            async with managed_provider_network_read_scope(
                session,
                selector=None,
                network_generation=held.manifest.generation_id,
                excluded_network_ids=(),
                access_scope_sha256="a" * 64,
            ) as manifest:
                driver = (await (await session.connection()).get_raw_connection()).driver_connection
                proof = await driver.fetchrow(
                    "SELECT session_user,current_user,current_setting('transaction_read_only') AS read_only,"
                    "current_setting('transaction_isolation') AS isolation,pg_backend_pid() AS pid,"
                    "(SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS superuser,"
                    "has_table_privilege(current_user,'mrf.npi','INSERT,UPDATE,DELETE,TRUNCATE') AS live_write,"
                    "has_database_privilege(current_user,current_database(),'TEMP') AS native_temp"
                )
                assert tuple(proof)[:4] == (held.role, held.role, "on", "repeatable read")
                assert proof["superuser"] is False and proof["live_write"] is False and proof["native_temp"] is True
                facts = await read_cms_profile_plan_inputs(session, manifest, _NPI, excluded_network_ids=())
                assert facts.manifest is manifest and facts.facts and all(fact["npi"] == _NPI for fact in facts.facts)
                await session.execute(text(f"INSERT INTO pg_temp.{plan.IMPORTED} VALUES(:npi,1)"), {"npi": int(_NPI)})
                assert (
                    await driver.copy_records_to_table(
                        plan.IMPORTED, records=[(1000000005, 2)], columns=["npi", "import_rank"], schema_name="pg_temp"
                    )
                    == "COPY 1"
                )
                await session.execute(plan.plan_analyze_statement(plan.IMPORTED))
                assert await session.scalar(text(f"SELECT count(*) FROM pg_temp.{plan.IMPORTED}")) == 2
                if failure is not None:
                    raise failure
                return proof["pid"]


async def test_native_reader_prepares_plan_temporary_tables_before_verified_snapshot(profile_transport, monkeypatch):
    import asyncio

    from api import custom_import_plan_sql as plan
    from process import registry_network_catalog_read as catalog

    held = profile_transport
    database = Database()
    database._database_override = held.fixture.engine.url.database
    calls = []
    original_pin, original_custody = catalog._pin, catalog.resolve_network_serving_manifest

    async def pin(*args, **options):
        calls.append("pin")
        return await original_pin(*args, **options)

    async def custody(*args, **options):
        calls.append("custody")
        return await original_custody(*args, **options)

    monkeypatch.setattr(catalog, "_pin", pin)
    monkeypatch.setattr(catalog, "resolve_network_serving_manifest", custody)
    names = [plan.IMPORTED, plan.CANDIDATES, plan.MEMBERSHIPS, plan.OCCURRENCES, plan.PRICES, plan.SELECTED]
    held.writer_statements.clear()
    try:
        pid = await _native_prepared_plan_request(database, held, None)
        for failure in (None, RuntimeError("prepared request rollback"), asyncio.CancelledError()):
            if failure is not None:
                with pytest.raises(type(failure)) as caught:
                    await _native_prepared_plan_request(database, held, failure)
                assert caught.value is failure
            assert not database.has_reader_session()
            async with database.reader_session() as reader:
                assert await reader.scalar(text("SELECT pg_backend_pid()")) == pid
                assert (
                    await reader.scalar(
                        text(
                            "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() "
                            "AND relname=ANY(CAST(:names AS text[]))"
                        ),
                        {"names": names},
                    )
                    == 0
                )
        assert calls == ["pin", "custody"] * 3
        assert database.engine is None and not held.writer_statements
    finally:
        await database.disconnect()
