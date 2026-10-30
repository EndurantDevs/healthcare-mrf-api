# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine signed import fixtures for managed provider detail composition."""

import json
import re
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from types import SimpleNamespace
from uuid import UUID

import pytest
from sqlalchemy import event, text
from sqlalchemy.dialects import postgresql

from api import custom_import_read_http as transport
from api import provider_detail_network_metadata as metadata
from api.endpoint.npi import GEO_SERVICE_LOCATIONS_ENV
from api.provider_detail_network_contract import parse_managed_detail_query
from api.provider_detail_network_read import managed_provider_detail_read_scope
from api.provider_profile_snapshot import provider_profile_read_snapshot
from process.custom_import import publication
from process.custom_import.read_identity import resolve_generation_snapshot, verify_published_generation
from process.custom_import.storage_layout import SNAPSHOT_MODELS, snapshot_models
from process.registry_network_catalog_read import RegistryNetworkCatalogError
from tests import test_custom_import_read_http as permits
from tests.custom_import_managed_detail_postgres_support import (
    BATCH_PATH,
    PROVIDER_NPI,
    ZERO_OFFICE_NPI,
    import_publication_owner,
    install_signed_routes,
    managed_document,
    publish_import,
    signed_headers,
)
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template
from tests.test_provider_profile_network_facts_postgres import published_profile_source as published_profile_source
from tests.test_registry_provider_profile_transport_postgres import profile_transport as profile_transport

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def managed_import_transport(profile_transport, monkeypatch):
    context = profile_transport
    async with import_publication_owner(context) as publication_case:
        case, target, external_target = await publish_import(context, publication_case)
        install_signed_routes(context, monkeypatch)
        context.writer_statements.clear()
        context.statements.clear()
        context.setup_statements.clear()
        context.transactions.clear()
        yield SimpleNamespace(context=context, case=case, target=target, external_target=external_target)


async def test_published_import_snapshot_is_readable_by_the_same_ordinary_reader(managed_import_transport):
    fixture = managed_import_transport
    context = fixture.context
    async with context.database.reader_session() as session:
        assert (await session.execute(text("SELECT session_user,current_user"))).one() == (context.role, context.role)
        await verify_published_generation(session, fixture.target)
        assert await resolve_generation_snapshot(session, fixture.target) is not None
    assert not context.writer_statements and context.database.engine is None


async def test_real_signed_http_reads_the_published_import_with_no_owner_session(managed_import_transport):
    fixture = managed_import_transport
    context = fixture.context
    request_by_field = {"target": fixture.external_target, "filters": [], "page_size": 10, "cursor": None}
    body = transport._canonical_json_bytes(request_by_field)
    path = transport.CUSTOM_IMPORT_READ_PATH
    _, reply = await context.app.asgi_client.post(
        path, data=body, headers=signed_headers(body, fixture.external_target, path=path, provider=False)
    )
    assert reply.status == 200, reply.json
    rendered = json.dumps(reply.json)
    assert PROVIDER_NPI in rendered and ZERO_OFFICE_NPI in rendered
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert not context.writer_statements and context.database.engine is None
    assert context.statements and all(
        identity[:4] == (context.role, context.role, "on", "repeatable read") for _, identity in context.statements
    )
    assert all(identity[5:] == (False, False) for _, identity in context.statements)
    assert len({identity[4] for _, identity in context.statements}) == 1
    assert len(set(context.transactions)) == 1
    prepared = managed_document(
        fixture.external_target, network_ids=[context.network], generation=context.manifest.generation_id
    )
    assert prepared["managed_detail"]["query"]["npi"] == prepared["native_batch"]["npis"][0]


@pytest.mark.parametrize(
    "builder",
    [
        publication._family_child_material_statement,
        publication._root_scalar_material_statement,
        publication._child_scalar_material_statement,
        publication._winner_material_statement,
    ],
    ids=("family-child", "root-scalar", "child-scalar", "winner"),
)
@pytest.mark.parametrize("retained", (False, True), ids=("canonical", "snapshot"))
async def test_materialization_pack_aliases_preserve_the_selected_storage_family(builder, retained):
    generation = SimpleNamespace(
        generation_id=1,
        dataset_id=2,
        definition_revision_id=3,
        schema_revision_id=4,
        execution_id=5,
        producing_fence=6,
        producing_token_sha256=b"t" * 32,
    )
    statement = builder(generation, models=snapshot_models(17) if retained else None)
    sql = str(statement.compile(dialect=postgresql.dialect()))
    hot_names = "|".join(model.__tablename__ for model in SNAPSHOT_MODELS)
    if retained:
        assert re.search(r"\bmrf\.(?:" + hot_names + r")\b", sql) is None
        assert "ci_snapshot_17.custom_import_pack AS" in sql
    else:
        assert "ci_snapshot_" not in sql
        assert "mrf.custom_import_pack AS" in sql


async def _managed_post(fixture, request_by_field, *, headers=None):
    context = fixture.context
    body = transport._canonical_json_bytes(request_by_field)
    start = len(context.transactions)
    _, reply = await context.app.asgi_client.post(
        BATCH_PATH, data=body, headers=signed_headers(body, fixture.external_target) if headers is None else headers
    )
    assert reply.headers["Cache-Control"] == "private, no-store"
    observed = context.transactions[start:]
    assert not observed or (None not in observed and len(set(observed)) == 1)
    assert not context.writer_statements and context.database.engine is None
    return reply


@pytest.mark.parametrize("published_profile_source", [True], indirect=True)
async def test_signed_managed_detail_pins_import_and_exact_office_policy(managed_import_transport, tmp_path):
    fixture = managed_import_transport
    context = fixture.context
    request_by_field = managed_document(fixture.external_target)
    visible = await _managed_post(fixture, request_by_field)
    (tmp_path / "visible.json").write_text(json.dumps({"status": visible.status, "body": visible.json}))
    assert visible.status == 200, visible.json
    assert visible.headers["X-Network-Access-Scope"] == request_by_field["managed_detail"]["access_scope_sha256"]
    assert visible.headers["X-Network-Generation"] == str(context.manifest.generation_id)
    assert visible.json["managed_detail"]["manifest_sha256"] == context.manifest.manifest_sha256
    provider_item = visible.json["items"][0]
    assert provider_item["status"] == 200 and provider_item["npi"] == PROVIDER_NPI
    assert set(provider_item["provider"]) == {"npi", "custom_import"}
    assert provider_item["provider"]["npi"] == PROVIDER_NPI
    assert provider_item["provider"]["custom_import"]["target"] == fixture.external_target
    detail = visible.json["managed_detail"]["data"]["npi"]
    assert detail["provider_first_name"] == "Taylor"
    assert len(detail["address_list"]) == 2
    memberships = [membership for address in detail["address_list"] for membership in address["network_memberships"]]
    assert len(memberships) == 1
    assert memberships[0]["network_id"] == context.network
    assert memberships[0]["location_id"] == str(context.sites[0]) != str(context.sites[1])
    assert memberships[0]["evidence_ids"]
    selected = await _managed_post(fixture, managed_document(fixture.external_target, network_ids=[context.network]))
    assert selected.status == 200, selected.json
    assert len(selected.json["managed_detail"]["data"]["npi"]["address_list"]) == 1
    denied = await _managed_post(
        fixture, managed_document(fixture.external_target, excluded_network_ids=[context.network])
    )
    assert denied.status == 200, denied.json
    denied_detail = denied.json["managed_detail"]["data"]["npi"]
    assert len(denied_detail["address_list"]) == 2
    assert all(not address["network_memberships"] for address in denied_detail["address_list"])
    assert all(address["telephone_number"] == "555-0100" for address in denied_detail["address_list"])
    assert "plan-1" not in json.dumps(denied.json["managed_detail"])
    assert context.statements and all(
        identity[:4] == (context.role, context.role, "on", "repeatable read") for _, identity in context.statements
    )
    assert all(identity[5:] == (False, False) for _, identity in context.statements)
    assert len({identity[4] for _, identity in context.statements}) == 1


async def test_signed_managed_detail_default_preserves_genuine_zero_office_identity(managed_import_transport):
    fixture = managed_import_transport
    default = await _managed_post(fixture, managed_document(fixture.external_target, npi=ZERO_OFFICE_NPI))
    assert default.status == 200, default.json
    assert default.json["managed_detail"]["data"]["npi"]["address_list"] == []
    selected = await _managed_post(
        fixture, managed_document(fixture.external_target, npi=ZERO_OFFICE_NPI, network_ids=[fixture.context.network])
    )
    assert selected.status == 404 and "managed_detail" not in selected.json


async def test_signed_managed_detail_invalid_expired_and_wrong_target_refuse_before_reader(managed_import_transport):
    fixture = managed_import_transport
    request_by_field = managed_document(fixture.external_target)
    body = transport._canonical_json_bytes(request_by_field)
    invalid = signed_headers(body, fixture.external_target)
    invalid[transport.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = permits._encoded(b"x" * 32)
    expired = signed_headers(
        body, fixture.external_target, issued_at="2000-01-01T00:00:00Z", expires_at="2000-01-01T00:01:00Z"
    )
    wrong = signed_headers(body, fixture.external_target | {"generation_id": fixture.target.generation_id + 1})
    for headers in (invalid, expired, wrong):
        reply = await _managed_post(fixture, request_by_field, headers=headers)
        assert reply.status == 404 and "managed_detail" not in reply.json
    assert not fixture.context.setup_statements
    assert not fixture.context.statements


@contextmanager
def _observe_collection_sql(session):
    """Retain the real aggregate result without changing SQL or Reader authority."""
    summaries = []

    def observe(execution):
        result = execution.invoke_statement()
        if str(execution.statement).startswith("WITH requested_offices AS MATERIALIZED"):
            frozen = result.freeze()
            summaries.append(dict(frozen().mappings().one()))
            return frozen()
        return result

    event.listen(session.sync_session, "do_orm_execute", observe, retval=True)
    try:
        yield summaries
    finally:
        event.remove(session.sync_session, "do_orm_execute", observe)


async def _collection_offices(context, session):
    manifest = context.manifest
    offices = (
        await session.execute(
            text(
                f"SELECT address.address_key,address.telephone_number,binding.location_id::text "
                f'FROM "{manifest.schema_name}".entity_address_unified address '
                f'JOIN "{manifest.schema_name}".provider_location_binding binding '
                "ON (binding.location_key,binding.entity_type,binding.entity_id)="
                "(address.location_key,address.entity_type,address.entity_id) "
                "WHERE coalesce(address.npi,address.inferred_npi)=:npi "
                "AND binding.provider_system='npi' AND binding.provider_id=:provider_id"
            ),
            {"npi": int(PROVIDER_NPI), "provider_id": PROVIDER_NPI},
        )
    ).all()
    assert len(offices) == 2 and {office[2] for office in offices} == set(map(str, context.sites))
    assert all(isinstance(office[0], UUID) for office in offices)
    return {office[2]: {"address_key": office[0], "telephone_number": office[1]} for office in offices}


def _contaminated_collection(offices, context):
    legacy_by_field = {key: [context.network, 2147483647] for key in metadata._LEGACY_ASSOCIATIONS}
    return [
        {
            "npi": int(PROVIDER_NPI),
            "qualifications": {"languages": ["English"]},
            "aca_network_array": [context.network, 2147483647],
            "address_list": [dict(address, **legacy_by_field) for address in offices.values()],
        },
        {
            "npi": int(ZERO_OFFICE_NPI),
            "address_groups": [{"members": [dict(offices[str(context.sites[0])], **legacy_by_field)]}],
        },
    ]


def _assert_collection_reader(context):
    assert not context.writer_statements and context.database.engine is None
    assert context.statements and all(
        identity[:4] == (context.role, context.role, "on", "repeatable read") for _, identity in context.statements
    )
    assert all(identity[5:] == (False, False) for _, identity in context.statements)
    assert len({identity[4] for _, identity in context.statements}) == 1
    assert None not in context.transactions and len(set(context.transactions)) == 1


@pytest.mark.parametrize("excluded", [False, True], ids=("default", "excluded"))
@pytest.mark.parametrize("published_profile_source", [True], indirect=True)
async def test_native_collection_projects_exact_provider_offices_once(
    managed_import_transport, excluded, record_property
):
    context = managed_import_transport.context
    query = parse_managed_detail_query({"npi": PROVIDER_NPI})
    exclusions = (context.network,) if excluded else ()
    async with provider_profile_read_snapshot(context.database, "mrf", include_detail=True) as session:
        async with managed_provider_detail_read_scope(
            session, query, excluded_network_ids=exclusions, access_scope_sha256="d" * 64
        ) as manifest:
            offices = await _collection_offices(context, session)
            record_property(
                "collection_uuid_types",
                sorted(
                    {
                        type(address["address_key"]).__module__ + "." + type(address["address_key"]).__name__
                        for address in offices.values()
                    }
                ),
            )
            provider_rows = _contaminated_collection(offices, context)
            original = deepcopy(provider_rows)
            with _observe_collection_sql(session) as summaries:
                projected = await metadata.project_managed_collection_metadata(
                    session, manifest, provider_rows, include_sources=True, include_evidence=True
                )
            assert len(summaries) == 1 and summaries[0]["bounded"] is True
            assert provider_rows == original and projected[0]["qualifications"] == original[0]["qualifications"]
            addresses = projected[0]["address_list"]
            assert all(address["telephone_number"] == "555-0100" for address in addresses)
            assert all(
                not (metadata._LEGACY_ASSOCIATIONS - {"network_memberships"}) & address.keys() for address in addresses
            )
            assert "aca_network_array" not in projected[0]
            memberships = [member for address in addresses for member in address["network_memberships"]]
            assert len(memberships) == (0 if excluded else 1)
            if not excluded:
                assert memberships[0]["network_id"] == context.network
                assert memberships[0]["provider_id"] == PROVIDER_NPI
                assert memberships[0]["location_id"] == str(context.sites[0])
                assert memberships[0]["evidence_ids"]
            foreign = projected[1]["address_groups"][0]["members"][0]
            assert foreign["telephone_number"] == "555-0100" and foreign["network_memberships"] == []
            assert not (metadata._LEGACY_ASSOCIATIONS - {"network_memberships"}) & foreign.keys()
    _assert_collection_reader(context)


@pytest.mark.parametrize("office", ["visible", "removed", "foreign-provider"])
@pytest.mark.parametrize("published_profile_source", [True], indirect=True)
async def test_native_selected_collection_refuses_unproved_exact_pairs(
    managed_import_transport, office, record_property
):
    context = managed_import_transport.context
    query = parse_managed_detail_query(
        {"npi": PROVIDER_NPI, "selector": {"namespace": "canonical", "values": [context.network]}}
    )
    async with provider_profile_read_snapshot(context.database, "mrf", include_detail=True) as session:
        async with managed_provider_detail_read_scope(
            session, query, excluded_network_ids=(), access_scope_sha256="d" * 64
        ) as manifest:
            offices = await _collection_offices(context, session)
            record_property(
                "collection_uuid_types",
                sorted(
                    {
                        type(address["address_key"]).__module__ + "." + type(address["address_key"]).__name__
                        for address in offices.values()
                    }
                ),
            )
            site = context.sites[1 if office == "removed" else 0]
            npi = ZERO_OFFICE_NPI if office == "foreign-provider" else PROVIDER_NPI
            provider_rows = [{"npi": int(npi), "address_list": [offices[str(site)]]}]
            expected = (
                nullcontext()
                if office == "visible"
                else pytest.raises(RegistryNetworkCatalogError, match="source_unavailable")
            )
            with _observe_collection_sql(session) as summaries, expected:
                projected_rows = await metadata.project_managed_collection_metadata(
                    session, manifest, provider_rows, include_sources=False, include_evidence=False
                )
                assert projected_rows[0]["address_list"][0]["network_memberships"][0]["location_id"] == str(site)
            assert len(summaries) == 1 and summaries[0]["bounded"] is True
    _assert_collection_reader(context)


@pytest.mark.parametrize("bound", ["facts", "bytes"])
@pytest.mark.parametrize("published_profile_source", [True], indirect=True)
async def test_native_collection_aggregate_refuses_before_fact_transfer(
    managed_import_transport, monkeypatch, bound, record_property
):
    context = managed_import_transport.context
    query = parse_managed_detail_query({"npi": PROVIDER_NPI})
    async with provider_profile_read_snapshot(context.database, "mrf", include_detail=True) as session:
        async with managed_provider_detail_read_scope(
            session, query, excluded_network_ids=(), access_scope_sha256="d" * 64
        ) as manifest:
            offices = await _collection_offices(context, session)
            record_property(
                "collection_uuid_types",
                sorted(
                    {
                        type(address["address_key"]).__module__ + "." + type(address["address_key"]).__name__
                        for address in offices.values()
                    }
                ),
            )
            provider_rows = [{"npi": int(PROVIDER_NPI), "address_list": [offices[str(context.sites[0])]]}]
            original_parameters = metadata.scoped_address_parameters

            def lower_query_budget(parameters):
                bounded_parameters = original_parameters(parameters)
                bounded_parameters["metadata_max_" + bound] = 0
                return bounded_parameters

            monkeypatch.setattr(metadata, "scoped_address_parameters", lower_query_budget)
            with _observe_collection_sql(session) as summaries:
                with pytest.raises(RegistryNetworkCatalogError, match="response_bounds"):
                    await metadata.project_managed_collection_metadata(
                        session, manifest, provider_rows, include_sources=True, include_evidence=True
                    )
            assert summaries == [{"bounded": False, "facts_json": None}]
    _assert_collection_reader(context)


@pytest.fixture
def signed_collection_coordinates(monkeypatch):
    """Prepare serving coordinates before Registry composition, preserving Source office types."""
    monkeypatch.setenv(GEO_SERVICE_LOCATIONS_ENV, "true")
    from tests import test_provider_profile_network_facts_postgres as profile_source

    original = profile_source._public_address_contacts

    async def prepare_coordinates(connection, source_table):
        await original(connection, source_table)
        await connection.execute(f"UPDATE {source_table} SET lat=37.5,long=-122.5")

    monkeypatch.setattr(profile_source, "_public_address_contacts", prepare_coordinates)


@pytest.fixture
async def signed_collection_transport(signed_collection_coordinates, managed_import_transport, monkeypatch):
    """Observe genuine import calls and preserve the route's original request session."""
    from sqlalchemy import MetaData

    from api.network_address_scope import current_network_address_scope
    from db.models import NUCCTaxonomy
    from process import registry_network_catalog_read as catalog
    from process.custom_import.read_core import CustomImportReadService

    fixture = managed_import_transport
    context = fixture.context
    model_table = NUCCTaxonomy.__table__.to_metadata(MetaData(), schema="mrf")
    async with context.fixture.engine.begin() as connection:
        await connection.run_sync(model_table.create)
        await connection.execute(text(f'GRANT SELECT ON mrf.nucc_taxonomy TO "{context.role}"'))
    observed = SimpleNamespace(fixture=fixture, pins=[], custody=[], import_reads=[], requests=[])
    original_pin, original_custody = catalog._pin, catalog.resolve_network_serving_manifest

    async def observe_pin(*args, **kwargs):
        observed.pins.append(args)
        return await original_pin(*args, **kwargs)

    async def observe_custody(*args, **kwargs):
        manifest = await original_custody(*args, **kwargs)
        observed.custody.append((args, kwargs, manifest))
        return manifest

    def observe_import_method(name):
        original = getattr(CustomImportReadService, name)

        async def observe(service, session, *args, **kwargs):
            scope = current_network_address_scope()
            assert scope.manifest.generation_id == context.manifest.generation_id
            assert observed.requests[-1][0].ctx.sa_session is session
            observed.import_reads.append((name, id(session)))
            return await original(service, session, *args, **kwargs)

        monkeypatch.setattr(CustomImportReadService, name, observe)

    monkeypatch.setattr(catalog, "_pin", observe_pin)
    monkeypatch.setattr(catalog, "resolve_network_serving_manifest", observe_custody)
    for name in ("prepare_npi_entity_relation", "scope_npi_entity_relation", "hydrate_npi_page"):
        observe_import_method(name)

    @context.app.middleware("request", priority=-100)
    async def remember_request(request):
        observed.requests.append((request, request.ctx.sa_session))

    @context.app.middleware("response")
    async def verify_restored_request(request, _reply):
        assert request.ctx.sa_session is observed.requests[-1][1]

    context.writer_statements.clear()
    context.statements.clear()
    context.setup_statements.clear()
    context.transactions.clear()
    yield observed


def _signed_collection_document(case, kind, policy, include_filter, *, page=False):
    from tests.test_network_catalog_routes import v2_envelope

    fixture = case.fixture
    context = fixture.context
    native_by_name = {"include_total": "true", "limit": "1" if page else "10", "primary_only": "false", "q": "taylor"}
    if kind == "geo":
        native_by_name.update(lat="37.5", long="-122.5", radius="1")
    else:
        native_by_name["npi"] = PROVIDER_NPI
        if page:
            native_by_name["offset"] = "1"
    query_by_field = {
        "kind": kind,
        "native_query": native_by_name,
        "network_generation": str(context.manifest.generation_id),
    }
    if policy == "selected":
        query_by_field["selector"] = {"namespace": "canonical", "values": [context.network]}
    request_by_field = {
        "target": fixture.external_target,
        "native_query": native_by_name,
        "context": [{"field_id": "service_code", "operator": "eq", "value": "SYNTHETIC_0"}],
        "filters": [{"field_id": "amount", "operator": "gte", "value": "12.00"}],
        "order": [{"field_id": "amount", "direction": "asc"}],
        "require_match": True,
        "include_filter": include_filter,
    }
    if kind == "batch":
        batch_by_field = {
            "npis": [PROVIDER_NPI],
            "address_limit": 5,
            "address_offset": 0,
            "include_sources": False,
            "include_evidence": False,
        }
        request_by_field["native_batch"] = query_by_field["native_batch"] = batch_by_field
    request_by_field["managed_collection"] = v2_envelope(
        query_by_field, excluded_network_ids=[context.network] if policy == "excluded" else []
    )
    return request_by_field


async def _signed_collection_post(case, kind, document, *, invalid_signature=False):
    context = case.fixture.context
    path = "/api/v1/extensions/custom-import/providers" + {"list": "", "geo": "/geo", "batch": "/batch"}[kind]
    body = transport._canonical_json_bytes(document)
    headers = signed_headers(body, case.fixture.external_target, path=path)
    if invalid_signature:
        headers[transport.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = permits._encoded(b"x" * 32)
    context.statements.clear()
    context.setup_statements.clear()
    context.transactions.clear()
    case.pins.clear()
    case.custody.clear()
    case.import_reads.clear()
    _, reply = await context.app.asgi_client.post(path, data=body, headers=headers)
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert all(request.ctx.sa_session is original for request, original in case.requests)
    assert not context.writer_statements and context.database.engine is None
    return reply


def _assert_collection_custody(case):
    """Require the original single native resolution and its exact authenticated manifest."""
    context = case.fixture.context
    assert len(case.pins) == len(case.custody) == 1
    arguments, options_by_name, manifest = case.custody[0]
    assert len(arguments) == 1 and arguments[0] is case.pins[0][0]
    assert case.pins[0][1:] == (context.manifest.generation_id, context.fixture.control_schema)
    assert options_by_name == {"generation_id": case.pins[0][1], "control_schema": case.pins[0][2]}
    assert arguments[0].get_server_pid() == context.statements[0][1][4]
    assert manifest == context.manifest
    seal = getattr(manifest, "_custody_seal_sha256", None)
    assert type(seal) is str and re.fullmatch(r"[0-9a-f]{64}", seal)


def _signed_collection_success(case, reply, document, *, include_filter, has_rows=True):
    from tests.test_registry_provider_collection_postgres import _providers

    context = case.fixture.context
    assert reply.status == 200, reply.json
    assert reply.json["managed_collection"] == {
        "client_id": document["managed_collection"]["client_id"],
        "policy_revision": document["managed_collection"]["policy_revision"],
        "generation": str(context.manifest.generation_id),
        "approved_custom_revision": str(context.manifest.approved_custom_revision),
        "manifest_sha256": context.manifest.manifest_sha256,
    }
    assert reply.headers["X-Network-Generation"] == str(context.manifest.generation_id)
    assert reply.headers["X-Network-Access-Scope"] == document["managed_collection"]["access_scope_sha256"]
    _assert_collection_reader(context)
    _assert_collection_custody(case)
    names = [name for name, _ in case.import_reads]
    assert "prepare_npi_entity_relation" in names and "scope_npi_entity_relation" in names
    assert len({session for _, session in case.import_reads}) == 1
    assert ("hydrate_npi_page" in names) is include_filter
    if has_rows:
        providers = _providers(document["managed_collection"]["query"]["kind"], reply.json)
        for provider in providers:
            assert ("custom_import" in provider) is include_filter
            if include_filter:
                assert provider["custom_import"]["target"] == case.fixture.external_target
                assert "SYNTHETIC_0" in json.dumps(provider["custom_import"])
    return reply.json


@pytest.mark.parametrize("published_profile_source", [True], indirect=True)
@pytest.mark.parametrize("kind", ["list", "geo", "batch"])
async def test_signed_collection_http_same_reader_policy_and_imports(signed_collection_transport, kind, tmp_path):
    from tests.test_registry_provider_collection_postgres import _assert_offices

    case = signed_collection_transport
    invalid = await _signed_collection_post(
        case, kind, _signed_collection_document(case, kind, "default", True), invalid_signature=True
    )
    assert invalid.status == 404 and "managed_collection" not in invalid.json
    assert not case.fixture.context.setup_statements
    assert not case.fixture.context.statements and not case.pins and not case.custody and not case.import_reads
    for policy in ("default", "selected", "excluded"):
        for include_filter in (True, False):
            document = _signed_collection_document(case, kind, policy, include_filter)
            reply = await _signed_collection_post(case, kind, document)
            (tmp_path / (kind + "-" + policy + "-" + str(include_filter) + ".json")).write_text(json.dumps(reply.json))
            data_by_field = _signed_collection_success(case, reply, document, include_filter=include_filter)
            _assert_offices(case.fixture.context, kind, data_by_field, policy)
    document = _signed_collection_document(case, kind, "selected", True, page=True)
    reply = await _signed_collection_post(case, kind, document)
    data_by_field = _signed_collection_success(case, reply, document, include_filter=True, has_rows=kind == "geo")
    if kind == "geo":
        assert data_by_field["total_count"] == 1 and data_by_field["has_more"] is False
    elif kind == "list":
        assert data_by_field["total"] == 1 and data_by_field["rows"] == []
    else:
        assert data_by_field["pagination"]["total"] == 1 and data_by_field["items"] == []


_SERVICE_NATIVE_PATH = "/api/v1/registry/serving/providers/collection"
_SERVICE_SIGNED_PATH = "/api/v1/extensions/custom-import/providers/by-service"
_SERVICE_OUTSIDE_NPI = "1000000005"
_SERVICE_GEOGRAPHY = {
    PROVIDER_NPI: ("Claims East", "NY", "10001"),
    ZERO_OFFICE_NPI: ("Claims Public", "NJ", "07001"),
    _SERVICE_OUTSIDE_NPI: ("Claims West", "CA", "94105"),
}


async def _seed_service_claims(context):
    """Create real claims-grain ORM rows without touching sealed directory facts."""
    from sqlalchemy import MetaData

    from api.endpoint import pricing

    metadata = MetaData()
    for model in (pricing.provider_table, pricing.provider_procedure_table, pricing.procedure_table):
        model.to_metadata(metadata, schema="mrf")
    async with context.fixture.engine.begin() as connection:
        await connection.run_sync(metadata.create_all)
        for index, (npi, geography) in enumerate(_SERVICE_GEOGRAPHY.items(), 1):
            await connection.execute(
                pricing.provider_table.insert().values(
                    provider_key=index,
                    npi=int(npi),
                    year=2024,
                    provider_name="Public Claims Provider " + str(index),
                    provider_type="Synthetic",
                    city=geography[0],
                    state=geography[1],
                    zip5=geography[2],
                )
            )
            await connection.execute(
                pricing.provider_procedure_table.insert().values(
                    npi=int(npi),
                    year=2024,
                    procedure_code=99213,
                    total_services=index * 10,
                    total_submitted_charges=index * 250,
                    total_allowed_amount=index * 200,
                    total_beneficiaries=index * 2,
                )
            )
        await connection.execute(
            pricing.procedure_table.insert().values(
                procedure_code=99213, source_year=2024, provider_count=3, service_description="Synthetic public service"
            )
        )
        for table in metadata.tables.values():
            await connection.execute(text(f'GRANT SELECT ON "mrf"."{table.name}" TO "{context.role}"'))
    missing_offices = await context.fixture.connection.fetchval(
        f'SELECT count(*) FROM "{context.manifest.schema_name}".entity_address_unified '
        "WHERE coalesce(npi,inferred_npi)=ANY($1::bigint[])",
        [int(ZERO_OFFICE_NPI), int(_SERVICE_OUTSIDE_NPI)],
    )
    assert missing_offices == 0


def _observe_service_failures(case, monkeypatch):
    """Retain genuine exception diagnostics while preserving the ordinary logger."""
    case.failures = []
    original = transport._log_failure

    def observe(failure):
        case.failures.append((type(failure).__name__, str(failure)))
        return original(failure)

    monkeypatch.setattr(transport, "_log_failure", observe)


@pytest.fixture
async def service_claims_transport(signed_collection_transport, monkeypatch):
    """Observe original native service, real pins and their actual Reader connection."""
    from api import custom_import_provider_service_http as service_http
    from process import registry_network_catalog_read as catalog

    case = signed_collection_transport
    context = case.fixture.context
    await _seed_service_claims(context)
    _observe_service_failures(case, monkeypatch)
    case.driver_proofs, case.native_reads, case.claim_sql = [], [], []
    networks = await context.fixture.connection.fetch(
        f'SELECT record_key::integer AS identity FROM "{context.fixture.control_schema}".registry_approved_record '
        "WHERE approved_revision=$1 AND record_kind='network' AND record_json->'archived'='false'::jsonb ORDER BY identity",
        context.manifest.approved_custom_revision,
    )
    case.networks = [entry["identity"] for entry in networks]
    original_pin, original_page = catalog._pin, service_http._service_page

    async def pin(*args, **kwargs):
        driver = args[0]
        proof = await driver.fetchrow(
            "SELECT session_user,current_user,current_setting('transaction_read_only'),"
            "current_setting('transaction_isolation'),pg_backend_pid(),"
            "(SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='virtualxid' AND granted)"
        )
        case.driver_proofs.append(tuple(proof))
        return await original_pin(*args, **kwargs)

    async def page(request, **kwargs):
        session = request.ctx.sa_session
        assert session.info["api_reader_verified"]
        case.native_reads.append(id(session))
        return await original_page(request, **kwargs)

    @context.app.middleware("request", priority=100)
    async def native_prior_session(request):
        if request.path == _SERVICE_NATIVE_PATH:
            request.ctx.sa_session = context.database._reader_database.session_factory()

    @event.listens_for(context.database._reader_database.engine.sync_engine, "after_cursor_execute")
    def observe_claims(connection, cursor, statement, parameters, execution, executemany):
        if "pricing_provider_procedure" in statement:
            assert len(case.claim_sql) < 100
            case.claim_sql.append(statement)

    monkeypatch.setattr(catalog, "_pin", pin)
    monkeypatch.setattr(service_http, "_service_page", page)
    context.writer_statements.clear()
    yield case


def _service_native_args(**changes):
    return {"code": "99213", "code_system": "HP_PROCEDURE_CODE", "year": "2024", "limit": "3"} | changes


def _service_policy(case, native_by_name, *, excluded):
    from tests.test_network_catalog_routes import v2_envelope

    return v2_envelope(
        {
            "kind": "by-service",
            "native_query": native_by_name,
            "network_generation": str(case.fixture.context.manifest.generation_id),
        },
        excluded_network_ids=case.networks if excluded else [],
    )


def _service_signed_document(case, *, require_match, include_filter, excluded, **paging):
    native_by_name = _service_native_args(**paging)
    return {
        "target": case.fixture.external_target,
        "native_query": native_by_name,
        "context": [{"field_id": "service_code", "operator": "eq", "value": "SYNTHETIC_0"}],
        "filters": [{"field_id": "amount", "operator": "gte", "value": "12.00"}] if require_match else [],
        "order": [{"field_id": "amount", "direction": "asc"}],
        "require_match": require_match,
        "include_filter": include_filter,
        "managed_collection": _service_policy(case, native_by_name, excluded=excluded),
    }


async def _service_http_post(case, document, *, signed, invalid=False, headers=None):
    context = case.fixture.context
    case.failures.clear()
    body = transport._canonical_json_bytes(document)
    path = _SERVICE_SIGNED_PATH if signed else _SERVICE_NATIVE_PATH
    if headers is None:
        headers = (
            signed_headers(body, case.fixture.external_target, path=path)
            if signed
            else {"Authorization": "Bearer synthetic-profile-control"}
        )
    if invalid:
        headers[transport.CUSTOM_IMPORT_READ_SIGNATURE_HEADER] = permits._encoded(b"x" * 32)
    for collection in (
        context.statements,
        context.setup_statements,
        context.transactions,
        case.pins,
        case.custody,
        case.import_reads,
        case.driver_proofs,
        case.native_reads,
        case.claim_sql,
    ):
        collection.clear()
    _, reply = await context.app.asgi_client.post(path, data=body, headers=headers)
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert all(request.ctx.sa_session is original for request, original in case.requests)
    assert not context.writer_statements and context.database.engine is None
    return reply


def _service_http_success(case, reply, document, *, signed, include_filter=False):
    context = case.fixture.context
    assert reply.status == 200, json.dumps({"response": reply.json, "failures": case.failures})
    _assert_collection_reader(context)
    assert len(case.driver_proofs) == len(case.native_reads) == 1
    _assert_collection_custody(case)
    proof = case.driver_proofs[0]
    assert proof[:4] == (context.role, context.role, "on", "repeatable read")
    assert proof[4] == context.statements[0][1][4] and proof[5] == context.transactions[0]
    policy = document["managed_collection"] if signed else document
    expected_by_field = {
        "client_id": policy["client_id"],
        "policy_revision": policy["policy_revision"],
        "generation": str(context.manifest.generation_id),
        "approved_custom_revision": str(context.manifest.approved_custom_revision),
        "manifest_sha256": context.manifest.manifest_sha256,
    }
    assert reply.headers["X-Network-Generation"] == expected_by_field["generation"]
    assert reply.headers["X-Network-Access-Scope"] == policy["access_scope_sha256"]
    if signed:
        assert reply.json["managed_collection"] == expected_by_field
        names = [name for name, _session in case.import_reads]
        assert "prepare_npi_entity_relation" in names and "scope_npi_entity_relation" in names
        assert {session for _name, session in case.import_reads} == set(case.native_reads)
        assert ("hydrate_npi_page" in names) is include_filter
    else:
        assert reply.json == expected_by_field | {"data": reply.json["data"]}
        assert not case.import_reads
    counts = [index for index, sql in enumerate(case.claim_sql) if "count(*)" in sql.lower() and "LIMIT" not in sql]
    pages = [index for index, sql in enumerate(case.claim_sql) if "LIMIT" in sql]
    assert counts and pages and counts[0] < pages[-1]
    return reply.json if signed else reply.json["data"]


def _assert_service_claim_items(items, *, signed, include_filter=False):
    for provider_item in items:
        npi = str(provider_item["npi"])
        assert type(provider_item["npi"]) is (str if signed else int)
        assert tuple(provider_item[name] for name in ("city", "state", "zip5")) == _SERVICE_GEOGRAPHY[npi]
        assert "locations" not in provider_item and "claims_geography" not in provider_item
        assert ("custom_import" in provider_item) is include_filter
        if include_filter:
            if npi == PROVIDER_NPI:
                assert provider_item["custom_import"] is not None and "SYNTHETIC_0" in json.dumps(
                    provider_item["custom_import"]
                )
            else:
                assert provider_item["custom_import"] is None


@pytest.mark.parametrize("published_profile_source", [True], indirect=True)
async def test_native_service_collection_preserves_addressless_claims_under_default_policy(service_claims_transport):
    case = service_claims_transport
    for excluded in (False, True):
        for limit, offset, expected in (
            ("3", "0", [_SERVICE_OUTSIDE_NPI, ZERO_OFFICE_NPI, PROVIDER_NPI]),
            ("2", "1", [ZERO_OFFICE_NPI, PROVIDER_NPI]),
            ("1", "2", [PROVIDER_NPI]),
        ):
            document = _service_policy(
                case, _service_native_args(limit=limit, offset=offset, order_by="total_services"), excluded=excluded
            )
            reply = await _service_http_post(case, document, signed=False)
            page_by_field = _service_http_success(case, reply, document, signed=False)
            assert page_by_field["pagination"]["total"] == 3 and page_by_field["pagination"]["offset"] == int(offset)
            assert [str(provider_item["npi"]) for provider_item in page_by_field["items"]] == expected
            _assert_service_claim_items(page_by_field["items"], signed=False)
    malformed = _service_policy(case, _service_native_args(), excluded=True)
    malformed["query"]["native_query"]["billing_entity_ref"] = "forbidden"
    rejected = await _service_http_post(case, malformed, signed=False)
    assert not case.fixture.context.setup_statements
    assert rejected.status == 400 and not case.fixture.context.statements and not case.pins
    unauthenticated = await _service_http_post(
        case,
        _service_policy(case, _service_native_args(), excluded=True),
        signed=False,
        headers={"Authorization": "Bearer invalid-control"},
    )
    assert not case.fixture.context.setup_statements
    assert unauthenticated.status == 403 and not case.fixture.context.statements and not case.pins


async def _assert_service_optional_metric_refusal(case):
    document = _service_signed_document(case, require_match=False, include_filter=True, excluded=True)
    document["filters"] = [{"field_id": "amount", "operator": "gte", "value": "12.00"}]
    refused = await _service_http_post(case, document, signed=True)
    assert refused.status == 400
    assert case.failures == [("CustomImportReadRequestError", "metric predicates require imported membership")]
    _assert_collection_reader(case.fixture.context)
    _assert_collection_custody(case)
    assert not case.native_reads


async def _assert_service_empty_required_page(case):
    document = _service_signed_document(
        case, require_match=True, include_filter=True, excluded=True, limit="1", offset="1"
    )
    reply = await _service_http_post(case, document, signed=True)
    page_by_field = _service_http_success(case, reply, document, signed=True, include_filter=True)
    assert page_by_field["pagination"]["total"] == 1 and page_by_field["items"] == []


@pytest.mark.parametrize("published_profile_source", [True], indirect=True)
async def test_signed_service_http_reader_policy_claims_and_native_complement(service_claims_transport):
    case = service_claims_transport
    invalid = await _service_http_post(
        case,
        _service_signed_document(case, require_match=True, include_filter=True, excluded=True),
        signed=True,
        invalid=True,
    )
    assert not case.fixture.context.setup_statements
    assert invalid.status == 404 and not case.fixture.context.statements and not case.pins and not case.import_reads
    await _assert_service_optional_metric_refusal(case)
    await _assert_service_empty_required_page(case)
    for excluded in (False, True):
        for require_match in (True, False):
            for include_filter in (True, False):
                document = _service_signed_document(
                    case, require_match=require_match, include_filter=include_filter, excluded=excluded
                )
                reply = await _service_http_post(case, document, signed=True)
                page_by_field = _service_http_success(case, reply, document, signed=True, include_filter=include_filter)
                expected = [PROVIDER_NPI] if require_match else [PROVIDER_NPI, _SERVICE_OUTSIDE_NPI, ZERO_OFFICE_NPI]
                assert page_by_field["pagination"]["total"] == len(expected)
                assert [provider_item["npi"] for provider_item in page_by_field["items"]] == expected
                _assert_service_claim_items(page_by_field["items"], signed=True, include_filter=include_filter)
    document = _service_signed_document(
        case, require_match=False, include_filter=True, excluded=True, limit="1", offset="1"
    )
    reply = await _service_http_post(case, document, signed=True)
    page_by_field = _service_http_success(case, reply, document, signed=True, include_filter=True)
    assert page_by_field["pagination"]["total"] == 3 and page_by_field["items"][0]["npi"] == _SERVICE_OUTSIDE_NPI
    _assert_service_claim_items(page_by_field["items"], signed=True, include_filter=True)
    malformed = _service_signed_document(case, require_match=True, include_filter=True, excluded=True)
    malformed["managed_collection"]["query"]["selector"] = {"namespace": "canonical", "values": []}
    rejected = await _service_http_post(case, malformed, signed=True)
    assert not case.fixture.context.setup_statements
    assert rejected.status == 400 and not case.fixture.context.statements and not case.pins and not case.import_reads
