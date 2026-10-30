# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed collection HTTP reads preserve exact approved offices and Reader custody."""

import json
from types import SimpleNamespace

import pytest
from sqlalchemy import MetaData, text

from api import provider_detail_network_metadata as metadata
from api.endpoint.npi import GEO_SERVICE_LOCATIONS_ENV
from db.models import NUCCTaxonomy
from process import registry_network_catalog_read as catalog
from tests import test_provider_profile_network_facts_postgres as profile_source
from tests.test_network_catalog_routes import v2_envelope
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template
from tests.test_provider_profile_network_facts_postgres import published_profile_source as published_profile_source
from tests.test_registry_provider_profile_transport_postgres import profile_transport as profile_transport

pytestmark = pytest.mark.asyncio
_PATH = "/api/v1/registry/serving/providers/collection"
_AUTH = {"Authorization": "Bearer synthetic-profile-control"}
_NPI = "1000000491"


@pytest.fixture(autouse=True)
def native_collection_coordinates(monkeypatch):
    """Give the genuine source offices coordinates before their immutable publication."""
    monkeypatch.setenv(GEO_SERVICE_LOCATIONS_ENV, "true")
    original = profile_source._public_address_contacts

    async def prepare_coordinates(connection, source_table):
        await original(connection, source_table)
        await connection.execute(f"UPDATE {source_table} SET lat=37.5,long=-122.5")

    monkeypatch.setattr(profile_source, "_public_address_contacts", prepare_coordinates)


@pytest.fixture
async def collection_transport(profile_transport, monkeypatch):
    context = profile_transport
    model_table = NUCCTaxonomy.__table__.to_metadata(MetaData(), schema="mrf")
    async with context.fixture.engine.begin() as connection:
        await connection.run_sync(model_table.create)
        await connection.execute(text(f'GRANT SELECT ON mrf.nucc_taxonomy TO "{context.role}"'))
    requests, pins, custody = [], [], []
    original_pin, original_custody = catalog._pin, catalog.resolve_network_serving_manifest

    async def observe_pin(*args, **kwargs):
        pins.append(args)
        return await original_pin(*args, **kwargs)

    async def observe_custody(*args, **kwargs):
        manifest = await original_custody(*args, **kwargs)
        custody.append((args, kwargs, manifest))
        return manifest

    monkeypatch.setattr(catalog, "_pin", observe_pin)
    monkeypatch.setattr(catalog, "resolve_network_serving_manifest", observe_custody)
    sentinel = object()

    @context.app.middleware("request")
    async def remember_request(request):
        request.ctx.sa_session = sentinel
        requests.append(request)

    @context.app.middleware("response")
    async def verify_restored_request(request, reply):
        assert request.ctx.sa_session is sentinel

    context.writer_statements.clear()
    context.statements.clear()
    context.transactions.clear()
    yield SimpleNamespace(context=context, requests=requests, pins=pins, custody=custody, sentinel=sentinel)


def _query(kind):
    native_by_name = {"include_total": "true", "limit": "10", "primary_only": "false"}
    query_by_field = {"kind": kind, "native_query": native_by_name}
    if kind == "list":
        native_by_name["npi"] = _NPI
    elif kind == "geo":
        native_by_name.update(lat="37.5", long="-122.5", radius="1")
    else:
        query_by_field["native_query"] = {}
        query_by_field["native_batch"] = {"npis": [_NPI], "address_limit": 5, "address_offset": 0}
    return query_by_field


def _reader_evidence(case):
    context = case.context
    assert not context.writer_statements and context.database.engine is None
    assert context.statements and all(
        identity[:4] == (context.role, context.role, "on", "repeatable read") for _, identity in context.statements
    )
    assert all(identity[5:] == (False, False) for _, identity in context.statements)
    assert len({identity[4] for _, identity in context.statements}) == 1
    assert None not in context.transactions and len(set(context.transactions)) == 1
    assert len(case.pins) == len(case.custody) == 1
    arguments, options_by_name, manifest = case.custody[0]
    assert len(arguments) == 1 and arguments[0] is case.pins[0][0]
    assert options_by_name == {"generation_id": case.pins[0][1], "control_schema": case.pins[0][2]}
    assert manifest == context.manifest
    seal = getattr(manifest, "_custody_seal_sha256", None)
    assert type(seal) is str and len(seal) == 64 and set(seal) <= set("0123456789abcdef")
    assert all(request.ctx.sa_session is case.sentinel for request in case.requests)


async def _post(case, document, *, headers=None):
    context = case.context
    context.statements.clear()
    context.transactions.clear()
    case.pins.clear()
    case.custody.clear()
    _, reply = await context.app.asgi_client.post(
        _PATH, data=json.dumps(document).encode(), headers=_AUTH if headers is None else headers
    )
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert not context.writer_statements
    return reply


def _success(case, reply, document):
    context = case.context
    assert reply.status == 200, reply.json
    assert set(reply.json) == {
        "client_id",
        "policy_revision",
        "generation",
        "approved_custom_revision",
        "manifest_sha256",
        "data",
    }
    assert reply.json["generation"] == reply.headers["X-Network-Generation"] == str(context.manifest.generation_id)
    assert reply.json["approved_custom_revision"] == str(context.manifest.approved_custom_revision)
    assert reply.json["manifest_sha256"] == context.manifest.manifest_sha256
    assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    _reader_evidence(case)
    return reply.json["data"]


def _providers(kind, document):
    if kind == "list":
        return document["rows"]
    if kind == "geo":
        return document["items"]
    assert document["found"] == 1 and document["not_found"] == 0
    assert all(item["status"] == 200 for item in document["items"])
    return [item["provider"] for item in document["items"]]


def _assert_offices(context, kind, document, policy):
    providers = _providers(kind, document)
    assert providers and all(int(provider["npi"]) == int(_NPI) for provider in providers)
    addresses = [address for provider in providers for address in metadata._addresses(provider)]
    assert all(address["telephone_number"] == "555-0100" for address in addresses)
    assert len({address["address_key"] for address in addresses}) == (1 if policy == "selected" else 2)
    forbidden_fields = metadata._LEGACY_ASSOCIATIONS - {"network_memberships"}
    assert all(not forbidden_fields & address.keys() for address in [*providers, *addresses])
    memberships = [member for address in addresses for member in address["network_memberships"]]
    if policy == "excluded":
        assert memberships == []
    else:
        assert memberships and all(
            member["network_id"] == context.network
            and member["provider_id"] == _NPI
            and member["location_id"] == str(context.sites[0]) != str(context.sites[1])
            for member in memberships
        )
    if kind == "geo":
        assert document["total_count"] == (1 if policy == "selected" else 2)
    elif kind == "list":
        assert document["total"] == 1
    else:
        assert document["pagination"]["total"] == 1


@pytest.mark.parametrize("published_profile_source", [True], indirect=True)
@pytest.mark.parametrize("kind", ["list", "geo", "batch"])
async def test_native_collection_http_default_selected_and_excluded_offices(collection_transport, kind, tmp_path):
    case = collection_transport
    for policy in ("default", "selected", "excluded"):
        query_by_field = _query(kind)
        query_by_field["network_generation"] = str(case.context.manifest.generation_id)
        if policy == "selected":
            query_by_field["selector"] = {"namespace": "canonical", "values": [case.context.network]}
        document = v2_envelope(
            query_by_field, excluded_network_ids=[case.context.network] if policy == "excluded" else []
        )
        reply = await _post(case, document)
        (tmp_path / (kind + "-" + policy + ".json")).write_text(json.dumps(reply.json))
        data_by_field = _success(case, reply, document)
        _assert_offices(case.context, kind, data_by_field, policy)
    query_by_field = _query(kind)
    query_by_field["selector"] = {"namespace": "canonical", "values": [case.context.network]}
    if kind == "geo":
        query_by_field["native_query"]["limit"] = "1"
    else:
        query_by_field["native_query"].update(limit="1", offset="1")
    document = v2_envelope(query_by_field)
    data_by_field = _success(case, await _post(case, document), document)
    if kind == "geo":
        assert data_by_field["total_count"] == 1 and data_by_field["has_more"] is False
    elif kind == "list":
        assert data_by_field["total"] == 1 and data_by_field["rows"] == []
    else:
        assert data_by_field["pagination"]["total"] == 1 and data_by_field["items"] == []


@pytest.mark.parametrize("published_profile_source", [True], indirect=True)
async def test_native_legacy_collection_reuses_the_exact_accounted_manifest(collection_transport):
    case = collection_transport
    context = case.context
    legacy_uuid = await context.fixture.connection.fetchval(
        f"SELECT record_json#>>'{{source_scope_json,legacy_uuid}}' "
        f'FROM "{context.fixture.control_schema}".registry_approved_record '
        "WHERE approved_revision=$1 AND record_kind='network_binding' "
        "AND record_json->>'network_id'=$2",
        context.manifest.approved_custom_revision,
        str(context.network),
    )
    assert legacy_uuid is not None
    query_by_field = _query("batch")
    query_by_field.update(
        selector={"namespace": "legacy_fhir_uuid", "values": [legacy_uuid]},
        network_generation=str(context.manifest.generation_id),
    )
    document = v2_envelope(query_by_field)
    data_by_field = _success(case, await _post(case, document), document)
    _assert_offices(context, "batch", data_by_field, "selected")
    assert case.pins[0][1:] == (context.manifest.generation_id, context.fixture.control_schema)
    assert case.custody[0][0][0] is case.pins[0][0]


async def test_native_collection_authority_refusal_precedes_reader(collection_transport):
    case = collection_transport
    document = v2_envelope(_query("list"))
    wrong = await _post(case, document, headers={"Authorization": "Bearer wrong"})
    assert wrong.status == 403
    assert not case.context.statements and not case.pins and not case.custody
    invalid = await _post(case, document | {"access_scope_sha256": "0" * 64})
    assert invalid.status == 400
    assert not case.context.statements and not case.pins and not case.custody
    typed = await _post(case, document | {"policy_revision": True})
    assert typed.status == 400
    assert not case.context.statements and not case.pins and not case.custody
