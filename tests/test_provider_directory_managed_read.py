# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Full-policy Source transport, active manifest reuse and default cursor scope."""

import asyncio
import base64
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sanic import Blueprint, Sanic

from api import provider_collection_network_read as collection
from api import provider_directory_cms_entities as cms
from api import provider_directory_cms_network_scope as scoped
from api import provider_directory_entities_contract as contract
from api import provider_directory_managed_read as managed
from api import provider_directory_medical_groups as doctors
from api.endpoint import network_catalog as endpoint
from api.network_address_scope import current_network_address_scope
from process import registry_network_catalog_read as catalog
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from tests.network_read_execution_support import with_read_identity_receipts
from tests.test_network_catalog_routes import v2_envelope
from tests.test_provider_collection_network_read import boundary as boundary
from tests.test_provider_directory_entities import ENTITY_ID, KEY
from tests.test_provider_directory_medical_groups_identity import _catalog

PATH = "/api/v1/registry/serving/directory/entities"
AUTH = {"Authorization": "Bearer synthetic-source-control-token"}
_CATALOG_SNAPSHOT = catalog.registry_catalog_read_snapshot


def _query(source="cms-npd", **changes):
    return {
        "kind": "medical-groups" if source == "cms-doctors" else "networks",
        "source_id": source,
        "shape": "entities",
        "limit": 25,
    } | changes


async def _send(held, query=None, *, document=None, raw=None, headers=None, suffix="", method="post"):
    body = (
        json.dumps(document if document is not None else v2_envelope(query or _query())).encode()
        if raw is None
        else raw
    )
    arguments_by_name = {"headers": AUTH if headers is None else headers}
    if method != "get":
        arguments_by_name["data"] = body
    _, reply = await getattr(held.app.asgi_client, method)(PATH + suffix, **arguments_by_name)
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert current_network_address_scope() is None
    for request in held.requests:
        assert request.ctx.sa_session is held.prior
    return reply


@pytest.fixture
def directory_boundary(boundary, monkeypatch):
    held = boundary
    held.prior = object()
    held.requests, held.reads = [], []
    held.session.execute = AsyncMock()
    held.session.in_transaction = lambda: held.state.active
    held.driver.is_in_transaction = lambda: held.state.active
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-source-control-token")
    monkeypatch.setenv(contract.CURSOR_KEY_ENV, base64.urlsafe_b64encode(KEY).decode().rstrip("="))
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "sample_registry")
    monkeypatch.setattr(managed, "provider_profile_read_snapshot", collection.provider_profile_read_snapshot)
    monkeypatch.setattr(catalog, "_pin", AsyncMock(return_value=held.manifest))
    monkeypatch.setattr(catalog, "registry_catalog_read_snapshot", _CATALOG_SNAPSHOT)

    @asynccontextmanager
    async def bounded(session, *, timeout_ms):
        assert session is held.session and timeout_ms == 2_000
        yield

    async def source_read(session, query, *, response_headers=None):
        assert session is held.session and held.requests[-1].ctx.sa_session is session
        assert query.network_ids is None and query.excluded_network_ids == ()
        assert query.network_manifest is held.manifest and query.network_generation == 9
        assert current_network_address_scope().manifest is held.manifest
        assert catalog._reuse_manifest(held.driver, query.network_manifest, 9, "sample_registry") is held.manifest
        held.reads.append(query)
        return {"generation_id": "gen_source", "items": [], "next_cursor": None}

    monkeypatch.setattr(managed, "_bounded_read_window", bounded)
    monkeypatch.setattr(managed, "read_cms_entities_snapshot", source_read)
    monkeypatch.setattr(managed, "read_medical_groups_snapshot", source_read)
    held.app = Sanic("managed_source_" + uuid4().hex)
    held.app.blueprint(Blueprint.group([endpoint.blueprint], version_prefix="/api/v"))
    endpoint.register_network_catalog_private_responses(held.app)

    @held.app.middleware("request")
    async def bind(request):
        request.ctx.sa_session = held.prior
        held.requests.append(request)

    return held


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["cms-npd", "cms-doctors"])
async def test_full_policy_source_uses_one_reader_and_retains_original_data(directory_boundary, source):
    held = directory_boundary
    document = v2_envelope(_query(source))
    reply = await _send(held, document=document)
    assert reply.status == 200, reply.json
    assert reply.json == {
        "client_id": document["client_id"],
        "policy_revision": document["policy_revision"],
        "generation": "9",
        "approved_custom_revision": "4",
        "manifest_sha256": "b" * 64,
        "data": {"generation_id": "gen_source", "items": [], "next_cursor": None},
    }
    assert reply.headers.get_list("X-Network-Generation") == ["9"]
    assert reply.headers.get_list("X-Network-Access-Scope") == [document["access_scope_sha256"]]
    assert "X-CMS-Retained-Publication" not in reply.headers
    assert len(held.reads) == len(held.state.snapshots) == 1 and not held.state.active
    assert [str(call.args[0]) for call in held.session.execute.await_args_list] == [
        "SET LOCAL lock_timeout = '250ms'",
        "SET LOCAL work_mem = '4MB'",
    ]
    catalog._pin.assert_awaited_once_with(held.driver, None, "sample_registry")
    assert catalog._PINNED_READ.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong"}])
async def test_source_authentication_precedes_parse_and_reader(directory_boundary, headers):
    reply = await _send(directory_boundary, raw=b"\xff", headers=headers)
    assert reply.status == 403 and reply.json == {"error": {"code": "network_catalog_forbidden"}}
    assert not directory_boundary.state.snapshots and not directory_boundary.reads
    catalog._pin.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["version", "digest", "extra", "private", "selector", "duplicate", "query", "body"])
async def test_source_policy_and_query_are_closed_before_reader(directory_boundary, damage):
    document = v2_envelope(_query())
    raw, suffix = None, ""
    match damage:
        case "version":
            document.pop("contract_version")
        case "digest":
            document["access_scope_sha256"] = "0" * 64
        case "extra":
            document["source"] = "untrusted"
        case "private":
            document["query"]["excluded_network_ids"] = []
        case "selector":
            document["query"]["network_ids"] = [7]
        case "duplicate":
            raw = json.dumps(document).encode()[:-1] + b',"client_id":"other"}'
        case "query":
            suffix = "?network_generation=9"
        case _:
            raw = b"x" * (1024 * 1024 + 1)
    reply = await _send(directory_boundary, document=document, raw=raw, suffix=suffix)
    assert reply.status == 400 and set(reply.json) == {"error"}
    assert not directory_boundary.state.snapshots and not directory_boundary.reads
    catalog._pin.assert_not_awaited()


@pytest.mark.parametrize(
    "changes",
    [
        {"limit": True},
        {"limit": 0},
        {"limit": 101},
        {"limit": "25"},
        {"network_generation": 9},
        {"network_generation": "09"},
        {"network_generation": None},
        {"source_id": "other"},
        {"kind": "medical-groups"},
        {"shape": "entity"},
        {"entity_id": ENTITY_ID},
        {"shape": "entity", "entity_id": ENTITY_ID, "cursor": "opaque", "generation_id": "gen_source"},
    ],
)
def test_managed_source_native_query_bounds(changes):
    with pytest.raises((ValueError, contract.DirectoryReadError)):
        managed.parse_managed_directory_query(_query() | changes)


def test_private_defaults_are_append_only_and_never_admitted_by_get():
    query = contract.DirectoryRead("networks", "cms-npd", "entities", None, None)
    assert query.scope("gen_source") == ["networks", "cms-npd", "entities", None, "gen_source", 25]
    assert not query.is_network_scoped
    scoped_query = replace(query, excluded_network_ids=(), network_generation=9, access_scope_sha256="a" * 64)
    assert scoped_query.is_network_scoped and scoped_query.scope("gen_source")[-3:] == [None, 9, "a" * 64]
    for field in ("excluded_network_ids", "network_manifest"):
        with pytest.raises(contract.DirectoryReadError):
            contract.parse_directory_read("networks", None, "entities", "source_id=cms-npd&" + field + "=opaque")
    entity = managed.parse_managed_directory_query(_query(shape="entity", entity_id=ENTITY_ID, limit=100))
    assert entity.limit == 100 and entity.entity_id == ENTITY_ID


def _cursor_plaintext(token, query, generation="gen_source"):
    encoded = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
    aad = b"provider-directory.cursor.v1\0" + json.dumps(query.scope(generation), separators=(",", ":")).encode()
    return json.loads(AESGCM(KEY).decrypt(encoded[:12], encoded[12:], aad))


def test_default_cursor_keeps_original_expiry_and_old_scope_bytes(monkeypatch):
    query = contract.DirectoryRead("networks", "cms-npd", "entities", None, None)
    selected = replace(query, network_ids=(7,), network_generation=9, access_scope_sha256="a" * 64)
    default = replace(query, excluded_network_ids=(), network_generation=9, access_scope_sha256="a" * 64)
    monkeypatch.setattr(contract.os, "urandom", lambda count: b"n" * count)
    for legacy in (query, selected):
        token = contract.issue_directory_cursor(KEY, legacy, "gen_source", ENTITY_ID, now=100)
        scope = json.dumps(legacy.scope("gen_source"), separators=(",", ":")).encode()
        plaintext = json.dumps([ENTITY_ID, 1000], separators=(",", ":")).encode()
        expected = (
            base64.urlsafe_b64encode(
                b"n" * 12 + AESGCM(KEY).encrypt(b"n" * 12, plaintext, b"provider-directory.cursor.v1\0" + scope)
            )
            .decode()
            .rstrip("=")
        )
        assert token == expected
    first = contract.issue_directory_cursor(KEY, default, "gen_source", ENTITY_ID, now=100)
    continued = replace(default, cursor=first)
    next_cursor = contract.issue_directory_cursor(KEY, continued, "gen_source", ENTITY_ID, now=500)
    assert _cursor_plaintext(next_cursor, default)[1] == 1000
    for changed in ({"access_scope_sha256": "b" * 64}, {"network_generation": 10}, {"limit": 50}):
        with pytest.raises(contract.DirectoryReadError) as caught:
            contract.read_directory_cursor(KEY, replace(continued, **changed), "gen_source", now=501)
        assert caught.value.status == 409
    legacy_cursor = contract.issue_directory_cursor(KEY, query, "gen_source", ENTITY_ID, now=100)
    with pytest.raises(contract.DirectoryReadError):
        contract.read_directory_cursor(KEY, replace(default, cursor=legacy_cursor), "gen_source", now=501)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "shape,status", [("entity", 404), ("relationships", 404), ("entities", 404), ("entities", 409), ("entities", 503)]
)
async def test_source_errors_are_sanitized_and_absence_is_specific(directory_boundary, monkeypatch, shape, status):
    async def denied(*args, **options):
        raise contract.DirectoryReadError(status)

    monkeypatch.setattr(managed, "read_cms_entities_snapshot", denied)
    query = _query(shape=shape) | ({"entity_id": ENTITY_ID} if shape != "entities" else {})
    reply = await _send(directory_boundary, query)
    expected = (
        "provider_directory_entity_not_found"
        if status == 404 and shape != "entities"
        else {
            404: "network_catalog_not_found",
            409: "network_catalog_source_changed",
            503: "network_catalog_unavailable",
        }[status]
    )
    assert reply.status == status and reply.json == {"error": {"code": expected}}
    assert not directory_boundary.state.active and catalog._PINNED_READ.get() is None


@pytest.mark.asyncio
async def test_source_deadline_is_sanitized_and_restores_reader(directory_boundary, monkeypatch):
    monkeypatch.setattr(managed, "_SOURCE_TIMEOUT_MS", 1)

    @asynccontextmanager
    async def delayed_reader(*args, **options):
        await asyncio.sleep(0.1)
        yield directory_boundary.session

    monkeypatch.setattr(managed, "provider_profile_read_snapshot", delayed_reader)
    reply = await _send(directory_boundary)
    assert reply.status == 503 and reply.json == {"error": {"code": "network_catalog_unavailable"}}
    assert not directory_boundary.state.active and not directory_boundary.reads
    catalog._pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_response_bound_and_framework_refusals_are_private(directory_boundary, monkeypatch):
    monkeypatch.setattr(managed, "MAX_DIRECTORY_BYTES", 100)
    reply = await _send(directory_boundary)
    assert reply.status == 503 and set(reply.json) == {"error"}
    refused = await _send(directory_boundary, suffix="/", method="get")
    assert refused.status in {404, 405}
    assert not directory_boundary.state.active


@pytest.mark.asyncio
async def test_cms_default_reuses_only_active_manifest_without_selected_or_office_queries(
    directory_boundary, monkeypatch
):
    held = directory_boundary
    source_pin = PinnedFHIRMembershipSource(
        "retained", "cms-npd", "endpoint", "dataset", "a" * 64, "release", 1, "alias", "2026-01-01"
    )
    coordinates = RegistryNetworkSourceCoordinates("fhir", "cms-npd", "retained", "dataset", "producer", "edition")
    recipe = RegistrySourceMembershipRecipe(source_pin, coordinates)
    monkeypatch.setattr(scoped, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(recipe,)))
    monkeypatch.setattr(
        scoped, "pin_retained_approved_membership_source", AsyncMock(return_value=ApprovedMembershipSource(4, "c" * 64))
    )
    monkeypatch.setattr(
        scoped, "_scope_source_generation", AsyncMock(return_value={"dataset_id": "dataset", "release_id": "release"})
    )
    monkeypatch.setattr(scoped, "_require_pinned_source", AsyncMock())
    second_pin = AsyncMock(side_effect=AssertionError("unexpected current-head lookup"))
    monkeypatch.setattr(scoped, "resolve_network_serving_manifest", second_pin)
    held.session.scalar = AsyncMock(side_effect=AssertionError("unexpected membership dependency"))

    async def settings_only(statement):
        assert str(statement) in {"SET LOCAL lock_timeout = '250ms'", "SET LOCAL work_mem = '4MB'"}

    held.session.execute = AsyncMock(side_effect=settings_only)
    query = replace(
        managed.parse_managed_directory_query(_query()),
        excluded_network_ids=(),
        network_manifest=held.manifest,
        network_generation=9,
    )
    async with managed._managed_directory_scope(held.request, query, v2_envelope(_query()), (), "a" * 64):
        scope = await scoped.pin_cms_network_scope(held.session, '"control"', query, None)
        assert scope.manifest is held.manifest and scope.excluded_network_ids == ()
        assert scope.parameters_by_name["scope_network_ids"] is None
        assert scope.parameters_by_name["scope_excluded_network_ids"] == []
    second_pin.assert_not_awaited()
    catalog._pin.assert_awaited_once()
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="source_changed"):
        await scoped.pin_cms_network_scope(held.session, '"control"', query, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["medical-groups", "sites"])
async def test_doctors_default_stays_public_on_same_snapshot(directory_boundary, monkeypatch, kind):
    held = directory_boundary
    with_read_identity_receipts(held.session, driver=held.driver)
    held.driver = held.session.connection.return_value.get_raw_connection.return_value.driver_connection

    async def execute(statement, parameters=None):
        sql = str(statement)
        if "current_setting('transaction_isolation')" in sql:
            return SimpleNamespace(one=lambda: ("repeatable read", "on"))
        if "pg_catalog.pg_class" in sql:
            assert parameters == {"names": list(doctors._READ_TABLES), "schema": "mrf"}
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=_catalog))
        assert sql in {"SET LOCAL lock_timeout = '250ms'", "SET LOCAL work_mem = '4MB'"}

    held.session.execute.side_effect = execute
    monkeypatch.setattr(managed, "read_medical_groups_snapshot", doctors.read_medical_groups_snapshot)
    monkeypatch.setattr(doctors, "_accepted_generation", AsyncMock(return_value="gen_doctors"))
    monkeypatch.setattr(doctors, "_require_complete_bindings", AsyncMock())
    record_by_field = {
        "organization_id": ENTITY_ID,
        "first_name": "Public source",
        "last_name": "Public source",
        "generation_id": "published",
        "row_number": 1,
        "observed_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    if kind == "sites":
        record_by_field["site_id"] = ENTITY_ID
    source_rows = AsyncMock(return_value=[record_by_field])
    monkeypatch.setattr(doctors, "_entity_rows", source_rows)
    monkeypatch.setattr(doctors, "_site_rows", source_rows)
    document = v2_envelope(_query("cms-doctors", kind=kind), excluded_network_ids=[7, 9])
    reply = await _send(held, document=document)
    assert reply.status == 200 and reply.json["data"]["items"][0]["id"] == ENTITY_ID
    assert reply.json["data"]["items"][0]["display_name"] == "Public source"
    assert reply.json["data"]["generation_id"] != "gen_doctors"
    assert "X-CMS-Retained-Publication" not in reply.headers
    assert source_rows.await_args.args[0] is held.session and source_rows.await_args.args[2].network_ids is None
    assert source_rows.await_args.args[2].excluded_network_ids == (7, 9)


@pytest.mark.asyncio
async def test_source_cancellation_restores_the_real_request(directory_boundary, monkeypatch):
    held = directory_boundary

    async def cancel(*args, **options):
        raise asyncio.CancelledError()

    monkeypatch.setattr(managed, "read_cms_entities_snapshot", cancel)
    request = SimpleNamespace(ctx=SimpleNamespace(sa_session=held.prior))
    with pytest.raises(asyncio.CancelledError):
        await managed.read_managed_directory(
            request,
            managed.parse_managed_directory_query(_query()),
            policy=v2_envelope(_query()),
            exclusions=(),
            access_scope_sha256="a" * 64,
        )
    assert request.ctx.sa_session is held.prior and current_network_address_scope() is None
    assert not held.state.active and catalog._PINNED_READ.get() is None
