# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Signed managed composition preserves policy, eligibility and exact identity."""

import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage, NotFound
from sqlalchemy.dialects import postgresql

from api import custom_import_provider_batch as batch
from api import custom_import_provider_detail as managed
from api import custom_import_read_http as transport
from api import provider_batch
from api.endpoint import npi
from api.network_address_scope import current_network_address_scope
from process.custom_import.read_contracts import CustomImportReadRequestError, CustomImportReadUnavailableError
from process.registry_network_catalog_read import RegistryNetworkCatalogError
from tests import test_custom_import_provider_batch as batch_fixtures
from tests import test_custom_import_read_http as permits
from tests.test_network_catalog_routes import v2_envelope
from tests.test_provider_detail_network_read import NPI
from tests.test_provider_detail_network_read import detail_boundary as detail_boundary


def _body(**changes):
    native_batch = json.loads(batch_fixtures._body())["native_batch"] | {"npis": [NPI]}
    return batch_fixtures._body(
        native_query={"include_total": "true"},
        native_batch=native_batch,
        managed_detail=v2_envelope({"npi": NPI}),
        **changes,
    )


def _verified(body, parsed):
    return transport._verify_provider_transport(
        headers=permits._provider_headers(body=body, path=batch.CUSTOM_IMPORT_PROVIDER_BATCH_PATH),
        body=body,
        request=parsed,
        trusted_now=transport._trusted_now(),
        keyring=transport._keyring(),
        path=batch.CUSTOM_IMPORT_PROVIDER_BATCH_PATH,
    )


@pytest.mark.parametrize("failure", ["version", "digest", "extra", "identity", "two", "binding", "query"])
def test_managed_policy_and_single_exact_identity_are_closed(failure):
    document = json.loads(_body())
    match failure:
        case "version":
            document["managed_detail"].pop("contract_version")
        case "digest":
            document["managed_detail"]["access_scope_sha256"] = "0" * 64
        case "extra":
            document["managed_detail"]["source"] = "untrusted"
        case "identity":
            document["native_batch"]["npis"] = ["9000000000"]
        case "two":
            document["native_batch"]["npis"].append("9000000000")
        case "binding":
            document["network_binding"] = None
        case "query":
            document["managed_detail"]["query"]["force_address_update"] = "true"
    with pytest.raises(CustomImportReadRequestError):
        batch._parse_batch_request(transport._canonical_json_bytes(document))


def test_managed_keeps_padded_identity_but_normalizes_native_integer():
    parsed = batch._parse_batch_request(_body())
    assert parsed.native_batch["npis"] == [49]
    assert parsed.managed_detail.query.npi == NPI
    assert parsed.managed_detail.policy["contract_version"] == "registry-catalog-policy/v2"
    with pytest.raises(CustomImportReadRequestError, match="not canonical"):
        batch._parse_batch_request(batch_fixtures._body(native_batch=json.loads(_body())["native_batch"]))


@pytest.fixture
def import_detail(detail_boundary, monkeypatch):
    boundary = detail_boundary

    # The isolated detail fixture yields its actual orchestration session here.
    @asynccontextmanager
    async def snapshot(database, schema, *, include_detail):
        async with managed.detail.provider_profile_read_snapshot(
            database, schema, include_detail=include_detail
        ) as held:
            boundary.import_session = held
            yield held

    monkeypatch.setattr(managed, "provider_profile_read_snapshot", snapshot)

    @asynccontextmanager
    async def bounded(held, **options):
        assert held is boundary.import_session and options["timeout_ms"] > 0
        yield

    monkeypatch.setattr(managed, "_bounded_read_window", bounded)
    return boundary


@pytest.mark.asyncio
@pytest.mark.parametrize("include_filter", [False, True])
async def test_import_and_detail_share_one_pin_and_strip_native_summary(import_detail, monkeypatch, include_filter):
    boundary = import_detail
    body = _body(include_filter=include_filter)
    parsed = batch._parse_batch_request(body)
    # Install the existing synthetic read-core boundary only; not native authority proof.
    async with managed.provider_profile_read_snapshot(npi.db, "mrf", include_detail=True) as session:
        batch_fixtures._install(monkeypatch, session)
    native_reader = provider_batch.read_native_batch

    async def native(*args, **options):
        assert current_network_address_scope().manifest is boundary.manifest
        assert options["session"] is boundary.import_session
        payload = await native_reader(*args, **options)
        payload["items"][0]["provider"]["unscoped_summary"] = "must be removed"
        return payload

    monkeypatch.setattr(provider_batch, "read_native_batch", native)
    batch_payload, manifest = await managed.read_managed_import_detail(
        boundary.request, parsed, _verified(body, parsed)
    )
    assert manifest is boundary.manifest
    provider_item = batch_payload["items"][0]
    assert provider_item["npi"] == NPI and provider_item["provider"]["npi"] == NPI
    assert set(provider_item["provider"]) == ({"npi", "custom_import"} if include_filter else {"npi"})
    assert set(batch_payload["managed_detail"]) == {
        "client_id",
        "policy_revision",
        "generation",
        "approved_custom_revision",
        "manifest_sha256",
        "data",
    }
    assert batch_payload["managed_detail"]["data"]["npi"]["metadata_projected"] is True
    assert boundary.state.seen[0][0] is boundary.request
    assert boundary.state.seen[0][2]["sync_geocode"] == "false"
    managed.detail._pin.assert_awaited_once()
    assert current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["absent", "empty", "finality"])
async def test_ineligible_or_changed_import_never_returns_partial_detail(import_detail, monkeypatch, failure):
    boundary = import_detail
    async with managed.provider_profile_read_snapshot(npi.db, "mrf", include_detail=True) as session:
        batch_fixtures._install(monkeypatch, session, finality_failure=failure == "finality")
    if failure != "finality":
        monkeypatch.setattr(
            provider_batch,
            "read_native_batch",
            AsyncMock(
                return_value={
                    "items": [] if failure == "empty" else [{"npi": 49, "status": 404}],
                    "requested": 1,
                    "found": 0,
                    "not_found": 1,
                    "pagination": {"total": 0},
                }
            ),
        )
    body = _body()
    parsed = batch._parse_batch_request(body)
    with pytest.raises((NotFound, CustomImportReadUnavailableError)):
        await managed.read_managed_import_detail(boundary.request, parsed, _verified(body, parsed))
    npi.read_npi_detail.assert_not_awaited()
    boundary.metadata.assert_not_awaited()
    assert current_network_address_scope() is None


@pytest.mark.asyncio
async def test_same_session_composer_refuses_missing_or_different_scope(detail_boundary):
    boundary = detail_boundary
    query = batch._parse_batch_request(_body()).managed_detail.query
    with pytest.raises(RegistryNetworkCatalogError, match="request_invalid"):
        await managed.detail.compose_managed_provider_detail(
            boundary.request, query, object(), boundary.manifest, excluded_network_ids=()
        )
    npi.read_npi_detail.assert_not_awaited()


@pytest.mark.asyncio
async def test_padded_native_eligibility_uses_exact_ten_character_import_value():
    class Session:
        async def execute(self, statement):
            self.statement = statement

            class Result:
                def scalars(self):
                    return self

                def all(self):
                    return [NPI]

            return Result()

    from types import SimpleNamespace

    from sqlalchemy import literal, select

    session = Session()
    context = SimpleNamespace(
        prepared=SimpleNamespace(statement=select(literal(NPI).label("entity_value")), normalized_order_terms=()),
        require_match=True,
    )
    assert await provider_batch._imported_batch_npis(context, [49], session) == [49]
    compiled = session.statement.compile(dialect=postgresql.dialect())
    assert NPI in compiled.params.values()


@pytest.mark.asyncio
@pytest.mark.parametrize("selected", [False, True])
async def test_known_zero_office_batch_survives_default_but_not_selected_scope(detail_boundary, monkeypatch, selected):
    boundary = detail_boundary
    query_by_field = {"npi": NPI}
    if selected:
        query_by_field["selector"] = {"namespace": "canonical", "values": [84]}
    query = managed.parse_managed_provider_detail(v2_envelope(query_by_field)).query
    params_by_field = {
        "npis": [49],
        "address_limit": 5,
        "address_offset": 0,
        "include_sources": False,
        "include_evidence": False,
    }
    state = provider_batch._NativeBatchState({49: {"npi": 49}}, {49: []})
    monkeypatch.setattr(provider_batch, "_prepare_native_batch", AsyncMock(return_value=state))
    eligible = AsyncMock(side_effect=lambda request, args, context, requested, found, session, **options: found)
    monkeypatch.setattr(provider_batch, "_batch_eligible_npis", eligible)

    async def hydrate(identities, state, params_by_field, args, session, **options):
        return [
            npi._npi_batch_provider_result(identity, state.details[identity], (), (), (), None, params_by_field)[0]
            for identity in identities
        ]

    monkeypatch.setattr(provider_batch, "_hydrate_native_batch", hydrate)
    async with managed.detail.provider_profile_read_snapshot(npi.db, "mrf", include_detail=True) as session:
        async with managed.detail.managed_provider_detail_read_scope(
            session, query, excluded_network_ids=(), access_scope_sha256="a" * 64
        ):
            batch_payload = await provider_batch.read_native_batch(
                boundary.request,
                params_by_field,
                native_args=provider_batch.parse_native_batch_query({}),
                session=session,
            )
    assert eligible.await_args.args[4] == ([] if selected else [49])
    assert batch_payload["items"][0]["status"] == (404 if selected else 200)
    assert batch_payload["found"] == (0 if selected else 1)
    assert current_network_address_scope() is None


def _managed_headers(body):
    return permits._headers(
        body=body,
        path=batch.CUSTOM_IMPORT_PROVIDER_BATCH_PATH,
        contract=transport.CUSTOM_IMPORT_PROVIDER_TRANSPORT_CONTRACT,
        body_sha256=transport.managed_provider_body_sha256,
        signature_message=transport.custom_import_provider_signature_message,
    )


@pytest.mark.asyncio
async def test_actual_signed_managed_http_allows_large_policy_and_strips_summary(import_detail, monkeypatch):
    from uuid import uuid4

    from sanic import Blueprint, Sanic

    from api.endpoint import extension_reads

    boundary = import_detail
    async with managed.provider_profile_read_snapshot(npi.db, "mrf", include_detail=True) as session:
        prepared, chunks = batch_fixtures._install(monkeypatch, session)
    document = json.loads(_body(include_filter=True))
    document["managed_detail"] = v2_envelope({"npi": NPI}, excluded_network_ids=list(range(1, 5001)))
    body = transport._canonical_json_bytes(document)
    assert transport._MAX_BODY_BYTES < len(body) < managed.MAX_MANAGED_BODY_BYTES
    app = Sanic("managed_signed_" + uuid4().hex)
    app.blueprint(Blueprint.group([extension_reads.blueprint], version_prefix="/api/v"))
    _, reply = await app.asgi_client.post(
        batch.CUSTOM_IMPORT_PROVIDER_BATCH_PATH, data=body, headers=_managed_headers(body)
    )
    assert reply.status == 200, reply.json
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert reply.headers["X-Network-Generation"] == "9"
    assert reply.headers["X-Network-Access-Scope"] == document["managed_detail"]["access_scope_sha256"]
    assert set(reply.json["items"][0]["provider"]) == {"npi", "custom_import"}
    assert prepared[0][0].entity_values == (NPI,) and chunks == [(NPI,)]
    assert reply.json["managed_detail"]["data"]["npi"]["npi"] == int(NPI)
    assert boundary.state.seen[0][0].path == batch.CUSTOM_IMPORT_PROVIDER_BATCH_PATH
    managed.detail._pin.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["signature", "too_large", "absent"])
async def test_managed_http_refusals_never_return_partial_data(import_detail, monkeypatch, failure):
    from uuid import uuid4

    from sanic import Blueprint, Sanic

    from api.endpoint import extension_reads

    boundary = import_detail
    async with managed.provider_profile_read_snapshot(npi.db, "mrf", include_detail=True) as session:
        batch_fixtures._install(monkeypatch, session)
    body = _body()
    headers = _managed_headers(body)
    if failure == "signature":
        body = _body(include_filter=True)
    elif failure == "too_large":
        body += b" " * managed.MAX_MANAGED_BODY_BYTES
    else:
        monkeypatch.setattr(
            provider_batch,
            "read_native_batch",
            AsyncMock(
                return_value={
                    "items": [{"npi": 49, "status": 404}],
                    "requested": 1,
                    "found": 0,
                    "not_found": 1,
                    "pagination": {"total": 0},
                }
            ),
        )
    app = Sanic("managed_refusal_" + uuid4().hex)
    app.blueprint(Blueprint.group([extension_reads.blueprint], version_prefix="/api/v"))
    _, reply = await app.asgi_client.post(batch.CUSTOM_IMPORT_PROVIDER_BATCH_PATH, data=body, headers=headers)
    assert reply.status == 404, reply.json
    assert set(reply.json) == {"error"} and reply.headers["Cache-Control"] == "private, no-store"
    npi.read_npi_detail.assert_not_awaited()
    boundary.metadata.assert_not_awaited()
    if failure != "absent":
        managed.detail._pin.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_batch_keeps_native_exception_status_without_managed_mode(monkeypatch):
    from tests.test_custom_import_provider_http import _Session

    session = _Session()
    batch_fixtures._install(monkeypatch, session)
    monkeypatch.setattr(provider_batch, "read_native_batch", AsyncMock(side_effect=NotFound("absent")))
    reply = await batch.serve_custom_import_provider_batch(batch_fixtures._request(batch_fixtures._body()), session)
    assert reply.status == 503 and session.rolled_back
    assert set(json.loads(reply.body)) == {"error"}


@pytest.mark.asyncio
async def test_combined_managed_output_bound_refuses_import_plus_detail(import_detail, monkeypatch):
    from uuid import uuid4

    from sanic import Blueprint, Sanic

    from api.endpoint import extension_reads

    permits._install_keyring(monkeypatch)
    monkeypatch.setattr(managed.detail, "MAX_DETAIL_BYTES", 2048)
    monkeypatch.setattr(
        batch,
        "_read_batch_payload",
        AsyncMock(
            return_value={
                "items": [
                    {"npi": NPI, "status": 200, "provider": {"npi": NPI, "custom_import": {"field": "x" * 3000}}}
                ],
                "requested": 1,
                "found": 1,
                "not_found": 0,
                "pagination": {"total": 1},
            }
        ),
    )
    app = Sanic("managed_bound_" + uuid4().hex)
    app.blueprint(Blueprint.group([extension_reads.blueprint], version_prefix="/api/v"))
    body = _body()
    _, reply = await app.asgi_client.post(
        batch.CUSTOM_IMPORT_PROVIDER_BATCH_PATH, data=body, headers=_managed_headers(body)
    )
    assert reply.status == 503 and set(reply.json) == {"error"}
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert current_network_address_scope() is None


@pytest.mark.parametrize(
    "native_query",
    [
        {},
        None,
        {"include_total": "false"},
        {"include_total": True},
        {"include_total": "TRUE"},
        {"include_total": "true", "state": "CA"},
        {"include_total": "true", "npi": NPI},
    ],
)
def test_managed_requires_exact_fixed_native_query(native_query):
    document = json.loads(_body())
    document["native_query"] = native_query
    with pytest.raises((CustomImportReadRequestError, transport.CustomImportReadTransportError, InvalidUsage)):
        batch._parse_batch_request(transport._canonical_json_bytes(document))


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("address_limit", 0),
        ("address_limit", 6),
        ("address_limit", "5"),
        ("address_limit", True),
        ("address_offset", 1),
        ("address_offset", False),
        ("include_sources", True),
        ("include_sources", "false"),
        ("include_sources", 0),
        ("include_evidence", True),
        ("include_evidence", "false"),
        ("include_evidence", 0),
        ("npis", [49]),
        ("npis", [NPI, NPI]),
        ("npis", ["9000000000"]),
    ],
)
def test_managed_requires_fixed_native_batch_scalars_and_identity(field, replacement):
    document = json.loads(_body())
    document["native_batch"][field] = replacement
    with pytest.raises(
        (
            CustomImportReadRequestError,
            transport.CustomImportReadTransportError,
            InvalidUsage,
        )
    ):
        batch._parse_batch_request(transport._canonical_json_bytes(document))


@pytest.mark.parametrize("field", ["npis", "address_limit", "address_offset", "include_sources", "include_evidence"])
def test_managed_refuses_each_missing_native_batch_field(field):
    document = json.loads(_body())
    document["native_batch"].pop(field)
    with pytest.raises(CustomImportReadRequestError):
        batch._parse_batch_request(transport._canonical_json_bytes(document))


@pytest.mark.parametrize("field", ["native_query", "native_batch"])
def test_managed_refuses_extra_or_missing_native_descriptor(field):
    document = json.loads(_body())
    if field == "native_batch":
        document[field]["extra"] = False
    else:
        document.pop(field)
    with pytest.raises((CustomImportReadRequestError, transport.CustomImportReadTransportError, InvalidUsage)):
        batch._parse_batch_request(transport._canonical_json_bytes(document))


def test_legacy_native_query_and_batch_options_remain_independent():
    native_batch = json.loads(batch_fixtures._body())["native_batch"] | {
        "address_limit": 7,
        "address_offset": 2,
        "include_sources": True,
        "include_evidence": True,
    }
    parsed = batch._parse_batch_request(batch_fixtures._body(native_batch=native_batch, native_query={"state": "CA"}))
    assert parsed.managed_detail is None
    assert parsed.native_batch == native_batch | {"npis": [9000000000, 9000000001]}
    assert parsed.provider.native_args.get("state") == "CA"
