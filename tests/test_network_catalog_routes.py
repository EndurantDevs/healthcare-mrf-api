# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual Sanic requests and approved reader SQL at an offline transport seam."""

import asyncio
import hashlib
import json
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sanic import Blueprint, Sanic
from sqlalchemy.exc import SQLAlchemyError

from api.endpoint import network_catalog as endpoint
from process import registry_network_catalog_read as catalog
from tests.test_registry_network_catalog_read import batch_boundary, legacy_selector, source_coordinates

_AUTH = {"Authorization": "Bearer synthetic-catalog-control-token"}
_PATH = "/api/v1/registry/serving/catalog"


def envelope(query=None, **changes):
    return {
        "client_id": "sample-client",
        "policy_revision": "0",
        "excluded_network_ids": [],
        "query": query if query is not None else {},
    } | changes


class Session:
    """Record real endpoint transaction ordering without opening PostgreSQL."""

    def __init__(self, driver):
        self.driver = driver
        self.events = []
        self.failure = None

    def begin(self):
        self.events.append("begin")
        return self

    async def __aenter__(self):
        self.driver.in_transaction = True
        return self

    async def __aexit__(self, kind, _value, _traceback):
        self.events.append(("exit", kind))
        self.driver.in_transaction = False

    async def execute(self, statement):
        self.events.append(str(statement))
        if self.failure is not None:
            raise self.failure

    async def connection(self):
        self.events.append("connection")
        return self

    async def get_raw_connection(self):
        self.events.append("driver")
        return SimpleNamespace(driver_connection=self.driver)


@pytest.fixture
def context(batch_boundary, monkeypatch):
    driver, manifest, revisions = batch_boundary
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-catalog-control-token")
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "sample_control")
    session = Session(driver)
    app = Sanic("catalog_routes_" + uuid4().hex)
    app.blueprint(Blueprint.group([endpoint.blueprint], version_prefix="/api/v"))
    endpoint.register_network_catalog_private_responses(app)

    @app.middleware("request")
    async def lazy_session(request):
        request.ctx.sa_session = session

    return SimpleNamespace(app=app, session=session, driver=driver, manifest=manifest, revisions=revisions)


async def send(context, document=None, *, path=_PATH, headers=None, raw=None):
    _, http_response = await context.app.asgi_client.post(
        path,
        data=raw if raw is not None else json.dumps(document if document is not None else envelope()).encode(),
        headers=_AUTH if headers is None else headers,
    )
    assert http_response.headers["Cache-Control"] == "private, no-store"
    return http_response


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong"}])
async def test_authentication_precedes_invalid_body_and_database(context, headers):
    http_response = await send(context, raw=b"\xff", headers=headers)
    assert http_response.status == 403 and http_response.json == {"error": {"code": "network_catalog_forbidden"}}
    assert context.session.events == [] and context.driver.calls == []
    assert "X-Network-Generation" not in http_response.headers


@pytest.mark.asyncio
async def test_absent_control_configuration_fails_closed(context, monkeypatch):
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN")
    http_response = await send(context)
    assert http_response.status == 403 and context.session.events == []


@pytest.mark.asyncio
async def test_actual_reader_pins_retained_revision_and_policy_before_count_page(context):
    source_fields = vars(source_coordinates())
    document = envelope(
        {
            "limit": 1,
            "offset": 0,
            "network_generation": "9",
            "search": "Alias",
            "archived": None,
            "source": source_fields,
        },
        policy_revision="9223372036854775807",
        excluded_network_ids=[2, 5],
    )
    http_response = await send(context, document, headers=_AUTH | {"X-Network-Access-Scope": "untrusted"})
    assert http_response.status == 200
    assert (
        http_response.json["client_id"] == "sample-client"
        and http_response.json["policy_revision"] == "9223372036854775807"
    )
    assert http_response.json["generation"] == http_response.headers["X-Network-Generation"] == "9"
    assert http_response.json["approved_custom_revision"] == "4" and context.revisions == [4]
    assert (
        http_response.json["items"][0]["priceable"] is None and http_response.json["items"][0]["benefit_codes"] is None
    )
    policy_by_field = {key: document[key] for key in ("client_id", "policy_revision", "excluded_network_ids")}
    expected = hashlib.sha256(
        json.dumps(policy_by_field, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    assert http_response.headers["X-Network-Access-Scope"] == expected
    assert context.session.events == [
        "begin",
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
        "SET LOCAL lock_timeout='1s'",
        "connection",
        "driver",
        ("exit", None),
    ]
    sql, parameters = context.driver.calls[-1]
    assert parameters == (4, (2, 5), None, "Alias", source_coordinates().sql_parameters, None, 0, 1, 1048576)
    assert sql.index("NOT record_key::integer=ANY") < sql.index("filtered AS") < sql.index("OFFSET $7")
    assert "registry_revision_control" not in sql and "network_registry_alias" not in sql
    assert context.driver.in_transaction is False


@pytest.mark.asyncio
@pytest.mark.parametrize("query", [{}, {"archived": False}, {"archived": True}, {"archived": None}])
async def test_archived_tristate_and_defaults_reach_actual_reader(context, query):
    http_response = await send(context, envelope(query))
    assert http_response.status == 200
    assert context.driver.calls[-1][1][2] is query.get("archived", False)


@pytest.mark.asyncio
@pytest.mark.parametrize("offset,total", [(0, 0), (10, 6)])
async def test_empty_and_beyond_end_preserve_authorized_total_and_pin(context, offset, total):
    context.driver.page.update(total=total, rows_json="[]")
    http_response = await send(context, envelope({"offset": offset}))
    assert http_response.status == 200 and http_response.json["items"] == [] and http_response.json["total"] == total
    assert http_response.headers["X-Network-Generation"] == "9"


@pytest.mark.asyncio
async def test_exact_canonical_detail_preserves_reader_page(context):
    http_response = await send(context, envelope({"network_id": 7, "network_generation": "9"}), path=_PATH + "/detail")
    assert http_response.status == 200 and http_response.json["items"][0]["network_id"] == 7
    assert context.driver.calls[-1][1][5:] == (7, 0, 1, 1048576)


@pytest.mark.asyncio
async def test_exact_legacy_selector_preserves_coordinates_and_scope(context):
    selector = legacy_selector()
    query_by_field = {
        "source": vars(selector.source),
        "namespace": selector.namespace,
        "value": selector.value,
        "source_scope": json.loads(selector.source_scope_json),
        "network_generation": "9",
    }
    http_response = await send(context, envelope(query_by_field), path=_PATH + "/legacy")
    assert http_response.status == 200 and http_response.json["items"][0]["network_id"] == 7
    sql, parameters = context.driver.calls[-2]
    assert "AS permitted_count" in sql and parameters[:2] == (4, selector.source.sql_parameters)
    assert json.loads(parameters[2]) == query_by_field["source_scope"] and parameters[3] == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["hidden", "ambiguous", "missing"])
async def test_legacy_hidden_ambiguous_or_missing_refuses_without_selector_values(context, kind):
    selector = legacy_selector()
    query_by_field = {
        "source": vars(selector.source),
        "namespace": selector.namespace,
        "value": selector.value,
        "source_scope": json.loads(selector.source_scope_json),
    }
    if kind == "ambiguous":
        context.driver.legacy["ambiguous"] = True
    else:
        context.driver.legacy["permitted_count"] = 0
    http_response = await send(
        context, envelope(query_by_field, excluded_network_ids=[7] if kind == "hidden" else []), path=_PATH + "/legacy"
    )
    assert http_response.status == 404 and http_response.json == {"error": {"code": "network_catalog_not_found"}}
    assert "X-Network-Generation" not in http_response.headers


@pytest.mark.asyncio
async def test_excluded_detail_never_reads_candidate_or_approved_metadata(context):
    http_response = await send(context, envelope({"network_id": 7}, excluded_network_ids=[7]), path=_PATH + "/detail")
    assert http_response.status == 404 and context.driver.calls == []
    assert context.session.events[-1] == ("exit", catalog.RegistryNetworkCatalogError)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw",
    [
        b'{"client_id":"a","client_id":"b"}',
        b'{"query":{"x":1,"x":2}}',
        b'{"a":NaN}',
        b'{"a":Infinity}',
        b"\xff",
        b"{}".decode().encode("utf-16"),
        b" " * 1048577,
        b"[]",
        b"{}{}",
        b"\xef\xbb\xbf{}",
    ],
)
async def test_closed_utf8_body_refuses_before_database(context, raw):
    http_response = await send(context, raw=raw)
    assert http_response.status == 400 and context.session.events == []


@pytest.mark.asyncio
async def test_one_mebibyte_body_boundary_is_inclusive(context):
    raw = json.dumps(envelope()).encode()
    http_response = await send(context, raw=raw + b" " * (1048576 - len(raw)))
    assert http_response.status == 200


_INVALID_ENVELOPES = [
    {},
    envelope(extra=True),
    envelope(client_id=""),
    envelope(client_id=" a"),
    envelope(client_id="a\u0085b"),
    envelope(client_id="a" * 65),
    envelope(client_id=True),
    envelope(client_id="\ud800"),
    *[envelope(policy_revision=value) for value in (0, True, "-1", "00", "01", "+1", "1.0", "9223372036854775808")],
    *[envelope(excluded_network_ids=value) for value in (None, [0], [True], [2147483648], [2, 1], [1, 1])],
    *[
        envelope(query=value)
        for value in (
            [],
            {"benefit_code": "x"},
            {"network_id": 7},
            {"limit": True},
            {"limit": 0},
            {"limit": 101},
            {"offset": -1},
            {"offset": 1000001},
            {"network_generation": 9},
            {"network_generation": "0"},
            {"network_generation": "09"},
            {"search": " "},
            {"search": None},
            {"archived": 0},
            {"source": {}},
        )
    ],
]
# Preserve explicit null query; the convenience constructor otherwise supplies an empty query.
_INVALID_ENVELOPES.append(envelope() | {"query": None})


@pytest.mark.asyncio
@pytest.mark.parametrize("document", _INVALID_ENVELOPES)
async def test_typed_policy_and_list_request_refusal(context, document):
    http_response = await send(context, document)
    assert http_response.status == 400 and context.session.events == []


@pytest.mark.asyncio
async def test_query_string_is_forbidden(context):
    http_response = await send(context, path=_PATH + "?limit=1")
    assert http_response.status == 400 and context.session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "suffix,query",
    [
        ("/detail", {}),
        ("/detail", {"network_id": "7"}),
        ("/detail", {"network_id": True}),
        ("/detail", {"network_id": 7, "namespace": "checksum_network"}),
        ("/legacy", {"network_id": 7}),
        (
            "/legacy",
            {"namespace": "network_id", "value": "7", "source": vars(source_coordinates()), "source_scope": {}},
        ),
        (
            "/legacy",
            {"namespace": "checksum_network", "value": "-17", "source": vars(source_coordinates()), "source_scope": []},
        ),
    ],
)
async def test_typed_selectors_are_never_reinterpreted(context, suffix, query):
    http_response = await send(context, envelope(query), path=_PATH + suffix)
    assert http_response.status == 400 and context.session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["missing", "extra", "wrong_type"])
async def test_all_six_source_coordinates_are_closed(context, mutation):
    source = vars(source_coordinates()).copy()
    if mutation == "missing":
        del source["edition_id"]
    elif mutation == "extra":
        source["scope"] = "other"
    else:
        source["dataset_id"] = 7
    http_response = await send(context, envelope({"source": source}))
    assert http_response.status == 400 and context.session.events == []


@pytest.mark.asyncio
async def test_exclusion_int4_and_fifty_thousand_boundaries(context):
    context.driver.page.update(total=0, rows_json="[]")
    http_response = await send(context, envelope(excluded_network_ids=list(range(1, 50000)) + [2147483647]))
    assert http_response.status == 200 and len(context.driver.calls[-1][1][1]) == 50000
    prior_events = list(context.session.events)
    http_response = await send(context, envelope(excluded_network_ids=list(range(1, 50002))))
    assert http_response.status == 400 and context.session.events == prior_events


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        asyncpg.PostgresError("private database detail"),
        asyncpg.InterfaceError("private database detail"),
        SQLAlchemyError("private database detail"),
        TimeoutError("private database detail"),
    ],
)
async def test_database_errors_are_sanitized_and_release_transaction(context, failure):
    context.session.failure = failure
    http_response = await send(context)
    assert http_response.status == 503 and http_response.json == {"error": {"code": "network_catalog_unavailable"}}
    assert context.session.events[-1][0] == "exit"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["membership", "arrays", "capability"])
async def test_retained_proof_and_unproven_capabilities_fail_closed(context, failure):
    if failure == "membership":
        context.driver.report["membership_rows"] = 3
    elif failure == "arrays":
        context.driver.parity["changed_arrays"] = 1
    else:
        document = json.loads(context.driver.page["rows_json"])[0]
        document["priceable"] = True
        context.driver.page["rows_json"] = json.dumps([document])
    http_response = await send(context)
    assert http_response.status == 503 and context.session.events[-1][0] == "exit"


@pytest.mark.asyncio
async def test_cancelled_reader_propagates_and_exits_request_transaction(context, monkeypatch):
    async def cancel(*_args, **_kwargs):
        raise asyncio.CancelledError()

    monkeypatch.setattr(endpoint, "read_registry_network_catalog", cancel)
    request = SimpleNamespace(
        headers=_AUTH,
        body=json.dumps(envelope()).encode(),
        query_string="",
        ctx=SimpleNamespace(sa_session=context.session),
    )
    with pytest.raises(asyncio.CancelledError):
        await endpoint._catalog_response(request, "list")
    assert context.session.events[-1] == ("exit", asyncio.CancelledError)
    assert context.driver.in_transaction is False


@pytest.mark.asyncio
async def test_fhir_uuid_selector_stays_source_scoped(context):
    identifier = "22222222-2222-4222-8222-222222222222"
    query_by_field = {
        "namespace": "legacy_fhir_uuid",
        "value": identifier,
        "source": vars(source_coordinates("fhir")),
        "source_scope": {"organization_id": "org-1", "legacy_uuid": identifier, "alias_scope": "sample-scope"},
    }
    http_response = await send(context, envelope(query_by_field), path=_PATH + "/legacy")
    assert http_response.status == 200
    parameters = context.driver.calls[-2][1]
    assert parameters[1] == source_coordinates("fhir").sql_parameters
    assert json.loads(parameters[2]) == query_by_field["source_scope"]


def test_wire_generation_and_client_limits_preserve_gateway_scalar_count():
    request = SimpleNamespace(
        body=json.dumps(
            envelope(
                {"network_generation": "9223372036854775807", "limit": 100, "offset": 1000000},
                client_id="é" * 64,
            )
        ).encode(),
        query_string="",
    )
    policy_by_field, excluded_ids, selector, generation, scope = endpoint._parameters(request, "list")
    assert len(policy_by_field["client_id"]) == 64 and len(policy_by_field["client_id"].encode()) == 128
    assert excluded_ids == () and selector.generation_id == generation == 9223372036854775807
    assert selector.limit == 100 and selector.offset == 1000000 and len(scope) == 64


@pytest.mark.asyncio
async def test_full_policy_envelope_response_budget_is_bounded(context, monkeypatch):
    async def oversized(*_args, **_kwargs):
        return {"generation": "9", "items": ["x" * 1048576]}

    monkeypatch.setattr(endpoint, "read_registry_network_catalog", oversized)
    http_response = await send(context)
    assert http_response.status == 503 and http_response.json == {"error": {"code": "network_catalog_unavailable"}}
    assert "X-Network-Generation" not in http_response.headers
    assert context.session.events[-1] == ("exit", None)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code,status",
    [
        ("registry_network_catalog_request_invalid", 400),
        ("registry_network_catalog_detail_denied", 404),
        ("registry_network_catalog_source_changed", 503),
    ],
)
async def test_reader_error_categories_remain_value_free(context, monkeypatch, code, status):
    async def refuse(*_args, **_kwargs):
        raise catalog.RegistryNetworkCatalogError(code)

    monkeypatch.setattr(endpoint, "read_registry_network_catalog", refuse)
    http_response = await send(context)
    assert http_response.status == status and code not in http_response.text
    assert "X-Network-Generation" not in http_response.headers
    assert context.session.events[-1] == ("exit", catalog.RegistryNetworkCatalogError)


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["", "/detail", "/legacy"])
@pytest.mark.parametrize("method", ["get", "head", "options", "put", "patch", "delete"])
async def test_framework_method_refusals_are_private_without_database(context, suffix, method):
    _, http_response = await getattr(context.app.asgi_client, method)(_PATH + suffix)
    assert http_response.status == 405 and http_response.headers["Cache-Control"] == "private, no-store"
    assert context.session.events == [] and context.driver.calls == []


@pytest.mark.asyncio
async def test_catalog_cache_middleware_does_not_change_other_paths(context):
    @context.app.get("/sample")
    async def sample(_request):
        from sanic import response

        return response.json({"sample": True})

    _, http_response = await context.app.asgi_client.get("/sample")
    assert http_response.status == 200 and "Cache-Control" not in http_response.headers
