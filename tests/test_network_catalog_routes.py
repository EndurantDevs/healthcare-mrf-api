# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual Sanic requests and approved reader SQL at an offline transport seam."""

import asyncio
import hashlib
import json
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sanic import Blueprint, Sanic
from sqlalchemy.exc import SQLAlchemyError

from api.endpoint import network_catalog as endpoint
from api.network_serving_read_context import _SNAPSHOT_SQL
from process import registry_network_catalog_read as catalog
from tests.test_registry_network_catalog_read import batch_boundary, legacy_selector, source_coordinates

_AUTH = {"Authorization": "Bearer synthetic-catalog-control-token"}
_PATH = "/api/v1/registry/serving/catalog"
_OWNED_READ_EVENTS = [
    "begin",
    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",
    *["connection", "driver"] * 2,
    "SET LOCAL lock_timeout='1s'",
    *["connection", "driver"] * 4,
    ("exit", None),
]


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
        self.transaction = None
        self.snapshot_queries = []
        self.driver.in_transaction = False
        self.driver.get_server_pid = lambda: 42
        self.original_fetchrow = driver.fetchrow
        self.driver.fetchrow = self.fetchrow

    def in_transaction(self):
        """Expose the fixture's actual owned transaction state."""
        return self.transaction is not None

    def get_transaction(self):
        """Keep the transaction object stable across native acquisitions."""
        return self.transaction

    def get_nested_transaction(self):
        """The route fixture does not enter a savepoint."""
        return None

    async def fetchrow(self, sql, *parameters):
        """Separate native snapshot checks from the existing catalog SQL observations."""
        if sql == _SNAPSHOT_SQL:
            self.snapshot_queries.append(sql)
            return self.driver.isolation, self.driver.read_only, True, 11, 22, "127.0.0.1", 5432, 42
        return await self.original_fetchrow(sql, *parameters)

    def begin(self):
        self.events.append("begin")
        return self

    async def __aenter__(self):
        self.transaction = object()
        self.driver.in_transaction = True
        return self

    async def __aexit__(self, kind, _value, _traceback):
        self.events.append(("exit", kind))
        self.transaction = None
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
    assert context.session.events == _OWNED_READ_EVENTS
    assert len(context.session.snapshot_queries) == 3
    sql, parameters = context.driver.calls[-1]
    assert parameters == (4, (2, 5), None, "Alias", source_coordinates().sql_parameters, None, 0, 1, 1048576, None)
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
    assert context.driver.calls[-1][1][5:] == ((7,), 0, 1, 1048576, None)


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


def v2_envelope(query=None, **changes):
    document = (
        envelope(query)
        | {
            "contract_version": "registry-catalog-policy/v2",
            "organization_visibility_default": "system_wide",
            "excluded_company_ids": [],
            "excluded_group_ids": [],
        }
        | changes
    )
    normalized_by_field = {key: value for key, value in document.items() if key not in {"query", "access_scope_sha256"}}
    normalized_by_field["policy_revision"] = int(document["policy_revision"])
    document["access_scope_sha256"] = hashlib.sha256(
        json.dumps(normalized_by_field, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    return document


@pytest.mark.asyncio
async def test_v2_integer_digest_translation_and_typed_sql_scope(context):
    identity = str(uuid4())
    document = v2_envelope(
        client_id="sample-é",
        policy_revision="9223372036854775807",
        excluded_company_ids=[identity],
        excluded_group_ids=[identity],
    )
    result = await send(context, document)
    assert result.status == 200
    assert result.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    sql, parameters = context.driver.calls[-1]
    assert parameters[-2:] == ((identity,), (identity,))
    assert sql.count("NOT company.record_key=ANY($11::text[])") == 2
    assert sql.index("NOT company.record_key=ANY($11::text[])") < sql.index("declared_benefits AS")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"contract_version": "unknown"},
        {"organization_visibility_default": "restricted"},
        {"excluded_company_ids": ["bad"]},
        {"excluded_group_ids": [str(UUID(int=0))]},
        {"access_scope_sha256": "a" * 64},
        {"policy_revision": 1},
        {"unknown": True},
    ],
)
async def test_v2_refusal_never_falls_back_or_opens_database(context, change):
    document = v2_envelope() | change
    result = await send(context, document)
    assert result.status == 400 and context.session.events == []


@pytest.mark.asyncio
async def test_organization_route_uses_v2_exceptions_before_page_and_detail(context):
    identity = str(uuid4())
    context.driver.page.update(total=0, rows_json="[]")
    document = v2_envelope({"record_kind": "company", "offset": 2, "limit": 1}, excluded_company_ids=[identity])
    result = await send(context, document, path="/api/v1/registry/serving/organizations")
    assert result.status == 200 and result.json["total"] == 0 and result.json["record_kind"] == "company"
    sql, parameters = context.driver.calls[-1]
    assert parameters[2] == (identity,) and parameters[4:6] == (2, 1)
    assert sql.index("NOT record_key=ANY") < sql.index("OFFSET $5 LIMIT $6")
    detail = v2_envelope({"record_kind": "company", "record_id": identity}, excluded_company_ids=[identity])
    result = await send(context, detail, path="/api/v1/registry/serving/organizations/detail")
    assert result.status == 404 and result.headers["X-Network-Access-Scope"] == detail["access_scope_sha256"]
    assert context.driver.calls[-1][1][-1] == (identity,)


@pytest.mark.asyncio
async def test_organization_route_requires_explicit_v2(context):
    result = await send(context, envelope({"record_kind": "company"}), path="/api/v1/registry/serving/organizations")
    assert result.status == 400 and context.session.events == []


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
@pytest.mark.parametrize("failure", ["native_indexes", "retained_indexes", "readiness_receipt", "source", "capability"])
async def test_retained_proof_and_unproven_capabilities_fail_closed(context, failure):
    if failure == "native_indexes":
        context.driver.native_ready = False
    elif failure == "retained_indexes":
        context.driver.index_digest = "e" * 64
    elif failure == "readiness_receipt":
        context.driver.report.pop("serving_readiness")
    elif failure == "source":
        context.manifest.source_generations["custom_membership"] = "c" * 64
    else:
        document = json.loads(context.driver.page["rows_json"])[0]
        document["priceable"] = True
        context.driver.page["rows_json"] = json.dumps([document])
    http_response = await send(context)
    assert http_response.status == 503 and context.session.events[-1][0] == "exit"
    assert http_response.json == {"error": {"code": "network_catalog_unavailable"}}


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
@pytest.mark.parametrize("trailing", ["", "/"])
async def test_framework_method_refusals_are_private_without_database(context, suffix, method, trailing):
    _, http_response = await getattr(context.app.asgi_client, method)(_PATH + suffix + trailing)
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


@pytest.mark.asyncio
async def test_declared_benefit_filter_routes_to_existing_pinned_reader(context):
    context.driver.page["rows_json"] = json.dumps(
        [
            {
                **catalog._closed_json(context.driver.page["rows_json"].encode(), catalog.MAX_RESPONSE_BYTES)[0],
                "benefit_codes": ["dental"],
            }
        ]
    )
    http_response = await send(context, envelope({"benefit_code": "dental", "network_generation": "9"}))
    assert http_response.status == 200
    assert http_response.json["items"][0]["benefit_codes"] == ["dental"]
    assert context.driver.calls[-1][1][-1] == "dental"


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["pharmacy", "Dental", "", None, ["dental"], True])
async def test_declared_benefit_filter_rejects_unsupported_types_before_transaction(context, code):
    http_response = await send(context, envelope({"benefit_code": code}))
    assert http_response.status == 400 and context.session.events == []


@pytest.mark.asyncio
async def test_legacy_batch_v2_policy_scope_generation_and_private_headers(context):
    document = v2_envelope({"namespace": "checksum_network", "values": ["42", "0"], "network_generation": "9"})
    reply = await send(context, document, path=_PATH + "/legacy")
    assert reply.status == 200 and reply.json["items"][0]["network_id"] == 7
    assert reply.headers["X-Network-Generation"] == "9"
    assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert context.driver.calls[-2][1] == (4, ("42", "0"), "checksum_network", ())
    assert context.driver.calls[-1][1][5] == (7,)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"source": {}},
        {"source_scope": {}},
        {"value": "42"},
        {"values": "42"},
        {"values": []},
        {"values": ["42", "42"]},
        {"values": ["+42"]},
        {"network_generation": "0"},
    ],
)
async def test_legacy_batch_closed_input_refuses_before_database(context, changes):
    query = {"namespace": "checksum_network", "values": ["42"]} | changes
    reply = await send(context, v2_envelope(query), path=_PATH + "/legacy")
    assert reply.status == 400 and context.driver.calls == [] and context.session.events == []


@pytest.mark.asyncio
async def test_legacy_batch_refusal_is_value_free_and_never_falls_back(context):
    context.driver.legacy_batch["complete"] = False
    document = v2_envelope({"namespace": "checksum_network", "values": ["42", "-17"]}, excluded_network_ids=[7])
    reply = await send(context, document, path=_PATH + "/legacy")
    assert reply.status == 404 and reply.json == {"error": {"code": "network_catalog_not_found"}}
    assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    assert "X-Network-Generation" not in reply.headers
    assert context.driver.calls[-1][1][-1] == (7,)


_AUTOCOMPLETE_PATH = "/api/v1/plan/network/autocomplete"


@pytest.fixture
def autocomplete_context(context, monkeypatch):
    from api.endpoint import plan

    context.app.blueprint(Blueprint.group([plan.blueprint], version_prefix="/api/v"))
    context.autocomplete_calls = []
    context.autocomplete_page = {
        "generation": "9",
        "approved_custom_revision": "4",
        "plans": [
            {
                "plan_id": "12345CA0000001",
                "marketing_name": "Example é",
                "state": "CA",
                "year": 2026,
                "network_checksum": {"42": None, "-17": ""},
            }
        ],
    }

    async def read(connection, query, *, excluded_network_ids):
        assert connection is context.driver and connection.in_transaction
        context.autocomplete_calls.append((query, excluded_network_ids))
        return context.autocomplete_page

    monkeypatch.setattr(endpoint, "read_registry_network_autocomplete", read)
    return context


@pytest.mark.asyncio
async def test_autocomplete_full_policy_pins_one_transaction_and_keeps_metadata_shape(autocomplete_context):
    context = autocomplete_context
    organization_id = str(UUID(int=1))
    document = v2_envelope(
        {"query": "É%_", "limit": 2, "zip_code": "01234", "network_generation": "9"},
        client_id="sample-é",
        policy_revision="12",
        excluded_network_ids=[84],
        excluded_company_ids=[organization_id],
        excluded_group_ids=[organization_id],
    )
    http_response = await send(context, document, path=_AUTOCOMPLETE_PATH)
    assert http_response.status == 200
    assert http_response.json == {"client_id": "sample-é", "policy_revision": "12"} | context.autocomplete_page
    assert http_response.headers["X-Network-Generation"] == "9"
    assert http_response.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    query, exclusions = context.autocomplete_calls[0]
    assert (query.query, query.limit, query.zip_code, query.generation_id, exclusions) == ("É%_", 2, "01234", 9, (84,))
    assert len(context.autocomplete_calls) == 1 and context.driver.calls == []
    assert context.session.events == _OWNED_READ_EVENTS
    assert len(context.session.snapshot_queries) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query",
    [
        {},
        {"query": "x"},
        {"query": "", "limit": 1},
        {"query": " x", "limit": 1},
        {"query": "x\u0090", "limit": 1},
        {"query": "x" * 129, "limit": 1},
        {"query": 1, "limit": 1},
        {"query": "x", "limit": True},
        {"query": "x", "limit": 0},
        {"query": "x", "limit": 51},
        {"query": "x", "limit": 1, "zip_code": None},
        {"query": "x", "limit": 1, "zip_code": "１２３４５"},
        {"query": "x", "limit": 1, "network_generation": "01"},
        {"query": "x", "limit": 1, "network_ids": [84]},
        {"query": "x", "limit": 1, "state": "CA"},
    ],
)
async def test_autocomplete_closed_inputs_refuse_before_transaction(autocomplete_context, query):
    http_response = await send(autocomplete_context, v2_envelope(query), path=_AUTOCOMPLETE_PATH)
    assert http_response.status == 400
    assert autocomplete_context.session.events == [] and autocomplete_context.autocomplete_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong"}])
async def test_autocomplete_authentication_precedes_body_and_database(autocomplete_context, headers):
    http_response = await send(autocomplete_context, path=_AUTOCOMPLETE_PATH, raw=b"\xff", headers=headers)
    assert http_response.status == 403 and autocomplete_context.session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["v1", "bad_digest", "duplicate", "query_string", "oversized"])
async def test_autocomplete_policy_and_wire_refusals_are_private(autocomplete_context, kind):
    document = v2_envelope({"query": "x", "limit": 1})
    path = _AUTOCOMPLETE_PATH
    raw = None
    if kind == "v1":
        document = envelope({"query": "x", "limit": 1})
    elif kind == "bad_digest":
        document["access_scope_sha256"] = "a" * 64
    elif kind == "duplicate":
        raw = json.dumps(document).replace('"query": "x"', '"query": "x", "query": "y"').encode()
    elif kind == "query_string":
        path += "?query=x"
    else:
        raw = b" " * (catalog.MAX_RESPONSE_BYTES + 1)
    http_response = await send(autocomplete_context, document, path=path, raw=raw)
    assert http_response.status == 400 and autocomplete_context.session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["stale", "database", "oversized", "cancelled"])
async def test_autocomplete_failure_never_retries_legacy_and_closes_transaction(
    autocomplete_context, monkeypatch, kind
):
    context = autocomplete_context

    async def refuse(*_args, **_kwargs):
        context.autocomplete_calls.append(kind)
        if kind == "cancelled":
            raise asyncio.CancelledError
        if kind == "database":
            raise asyncpg.InterfaceError("synthetic detail must not escape")
        if kind == "stale":
            raise catalog.RegistryNetworkCatalogError("registry_network_catalog_source_changed")
        return {"generation": "9", "plans": ["x" * catalog.MAX_RESPONSE_BYTES]}

    monkeypatch.setattr(endpoint, "read_registry_network_autocomplete", refuse)
    if kind == "cancelled":
        request = SimpleNamespace(
            ctx=SimpleNamespace(sa_session=context.session),
            headers=_AUTH,
            query_string="",
            body=json.dumps(v2_envelope({"query": "x", "limit": 1})).encode(),
        )
        with pytest.raises(asyncio.CancelledError):
            await endpoint._catalog_response(request, "autocomplete")
    else:
        http_response = await send(context, v2_envelope({"query": "x", "limit": 1}), path=_AUTOCOMPLETE_PATH)
        assert http_response.status == 503 and http_response.json == {"error": {"code": "network_catalog_unavailable"}}
        assert "X-Network-Generation" not in http_response.headers
    assert len(context.autocomplete_calls) == 1 and not context.driver.in_transaction
    assert context.session.events[-1][0] == "exit"


@pytest.mark.asyncio
@pytest.mark.parametrize("path", [_AUTOCOMPLETE_PATH, _AUTOCOMPLETE_PATH + "/"])
async def test_autocomplete_post_framework_error_is_private_without_changing_get(autocomplete_context, path):
    from sanic.exceptions import PayloadTooLarge

    context = autocomplete_context

    @context.app.middleware("request")
    async def refuse_body(request):
        if request.method == "POST":
            raise PayloadTooLarge("synthetic body exceeds the request bound")

    _, refusal = await context.app.asgi_client.post(path, data=b"x")
    assert refusal.status == 413 and refusal.headers["Cache-Control"] == "private, no-store"
    _, legacy = await context.app.asgi_client.get(path)
    assert legacy.status == 200 and legacy.json == {"plans": []}
    assert "Cache-Control" not in legacy.headers and context.session.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("policy_version", ["v1", "v2"])
@pytest.mark.parametrize("failure", ["source_changed", "malformed", "unavailable", "framework_conflict"])
async def test_legacy_source_change_mapping_requires_exact_reader_error_and_full_policy(
    context, monkeypatch, policy_version, failure
):
    from sanic.exceptions import SanicException

    code = {
        "source_changed": "registry_network_catalog_source_changed",
        "malformed": "registry_network_catalog_source_changed:synthetic-private-detail",
        "unavailable": "registry_network_catalog_unavailable",
    }.get(failure)

    async def refuse(*_args, **_kwargs):
        if failure == "framework_conflict":
            raise SanicException("synthetic-private-detail", status_code=409)
        raise catalog.RegistryNetworkCatalogError(code)

    monkeypatch.setattr(endpoint, "resolve_registry_network_catalog_legacy", refuse)
    query_by_field = {"namespace": "checksum_network", "values": ["42"], "network_generation": "9"}
    document = (v2_envelope if policy_version == "v2" else envelope)(query_by_field)
    reply = await send(context, document, path=_PATH + "/legacy")
    expected = 409 if policy_version == "v2" and failure == "source_changed" else 503
    assert reply.status == expected
    assert reply.json == {
        "error": {"code": "network_catalog_source_changed" if expected == 409 else "network_catalog_unavailable"}
    }
    assert "synthetic-private-detail" not in reply.text and "X-Network-Generation" not in reply.headers
    if policy_version == "v2":
        assert reply.headers["X-Network-Access-Scope"] == document["access_scope_sha256"]
    elif failure == "framework_conflict":
        policy_by_field = {key: document[key] for key in ("client_id", "policy_revision", "excluded_network_ids")}
        expected_scope = hashlib.sha256(
            json.dumps(policy_by_field, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
        ).hexdigest()
        assert reply.headers["X-Network-Access-Scope"] == expected_scope
    else:
        assert "X-Network-Access-Scope" not in reply.headers
    assert not context.driver.in_transaction and context.session.events[-1][0] == "exit"
