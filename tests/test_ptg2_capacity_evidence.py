# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import base64
import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sanic import Sanic
from sanic.request import RequestParameters
from sanic.response import raw

from api import ptg2_capacity_evidence as capacity


BASE_TIME = datetime(2026, 7, 14, 10, 0, 0, tzinfo=timezone.utc)
CHALLENGE_ONE = "01" * 32
CHALLENGE_TWO = "02" * 32
CHALLENGE_THREE = "03" * 32
RUN_ONE = "a1" * 32
RUN_TWO = "a2" * 32
CONTENTION_RUN_ONE = "b1" * 32
CONTENTION_RUN_TWO = "b2" * 32
PROCESS_ONE = "ab" * 32
PROCESS_TWO = "cd" * 32
RELEASE_DIGEST = "11" * 32
ENVIRONMENT_ID = "22" * 32
KEY_ID = "capacity-api-key-2026-07"
PRIVATE_KEY_BYTES = bytes(range(1, 33))
PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(PRIVATE_KEY_BYTES)
PUBLIC_KEY_BYTES = PRIVATE_KEY.public_key().public_bytes(
    serialization.Encoding.Raw,
    serialization.PublicFormat.Raw,
)
ATTACKER_PUBLIC_KEY_BYTES = (
    Ed25519PrivateKey.from_private_bytes(bytes(range(33, 65)))
    .public_key()
    .public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
)

SIGNING_ENV = {
    capacity.CAPACITY_ISOLATED_PROCESS_ENV: "1",
    capacity.CAPACITY_PRIVATE_KEY_ENV: PRIVATE_KEY_BYTES.hex(),
    capacity.CAPACITY_KEY_ID_ENV: KEY_ID,
    capacity.CAPACITY_RELEASE_DIGEST_ENV: RELEASE_DIGEST,
    capacity.CAPACITY_ENVIRONMENT_ID_ENV: ENVIRONMENT_ID,
}

SIGNED_PAYLOAD_FIELDS = {
    "evidence_version",
    "signature_version",
    "signature_domain",
    "signature_algorithm",
    "api_evidence_key_id",
    "release_digest",
    "environment_id",
    "method",
    "path",
    "query_contract",
    "query_contract_digest",
    "page_limit",
    "challenge_digest",
    "run_digest",
    "semantic_query_digest",
    "scope_digest",
    "process_instance_digest",
    "process_started_at",
    "server_received_at",
    "server_observed_at",
    "server_duration_ns",
    "isolated",
    "observation_ordinal",
    "contention_run_id",
    "semantic_class",
    "selection_method",
    "selection_ordinal",
    "cold",
    "first_observation",
    "response_status",
    "response_body_sha256",
    "result_count",
}


def _install_fresh_process_identity(monkeypatch):
    process_identity = capacity._ProcessIdentity(
        process_id=os.getpid(),
        instance=PROCESS_ONE,
        started_at=BASE_TIME - timedelta(seconds=1),
        challenge_state=capacity.CapacityEvidenceState(),
    )
    monkeypatch.setattr(capacity, "_PROCESS_IDENTITY", process_identity)
    return process_identity


@pytest.fixture(autouse=True)
def _stable_api_process(monkeypatch):
    monkeypatch.setattr(capacity._PROCESS_IDENTITY, "process_id", os.getpid())
    monkeypatch.setattr(capacity._PROCESS_IDENTITY, "instance", PROCESS_ONE)
    monkeypatch.setattr(capacity._PROCESS_IDENTITY, "started_at", BASE_TIME)
    monkeypatch.setattr(
        capacity._PROCESS_IDENTITY,
        "challenge_state",
        capacity.CapacityEvidenceState(),
    )
    monkeypatch.setattr(capacity, "require_control_auth", lambda _request: None)


def _query(**overrides):
    query_parameter_map = {
        "plan_id": "tenant-plan-secret",
        "snapshot_id": "tenant-snapshot-secret",
        "mode": "exact_source",
        "code_system": "CPT",
        "code": "99213",
        "npi": "1234567890",
        "include_providers": "true",
        "include_details": "true",
        "include_sources": "true",
        "include_allowed_amounts": "false",
        "include_unverified_addresses": "true",
        "order_by": "npi",
        "order": "asc",
        "limit": "100",
        "offset": "0",
    }
    query_parameter_map.update(overrides)
    return query_parameter_map


def _request(query_parameters=None, **request_option_map):
    option_map = {
        "challenge": CHALLENGE_ONE,
        "run_nonce": RUN_ONE,
        "contention_run_id": CONTENTION_RUN_ONE,
        "semantic_class": "negative",
        "selection_ordinal": 0,
        "challenged": True,
        "headers": None,
        "method": "GET",
        "path": capacity.CAPACITY_QUERY_PATH,
    }
    option_map.update(request_option_map)
    request_header_map = {}
    if option_map["challenged"]:
        request_header_map = {
            capacity.CAPACITY_CHALLENGE_HEADER: option_map["challenge"],
            capacity.CAPACITY_RUN_NONCE_HEADER: option_map["run_nonce"],
            capacity.CAPACITY_CONTENTION_RUN_ID_HEADER: option_map[
                "contention_run_id"
            ],
            capacity.CAPACITY_SEMANTIC_CLASS_HEADER: option_map["semantic_class"],
            capacity.CAPACITY_SELECTION_ORDINAL_HEADER: str(
                option_map["selection_ordinal"]
            ),
        }
    if option_map["headers"] is not None:
        request_header_map = option_map["headers"]
    return SimpleNamespace(
        args=query_parameters or _query(),
        headers=request_header_map,
        method=option_map["method"],
        path=option_map["path"],
        ctx=SimpleNamespace(),
    )


def _response(*, body=b'{"items":[]}', status=200, headers=None):
    return SimpleNamespace(body=body, status=status, headers=headers or {})


def _issue(query_parameters=None, **issue_option_map):
    default_option_map = {
        "challenge": CHALLENGE_ONE,
        "run_nonce": RUN_ONE,
        "contention_run_id": CONTENTION_RUN_ONE,
        "semantic_class": "negative",
        "selection_ordinal": 0,
        "state": None,
        "begin_at": BASE_TIME,
        "finish_at": BASE_TIME,
        "body": b'{"items":[]}',
        "status": 200,
        "environ": None,
        "begin_monotonic_ns": 1_000_000_000,
        "finish_monotonic_ns": None,
    }
    default_option_map.update(issue_option_map)
    query_parameters = query_parameters or _query()
    request = _request(
        query_parameters,
        challenge=default_option_map["challenge"],
        run_nonce=default_option_map["run_nonce"],
        contention_run_id=default_option_map["contention_run_id"],
        semantic_class=default_option_map["semantic_class"],
        selection_ordinal=default_option_map["selection_ordinal"],
    )
    server_state = default_option_map["state"] or capacity.CapacityEvidenceState()
    context = capacity.begin_capacity_evidence(
        request,
        state=server_state,
        environ=default_option_map["environ"] or SIGNING_ENV,
        observed_at=default_option_map["begin_at"],
        monotonic_ns=default_option_map["begin_monotonic_ns"],
    )
    assert isinstance(context, capacity.CapacityEvidenceContext)
    response = _response(
        body=default_option_map["body"], status=default_option_map["status"]
    )
    finished = capacity.finish_capacity_evidence(
        request,
        response,
        observed_at=default_option_map["finish_at"],
        monotonic_ns=(
            default_option_map["finish_monotonic_ns"]
            if default_option_map["finish_monotonic_ns"] is not None
            else default_option_map["begin_monotonic_ns"]
            + int(
                (
                    default_option_map["finish_at"]
                    - default_option_map["begin_at"]
                ).total_seconds()
                * 1_000_000_000
            )
        ),
        result_count=len(json.loads(default_option_map["body"])["items"]),
    )
    assert finished is response
    return request, response


def _collect(query_parameters, response, **collector_option_map):
    default_option_map = {
        "challenge": CHALLENGE_ONE,
        "run_nonce": RUN_ONE,
        "received_at": BASE_TIME,
        "state": None,
        "headers": None,
        "response_body": None,
        "response_status": None,
        "redirect_count": 0,
        "key_id": KEY_ID,
        "public_key": PUBLIC_KEY_BYTES,
        "release_digest": RELEASE_DIGEST,
        "environment_id": ENVIRONMENT_ID,
        "contention_run_id": CONTENTION_RUN_ONE,
        "semantic_class": "negative",
        "selection_ordinal": 0,
    }
    default_option_map.update(collector_option_map)
    return capacity.collect_capacity_http_observation(
        default_option_map["headers"] or response.headers,
        challenge=default_option_map["challenge"],
        run_nonce=default_option_map["run_nonce"],
        query_parameters=query_parameters,
        response_status_code=(
            response.status
            if default_option_map["response_status"] is None
            else default_option_map["response_status"]
        ),
        response_body=(
            response.body
            if default_option_map["response_body"] is None
            else default_option_map["response_body"]
        ),
        response_redirect_count=default_option_map["redirect_count"],
        collector_received_at=default_option_map["received_at"],
        expected_api_evidence_key_id=default_option_map["key_id"],
        expected_api_evidence_public_key=default_option_map["public_key"],
        expected_release_digest=default_option_map["release_digest"],
        expected_environment_id=default_option_map["environment_id"],
        expected_contention_run_id=default_option_map["contention_run_id"],
        expected_semantic_class=default_option_map["semantic_class"],
        expected_selection_ordinal=default_option_map["selection_ordinal"],
        state=default_option_map["state"] or capacity.CapacityEvidenceState(),
    )


def _payload(response):
    encoded = response.headers[capacity.CAPACITY_PAYLOAD_HEADER]
    raw_payload = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
    return json.loads(raw_payload)


def _headers_with_tampered_payload(response, **payload_updates):
    """Return response headers with unsigned edits to the signed payload bytes."""

    signed_payload = _payload(response)
    signed_payload.update(payload_updates)
    encoded_payload = base64.urlsafe_b64encode(
        capacity.canonical_json_bytes(signed_payload)
    ).rstrip(b"=")
    tampered_header_map = dict(response.headers)
    tampered_header_map[capacity.CAPACITY_PAYLOAD_HEADER] = encoded_payload.decode(
        "ascii"
    )
    return tampered_header_map


def test_normal_public_request_is_completely_untouched(monkeypatch):
    monkeypatch.setattr(
        capacity,
        "require_control_auth",
        lambda _request: pytest.fail("normal request must not authenticate"),
    )
    request = _request(challenged=False)
    response = _response(headers={"ETag": "public"})
    malformed_environment_map = {capacity.CAPACITY_ISOLATED_PROCESS_ENV: "true"}

    assert (
        capacity.begin_capacity_evidence(request, environ=malformed_environment_map)
        is None
    )
    assert (
        capacity.finish_capacity_evidence(
            request, response, environ=malformed_environment_map
        )
        is response
    )
    assert response.headers == {"ETag": "public"}


def test_isolated_process_rejects_prewarm_route_and_remains_consumed(monkeypatch):
    _install_fresh_process_identity(monkeypatch)
    prewarm_request = _request(challenged=False)
    prewarm_request.path = "/api/v1/healthcheck/ready"

    with pytest.raises(capacity.CapacityEvidenceError, match="isolated_route_forbidden"):
        capacity.guard_isolated_capacity_process_request(
            prewarm_request,
            environ=SIGNING_ENV,
        )

    with pytest.raises(
        capacity.CapacityEvidenceError,
        match="isolated_process_already_used",
    ):
        capacity.guard_isolated_capacity_process_request(
            _request(),
            environ=SIGNING_ENV,
        )


def test_isolated_process_rejects_pricing_alias_before_handler(monkeypatch):
    _install_fresh_process_identity(monkeypatch)
    alias_request = _request()
    alias_request.path = "/api/v1/pricing/providers/by-procedure"

    with pytest.raises(capacity.CapacityEvidenceError, match="isolated_route_forbidden"):
        capacity.guard_isolated_capacity_process_request(
            alias_request,
            environ=SIGNING_ENV,
        )


def test_isolated_process_accepts_exactly_one_challenged_request(monkeypatch):
    _install_fresh_process_identity(monkeypatch)
    request = _request()
    capacity.guard_isolated_capacity_process_request(request, environ=SIGNING_ENV)

    context = capacity.begin_capacity_evidence(
        request,
        environ=SIGNING_ENV,
        observed_at=BASE_TIME,
        monotonic_ns=1_000_000_000,
    )
    assert context is not None
    assert context.observation_ordinal == 0

    second_request = _request(
        _query(code="99214"),
        challenge=CHALLENGE_TWO,
    )
    with pytest.raises(
        capacity.CapacityEvidenceError,
        match="isolated_process_already_used",
    ):
        capacity.guard_isolated_capacity_process_request(
            second_request,
            environ=SIGNING_ENV,
        )


def _initialized_capacity_evidence_app(monkeypatch) -> Sanic:
    """Build the routed API with deterministic PTG serving dependencies."""

    import api as api_package
    from api import control as control_api
    from api.endpoint import pricing

    async def fake_search(_session, _args, pagination):
        return {
            "items": [{"npi": 1234567890, "tic_prices": []}],
            "pagination": {
                "total": 1,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"source": "ptg2"},
        }

    def bind_test_session(app):
        @app.middleware("request")
        async def bind_session(request):
            request.ctx.sa_session = object()

    async def skip_control_schema_ensure():
        return None

    process_identity = _install_fresh_process_identity(monkeypatch)
    process_identity.started_at = datetime.now(timezone.utc).replace(microsecond=0)
    for environment_name, environment_value in SIGNING_ENV.items():
        monkeypatch.setenv(environment_name, environment_value)
    monkeypatch.setattr(api_package.db, "init_app", bind_test_session)
    monkeypatch.setattr(
        control_api,
        "ensure_import_run_table",
        skip_control_schema_ensure,
    )
    monkeypatch.setattr(pricing, "search_current_ptg2_index", fake_search)

    app = Sanic(f"ptg2-capacity-evidence-{uuid.uuid4().hex}")
    api_package.init_api(app)
    return app


def _capacity_evidence_request_header_map() -> dict[str, str]:
    """Return the headers for the first isolated evidence request."""

    return {
        capacity.CAPACITY_CHALLENGE_HEADER: CHALLENGE_ONE,
        capacity.CAPACITY_RUN_NONCE_HEADER: RUN_ONE,
        capacity.CAPACITY_CONTENTION_RUN_ID_HEADER: CONTENTION_RUN_ONE,
        capacity.CAPACITY_SEMANTIC_CLASS_HEADER: "matched_positive",
        capacity.CAPACITY_SELECTION_ORDINAL_HEADER: "0",
    }


@pytest.mark.asyncio
async def test_initialized_sanic_app_allows_one_canonical_evidence_request(
    monkeypatch,
):
    """Exercise the global guard and pricing handler through Sanic routing."""

    app = _initialized_capacity_evidence_app(monkeypatch)
    request_header_map = _capacity_evidence_request_header_map()

    _request_one, response_one = await app.asgi_client.get(
        capacity.CAPACITY_QUERY_PATH,
        params=_query(),
        headers=request_header_map,
    )
    _request_two, response_two = await app.asgi_client.get(
        capacity.CAPACITY_QUERY_PATH,
        params=_query(code="99214"),
        headers={
            **request_header_map,
            capacity.CAPACITY_CHALLENGE_HEADER: CHALLENGE_TWO,
        },
    )

    assert response_one.status == 200
    assert capacity.CAPACITY_PAYLOAD_HEADER in response_one.headers
    assert capacity.CAPACITY_SIGNATURE_HEADER in response_one.headers
    assert response_two.status == 503
    assert response_two.json == {"error": "capacity_evidence_process_isolated"}
