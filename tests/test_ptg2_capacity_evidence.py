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


def test_challenged_request_requires_explicit_isolated_process():
    environment_map = dict(SIGNING_ENV)
    environment_map.pop(capacity.CAPACITY_ISOLATED_PROCESS_ENV)

    with pytest.raises(
        capacity.CapacityEvidenceError, match="isolated_process_required"
    ):
        capacity.begin_capacity_evidence(
            _request(), environ=environment_map, observed_at=BASE_TIME
        )


def test_isolated_fixed_route_without_challenge_fails_closed():
    request = _request(challenged=False)

    with pytest.raises(capacity.CapacityEvidenceError, match="challenge_required"):
        capacity.begin_capacity_evidence(
            request, environ=SIGNING_ENV, observed_at=BASE_TIME
        )
    with pytest.raises(capacity.CapacityEvidenceError, match="evidence_not_begun"):
        capacity.finish_capacity_evidence(request, _response(), environ=SIGNING_ENV)


def test_begin_authenticates_and_finish_never_reauthenticates(monkeypatch):
    auth_calls = []
    monkeypatch.setattr(
        capacity, "require_control_auth", lambda request: auth_calls.append(request)
    )
    request = _request()
    response = _response()

    capacity.begin_capacity_evidence(
        request,
        state=capacity.CapacityEvidenceState(),
        environ=SIGNING_ENV,
        observed_at=BASE_TIME,
        monotonic_ns=1_000_000_000,
    )
    capacity.finish_capacity_evidence(
        request,
        response,
        observed_at=BASE_TIME,
        monotonic_ns=1_000_000_000,
        result_count=0,
    )

    assert auth_calls == [request]


def test_signing_secret_is_not_loaded_for_unchallenged_request():
    class SecretGuard(dict):
        def get(self, key, default=None):
            if key == capacity.CAPACITY_PRIVATE_KEY_ENV:
                pytest.fail("unchallenged request read the signing secret")
            return super().get(key, default)

    request = _request(challenged=False, path="/api/v1/health")

    assert capacity.begin_capacity_evidence(request, environ=SecretGuard()) is None


def test_capacity_query_requires_the_exact_plan_and_snapshot_scope():
    canonical = capacity.canonicalize_capacity_query(_query())

    assert canonical.scope_identity == (
        "tenant-plan-secret",
        "tenant-snapshot-secret",
    )
    assert canonical.page_limit == 100
    for missing_field in ("plan_id", "snapshot_id"):
        parameters = _query()
        parameters.pop(missing_field)
        with pytest.raises(
            capacity.CapacityEvidenceError, match="invalid_request_contract"
        ):
            capacity.canonicalize_capacity_query(parameters)


@pytest.mark.parametrize(
    ("extra_name", "extra_value"),
    [
        ("plan_market_type", "group"),
        ("source_key", "alternate"),
        ("plan_external_id", "alternate-plan"),
        ("zip5", "10001"),
    ],
)
def test_capacity_query_rejects_optional_or_alternate_scope(extra_name, extra_value):
    with pytest.raises(
        capacity.CapacityEvidenceError, match="invalid_request_contract"
    ):
        capacity.canonicalize_capacity_query(_query(**{extra_name: extra_value}))


@pytest.mark.parametrize("limit", ["1", "99", "101", "200", "0100", 100])
def test_capacity_query_binds_page_limit_to_exact_string_100(limit):
    with pytest.raises(capacity.CapacityEvidenceError):
        capacity.canonicalize_capacity_query(_query(limit=limit))


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("get", capacity.CAPACITY_QUERY_PATH),
        ("POST", capacity.CAPACITY_QUERY_PATH),
        ("GET", "/api/v1/pricing/providers/by-procedure"),
        ("GET", "/api/v1/pricing/providers/audit-search-by-procedure"),
    ],
)
def test_capacity_query_accepts_only_fixed_method_and_standard_route(method, path):
    with pytest.raises(
        capacity.CapacityEvidenceError, match="invalid_request_contract"
    ):
        capacity.canonicalize_capacity_query(_query(), method=method, path=path)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("mode", "product_search"),
        ("include_sources", "false"),
        ("order_by", "code"),
        ("order", "ASC"),
        ("offset", "1"),
    ],
)
def test_capacity_query_rejects_fixed_parameter_variants(field_name, value):
    with pytest.raises(
        capacity.CapacityEvidenceError, match="invalid_request_contract"
    ):
        capacity.canonicalize_capacity_query(_query(**{field_name: value}))


def test_semantic_identity_is_canonical_and_ignores_plan_syntax():
    alias = capacity.canonicalize_capacity_query(
        _query(
            plan_id="plan-alias-one",
            code_system=" ms-drg ",
            code=" 7 ",
            npi=" 1234567890 ",
        )
    )
    canonical = capacity.canonicalize_capacity_query(
        _query(
            plan_id="plan-alias-two",
            code_system="MS_DRG",
            code="007",
            npi="1234567890",
        )
    )

    assert alias.semantic_identity == canonical.semantic_identity
    assert alias.scope_identity != canonical.scope_identity


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("code_system", "CPT/HCPCS"),
        ("code", "99213 99214"),
        ("npi", "0123456789"),
        ("npi", 1234567890),
        ("npi", True),
    ],
)
def test_semantic_components_reject_ambiguous_values(field_name, value):
    with pytest.raises(capacity.CapacityEvidenceError):
        capacity.canonicalize_capacity_query(_query(**{field_name: value}))


@pytest.mark.parametrize(
    ("header_name", "bad_value", "expected_error"),
    [
        (capacity.CAPACITY_CHALLENGE_HEADER, "0" * 64, "invalid_nonce"),
        (capacity.CAPACITY_CHALLENGE_HEADER, "AB" * 32, "invalid_nonce"),
        (capacity.CAPACITY_CHALLENGE_HEADER, "01" * 31, "invalid_nonce"),
        (capacity.CAPACITY_RUN_NONCE_HEADER, "gg" * 32, "invalid_nonce"),
        (capacity.CAPACITY_RUN_NONCE_HEADER, f"{'a1' * 32} ", "invalid_header"),
    ],
)
def test_request_nonces_are_exactly_32_lowercase_nonzero_bytes(
    header_name, bad_value, expected_error
):
    request_header_map = {
        capacity.CAPACITY_CHALLENGE_HEADER: CHALLENGE_ONE,
        capacity.CAPACITY_RUN_NONCE_HEADER: RUN_ONE,
    }
    request_header_map[header_name] = bad_value

    with pytest.raises(capacity.CapacityEvidenceError, match=expected_error):
        capacity.begin_capacity_evidence(
            _request(headers=request_header_map), environ=SIGNING_ENV, observed_at=BASE_TIME
        )


@pytest.mark.parametrize(
    ("header_name", "bad_value", "expected_error"),
    [
        (
            capacity.CAPACITY_CONTENTION_RUN_ID_HEADER,
            "B1" * 32,
            "invalid_digest",
        ),
        (
            capacity.CAPACITY_CONTENTION_RUN_ID_HEADER,
            "0" * 64,
            "invalid_digest",
        ),
        (
            capacity.CAPACITY_SEMANTIC_CLASS_HEADER,
            "positive",
            "invalid_semantic_class",
        ),
        (
            capacity.CAPACITY_SELECTION_ORDINAL_HEADER,
            "01",
            "invalid_selection_ordinal",
        ),
        (
            capacity.CAPACITY_SELECTION_ORDINAL_HEADER,
            "-1",
            "invalid_selection_ordinal",
        ),
    ],
)
def test_measurement_headers_are_closed_and_canonical(
    header_name, bad_value, expected_error
):
    request = _request()
    request.headers[header_name] = bad_value

    with pytest.raises(capacity.CapacityEvidenceError, match=expected_error):
        capacity.begin_capacity_evidence(
            request,
            state=capacity.CapacityEvidenceState(),
            environ=SIGNING_ENV,
            observed_at=BASE_TIME,
            monotonic_ns=1_000_000_000,
        )


def test_unknown_capacity_request_header_fails_closed():
    request = _request(challenged=False)
    request.headers = {"X-HealthPorta-PTG2-Capacity-Unknown": "1"}

    with pytest.raises(capacity.CapacityEvidenceError, match="unexpected_header"):
        capacity.begin_capacity_evidence(
            request,
            state=capacity.CapacityEvidenceState(),
            environ=SIGNING_ENV,
            observed_at=BASE_TIME,
        )


@pytest.mark.parametrize(
    "duplicated_header",
    [capacity.CAPACITY_CHALLENGE_HEADER, capacity.CAPACITY_RUN_NONCE_HEADER],
)
def test_begin_rejects_case_variant_duplicate_headers(duplicated_header):
    value = CHALLENGE_ONE if "Challenge" in duplicated_header else RUN_ONE
    duplicate_header_map = {
        capacity.CAPACITY_CHALLENGE_HEADER: CHALLENGE_ONE,
        capacity.CAPACITY_RUN_NONCE_HEADER: RUN_ONE,
        duplicated_header.lower(): value,
    }

    with pytest.raises(capacity.CapacityEvidenceError, match="duplicate_header"):
        capacity.begin_capacity_evidence(
            _request(headers=duplicate_header_map), environ=SIGNING_ENV, observed_at=BASE_TIME
        )


def test_challenge_and_run_nonce_must_be_independent():
    with pytest.raises(capacity.CapacityEvidenceError, match="nonces_must_differ"):
        capacity.begin_capacity_evidence(
            _request(challenge=RUN_ONE, run_nonce=RUN_ONE),
            environ=SIGNING_ENV,
            observed_at=BASE_TIME,
        )


def test_begin_atomically_claims_query_before_handler_and_ignores_plan_variant():
    state = capacity.CapacityEvidenceState()
    capacity.begin_capacity_evidence(
        _request(_query(plan_id="plan-one")),
        state=state,
        environ=SIGNING_ENV,
        observed_at=BASE_TIME,
    )

    with pytest.raises(capacity.CapacityEvidenceError, match="repeated_logical_query"):
        capacity.begin_capacity_evidence(
            _request(
                _query(plan_id="plan-two"),
                challenge=CHALLENGE_TWO,
            ),
            state=state,
            environ=SIGNING_ENV,
            observed_at=BASE_TIME,
        )


def test_begin_rejects_replayed_challenge_with_a_different_query():
    state = capacity.CapacityEvidenceState()
    capacity.begin_capacity_evidence(
        _request(_query(code="99213")),
        state=state,
        environ=SIGNING_ENV,
        observed_at=BASE_TIME,
    )

    with pytest.raises(capacity.CapacityEvidenceError, match="replayed_challenge"):
        capacity.begin_capacity_evidence(
            _request(_query(code="99214"), challenge=CHALLENGE_ONE),
            state=state,
            environ=SIGNING_ENV,
            observed_at=BASE_TIME,
        )


def test_one_isolated_process_state_is_bound_to_one_run_nonce():
    state = capacity.CapacityEvidenceState()
    capacity.begin_capacity_evidence(
        _request(_query(code="99213")),
        state=state,
        environ=SIGNING_ENV,
        observed_at=BASE_TIME,
    )

    with pytest.raises(capacity.CapacityEvidenceError, match="run_nonce_mismatch"):
        capacity.begin_capacity_evidence(
            _request(
                _query(code="99214"),
                challenge=CHALLENGE_TWO,
                run_nonce=RUN_TWO,
            ),
            state=state,
            environ=SIGNING_ENV,
            observed_at=BASE_TIME,
        )


def test_bounded_query_state_fails_closed_without_partial_challenge_claim():
    state = capacity.CapacityEvidenceState(max_challenges=3, max_queries=1)
    capacity.begin_capacity_evidence(
        _request(_query(code="99213")),
        state=state,
        environ=SIGNING_ENV,
        observed_at=BASE_TIME,
    )
    second = _request(_query(code="99214"), challenge=CHALLENGE_TWO)

    for _attempt in range(2):
        with pytest.raises(
            capacity.CapacityEvidenceError, match="query_capacity_exhausted"
        ):
            capacity.begin_capacity_evidence(
                second,
                state=state,
                environ=SIGNING_ENV,
                observed_at=BASE_TIME,
            )


def test_bounded_challenge_state_fails_closed():
    state = capacity.CapacityEvidenceState(max_challenges=1, max_queries=3)
    capacity.begin_capacity_evidence(
        _request(_query(code="99213")),
        state=state,
        environ=SIGNING_ENV,
        observed_at=BASE_TIME,
    )

    with pytest.raises(
        capacity.CapacityEvidenceError, match="challenge_capacity_exhausted"
    ):
        capacity.begin_capacity_evidence(
            _request(_query(code="99214"), challenge=CHALLENGE_TWO),
            state=state,
            environ=SIGNING_ENV,
            observed_at=BASE_TIME,
        )


def test_second_process_local_observation_is_signed_as_not_cold():
    server_state = capacity.CapacityEvidenceState()
    first_query = _query(code="99213")
    second_query = _query(code="99214")
    _first_request, first_response = _issue(first_query, state=server_state)
    _second_request, second_response = _issue(
        second_query,
        challenge=CHALLENGE_TWO,
        selection_ordinal=1,
        state=server_state,
    )

    first_payload = _payload(first_response)
    second_payload = _payload(second_response)

    assert first_payload["observation_ordinal"] == 0
    assert first_payload["cold"] is True
    assert first_payload["first_observation"] is True
    assert second_payload["observation_ordinal"] == 1
    assert second_payload["cold"] is False
    assert second_payload["first_observation"] is False


@pytest.mark.parametrize(
    ("semantic_class", "body", "expected_error"),
    [
        ("matched_positive", b'{"items":[]}', "semantic_result_mismatch"),
        ("negative", b'{"items":[{}]}', "semantic_result_mismatch"),
    ],
)
def test_finish_rejects_semantic_class_result_mismatch(
    semantic_class, body, expected_error
):
    with pytest.raises(capacity.CapacityEvidenceError, match=expected_error):
        _issue(body=body, semantic_class=semantic_class)


def test_finish_requires_handler_supplied_exact_result_count():
    request = _request()
    response = _response(body=b'{"items":[]}')
    capacity.begin_capacity_evidence(
        request,
        state=capacity.CapacityEvidenceState(),
        environ=SIGNING_ENV,
        observed_at=BASE_TIME,
        monotonic_ns=1_000_000_000,
    )

    with pytest.raises(capacity.CapacityEvidenceError, match="invalid_payload"):
        capacity.finish_capacity_evidence(
            request,
            response,
            observed_at=BASE_TIME,
            monotonic_ns=1_000_000_000,
        )


def test_finish_rejects_handler_count_that_disagrees_with_serialized_items():
    request = _request(semantic_class="random")
    response = _response(body=b'{"items":[{},{}]}')
    capacity.begin_capacity_evidence(
        request,
        state=capacity.CapacityEvidenceState(),
        environ=SIGNING_ENV,
        observed_at=BASE_TIME,
        monotonic_ns=1_000_000_000,
    )

    with pytest.raises(capacity.CapacityEvidenceError, match="result_count_mismatch"):
        capacity.finish_capacity_evidence(
            request,
            response,
            observed_at=BASE_TIME,
            monotonic_ns=1_000_000_000,
            result_count=1,
        )


def test_finish_signs_closed_payload_over_actual_status_and_body():
    body = b'{"items":[{"cost":17.25}]}'
    query_parameters = _query()
    _request_value, response = _issue(
        query_parameters, body=body, semantic_class="matched_positive"
    )

    payload = _payload(response)
    observation = _collect(
        query_parameters, response, semantic_class="matched_positive"
    )

    assert set(payload) == SIGNED_PAYLOAD_FIELDS
    assert payload["response_status"] == 200
    assert payload["response_body_sha256"] == hashlib.sha256(body).hexdigest()
    assert payload["page_limit"] == 100
    assert payload["isolated"] is True
    assert payload["observation_ordinal"] == 0
    assert (
        observation["api_evidence_signature"]
        == response.headers[capacity.CAPACITY_SIGNATURE_HEADER]
    )


def test_begin_and_finish_support_sanic_request_and_response_mappings():
    query_parameters = RequestParameters(
        {
            parameter_name: [parameter_value]
            for parameter_name, parameter_value in _query().items()
        }
    )
    request_headers = raw(b"").headers
    request_headers[capacity.CAPACITY_CHALLENGE_HEADER] = CHALLENGE_ONE
    request_headers[capacity.CAPACITY_RUN_NONCE_HEADER] = RUN_ONE
    request_headers[capacity.CAPACITY_CONTENTION_RUN_ID_HEADER] = CONTENTION_RUN_ONE
    request_headers[capacity.CAPACITY_SEMANTIC_CLASS_HEADER] = "negative"
    request_headers[capacity.CAPACITY_SELECTION_ORDINAL_HEADER] = "0"
    request = SimpleNamespace(
        args=query_parameters,
        headers=request_headers,
        method="GET",
        path=capacity.CAPACITY_QUERY_PATH,
        ctx=SimpleNamespace(),
    )
    response = raw(b'{"items":[]}', status=200)

    capacity.begin_capacity_evidence(
        request,
        state=capacity.CapacityEvidenceState(),
        environ=SIGNING_ENV,
        observed_at=BASE_TIME,
        monotonic_ns=1_000_000_000,
    )
    capacity.finish_capacity_evidence(
        request,
        response,
        observed_at=BASE_TIME,
        monotonic_ns=1_000_000_000,
        result_count=0,
    )

    observation = _collect(_query(), response)
    assert observation["response_status"] == 200


def test_finish_rejects_preexisting_capacity_prefixed_response_header():
    request = _request()
    capacity.begin_capacity_evidence(
        request,
        state=capacity.CapacityEvidenceState(),
        environ=SIGNING_ENV,
        observed_at=BASE_TIME,
        monotonic_ns=1_000_000_000,
    )
    response = _response(headers={"X-HealthPorta-PTG2-Capacity-Raw-NPI": "1234567890"})

    with pytest.raises(capacity.CapacityEvidenceError, match="unexpected_header"):
        capacity.finish_capacity_evidence(
            request,
            response,
            observed_at=BASE_TIME,
            monotonic_ns=1_000_000_000,
            result_count=0,
        )


def test_challenged_finish_without_begin_cannot_attest_late():
    request = _request()
    response = _response()

    with pytest.raises(capacity.CapacityEvidenceError, match="evidence_not_begun"):
        capacity.maybe_attach_capacity_evidence_headers(request, response)
    assert response.headers == {}


def test_finish_context_is_one_use():
    request = _request()
    capacity.begin_capacity_evidence(
        request,
        state=capacity.CapacityEvidenceState(),
        environ=SIGNING_ENV,
        observed_at=BASE_TIME,
        monotonic_ns=1_000_000_000,
    )
    capacity.finish_capacity_evidence(
        request,
        _response(),
        observed_at=BASE_TIME,
        monotonic_ns=1_000_000_000,
        result_count=0,
    )

    with pytest.raises(capacity.CapacityEvidenceError, match="evidence_not_begun"):
        capacity.finish_capacity_evidence(request, _response())


def test_collector_rejects_forged_signature():
    query_parameters = _query()
    _request_value, response = _issue(query_parameters)
    forged_header_map = dict(response.headers)
    signature = forged_header_map[capacity.CAPACITY_SIGNATURE_HEADER]
    forged_header_map[capacity.CAPACITY_SIGNATURE_HEADER] = (
        "A" if signature[0] != "A" else "B"
    ) + signature[1:]

    with pytest.raises(capacity.CapacityEvidenceError, match="invalid_signature"):
        _collect(query_parameters, response, headers=forged_header_map)


def test_collector_rejects_v2_evidence_without_compatibility_fallback():
    query_parameters = _query()
    _request_value, response = _issue(query_parameters)
    v2_header_map = dict(response.headers)
    v2_header_map[capacity.CAPACITY_VERSION_HEADER] = "2"

    with pytest.raises(capacity.CapacityEvidenceError, match="unsupported_version"):
        _collect(query_parameters, response, headers=v2_header_map)


@pytest.mark.parametrize(
    ("field_name", "tampered_value"),
    [
        ("server_received_at", "2026-07-14T09:59:59Z"),
        ("server_duration_ns", 0),
        ("contention_run_id", CONTENTION_RUN_TWO),
        ("semantic_class", "random"),
        ("selection_ordinal", 17),
        ("result_count", 1),
    ],
)
def test_collector_rejects_unsigned_measurement_field_tampering(
    field_name, tampered_value
):
    query_parameters = _query()
    _request_value, response = _issue(
        query_parameters,
        finish_at=BASE_TIME + timedelta(milliseconds=25),
    )
    tampered_headers = _headers_with_tampered_payload(
        response, **{field_name: tampered_value}
    )

    with pytest.raises(capacity.CapacityEvidenceError, match="invalid_signature"):
        _collect(query_parameters, response, headers=tampered_headers)


@pytest.mark.parametrize(
    ("collector_override", "expected_field"),
    [
        ({"contention_run_id": CONTENTION_RUN_TWO}, "contention_run_id"),
        ({"semantic_class": "random"}, "semantic_class"),
        ({"selection_ordinal": 7}, "selection_ordinal"),
    ],
)
def test_collector_requires_independent_measurement_expectations(
    collector_override, expected_field
):
    query_parameters = _query()
    _request_value, response = _issue(query_parameters)

    with pytest.raises(capacity.CapacityEvidenceError) as error_info:
        _collect(query_parameters, response, **collector_override)

    assert error_info.value.field == expected_field


def test_server_signs_precise_monotonic_duration_and_request_start():
    query_parameters = _query()
    finish_at = BASE_TIME + timedelta(milliseconds=27, microseconds=500)
    _request_value, response = _issue(query_parameters, finish_at=finish_at)

    observation = _collect(query_parameters, response)

    assert observation["server_received_at"] == "2026-07-14T10:00:00Z"
    assert observation["server_observed_at"] == "2026-07-14T10:00:00Z"
    assert observation["server_duration_ns"] == 27_500_000


def test_server_rejects_monotonic_duration_incoherent_with_wall_timestamps():
    with pytest.raises(
        capacity.CapacityEvidenceError, match="duration_timestamp_mismatch"
    ):
        _issue(
            finish_at=BASE_TIME + timedelta(seconds=3),
            finish_monotonic_ns=1_000_000_000,
        )


def test_collector_rejects_response_body_tamper():
    query_parameters = _query()
    _request_value, response = _issue(query_parameters)

    with pytest.raises(capacity.CapacityEvidenceError, match="response_body_mismatch"):
        _collect(query_parameters, response, response_body=b'{"items":[1]}')


def test_collector_rejects_response_status_tamper():
    query_parameters = _query()
    _request_value, response = _issue(query_parameters)

    with pytest.raises(
        capacity.CapacityEvidenceError, match="response_status_mismatch"
    ):
        _collect(query_parameters, response, response_status=503)


@pytest.mark.parametrize(
    ("trust_override", "expected_error"),
    [
        ({"key_id": "other-api-key"}, "key_id_mismatch"),
        ({"public_key": ATTACKER_PUBLIC_KEY_BYTES}, "invalid_signature"),
        ({"release_digest": "33" * 32}, "release_digest_mismatch"),
        ({"environment_id": "44" * 32}, "environment_id_mismatch"),
    ],
)
def test_collector_requires_pinned_key_release_and_environment(
    trust_override, expected_error
):
    query_parameters = _query()
    _request_value, response = _issue(query_parameters)

    with pytest.raises(capacity.CapacityEvidenceError, match=expected_error):
        _collect(query_parameters, response, **trust_override)


def test_collector_rejects_redirects_before_accepting_evidence():
    query_parameters = _query()
    _request_value, response = _issue(query_parameters)

    with pytest.raises(capacity.CapacityEvidenceError, match="redirect_not_allowed"):
        _collect(query_parameters, response, redirect_count=1)


@pytest.mark.parametrize(
    "response_header",
    [
        capacity.CAPACITY_CHALLENGE_ECHO_HEADER,
        capacity.CAPACITY_RUN_NONCE_ECHO_HEADER,
        capacity.CAPACITY_QUERY_DIGEST_HEADER,
        capacity.CAPACITY_PAYLOAD_HEADER,
        capacity.CAPACITY_SIGNATURE_HEADER,
    ],
)
def test_collector_rejects_case_variant_duplicate_response_headers(
    response_header,
):
    query_parameters = _query()
    _request_value, response = _issue(query_parameters)
    duplicate_header_map = dict(response.headers)
    duplicate_header_map[response_header.lower()] = response.headers[response_header]

    with pytest.raises(capacity.CapacityEvidenceError, match="duplicate_header"):
        _collect(query_parameters, response, headers=duplicate_header_map)


def test_collector_validates_challenge_run_and_query_echoes():
    query_parameters = _query()
    _request_value, response = _issue(query_parameters)

    with pytest.raises(capacity.CapacityEvidenceError, match="challenge_mismatch"):
        _collect(query_parameters, response, challenge=CHALLENGE_TWO)
    with pytest.raises(capacity.CapacityEvidenceError, match="run_nonce_mismatch"):
        _collect(query_parameters, response, run_nonce=RUN_TWO)

    changed_query_header_map = dict(response.headers)
    changed_query_header_map[capacity.CAPACITY_QUERY_DIGEST_HEADER] = "ff" * 32
    with pytest.raises(
        capacity.CapacityEvidenceError, match="semantic_query_digest_mismatch"
    ):
        _collect(query_parameters, response, headers=changed_query_header_map)


def test_collector_accepts_300_second_total_with_five_second_receive_skew():
    query_parameters = _query()
    _request_value, response = _issue(
        query_parameters, finish_at=BASE_TIME + timedelta(seconds=295)
    )

    observation = _collect(
        query_parameters,
        response,
        received_at=BASE_TIME + timedelta(seconds=300),
    )

    assert observation["server_observed_at"] == "2026-07-14T10:04:55Z"
    assert observation["collector_received_at"] == "2026-07-14T10:05:00Z"


def test_collector_rejects_composed_300_plus_5_second_window():
    query_parameters = _query()
    _request_value, response = _issue(
        query_parameters, finish_at=BASE_TIME + timedelta(seconds=300)
    )

    with pytest.raises(capacity.CapacityEvidenceError, match="stale_process"):
        _collect(
            query_parameters,
            response,
            received_at=BASE_TIME + timedelta(seconds=305),
        )


def test_collector_rejects_more_than_five_seconds_receive_skew():
    query_parameters = _query()
    _request_value, response = _issue(
        query_parameters, finish_at=BASE_TIME + timedelta(seconds=294)
    )

    with pytest.raises(capacity.CapacityEvidenceError, match="receive_skew"):
        _collect(
            query_parameters,
            response,
            received_at=BASE_TIME + timedelta(seconds=299, microseconds=1),
        )


def test_server_rejects_process_older_than_300_seconds_before_handler():
    with pytest.raises(capacity.CapacityEvidenceError, match="stale_process"):
        capacity.begin_capacity_evidence(
            _request(),
            state=capacity.CapacityEvidenceState(),
            environ=SIGNING_ENV,
            observed_at=BASE_TIME + timedelta(seconds=300, microseconds=1),
        )


def test_collector_rejects_reused_process_identity_for_cold_observations():
    collector_state = capacity.CapacityEvidenceState()
    first_query = _query(code="99213")
    _request_value, first_response = _issue(first_query)
    _collect(first_query, first_response, state=collector_state)

    second_query = _query(code="99214")
    _request_value, second_response = _issue(
        second_query,
        challenge=CHALLENGE_TWO,
    )

    with pytest.raises(
        capacity.CapacityEvidenceError, match="reused_cold_process_identity"
    ):
        _collect(
            second_query,
            second_response,
            challenge=CHALLENGE_TWO,
            state=collector_state,
        )


def test_collector_accepts_distinct_cold_processes_for_one_run(monkeypatch):
    collector_state = capacity.CapacityEvidenceState()
    first_query = _query(code="99213")
    _request_value, first_response = _issue(first_query)
    first = _collect(first_query, first_response, state=collector_state)

    monkeypatch.setattr(capacity._PROCESS_IDENTITY, "instance", PROCESS_TWO)
    second_query = _query(code="99214")
    _request_value, second_response = _issue(
        second_query,
        challenge=CHALLENGE_TWO,
    )
    second = _collect(
        second_query,
        second_response,
        challenge=CHALLENGE_TWO,
        state=collector_state,
    )

    assert first["cold"] is True
    assert second["cold"] is True
    assert first["process_instance_digest"] != second["process_instance_digest"]


def test_collector_bounds_distinct_cold_process_identities(monkeypatch):
    collector_state = capacity.CapacityEvidenceState(max_processes=1)
    first_query = _query(code="99213")
    _request_value, first_response = _issue(first_query)
    _collect(first_query, first_response, state=collector_state)

    monkeypatch.setattr(capacity._PROCESS_IDENTITY, "instance", PROCESS_TWO)
    second_query = _query(code="99214")
    _request_value, second_response = _issue(
        second_query,
        challenge=CHALLENGE_TWO,
    )

    with pytest.raises(
        capacity.CapacityEvidenceError, match="process_capacity_exhausted"
    ):
        _collect(
            second_query,
            second_response,
            challenge=CHALLENGE_TWO,
            state=collector_state,
        )


def test_one_collector_state_rejects_exact_replay():
    query_parameters = _query()
    _request_value, response = _issue(query_parameters)
    collector_state = capacity.CapacityEvidenceState()
    _collect(query_parameters, response, state=collector_state)

    with pytest.raises(capacity.CapacityEvidenceError, match="replayed_challenge"):
        _collect(query_parameters, response, state=collector_state)


def test_one_collector_state_rejects_same_query_with_fresh_challenge():
    query_parameters = _query()
    collector_state = capacity.CapacityEvidenceState()
    _request_value, first_response = _issue(
        query_parameters,
        challenge=CHALLENGE_ONE,
        state=capacity.CapacityEvidenceState(),
    )
    _request_value, second_response = _issue(
        query_parameters,
        challenge=CHALLENGE_TWO,
        state=capacity.CapacityEvidenceState(),
    )
    _collect(query_parameters, first_response, state=collector_state)

    with pytest.raises(capacity.CapacityEvidenceError, match="repeated_logical_query"):
        _collect(
            query_parameters,
            second_response,
            challenge=CHALLENGE_TWO,
            state=collector_state,
        )


def test_query_and_scope_digests_are_run_scoped_and_plan_semantic():
    first_query = _query(plan_id="plan-one")
    second_query = _query(plan_id="plan-two")
    _request_value, first_response = _issue(
        first_query,
        challenge=CHALLENGE_ONE,
        run_nonce=RUN_ONE,
    )
    _request_value, plan_variant_response = _issue(
        second_query,
        challenge=CHALLENGE_TWO,
        run_nonce=RUN_ONE,
    )
    _request_value, other_run_response = _issue(
        first_query,
        challenge=CHALLENGE_THREE,
        run_nonce=RUN_TWO,
    )
    first = _collect(first_query, first_response)
    plan_variant = _collect(
        second_query,
        plan_variant_response,
        challenge=CHALLENGE_TWO,
    )
    other_run = _collect(
        first_query,
        other_run_response,
        challenge=CHALLENGE_THREE,
        run_nonce=RUN_TWO,
    )

    assert first["semantic_query_digest"] == plan_variant["semantic_query_digest"]
    assert first["scope_digest"] != plan_variant["scope_digest"]
    assert first["semantic_query_digest"] != other_run["semantic_query_digest"]
    assert first["scope_digest"] != other_run["scope_digest"]
    assert first["process_instance_digest"] != other_run["process_instance_digest"]


def test_observation_is_closed_and_contains_no_raw_client_or_process_values():
    query_parameters = _query()
    _request_value, response = _issue(query_parameters)
    observation = _collect(query_parameters, response)
    serialized = json.dumps(observation, sort_keys=True)

    assert set(observation) == SIGNED_PAYLOAD_FIELDS | {
        "api_evidence_signature",
        "collector_received_at",
    }
    assert not any("component" in name for name in observation)
    for raw_value in (
        query_parameters["plan_id"],
        query_parameters["snapshot_id"],
        query_parameters["code"],
        query_parameters["npi"],
        CHALLENGE_ONE,
        RUN_ONE,
        PROCESS_ONE,
    ):
        assert raw_value not in serialized


def test_durable_replay_boundary_declares_challenge_and_query_uniqueness():
    assert capacity.CapacityEvidenceState.durable_replay_unique_keys == (
        ("run_digest", "challenge_digest"),
        ("run_digest", "semantic_query_digest"),
        ("run_digest", "process_instance_digest"),
    )


@pytest.mark.parametrize(
    ("environment_change", "expected_error"),
    [
        ({capacity.CAPACITY_ISOLATED_PROCESS_ENV: "true"}, "invalid_environment"),
        ({capacity.CAPACITY_PRIVATE_KEY_ENV: "AA" * 32}, "invalid_nonce"),
        ({capacity.CAPACITY_KEY_ID_ENV: "key id with spaces"}, "invalid_environment"),
        ({capacity.CAPACITY_RELEASE_DIGEST_ENV: "0" * 64}, "invalid_digest"),
        ({capacity.CAPACITY_ENVIRONMENT_ID_ENV: "short"}, "invalid_digest"),
    ],
)
def test_challenged_request_strictly_validates_explicit_environment(
    environment_change, expected_error
):
    environment_map = dict(SIGNING_ENV)
    environment_map.update(environment_change)

    with pytest.raises(capacity.CapacityEvidenceError, match=expected_error):
        capacity.begin_capacity_evidence(
            _request(),
            state=capacity.CapacityEvidenceState(),
            environ=environment_map,
            observed_at=BASE_TIME,
        )


def test_collector_rejects_payload_field_addition_even_before_signature_check():
    query_parameters = _query()
    _request_value, response = _issue(query_parameters)
    signed_evidence = _payload(response)
    signed_evidence["raw_npi"] = query_parameters["npi"]
    raw_payload = json.dumps(signed_evidence, sort_keys=True, separators=(",", ":")).encode(
        "ascii"
    )
    forged_header_map = dict(response.headers)
    forged_header_map[capacity.CAPACITY_PAYLOAD_HEADER] = (
        base64.urlsafe_b64encode(raw_payload).rstrip(b"=").decode("ascii")
    )

    with pytest.raises(capacity.CapacityEvidenceError, match="invalid_payload"):
        _collect(query_parameters, response, headers=forged_header_map)
