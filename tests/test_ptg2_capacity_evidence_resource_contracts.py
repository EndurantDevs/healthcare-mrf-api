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

from tests.test_ptg2_capacity_evidence import (
    ATTACKER_PUBLIC_KEY_BYTES,
    BASE_TIME,
    CHALLENGE_ONE,
    CHALLENGE_THREE,
    CHALLENGE_TWO,
    CONTENTION_RUN_ONE,
    CONTENTION_RUN_TWO,
    ENVIRONMENT_ID,
    KEY_ID,
    PRIVATE_KEY,
    PRIVATE_KEY_BYTES,
    PROCESS_ONE,
    PROCESS_TWO,
    PUBLIC_KEY_BYTES,
    RELEASE_DIGEST,
    RUN_ONE,
    RUN_TWO,
    SIGNED_PAYLOAD_FIELDS,
    SIGNING_ENV,
    _capacity_evidence_request_header_map,
    _collect,
    _headers_with_tampered_payload,
    _initialized_capacity_evidence_app,
    _install_fresh_process_identity,
    _issue,
    _payload,
    _query,
    _request,
    _response,
    _stable_api_process,
)

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
