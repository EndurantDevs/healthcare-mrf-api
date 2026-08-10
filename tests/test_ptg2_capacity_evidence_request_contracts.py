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
