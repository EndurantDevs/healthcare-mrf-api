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
