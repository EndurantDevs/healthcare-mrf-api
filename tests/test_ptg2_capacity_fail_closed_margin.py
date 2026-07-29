# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed boundary contracts for capacity evidence parsing and isolation."""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import api.ptg2_capacity_evidence as capacity


def _assert_capacity_error(call, code: str, field: str) -> None:
    with pytest.raises(capacity.CapacityEvidenceError) as raised:
        call()
    assert raised.value.code == code
    assert raised.value.field == field


@pytest.mark.parametrize("option", (False, 0, "1", object()))
def test_replay_state_capacity_must_be_a_positive_integer(option) -> None:
    _assert_capacity_error(
        lambda: capacity.CapacityEvidenceState(max_processes=option),
        "invalid_state",
        "capacity",
    )


def _process_claim(
    *,
    ordinal: int,
    cold: bool,
    first: bool,
) -> capacity._CollectorProcessClaim:
    return capacity._CollectorProcessClaim(
        run_digest="a" * 64,
        process_instance_digest="b" * 64,
        process_started_at="2026-07-30T00:00:00Z",
        observation_ordinal=ordinal,
        is_cold=cold,
        is_first_observation=first,
    )


@pytest.mark.parametrize(
    ("claim", "field"),
    (
        (_process_claim(ordinal=1, cold=True, first=True), "observation_ordinal"),
        (_process_claim(ordinal=0, cold=True, first=False), "first_observation"),
    ),
)
def test_collector_cold_process_claim_is_internally_consistent(claim, field) -> None:
    state = capacity.CapacityEvidenceState()
    _assert_capacity_error(
        lambda: state._claim_cold_process_locked(process_claim=claim),
        "invalid_cold_state",
        field,
    )


def test_warm_collector_observation_does_not_consume_process_capacity() -> None:
    state = capacity.CapacityEvidenceState(max_processes=1)
    state._claim_cold_process_locked(
        process_claim=_process_claim(ordinal=1, cold=False, first=False)
    )
    assert state._cold_process_count == 0
    assert state._cold_processes_by_run == {}


def test_evidence_context_can_be_consumed_only_once() -> None:
    context = object.__new__(capacity.CapacityEvidenceContext)
    context._finished = False
    context._finish_lock = threading.Lock()
    context.consume_for_finish()
    _assert_capacity_error(
        context.consume_for_finish,
        "evidence_already_finished",
        "context",
    )


def test_nonce_generation_rejects_the_all_zero_sentinel(monkeypatch) -> None:
    values = iter(("0" * 64, "1" * 64))
    monkeypatch.setattr(capacity.secrets, "token_hex", lambda _size: next(values))
    assert capacity._new_nonce() == "1" * 64


def test_process_identity_reset_replaces_every_inherited_mutable(monkeypatch) -> None:
    started_at = datetime(2026, 7, 30, tzinfo=timezone.utc)
    identity = capacity._ProcessIdentity(
        process_id=1,
        instance="old",
        started_at=started_at,
        challenge_state=capacity.CapacityEvidenceState(),
        isolated_request_instance="old",
    )
    old_lock = identity.lock
    old_state = identity.challenge_state
    monkeypatch.setattr(capacity.os, "getpid", lambda: 42)
    monkeypatch.setattr(capacity, "_new_nonce", lambda: "2" * 64)
    monkeypatch.setattr(capacity, "_utc_now_seconds", lambda: started_at)

    identity.reset_after_fork()

    assert identity.process_id == 42
    assert identity.instance == "2" * 64
    assert identity.started_at == started_at
    assert identity.challenge_state is not old_state
    assert identity.isolated_request_instance is None
    assert identity.lock is not old_lock


def test_process_identity_claim_resets_a_forked_process(monkeypatch) -> None:
    started_at = datetime(2026, 7, 30, tzinfo=timezone.utc)
    identity = capacity._ProcessIdentity(
        process_id=1,
        instance="old",
        started_at=started_at,
        challenge_state=capacity.CapacityEvidenceState(),
    )
    monkeypatch.setattr(capacity.os, "getpid", lambda: 42)
    monkeypatch.setattr(capacity, "_new_nonce", lambda: "3" * 64)
    monkeypatch.setattr(capacity, "_utc_now_seconds", lambda: started_at)

    assert identity.claim_isolated_request() == "3" * 64
    assert identity.current()[0] == "3" * 64


@pytest.mark.parametrize("value", ({"bad": object()}, float("nan")))
def test_canonical_evidence_json_rejects_unstable_values(value) -> None:
    _assert_capacity_error(
        lambda: capacity.canonical_json_bytes(value),
        "invalid_canonical_json",
        "value",
    )


@pytest.mark.parametrize(
    ("call", "code", "field"),
    (
        (lambda: capacity.normalize_capacity_code_system(1), "invalid_code_system", "code_system"),
        (lambda: capacity.normalize_capacity_code("CPT", 70553), "invalid_code", "code"),
        (lambda: capacity.normalize_capacity_code("MS_DRG", "1234"), "invalid_code", "code"),
        (lambda: capacity.normalize_capacity_code("CPT", "ABC12"), "invalid_code", "code"),
        (lambda: capacity._selection_ordinal(str(1 << 63), "ordinal"), "invalid_selection_ordinal", "ordinal"),
        (lambda: capacity._validated_monotonic_ns(True, "clock"), "invalid_timestamp", "clock"),
        (lambda: capacity._validated_monotonic_ns(-1, "clock"), "invalid_timestamp", "clock"),
        (lambda: capacity._utc_timestamp(datetime(2026, 7, 30), "observed"), "invalid_timestamp", "observed"),
        (lambda: capacity._parse_timestamp("2026-07-30 00:00:00", "observed"), "invalid_timestamp", "observed"),
        (lambda: capacity._parse_timestamp("2026-02-30T00:00:00Z", "observed"), "invalid_timestamp", "observed"),
    ),
)
def test_scalar_evidence_coordinates_fail_closed(call, code, field) -> None:
    _assert_capacity_error(call, code, field)


class _BrokenItems:
    def items(self, *args, **kwargs):
        if args or kwargs:
            raise TypeError("multi unsupported")
        raise ValueError("iteration failed")


class _MissingItems:
    items = None


@pytest.mark.parametrize("mapping", (_BrokenItems(), _MissingItems()))
def test_mapping_protocol_failures_are_redacted(mapping) -> None:
    _assert_capacity_error(
        lambda: capacity._mapping_items(mapping),
        "invalid_mapping",
        "items",
    )


def test_mapping_and_parameter_names_must_be_exact_scalars() -> None:
    _assert_capacity_error(
        lambda: capacity._mapping_keys({1: "value"}),
        "invalid_request_contract",
        "parameter",
    )
    _assert_capacity_error(
        lambda: capacity._single_parameter({"code": ["70553"]}, "code"),
        "duplicate_parameter",
        "code",
    )
    for value in (" 70553", "70553\n", "705\x0153"):
        _assert_capacity_error(
            lambda candidate=value: capacity._strict_query_scalar(candidate, "code"),
            "invalid_request_contract",
            "code",
        )


class _FailingAccessor(dict):
    def getall(self, _name):
        raise KeyError("absent")

    def getlist(self, _name):
        raise TypeError("unsupported")


class _DuplicateAccessor(dict):
    def getall(self, _name):
        return ["one", "two"]


class _AccessorOnly(dict):
    def items(self, *_args, **_kwargs):
        return []

    def getall(self, _name):
        return ["value"]


def test_mapping_accessors_fall_back_or_reject_duplicates() -> None:
    assert capacity._single_parameter(_FailingAccessor(code="70553"), "code") == "70553"
    _assert_capacity_error(
        lambda: capacity._single_parameter(_DuplicateAccessor(code="70553"), "code"),
        "duplicate_parameter",
        "code",
    )
    _assert_capacity_error(
        lambda: capacity._header_values(_DuplicateAccessor(Header="value"), "Header"),
        "duplicate_header",
        "Header",
    )
    assert capacity._header_values(_AccessorOnly(), "Header") == ["value"]


def test_request_and_state_protocols_reject_ambiguous_objects() -> None:
    _assert_capacity_error(
        lambda: capacity.canonicalize_capacity_query(None),
        "invalid_request_contract",
        "parameters",
    )
    invalid_claim = SimpleNamespace()
    setattr(invalid_claim, capacity._ISOLATED_REQUEST_ATTRIBUTE, 42)
    _assert_capacity_error(
        lambda: capacity._isolated_request_claim(invalid_claim),
        "invalid_request",
        "isolated_request",
    )
    invalid_context = SimpleNamespace()
    setattr(invalid_context, capacity._REQUEST_CONTEXT_ATTRIBUTE, object())
    _assert_capacity_error(
        lambda: capacity._take_request_context(invalid_context),
        "invalid_request",
        "context",
    )
    _assert_capacity_error(
        lambda: capacity._request_evidence_state(object(), capacity.CapacityEvidenceState()),
        "invalid_state",
        "server",
    )


def test_isolation_guards_require_mapping_environment() -> None:
    request = SimpleNamespace(path="/health", headers={})
    _assert_capacity_error(
        lambda: capacity.guard_isolated_capacity_process_request(request, environ=[]),
        "invalid_environment",
        "mapping",
    )
    _assert_capacity_error(
        lambda: capacity.begin_capacity_evidence(request, environ=[]),
        "invalid_environment",
        "mapping",
    )


def test_capacity_header_names_must_be_strings() -> None:
    _assert_capacity_error(
        lambda: capacity._validate_capacity_header_names({1: "value"}, frozenset()),
        "invalid_header",
        "name",
    )


@pytest.mark.parametrize(
    ("environment", "strict", "expected"),
    (({}, True, False), ({capacity.CAPACITY_ISOLATED_PROCESS_ENV: "1"}, True, True)),
)
def test_isolated_process_configuration_is_explicit(environment, strict, expected) -> None:
    assert capacity._is_explicitly_isolated(environment, strict=strict) is expected


def test_invalid_isolation_and_required_environment_values_fail_closed() -> None:
    _assert_capacity_error(
        lambda: capacity._is_explicitly_isolated(
            {capacity.CAPACITY_ISOLATED_PROCESS_ENV: "true"}, strict=True
        ),
        "invalid_environment",
        capacity.CAPACITY_ISOLATED_PROCESS_ENV,
    )
    _assert_capacity_error(
        lambda: capacity._required_environment_value({}, "KEY"),
        "missing_environment",
        "KEY",
    )
    _assert_capacity_error(
        lambda: capacity._required_environment_value({"KEY": " value "}, "KEY"),
        "invalid_environment",
        "KEY",
    )


class _ImmutableRequest:
    __slots__ = ()


def test_request_context_attachment_rejects_duplicate_and_immutable_targets() -> None:
    request = SimpleNamespace()
    context = object.__new__(capacity.CapacityEvidenceContext)
    capacity._attach_request_context(request, context)
    _assert_capacity_error(
        lambda: capacity._attach_request_context(request, context),
        "evidence_already_begun",
        "context",
    )
    _assert_capacity_error(
        lambda: capacity._attach_request_context(_ImmutableRequest(), context),
        "invalid_request",
        "context",
    )
    capacity._attach_isolated_request_claim(request, "process")
    _assert_capacity_error(
        lambda: capacity._attach_isolated_request_claim(request, "process"),
        "isolated_request_already_claimed",
        "request",
    )
    _assert_capacity_error(
        lambda: capacity._attach_isolated_request_claim(_ImmutableRequest(), "process"),
        "invalid_request",
        "isolated_request",
    )


def test_response_shape_is_validated_before_signing() -> None:
    assert capacity._response_status(SimpleNamespace(status=None, status_code=200)) == 200
    _assert_capacity_error(
        lambda: capacity._response_status(SimpleNamespace(status=200, status_code=201)),
        "invalid_response",
        "status",
    )
    _assert_capacity_error(
        lambda: capacity._response_status(SimpleNamespace(status=True, status_code=None)),
        "invalid_response",
        "status",
    )
    assert capacity._response_body(SimpleNamespace(body=bytearray(b"{}"))) == b"{}"
    assert capacity._response_body(SimpleNamespace(body=memoryview(b"{}"))) == b"{}"
    _assert_capacity_error(
        lambda: capacity._response_body(SimpleNamespace(body="{}")),
        "invalid_response",
        "body",
    )
    _assert_capacity_error(
        lambda: capacity._response_headers(SimpleNamespace(headers=())),
        "invalid_response",
        "headers",
    )


@pytest.mark.parametrize("body", (b"not-json", b"[]"))
def test_response_result_count_requires_a_json_object(body) -> None:
    _assert_capacity_error(
        lambda: capacity._response_result_count(body, 0),
        "invalid_response",
        "result_count",
    )
