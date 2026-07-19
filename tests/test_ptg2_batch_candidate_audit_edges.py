from __future__ import annotations

import datetime
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_batch_candidate_audit as batch_audit
from process.ptg_parts import ptg2_batch_candidate_audit_report_schema as report_schema
from process.ptg_parts import ptg2_candidate_audit_batch_response as batch_response
from process.ptg_parts.ptg2_batch_candidate_audit_report import (
    validate_batch_candidate_release_audit_report,
)
from tests.test_ptg2_batch_candidate_audit import (
    _audit_sample,
    _http_config,
    _request,
    _response_payload,
    _source_witness,
    _target,
    _v4_report,
)


class ChunkContent:
    def __init__(self, chunks):
        self.chunks = chunks

    async def iter_chunked(self, _chunk_size):
        for chunk in self.chunks:
            yield chunk


def _validate(report, *, evaluated_at=None):
    return validate_batch_candidate_release_audit_report(
        report,
        snapshot_id="candidate-snapshot",
        source_key="derived-source",
        plan_id="12-3456789",
        plan_market_type="group",
        evaluated_at=evaluated_at,
    )


@pytest.mark.parametrize("deadline_seconds", (0, -1, 301))
def test_batch_deadline_rejects_nonpositive_or_excessive_values(deadline_seconds):
    http_config = replace(
        _http_config("https://candidate-api.internal.example"),
        deadline_seconds=deadline_seconds,
    )

    with pytest.raises(
        batch_audit.BatchCandidateAuditContractError,
        match="batch_deadline_invalid",
    ):
        batch_audit._validated_http_deadline(http_config)


@pytest.mark.asyncio
async def test_batch_client_rejects_oversized_and_invalid_json_responses():
    oversized_response = SimpleNamespace(
        content=ChunkContent(
            [b"x" * (batch_audit.PTG2_BATCH_AUDIT_MAX_RESPONSE_BYTES + 1)]
        )
    )
    with pytest.raises(
        batch_audit.BatchCandidateAuditContractError,
        match="batch_response_too_large",
    ):
        await batch_audit._bounded_response_body(oversized_response)

    with pytest.raises(
        batch_audit.BatchCandidateAuditContractError,
        match="batch_response_json_invalid",
    ):
        batch_audit._response_payload(b"\xff")


def test_event_loop_contract_requires_uvloop_when_configured(monkeypatch):
    monkeypatch.setattr(
        batch_audit.asyncio,
        "get_running_loop",
        lambda: SimpleNamespace(),
    )

    with pytest.raises(
        batch_audit.BatchCandidateAuditContractError,
        match="uvloop_required",
    ):
        batch_audit._event_loop_contract(require_uvloop=True)
    assert "SimpleNamespace" in batch_audit._event_loop_contract(
        require_uvloop=False
    )


def _connector_failure() -> batch_audit.aiohttp.ClientConnectorError:
    connection_key = SimpleNamespace(
        host="private.example",
        port=443,
        ssl=True,
    )
    return batch_audit.aiohttp.ClientConnectorError(
        connection_key,
        OSError("sensitive-transport-detail"),
    )


@pytest.mark.parametrize(
    ("transport_exception", "expected_reason"),
    (
        (
            batch_audit.aiohttp.ServerTimeoutError(
                "sensitive-transport-detail"
            ),
            "batch_deadline_exceeded",
        ),
        (_connector_failure(), "batch_endpoint_connect_failed"),
        (
            batch_audit.aiohttp.ServerDisconnectedError(
                "sensitive-transport-detail"
            ),
            "batch_endpoint_server_disconnected",
        ),
        (
            batch_audit.aiohttp.ClientPayloadError(
                "sensitive-transport-detail"
            ),
            "batch_response_incomplete",
        ),
        (
            batch_audit.aiohttp.ClientError("sensitive-transport-detail"),
            "batch_endpoint_transport_failed",
        ),
    ),
)
@pytest.mark.asyncio
async def test_batch_client_classifies_transport_without_retry_or_details(
    monkeypatch,
    transport_exception,
    expected_reason,
):
    observed_posts = []

    class FailingRequestContext:
        async def __aenter__(self):
            raise transport_exception

        async def __aexit__(self, *_exc_info):
            return False

    class RecordingClientSession:
        def __init__(self, **session_options):
            self.session_options = session_options

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_exc_info):
            return False

        def post(self, request_url, **request_options):
            observed_posts.append((request_url, request_options))
            return FailingRequestContext()

    monkeypatch.setattr(
        batch_audit.aiohttp,
        "ClientSession",
        RecordingClientSession,
    )

    with pytest.raises(
        batch_audit.BatchCandidateAuditTransportError,
        match=expected_reason,
    ) as exc_info:
        await batch_audit.run_batch_candidate_audit(
            audit_target=_target(),
            http_config=_http_config("https://private.example"),
        )

    assert len(observed_posts) == 1
    assert exc_info.value.reason == expected_reason
    assert str(exc_info.value) == expected_reason
    assert "sensitive-transport-detail" not in str(exc_info.value)
    assert "private.example" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_batch_client_wraps_response_contract_failure(monkeypatch):
    monkeypatch.setattr(
        batch_audit,
        "_post_batch_request",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        batch_audit,
        "_event_loop_contract",
        lambda **_kwargs: "uvloop",
    )

    with pytest.raises(
        batch_audit.BatchCandidateAuditContractError,
        match="batch_response_invalid",
    ):
        await batch_audit.run_batch_candidate_audit(
            audit_target=_target(),
            http_config=_http_config("https://candidate-api.internal.example"),
        )


@pytest.mark.parametrize(
    ("raw_digest", "message"),
    (
        ("abc", "digest is invalid"),
        ("z" * 64, "digest is invalid"),
    ),
)
def test_report_digest_rejects_wrong_length_or_nonhex(raw_digest, message):
    with pytest.raises(ValueError, match=message):
        report_schema.report_digest_hex(raw_digest, field_name="digest")


def test_report_schema_rejects_noncanonical_and_malformed_scalars(monkeypatch):
    with pytest.raises(ValueError, match="not canonical JSON"):
        report_schema.canonical_report_bytes({"not_json": object()})
    with pytest.raises(ValueError, match="mapping is invalid"):
        report_schema.strict_report_mapping([], field_name="mapping")
    with pytest.raises(ValueError, match="mapping is invalid"):
        report_schema.strict_report_mapping(
            {"unexpected": True},
            field_name="mapping",
            expected_fields=frozenset({"expected"}),
        )
    for invalid_number in (True, -1, 1.5):
        with pytest.raises(ValueError, match="counter is invalid"):
            report_schema.strict_nonnegative_report_int(
                invalid_number,
                field_name="counter",
            )

    for invalid_timestamp in (None, "", "not-a-timestamp"):
        with pytest.raises(ValueError, match="timestamp is invalid"):
            report_schema.report_timestamp(
                invalid_timestamp,
                field_name="timestamp",
            )
    with pytest.raises(ValueError, match="must include a timezone"):
        report_schema.report_timestamp(
            "2026-07-19T12:00:00",
            field_name="timestamp",
        )

    monkeypatch.setenv(report_schema.REPORT_MAX_AGE_MINUTES_ENV, "invalid")
    assert report_schema.report_max_age() == datetime.timedelta(minutes=120)
    monkeypatch.setenv(report_schema.REPORT_MAX_AGE_MINUTES_ENV, "9999")
    assert report_schema.report_max_age() == datetime.timedelta(days=1)


@pytest.mark.parametrize(
    ("helper", "invalid_value", "message"),
    (
        (batch_response._lower_hex_digest, "z" * 64, "digest_invalid"),
        (batch_response._positive_int, 0, "count_invalid"),
        (batch_response._positive_int, True, "count_invalid"),
        (batch_response._nonnegative_int, -1, "count_invalid"),
        (batch_response._nonnegative_int, 1.5, "count_invalid"),
    ),
)
def test_batch_response_scalar_contracts(helper, invalid_value, message):
    with pytest.raises(ValueError, match=message):
        helper(invalid_value, field_name=message.removesuffix("_invalid"))


@pytest.mark.parametrize("invalid_duration", (True, -1, float("inf"), "1"))
def test_batch_response_rejects_invalid_duration(invalid_duration):
    with pytest.raises(ValueError, match="audit_batch_duration_invalid"):
        batch_response._validated_duration_ms(invalid_duration)


def test_batch_response_rejects_wrong_fields_and_contract():
    request = _request()
    with pytest.raises(ValueError, match="response_fields_invalid"):
        batch_response.parse_audit_batch_response(
            {},
            request=request,
            expected_source_witness=_source_witness(),
            expected_audit_sample=_audit_sample(),
        )

    response_payload_by_field = _response_payload(request.request_digest)
    response_payload_by_field["contract"] = "wrong"
    with pytest.raises(ValueError, match="response_contract_invalid"):
        batch_response.parse_audit_batch_response(
            response_payload_by_field,
            request=request,
            expected_source_witness=_source_witness(),
            expected_audit_sample=_audit_sample(),
        )


@pytest.mark.parametrize(
    ("section", "field_name", "invalid_value", "message"),
    (
        (None, "duration_seconds", -1, "timing is invalid"),
        (None, "status", "fail", "release contract is invalid"),
        ("target", "source_key_sha256", "0" * 64, "does not match"),
        ("target", "transport_contract", "insecure", "transport contract"),
        ("redaction", "policy", "none", "redaction contract"),
        ("source", "source_count", 0, "source count"),
        ("source", "source_set_digest", "0" * 64, "source set"),
        ("coverage", "emitted_rate_row_count", 999, "bounded coverage"),
        ("checks", "source_witnesses", 999, "checks are invalid"),
        ("batch", "endpoint_duration_ms", -1, "endpoint timing"),
        ("api_audit_sample", "sample_digest_validated", False, "sample binding"),
    ),
)
def test_v4_report_rejects_invalid_section_contracts(
    section,
    field_name,
    invalid_value,
    message,
):
    report_by_field = deepcopy(_v4_report())
    target_mapping = report_by_field if section is None else report_by_field[section]
    target_mapping[field_name] = invalid_value

    with pytest.raises(ValueError, match=message):
        _validate(report_by_field)


def test_v4_report_rejects_future_stale_and_unknown_io_sections():
    now = datetime.datetime.now(datetime.timezone.utc)
    future_report = deepcopy(_v4_report())
    future_report["started_at"] = (
        now + datetime.timedelta(minutes=10, seconds=-1)
    ).isoformat()
    future_report["completed_at"] = (now + datetime.timedelta(minutes=10)).isoformat()
    future_report["duration_seconds"] = 1.0
    with pytest.raises(ValueError, match="completion time is in the future"):
        _validate(future_report, evaluated_at=now)

    stale_report = deepcopy(_v4_report())
    stale_report["started_at"] = (now - datetime.timedelta(hours=3, seconds=1)).isoformat()
    stale_report["completed_at"] = (now - datetime.timedelta(hours=3)).isoformat()
    stale_report["duration_seconds"] = 1.0
    with pytest.raises(ValueError, match="too old"):
        _validate(stale_report, evaluated_at=now)

    io_report = deepcopy(_v4_report())
    io_report["io"]["unexpected"] = {}
    with pytest.raises(ValueError, match="I/O fields are invalid"):
        _validate(io_report)


def test_v4_report_field_set_and_naive_timestamp_contracts():
    report_by_field = deepcopy(_v4_report())
    report_by_field.pop("profile")
    with pytest.raises(ValueError, match="report fields are invalid"):
        _validate(report_by_field)

    evidence_by_field = _validate(
        _v4_report(),
        evaluated_at=datetime.datetime.now(datetime.timezone.utc).replace(
            tzinfo=None
        ),
    )
    assert evidence_by_field["contract"] == (
        report_schema.PTG2_BATCH_AUDIT_ATTESTATION_CONTRACT
    )
