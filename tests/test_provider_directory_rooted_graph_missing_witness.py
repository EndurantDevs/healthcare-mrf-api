# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact retained direct-read absence witness boundaries."""

from dataclasses import replace
import json

import pytest

from process.provider_directory_rooted_graph_result_contract import (
    _edge_hash,
    _resource_hash,
    _sha256_text,
    build_provider_directory_rooted_graph_missing_witness,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import (
    missing_outcome_json_text,
)
from tests.test_provider_directory_rooted_graph_result_boundaries import (
    _direct_claim,
    _role_result,
)


def _missing_response_values() -> tuple[str, str, int]:
    response_json_text = missing_outcome_json_text(404)
    return (
        response_json_text,
        _sha256_text(response_json_text),
        len(response_json_text.encode("utf-8")),
    )


def test_direct_missing_witness_is_exact_non_error_terminal_proof() -> None:
    """Bind status, exact body, empty sets, and redacted representation."""

    direct_claim = _direct_claim()
    response_json_text, response_sha256, response_bytes = _missing_response_values()
    not_found = build_provider_directory_rooted_graph_missing_witness(
        direct_claim,
        404,
        response_sha256,
        response_bytes,
        response_json_text,
    )
    gone = build_provider_directory_rooted_graph_missing_witness(
        direct_claim,
        410,
        response_sha256,
        response_bytes,
        response_json_text,
    )

    assert not_found.query_id == direct_claim.query_id
    assert not_found.result_sha256 == gone.result_sha256
    assert not_found.resource_set_sha256 == _resource_hash(())
    assert not_found.edge_set_sha256 == _edge_hash(())
    assert not_found.terminal_record_sha256 != gone.terminal_record_sha256
    assert response_json_text not in repr(not_found)

    changed_response = json.loads(response_json_text)
    changed_response["issue"][0]["diagnostics"] = "synthetic detail"
    changed_json_text = json.dumps(changed_response, separators=(",", ":"))
    changed_witness = build_provider_directory_rooted_graph_missing_witness(
        direct_claim,
        404,
        _sha256_text(changed_json_text),
        len(changed_json_text.encode("utf-8")),
        changed_json_text,
    )
    assert changed_witness.result_sha256 != not_found.result_sha256
    assert changed_witness.terminal_record_sha256 != not_found.terminal_record_sha256


def test_direct_missing_witness_rejects_envelope_and_hash_drift() -> None:
    """Reject wrong claim/status/hash/length before store terminalization."""

    direct_claim = _direct_claim()
    response_json_text, response_sha256, response_bytes = _missing_response_values()
    for claim, status in (
        (direct_claim, 200),
        (direct_claim, 404.0),
        (_role_result()[0], 404),
    ):
        with pytest.raises(ValueError, match="missing_invalid"):
            build_provider_directory_rooted_graph_missing_witness(
                claim,
                status,
                response_sha256,
                response_bytes,
                response_json_text,
            )
    for response_hash, byte_count in (
        ("A" * 64, response_bytes),
        (response_sha256, 0),
        ("a" * 64, response_bytes),
    ):
        with pytest.raises(ValueError, match="missing_invalid"):
            build_provider_directory_rooted_graph_missing_witness(
                direct_claim,
                404,
                response_hash,
                byte_count,
                response_json_text,
            )
    with pytest.raises(ValueError, match="missing_invalid"):
        build_provider_directory_rooted_graph_missing_witness(
            direct_claim,
            404,
            response_sha256,
            response_bytes,
            None,
        )


def test_direct_missing_witness_accepts_exact_decimal_json() -> None:
    """Accept exactly representable decimals under strict transport semantics."""

    direct_claim = _direct_claim()
    response_json_text, _, _ = _missing_response_values()
    valid_decimal_response = json.loads(response_json_text)
    valid_decimal_response["score"] = 1.5
    valid_decimal_json_text = json.dumps(
        valid_decimal_response,
        separators=(",", ":"),
    )
    valid_witness = build_provider_directory_rooted_graph_missing_witness(
        direct_claim,
        404,
        _sha256_text(valid_decimal_json_text),
        len(valid_decimal_json_text.encode("utf-8")),
        valid_decimal_json_text,
    )
    assert valid_witness.missing_response_json_text == valid_decimal_json_text


@pytest.mark.parametrize(
    "invalid_json_text",
    (
        (
            '{"resourceType":"OperationOutcome",'
            '"resourceType":"OperationOutcome",'
            '"issue":[{"severity":"error","code":"not-found"}]}'
        ),
        (
            '{"resourceType":"OperationOutcome","score":NaN,'
            '"issue":[{"severity":"error","code":"not-found"}]}'
        ),
        (
            '{"resourceType":"OperationOutcome",'
            '"score":0.123456789012345678901,'
            '"issue":[{"severity":"error","code":"not-found"}]}'
        ),
        (
            '{"resourceType":"OperationOutcome","score":1e999,'
            '"issue":[{"severity":"error","code":"not-found"}]}'
        ),
        (
            '{"resourceType":"OperationOutcome",'
            '"issue":[{"severity":"information","code":"processing"}]}'
        ),
    ),
)
def test_direct_missing_witness_rejects_invalid_json_or_issue_shape(
    invalid_json_text: str,
) -> None:
    """Reject duplicate, nonfinite, imprecise, and wrong-shaped JSON."""

    with pytest.raises(ValueError, match="missing_invalid"):
        build_provider_directory_rooted_graph_missing_witness(
            _direct_claim(),
            404,
            _sha256_text(invalid_json_text),
            len(invalid_json_text.encode("utf-8")),
            invalid_json_text,
        )


def test_direct_missing_witness_dataclass_rejects_tampering() -> None:
    """Revalidate retained body and result hash on dataclass replacement."""

    direct_claim = _direct_claim()
    response_json_text, response_sha256, response_bytes = _missing_response_values()
    not_found = build_provider_directory_rooted_graph_missing_witness(
        direct_claim,
        404,
        response_sha256,
        response_bytes,
        response_json_text,
    )
    with pytest.raises(ValueError, match="missing_invalid"):
        replace(not_found, missing_response_json_text="not-json")
    with pytest.raises(ValueError, match="missing_invalid"):
        replace(not_found, result_sha256="A" * 64)
