# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Terminal-record hashes for rooted-graph query outcomes."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
import re
from typing import Any

from process.provider_directory_rooted_graph_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_MISSING_HTTP_STATUSES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_MISSING_RESPONSE_BYTES,
    PROVIDER_DIRECTORY_ROOTED_GRAPH_MISSING_OUTCOME_ISSUE_SHAPES,
)
from process.provider_directory_rooted_graph_identity import (
    ROOTED_GRAPH_QUERY_PATTERN,
    SHA256_PATTERN,
)
from process.provider_directory_rooted_graph_query import (
    ROOTED_GRAPH_QUERY_DIRECT_READ,
)
from process.provider_directory_rooted_graph_store_contract import (
    ProviderDirectoryRootedGraphWorkClaim,
    _sha256_text,
)


PROVIDER_DIRECTORY_ROOTED_GRAPH_TERMINAL_RECORD_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-terminal-record.v2"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_SET_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-resource-set.v1"
)
PROVIDER_DIRECTORY_ROOTED_GRAPH_EDGE_SET_CONTRACT_ID = (
    "healthporta.provider-directory.rooted-graph-edge-set.v1"
)
ERROR_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,127}\Z")

_MISSING_ISSUE_SHAPES_BY_STATUS = {
    status: frozenset(frozenset(issue_shape) for issue_shape in issue_shapes)
    for status, issue_shapes in (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_MISSING_OUTCOME_ISSUE_SHAPES
    )
}


def _strict_missing_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    object_by_field: dict[str, Any] = {}
    for field_name, field_value in pairs:
        if field_name in object_by_field:
            raise ValueError
        object_by_field[field_name] = field_value
    return object_by_field


def _reject_missing_json_constant(_raw_value: str) -> None:
    raise ValueError


def _strict_missing_json_float(raw_value: str) -> float:
    try:
        parsed_value = float(raw_value)
        roundtrip_token = json.dumps(
            parsed_value,
            allow_nan=False,
            separators=(",", ":"),
        )
    except (OverflowError, ValueError):
        raise ValueError
    if not math.isfinite(parsed_value) or roundtrip_token != raw_value:
        raise ValueError
    return parsed_value


def validate_rooted_graph_missing_outcome_payload(
    payload: object,
    missing_http_status: object,
) -> None:
    """Accept only the reviewed direct-read absence outcome shapes."""

    issues = payload.get("issue") if type(payload) is dict else None
    if (
        type(missing_http_status) is not int
        or missing_http_status not in _MISSING_ISSUE_SHAPES_BY_STATUS
        or type(payload) is not dict
        or payload.get("resourceType") != "OperationOutcome"
        or type(issues) is not list
        or not issues
        or any(type(issue) is not dict for issue in issues)
    ):
        raise ValueError("provider_directory_rooted_graph_missing_invalid")
    issue_shape = frozenset(
        (issue.get("severity"), issue.get("code")) for issue in issues
    )
    if (
        len(issue_shape) != len(issues)
        or issue_shape not in _MISSING_ISSUE_SHAPES_BY_STATUS[missing_http_status]
    ):
        raise ValueError("provider_directory_rooted_graph_missing_invalid")


def validate_rooted_graph_missing_response(
    missing_http_status: object,
    missing_response_sha256: object,
    missing_response_bytes: object,
    missing_response_json_text: object,
) -> None:
    """Recompute the exact bounded JSON absence witness retained by the store."""

    try:
        if type(missing_response_json_text) is not str:
            raise ValueError
        encoded_response = missing_response_json_text.encode("utf-8")
        outcome_by_field = json.loads(
            missing_response_json_text,
            object_pairs_hook=_strict_missing_json_object,
            parse_constant=_reject_missing_json_constant,
            parse_float=_strict_missing_json_float,
        )
    except (
        MemoryError,
        OverflowError,
        RecursionError,
        UnicodeError,
        ValueError,
    ):
        raise ValueError("provider_directory_rooted_graph_missing_invalid") from None
    if (
        type(missing_response_sha256) is not str
        or SHA256_PATTERN.fullmatch(missing_response_sha256) is None
        or type(missing_response_bytes) is not int
        or not 0
        < missing_response_bytes
        <= PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_MISSING_RESPONSE_BYTES
        or len(encoded_response) != missing_response_bytes
        or _sha256_text(missing_response_json_text) != missing_response_sha256
    ):
        raise ValueError("provider_directory_rooted_graph_missing_invalid")
    validate_rooted_graph_missing_outcome_payload(
        outcome_by_field,
        missing_http_status,
    )


@dataclass(frozen=True, slots=True, repr=False)
class ProviderDirectoryRootedGraphMissingWitness:
    """One successful exact 404/410 direct-read closure witness."""

    query_id: str
    missing_http_status: int
    missing_response_sha256: str
    missing_response_bytes: int
    missing_response_json_text: str = field(repr=False)
    result_sha256: str
    resource_set_sha256: str
    edge_set_sha256: str
    terminal_record_sha256: str

    def __post_init__(self) -> None:
        hashes = (
            self.result_sha256,
            self.resource_set_sha256,
            self.edge_set_sha256,
            self.terminal_record_sha256,
        )
        try:
            validate_rooted_graph_missing_response(
                self.missing_http_status,
                self.missing_response_sha256,
                self.missing_response_bytes,
                self.missing_response_json_text,
            )
        except ValueError:
            raise ValueError(
                "provider_directory_rooted_graph_missing_invalid"
            ) from None
        if (
            type(self.query_id) is not str
            or ROOTED_GRAPH_QUERY_PATTERN.fullmatch(self.query_id) is None
            or self.missing_http_status
            not in PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_MISSING_HTTP_STATUSES
            or any(
                type(value) is not str or SHA256_PATTERN.fullmatch(value) is None
                for value in hashes
            )
        ):
            raise ValueError("provider_directory_rooted_graph_missing_invalid")


def _set_hash(contract_id: str, identity_rows: tuple[str, ...]) -> str:
    return _sha256_text(contract_id + "\x1f" + "\x1e".join(identity_rows))


def _resource_hash(resource_witnesses) -> str:
    identity_rows = tuple(
        "\x1f".join(
            (
                witness.resource_type,
                witness.resource_id,
                witness.payload_sha256,
                witness.closure_scope,
            )
        )
        for witness in resource_witnesses
    )
    return _set_hash(
        PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_SET_CONTRACT_ID,
        identity_rows,
    )


def _edge_hash(edge_witnesses) -> str:
    identity_rows = tuple(
        witness.edge_sha256 + "\x1f" + witness.closure_scope
        for witness in edge_witnesses
    )
    return _set_hash(
        PROVIDER_DIRECTORY_ROOTED_GRAPH_EDGE_SET_CONTRACT_ID,
        identity_rows,
    )


def _terminal_hash(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    result_sha256: str,
    resource_count: int,
    edge_count: int,
    advertised_total: int | None,
    terminal_page_count: int,
) -> str:
    fields = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_TERMINAL_RECORD_CONTRACT_ID,
        claim.query_identity_sha256,
        "completed",
        result_sha256,
        str(resource_count),
        str(edge_count),
        "" if advertised_total is None else str(advertised_total),
        str(terminal_page_count),
        "true",
        "",
    )
    return _sha256_text("\x1f".join(fields))


def _missing_terminal_hash(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    missing_http_status: int,
    missing_response_sha256: str,
    missing_response_bytes: int,
    result_sha256: str,
) -> str:
    fields = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_TERMINAL_RECORD_CONTRACT_ID,
        claim.query_identity_sha256,
        "missing",
        result_sha256,
        "0",
        "0",
        "",
        "1",
        "true",
        str(missing_http_status),
        missing_response_sha256,
        str(missing_response_bytes),
    )
    return _sha256_text("\x1f".join(fields))


def build_rooted_graph_missing_witness(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    missing_http_status: int,
    missing_response_sha256: str,
    missing_response_bytes: int,
    missing_response_json_text: str,
) -> ProviderDirectoryRootedGraphMissingWitness:
    """Close an exact absent direct read without recording an acquisition error."""

    if (
        type(claim) is not ProviderDirectoryRootedGraphWorkClaim
        or claim.kind != ROOTED_GRAPH_QUERY_DIRECT_READ
        or type(missing_http_status) is not int
        or missing_http_status
        not in PROVIDER_DIRECTORY_ROOTED_GRAPH_DIRECT_MISSING_HTTP_STATUSES
    ):
        raise ValueError("provider_directory_rooted_graph_missing_invalid")
    validate_rooted_graph_missing_response(
        missing_http_status,
        missing_response_sha256,
        missing_response_bytes,
        missing_response_json_text,
    )
    resource_set_sha256 = _resource_hash(())
    edge_set_sha256 = _edge_hash(())
    result_sha256 = _sha256_text(
        "\x1f".join(
            (
                resource_set_sha256,
                edge_set_sha256,
                missing_response_sha256,
                str(missing_response_bytes),
            )
        )
    )
    return ProviderDirectoryRootedGraphMissingWitness(
        query_id=claim.query_id,
        missing_http_status=missing_http_status,
        missing_response_sha256=missing_response_sha256,
        missing_response_bytes=missing_response_bytes,
        missing_response_json_text=missing_response_json_text,
        result_sha256=result_sha256,
        resource_set_sha256=resource_set_sha256,
        edge_set_sha256=edge_set_sha256,
        terminal_record_sha256=_missing_terminal_hash(
            claim,
            missing_http_status,
            missing_response_sha256,
            missing_response_bytes,
            result_sha256,
        ),
    )


def rooted_graph_error_terminal_sha256(
    claim: ProviderDirectoryRootedGraphWorkClaim,
    error_code: str,
) -> str:
    """Build an acquisition-neutral terminal error witness."""

    if type(claim) is not ProviderDirectoryRootedGraphWorkClaim:
        raise ValueError("provider_directory_rooted_graph_claim_invalid")
    if type(error_code) is not str or ERROR_PATTERN.fullmatch(error_code) is None:
        raise ValueError("provider_directory_rooted_graph_error_invalid")
    fields = (
        PROVIDER_DIRECTORY_ROOTED_GRAPH_TERMINAL_RECORD_CONTRACT_ID,
        claim.query_identity_sha256,
        "error",
        "",
        "0",
        "0",
        "",
        "0",
        "false",
        error_code,
    )
    return _sha256_text("\x1f".join(fields))


build_provider_directory_rooted_graph_missing_witness = (
    build_rooted_graph_missing_witness
)
provider_directory_rooted_graph_error_terminal_sha256 = (
    rooted_graph_error_terminal_sha256
)


__all__ = (
    "build_provider_directory_rooted_graph_missing_witness",
    "build_rooted_graph_missing_witness",
    "validate_rooted_graph_missing_outcome_payload",
    "validate_rooted_graph_missing_response",
    "provider_directory_rooted_graph_error_terminal_sha256",
    "rooted_graph_error_terminal_sha256",
    "ProviderDirectoryRootedGraphMissingWitness",
    "ERROR_PATTERN",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_EDGE_SET_CONTRACT_ID",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_MAX_MISSING_RESPONSE_BYTES",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_RESOURCE_SET_CONTRACT_ID",
    "PROVIDER_DIRECTORY_ROOTED_GRAPH_TERMINAL_RECORD_CONTRACT_ID",
)
