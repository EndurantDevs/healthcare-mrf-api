# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Execution proofs for a reviewed current-version Provider Directory census."""

from __future__ import annotations

import hashlib
import json
import urllib.parse
from dataclasses import dataclass
from typing import Any, Mapping

from process.provider_directory_fhir_census_binding import (
    CurrentVersionCensusContract,
)
from process.provider_directory_fhir_census_contract import (
    CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY,
)


CURRENT_VERSION_CENSUS_FETCH_MODE = "current_version_exact_census"
CURRENT_VERSION_CENSUS_BLOCKED_ERROR = (
    "provider_directory_current_version_census_completeness_blocked"
)
CURRENT_VERSION_CENSUS_RETRYABLE_ERROR = (
    "provider_directory_current_version_census_completeness_retryable"
)
_SMILE_QUERY_NAMES = frozenset(
    {"_bundletype", "_count", "_getpages", "_getpagesoffset", "_pretty"}
)


@dataclass(frozen=True)
class CurrentVersionCensusContinuation:
    """Validated raw continuation and its canonical replay identity."""

    url: str
    identity: str
    token: str
    offset: int


def current_version_census_proof_identity(
    contract: CurrentVersionCensusContract,
) -> str:
    """Hash the complete admitted acquisition contract."""

    payload = json.dumps(
        contract.identity(),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def validated_current_version_census_total(payload: Any) -> int:
    """Return one exact FHIR Bundle total without bool coercion."""

    if not isinstance(payload, Mapping):
        raise ValueError("provider_directory_current_version_census_count_payload_invalid")
    if payload.get("resourceType") != "Bundle":
        raise ValueError("provider_directory_current_version_census_count_bundle_required")
    if payload.get("type") != "searchset":
        raise ValueError("provider_directory_current_version_census_count_searchset_required")
    total = payload.get("total")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        raise ValueError("provider_directory_current_version_census_count_total_invalid")
    return total


def current_version_census_initial_proof(
    contract: CurrentVersionCensusContract,
    resource_type: str,
    pre_count: int,
) -> dict[str, Any]:
    """Create the durable pre-census proof bound to the admitted contract."""

    if resource_type not in contract.resources:
        raise ValueError("provider_directory_current_version_census_resource_not_bound")
    if isinstance(pre_count, bool) or not isinstance(pre_count, int) or pre_count < 0:
        raise ValueError("provider_directory_current_version_census_pre_count_invalid")
    return {
        "strategy_version": contract.strategy_version,
        "contract_identity": current_version_census_proof_identity(contract),
        "cutoff": contract.cutoff,
        "resource_type": resource_type,
        "pre_count": pre_count,
        "verified": False,
    }


def current_version_census_persisted_pre_count(
    completeness: Mapping[str, Any],
    contract: CurrentVersionCensusContract,
    resource_type: str,
) -> int | None:
    """Load a compatible pre-count or reject a drifted checkpoint."""

    if not completeness:
        return None
    expected_by_field = {
        "strategy_version": contract.strategy_version,
        "contract_identity": current_version_census_proof_identity(contract),
        "cutoff": contract.cutoff,
        "resource_type": resource_type,
    }
    if any(
        completeness.get(name) != expected
        for name, expected in expected_by_field.items()
    ):
        raise ValueError("provider_directory_current_version_census_checkpoint_identity_mismatch")
    pre_count = completeness.get("pre_count")
    if isinstance(pre_count, bool) or not isinstance(pre_count, int) or pre_count < 0:
        raise ValueError("provider_directory_current_version_census_checkpoint_pre_count_invalid")
    return pre_count


def current_version_census_completed_proof(
    initial_proof: Mapping[str, Any],
    *,
    post_count: int,
    processed_rows: int,
    unique_candidate_rows: int,
) -> dict[str, Any]:
    """Require exact pre/page/post equality before marking a resource verified."""

    count_by_name = {
        "pre_count": initial_proof.get("pre_count"),
        "post_count": post_count,
        "processed_rows": processed_rows,
        "unique_candidate_rows": unique_candidate_rows,
    }
    if any(
        isinstance(count, bool) or not isinstance(count, int) or count < 0
        for count in count_by_name.values()
    ):
        raise ValueError("provider_directory_current_version_census_proof_count_invalid")
    pre_count = count_by_name["pre_count"]
    failure = None
    if post_count != pre_count:
        failure = "census_drift"
    elif processed_rows < pre_count:
        failure = "cursor_loss"
    elif processed_rows != unique_candidate_rows:
        failure = "duplicate_resource_ids"
    elif processed_rows != pre_count:
        failure = "processed_count_mismatch"
    completed_proof_by_field = {
        **dict(initial_proof),
        "post_count": post_count,
        "processed_rows": processed_rows,
        "unique_candidate_rows": unique_candidate_rows,
        "verified": failure is None,
    }
    if failure is not None:
        completed_proof_by_field["failure"] = failure
    return completed_proof_by_field


def validated_current_version_census_completed_proof(
    completeness: Mapping[str, Any],
    contract: CurrentVersionCensusContract,
    resource_type: str,
    *,
    rows_processed: int | None = None,
    pages_processed: int | None = None,
) -> dict[str, Any]:
    """Require a complete, identity-bound four-way census proof."""

    current_version_census_persisted_pre_count(
        completeness,
        contract,
        resource_type,
    )
    required_count_fields = (
        "pre_count",
        "post_count",
        "processed_rows",
        "unique_candidate_rows",
    )
    count_by_name = {
        field_name: completeness.get(field_name)
        for field_name in required_count_fields
    }
    has_invalid_count = any(
        isinstance(count, bool) or not isinstance(count, int) or count < 0
        for count in count_by_name.values()
    )
    has_equal_counts = len(set(count_by_name.values())) == 1
    completed_row_count = count_by_name["processed_rows"]
    has_valid_completed_row_count = (
        type(completed_row_count) is int and completed_row_count >= 0
    )
    has_invalid_page_state = bool(
        pages_processed is not None
        and (
            isinstance(pages_processed, bool)
            or not isinstance(pages_processed, int)
            or pages_processed <= 0
            or not has_valid_completed_row_count
            or (completed_row_count == 0 and pages_processed != 1)
            or (completed_row_count > 0 and pages_processed > completed_row_count)
        )
    )
    if (
        has_invalid_count
        or not has_equal_counts
        or has_invalid_page_state
        or completeness.get("verified") is not True
        or completeness.get("failure") is not None
        or (
            rows_processed is not None
            and count_by_name["processed_rows"] != rows_processed
        )
    ):
        raise ValueError(
            "provider_directory_current_version_census_completed_proof_invalid"
        )
    return dict(completeness)


def _effective_https_port(parsed_url: urllib.parse.SplitResult) -> int:
    try:
        return parsed_url.port or 443
    except ValueError:
        return 0


def _normalized_path(path: str) -> str | None:
    decoded_path = urllib.parse.unquote(path)
    if (
        "\\" in decoded_path
        or "//" in decoded_path
        or urllib.parse.unquote(decoded_path) != decoded_path
        or "%2f" in path.lower()
        or "%5c" in path.lower()
        or any(segment in {".", ".."} for segment in decoded_path.split("/"))
    ):
        return None
    return decoded_path.rstrip("/")


def _query_values(
    query_items: list[tuple[str, str]],
    name: str,
) -> list[str]:
    return [value for key, value in query_items if key.lower() == name]


def _smile_cursor_parts(
    parsed_url: urllib.parse.SplitResult,
    *,
    expected_page_count: int,
) -> tuple[str, int, list[tuple[str, str]]]:
    query_items = urllib.parse.parse_qsl(
        parsed_url.query,
        keep_blank_values=True,
        strict_parsing=True,
    )
    if any(name.lower() not in _SMILE_QUERY_NAMES for name, _value in query_items):
        raise ValueError("untrusted_current_version_census_pagination_link")
    token_values = _query_values(query_items, "_getpages")
    offset_values = _query_values(query_items, "_getpagesoffset")
    count_values = _query_values(query_items, "_count")
    pretty_values = _query_values(query_items, "_pretty")
    bundle_type_values = _query_values(query_items, "_bundletype")
    if (
        len(token_values) != 1
        or not token_values[0]
        or len(token_values[0]) > 512
        or any(character.isspace() for character in token_values[0])
        or len(offset_values) != 1
        or not offset_values[0].isdigit()
        or len(count_values) != 1
        or count_values[0] != str(expected_page_count)
        or (pretty_values and (len(pretty_values) != 1 or pretty_values[0].lower() not in {"true", "false"}))
        or (bundle_type_values and (len(bundle_type_values) != 1 or bundle_type_values[0].lower() != "searchset"))
    ):
        raise ValueError("untrusted_current_version_census_pagination_link")
    return token_values[0], int(offset_values[0]), query_items


def _validate_page_state(page_entry_count: int, expected_page_count: int) -> None:
    if (
        isinstance(page_entry_count, bool)
        or not isinstance(page_entry_count, int)
        or page_entry_count < 0
        or isinstance(expected_page_count, bool)
        or not isinstance(expected_page_count, int)
        or expected_page_count <= 0
    ):
        raise ValueError("provider_directory_current_version_census_page_state_invalid")


def _validate_resume_state(
    pages_processed: int,
    rows_processed: int,
) -> None:
    has_initial_state = pages_processed == 0 and rows_processed == 0
    has_page_progress = (
        pages_processed > 0
        and rows_processed > 0
        and pages_processed <= rows_processed
    )
    if (
        isinstance(pages_processed, bool)
        or not isinstance(pages_processed, int)
        or pages_processed < 0
        or isinstance(rows_processed, bool)
        or not isinstance(rows_processed, int)
        or rows_processed < 0
        or not (has_initial_state or has_page_progress)
    ):
        raise ValueError(
            "provider_directory_current_version_census_resume_state_invalid"
        )


def _validate_continuation_location(
    parsed_base: urllib.parse.SplitResult,
    parsed_next: urllib.parse.SplitResult,
) -> None:
    expected_origin = (
        (parsed_base.hostname or "").lower(),
        _effective_https_port(parsed_base),
    )
    next_origin = (
        (parsed_next.hostname or "").lower(),
        _effective_https_port(parsed_next),
    )
    allowed_paths = {
        _normalized_path(parsed_base.path),
        _normalized_path(parsed_base.path.rsplit("/", 1)[0]),
    }
    if (
        parsed_next.scheme.lower() != "https"
        or next_origin != expected_origin
        or parsed_next.username is not None
        or parsed_next.password is not None
        or bool(parsed_next.fragment)
        or _normalized_path(parsed_next.path) not in allowed_paths
    ):
        raise ValueError("untrusted_current_version_census_pagination_link")


def _validate_cursor_progress(
    parsed_current: urllib.parse.SplitResult,
    token: str,
    offset: int,
    page_entry_count: int,
) -> None:
    current_query_items = urllib.parse.parse_qsl(
        parsed_current.query,
        keep_blank_values=True,
    )
    current_tokens = _query_values(current_query_items, "_getpages")
    current_offsets = _query_values(current_query_items, "_getpagesoffset")
    if not current_tokens and not current_offsets:
        if offset != page_entry_count:
            raise ValueError("untrusted_current_version_census_pagination_link")
        return
    has_valid_progress = bool(
        len(current_tokens) == 1
        and len(current_offsets) == 1
        and current_offsets[0].isdigit()
        and token == current_tokens[0]
        and offset == int(current_offsets[0]) + page_entry_count
    )
    if not has_valid_progress:
        raise ValueError("untrusted_current_version_census_pagination_link")


def _cursor_identity(
    parsed_next: urllib.parse.SplitResult,
    query_items: list[tuple[str, str]],
) -> str:
    identity_payload = json.dumps(
        sorted((name.lower(), query_value) for name, query_value in query_items),
        separators=(",", ":"),
    )
    return hashlib.sha256(
        f"{_normalized_path(parsed_next.path)}\n{identity_payload}".encode("utf-8")
    ).hexdigest()


def validated_current_version_census_resume_url(
    contract: CurrentVersionCensusContract,
    resource_type: str,
    start_url: str,
    next_url: str | None,
    *,
    pages_processed: int,
    rows_processed: int,
    expected_page_count: int,
) -> str:
    """Validate a persisted next URL before any resumed transport."""

    _validate_page_state(rows_processed, expected_page_count)
    _validate_resume_state(pages_processed, rows_processed)
    if start_url != contract.start_url(resource_type, expected_page_count):
        raise ValueError(
            "provider_directory_current_version_census_resume_start_url_invalid"
        )
    if pages_processed == 0:
        if next_url != start_url:
            raise ValueError(
                "provider_directory_current_version_census_resume_url_invalid"
            )
        return start_url
    if (
        not next_url
        or next_url == start_url
        or next_url.startswith("//")
        or next_url.strip() != next_url
    ):
        raise ValueError(
            "provider_directory_current_version_census_resume_url_invalid"
        )
    parsed_base = urllib.parse.urlsplit(dict(contract.start_urls)[resource_type])
    parsed_next = urllib.parse.urlsplit(next_url)
    _validate_continuation_location(parsed_base, parsed_next)
    _token, offset, _query_items = _smile_cursor_parts(
        parsed_next,
        expected_page_count=expected_page_count,
    )
    if offset != rows_processed:
        raise ValueError(
            "provider_directory_current_version_census_resume_offset_invalid"
        )
    return next_url


def resolved_current_version_census_next_url(
    contract: CurrentVersionCensusContract,
    resource_type: str,
    current_url: str,
    next_link: str,
    *,
    page_entry_count: int,
    expected_page_count: int,
) -> CurrentVersionCensusContinuation:
    """Validate a Smile cursor while preserving the upstream URL bytes."""

    if contract.continuation_strategy != CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY:
        raise ValueError("provider_directory_current_version_census_continuation_unsupported")
    if (
        resource_type not in contract.resources
        or not next_link
        or next_link.startswith("//")
        or next_link.strip() != next_link
    ):
        raise ValueError("untrusted_current_version_census_pagination_link")
    _validate_page_state(page_entry_count, expected_page_count)
    if page_entry_count == 0:
        raise ValueError("untrusted_current_version_census_pagination_link")
    reviewed_url = dict(contract.start_urls)[resource_type]
    parsed_base = urllib.parse.urlsplit(reviewed_url)
    parsed_current = urllib.parse.urlsplit(current_url)
    next_url = urllib.parse.urljoin(current_url, next_link)
    parsed_next = urllib.parse.urlsplit(next_url)
    _validate_continuation_location(parsed_base, parsed_next)
    token, offset, query_items = _smile_cursor_parts(
        parsed_next,
        expected_page_count=expected_page_count,
    )
    _validate_cursor_progress(parsed_current, token, offset, page_entry_count)
    return CurrentVersionCensusContinuation(
        next_url,
        _cursor_identity(parsed_next, query_items),
        token,
        offset,
    )
