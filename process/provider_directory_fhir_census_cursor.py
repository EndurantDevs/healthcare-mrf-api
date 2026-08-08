# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-neutral Smile opaque-cursor validation for exact FHIR censuses."""

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
from process.provider_directory_fhir_census_page_geometry import (
    validate_current_version_census_checkpoint_geometry,
    validate_census_page_entries,
    validate_current_version_census_resume_state,
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


@dataclass(frozen=True)
class CurrentVersionCensusResumeState:
    """Bound checkpoint coordinates required to validate a resumed cursor."""

    pages_processed: int
    rows_processed: int
    expected_page_count: int
    pre_total: int
    proof_by_field: Mapping[str, Any]


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
    query_name: str,
) -> list[str]:
    return [
        query_value
        for item_name, query_value in query_items
        if item_name.lower() == query_name
    ]


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
    if any(
        query_name.lower() not in _SMILE_QUERY_NAMES
        for query_name, _query_value in query_items
    ):
        raise ValueError("untrusted_current_version_census_pagination_link")
    token_values = _query_values(query_items, "_getpages")
    offset_values = _query_values(query_items, "_getpagesoffset")
    count_values = _query_values(query_items, "_count")
    pretty_values = _query_values(query_items, "_pretty")
    bundle_type_values = _query_values(query_items, "_bundletype")
    has_invalid_cursor = bool(
        len(token_values) != 1
        or not token_values[0]
        or len(token_values[0]) > 512
        or any(character.isspace() for character in token_values[0])
        or len(offset_values) != 1
        or not offset_values[0].isdigit()
        or len(offset_values[0]) > 20
        or len(count_values) != 1
        or count_values[0] != str(expected_page_count)
        or (
            pretty_values
            and (
                len(pretty_values) != 1
                or pretty_values[0].lower() not in {"true", "false"}
            )
        )
        or (
            bundle_type_values
            and (
                len(bundle_type_values) != 1
                or bundle_type_values[0].lower() != "searchset"
            )
        )
    )
    if has_invalid_cursor:
        raise ValueError("untrusted_current_version_census_pagination_link")
    return token_values[0], int(offset_values[0]), query_items


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


def _stable_cursor_query(
    query_items: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    return sorted(
        (query_name.lower(), query_value)
        for query_name, query_value in query_items
        if query_name.lower() != "_getpagesoffset"
    )


def _validate_cursor_progress(
    parsed_current: urllib.parse.SplitResult,
    parsed_next: urllib.parse.SplitResult,
    next_query_items: list[tuple[str, str]],
    next_cursor_token: str,
    next_cursor_offset: int,
    expected_page_count: int,
) -> None:
    current_query_items = urllib.parse.parse_qsl(
        parsed_current.query,
        keep_blank_values=True,
    )
    current_tokens = _query_values(current_query_items, "_getpages")
    current_offsets = _query_values(current_query_items, "_getpagesoffset")
    if not current_tokens and not current_offsets:
        if next_cursor_offset != expected_page_count:
            raise ValueError("untrusted_current_version_census_pagination_link")
        return
    try:
        current_cursor_token, current_cursor_offset, strict_current_query = (
            _smile_cursor_parts(
                parsed_current,
                expected_page_count=expected_page_count,
            )
        )
    except ValueError as exc:
        raise ValueError(
            "untrusted_current_version_census_pagination_link"
        ) from exc
    has_valid_progress = bool(
        next_cursor_token == current_cursor_token
        and next_cursor_offset == current_cursor_offset + expected_page_count
        and _normalized_path(parsed_current.path)
        == _normalized_path(parsed_next.path)
        and _stable_cursor_query(strict_current_query)
        == _stable_cursor_query(next_query_items)
    )
    if not has_valid_progress:
        raise ValueError("untrusted_current_version_census_pagination_link")


def _cursor_identity(
    parsed_next: urllib.parse.SplitResult,
    query_items: list[tuple[str, str]],
) -> str:
    identity_payload = json.dumps(
        sorted(
            (query_name.lower(), query_value)
            for query_name, query_value in query_items
        ),
        separators=(",", ":"),
    )
    return hashlib.sha256(
        f"{_normalized_path(parsed_next.path)}\n{identity_payload}".encode("utf-8")
    ).hexdigest()


def _validate_resumed_continuation_url(
    contract: CurrentVersionCensusContract,
    resource_type: str,
    start_url: str,
    next_url: str | None,
    *,
    expected_page_count: int,
) -> int:
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
    _cursor_token, cursor_offset, _query_items = _smile_cursor_parts(
        parsed_next,
        expected_page_count=expected_page_count,
    )
    return cursor_offset


def validate_census_resume_url(
    contract: CurrentVersionCensusContract,
    resource_type: str,
    start_url: str,
    next_url: str | None,
    resume_state: CurrentVersionCensusResumeState,
) -> str:
    """Validate a persisted logical cursor before resumed transport."""

    validate_current_version_census_resume_state(
        resume_state.pages_processed,
        resume_state.rows_processed,
        resume_state.expected_page_count,
        resume_state.pre_total,
    )
    validate_current_version_census_checkpoint_geometry(
        resume_state.proof_by_field,
        pages_processed=resume_state.pages_processed,
        rows_processed=resume_state.rows_processed,
        expected_page_count=resume_state.expected_page_count,
    )
    if start_url != contract.start_url(
        resource_type,
        resume_state.expected_page_count,
    ):
        raise ValueError(
            "provider_directory_current_version_census_resume_start_url_invalid"
        )
    if resume_state.pages_processed == 0:
        if next_url != start_url:
            raise ValueError(
                "provider_directory_current_version_census_resume_url_invalid"
            )
        return start_url
    cursor_offset = _validate_resumed_continuation_url(
        contract,
        resource_type,
        start_url,
        next_url,
        expected_page_count=resume_state.expected_page_count,
    )
    if cursor_offset != (
        resume_state.pages_processed * resume_state.expected_page_count
    ):
        raise ValueError(
            "provider_directory_current_version_census_resume_offset_invalid"
        )
    assert next_url is not None
    return next_url


def resolved_current_version_census_next_url(
    contract: CurrentVersionCensusContract,
    resource_type: str,
    current_url: str,
    next_link: str,
    *,
    page_entry_count: int,
    expected_page_count: int,
    pre_total: int,
) -> CurrentVersionCensusContinuation:
    """Validate one bounded cursor while preserving upstream URL bytes."""

    if (
        contract.continuation_strategy
        != CURRENT_VERSION_CENSUS_SMILE_CONTINUATION_STRATEGY
    ):
        raise ValueError(
            "provider_directory_current_version_census_continuation_unsupported"
        )
    if (
        resource_type not in contract.resources
        or not next_link
        or next_link.startswith("//")
        or next_link.strip() != next_link
    ):
        raise ValueError("untrusted_current_version_census_pagination_link")
    validate_census_page_entries(
        page_entry_count,
        expected_page_count,
    )
    if isinstance(pre_total, bool) or not isinstance(pre_total, int) or pre_total < 0:
        raise ValueError(
            "provider_directory_current_version_census_pre_count_invalid"
        )
    reviewed_url = dict(contract.start_urls)[resource_type]
    parsed_base = urllib.parse.urlsplit(reviewed_url)
    parsed_current = urllib.parse.urlsplit(current_url)
    next_url = urllib.parse.urljoin(current_url, next_link)
    parsed_next = urllib.parse.urlsplit(next_url)
    _validate_continuation_location(parsed_base, parsed_next)
    cursor_token, cursor_offset, query_items = _smile_cursor_parts(
        parsed_next,
        expected_page_count=expected_page_count,
    )
    _validate_cursor_progress(
        parsed_current,
        parsed_next,
        query_items,
        cursor_token,
        cursor_offset,
        expected_page_count,
    )
    if cursor_offset >= pre_total:
        raise ValueError("untrusted_current_version_census_pagination_link")
    return CurrentVersionCensusContinuation(
        next_url,
        _cursor_identity(parsed_next, query_items),
        cursor_token,
        cursor_offset,
    )
