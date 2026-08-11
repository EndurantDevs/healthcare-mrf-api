# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact advertised-count profiles for reviewed FHIR subset traversal."""

from __future__ import annotations

from typing import Any, Mapping


SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION = (
    "provider-directory-fhir-server-issued-traversal-subset-v3"
)
SERVER_ISSUED_SUBSET_BOUNDED_STRATEGY_VERSION = (
    "provider-directory-fhir-server-issued-traversal-subset-v4"
)
SERVER_ISSUED_SUBSET_STRATEGY_VERSION = (
    "provider-directory-fhir-server-issued-traversal-subset-v5"
)
SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES = (
    "advertised-count-stability",
    "source-issued-continuation",
    "returned-resource-content",
)
SERVER_ISSUED_SUBSET_BOUNDED_COMPLETION_SCOPES = (
    "advertised-count-monotone-decrease-at-most-one",
    "source-issued-continuation",
    "returned-resource-content",
)
SERVER_ISSUED_SUBSET_COMPLETION_SCOPES = (
    "advertised-count-monotone-decrease-bounded-by-one-percent-and-twenty-pages",
    "terminal-logical-window-covers-advertised-pre",
    "source-issued-continuation",
    "returned-resource-content",
)
SERVER_ISSUED_SUBSET_BOUNDED_MAX_ADVERTISED_COUNT_DECREASE = 1
SERVER_ISSUED_SUBSET_MAX_ADVERTISED_COUNT_DECREASE_PAGES = 20
SERVER_ISSUED_SUBSET_MAX_ADVERTISED_COUNT_DECREASE_BASIS_POINTS = 100


def _reviewed_subset_profile_version(
    strategy_version: Any,
    completion_scopes: Any,
) -> int | None:
    profile = (strategy_version, completion_scopes)
    if profile == (
        SERVER_ISSUED_SUBSET_EXACT_STRATEGY_VERSION,
        SERVER_ISSUED_SUBSET_EXACT_COMPLETION_SCOPES,
    ):
        return 3
    if profile == (
        SERVER_ISSUED_SUBSET_BOUNDED_STRATEGY_VERSION,
        SERVER_ISSUED_SUBSET_BOUNDED_COMPLETION_SCOPES,
    ):
        return 4
    if profile == (
        SERVER_ISSUED_SUBSET_STRATEGY_VERSION,
        SERVER_ISSUED_SUBSET_COMPLETION_SCOPES,
    ):
        return 5
    return None


def is_reviewed_subset_profile(
    strategy_version: Any,
    completion_scopes: Any,
) -> bool:
    """Return whether a strategy and scope tuple is exactly allowlisted."""

    return _reviewed_subset_profile_version(
        strategy_version,
        completion_scopes,
    ) is not None


def reviewed_subset_max_advertised_count_decrease(
    strategy_version: Any,
    completion_scopes: Any,
) -> int | None:
    """Return the fixed bound for an exact or legacy bounded profile."""

    profile_version = _reviewed_subset_profile_version(
        strategy_version,
        completion_scopes,
    )
    if profile_version == 3:
        return 0
    if profile_version == 4:
        return SERVER_ISSUED_SUBSET_BOUNDED_MAX_ADVERTISED_COUNT_DECREASE
    return None


def reviewed_subset_advertised_count_decrease_limit(
    strategy_version: Any,
    completion_scopes: Any,
    *,
    pre_count: Any,
    page_count: Any,
) -> int | None:
    """Return the exact profile-bound decrease limit for one resource."""

    if (
        type(pre_count) is not int
        or pre_count < 0
        or type(page_count) is not int
        or not 1 <= page_count <= 1000
    ):
        return None
    profile_version = _reviewed_subset_profile_version(
        strategy_version,
        completion_scopes,
    )
    if profile_version == 3:
        return 0
    if profile_version == 4:
        return SERVER_ISSUED_SUBSET_BOUNDED_MAX_ADVERTISED_COUNT_DECREASE
    if profile_version == 5:
        percentage_limit = (
            pre_count
            * SERVER_ISSUED_SUBSET_MAX_ADVERTISED_COUNT_DECREASE_BASIS_POINTS
            + 9_999
        ) // 10_000
        page_limit = (
            page_count
            * SERVER_ISSUED_SUBSET_MAX_ADVERTISED_COUNT_DECREASE_PAGES
        )
        return min(page_limit, percentage_limit)
    return None


def reviewed_subset_decrease_limit(
    strategy_version: Any,
    completion_scopes: Any,
    *,
    pre_count: Any,
    page_count: Any,
    invalid_error: str = "provider_directory_reviewed_subset_profile_invalid",
) -> int:
    """Return one valid decrease limit or reject the profile inputs."""

    limit = reviewed_subset_advertised_count_decrease_limit(
        strategy_version,
        completion_scopes,
        pre_count=pre_count,
        page_count=page_count,
    )
    if limit is None:
        raise ValueError(invalid_error)
    return limit


def is_reviewed_subset_terminal_window_required(
    strategy_version: Any,
    completion_scopes: Any,
) -> bool:
    """Return whether the profile binds pre-count to its terminal window."""

    return _reviewed_subset_profile_version(
        strategy_version,
        completion_scopes,
    ) == 5


def is_advertised_pre_in_terminal_window(
    advertised_pre: Any,
    terminal_geometry: Any,
) -> bool:
    """Return whether one pre-count is bracketed by terminal offsets."""

    if type(advertised_pre) is not int or not isinstance(
        terminal_geometry,
        Mapping,
    ):
        return False
    terminal_start = terminal_geometry.get("terminal_page_start_offset")
    terminal_end = terminal_geometry.get("logical_window_end_offset")
    return bool(
        type(terminal_start) is int
        and type(terminal_end) is int
        and terminal_start <= advertised_pre <= terminal_end
    )
