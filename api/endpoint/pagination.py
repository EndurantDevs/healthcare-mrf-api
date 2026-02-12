# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from sanic.exceptions import InvalidUsage


@dataclass(frozen=True)
class PaginationParams:
    page: int
    limit: int
    offset: int
    source: str


def _parse_non_negative_int(raw: Any, param_name: str) -> Optional[int]:
    if raw in (None, "", "null"):
        return None
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise InvalidUsage(f"Parameter '{param_name}' must be an integer") from exc
    if value < 0:
        raise InvalidUsage(f"Parameter '{param_name}' must be non-negative")
    return value


def parse_pagination(
    args,
    *,
    default_limit: int,
    max_limit: int,
    default_page: int = 1,
    allow_offset: bool = True,
    allow_start: bool = True,
    allow_page_size: bool = True,
) -> PaginationParams:
    """Parse mixed pagination styles into a canonical window.

    Supported aliases:
    - canonical: page + limit
    - offset + limit
    - start + limit
    - page + page_size
    """

    if default_limit <= 0 or max_limit <= 0:
        raise ValueError("default_limit and max_limit must be positive")

    raw_limit = _parse_non_negative_int(args.get("limit"), "limit")
    raw_page_size = (
        _parse_non_negative_int(args.get("page_size"), "page_size")
        if allow_page_size
        else None
    )
    raw_page = _parse_non_negative_int(args.get("page"), "page")
    raw_offset = (
        _parse_non_negative_int(args.get("offset"), "offset") if allow_offset else None
    )
    raw_start = (
        _parse_non_negative_int(args.get("start"), "start") if allow_start else None
    )

    if raw_offset is not None and raw_start is not None and raw_offset != raw_start:
        raise InvalidUsage("Parameters 'offset' and 'start' must match when both are provided")

    if (
        raw_limit is not None
        and raw_page_size is not None
        and raw_limit != raw_page_size
    ):
        raise InvalidUsage("Parameters 'limit' and 'page_size' must match when both are provided")

    if raw_limit is None and raw_page_size is not None:
        raw_limit = raw_page_size

    limit = raw_limit if raw_limit is not None else default_limit
    if limit <= 0:
        limit = default_limit
    limit = max(1, min(limit, max_limit))

    if raw_page is None or raw_page == 0:
        page = default_page
    else:
        page = raw_page
    if page < 1:
        raise InvalidUsage("Parameter 'page' must be >= 1")

    explicit_offset = raw_offset if raw_offset is not None else raw_start
    if explicit_offset is not None:
        if raw_page is not None and raw_page > 0:
            expected_offset = (page - 1) * limit
            if expected_offset != explicit_offset:
                raise InvalidUsage(
                    "Parameters 'page' and 'offset/start' must refer to the same window"
                )
        offset = explicit_offset
        page = (offset // limit) + 1
        source = "offset" if raw_offset is not None else "start"
    else:
        offset = (page - 1) * limit
        source = "page"

    return PaginationParams(page=page, limit=limit, offset=offset, source=source)


def parse_bool_alias(args, primary: str, alias: str, *, default: bool) -> bool:
    def _coerce(raw: Any, name: str) -> Optional[bool]:
        if raw in (None, "", "null"):
            return None
        if isinstance(raw, bool):
            return raw
        lowered = str(raw).strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return True
        if lowered in {"false", "0", "no", "n", "off"}:
            return False
        raise InvalidUsage(f"Parameter '{name}' must be boolean-like")

    primary_value = _coerce(args.get(primary), primary)
    alias_value = _coerce(args.get(alias), alias)
    if (
        primary_value is not None
        and alias_value is not None
        and primary_value != alias_value
    ):
        raise InvalidUsage(
            f"Parameters '{primary}' and '{alias}' conflict and must have the same value"
        )
    if primary_value is not None:
        return primary_value
    if alias_value is not None:
        return alias_value
    return default
