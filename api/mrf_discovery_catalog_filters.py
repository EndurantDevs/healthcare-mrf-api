# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stored discovery-catalog identity filters."""

from __future__ import annotations

from typing import Any

from sqlalchemy import func, or_


def source_file_identity_scope_condition(
    file_table: Any,
    source_table: Any,
) -> Any:
    """Reject stored employer files that conflict with an exact source EIN."""

    source_raw_metadata = source_table.c.metadata_json["raw"]
    lookup_type = func.lower(
        func.coalesce(
            source_raw_metadata["query_context_lookup_type"].as_string(),
            "",
        )
    )
    requested_ein = func.regexp_replace(
        func.coalesce(
            source_raw_metadata["query_context_employer_ein"].as_string(),
            "",
        ),
        r"\D",
        "",
        "g",
    )
    matched_ein = func.regexp_replace(
        func.coalesce(
            file_table.c.metadata_json["anthem_matched_ein"].as_string(),
            file_table.c.metadata_json["ein"].as_string(),
            "",
        ),
        r"\D",
        "",
        "g",
    )
    return or_(
        lookup_type != "employer_ein",
        requested_ein == "",
        matched_ein == "",
        matched_ein == requested_ein,
    )
