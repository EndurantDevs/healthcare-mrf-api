# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict source-scoped continuation validation for FHIR formulary searches."""

from __future__ import annotations

import urllib.parse
from collections.abc import Mapping

from process.formulary_fhir.identity import canonical_fhir_base


MEDICATION_PAGE_COUNT = 100


class FHIRTransportError(RuntimeError):
    """Report a bounded FHIR transport or response contract failure."""

    def __init__(
        self,
        message: str,
        *,
        throttled: bool = False,
        retryable: bool = False,
        retry_after: float = 0.0,
    ) -> None:
        super().__init__(message)
        self.throttled = throttled
        self.retryable = retryable
        self.retry_after = retry_after


def _single_alias(alias: str) -> str:
    alias_value = str(alias or "").strip()
    if not alias_value:
        raise ValueError("DrugPlan alias is required")
    if "," in alias_value or "\x00" in alias_value or len(alias_value) > 512:
        raise ValueError("exactly one bounded DrugPlan alias is required")
    return alias_value


def _query_values_by_name(
    parsed: urllib.parse.SplitResult,
) -> dict[str, list[str]]:
    query_values_by_name: dict[str, list[str]] = {}
    for query_name, query_value in urllib.parse.parse_qsl(
        parsed.query,
        keep_blank_values=True,
    ):
        query_values_by_name.setdefault(query_name, []).append(query_value)
    return query_values_by_name


def _is_valid_smile_cursor_query(parsed: urllib.parse.SplitResult) -> bool:
    query_values_by_name = _query_values_by_name(parsed)
    allowed_names = {
        "_bundletype",
        "_count",
        "_getpages",
        "_getpagesoffset",
        "_pretty",
    }
    return bool(
        set(query_values_by_name).issubset(allowed_names)
        and all(
            len(query_values) == 1
            for query_values in query_values_by_name.values()
        )
        and {"_count", "_getpages", "_getpagesoffset"}.issubset(
            query_values_by_name
        )
        and query_values_by_name["_count"][0].isdigit()
        and 1
        <= int(query_values_by_name["_count"][0])
        <= MEDICATION_PAGE_COUNT
        and query_values_by_name["_getpages"][0]
        and len(query_values_by_name["_getpages"][0]) <= 512
        and query_values_by_name["_getpagesoffset"][0].isdigit()
        and (
            "_pretty" not in query_values_by_name
            or query_values_by_name["_pretty"][0].lower()
            in {"true", "false"}
        )
        and (
            "_bundletype" not in query_values_by_name
            or query_values_by_name["_bundletype"][0] == "searchset"
        )
    )


def _has_valid_alias_filter(
    query_values_by_name: Mapping[str, list[str]],
    resource_type: str,
    expected_alias: str | None,
) -> bool:
    if resource_type != "MedicationKnowledge":
        return "DrugPlan" not in query_values_by_name
    aliases = query_values_by_name.get("DrugPlan", [])
    return bool(
        len(aliases) == 1
        and _single_alias(aliases[0])
        and (expected_alias is None or aliases[0] == expected_alias)
    )


def _is_valid_collection_cursor(
    parsed: urllib.parse.SplitResult,
    *,
    current_path: str,
    resource_type: str,
    expected_alias: str | None,
) -> bool:
    query_values_by_name = _query_values_by_name(parsed)
    allowed_names = {
        "_after",
        "_count",
        "_elements",
        "_lastUpdated",
        "_offset",
        "_profile",
        "_sort",
    }
    if resource_type == "MedicationKnowledge":
        allowed_names.add("DrugPlan")
    page_token_count = int("_after" in query_values_by_name) + int(
        "_offset" in query_values_by_name
    )
    has_valid_page_token = bool(
        (
            "_after" in query_values_by_name
            and query_values_by_name["_after"][0]
        )
        or (
            "_offset" in query_values_by_name
            and query_values_by_name["_offset"][0].isdigit()
        )
    )
    return bool(
        resource_type in {"List", "MedicationKnowledge"}
        and parsed.path.rstrip("/") == current_path
        and set(query_values_by_name).issubset(allowed_names)
        and page_token_count == 1
        and has_valid_page_token
        and _has_valid_alias_filter(
            query_values_by_name,
            resource_type,
            expected_alias,
        )
        and len(query_values_by_name.get("_count", [])) == 1
        and query_values_by_name["_count"][0].isdigit()
        and 1
        <= int(query_values_by_name["_count"][0])
        <= MEDICATION_PAGE_COUNT
        and len(query_values_by_name.get("_lastUpdated", [])) <= 2
        and all(
            len(query_values) == 1
            for query_name, query_values in query_values_by_name.items()
            if query_name != "_lastUpdated"
        )
    )


def _is_trusted_endpoint(
    parsed: urllib.parse.SplitResult,
    base: urllib.parse.SplitResult,
) -> bool:
    return bool(
        parsed.scheme.lower() == "https"
        and parsed.hostname == base.hostname
        and parsed.port in (None, 443)
        and parsed.username is None
        and parsed.password is None
        and not parsed.fragment
    )


def validated_next_url(
    base_url: str,
    current_url: str,
    candidate: str,
    *,
    resource_type: str | None = None,
    expected_alias: str | None = None,
) -> str:
    """Validate and return one source-scoped FHIR continuation URL."""

    base = urllib.parse.urlsplit(canonical_fhir_base(base_url))
    current = urllib.parse.urlsplit(current_url)
    parsed = urllib.parse.urlsplit(candidate)
    base_path = base.path.rstrip("/")
    current_path = current.path.rstrip("/")
    allowed_paths = {base_path, current_path}
    expected_resource_type = resource_type or current_path.rsplit("/", 1)[-1]
    is_valid_next_cursor = bool(
        _is_valid_smile_cursor_query(parsed)
        or _is_valid_collection_cursor(
            parsed,
            current_path=current_path,
            resource_type=expected_resource_type,
            expected_alias=expected_alias,
        )
    )
    is_current_collection = current_path in {
        f"{base_path}/List",
        f"{base_path}/MedicationKnowledge",
    }
    is_current_smile_root = (
        current_path == base_path and _is_valid_smile_cursor_query(current)
    )
    if not (
        _is_trusted_endpoint(parsed, base)
        and _is_trusted_endpoint(current, base)
        and (is_current_collection or is_current_smile_root)
        and parsed.path.rstrip("/") in allowed_paths
        and is_valid_next_cursor
    ):
        raise FHIRTransportError("untrusted formulary continuation link")
    return candidate
