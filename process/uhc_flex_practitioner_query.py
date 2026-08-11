# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure exact-query and response validation for Flex Practitioners."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import re
from typing import Any
import urllib.parse

from process.uhc_flex_official_cohort_contract import canonical_uhc_flex_npi
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_API_BASE,
    UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
    UHC_FLEX_PRACTITIONER_QUERY_COUNT,
    UHC_FLEX_PRACTITIONER_SEARCH_PARAMETER,
)
from process.uhc_flex_official_cohort_contract import (
    UHC_FLEX_OFFICIAL_NPI_SYSTEM,
    UHC_FLEX_OFFICIAL_RESOURCE_TYPE,
)


UHC_FLEX_PRACTITIONER_MATCHED = "matched"
UHC_FLEX_PRACTITIONER_UNMATCHED = "unmatched"
UHC_FLEX_PRACTITIONER_MAX_RESOURCE_JSON_BYTES = 1 << 20

_FHIR_ID_PATTERN = re.compile(r"[A-Za-z0-9\-.]{1,64}\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_RETRYABLE_HTTP_STATUSES = frozenset({408, 423, 425, 429})
_RETRY_CATEGORIES = frozenset({"invalid", "retryable", "success", "terminal"})


class UHCFlexPractitionerQueryError(ValueError):
    """Expose a safe code without embedding response data or an NPI."""

    def __init__(self, code: str = "payload_invalid") -> None:
        message_by_code = {
            "cross_npi": "Flex Practitioner result belongs to another NPI",
            "duplicate_resource_conflict": (
                "Flex Practitioner result has conflicting duplicate resources"
            ),
            "entry_invalid": "Flex Practitioner Bundle entry is invalid",
            "next_link_forbidden": (
                "Flex Practitioner exact query returned a next link"
            ),
            "operation_outcome": (
                "Flex Practitioner exact query returned an OperationOutcome"
            ),
            "payload_invalid": "Flex Practitioner response is invalid",
            "practitioner_required": (
                "Flex Practitioner exact query returned another resource type"
            ),
            "requested_npi_invalid": "Flex Practitioner requested NPI is invalid",
            "requested_npi_missing": (
                "Flex Practitioner result lacks the requested NPI identifier"
            ),
            "resource_id_invalid": "Flex Practitioner resource ID is invalid",
            "resource_npi_invalid": (
                "Flex Practitioner resource NPI identifier is invalid"
            ),
            "result_cap_exceeded": (
                "Flex Practitioner exact query exceeded its fixed result cap"
            ),
            "result_invalid": "Flex Practitioner query result is invalid",
            "searchset_required": (
                "Flex Practitioner response is not a searchset Bundle"
            ),
            "total_invalid": "Flex Practitioner Bundle total is invalid",
            "total_mismatch": (
                "Flex Practitioner Bundle total does not match its entries"
            ),
        }
        self.code = code if code in message_by_code else "payload_invalid"
        super().__init__(message_by_code[self.code])


@dataclass(frozen=True, slots=True)
class UHCFlexPractitionerRetryDecision:
    """Classify a failure without retaining exception or response text."""

    category: str
    reason_code: str

    def __post_init__(self) -> None:
        if (
            self.category not in _RETRY_CATEGORIES
            or type(self.reason_code) is not str
            or not self.reason_code
        ):
            raise ValueError("Flex Practitioner retry decision is invalid")

    @property
    def is_retryable(self) -> bool:
        """Return whether an operator may retry the identical request."""

        return self.category == "retryable"


def _canonical_requested_npi(requested_npi: object) -> int:
    try:
        return canonical_uhc_flex_npi(requested_npi)
    except ValueError:
        raise UHCFlexPractitionerQueryError("requested_npi_invalid") from None


def uhc_flex_practitioner_query_url(requested_npi: object) -> str:
    """Build the only admitted request: one system-qualified NPI token."""

    canonical_npi = _canonical_requested_npi(requested_npi)
    query_pairs = (
        (
            UHC_FLEX_PRACTITIONER_SEARCH_PARAMETER,
            f"{UHC_FLEX_OFFICIAL_NPI_SYSTEM}|{canonical_npi}",
        ),
        ("_count", str(UHC_FLEX_PRACTITIONER_QUERY_COUNT)),
    )
    query_text = urllib.parse.urlencode(query_pairs)
    return (
        f"{UHC_FLEX_PRACTITIONER_API_BASE}/"
        f"{UHC_FLEX_OFFICIAL_RESOURCE_TYPE}?{query_text}"
    )


def classify_uhc_flex_practitioner_http_status(
    http_status: object,
) -> UHCFlexPractitionerRetryDecision:
    """Classify one status; only unchanged transient requests may be retried."""

    if type(http_status) is not int or not 100 <= http_status <= 599:
        return UHCFlexPractitionerRetryDecision("invalid", "http_status_invalid")
    if http_status == 200:
        return UHCFlexPractitionerRetryDecision("success", "http_200")
    if http_status in _RETRYABLE_HTTP_STATUSES or 500 <= http_status <= 599:
        return UHCFlexPractitionerRetryDecision(
            "retryable",
            "http_transient",
        )
    return UHCFlexPractitionerRetryDecision("terminal", "http_terminal")


def classify_uhc_flex_practitioner_exception(
    error: object,
) -> UHCFlexPractitionerRetryDecision:
    """Retry only built-in timeout and connection failure families."""

    if isinstance(error, (TimeoutError, ConnectionError)):
        return UHCFlexPractitionerRetryDecision(
            "retryable",
            "transport_transient",
        )
    if isinstance(error, UHCFlexPractitionerQueryError):
        return UHCFlexPractitionerRetryDecision(
            "terminal",
            "response_validation",
        )
    if isinstance(error, BaseException):
        return UHCFlexPractitionerRetryDecision(
            "terminal",
            "exception_terminal",
        )
    return UHCFlexPractitionerRetryDecision("invalid", "exception_invalid")


def _canonical_resource_json(resource_by_field: dict[str, Any]) -> str:
    try:
        canonical_json = json.dumps(
            resource_by_field,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        if (
            len(canonical_json.encode("utf-8"))
            > UHC_FLEX_PRACTITIONER_MAX_RESOURCE_JSON_BYTES
        ):
            raise ValueError
    except (
        MemoryError,
        OverflowError,
        RecursionError,
        TypeError,
        UnicodeError,
        ValueError,
    ):
        raise UHCFlexPractitionerQueryError("payload_invalid") from None
    return canonical_json


def _resource_id(resource_by_field: dict[str, Any]) -> str:
    resource_id = resource_by_field.get("id")
    if type(resource_id) is not str or _FHIR_ID_PATTERN.fullmatch(resource_id) is None:
        raise UHCFlexPractitionerQueryError("resource_id_invalid")
    return resource_id


def _exact_resource_npis(resource_by_field: dict[str, Any]) -> tuple[int, ...]:
    identifiers = resource_by_field.get("identifier")
    if type(identifiers) is not list:
        raise UHCFlexPractitionerQueryError("requested_npi_missing")
    exact_npis: list[int] = []
    for identifier_by_field in identifiers:
        if type(identifier_by_field) is not dict:
            raise UHCFlexPractitionerQueryError("payload_invalid")
        if identifier_by_field.get("system") != UHC_FLEX_OFFICIAL_NPI_SYSTEM:
            continue
        raw_npi = identifier_by_field.get("value")
        if type(raw_npi) is not str or not re.fullmatch(r"[0-9]{10}", raw_npi):
            raise UHCFlexPractitionerQueryError("resource_npi_invalid")
        try:
            exact_npis.append(canonical_uhc_flex_npi(int(raw_npi)))
        except ValueError:
            raise UHCFlexPractitionerQueryError("resource_npi_invalid") from None
    return tuple(exact_npis)


def _validate_resource_npi(
    resource_by_field: dict[str, Any],
    requested_npi: int,
) -> None:
    exact_npis = _exact_resource_npis(resource_by_field)
    if not exact_npis:
        raise UHCFlexPractitionerQueryError("requested_npi_missing")
    if any(resource_npi != requested_npi for resource_npi in exact_npis):
        raise UHCFlexPractitionerQueryError("cross_npi")


def _bundle_entries(bundle_by_field: dict[str, Any]) -> list[dict[str, Any]]:
    if "entry" not in bundle_by_field:
        return []
    raw_entries = bundle_by_field["entry"]
    if type(raw_entries) is not list:
        raise UHCFlexPractitionerQueryError("entry_invalid")
    if len(raw_entries) > UHC_FLEX_PRACTITIONER_QUERY_COUNT:
        raise UHCFlexPractitionerQueryError("result_cap_exceeded")
    if any(type(entry_by_field) is not dict for entry_by_field in raw_entries):
        raise UHCFlexPractitionerQueryError("entry_invalid")
    return raw_entries


def _reject_next_link(bundle_by_field: dict[str, Any]) -> None:
    if "link" not in bundle_by_field:
        return
    raw_links = bundle_by_field["link"]
    if type(raw_links) is not list:
        raise UHCFlexPractitionerQueryError("payload_invalid")
    for link_by_field in raw_links:
        if type(link_by_field) is not dict:
            raise UHCFlexPractitionerQueryError("payload_invalid")
        relation = link_by_field.get("relation")
        if type(relation) is not str or relation != relation.strip():
            raise UHCFlexPractitionerQueryError("payload_invalid")
        if relation.lower() == "next":
            raise UHCFlexPractitionerQueryError("next_link_forbidden")


def _validate_bundle_total(
    bundle_by_field: dict[str, Any],
    returned_entry_count: int,
) -> None:
    if "total" not in bundle_by_field:
        return
    total = bundle_by_field["total"]
    if type(total) is not int or total < 0:
        raise UHCFlexPractitionerQueryError("total_invalid")
    if total > UHC_FLEX_PRACTITIONER_QUERY_COUNT:
        raise UHCFlexPractitionerQueryError("result_cap_exceeded")
    if total != returned_entry_count:
        raise UHCFlexPractitionerQueryError("total_mismatch")


def _entry_practitioner(entry_by_field: dict[str, Any]) -> dict[str, Any]:
    resource_by_field = entry_by_field.get("resource")
    if type(resource_by_field) is not dict:
        raise UHCFlexPractitionerQueryError("entry_invalid")
    resource_type = resource_by_field.get("resourceType")
    if resource_type == "OperationOutcome":
        raise UHCFlexPractitionerQueryError("operation_outcome")
    if resource_type != UHC_FLEX_OFFICIAL_RESOURCE_TYPE:
        raise UHCFlexPractitionerQueryError("practitioner_required")
    return resource_by_field


def _validated_resource_json_rows(
    entries: list[dict[str, Any]],
    requested_npi: int,
) -> tuple[tuple[str, str], ...]:
    canonical_json_by_id: dict[str, str] = {}
    admitted_resource_ids: set[str] = set()
    ambiguous_resource_count = 0
    for entry_by_field in entries:
        resource_by_field = _entry_practitioner(entry_by_field)
        exact_npis = _exact_resource_npis(resource_by_field)
        if not exact_npis:
            raise UHCFlexPractitionerQueryError("requested_npi_missing")
        if requested_npi not in exact_npis:
            raise UHCFlexPractitionerQueryError("cross_npi")
        resource_id = _resource_id(resource_by_field)
        canonical_json = _canonical_resource_json(resource_by_field)
        previous_json = canonical_json_by_id.get(resource_id)
        if previous_json is not None and previous_json != canonical_json:
            raise UHCFlexPractitionerQueryError("duplicate_resource_conflict")
        canonical_json_by_id[resource_id] = canonical_json
        if any(resource_npi != requested_npi for resource_npi in exact_npis):
            ambiguous_resource_count += 1
        else:
            admitted_resource_ids.add(resource_id)
    if not admitted_resource_ids and ambiguous_resource_count:
        raise UHCFlexPractitionerQueryError("cross_npi")
    return tuple((key, canonical_json_by_id[key]) for key in sorted(admitted_resource_ids))


def _query_result_sha256(
    requested_npi: int,
    outcome: str,
    resource_json_rows: tuple[tuple[str, str], ...],
) -> str:
    resource_hashes = [
        {
            "resource_id": resource_id,
            "sha256": hashlib.sha256(resource_json.encode("utf-8")).hexdigest(),
        }
        for resource_id, resource_json in resource_json_rows
    ]
    result_identity_by_field = {
        "contract_id": UHC_FLEX_PRACTITIONER_QUERY_CONTRACT_ID,
        "outcome": outcome,
        "requested_npi": requested_npi,
        "resources": resource_hashes,
    }
    canonical_identity = json.dumps(
        result_identity_by_field,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical_identity.encode("utf-8")).hexdigest()


def _validate_stored_result_resources(
    requested_npi: int,
    resource_json_rows: object,
) -> tuple[tuple[str, str], ...]:
    if type(resource_json_rows) is not tuple:
        raise UHCFlexPractitionerQueryError("result_invalid")
    validated_rows: list[tuple[str, str]] = []
    for resource_row in resource_json_rows:
        if (
            type(resource_row) is not tuple
            or len(resource_row) != 2
            or type(resource_row[0]) is not str
            or type(resource_row[1]) is not str
        ):
            raise UHCFlexPractitionerQueryError("result_invalid")
        resource_id, resource_json = resource_row
        try:
            resource_by_field = json.loads(resource_json)
        except (MemoryError, RecursionError, UnicodeError, ValueError):
            raise UHCFlexPractitionerQueryError("result_invalid") from None
        if type(resource_by_field) is not dict:
            raise UHCFlexPractitionerQueryError("result_invalid")
        if _resource_id(resource_by_field) != resource_id:
            raise UHCFlexPractitionerQueryError("result_invalid")
        _validate_resource_npi(resource_by_field, requested_npi)
        if _canonical_resource_json(resource_by_field) != resource_json:
            raise UHCFlexPractitionerQueryError("result_invalid")
        validated_rows.append((resource_id, resource_json))
    if tuple(sorted(set(validated_rows))) != tuple(validated_rows):
        raise UHCFlexPractitionerQueryError("result_invalid")
    return tuple(validated_rows)


@dataclass(frozen=True, slots=True, repr=False)
class UHCFlexPractitionerQueryResult:
    """Hold a deterministic matched or explicit unmatched response result."""

    requested_npi: int
    outcome: str
    result_sha256: str
    _resource_json_rows: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        canonical_npi = _canonical_requested_npi(self.requested_npi)
        resource_rows = _validate_stored_result_resources(
            canonical_npi,
            self._resource_json_rows,
        )
        expected_outcome = (
            UHC_FLEX_PRACTITIONER_MATCHED
            if resource_rows
            else UHC_FLEX_PRACTITIONER_UNMATCHED
        )
        expected_hash = _query_result_sha256(
            canonical_npi,
            expected_outcome,
            resource_rows,
        )
        if (
            self.outcome != expected_outcome
            or type(self.result_sha256) is not str
            or _SHA256_PATTERN.fullmatch(self.result_sha256) is None
            or self.result_sha256 != expected_hash
        ):
            raise UHCFlexPractitionerQueryError("result_invalid")

    @property
    def is_unmatched(self) -> bool:
        """Return whether the exact query produced no Practitioner match."""

        return self.outcome == UHC_FLEX_PRACTITIONER_UNMATCHED

    @property
    def resource_count(self) -> int:
        """Return the deduplicated Practitioner resource count."""

        return len(self._resource_json_rows)

    @property
    def resource_ids(self) -> tuple[str, ...]:
        """Return deterministic resource IDs in ascending order."""

        return tuple(resource_id for resource_id, _ in self._resource_json_rows)

    @property
    def resource_sha256_by_id(self) -> tuple[tuple[str, str], ...]:
        """Return exact response-resource hashes keyed by resource ID."""

        return tuple(
            (
                resource_id,
                hashlib.sha256(resource_json.encode("utf-8")).hexdigest(),
            )
            for resource_id, resource_json in self._resource_json_rows
        )

    def resource_payloads(self) -> tuple[dict[str, Any], ...]:
        """Return fresh parser-ready payloads without exposing stored state."""

        return tuple(
            json.loads(resource_json)
            for _resource_id_value, resource_json in self._resource_json_rows
        )


def validate_uhc_flex_practitioner_search_bundle(
    requested_npi: object,
    response_payload: object,
) -> UHCFlexPractitionerQueryResult:
    """Validate one bounded exact-NPI search response without broad claims."""

    canonical_npi = _canonical_requested_npi(requested_npi)
    if type(response_payload) is not dict:
        raise UHCFlexPractitionerQueryError("payload_invalid")
    resource_type = response_payload.get("resourceType")
    if resource_type == "OperationOutcome":
        raise UHCFlexPractitionerQueryError("operation_outcome")
    if resource_type != "Bundle" or response_payload.get("type") != "searchset":
        raise UHCFlexPractitionerQueryError("searchset_required")
    _reject_next_link(response_payload)
    entries = _bundle_entries(response_payload)
    _validate_bundle_total(response_payload, len(entries))
    resource_json_rows = _validated_resource_json_rows(
        entries,
        canonical_npi,
    )
    outcome = (
        UHC_FLEX_PRACTITIONER_MATCHED
        if resource_json_rows
        else UHC_FLEX_PRACTITIONER_UNMATCHED
    )
    return UHCFlexPractitionerQueryResult(
        requested_npi=canonical_npi,
        outcome=outcome,
        result_sha256=_query_result_sha256(
            canonical_npi,
            outcome,
            resource_json_rows,
        ),
        _resource_json_rows=resource_json_rows,
    )


__all__ = (
    "UHCFlexPractitionerQueryError",
    "UHCFlexPractitionerQueryResult",
    "UHCFlexPractitionerRetryDecision",
    "UHC_FLEX_PRACTITIONER_MATCHED",
    "UHC_FLEX_PRACTITIONER_MAX_RESOURCE_JSON_BYTES",
    "UHC_FLEX_PRACTITIONER_UNMATCHED",
    "classify_uhc_flex_practitioner_exception",
    "classify_uhc_flex_practitioner_http_status",
    "uhc_flex_practitioner_query_url",
    "validate_uhc_flex_practitioner_search_bundle",
)
