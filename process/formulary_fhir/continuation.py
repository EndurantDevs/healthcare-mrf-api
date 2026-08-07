# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Search-bound, source-scoped FHIR continuation contracts."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
import urllib.parse
from dataclasses import dataclass, field

from process.formulary_fhir.identity import canonical_fhir_base
from process.formulary_fhir.types import (
    COVERAGE_PLAN_PROFILE_URI,
    FORMULARY_DRUG_PROFILE_URI,
    FormularySourceConfig,
)


COVERAGE_PLAN_ELEMENTS = "id,meta,status,title,name,date,identifier,extension"
FORMULARY_DRUG_ELEMENTS = "id,meta,status,code,extension"
FHIR_RESOURCE_TYPES = frozenset({"List", "MedicationKnowledge"})
ALIAS_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,511}\Z")
MAX_CONTINUATION_URL_BYTES = 4_096
MAX_CURSOR_TOKEN_BYTES = 512


class FHIRTransportError(RuntimeError):
    """Report a bounded FHIR transport or search-contract failure."""

    def __init__(
        self,
        message: str,
        *,
        is_transient: bool = False,
        retry_after_seconds: float = 0.0,
    ) -> None:
        super().__init__(message)
        self.is_transient = is_transient
        self.retry_after_seconds = retry_after_seconds


def canonical_cutoff(cutoff: object) -> tuple[dt.datetime, str]:
    """Return one timezone-aware cutoff as UTC and canonical FHIR text."""

    if type(cutoff) is not dt.datetime or cutoff.tzinfo is None:
        raise ValueError("FHIR census cutoff must be timezone-aware")
    try:
        utc_cutoff = cutoff.astimezone(dt.UTC)
    except (OverflowError, ValueError):
        raise ValueError("FHIR census cutoff is invalid") from None
    canonical_text = utc_cutoff.isoformat().replace("+00:00", "Z")
    return utc_cutoff, canonical_text


def validated_alias(alias: object) -> str:
    """Require one bounded exact DrugPlan alias without echoing its value."""

    if type(alias) is not str or not ALIAS_PATTERN.fullmatch(alias):
        raise ValueError("FHIR formulary alias is invalid")
    return alias


@dataclass(frozen=True, slots=True, repr=False)
class FHIRSearchContract:
    """Immutable current-version search identity for one bounded traversal."""

    canonical_base: str = field(repr=False)
    resource_type: str
    cutoff_at: dt.datetime
    cutoff_text: str = field(repr=False)
    page_size: int
    max_pages: int
    max_total_resources: int
    profile_uri: str
    element_projection: str
    alias: str | None = field(default=None, repr=False)
    contract_hash: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        contract_fields_by_name = {
            "canonical_base": self.canonical_base,
            "resource_type": self.resource_type,
            "cutoff": self.cutoff_text,
            "page_size": self.page_size,
            "max_pages": self.max_pages,
            "max_total_resources": self.max_total_resources,
            "profile_uri": self.profile_uri,
            "element_projection": self.element_projection,
            "alias": self.alias,
        }
        canonical_json = json.dumps(
            contract_fields_by_name,
            sort_keys=True,
            separators=(",", ":"),
        )
        object.__setattr__(
            self,
            "contract_hash",
            hashlib.sha256(canonical_json.encode("utf-8")).hexdigest(),
        )

    def __repr__(self) -> str:
        return (
            "FHIRSearchContract("
            f"resource_type={self.resource_type!r}, "
            f"contract={self.contract_hash[:12]!r})"
        )


def coverage_plan_search_contract(
    config: FormularySourceConfig,
    cutoff: object,
) -> FHIRSearchContract:
    """Build the exact approved CoveragePlan current-version search."""

    cutoff_at, cutoff_text = canonical_cutoff(cutoff)
    return FHIRSearchContract(
        canonical_base=canonical_fhir_base(config.canonical_base),
        resource_type="List",
        cutoff_at=cutoff_at,
        cutoff_text=cutoff_text,
        page_size=config.page_size,
        max_pages=config.max_pages,
        max_total_resources=config.max_total_resources,
        profile_uri=COVERAGE_PLAN_PROFILE_URI,
        element_projection=COVERAGE_PLAN_ELEMENTS,
    )


def medication_search_contract(
    config: FormularySourceConfig,
    alias: object,
    cutoff: object,
) -> FHIRSearchContract:
    """Build the exact approved single-alias current-version search."""

    cutoff_at, cutoff_text = canonical_cutoff(cutoff)
    return FHIRSearchContract(
        canonical_base=canonical_fhir_base(config.canonical_base),
        resource_type="MedicationKnowledge",
        cutoff_at=cutoff_at,
        cutoff_text=cutoff_text,
        page_size=config.page_size,
        max_pages=config.max_pages,
        max_total_resources=config.max_total_resources,
        profile_uri=FORMULARY_DRUG_PROFILE_URI,
        element_projection=FORMULARY_DRUG_ELEMENTS,
        alias=validated_alias(alias),
    )


def collection_url(contract: FHIRSearchContract) -> str:
    """Return the sole approved collection URL for a search contract."""

    return f"{contract.canonical_base}/{contract.resource_type}"


def _common_query_pairs(contract: FHIRSearchContract) -> list[tuple[str, str]]:
    query_pairs: list[tuple[str, str]] = []
    if contract.alias is not None:
        query_pairs.append(("DrugPlan", contract.alias))
    query_pairs.extend(
        (
            ("_lastUpdated", f"lt{contract.cutoff_text}"),
            ("_profile", contract.profile_uri),
            ("_total", "accurate"),
        )
    )
    return query_pairs


def count_query_pairs(contract: FHIRSearchContract) -> tuple[tuple[str, str], ...]:
    """Return the exact accurate-count query for one search contract."""

    query_pairs = _common_query_pairs(contract)
    query_pairs.append(("_summary", "count"))
    return tuple(query_pairs)


def page_query_pairs(contract: FHIRSearchContract) -> tuple[tuple[str, str], ...]:
    """Return the exact projected page query for one search contract."""

    query_pairs = _common_query_pairs(contract)
    query_pairs.extend(
        (
            ("_count", str(contract.page_size)),
            ("_elements", contract.element_projection),
        )
    )
    return tuple(query_pairs)


@dataclass(frozen=True, slots=True, repr=False)
class FHIRContinuation:
    """One in-memory next request bound to a redacted search contract."""

    _request_url: str = field(repr=False)
    resource_type: str
    search_contract_hash: str = field(repr=False)
    url_fingerprint: str = field(repr=False)

    @property
    def request_url(self) -> str:
        """Return the private request URL only to the bounded client."""

        return self._request_url

    def __repr__(self) -> str:
        return (
            "FHIRContinuation("
            f"resource_type={self.resource_type!r}, location=<redacted>, "
            f"contract={self.search_contract_hash[:12]!r})"
        )


def _parsed_candidate(candidate: object, contract: FHIRSearchContract):
    if (
        type(candidate) is not str
        or not candidate
        or len(candidate.encode("utf-8")) > MAX_CONTINUATION_URL_BYTES
        or candidate != candidate.strip()
        or any(not character.isprintable() for character in candidate)
        or "\\" in candidate
    ):
        raise FHIRTransportError("FHIR continuation is invalid")
    try:
        parsed_candidate = urllib.parse.urlsplit(candidate)
        parsed_port = parsed_candidate.port
    except (UnicodeError, ValueError):
        raise FHIRTransportError("FHIR continuation is invalid") from None
    parsed_base = urllib.parse.urlsplit(contract.canonical_base)
    is_same_origin = bool(
        candidate.startswith("https://")
        and parsed_candidate.scheme == "https"
        and parsed_candidate.netloc == parsed_base.netloc
        and parsed_candidate.hostname == parsed_base.hostname
        and parsed_port is None
        and parsed_candidate.username is None
        and parsed_candidate.password is None
        and not parsed_candidate.fragment
        and "%" not in parsed_candidate.path
    )
    if not is_same_origin:
        raise FHIRTransportError("FHIR continuation origin is invalid")
    return parsed_candidate, parsed_base


def _query_pairs(parsed_candidate) -> tuple[tuple[str, str], ...]:
    try:
        query_pairs = urllib.parse.parse_qsl(
            parsed_candidate.query,
            keep_blank_values=True,
            strict_parsing=True,
            max_num_fields=12,
        )
    except (UnicodeError, ValueError):
        raise FHIRTransportError("FHIR continuation query is invalid") from None
    if not query_pairs or len({name for name, _text in query_pairs}) != len(
        query_pairs
    ):
        raise FHIRTransportError("FHIR continuation query is invalid")
    return tuple(query_pairs)


def _is_valid_cursor_token(token_text: str) -> bool:
    return bool(
        token_text
        and len(token_text.encode("utf-8")) <= MAX_CURSOR_TOKEN_BYTES
        and token_text == token_text.strip()
        and all(character.isprintable() for character in token_text)
        and not any(character.isspace() for character in token_text)
    )


def _validate_collection_query(
    query_pairs: tuple[tuple[str, str], ...],
    contract: FHIRSearchContract,
) -> None:
    query_by_name = dict(query_pairs)
    expected_by_name = dict(page_query_pairs(contract))
    page_names = {"_after", "_offset"} & set(query_by_name)
    if (
        len(page_names) != 1
        or set(query_by_name) != set(expected_by_name) | page_names
        or any(
            query_by_name[field_name] != expected_text
            for field_name, expected_text in expected_by_name.items()
        )
    ):
        raise FHIRTransportError("FHIR continuation search contract changed")
    page_name = next(iter(page_names))
    page_text = query_by_name[page_name]
    if page_name == "_after" and not _is_valid_cursor_token(page_text):
        raise FHIRTransportError("FHIR continuation page token is invalid")
    if page_name == "_offset" and not _is_valid_offset(page_text, contract):
        raise FHIRTransportError("FHIR continuation page offset is invalid")


def _is_valid_offset(offset_text: str, contract: FHIRSearchContract) -> bool:
    return bool(
        offset_text.isdigit()
        and (offset_text == "0" or not offset_text.startswith("0"))
        and 0 < int(offset_text) <= contract.max_total_resources
    )


def _validate_smile_query(
    query_pairs: tuple[tuple[str, str], ...],
    contract: FHIRSearchContract,
) -> None:
    query_by_name = dict(query_pairs)
    required_names = {"_count", "_getpages", "_getpagesoffset"}
    allowed_names = required_names | {"_bundletype", "_pretty"}
    is_valid = bool(
        required_names.issubset(query_by_name)
        and set(query_by_name).issubset(allowed_names)
        and query_by_name["_count"] == str(contract.page_size)
        and _is_valid_cursor_token(query_by_name["_getpages"])
        and _is_valid_offset(query_by_name["_getpagesoffset"], contract)
        and query_by_name.get("_bundletype", "searchset") == "searchset"
        and query_by_name.get("_pretty", "false") in {"true", "false"}
    )
    if not is_valid:
        raise FHIRTransportError("FHIR continuation cursor contract is invalid")


def validated_next_link(
    candidate: object,
    *,
    contract: FHIRSearchContract,
) -> FHIRContinuation:
    """Validate one next link and bind it to the active in-memory search."""

    parsed_candidate, parsed_base = _parsed_candidate(candidate, contract)
    query_pairs = _query_pairs(parsed_candidate)
    collection_path = urllib.parse.urlsplit(collection_url(contract)).path
    if parsed_candidate.path == collection_path:
        _validate_collection_query(query_pairs, contract)
    elif parsed_candidate.path == parsed_base.path:
        _validate_smile_query(query_pairs, contract)
    else:
        raise FHIRTransportError("FHIR continuation path is invalid")
    assert isinstance(candidate, str)
    url_fingerprint = hashlib.sha256(candidate.encode("utf-8")).hexdigest()
    return FHIRContinuation(
        _request_url=candidate,
        resource_type=contract.resource_type,
        search_contract_hash=contract.contract_hash,
        url_fingerprint=url_fingerprint,
    )
