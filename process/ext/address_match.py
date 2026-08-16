# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Conservative pairwise matching for already-canonicalized provider addresses."""

from __future__ import annotations

import json
import re
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from typing import Any, Literal

from process.ext import address_canon
from process.ext.address_pub28 import (
    PUB28_DIRECTIONAL_MAP,
    PUB28_STREET_SUFFIX_MAP,
    PUB28_UNIT_DESIGNATOR_MAP,
)


MatchClassification = Literal["exact", "premise_only"]
_TOKEN_RE = re.compile(r"[a-z0-9]+", re.IGNORECASE)
_UNIT_PREFIXES = tuple(sorted(set(PUB28_UNIT_DESIGNATOR_MAP.values()), key=len, reverse=True))
_ROUTE_MARKERS = {
    token for token, normalized in PUB28_STREET_SUFFIX_MAP.items() if normalized == "hwy"
} | {"route", "rte", "interstate", "us", "sr", "sh", "cr", "i", "fm"}
_PARSER_RULES = (
    "candidate_confirmed_bare_unit",
    "unit_designator_punctuation",
    "candidate_confirmed_spaced_unit",
    "formatted_address_omits_descriptor",
    "canonical_premise_key",
)
_PUNCTUATED_UNIT_RE = re.compile(
    rf"\b({'|'.join(re.escape(value) for value in PUB28_UNIT_DESIGNATOR_MAP if value.isalpha())})\b\s*:\s*",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class AddressRecord:
    """One source address or one endpoint-visible HealthPorta candidate."""

    npi: str | None
    first_line: str | None
    second_line: str | None
    city: str | None
    state: str | None
    zip_code: str | None
    country: str | None = "US"
    address_key: str | None = None
    premise_key: str | None = None
    formatted_address: str | None = None
    is_healthporta_visible: bool = False


@dataclass(frozen=True)
class AddressMatch:
    """Auditable result for one selected HealthPorta address key."""

    target_address_key: str
    classification: MatchClassification
    rule: str
    source_unit: str
    target_unit: str
    city_differs: bool


def _is_valid_npi(value: str | None) -> bool:
    digits = str(value or "").strip()
    if not re.fullmatch(r"[0-9]{10}", digits):
        return False
    if not 1_000_000_000 <= int(digits) <= 2_999_999_999:
        return False
    prefixed_digits = [int(digit) for digit in f"80840{digits}"]
    total = 0
    for index, digit in enumerate(reversed(prefixed_digits)):
        product = digit * (2 if index % 2 else 1)
        total += product // 10 + product % 10
    return total % 10 == 0


def _stored_key(value: str | None) -> str | None:
    try:
        return str(uuid.UUID(str(value or "")))
    except ValueError:
        return None


def _computed_address_key(record: AddressRecord, first_line: str | None = None, second_line: str | None = None) -> str | None:
    key = address_canon.address_key_v1(
        record.first_line if first_line is None else first_line,
        record.second_line if second_line is None else second_line,
        record.city,
        record.state,
        record.zip_code,
        record.country,
    )
    return str(key) if key else None


def _computed_premise_key(record: AddressRecord) -> str | None:
    identity = address_canon.premise_identity_key_v1(
        record.first_line,
        record.second_line,
        record.city,
        record.state,
        record.zip_code,
        record.country,
    )
    key = address_canon.key_from_identity(identity)
    return str(key) if key else None


def _unit(record: AddressRecord) -> str:
    return address_canon.unit_norm(record.first_line, record.second_line)


def _unit_value(unit: str) -> tuple[str, str] | None:
    for prefix in _UNIT_PREFIXES:
        if unit.startswith(prefix) and len(unit) > len(prefix):
            return prefix, unit[len(prefix):]
    return None


def _classification(source: AddressRecord, target: AddressRecord) -> MatchClassification:
    return "exact" if _unit(source) == _unit(target) else "premise_only"


def _is_city_different(source: AddressRecord, target: AddressRecord) -> bool:
    return address_canon.city_norm(source.city) != address_canon.city_norm(target.city)


def _result(source: AddressRecord, target: AddressRecord, rule: str, classification: MatchClassification | None = None) -> AddressMatch:
    target_key = _stored_key(target.address_key)
    assert target_key is not None
    return AddressMatch(
        target_address_key=target_key,
        classification=classification or _classification(source, target),
        rule=rule,
        source_unit=_unit(source),
        target_unit=_unit(target),
        city_differs=_is_city_different(source, target),
    )


def _is_candidate_eligible(source: AddressRecord, target: AddressRecord) -> bool:
    source_npi = str(source.npi or "").strip()
    target_npi = str(target.npi or "").strip()
    target_key = _stored_key(target.address_key)
    stored_premise = _stored_key(target.premise_key)
    return bool(
        _is_valid_npi(source_npi)
        and source_npi == target_npi
        and target.is_healthporta_visible
        and target_key
        and target_key == _computed_address_key(target)
        and (
            not target.premise_key
            or stored_premise == _computed_premise_key(target)
        )
        and address_canon.country_code(source.country) == address_canon.country_code(target.country)
        and address_canon.state_code(source.state)
        and address_canon.state_code(source.state) == address_canon.state_code(target.state)
        and address_canon.zip5_norm(source.zip_code)
        and address_canon.zip5_norm(source.zip_code) == address_canon.zip5_norm(target.zip_code)
        and address_canon.street_norm(source.first_line, source.second_line)
        and address_canon.street_norm(target.first_line, target.second_line)
    )


def _street_relation(
    source_line1: str | None,
    source_line2: str | None,
    target_line1: str | None,
    target_line2: str | None,
) -> str | None:
    source_street = address_canon.street_norm(source_line1, source_line2)
    target_street = address_canon.street_norm(target_line1, target_line2)
    if not source_street or not target_street:
        return None
    if source_street == target_street:
        return "same_street"

    source_direction = address_canon.street_direction_token(source_line1, source_line2)
    target_direction = address_canon.street_direction_token(target_line1, target_line2)
    if (
        source_direction
        and source_direction == target_direction
        and address_canon.street_directionless_norm(source_line1, source_line2)
        == address_canon.street_directionless_norm(target_line1, target_line2)
    ):
        return "direction_relocation"

    source_suffix = address_canon.street_suffix_token(source_line1, source_line2)
    target_suffix = address_canon.street_suffix_token(target_line1, target_line2)
    if (
        bool(source_suffix) != bool(target_suffix)
        and source_direction == target_direction
        and address_canon.street_suffixless_norm(source_line1, source_line2)
        == address_canon.street_suffixless_norm(target_line1, target_line2)
    ):
        return "terminal_suffix_omission"
    return None


def _has_explicit_unit(target: AddressRecord) -> bool:
    text = f"{target.first_line or ''} {target.second_line or ''}"
    tokens = _TOKEN_RE.findall(text.lower())
    return bool(_unit(target) and ("#" in text or any(token in PUB28_UNIT_DESIGNATOR_MAP for token in tokens)))


def _is_route_number_removed(tokens: list[str], base_end: int) -> bool:
    if base_end < 1 or base_end >= len(tokens):
        return False
    marker = tokens[base_end - 1]
    return marker in _ROUTE_MARKERS or (
        marker == "s" and base_end >= 2 and tokens[base_end - 2] == "u"
    ) or (
        marker in {"road", "rd"} and base_end >= 2 and tokens[base_end - 2] in {"county", "state"}
    ) or (
        marker in {"no", "number"} and base_end >= 2 and tokens[base_end - 2] in _ROUTE_MARKERS
    ) or (
        marker == "loop"
        and (
            base_end == 2
            or tokens[base_end - 2] in PUB28_DIRECTIONAL_MAP
            or tokens[base_end - 2] in {"business", "state"}
        )
    )


def _bare_unit_evidence(source_address: AddressRecord, target_address: AddressRecord) -> tuple[str, str] | None:
    target_unit = _unit(target_address)
    unit_parts = _unit_value(target_unit)
    if (
        str(source_address.second_line or "").strip()
        or _unit(source_address)
        or not unit_parts
        or not _has_explicit_unit(target_address)
    ):
        return None
    prefix, _ = unit_parts
    matches = list(_TOKEN_RE.finditer(source_address.first_line or ""))
    tokens = [match.group(0).lower() for match in matches]
    for tail_size in (1, 2):
        base_end = len(tokens) - tail_size
        if base_end < 2:
            continue
        tail = tokens[base_end:]
        if any(token in PUB28_DIRECTIONAL_MAP or token in PUB28_STREET_SUFFIX_MAP for token in tail):
            continue
        if _is_route_number_removed(tokens, base_end):
            continue
        bare_value = "".join(tail)
        base = (source_address.first_line or "")[: matches[base_end].start()].rstrip(" ,")
        alternate_second = f"{prefix} {bare_value}"
        if address_canon.unit_norm(base, alternate_second) != target_unit:
            continue
        relation = _street_relation(base, "", target_address.first_line, target_address.second_line)
        if not relation:
            continue
        alternate_key = _computed_address_key(source_address, base, alternate_second)
        target_key = _stored_key(target_address.address_key)
        key_is_confirmed = (
            alternate_key == target_key
            if relation == "same_street"
            else _computed_address_key(target_address) == target_key
        )
        if key_is_confirmed:
            return relation, base
    return None


def _bare_unit_result(source: AddressRecord, target: AddressRecord) -> AddressMatch | None:
    if _bare_unit_evidence(source, target):
        return _result(source, target, "candidate_confirmed_bare_unit", "exact")
    return None


def _punctuation_result(source: AddressRecord, target: AddressRecord) -> AddressMatch | None:
    if _unit(source):
        return None
    first = _PUNCTUATED_UNIT_RE.sub(r"\1 ", source.first_line or "")
    second = _PUNCTUATED_UNIT_RE.sub(r"\1 ", source.second_line or "")
    if (first, second) == (source.first_line or "", source.second_line or ""):
        return None
    alternate_unit = address_canon.unit_norm(first, second)
    if not alternate_unit or alternate_unit != _unit(target):
        return None
    if _computed_address_key(source, first, second) != _stored_key(target.address_key):
        return None
    return _result(source, target, "unit_designator_punctuation", "exact")


def _spaced_unit_relation(source: AddressRecord, target: AddressRecord) -> str | None:
    source_unit = _unit(source)
    source_parts = _unit_value(source_unit)
    target_tokens = _TOKEN_RE.findall(target.second_line or "")
    if (
        not source_parts
        or _unit(target)
        or not 1 <= len(target_tokens) <= 2
        or any(token.lower() in PUB28_DIRECTIONAL_MAP or token.lower() in PUB28_STREET_SUFFIX_MAP for token in target_tokens)
        or source_parts[1] != "".join(target_tokens).lower()
    ):
        return None
    return _street_relation(source.first_line, source.second_line, target.first_line, "")


def _spaced_unit_result(source: AddressRecord, target: AddressRecord) -> AddressMatch | None:
    if not _spaced_unit_relation(source, target):
        return None
    return _result(source, target, "candidate_confirmed_spaced_unit", "exact")


def _descriptor_result(source: AddressRecord, target: AddressRecord) -> AddressMatch | None:
    if source.second_line or not target.second_line or _unit(target) or not target.formatted_address:
        return None
    formatted_street = re.split(r"[,\n]", target.formatted_address, maxsplit=1)[0]
    if address_canon.street_norm(formatted_street, "") != address_canon.street_norm(target.first_line, ""):
        return None
    descriptor = re.sub(r"[^a-z0-9]", "", target.second_line.lower())
    formatted = re.sub(r"[^a-z0-9]", "", target.formatted_address.lower())
    if descriptor and descriptor in formatted:
        return None
    if _computed_address_key(source) != _computed_address_key(target, target.first_line, ""):
        return None
    return _result(source, target, "formatted_address_omits_descriptor", "exact")


def _has_explicit_conflict(source: AddressRecord, candidates: Sequence[AddressRecord]) -> bool:
    stem = address_canon.street_completion_norm(source.first_line, "")
    related_candidates = [
        candidate
        for candidate in candidates
        if address_canon.street_completion_norm(candidate.first_line, "") == stem
    ]
    directions = {
        value
        for record in [source, *related_candidates]
        if (value := address_canon.street_direction_token(record.first_line, ""))
    }
    suffixes = {
        value
        for record in [source, *related_candidates]
        if (value := address_canon.street_suffix_token(record.first_line, ""))
    }
    return len(directions) > 1 or len(suffixes) > 1


def _unique_best_result(results: Sequence[AddressMatch]) -> AddressMatch | None:
    exact_matches = [result for result in results if result.classification == "exact"]
    by_key = {result.target_address_key: result for result in exact_matches or results}
    return next(iter(by_key.values())) if len(by_key) == 1 else None


def _eligible_candidates(source: AddressRecord, candidates: Sequence[AddressRecord]) -> list[AddressRecord]:
    candidates_by_key: dict[str, AddressRecord] = {}
    for candidate in candidates:
        if not _is_candidate_eligible(source, candidate):
            continue
        key = _stored_key(candidate.address_key)
        assert key is not None
        existing_candidate = candidates_by_key.get(key)
        if existing_candidate is None or (not existing_candidate.formatted_address and candidate.formatted_address):
            candidates_by_key[key] = candidate
    return list(candidates_by_key.values())


def _select_parser_match(
    source_address: AddressRecord,
    eligible_candidates: Sequence[AddressRecord],
    candidates_by_key: Mapping[str | None, AddressRecord],
    pair_matches: Sequence[AddressMatch],
) -> AddressMatch | None:
    for rule in _PARSER_RULES:
        rule_matches = [pair_match for pair_match in pair_matches if pair_match.rule == rule]
        if not rule_matches:
            continue
        if rule == "candidate_confirmed_bare_unit":
            bare_evidence_pairs = [
                (pair_match, _bare_unit_evidence(source_address, candidates_by_key[pair_match.target_address_key]))
                for pair_match in rule_matches
            ]
            if any(
                evidence
                and _has_explicit_conflict(
                    replace(source_address, first_line=evidence[1], second_line=""),
                    eligible_candidates,
                )
                for _, evidence in bare_evidence_pairs
            ):
                return None
        if rule == "candidate_confirmed_spaced_unit":
            spaced_relation_pairs = [
                (pair_match, _spaced_unit_relation(source_address, candidates_by_key[pair_match.target_address_key]))
                for pair_match in rule_matches
            ]
            same_street_matches = [
                pair_match for pair_match, relation in spaced_relation_pairs if relation == "same_street"
            ]
            if same_street_matches:
                return _unique_best_result(same_street_matches)
            if any(
                relation and _has_explicit_conflict(source_address, eligible_candidates)
                for _, relation in spaced_relation_pairs
            ):
                return None
        return _unique_best_result(rule_matches)
    return None


def compare_address_pair(source: AddressRecord, target: AddressRecord) -> AddressMatch | None:
    """Compare one address pair after applying mandatory provider and geography gates."""

    if not _is_candidate_eligible(source, target):
        return None
    if _computed_address_key(source) == _stored_key(target.address_key):
        return _result(source, target, "canonical_address_key", "exact")
    for matcher in (_bare_unit_result, _punctuation_result, _spaced_unit_result, _descriptor_result):
        result = matcher(source, target)
        if result:
            return result
    target_premise = _computed_premise_key(target)
    if _computed_premise_key(source) == target_premise:
        return _result(source, target, "canonical_premise_key")
    relation = _street_relation(source.first_line, source.second_line, target.first_line, target.second_line)
    if relation in {"direction_relocation", "terminal_suffix_omission"}:
        return _result(source, target, relation)
    return None


def match_address_candidates(source_address: AddressRecord, candidates: Sequence[AddressRecord]) -> AddressMatch | None:
    """Select one auditable match, deduplicating endpoint rows by stored address key."""

    eligible_candidates = _eligible_candidates(source_address, candidates)
    candidates_by_key = {_stored_key(candidate.address_key): candidate for candidate in eligible_candidates}
    pair_matches = [
        pair_match
        for candidate in eligible_candidates
        if (pair_match := compare_address_pair(source_address, candidate))
    ]
    if not pair_matches:
        return None

    canonical_matches = [pair_match for pair_match in pair_matches if pair_match.rule == "canonical_address_key"]
    if canonical_matches:
        return _unique_best_result(canonical_matches)
    bare_matches = [pair_match for pair_match in pair_matches if pair_match.rule == "candidate_confirmed_bare_unit"]
    same_street_bare_matches = [
        pair_match
        for pair_match in bare_matches
        if (_bare_unit_evidence(source_address, candidates_by_key[pair_match.target_address_key]) or (None,))[0]
        == "same_street"
    ]
    if same_street_bare_matches:
        return _unique_best_result(same_street_bare_matches)
    preferred_matches = [pair_match for pair_match in pair_matches if pair_match.classification == "exact"] or pair_matches
    if len({pair_match.target_address_key for pair_match in preferred_matches}) > 1:
        return None
    parser_matches = [
        pair_match
        for pair_match in preferred_matches
        if pair_match.rule in _PARSER_RULES
    ]
    if parser_matches:
        return _select_parser_match(source_address, eligible_candidates, candidates_by_key, parser_matches)
    relaxed_matches = [
        pair_match
        for pair_match in preferred_matches
        if pair_match.rule in {"direction_relocation", "terminal_suffix_omission"}
    ]
    if relaxed_matches and _has_explicit_conflict(source_address, eligible_candidates):
        return None
    return _unique_best_result(relaxed_matches)


def _record_from_mapping(value: Mapping[str, Any]) -> AddressRecord:
    allowed = AddressRecord.__dataclass_fields__
    return AddressRecord(**{name: value[name] for name in allowed if name in value})

def main() -> None:
    """Match JSON batches from stdin for non-Python workbook consumers."""

    rows = json.load(sys.stdin)
    outputs = []
    for row in rows:
        source = _record_from_mapping(row["source"])
        candidates = [_record_from_mapping(candidate) for candidate in row.get("candidates", [])]
        result = match_address_candidates(source, candidates)
        outputs.append(asdict(result) if result else None)
    json.dump(outputs, sys.stdout, separators=(",", ":"))


if __name__ == "__main__":
    main()
