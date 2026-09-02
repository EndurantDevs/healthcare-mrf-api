# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Parse hospital MRF locators and bind their records without fuzzy matching."""

from __future__ import annotations

import html
import re
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlsplit, urlunsplit

from process.ptg_parts.domain import PTG2_STRIPPED_QUERY_PARAMS


MAX_HOSPITAL_HPT_LOCATOR_BYTES = 1_000_000
_HOSPITAL_MRF_CREDENTIAL_QUERY_KEYS = frozenset(
    PTG2_STRIPPED_QUERY_PARAMS
) | {"si", "sr"}
_MRF_URL_WITH_CONTACT_NAME = re.compile(
    r"(?P<url>https?://\S+)\s+contact-name:\s*(?P<name>\S(?:.*\S)?)",
    re.IGNORECASE,
)
_LOCATOR_RECORD_FIELDS = frozenset(
    {
        "contact-email",
        "contact-name",
        "location name",
        "location-name",
        "mrf-url",
        "source-page-url",
    }
)
_LOCATOR_FIELD_ALIASES = {
    "location name": "location-name",
    "mrf_url": "mrf-url",
    "source-page_url": "source-page-url",
}
_LOCATOR_FIELD_PREFIXES = _LOCATOR_RECORD_FIELDS | _LOCATOR_FIELD_ALIASES.keys()


class HospitalHptLocatorError(ValueError):
    """Raised when a hospital MRF locator is unsafe or incomplete."""


@dataclass(frozen=True)
class HospitalHptLocatorRecord:
    """Identify one hospital location and its MRF URL."""

    location_name: str
    mrf_url: str


@dataclass(frozen=True)
class HospitalMrfBinding:
    """Bind one project hospital to one locator record."""

    hospital_id: str
    record_index: int | None
    mrf_url: str


@dataclass(frozen=True)
class HospitalHptLocatorMatch:
    """Report exact bindings and every unresolved hospital or record."""

    bindings: tuple[HospitalMrfBinding, ...]
    content_targets: tuple[str, ...]
    unmatched_hospital_ids: tuple[str, ...]
    unmatched_record_indexes: tuple[int, ...]
    ambiguous_hospital_ids: tuple[str, ...]
    ambiguous_record_indexes: tuple[int, ...]


def _locator_error(reason: str) -> HospitalHptLocatorError:
    return HospitalHptLocatorError(f"hospital_hpt_locator_invalid:{reason}")


def _decoded_locator(payload: bytes) -> str:
    if type(payload) is not bytes:
        raise _locator_error("payload_type")
    if len(payload) > MAX_HOSPITAL_HPT_LOCATOR_BYTES:
        raise _locator_error("payload_too_large")
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise _locator_error("utf8") from exc
    text = re.sub(r"\r+\n", "\n", text).replace("\t", " ")
    if "\r" in text or "\ufeff" in text:
        raise _locator_error("control_character")
    if any(
        character != "\n"
        and unicodedata.category(character) in {"Cc", "Zl", "Zp"}
        for character in text
    ):
        raise _locator_error("control_character")
    return text


def _validated_mrf_url(value: str) -> str:
    if not value or any(character.isspace() for character in value):
        raise _locator_error("mrf_url")
    try:
        parsed = urlsplit(value)
        _validated_port = parsed.port
    except ValueError as exc:
        raise _locator_error("mrf_url") from exc
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or "#" in value
    ):
        raise _locator_error("mrf_url")
    return value


def _record(fields: Mapping[str, str]) -> HospitalHptLocatorRecord:
    location_name = fields.get("location-name", "")
    if not location_name:
        raise _locator_error("location_name")
    return HospitalHptLocatorRecord(
        location_name=location_name,
        mrf_url=_validated_mrf_url(fields.get("mrf-url", "")),
    )


def _field_key(value: str) -> str:
    key = value.strip().casefold()
    return _LOCATOR_FIELD_ALIASES.get(key, key)


def _has_reserved_field_prefix(value: str) -> bool:
    candidate = value.strip().casefold()
    return any(
        candidate.startswith(field)
        and len(candidate) > len(field)
        and candidate[len(field)].isspace()
        for field in _LOCATOR_FIELD_PREFIXES
    )


def _is_inter_record_heading(
    lines: Sequence[str],
    index: int,
    fields_by_key: Mapping[str, str],
    is_preceded_by_blank: bool,
) -> bool:
    """Accept one bare section heading only between complete records."""

    if (
        not is_preceded_by_blank
        or not {"location-name", "mrf-url"} <= fields_by_key.keys()
        or ":" in lines[index]
        or _has_reserved_field_prefix(lines[index])
        or _field_key(lines[index]) in _LOCATOR_RECORD_FIELDS
    ):
        return False
    for next_line in lines[index + 1 :]:
        if not next_line.strip():
            continue
        raw_key, separator, _raw_value = next_line.partition(":")
        return bool(separator and _field_key(raw_key) == "location-name")
    return False


def _line_fields(
    line: str,
    fields_by_key: Mapping[str, str],
    is_empty_mrf_continuation_allowed: bool,
) -> tuple[tuple[tuple[str, str], ...], bool]:
    stripped_line = line.strip()
    has_mrf_url = "mrf-url" in fields_by_key
    if (
        stripped_line.casefold().startswith(("http://", "https://"))
        and fields_by_key.get("location-name")
        and fields_by_key.get("source-page-url")
        and (
            not has_mrf_url
            or (
                is_empty_mrf_continuation_allowed
                and not fields_by_key["mrf-url"]
            )
        )
    ):
        return (("mrf-url", stripped_line),), has_mrf_url
    if stripped_line.casefold().startswith(("http://", "https://")):
        if not fields_by_key.get("mrf-url"):
            raise _locator_error("mrf_url")
        raise _locator_error("line")
    raw_key, separator, raw_value = line.partition(":")
    if _has_reserved_field_prefix(raw_key):
        raise _locator_error("line")
    key = _field_key(raw_key)
    if not separator or not key:
        raise _locator_error("line")
    field_value = raw_value.strip()
    contact_match = (
        _MRF_URL_WITH_CONTACT_NAME.fullmatch(field_value)
        if key == "mrf-url"
        else None
    )
    if contact_match:
        return (
            (
                ("mrf-url", contact_match.group("url")),
                ("contact-name", contact_match.group("name")),
            ),
            False,
        )
    return ((key, field_value),), False


def parse_hospital_hpt_locator(
    locator_payload: bytes,
) -> tuple[HospitalHptLocatorRecord, ...]:
    """Parse a bounded UTF-8 locator into validated location records."""

    locator_records: list[HospitalHptLocatorRecord] = []
    fields_by_key: dict[str, str] = {}
    has_records_started = False
    is_empty_mrf_continuation_allowed = False
    lines = _decoded_locator(locator_payload).split("\n")
    is_preceded_by_blank = False
    for index, line in enumerate(lines):
        if not line.strip():
            is_preceded_by_blank = True
            continue
        if has_records_started and _is_inter_record_heading(
            lines, index, fields_by_key, is_preceded_by_blank
        ):
            is_preceded_by_blank = False
            continue
        if not has_records_started:
            raw_key, _separator, _raw_value = line.partition(":")
            candidate_key = _field_key(raw_key)
            if candidate_key != "location-name":
                if candidate_key in _LOCATOR_RECORD_FIELDS:
                    raise _locator_error("location_name")
                if _has_reserved_field_prefix(raw_key):
                    raise _locator_error("line")
                continue
            has_records_started = True
        line_fields, replaces_empty_mrf_url = _line_fields(
            line, fields_by_key, is_empty_mrf_continuation_allowed
        )
        is_preceded_by_blank = False
        is_empty_mrf_continuation_allowed = line_fields == (("mrf-url", ""),)
        for key, field_value in line_fields:
            if key == "location-name" and key in fields_by_key:
                locator_records.append(_record(fields_by_key))
                fields_by_key = {}
            if key in fields_by_key:
                if replaces_empty_mrf_url and key == "mrf-url":
                    fields_by_key[key] = field_value
                    continue
                raise _locator_error("duplicate_field")
            fields_by_key[key] = field_value
    if fields_by_key:
        locator_records.append(_record(fields_by_key))
    if not locator_records:
        raise _locator_error("empty")
    return tuple(locator_records)


def normalized_hospital_location_name(value: str) -> str:
    """Canonicalize only representation differences allowed for exact matching."""

    unescaped = html.unescape(value)
    normalized = unicodedata.normalize("NFC", unescaped)
    return " ".join(normalized.split()).casefold()


def hospital_mrf_selector(
    mrf_url: str, *, allow_credentials: bool = False
) -> str | None:
    """Return one exact queryless MRF identity or fail closed."""

    if (
        not mrf_url
        or "#" in mrf_url
        or "\\" in mrf_url
        or any(
            character.isspace() or unicodedata.category(character) == "Cc"
            for character in mrf_url
        )
    ):
        return None
    try:
        parsed = urlsplit(mrf_url)
        port = parsed.port
    except ValueError:
        return None
    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()
    if (
        scheme not in {"http", "https"}
        or not hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    if parsed.query:
        try:
            query = parse_qsl(
                parsed.query, keep_blank_values=True, max_num_fields=64
            )
        except ValueError:
            return None
        if (
            not allow_credentials
            or ";" in parsed.query
            or not query
            or any(
                not key
                or key.casefold() not in _HOSPITAL_MRF_CREDENTIAL_QUERY_KEYS
                for key, _value in query
            )
        ):
            return None
    default_port = 443 if scheme == "https" else 80
    host = f"[{hostname}]" if ":" in hostname else hostname
    netloc = host if port in {None, default_port} else f"{host}:{port}"
    return urlunsplit((scheme, netloc, parsed.path, "", ""))


def _indexes_by_name(values: Iterable[str]) -> dict[str, list[int]]:
    indexes_by_name: dict[str, list[int]] = {}
    for index, value in enumerate(values):
        indexes_by_name.setdefault(
            normalized_hospital_location_name(value), []
        ).append(index)
    return indexes_by_name


def _hospital_locator_name(hospital: Mapping[str, str]) -> str:
    return hospital.get("locator_name") or hospital["name"]


def _selector_binding(
    hospital: Mapping[str, str],
    locator_records: Sequence[HospitalHptLocatorRecord],
    named_indexes: Sequence[int],
    selector: str,
) -> tuple[HospitalMrfBinding | None, tuple[int, ...]]:
    selector_indexes = tuple(
        index
        for index, locator_record in enumerate(locator_records)
        if hospital_mrf_selector(
            locator_record.mrf_url, allow_credentials=True
        ) == selector
    )
    if not selector_indexes:
        return None, ()
    selector_index_set = set(selector_indexes)
    named_selector_indexes = tuple(
        index for index in named_indexes if index in selector_index_set
    )
    selected_index = (named_selector_indexes or selector_indexes)[0]
    selected_names = {
        normalized_hospital_location_name(locator_records[index].location_name)
        for index in selector_indexes
    }
    return HospitalMrfBinding(
        hospital_id=hospital["hospital_id"],
        record_index=(
            selected_index
            if named_selector_indexes or len(selected_names) == 1
            else None
        ),
        mrf_url=locator_records[selected_index].mrf_url,
    ), selector_indexes


def match_hospital_hpt_locator(
    hospitals: Iterable[Mapping[str, str]], cms_hpt_url: str,
    locator_records: Sequence[HospitalHptLocatorRecord],
) -> HospitalHptLocatorMatch:
    """Bind exact names or reviewed exact content selectors."""
    cohort_hospitals = tuple(hospital for hospital in hospitals
                             if hospital.get("cms_hpt_url") == cms_hpt_url)
    record_names = _indexes_by_name(locator_record.location_name
                                    for locator_record in locator_records)
    bindings: list[HospitalMrfBinding] = []
    unmatched_hospital_ids: list[str] = []
    ambiguous_hospital_ids: list[str] = []
    ambiguous_record_indexes: set[int] = set()
    bound_record_indexes: set[int] = set()
    for hospital in cohort_hospitals:
        indexes = record_names.get(
            normalized_hospital_location_name(_hospital_locator_name(hospital)), []
        )
        selector = hospital.get("locator_mrf_url")
        if selector:
            binding, selector_indexes = _selector_binding(
                hospital, locator_records, indexes, selector
            )
            if binding is None:
                unmatched_hospital_ids.append(hospital["hospital_id"])
            else:
                bindings.append(binding)
                bound_record_indexes.update(selector_indexes)
            continue
        indexes_by_mrf_url = {
            locator_records[index].mrf_url: index for index in reversed(indexes)
        }
        if len(indexes_by_mrf_url) == 1:
            record_index = next(iter(indexes_by_mrf_url.values()))
            bindings.append(
                HospitalMrfBinding(
                    hospital_id=hospital["hospital_id"],
                    record_index=record_index,
                    mrf_url=locator_records[record_index].mrf_url,
                )
            )
            bound_record_indexes.add(record_index)
        elif indexes_by_mrf_url:
            ambiguous_hospital_ids.append(hospital["hospital_id"])
            ambiguous_record_indexes.update(indexes)
        else:
            unmatched_hospital_ids.append(hospital["hospital_id"])
    return HospitalHptLocatorMatch(
        bindings=tuple(bindings),
        content_targets=tuple(dict.fromkeys(binding.mrf_url for binding in bindings)),
        unmatched_hospital_ids=tuple(unmatched_hospital_ids),
        unmatched_record_indexes=tuple(
            index
            for index in range(len(locator_records))
            if index not in bound_record_indexes
            and index not in ambiguous_record_indexes
        ),
        ambiguous_hospital_ids=tuple(ambiguous_hospital_ids),
        ambiguous_record_indexes=tuple(sorted(ambiguous_record_indexes)),
    )
