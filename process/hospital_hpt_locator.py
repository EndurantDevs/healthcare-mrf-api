# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Parse hospital MRF locators and bind their records without fuzzy matching."""

from __future__ import annotations

import html
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlsplit, urlunsplit

from process.ptg_parts.domain import PTG2_STRIPPED_QUERY_PARAMS


MAX_HOSPITAL_HPT_LOCATOR_BYTES = 1_000_000
_HOSPITAL_MRF_CREDENTIAL_QUERY_KEYS = frozenset(
    PTG2_STRIPPED_QUERY_PARAMS
) | {"si", "sr"}


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
    text = text.replace("\r\n", "\n").replace("\t", " ")
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


def _line_field(
    line: str, fields_by_key: Mapping[str, str]
) -> tuple[str, str]:
    stripped_line = line.strip()
    if (
        stripped_line.startswith(("http://", "https://"))
        and {"location-name", "source-page-url"} <= fields_by_key.keys()
        and "mrf-url" not in fields_by_key
    ):
        return "mrf-url", stripped_line
    raw_key, separator, raw_value = line.partition(":")
    key = raw_key.strip().casefold()
    if not separator or not key or any(character.isspace() for character in key):
        raise _locator_error("line")
    return key, raw_value.strip()


def parse_hospital_hpt_locator(
    payload: bytes,
) -> tuple[HospitalHptLocatorRecord, ...]:
    """Parse a bounded UTF-8 locator into validated location records."""

    records: list[HospitalHptLocatorRecord] = []
    fields_by_key: dict[str, str] = {}
    for line in _decoded_locator(payload).split("\n"):
        if not line.strip():
            if fields_by_key:
                records.append(_record(fields_by_key))
                fields_by_key = {}
            continue
        key, value = _line_field(line, fields_by_key)
        if key == "location-name" and key in fields_by_key:
            records.append(_record(fields_by_key))
            fields_by_key = {}
        if key in fields_by_key:
            raise _locator_error("duplicate_field")
        fields_by_key[key] = value
    if fields_by_key:
        records.append(_record(fields_by_key))
    if not records:
        raise _locator_error("empty")
    return tuple(records)


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
