# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Parse hospital MRF locators and bind their records without fuzzy matching."""

from __future__ import annotations

import html
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from urllib.parse import urlsplit


MAX_HOSPITAL_HPT_LOCATOR_BYTES = 1_000_000


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
    record_index: int
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
    text = text.replace("\r\n", "\n")
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


def _line_field(line: str) -> tuple[str, str]:
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
        key, value = _line_field(line)
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


def _indexes_by_name(values: Iterable[str]) -> dict[str, list[int]]:
    indexes_by_name: dict[str, list[int]] = {}
    for index, value in enumerate(values):
        indexes_by_name.setdefault(
            normalized_hospital_location_name(value), []
        ).append(index)
    return indexes_by_name


def _hospital_locator_name(hospital: Mapping[str, str]) -> str:
    return hospital.get("locator_name") or hospital["name"]


def match_hospital_hpt_locator(
    hospitals: Iterable[Mapping[str, str]],
    cms_hpt_url: str,
    locator_records: Sequence[HospitalHptLocatorRecord],
) -> HospitalHptLocatorMatch:
    """Bind exact names; one locator record may serve multiple facilities."""

    cohort_hospitals = tuple(
        hospital for hospital in hospitals if hospital.get("cms_hpt_url") == cms_hpt_url
    )
    record_names = _indexes_by_name(
        locator_record.location_name for locator_record in locator_records
    )
    bindings: list[HospitalMrfBinding] = []
    unmatched_hospital_ids: list[str] = []
    ambiguous_hospital_ids: list[str] = []
    ambiguous_record_indexes: set[int] = set()
    for hospital in cohort_hospitals:
        indexes = record_names.get(
            normalized_hospital_location_name(_hospital_locator_name(hospital)), []
        )
        selected_mrf_url = hospital.get("locator_mrf_url")
        if selected_mrf_url:
            indexes = [
                index
                for index in indexes
                if locator_records[index].mrf_url == selected_mrf_url
            ]
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
        elif indexes_by_mrf_url:
            ambiguous_hospital_ids.append(hospital["hospital_id"])
            ambiguous_record_indexes.update(indexes)
        else:
            unmatched_hospital_ids.append(hospital["hospital_id"])
    bound_record_indexes = {binding.record_index for binding in bindings}
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
