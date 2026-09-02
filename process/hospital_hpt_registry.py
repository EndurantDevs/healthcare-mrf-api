# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict loader for the source-neutral hospital locator registry."""

from __future__ import annotations

import hashlib
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from process.hospital_hpt_locator import hospital_mrf_selector


HOSPITAL_HPT_REGISTRY_PATH = (
    Path(__file__).resolve().parents[1] / "specs/hospital_hpt_registry.yaml"
)
EXPECTED_HOSPITAL_HPT_REGISTRY_COUNT = 7_356
EXPECTED_HOSPITAL_HPT_REGISTRY_SHA256 = (
    "550cf3fc7ef6c638fb659902af79e91a899254c1435301e09b903480e4d624fe"
)
MAX_HOSPITAL_HPT_SELECTION = 200
_DOCUMENT_FIELDS = frozenset({"version", "hospitals"})
_HOSPITAL_REQUIRED_FIELDS = frozenset({"name", "cms_hpt_url"})
_HOSPITAL_ID_FIELDS = frozenset({"hospital_id", "hospital_ids"})
_HOSPITAL_OPTIONAL_FIELDS = frozenset(
    {
        "alias_of",
        "fallback_mrf_url",
        "locator_name",
        "locator_mrf_url",
    }
)
_SELECTION_FIELDS = frozenset(
    {"hospital_id", "hospital_ids", "all_hospitals", "test_mode"}
)


class HospitalHptRegistryError(ValueError):
    """Raised when the checked-in hospital registry is invalid."""


def _registry_error(reason: str) -> HospitalHptRegistryError:
    return HospitalHptRegistryError(f"hospital_hpt_registry_invalid:{reason}")


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeyLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    loader.flatten_mapping(node)
    mapping_by_key: dict[Any, Any] = {}
    for key, value in loader.construct_pairs(node, deep=deep):
        try:
            is_duplicate = key in mapping_by_key
        except TypeError as exc:
            raise _registry_error("document_unreadable") from exc
        if is_duplicate:
            raise _registry_error("duplicate_field")
        mapping_by_key[key] = value
    return mapping_by_key


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def _strict_text(value: Any, field: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise _registry_error(f"{field}_invalid")
    return value


def _validated_hospital_id(value: Any, field: str = "hospital_id") -> str:
    hospital_id = _strict_text(value, field)
    if any(character.isspace() for character in hospital_id):
        raise _registry_error(f"{field}_invalid")
    return hospital_id


def _validated_http_url(value: Any, field: str) -> str:
    url = _strict_text(value, field)
    if any(character.isspace() for character in url) or "#" in url:
        raise _registry_error(f"{field}_invalid")
    try:
        parsed = urlsplit(url)
        _validated_port = parsed.port
    except ValueError as exc:
        raise _registry_error(f"{field}_invalid") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise _registry_error(f"{field}_invalid")
    return url


def _validated_locator(value: Any) -> str:
    return _validated_http_url(value, "cms_hpt_url")


def _validated_mrf_selector(value: Any) -> str:
    selector = _strict_text(value, "locator_mrf_url")
    if hospital_mrf_selector(selector) != selector:
        raise _registry_error("locator_mrf_url_invalid")
    return selector


def _validate_hospital_aliases(hospitals: list[dict[str, str]]) -> None:
    """Reject aliases without one same-locator canonical target."""

    hospital_by_id = {hospital["hospital_id"]: hospital for hospital in hospitals}
    for hospital in hospitals:
        canonical_id = hospital.get("alias_of")
        if canonical_id is None:
            continue
        canonical = hospital_by_id.get(canonical_id)
        if (
            canonical is None
            or canonical is hospital
            or canonical.get("alias_of") is not None
            or canonical["cms_hpt_url"] != hospital["cms_hpt_url"]
        ):
            raise _registry_error("alias_of_invalid")
        hospital.setdefault(
            "locator_name", canonical.get("locator_name") or canonical["name"]
        )
        if "locator_mrf_url" in canonical:
            hospital.setdefault(
                "locator_mrf_url", canonical["locator_mrf_url"]
            )
        if "fallback_mrf_url" in canonical:
            hospital.setdefault(
                "fallback_mrf_url", canonical["fallback_mrf_url"]
            )


def _validated_hospital_entries(entry: Any, hospital_ids: set[str]) -> tuple[dict[str, str], ...]:
    """Expand one catalog entry, grouping shared IDs under its first ID."""

    fields = set(entry) if type(entry) is dict else set()
    if (
        type(entry) is not dict
        or not _HOSPITAL_REQUIRED_FIELDS <= fields
        or len(fields & _HOSPITAL_ID_FIELDS) != 1
        or fields
        - _HOSPITAL_REQUIRED_FIELDS
        - _HOSPITAL_ID_FIELDS
        - _HOSPITAL_OPTIONAL_FIELDS
    ):
        raise _registry_error("hospital_fields")
    entry_ids = (
        [entry["hospital_id"]]
        if "hospital_id" in entry
        else entry["hospital_ids"]
    )
    if type(entry_ids) is not list or not entry_ids:
        raise _registry_error("hospital_ids_invalid")
    hospital_by_field = {
        "name": _strict_text(entry["name"], "name"),
        "cms_hpt_url": _validated_locator(entry["cms_hpt_url"]),
    }
    if "locator_name" in entry:
        hospital_by_field["locator_name"] = _strict_text(
            entry["locator_name"], "locator_name"
        )
    if "locator_mrf_url" in entry:
        hospital_by_field["locator_mrf_url"] = _validated_mrf_selector(
            entry["locator_mrf_url"]
        )
    if "fallback_mrf_url" in entry:
        hospital_by_field["fallback_mrf_url"] = _validated_http_url(
            entry["fallback_mrf_url"], "fallback_mrf_url"
        )
    if "alias_of" in entry:
        hospital_by_field["alias_of"] = _validated_hospital_id(
            entry["alias_of"], "alias_of"
        )
    validated_ids = tuple(map(_validated_hospital_id, entry_ids))
    if len(set(validated_ids)) != len(validated_ids) or hospital_ids.intersection(
        validated_ids
    ):
        raise _registry_error("duplicate_hospital_id")
    hospital_ids.update(validated_ids)
    implicit_canonical_id = (
        validated_ids[0]
        if "hospital_ids" in entry and "alias_of" not in entry
        else None
    )
    expanded_hospitals = []
    for hospital_id in validated_ids:
        expanded_hospital_by_field = {"hospital_id": hospital_id, **hospital_by_field}
        if implicit_canonical_id is not None and hospital_id != implicit_canonical_id:
            expanded_hospital_by_field["alias_of"] = implicit_canonical_id
        expanded_hospitals.append(expanded_hospital_by_field)
    return tuple(expanded_hospitals)


def _load_hospital_hpt_registry_path(
    registry_path: Path,
) -> tuple[dict[str, str], ...]:
    """Load and validate one complete registry document."""

    try:
        document = yaml.load(
            registry_path.read_text(encoding="utf-8"),
            Loader=_UniqueKeyLoader,
        )
    except HospitalHptRegistryError:
        raise
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise _registry_error("document_unreadable") from exc
    if (
        type(document) is not dict
        or set(document) != _DOCUMENT_FIELDS
        or type(document.get("version")) is not int
        or document["version"] != 1
        or type(document.get("hospitals")) is not list
        or not document["hospitals"]
    ):
        raise _registry_error("document_shape")

    hospital_ids: set[str] = set()
    hospitals: list[dict[str, str]] = []
    for entry in document["hospitals"]:
        hospitals.extend(_validated_hospital_entries(entry, hospital_ids))
    _validate_hospital_aliases(hospitals)
    return tuple(hospitals)


@lru_cache(maxsize=1)
def _cached_hospital_hpt_registry() -> tuple[dict[str, str], ...]:
    try:
        registry_sha256 = hashlib.sha256(
            HOSPITAL_HPT_REGISTRY_PATH.read_bytes()
        ).hexdigest()
    except OSError as exc:
        raise _registry_error("document_unreadable") from exc
    if registry_sha256 != EXPECTED_HOSPITAL_HPT_REGISTRY_SHA256:
        raise _registry_error("checksum")
    hospitals = _load_hospital_hpt_registry_path(HOSPITAL_HPT_REGISTRY_PATH)
    if len(hospitals) != EXPECTED_HOSPITAL_HPT_REGISTRY_COUNT:
        raise _registry_error("count")
    return hospitals


def load_hospital_hpt_registry() -> tuple[dict[str, str], ...]:
    """Load the fixed checked-in hospital locator registry."""

    return tuple(dict(hospital) for hospital in _cached_hospital_hpt_registry())


def hospital_hpt_registry_groups() -> tuple[tuple[dict[str, str], ...], ...]:
    """Group reviewed aliases under one canonical hospital identity."""

    hospitals = load_hospital_hpt_registry()
    groups_by_canonical_id: dict[str, list[dict[str, str]]] = {}
    for hospital in hospitals:
        canonical_id = hospital.get("alias_of", hospital["hospital_id"])
        groups_by_canonical_id.setdefault(canonical_id, []).append(hospital)
    return tuple(
        tuple(
            sorted(
                groups_by_canonical_id[canonical_id],
                key=lambda hospital: hospital["hospital_id"] != canonical_id,
            )
        )
        for canonical_id in sorted(groups_by_canonical_id)
    )


@lru_cache(maxsize=1)
def _group_ids_by_hospital_id() -> dict[str, tuple[str, ...]]:
    groups_by_id: dict[str, tuple[str, ...]] = {}
    for hospitals in hospital_hpt_registry_groups():
        group_ids = tuple(hospital["hospital_id"] for hospital in hospitals)
        groups_by_id.update((hospital_id, group_ids) for hospital_id in group_ids)
    return groups_by_id


def hospital_hpt_group_ids(hospital_id: str) -> tuple[str, ...]:
    """Return canonical-first IDs for one reviewed facility group."""

    return _group_ids_by_hospital_id().get(hospital_id, ())


def selected_hospital_hpt_registry(
    params: dict[str, Any], *, runtime: bool = False
) -> tuple[dict[str, str], ...]:
    """Select one, a bounded set, or the complete reviewed registry."""

    allowed = _SELECTION_FIELDS | ({"run_id"} if runtime else set())
    if type(params) is not dict or set(params) - allowed:
        raise _registry_error("selection_fields")
    if params.get("test_mode") not in (None, False):
        raise _registry_error("live_test_mode")
    hospital_id = str(params.get("hospital_id") or "").strip()
    hospital_ids_value = params.get("hospital_ids")
    includes_all_hospitals = params.get("all_hospitals") is True
    scope_count = sum(
        (bool(hospital_id), hospital_ids_value is not None, includes_all_hospitals)
    )
    if scope_count != 1:
        raise _registry_error("selection_scope")
    hospitals = load_hospital_hpt_registry()
    if includes_all_hospitals:
        return hospitals
    if hospital_ids_value is not None:
        if (
            type(hospital_ids_value) is not list
            or not hospital_ids_value
            or len(hospital_ids_value) > MAX_HOSPITAL_HPT_SELECTION
        ):
            raise _registry_error("hospital_ids_invalid")
        hospital_ids = tuple(
            dict.fromkeys(
                _strict_text(hospital_id_value, "hospital_id")
                for hospital_id_value in hospital_ids_value
            )
        )
    else:
        hospital_ids = (hospital_id,)
    hospital_by_id = {hospital["hospital_id"]: hospital for hospital in hospitals}
    if any(hospital_id not in hospital_by_id for hospital_id in hospital_ids):
        raise _registry_error("hospital_id_unknown")
    selected_canonical_ids = {
        hospital_by_id[hospital_id].get("alias_of", hospital_id)
        for hospital_id in hospital_ids
    }
    selected_hospitals = tuple(
        hospital
        for hospital in hospitals
        if hospital.get("alias_of", hospital["hospital_id"])
        in selected_canonical_ids
    )
    if len(selected_hospitals) > MAX_HOSPITAL_HPT_SELECTION:
        raise _registry_error("hospital_ids_invalid")
    return selected_hospitals
