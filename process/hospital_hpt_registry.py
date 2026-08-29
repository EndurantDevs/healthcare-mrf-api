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
EXPECTED_HOSPITAL_HPT_REGISTRY_COUNT = 7_314
EXPECTED_HOSPITAL_HPT_REGISTRY_SHA256 = (
    "5dba2c4cdc134870ead0a46b10a2d6c69604a20a68c9ce04f2d3f1fb6bb415c7"
)
MAX_HOSPITAL_HPT_SELECTION = 200
_DOCUMENT_FIELDS = frozenset({"version", "hospitals"})
_HOSPITAL_REQUIRED_FIELDS = frozenset({"hospital_id", "name", "cms_hpt_url"})
_HOSPITAL_OPTIONAL_FIELDS = frozenset(
    {"fallback_mrf_url", "locator_name", "locator_mrf_url"}
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


def _load_hospital_hpt_registry_path(
    registry_path: Path,
) -> tuple[dict[str, str], ...]:
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
        fields = set(entry) if type(entry) is dict else set()
        if (
            type(entry) is not dict
            or not _HOSPITAL_REQUIRED_FIELDS <= fields
            or fields - _HOSPITAL_REQUIRED_FIELDS - _HOSPITAL_OPTIONAL_FIELDS
        ):
            raise _registry_error("hospital_fields")
        hospital_id = _strict_text(entry["hospital_id"], "hospital_id")
        if any(character.isspace() for character in hospital_id):
            raise _registry_error("hospital_id_invalid")
        if hospital_id in hospital_ids:
            raise _registry_error("duplicate_hospital_id")
        hospital_ids.add(hospital_id)
        hospital_by_field = {
            "hospital_id": hospital_id,
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
        hospitals.append(hospital_by_field)
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
    selected_id_set = set(hospital_ids)
    selected_hospitals = tuple(
        hospital
        for hospital in hospitals
        if hospital["hospital_id"] in selected_id_set
    )
    if len(selected_hospitals) != len(selected_id_set):
        raise _registry_error("hospital_id_unknown")
    return selected_hospitals
