# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-neutral registry identity for UHC's corporate official files."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit


SOURCE_REGISTRY_CONTRACT = "healthporta-provider-directory-source-neutral-registry-v1"
UHC_PROVIDER_FILE_ENTRY_ID = "uhc-provider-files"
UHC_PROVIDER_FILE_DISPLAY_NAME = "UnitedHealthcare Official Provider Files"
UHC_PROVIDER_FILE_OWNER_ID = "unitedhealthcare"
UHC_PROVIDER_FILE_CATALOG_SOURCE_ID = "uhc_provider_files"
UHC_PROVIDER_FILE_ADAPTER_ID = "uhc-provider-file-catalog"
UHC_PROVIDER_FILE_ADAPTER_CONTRACT = "healthporta-uhc-provider-file-catalog-v1"
UHC_PROVIDER_FILE_PORTAL_BASE = "https://www.uhc.com/legal/interoperability-apis"
UHC_PROVIDER_FILE_CATALOG_BASE = "https://providermrf.uhc.com"
UHC_PROVIDER_FILE_ENABLING_GATE = (
    "Register a corporate official-file source without FHIR endpoint semantics, "
    "establish authoritative product jurisdiction metadata, then prove a complete "
    "reproducible build and reviewed publication contract before enabling acquisition "
    "or Profile selection."
)

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SOURCE_NEUTRAL_REGISTRY_PATH = (
    _REPOSITORY_ROOT / "specs/provider_directory_source_neutral_registry.json"
)


class UHCProviderFileIdentityError(ValueError):
    """Raised when an official-file identity is incomplete or ambiguous."""


@dataclass(frozen=True)
class UHCOfficialFileSourceIdentity:
    entry_id: str
    display_name: str
    owner_id: str
    source_kind: str
    portal_base: str
    catalog_base: str
    adapter_id: str
    adapter_contract: str
    registered_source_id: None
    acquisition_manifest_entry_id: None
    acquisition_runnable: bool
    profile_eligible: bool
    publication_ready: bool
    enabling_gate: str


def _read_json_document(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise UHCProviderFileIdentityError(
            f"invalid identity document: {path.name}"
        ) from error
    if not isinstance(document, dict):
        raise UHCProviderFileIdentityError(f"invalid identity document: {path.name}")
    return document


def _exact_fields(value: Mapping[str, Any], fields: set[str], label: str) -> None:
    if set(value) != fields:
        raise UHCProviderFileIdentityError(f"{label} fields are invalid")


def _canonical_https_base(value: Any, expected: str, label: str) -> str:
    if value != expected or not isinstance(value, str):
        raise UHCProviderFileIdentityError(
            f"{label} is not the reviewed HTTPS base"
        )
    parsed_url = urlsplit(value)
    canonical_url = urlunsplit(
        (parsed_url.scheme, parsed_url.netloc, parsed_url.path, "", "")
    )
    if (
        parsed_url.scheme != "https"
        or not parsed_url.hostname
        or parsed_url.username
        or parsed_url.password
        or parsed_url.query
        or parsed_url.fragment
        or canonical_url != value
    ):
        raise UHCProviderFileIdentityError(
            f"{label} is not the reviewed HTTPS base"
        )
    return value


def source_identity_from_registry() -> UHCOfficialFileSourceIdentity:
    """Validate and return the source-neutral UHC corporate file identity."""

    return _source_identity_from_registry_path(SOURCE_NEUTRAL_REGISTRY_PATH)


def _source_identity_from_registry_path(
    registry_path: Path,
) -> UHCOfficialFileSourceIdentity:
    """Load a testable path only when every registry identity field is exact."""

    registry = _read_json_document(registry_path)
    _exact_fields(registry, {"schema_version", "contract", "entries"}, "registry")
    if (
        registry["schema_version"] != 1
        or registry["contract"] != SOURCE_REGISTRY_CONTRACT
        or not isinstance(registry["entries"], list)
        or len(registry["entries"]) != 1
        or not isinstance(registry["entries"][0], dict)
    ):
        raise UHCProviderFileIdentityError("source-neutral registry is invalid")
    entry = registry["entries"][0]
    _validate_source_registry_entry(entry)
    return UHCOfficialFileSourceIdentity(**entry)


def _validate_source_registry_entry(entry: dict[str, Any]) -> None:
    fields = {
        "entry_id",
        "display_name",
        "owner_id",
        "source_kind",
        "portal_base",
        "catalog_base",
        "adapter_id",
        "adapter_contract",
        "registered_source_id",
        "acquisition_manifest_entry_id",
        "acquisition_runnable",
        "profile_eligible",
        "publication_ready",
        "enabling_gate",
    }
    _exact_fields(entry, fields, "source-neutral registry entry")
    expected_text_by_field = {
        "entry_id": UHC_PROVIDER_FILE_ENTRY_ID,
        "display_name": UHC_PROVIDER_FILE_DISPLAY_NAME,
        "owner_id": UHC_PROVIDER_FILE_OWNER_ID,
        "source_kind": "official_provider_files",
        "adapter_id": UHC_PROVIDER_FILE_ADAPTER_ID,
        "adapter_contract": UHC_PROVIDER_FILE_ADAPTER_CONTRACT,
    }
    if any(
        entry[field] != expected
        for field, expected in expected_text_by_field.items()
    ):
        raise UHCProviderFileIdentityError("source-neutral UHC identity is invalid")
    _canonical_https_base(
        entry["portal_base"],
        UHC_PROVIDER_FILE_PORTAL_BASE,
        "portal_base",
    )
    _canonical_https_base(
        entry["catalog_base"],
        UHC_PROVIDER_FILE_CATALOG_BASE,
        "catalog_base",
    )
    if (
        entry["registered_source_id"] is not None
        or entry["acquisition_manifest_entry_id"] is not None
        or entry["acquisition_runnable"] is not False
        or entry["profile_eligible"] is not False
        or entry["publication_ready"] is not False
        or entry["enabling_gate"] != UHC_PROVIDER_FILE_ENABLING_GATE
    ):
        raise UHCProviderFileIdentityError("source-neutral UHC gates are invalid")


__all__ = [
    "SOURCE_NEUTRAL_REGISTRY_PATH",
    "UHCOfficialFileSourceIdentity",
    "UHCProviderFileIdentityError",
    "UHC_PROVIDER_FILE_ADAPTER_ID",
    "UHC_PROVIDER_FILE_CATALOG_SOURCE_ID",
    "UHC_PROVIDER_FILE_DISPLAY_NAME",
    "UHC_PROVIDER_FILE_ENABLING_GATE",
    "UHC_PROVIDER_FILE_ENTRY_ID",
    "UHC_PROVIDER_FILE_OWNER_ID",
    "UHC_PROVIDER_FILE_PORTAL_BASE",
    "source_identity_from_registry",
]
