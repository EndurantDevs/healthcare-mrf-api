# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable UHC IFP and Community & State drug-file catalog contract."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import quote, unquote, urljoin, urlsplit

from yarl import URL

from process.uhc_provider_file_catalog_artifacts import (
    validate_retained_catalog_payloads,
)
from process.uhc_provider_file_catalog_contract import (
    CATALOG_BASE_URL,
    CATALOG_URLS,
    MAX_RAW_ENTRIES_PER_COLLECTION,
    UHCFileCatalogError,
    UHCFileCatalogItem,
    _SAFE_BASENAME,
    _size_bytes,
    _timestamp,
    canonical_json,
    sha256_text,
    trusted_public_https_url,
)


DRUG_FORMULARY = "drug_formulary"
DRUG_CATALOG_CONTRACT = "healthporta-uhc-drug-file-catalog-v1"
DRUG_RAW_PROJECTION_CONTRACT = "healthporta-uhc-drug-listing-projection-v1"
DRUG_CATALOG_ACQUISITION_CONTRACT = "healthporta-uhc-drug-catalog-acquisition-v1"
EXPECTED_DRUG_FILE_COUNTS = {"cs": 24, "ifp": 24}


@dataclass(frozen=True)
class UHCObservedDrugCatalog:
    """Bind the 48 drug files to their retained two-listing observation."""

    files: tuple[UHCFileCatalogItem, ...]
    drug_set_sha256: str
    raw_listing_projection_sha256: str
    source_raw_set_sha256: str
    acquisition_contract_sha256: str
    collection_summary: tuple[dict[str, Any], ...]


def _canonical_drug_source_url(value: object) -> str:
    """Reject transport-equivalent spellings before identity hashing."""

    source_url = trusted_public_https_url(value)
    decoded_path = unquote(urlsplit(source_url).path)
    if any(segment in {".", ".."} for segment in decoded_path.split("/")):
        raise UHCFileCatalogError("UHC drug catalog source URL contains a dot segment")
    try:
        canonical_url = str(URL(source_url))
    except (TypeError, ValueError):
        raise UHCFileCatalogError("UHC drug catalog source URL is invalid") from None
    if canonical_url != source_url:
        raise UHCFileCatalogError("UHC drug catalog source URL is not canonical")
    return source_url


def _drug_source_url(
    family: str,
    file_name: str,
    entry_by_field: dict[str, Any],
) -> str:
    is_external = entry_by_field.get("isExternal", False)
    if type(is_external) is not bool:
        raise UHCFileCatalogError("UHC drug catalog external marker is invalid")
    if is_external:
        source_url = _canonical_drug_source_url(entry_by_field.get("url"))
        if unquote(urlsplit(source_url).path.rsplit("/", 1)[-1]) != file_name:
            raise UHCFileCatalogError(
                "UHC drug external URL does not match its basename"
            )
        return source_url
    blob_path = str(entry_by_field.get("blobPath") or "").strip().lstrip("/")
    expected_blob_path = f"ui/{family}/drugs/{file_name}"
    if blob_path != expected_blob_path or ".." in blob_path.split("/"):
        raise UHCFileCatalogError(
            "UHC drug catalog blob path does not match its collection"
        )
    return _canonical_drug_source_url(
        urljoin(CATALOG_BASE_URL, f"/api/stream/{quote(blob_path, safe='/')}")
    )


def _catalog_file_from_entry(
    family: str,
    entry_by_field: dict[str, Any],
) -> UHCFileCatalogItem | None:
    file_name = str(entry_by_field.get("name") or "").strip()
    if not file_name.endswith(".json"):
        return None
    if not _SAFE_BASENAME.fullmatch(file_name) or not file_name.isprintable():
        raise UHCFileCatalogError("UHC drug catalog basename is unsafe")
    catalog_modified_at = _timestamp(entry_by_field.get("date"))
    size_bytes = _size_bytes(entry_by_field)
    if size_bytes is not None and size_bytes <= 0:
        raise UHCFileCatalogError("UHC drug catalog byte count is invalid")
    source_url = _drug_source_url(family, file_name, entry_by_field)
    identity_by_field = {
        "contract": DRUG_CATALOG_CONTRACT,
        "family": family,
        "collection_kind": DRUG_FORMULARY,
        "file_name": file_name,
        "source_url": source_url,
        "catalog_modified_at": catalog_modified_at,
        "size_bytes": size_bytes,
    }
    catalog_entry_sha256 = sha256_text(canonical_json(identity_by_field))
    file_id = sha256_text(
        canonical_json({"domain": "catalog-file-id", "source": identity_by_field})
    )
    return UHCFileCatalogItem(
        family=family,
        collection_kind=DRUG_FORMULARY,
        file_id=file_id,
        file_name=file_name,
        source_url=source_url,
        catalog_modified_at=catalog_modified_at,
        catalog_entry_sha256=catalog_entry_sha256,
        size_bytes=size_bytes,
    )


def _catalog_files_from_payload(
    family: str,
    family_payload: Any,
) -> tuple[UHCFileCatalogItem, ...]:
    if type(family_payload) is not dict:
        raise UHCFileCatalogError(f"UHC {family} drug catalog is invalid")
    entries = family_payload.get("drugs")
    if type(entries) is not list:
        raise UHCFileCatalogError(f"UHC {family} drug collection is invalid")
    if not entries or len(entries) > MAX_RAW_ENTRIES_PER_COLLECTION:
        raise UHCFileCatalogError(f"UHC {family} drug collection count is invalid")
    files: list[UHCFileCatalogItem] = []
    logical_names: set[str] = set()
    for entry_by_field in entries:
        if type(entry_by_field) is not dict:
            raise UHCFileCatalogError("UHC drug catalog entry is not an object")
        catalog_file = _catalog_file_from_entry(family, entry_by_field)
        if catalog_file is None:
            continue
        if catalog_file.file_name in logical_names:
            raise UHCFileCatalogError("UHC drug catalog contains a duplicate basename")
        logical_names.add(catalog_file.file_name)
        files.append(catalog_file)
    if len(files) != EXPECTED_DRUG_FILE_COUNTS[family]:
        raise UHCFileCatalogError(f"UHC {family} drug collection count is invalid")
    return tuple(sorted(files, key=lambda catalog_file: catalog_file.file_name))


def _drug_set_sha256(
    files: Iterable[UHCFileCatalogItem],
    collection_summary: tuple[dict[str, Any], ...],
) -> str:
    return sha256_text(
        canonical_json(
            {
                "collections": collection_summary,
                "contract": DRUG_CATALOG_CONTRACT,
                "files": [catalog_file.identity_payload() for catalog_file in files],
            }
        )
    )


def _validate_drug_file(catalog_file: UHCFileCatalogItem) -> None:
    if (
        type(catalog_file) is not UHCFileCatalogItem
        or catalog_file.family not in CATALOG_URLS
        or catalog_file.collection_kind != DRUG_FORMULARY
        or not _SAFE_BASENAME.fullmatch(catalog_file.file_name)
        or not catalog_file.file_name.isprintable()
    ):
        raise UHCFileCatalogError("UHC drug catalog file identity is invalid")
    source_url = _canonical_drug_source_url(catalog_file.source_url)
    source_path = unquote(urlsplit(source_url).path)
    is_expected_internal = source_path == (
        f"/api/stream/ui/{catalog_file.family}/drugs/" f"{catalog_file.file_name}"
    )
    if not is_expected_internal and source_path.rsplit("/", 1)[-1] != (
        catalog_file.file_name
    ):
        raise UHCFileCatalogError("UHC drug catalog source URL is invalid")
    identity_by_field = {
        "contract": DRUG_CATALOG_CONTRACT,
        "family": catalog_file.family,
        "collection_kind": DRUG_FORMULARY,
        "file_name": catalog_file.file_name,
        "source_url": source_url,
        "catalog_modified_at": _timestamp(catalog_file.catalog_modified_at),
        "size_bytes": catalog_file.size_bytes,
    }
    expected_entry_sha256 = sha256_text(canonical_json(identity_by_field))
    expected_file_id = sha256_text(
        canonical_json({"domain": "catalog-file-id", "source": identity_by_field})
    )
    if (
        catalog_file.catalog_entry_sha256 != expected_entry_sha256
        or catalog_file.file_id != expected_file_id
    ):
        raise UHCFileCatalogError("UHC drug catalog file identity is inconsistent")


def _require_unique_source_urls(
    files: tuple[UHCFileCatalogItem, ...],
) -> None:
    normalized_source_coordinates = []
    for catalog_file in files:
        source_url = _canonical_drug_source_url(catalog_file.source_url)
        parsed_source = urlsplit(source_url)
        decoded_path = unquote(parsed_source.path)
        normalized_source_coordinates.append(
            (
                (parsed_source.hostname or "").lower(),
                parsed_source.port or 443,
                decoded_path,
            )
        )
    if len(set(normalized_source_coordinates)) != len(normalized_source_coordinates):
        raise UHCFileCatalogError(
            "UHC drug catalog reuses one source URL across file identities"
        )


def _acquisition_contract_sha256(
    drug_set_sha256: str,
    raw_listing_projection_sha256: str,
) -> str:
    return sha256_text(
        canonical_json(
            {
                "contract": DRUG_CATALOG_ACQUISITION_CONTRACT,
                "drug_set_sha256": drug_set_sha256,
                "raw_listing_projection_sha256": (raw_listing_projection_sha256),
            }
        )
    )


def validate_observed_drug_catalog(catalog: UHCObservedDrugCatalog) -> None:
    """Recompute the immutable 48-file drug catalog boundary."""

    if type(catalog) is not UHCObservedDrugCatalog:
        raise UHCFileCatalogError("UHC observed drug catalog has an invalid type")
    if len(catalog.files) != sum(EXPECTED_DRUG_FILE_COUNTS.values()):
        raise UHCFileCatalogError("UHC drug catalog file count is invalid")
    for catalog_file in catalog.files:
        _validate_drug_file(catalog_file)
    if len({catalog_file.file_id for catalog_file in catalog.files}) != len(
        catalog.files
    ):
        raise UHCFileCatalogError("UHC drug catalog file identity collision")
    _require_unique_source_urls(catalog.files)
    expected_collection_summaries = tuple(
        {
            "availability": "published",
            "catalog_support": "cataloged",
            "collection_kind": DRUG_FORMULARY,
            "family": family,
            "file_count": EXPECTED_DRUG_FILE_COUNTS[family],
        }
        for family in sorted(EXPECTED_DRUG_FILE_COUNTS)
    )
    if catalog.collection_summary != expected_collection_summaries:
        raise UHCFileCatalogError("UHC drug collection summary is inconsistent")
    for digest_value in (
        catalog.raw_listing_projection_sha256,
        catalog.source_raw_set_sha256,
    ):
        if (
            type(digest_value) is not str
            or len(digest_value) != 64
            or any(character not in "0123456789abcdef" for character in digest_value)
        ):
            raise UHCFileCatalogError("UHC drug catalog proof is invalid")
    expected_set_sha256 = _drug_set_sha256(
        catalog.files,
        catalog.collection_summary,
    )
    expected_acquisition_sha256 = _acquisition_contract_sha256(
        expected_set_sha256,
        catalog.raw_listing_projection_sha256,
    )
    if (
        catalog.drug_set_sha256 != expected_set_sha256
        or catalog.acquisition_contract_sha256 != expected_acquisition_sha256
    ):
        raise UHCFileCatalogError("UHC drug catalog hashes are inconsistent")


def _raw_listing_projection_sha256(
    payloads_by_family: dict[str, Any],
) -> str:
    projection_by_field = {
        "collections": [
            {
                "entries": payloads_by_family[family]["drugs"],
                "family": family,
            }
            for family in sorted(CATALOG_URLS)
        ],
        "contract": DRUG_RAW_PROJECTION_CONTRACT,
    }
    try:
        return sha256_text(canonical_json(projection_by_field))
    except (TypeError, ValueError):
        raise UHCFileCatalogError("UHC drug listing projection is invalid") from None


def observed_drug_catalog_from_payloads(
    payloads_by_family: dict[str, Any],
    *,
    source_raw_set_sha256: str,
) -> UHCObservedDrugCatalog:
    """Derive one exact drug set from already acquired IFP and CS listings."""

    if set(payloads_by_family) != set(CATALOG_URLS):
        raise UHCFileCatalogError("UHC drug catalog family set is incomplete")
    if (
        type(source_raw_set_sha256) is not str
        or len(source_raw_set_sha256) != 64
        or any(
            character not in "0123456789abcdef" for character in source_raw_set_sha256
        )
    ):
        raise UHCFileCatalogError("UHC drug source catalog proof is invalid")
    files_by_family = {
        family: _catalog_files_from_payload(family, payloads_by_family[family])
        for family in sorted(CATALOG_URLS)
    }
    files = tuple(
        catalog_file
        for family in sorted(files_by_family)
        for catalog_file in files_by_family[family]
    )
    if len({catalog_file.file_id for catalog_file in files}) != len(files):
        raise UHCFileCatalogError("UHC drug catalog file identity collision")
    _require_unique_source_urls(files)
    collection_summaries = tuple(
        {
            "availability": "published",
            "catalog_support": "cataloged",
            "collection_kind": DRUG_FORMULARY,
            "family": family,
            "file_count": len(files_by_family[family]),
        }
        for family in sorted(files_by_family)
    )
    drug_set_sha256 = _drug_set_sha256(files, collection_summaries)
    raw_listing_projection_sha256 = _raw_listing_projection_sha256(payloads_by_family)
    return UHCObservedDrugCatalog(
        files=files,
        drug_set_sha256=drug_set_sha256,
        raw_listing_projection_sha256=raw_listing_projection_sha256,
        source_raw_set_sha256=source_raw_set_sha256,
        acquisition_contract_sha256=_acquisition_contract_sha256(
            drug_set_sha256,
            raw_listing_projection_sha256,
        ),
        collection_summary=collection_summaries,
    )


def validate_retained_drug_catalog_proof(
    raw_proof: Any,
) -> tuple[dict[str, Any], UHCObservedDrugCatalog]:
    """Rehash the retained listings before deriving their drug-file set."""

    normalized_proof, payloads_by_family = validate_retained_catalog_payloads(raw_proof)
    catalog = observed_drug_catalog_from_payloads(
        payloads_by_family,
        source_raw_set_sha256=normalized_proof["raw_set_sha256"],
    )
    validate_observed_drug_catalog(catalog)
    return normalized_proof, catalog


__all__ = (
    "DRUG_CATALOG_ACQUISITION_CONTRACT",
    "DRUG_CATALOG_CONTRACT",
    "DRUG_FORMULARY",
    "DRUG_RAW_PROJECTION_CONTRACT",
    "EXPECTED_DRUG_FILE_COUNTS",
    "UHCObservedDrugCatalog",
    "observed_drug_catalog_from_payloads",
    "validate_observed_drug_catalog",
    "validate_retained_drug_catalog_proof",
)
