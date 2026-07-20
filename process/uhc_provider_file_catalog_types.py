# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Validated identities for UHC's public provider-file catalogs."""

from __future__ import annotations

from typing import Any, Iterable

from process.uhc_provider_file_catalog_contract import (
    CATALOG_BASE_URL,
    CATALOG_CONTRACT,
    CATALOG_CONTRACT_LIMITS,
    CATALOG_URLS,
    MAX_FILE_NAME_LENGTH,
    MAX_RAW_ENTRIES_PER_COLLECTION,
    MAX_RAW_ENTRIES_TOTAL,
    MAX_SIZE_BYTES,
    PLAN_REFERENCE,
    PROVIDER_MEMBERSHIP,
    TRUSTED_HOSTS,
    UHCFileCatalogError,
    UHCFileCatalogItem,
    UHCFileCatalogNotFound,
    UHCObservedFileCatalog,
    _catalog_file,
    _COLLECTION_KINDS,
    _SAFE_BASENAME,
    _size_bytes,
    _source_url,
    _timestamp,
    canonical_json,
    sha256_text,
    trusted_public_https_url,
)


def catalog_set_sha256(
    files: Iterable[UHCFileCatalogItem],
    collection_summary: Iterable[dict[str, Any]],
) -> str:
    """Hash only source-derived catalog semantics in deterministic order."""

    ordered_files = sorted(
        files,
        key=lambda catalog_file: (
            catalog_file.family,
            catalog_file.collection_kind,
            catalog_file.file_name,
            catalog_file.file_id,
        ),
    )
    remote_summaries = [
        {
            field_name: field_value
            for field_name, field_value in collection.items()
            if field_name != "catalog_support"
        }
        for collection in sorted(
            collection_summary,
            key=lambda collection: (
                collection["family"],
                collection["collection_kind"],
            ),
        )
    ]
    return sha256_text(
        canonical_json(
            {
                "contract": CATALOG_CONTRACT,
                "collections": remote_summaries,
                "files": [catalog_file.identity_payload() for catalog_file in ordered_files],
            }
        )
    )


def _summary_by_collection(
    catalog: UHCObservedFileCatalog,
) -> dict[tuple[str, str], dict[str, Any]]:
    summary_by_collection: dict[tuple[str, str], dict[str, Any]] = {}
    for source_collection in catalog.collection_summary:
        if not isinstance(source_collection, dict):
            raise UHCFileCatalogError("UHC collection summary is invalid")
        family = source_collection.get("family")
        collection_kind = source_collection.get("collection_kind")
        collection_key = (family, collection_kind)
        if (
            family not in CATALOG_URLS
            or collection_kind not in _COLLECTION_KINDS
            or collection_key in summary_by_collection
            or isinstance(source_collection.get("file_count"), bool)
            or not isinstance(source_collection.get("file_count"), int)
            or source_collection["file_count"] < 0
        ):
            raise UHCFileCatalogError("UHC collection summary is invalid")
        if source_collection["file_count"]:
            expected_availability = "published"
            expected_support = "cataloged"
        elif collection_key == ("cs", PLAN_REFERENCE):
            expected_availability = "not_published_by_source"
            expected_support = "not_applicable"
        else:
            raise UHCFileCatalogError("UHC required catalog collection is empty")
        if (
            source_collection.get("availability") != expected_availability
            or source_collection.get("catalog_support") != expected_support
        ):
            raise UHCFileCatalogError("UHC collection support is inconsistent")
        summary_by_collection[collection_key] = source_collection
    return summary_by_collection


def _validate_catalog_file_identity(catalog_file: UHCFileCatalogItem) -> None:
    if (
        not isinstance(catalog_file, UHCFileCatalogItem)
        or catalog_file.family not in CATALOG_URLS
        or catalog_file.collection_kind not in _COLLECTION_KINDS
        or not _SAFE_BASENAME.fullmatch(catalog_file.file_name)
        or not catalog_file.file_name.isprintable()
        or isinstance(catalog_file.size_bytes, bool)
        or (
            catalog_file.size_bytes is not None
            and (
                not isinstance(catalog_file.size_bytes, int)
                or catalog_file.size_bytes < 0
                or catalog_file.size_bytes > MAX_SIZE_BYTES
            )
        )
    ):
        raise UHCFileCatalogError("UHC catalog file identity is invalid")
    trusted_public_https_url(catalog_file.source_url)
    if _timestamp(catalog_file.catalog_modified_at) != catalog_file.catalog_modified_at:
        raise UHCFileCatalogError("UHC catalog timestamp is not canonical")


def _expected_file_hashes(catalog_file: UHCFileCatalogItem) -> tuple[str, str]:
    source_identity_by_field = {
        "contract": CATALOG_CONTRACT,
        "family": catalog_file.family,
        "collection_kind": catalog_file.collection_kind,
        "file_name": catalog_file.file_name,
        "source_url": catalog_file.source_url,
        "catalog_modified_at": catalog_file.catalog_modified_at,
        "size_bytes": catalog_file.size_bytes,
    }
    catalog_entry_hash = sha256_text(canonical_json(source_identity_by_field))
    file_id = sha256_text(
        canonical_json(
            {"domain": "catalog-file-id", "source": source_identity_by_field}
        )
    )
    return catalog_entry_hash, file_id


def _validate_catalog_files(
    catalog: UHCObservedFileCatalog,
    expected_collection_keys: set[tuple[str, str]],
) -> dict[tuple[str, str], int]:
    logical_names: set[tuple[str, str, str]] = set()
    file_ids: set[str] = set()
    file_count_by_collection = dict.fromkeys(expected_collection_keys, 0)
    for catalog_file in catalog.files:
        _validate_catalog_file_identity(catalog_file)
        expected_entry_hash, expected_file_id = _expected_file_hashes(catalog_file)
        logical_name = (
            catalog_file.family,
            catalog_file.collection_kind,
            catalog_file.file_name,
        )
        if (
            catalog_file.catalog_entry_sha256 != expected_entry_hash
            or catalog_file.file_id != expected_file_id
            or logical_name in logical_names
            or catalog_file.file_id in file_ids
        ):
            raise UHCFileCatalogError("UHC catalog file identity is inconsistent")
        logical_names.add(logical_name)
        file_ids.add(catalog_file.file_id)
        file_count_by_collection[
            (catalog_file.family, catalog_file.collection_kind)
        ] += 1
    return file_count_by_collection


def validate_observed_catalog(catalog: UHCObservedFileCatalog) -> None:
    """Recompute every semantic identity at the persistence boundary."""

    if not isinstance(catalog, UHCObservedFileCatalog):
        raise UHCFileCatalogError("UHC observed catalog has an invalid type")
    summary_by_collection = _summary_by_collection(catalog)
    expected_keys = {
        (family, collection_kind)
        for family in CATALOG_URLS
        for collection_kind in _COLLECTION_KINDS
    }
    if set(summary_by_collection) != expected_keys:
        raise UHCFileCatalogError("UHC catalog collection set is incomplete")
    file_count_by_collection = _validate_catalog_files(catalog, expected_keys)
    if any(
        summary_by_collection[collection_key]["file_count"] != file_count
        for collection_key, file_count in file_count_by_collection.items()
    ):
        raise UHCFileCatalogError("UHC catalog collection counts are inconsistent")
    expected_set_hash = catalog_set_sha256(catalog.files, catalog.collection_summary)
    if catalog.catalog_set_sha256 != expected_set_hash:
        raise UHCFileCatalogError("UHC semantic catalog hash is inconsistent")


def _collection_files(
    family: str,
    collection_kind: str,
    catalog_entries: list[Any],
    logical_names: set[tuple[str, str, str]],
) -> list[UHCFileCatalogItem]:
    collection_files: list[UHCFileCatalogItem] = []
    for catalog_entry in catalog_entries:
        if not isinstance(catalog_entry, dict):
            raise UHCFileCatalogError("UHC catalog entry is not an object")
        catalog_file = _catalog_file(family, collection_kind, catalog_entry)
        if catalog_file is None:
            continue
        logical_name = (family, collection_kind, catalog_file.file_name)
        if logical_name in logical_names:
            raise UHCFileCatalogError("UHC catalog contains a duplicate basename")
        logical_names.add(logical_name)
        collection_files.append(catalog_file)
    return collection_files


def _catalog_collection(
    family: str,
    section: str,
    collection_kind: str,
    family_payload: dict[str, Any],
    logical_names: set[tuple[str, str, str]],
) -> tuple[list[UHCFileCatalogItem], dict[str, Any]]:
    catalog_entries = family_payload.get(section)
    if family == "cs" and section == "plans" and catalog_entries in (None, []):
        catalog_entries = []
    if not isinstance(catalog_entries, list):
        raise UHCFileCatalogError(f"UHC {family} {section} collection is invalid")
    if len(catalog_entries) > MAX_RAW_ENTRIES_PER_COLLECTION:
        raise UHCFileCatalogError(
            f"UHC {family} {section} collection exceeds its entry bound"
        )
    collection_files = _collection_files(
        family,
        collection_kind,
        catalog_entries,
        logical_names,
    )
    if collection_kind == PROVIDER_MEMBERSHIP and not collection_files:
        raise UHCFileCatalogError(f"UHC {family} has no provider JSON files")
    if family == "ifp" and collection_kind == PLAN_REFERENCE and not collection_files:
        raise UHCFileCatalogError("UHC ifp has no plan-reference JSON files")
    has_published_files = bool(collection_files)
    return collection_files, {
        "family": family,
        "collection_kind": collection_kind,
        "availability": (
            "published" if has_published_files else "not_published_by_source"
        ),
        "catalog_support": "cataloged" if has_published_files else "not_applicable",
        "file_count": len(collection_files),
    }


def _validate_raw_entry_limits(payloads_by_family: dict[str, Any]) -> None:
    """Bound listing expansion before parsing any entry objects."""

    total_entry_count = 0
    for family in sorted(CATALOG_URLS):
        family_payload = payloads_by_family.get(family)
        if not isinstance(family_payload, dict):
            raise UHCFileCatalogError(f"UHC {family} catalog is invalid")
        for section in ("providers", "plans"):
            catalog_entries = family_payload.get(section)
            if family == "cs" and section == "plans" and catalog_entries in (None, []):
                catalog_entries = []
            if not isinstance(catalog_entries, list):
                raise UHCFileCatalogError(
                    f"UHC {family} {section} collection is invalid"
                )
            if len(catalog_entries) > MAX_RAW_ENTRIES_PER_COLLECTION:
                raise UHCFileCatalogError(
                    f"UHC {family} {section} collection exceeds its entry bound"
                )
            total_entry_count += len(catalog_entries)
            if total_entry_count > MAX_RAW_ENTRIES_TOTAL:
                raise UHCFileCatalogError("UHC catalog exceeds its total entry bound")


def observed_catalog_from_payloads(
    payloads_by_family: dict[str, Any],
) -> UHCObservedFileCatalog:
    """Parse both listing payloads into one validated semantic catalog."""

    _validate_raw_entry_limits(payloads_by_family)
    files: list[UHCFileCatalogItem] = []
    collection_summaries: list[dict[str, Any]] = []
    logical_names: set[tuple[str, str, str]] = set()
    for family in sorted(CATALOG_URLS):
        family_payload = payloads_by_family.get(family)
        if not isinstance(family_payload, dict):
            raise UHCFileCatalogError(f"UHC {family} catalog is invalid")
        for section, collection_kind in (
            ("providers", PROVIDER_MEMBERSHIP),
            ("plans", PLAN_REFERENCE),
        ):
            collection_files, collection_summary = _catalog_collection(
                family,
                section,
                collection_kind,
                family_payload,
                logical_names,
            )
            files.extend(collection_files)
            collection_summaries.append(collection_summary)
    ordered_files = tuple(
        sorted(
            files,
            key=lambda catalog_file: (
                catalog_file.family,
                catalog_file.collection_kind,
                catalog_file.file_name,
            ),
        )
    )
    if len({catalog_file.file_id for catalog_file in ordered_files}) != len(
        ordered_files
    ):
        raise UHCFileCatalogError("UHC catalog file identity collision")
    ordered_summaries = tuple(
        sorted(
            collection_summaries,
            key=lambda collection: (
                collection["family"],
                collection["collection_kind"],
            ),
        )
    )
    catalog = UHCObservedFileCatalog(
        files=ordered_files,
        catalog_set_sha256=catalog_set_sha256(ordered_files, ordered_summaries),
        collection_summary=ordered_summaries,
    )
    validate_observed_catalog(catalog)
    return catalog
