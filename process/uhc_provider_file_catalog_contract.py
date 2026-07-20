# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded URL and file-entry contract for UHC catalog listings."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, unquote, urljoin, urlsplit, urlunsplit


CATALOG_BASE_URL = "https://providermrf.uhc.com"
CATALOG_URLS = {
    "ifp": f"{CATALOG_BASE_URL}/api/files/ui/ifp/",
    "cs": f"{CATALOG_BASE_URL}/api/files/ui/cs/",
}
TRUSTED_HOSTS = frozenset(
    {"providermrf.uhc.com", "legacy.providerlookuponline.com"}
)
CATALOG_CONTRACT = "healthporta-uhc-provider-file-catalog-v1"
PROVIDER_MEMBERSHIP = "provider_membership"
PLAN_REFERENCE = "plan_reference"
MAX_RAW_ENTRIES_PER_COLLECTION = 4_096
MAX_RAW_ENTRIES_TOTAL = 8_192
MAX_FILE_NAME_LENGTH = 256
MAX_SIZE_BYTES = (1 << 63) - 1
CATALOG_CONTRACT_LIMITS = {
    "max_raw_entries_per_collection": MAX_RAW_ENTRIES_PER_COLLECTION,
    "max_raw_entries_total": MAX_RAW_ENTRIES_TOTAL,
    "max_file_name_length": MAX_FILE_NAME_LENGTH,
    "max_size_bytes": MAX_SIZE_BYTES,
}
_SAFE_BASENAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._ -]{0,250}\.json$")
_COLLECTION_KINDS = frozenset({PROVIDER_MEMBERSHIP, PLAN_REFERENCE})


class UHCFileCatalogError(RuntimeError):
    """Report a bounded public-catalog contract failure."""


class UHCFileCatalogNotFound(LookupError):
    """Report an unknown historical semantic or raw catalog hash."""


@dataclass(frozen=True)
class UHCFileCatalogItem:
    family: str
    collection_kind: str
    file_id: str
    file_name: str
    source_url: str
    catalog_modified_at: str
    catalog_entry_sha256: str
    size_bytes: int | None

    def identity_payload(self) -> dict[str, Any]:
        """Return the complete immutable identity serialized into set hashes."""

        return {
            "family": self.family,
            "collection_kind": self.collection_kind,
            "file_id": self.file_id,
            "file_name": self.file_name,
            "source_url": self.source_url,
            "catalog_modified_at": self.catalog_modified_at,
            "size_bytes": self.size_bytes,
            "catalog_entry_sha256": self.catalog_entry_sha256,
        }


@dataclass(frozen=True)
class UHCObservedFileCatalog:
    files: tuple[UHCFileCatalogItem, ...]
    catalog_set_sha256: str
    collection_summary: tuple[dict[str, Any], ...]


def canonical_json(value: Any) -> str:
    """Serialize a value with the catalog's deterministic JSON contract."""

    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_text(value: str) -> str:
    """Return a lowercase SHA-256 digest for UTF-8 text."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def trusted_public_https_url(value: Any) -> str:
    """Validate one canonical credential-free UHC HTTPS URL."""

    raw_url = str(value or "")
    url = raw_url.strip()
    if raw_url != url or not url.isprintable():
        raise UHCFileCatalogError("UHC catalog URL is not public and trusted")
    parsed = urlsplit(url)
    try:
        port = parsed.port
    except ValueError as error:
        raise UHCFileCatalogError("UHC catalog URL has an invalid port") from error
    if (
        parsed.scheme != "https"
        or (parsed.hostname or "").lower() not in TRUSTED_HOSTS
        or port not in (None, 443)
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise UHCFileCatalogError("UHC catalog URL is not public and trusted")
    canonical_netloc = (parsed.hostname or "").lower()
    if port is not None:
        canonical_netloc = f"{canonical_netloc}:{port}"
    canonical_url = urlunsplit(("https", canonical_netloc, parsed.path, "", ""))
    if canonical_url != url:
        raise UHCFileCatalogError("UHC catalog URL is not canonical")
    return canonical_url


def _timestamp(value: Any) -> str:
    raw_timestamp = str(value or "").strip()
    if not raw_timestamp or len(raw_timestamp) > 64:
        raise UHCFileCatalogError("UHC catalog timestamp is missing")
    try:
        parsed_timestamp = datetime.fromisoformat(
            raw_timestamp[:-1] + "+00:00"
            if raw_timestamp.endswith("Z")
            else raw_timestamp
        )
    except ValueError as error:
        raise UHCFileCatalogError("UHC catalog timestamp is invalid") from error
    if parsed_timestamp.tzinfo is None or parsed_timestamp.utcoffset() is None:
        raise UHCFileCatalogError("UHC catalog timestamp must include a timezone")
    return parsed_timestamp.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _size_bytes(entry: dict[str, Any]) -> int | None:
    size_value = entry.get("size", entry.get("contentLength"))
    if size_value is None and isinstance(entry.get("properties"), dict):
        size_value = entry["properties"].get("contentLength")
    if size_value in (None, ""):
        return None
    if isinstance(size_value, bool) or not isinstance(size_value, (int, str)):
        raise UHCFileCatalogError("UHC catalog file size is invalid")
    if isinstance(size_value, str):
        normalized_size = size_value.strip()
        if len(normalized_size) > 19 or not normalized_size.isdigit():
            raise UHCFileCatalogError("UHC catalog file size is invalid")
        size_value = normalized_size
    size_bytes = int(size_value)
    if size_bytes < 0 or size_bytes > MAX_SIZE_BYTES:
        raise UHCFileCatalogError("UHC catalog file size is invalid")
    return size_bytes


def _source_url(
    family: str,
    collection_kind: str,
    file_name: str,
    entry: dict[str, Any],
) -> str:
    is_external = entry.get("isExternal", False)
    if not isinstance(is_external, bool):
        raise UHCFileCatalogError("UHC catalog external marker is invalid")
    if is_external:
        source_url = trusted_public_https_url(entry.get("url"))
        if unquote(urlsplit(source_url).path.rsplit("/", 1)[-1]) != file_name:
            raise UHCFileCatalogError("UHC external URL does not match its basename")
        return source_url
    blob_path = str(entry.get("blobPath") or "").strip().lstrip("/")
    section = "providers" if collection_kind == PROVIDER_MEMBERSHIP else "plans"
    expected_blob_path = f"ui/{family}/{section}/{file_name}"
    if blob_path != expected_blob_path or ".." in blob_path.split("/"):
        raise UHCFileCatalogError("UHC catalog blob path does not match its collection")
    return trusted_public_https_url(
        urljoin(CATALOG_BASE_URL, f"/api/stream/{quote(blob_path, safe='/')}")
    )


def _catalog_file(
    family: str,
    collection_kind: str,
    entry: dict[str, Any],
) -> UHCFileCatalogItem | None:
    file_name = str(entry.get("name") or "").strip()
    if not file_name.endswith(".json"):
        return None
    if not _SAFE_BASENAME.fullmatch(file_name) or not file_name.isprintable():
        raise UHCFileCatalogError("UHC catalog basename is unsafe")
    modified_at = _timestamp(entry.get("date"))
    size_bytes = _size_bytes(entry)
    source_url = _source_url(family, collection_kind, file_name, entry)
    source_identity_by_field = {
        "contract": CATALOG_CONTRACT,
        "family": family,
        "collection_kind": collection_kind,
        "file_name": file_name,
        "source_url": source_url,
        "catalog_modified_at": modified_at,
        "size_bytes": size_bytes,
    }
    catalog_entry_sha256 = sha256_text(canonical_json(source_identity_by_field))
    file_id = sha256_text(
        canonical_json(
            {"domain": "catalog-file-id", "source": source_identity_by_field}
        )
    )
    return UHCFileCatalogItem(
        family=family,
        collection_kind=collection_kind,
        file_id=file_id,
        file_name=file_name,
        source_url=source_url,
        catalog_modified_at=modified_at,
        catalog_entry_sha256=catalog_entry_sha256,
        size_bytes=size_bytes,
    )
