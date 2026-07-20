# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable UHC catalog binding identities used by retained admission."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from process.uhc_retained_types import UHCRetainedAdmissionError


_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_UHC_CATALOG_CONTRACT = "healthporta-uhc-provider-file-catalog-v1"
_UHC_CATALOG_HOSTS = frozenset(
    {"providermrf.uhc.com", "legacy.providerlookuponline.com"}
)


class UHCSourceBindingMismatch(UHCRetainedAdmissionError):
    """A stable catalog file identity was rebound to different source/content."""


@dataclass(frozen=True)
class SourceBinding:
    source_file_id: str
    family: str
    collection_kind: str
    file_name: str
    source_url: str
    catalog_modified_at: str
    size_bytes: int | None
    catalog_set_sha256: str
    catalog_entry_sha256: str
    artifact_sha256: str


def require_digest(field_value: str, field: str) -> str:
    """Return one lowercase SHA-256 digest or fail closed."""

    if not _DIGEST_RE.fullmatch(field_value):
        raise UHCRetainedAdmissionError(f"{field} is not a SHA-256 digest")
    return field_value


def safe_file_name(file_name: str) -> str:
    """Return one confined printable catalog basename."""

    if (
        not file_name
        or Path(file_name).name != file_name
        or len(file_name) > 256
        or any(ord(character) < 32 for character in file_name)
    ):
        raise UHCRetainedAdmissionError("source file name is unsafe")
    return file_name


def _canonical_catalog_url(source_url: str) -> str:
    """Require the exact credential-free HTTPS identity accepted by the catalog."""

    if source_url != source_url.strip() or not source_url.isprintable():
        raise UHCRetainedAdmissionError("catalog source URL is not canonical")
    parsed = urlsplit(source_url)
    try:
        port = parsed.port
    except ValueError as error:
        raise UHCRetainedAdmissionError(
            "catalog source URL is not canonical"
        ) from error
    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme != "https"
        or host not in _UHC_CATALOG_HOSTS
        or port not in (None, 443)
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise UHCRetainedAdmissionError("catalog source URL is not canonical")
    canonical_netloc = host if port is None else f"{host}:{port}"
    canonical_url = urlunsplit(("https", canonical_netloc, parsed.path, "", ""))
    if canonical_url != source_url:
        raise UHCRetainedAdmissionError("catalog source URL is not canonical")
    return source_url


def _canonical_catalog_timestamp(catalog_modified_at: str) -> str:
    """Require the catalog's normalized UTC timestamp identity."""

    if not catalog_modified_at or len(catalog_modified_at) > 64:
        raise UHCRetainedAdmissionError("catalog timestamp is not canonical")
    try:
        parsed = dt.datetime.fromisoformat(
            catalog_modified_at[:-1] + "+00:00"
            if catalog_modified_at.endswith("Z")
            else catalog_modified_at
        )
    except ValueError as error:
        raise UHCRetainedAdmissionError("catalog timestamp is not canonical") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise UHCRetainedAdmissionError("catalog timestamp is not canonical")
    canonical = parsed.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")
    if canonical != catalog_modified_at:
        raise UHCRetainedAdmissionError("catalog timestamp is not canonical")
    return catalog_modified_at


def expected_catalog_file_hash_pair(
    *,
    family: str,
    collection_kind: str,
    file_name: str,
    source_url: str,
    catalog_modified_at: str,
    size_bytes: int | None,
) -> tuple[str, str]:
    """Recompute UHC catalog-entry and stable file identities from all fields."""

    _canonical_catalog_url(source_url)
    _canonical_catalog_timestamp(catalog_modified_at)
    has_invalid_size = isinstance(size_bytes, bool) or (
        size_bytes is not None
        and (
            not isinstance(size_bytes, int)
            or size_bytes < 0
            or size_bytes > 2**63 - 1
        )
    )
    if has_invalid_size:
        raise UHCRetainedAdmissionError("catalog file byte count is invalid")
    source_identity_by_field = {
        "contract": _UHC_CATALOG_CONTRACT,
        "family": family,
        "collection_kind": collection_kind,
        "file_name": file_name,
        "source_url": source_url,
        "catalog_modified_at": catalog_modified_at,
        "size_bytes": size_bytes,
    }
    encoded_identity = json.dumps(
        source_identity_by_field,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    catalog_entry_sha256 = hashlib.sha256(encoded_identity).hexdigest()
    file_identity = json.dumps(
        {"domain": "catalog-file-id", "source": source_identity_by_field},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return catalog_entry_sha256, hashlib.sha256(file_identity).hexdigest()
