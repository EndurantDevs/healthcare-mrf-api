# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded fetch and durable raw proof for UHC's two public listings."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

import aiohttp

from process.ptg_parts.artifacts import PTG2ArtifactStore, sha256_file
from process.uhc_provider_file_catalog_types import (
    CATALOG_URLS,
    UHCFileCatalogError,
    UHCObservedFileCatalog,
    canonical_json,
    observed_catalog_from_payloads,
    trusted_public_https_url,
)


CATALOG_MAX_BYTES = 5 * 1024 * 1024
CATALOG_ARTIFACT_KIND = "provider-directory-uhc-file-catalog"
USER_AGENT = "HealthPorta-UHC-File-Catalog/1.0"
REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
MAX_REDIRECTS = 5


@dataclass(frozen=True)
class UHCRawCatalogDocument:
    family: str
    url: str
    response_url: str
    payload: Any
    raw_bytes: bytes
    raw_sha256: str
    etag: str | None
    last_modified: str | None


@dataclass(frozen=True)
class UHCRawCatalogSnapshot:
    documents: tuple[UHCRawCatalogDocument, ...]
    raw_set_sha256: str

    @property
    def payloads_by_family(self) -> dict[str, Any]:
        """Return decoded listing payloads keyed by bound UHC family."""

        return {document.family: document.payload for document in self.documents}


def raw_set_sha256_from_documents(documents: list[dict[str, Any]]) -> str:
    """Hash the exact bounded raw-document identities."""

    document_identities = [
        {
            "family": str(document["family"]),
            "url": trusted_public_https_url(document["url"]),
            "response_url": trusted_public_https_url(
                document.get("response_url") or document["url"]
            ),
            "raw_sha256": str(document["raw_sha256"]),
            "byte_count": int(document["byte_count"]),
        }
        for document in sorted(
            documents,
            key=lambda raw_document: str(raw_document["family"]),
        )
    ]
    return hashlib.sha256(
        canonical_json(document_identities).encode("utf-8")
    ).hexdigest()


def catalog_artifact_root() -> Path:
    """Resolve the mandatory durable root for UHC listing proof."""

    configured = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT")
    if not configured:
        raise UHCFileCatalogError(
            "UHC catalog refresh requires the provider-directory artifact root"
        )
    try:
        base = Path(configured).resolve()
        temporary_root = Path(tempfile.gettempdir()).resolve()
        if base == Path("/") or base.is_relative_to(temporary_root):
            raise UHCFileCatalogError(
                "UHC catalog proof cannot use temporary storage"
            )
        root = base / "uhc-file-catalog"
        root.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise UHCFileCatalogError("UHC catalog proof storage is unavailable") from error
    return root


@asynccontextmanager
async def _validated_get(session: aiohttp.ClientSession, url: str):
    request_url = trusted_public_https_url(url)
    redirect_count = 0
    while True:
        async with session.get(
            request_url,
            allow_redirects=False,
            headers={"Accept-Encoding": "identity"},
        ) as response:
            response_url = trusted_public_https_url(str(response.url))
            if response.status not in REDIRECT_STATUSES:
                yield response
                return
            location = str(response.headers.get("Location") or "").strip()
            if not location or redirect_count >= MAX_REDIRECTS:
                raise UHCFileCatalogError("UHC catalog redirect is invalid")
            request_url = trusted_public_https_url(urljoin(response_url, location))
            redirect_count += 1


def _declared_length(response: aiohttp.ClientResponse) -> int | None:
    value = response.headers.get("Content-Length")
    if value is None:
        return None
    try:
        length = int(value)
    except ValueError as error:
        raise UHCFileCatalogError("UHC catalog Content-Length is invalid") from error
    if length < 0 or length > CATALOG_MAX_BYTES:
        raise UHCFileCatalogError("UHC catalog declared length exceeds its bound")
    return length


async def _fetch_document(
    session: aiohttp.ClientSession,
    family: str,
    url: str,
) -> UHCRawCatalogDocument:
    async with _validated_get(session, url) as response:
        if response.status != 200:
            raise UHCFileCatalogError("UHC catalog endpoint is unavailable")
        content_encoding = str(response.headers.get("Content-Encoding") or "").lower()
        if content_encoding not in {"", "identity"}:
            raise UHCFileCatalogError("UHC catalog response is encoded")
        declared_length = _declared_length(response)
        chunks: list[bytes] = []
        byte_count = 0
        async for chunk in response.content.iter_chunked(64 * 1024):
            byte_count += len(chunk)
            if byte_count > CATALOG_MAX_BYTES:
                raise UHCFileCatalogError("UHC catalog response exceeds its byte bound")
            chunks.append(bytes(chunk))
        if declared_length is not None and byte_count != declared_length:
            raise UHCFileCatalogError("UHC catalog response length is incomplete")
        raw_bytes = b"".join(chunks)
        try:
            catalog_payload = json.loads(raw_bytes)
        except (UnicodeDecodeError, ValueError, RecursionError) as error:
            raise UHCFileCatalogError("UHC catalog response is not exact JSON") from error
        return UHCRawCatalogDocument(
            family=family,
            url=trusted_public_https_url(url),
            response_url=trusted_public_https_url(str(response.url)),
            payload=catalog_payload,
            raw_bytes=raw_bytes,
            raw_sha256=hashlib.sha256(raw_bytes).hexdigest(),
            etag=response.headers.get("ETag"),
            last_modified=response.headers.get("Last-Modified"),
        )


async def fetch_catalog_snapshot(
    session: aiohttp.ClientSession,
) -> UHCRawCatalogSnapshot:
    """Fetch both official listing documents under the bounded HTTP contract."""

    documents = tuple(
        [
            await _fetch_document(session, family, CATALOG_URLS[family])
            for family in sorted(CATALOG_URLS)
        ]
    )
    proof_documents = [
        {
            "family": document.family,
            "url": document.url,
            "response_url": document.response_url,
            "raw_sha256": document.raw_sha256,
            "byte_count": len(document.raw_bytes),
        }
        for document in documents
    ]
    return UHCRawCatalogSnapshot(
        documents=documents,
        raw_set_sha256=raw_set_sha256_from_documents(proof_documents),
    )


def _publish_raw_document(
    store: PTG2ArtifactStore,
    document: UHCRawCatalogDocument,
) -> Path:
    final_path = store.artifact_path(
        document.raw_sha256,
        kind=CATALOG_ARTIFACT_KIND,
        suffix=".json",
    )
    descriptor, raw_path = tempfile.mkstemp(
        prefix="uhc-catalog-",
        suffix=".partial",
        dir=store.tmp_dir,
    )
    temporary_path = Path(raw_path)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(document.raw_bytes)
            output.flush()
            os.fsync(output.fileno())
        with store.named_lock("uhc-file-catalog", document.raw_sha256):
            final_path.parent.mkdir(parents=True, exist_ok=True)
            if final_path.exists():
                digest, byte_count = sha256_file(final_path)
                if (
                    digest != document.raw_sha256
                    or byte_count != len(document.raw_bytes)
                ):
                    raise UHCFileCatalogError("UHC raw catalog artifact is corrupt")
                temporary_path.unlink(missing_ok=True)
            else:
                os.replace(temporary_path, final_path)
                _fsync_directory(final_path.parent)
        return final_path
    finally:
        temporary_path.unlink(missing_ok=True)


def _fsync_directory(directory: Path) -> None:
    """Durably commit a retained artifact rename in its exact parent."""

    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def retain_catalog_snapshot(snapshot: UHCRawCatalogSnapshot) -> dict[str, Any]:
    """Durably retain both raw listings and return content-addressed proof."""

    try:
        return _retain_catalog_snapshot(snapshot)
    except OSError as error:
        raise UHCFileCatalogError("UHC catalog proof storage is unavailable") from error


def _retain_catalog_snapshot(snapshot: UHCRawCatalogSnapshot) -> dict[str, Any]:
    """Persist one raw snapshot after the public storage error boundary."""

    store = PTG2ArtifactStore(catalog_artifact_root())
    retained_documents: list[dict[str, Any]] = []
    for document in snapshot.documents:
        final_path = _publish_raw_document(store, document)
        storage_uri = store.storage_uri(final_path)
        proof_by_field = {
            "family": document.family,
            "url": document.url,
            "response_url": document.response_url,
            "raw_sha256": document.raw_sha256,
            "byte_count": len(document.raw_bytes),
            "storage_uri": storage_uri,
            "etag": document.etag,
            "last_modified": document.last_modified,
        }
        store.record_manifest(
            {
                "artifact_kind": CATALOG_ARTIFACT_KIND,
                "status": "available",
                **proof_by_field,
            }
        )
        retained_documents.append(proof_by_field)
    if raw_set_sha256_from_documents(retained_documents) != snapshot.raw_set_sha256:
        raise UHCFileCatalogError("UHC retained raw catalog set proof changed")
    return {
        "raw_set_sha256": snapshot.raw_set_sha256,
        "documents": retained_documents,
    }


def _validated_document_identity(
    raw_document: Any,
    seen_families: set[str],
) -> tuple[str, str, str, str, int, str]:
    if not isinstance(raw_document, dict):
        raise UHCFileCatalogError("UHC retained raw catalog document is invalid")
    family = str(raw_document.get("family") or "")
    if family not in CATALOG_URLS or family in seen_families:
        raise UHCFileCatalogError("UHC retained raw catalog families are invalid")
    seen_families.add(family)
    url = trusted_public_https_url(raw_document.get("url"))
    response_url = trusted_public_https_url(raw_document.get("response_url") or url)
    if url != CATALOG_URLS[family]:
        raise UHCFileCatalogError("UHC raw catalog URL does not match its family")
    digest = str(raw_document.get("raw_sha256") or "")
    byte_count = raw_document.get("byte_count")
    if (
        len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or isinstance(byte_count, bool)
        or not isinstance(byte_count, int)
        or byte_count <= 0
        or byte_count > CATALOG_MAX_BYTES
    ):
        raise UHCFileCatalogError("UHC retained raw catalog identity is invalid")
    storage_uri = str(raw_document.get("storage_uri") or "")
    return family, url, response_url, digest, byte_count, storage_uri


def _validated_retained_path(
    store: PTG2ArtifactStore,
    digest: str,
    byte_count: int,
    storage_uri: str,
) -> tuple[Path, bytes]:
    parsed_storage_uri = urlsplit(storage_uri)
    if (
        parsed_storage_uri.scheme != "file"
        or parsed_storage_uri.netloc
        or parsed_storage_uri.query
        or parsed_storage_uri.fragment
    ):
        raise UHCFileCatalogError("UHC raw catalog storage URI is invalid")
    retained_path = store.path_from_uri(storage_uri).resolve()
    expected_path = store.artifact_path(
        digest,
        kind=CATALOG_ARTIFACT_KIND,
        suffix=".json",
    ).resolve()
    if retained_path != expected_path or not retained_path.is_relative_to(
        store.root.resolve()
    ):
        raise UHCFileCatalogError("UHC raw catalog proof escapes its artifact root")
    if storage_uri != store.storage_uri(expected_path):
        raise UHCFileCatalogError("UHC raw catalog storage URI is not canonical")
    try:
        with retained_path.open("rb") as retained_file:
            raw_bytes = retained_file.read(byte_count + 1)
    except OSError as error:
        raise UHCFileCatalogError("UHC raw catalog proof is unavailable") from error
    if len(raw_bytes) != byte_count or hashlib.sha256(raw_bytes).hexdigest() != digest:
        raise UHCFileCatalogError("UHC raw catalog artifact is corrupt")
    return retained_path, raw_bytes


def _validated_raw_document(
    store: PTG2ArtifactStore,
    raw_document: Any,
    seen_families: set[str],
) -> tuple[dict[str, Any], Any]:
    family, url, response_url, digest, byte_count, storage_uri = (
        _validated_document_identity(raw_document, seen_families)
    )
    retained_path, raw_bytes = _validated_retained_path(
        store,
        digest,
        byte_count,
        storage_uri,
    )
    try:
        payload = json.loads(raw_bytes)
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise UHCFileCatalogError("UHC retained raw catalog is not exact JSON") from error
    return (
        {
            "family": family,
            "url": url,
            "response_url": response_url,
            "raw_sha256": digest,
            "byte_count": byte_count,
            "storage_uri": store.storage_uri(retained_path),
        },
        payload,
    )


def validate_retained_catalog_proof(
    raw_proof: Any,
) -> tuple[dict[str, Any], UHCObservedFileCatalog]:
    """Rehash, decode, and semantically bind both retained listings."""

    normalized_proof, payloads_by_family = _validated_catalog_proof(raw_proof)
    return normalized_proof, observed_catalog_from_payloads(payloads_by_family)


def validate_retained_raw_proof(raw_proof: Any) -> dict[str, Any]:
    """Rehash the two retained listings and return their immutable proof."""

    normalized_proof, _payloads_by_family = _validated_catalog_proof(raw_proof)
    return normalized_proof


def _validated_catalog_proof(
    raw_proof: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return one byte-rehashed proof and its exact decoded payloads."""

    if not isinstance(raw_proof, dict):
        raise UHCFileCatalogError("UHC retained raw catalog proof is invalid")
    raw_set_sha256 = str(raw_proof.get("raw_set_sha256") or "")
    raw_documents = raw_proof.get("documents")
    if (
        len(raw_set_sha256) != 64
        or any(character not in "0123456789abcdef" for character in raw_set_sha256)
        or not isinstance(raw_documents, list)
        or len(raw_documents) != len(CATALOG_URLS)
    ):
        raise UHCFileCatalogError("UHC retained raw catalog proof is invalid")

    try:
        normalized_documents, payloads_by_family = _validated_catalog_documents(
            raw_documents
        )
    except OSError as error:
        raise UHCFileCatalogError("UHC catalog proof storage is unavailable") from error
    if raw_set_sha256_from_documents(normalized_documents) != raw_set_sha256:
        raise UHCFileCatalogError("UHC retained raw catalog set proof changed")
    return (
        {
            "raw_set_sha256": raw_set_sha256,
            "documents": normalized_documents,
        },
        payloads_by_family,
    )


def _validated_catalog_documents(
    raw_documents: list[Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Rehash retained documents inside the filesystem error boundary."""

    store = PTG2ArtifactStore(catalog_artifact_root())
    normalized_documents: list[dict[str, Any]] = []
    payloads_by_family: dict[str, Any] = {}
    seen_families: set[str] = set()
    for raw_document in raw_documents:
        normalized_document, catalog_payload = _validated_raw_document(
            store,
            raw_document,
            seen_families,
        )
        normalized_documents.append(normalized_document)
        payloads_by_family[normalized_document["family"]] = catalog_payload
    normalized_documents.sort(key=lambda document: document["family"])
    return normalized_documents, payloads_by_family
