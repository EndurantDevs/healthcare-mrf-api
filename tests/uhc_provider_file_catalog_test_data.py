# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json

from process import uhc_provider_file_catalog_artifacts as artifacts
from process import uhc_provider_file_catalog_types as catalog_types


def catalog_entry(family: str, section: str, index: int) -> dict:
    prefix = "Providers" if section == "providers" else "Plans"
    name = f"JSON_{prefix}_{family.upper()}_{index:03d}.json"
    return {
        "name": name,
        "date": "2026-07-20T00:00:00Z",
        "size": 1_000 + index,
        "blobPath": f"ui/{family}/{section}/{name}",
    }


def live_catalog_payloads() -> dict:
    return {
        "cs": {
            "providers": [catalog_entry("cs", "providers", index) for index in range(53)],
        },
        "ifp": {
            "providers": [
                catalog_entry("ifp", "providers", index) for index in range(25)
            ],
            "plans": [catalog_entry("ifp", "plans", index) for index in range(24)],
        },
    }


def raw_catalog_document(
    family: str,
    payloads_by_family: dict | None = None,
) -> artifacts.UHCRawCatalogDocument:
    payloads_by_family = payloads_by_family or live_catalog_payloads()
    raw_bytes = json.dumps(payloads_by_family[family], sort_keys=True).encode()
    return artifacts.UHCRawCatalogDocument(
        family=family,
        url=catalog_types.CATALOG_URLS[family],
        response_url=catalog_types.CATALOG_URLS[family],
        payload=payloads_by_family[family],
        raw_bytes=raw_bytes,
        raw_sha256=hashlib.sha256(raw_bytes).hexdigest(),
        etag=f'"{family}-etag"',
        last_modified="Mon, 20 Jul 2026 00:00:00 GMT",
    )


def raw_catalog_snapshot(
    *,
    raw_set_sha256: str | None = None,
    payloads_by_family: dict | None = None,
) -> artifacts.UHCRawCatalogSnapshot:
    payloads_by_family = payloads_by_family or live_catalog_payloads()
    documents = tuple(
        raw_catalog_document(family, payloads_by_family)
        for family in sorted(catalog_types.CATALOG_URLS)
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
    return artifacts.UHCRawCatalogSnapshot(
        documents=documents,
        raw_set_sha256=(
            raw_set_sha256
            or artifacts.raw_set_sha256_from_documents(proof_documents)
        ),
    )
