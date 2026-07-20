# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json

import pytest

from process import uhc_provider_file_catalog_artifacts as artifacts
from process import uhc_provider_file_catalog_types as catalog_types
from tests.uhc_provider_file_catalog_test_data import live_catalog_payloads


class _Content:
    def __init__(self, chunks):
        self._chunks = chunks

    async def iter_chunked(self, _chunk_size):
        for chunk in self._chunks:
            yield chunk


class _Response:
    def __init__(self, *, chunks=(), status=200, headers=None, url=None):
        self.content = _Content(chunks)
        self.status = status
        self.headers = headers or {}
        self.url = url or catalog_types.CATALOG_URLS["cs"]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_fetch_streams_to_eof_before_accepting_json():
    catalog_session = _Session(
        [_Response(chunks=[b'{"providers": []}', b" trailing"])]
    )

    with pytest.raises(catalog_types.UHCFileCatalogError, match="exact JSON"):
        await artifacts._fetch_document(
            catalog_session,
            "cs",
            catalog_types.CATALOG_URLS["cs"],
        )


@pytest.mark.asyncio
async def test_fetch_converts_deep_json_recursion_to_catalog_error(monkeypatch):
    deeply_nested_json = (b"[" * 2_000) + (b"]" * 2_000)
    catalog_session = _Session([_Response(chunks=[deeply_nested_json])])
    monkeypatch.setattr(
        artifacts.json,
        "loads",
        lambda _raw_bytes: (_ for _ in ()).throw(RecursionError("too deep")),
    )

    with pytest.raises(catalog_types.UHCFileCatalogError, match="exact JSON"):
        await artifacts._fetch_document(
            catalog_session,
            "cs",
            catalog_types.CATALOG_URLS["cs"],
        )


@pytest.mark.asyncio
async def test_fetch_enforces_cumulative_cap(monkeypatch):
    monkeypatch.setattr(artifacts, "CATALOG_MAX_BYTES", 10)
    catalog_session = _Session([_Response(chunks=[b"123456", b"78901"])])

    with pytest.raises(catalog_types.UHCFileCatalogError, match="byte bound"):
        await artifacts._fetch_document(
            catalog_session,
            "cs",
            catalog_types.CATALOG_URLS["cs"],
        )


@pytest.mark.asyncio
async def test_fetch_requires_exact_declared_length_and_identity_encoding():
    short_session = _Session(
        [_Response(chunks=[b"{}"], headers={"Content-Length": "3"})]
    )
    encoded_session = _Session(
        [_Response(chunks=[b"{}"], headers={"Content-Encoding": "gzip"})]
    )

    with pytest.raises(catalog_types.UHCFileCatalogError, match="incomplete"):
        await artifacts._fetch_document(
            short_session,
            "cs",
            catalog_types.CATALOG_URLS["cs"],
        )
    with pytest.raises(catalog_types.UHCFileCatalogError, match="encoded"):
        await artifacts._fetch_document(
            encoded_session,
            "cs",
            catalog_types.CATALOG_URLS["cs"],
        )


@pytest.mark.asyncio
async def test_fetch_revalidates_redirect_without_query_credentials():
    redirect = _Response(
        status=302,
        headers={"Location": "/api/files/ui/cs/?token=secret"},
    )
    catalog_session = _Session([redirect])

    with pytest.raises(catalog_types.UHCFileCatalogError, match="trusted"):
        await artifacts._fetch_document(
            catalog_session,
            "cs",
            catalog_types.CATALOG_URLS["cs"],
        )


@pytest.mark.asyncio
async def test_fetch_accepts_exact_identity_document_and_valid_redirect():
    raw_bytes = b'{"providers":[]}'
    redirected_url = "https://legacy.providerlookuponline.com/catalog.json"
    catalog_session = _Session(
        [
            _Response(
                status=302,
                headers={"Location": redirected_url},
                url=catalog_types.CATALOG_URLS["cs"],
            ),
            _Response(
                chunks=[raw_bytes[:5], raw_bytes[5:]],
                headers={
                    "Content-Length": str(len(raw_bytes)),
                    "Content-Encoding": "identity",
                    "ETag": '"catalog"',
                    "Last-Modified": "Mon, 20 Jul 2026 00:00:00 GMT",
                },
                url=redirected_url,
            ),
        ]
    )

    document = await artifacts._fetch_document(
        catalog_session,
        "cs",
        catalog_types.CATALOG_URLS["cs"],
    )

    assert document.payload == {"providers": []}
    assert document.response_url == redirected_url
    assert document.raw_sha256 == hashlib.sha256(raw_bytes).hexdigest()
    assert all(
        request_options["allow_redirects"] is False
        for _request_url, request_options in catalog_session.requests
    )
    assert all(
        request_options["headers"] == {"Accept-Encoding": "identity"}
        for _request_url, request_options in catalog_session.requests
    )


@pytest.mark.asyncio
async def test_fetch_snapshot_binds_both_official_families():
    response_by_family = {
        family: json.dumps(live_catalog_payloads()[family], sort_keys=True).encode()
        for family in sorted(catalog_types.CATALOG_URLS)
    }
    catalog_session = _Session(
        [
            _Response(
                chunks=[response_by_family[family]],
                headers={"Content-Length": str(len(response_by_family[family]))},
                url=catalog_types.CATALOG_URLS[family],
            )
            for family in sorted(catalog_types.CATALOG_URLS)
        ]
    )

    snapshot = await artifacts.fetch_catalog_snapshot(catalog_session)

    assert set(snapshot.payloads_by_family) == set(catalog_types.CATALOG_URLS)
    assert snapshot.raw_set_sha256 == artifacts.raw_set_sha256_from_documents(
        [
            {
                "family": document.family,
                "url": document.url,
                "response_url": document.response_url,
                "raw_sha256": document.raw_sha256,
                "byte_count": len(document.raw_bytes),
            }
            for document in snapshot.documents
        ]
    )


@pytest.mark.asyncio
async def test_fetch_rejects_unavailable_invalid_length_and_redirect_limit(monkeypatch):
    unavailable_session = _Session([_Response(status=503)])
    with pytest.raises(catalog_types.UHCFileCatalogError, match="unavailable"):
        await artifacts._fetch_document(
            unavailable_session,
            "cs",
            catalog_types.CATALOG_URLS["cs"],
        )

    for declared_length in ("invalid", "5000001"):
        monkeypatch.setattr(artifacts, "CATALOG_MAX_BYTES", 5_000_000)
        invalid_length_session = _Session(
            [_Response(chunks=[b"{}"], headers={"Content-Length": declared_length})]
        )
        with pytest.raises(catalog_types.UHCFileCatalogError, match="(?i)length"):
            await artifacts._fetch_document(
                invalid_length_session,
                "cs",
                catalog_types.CATALOG_URLS["cs"],
            )

    redirect_responses = [
        _Response(
            status=302,
            headers={"Location": catalog_types.CATALOG_URLS["cs"]},
        )
        for _redirect_count in range(artifacts.MAX_REDIRECTS + 1)
    ]
    with pytest.raises(catalog_types.UHCFileCatalogError, match="redirect"):
        await artifacts._fetch_document(
            _Session(redirect_responses),
            "cs",
            catalog_types.CATALOG_URLS["cs"],
        )
