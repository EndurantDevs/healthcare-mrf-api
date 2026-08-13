# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import io
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process import uhc_provider_file_catalog_artifacts as artifacts
from process import uhc_provider_file_catalog_types as catalog_types
from process.formulary_fhir.source_artifact_contract import SourceArtifactIdentity
from process.formulary_fhir import uhc_drug_transport as drug_transport
from tests.uhc_provider_file_catalog_test_data import live_catalog_payloads


class _Content:
    def __init__(self, chunks):
        self._chunks = chunks

    async def iter_chunked(self, _chunk_size):
        for chunk in self._chunks:
            yield chunk


class _Response:
    def __init__(
        self, *, chunks=(), status=200, headers=None, url=None, content_length=None
    ):
        self.content = _Content(chunks)
        self.status = status
        self.headers = headers or {}
        self.url = url or catalog_types.CATALOG_URLS["cs"]
        self.content_length = content_length

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
async def test_fetch_rejects_implicit_response_url_change():
    catalog_session = _Session(
        [
            _Response(
                chunks=[b"{}"],
                url=catalog_types.CATALOG_URLS["ifp"],
            )
        ]
    )

    with pytest.raises(catalog_types.UHCFileCatalogError, match="response URL"):
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
async def test_drug_stream_accepts_validated_redirect():
    source_url = catalog_types.CATALOG_URLS["cs"]
    redirected_url = catalog_types.CATALOG_URLS["ifp"]
    raw_bytes = b"[{}]"
    catalog_session = _Session(
        [
            _Response(
                status=302,
                headers={"Location": redirected_url},
                url=source_url,
            ),
            _Response(
                chunks=[raw_bytes],
                headers={"Content-Length": str(len(raw_bytes))},
                url=redirected_url,
                content_length=len(raw_bytes),
            ),
        ]
    )

    digest, byte_count = await drug_transport.stream_uhc_drug_response(
        catalog_session,
        SimpleNamespace(source_url=source_url, expected_byte_count=len(raw_bytes)),
        io.BytesIO(),
        max_bytes=100,
        cancel_check=None,
    )

    assert (digest, byte_count) == (hashlib.sha256(raw_bytes).hexdigest(), 4)


def _external_drug_identity() -> SourceArtifactIdentity:
    file_name = "drug-00.json"
    return SourceArtifactIdentity(
        source_id="official-formulary",
        source_file_set_sha256="a" * 64,
        source_file_id="b" * 64,
        raw_listing_projection_sha256="c" * 64,
        family="cs",
        file_name=file_name,
        source_url=(
            "https://legacy.providerlookuponline.com/files/" f"{file_name}"
        ),
        catalog_modified_at="2026-08-10T00:00:00Z",
        catalog_entry_sha256="d" * 64,
        expected_byte_count=4,
    )


async def _stream_drug(session: _Session, identity: SourceArtifactIdentity):
    return await drug_transport.stream_uhc_drug_response(
        session,
        identity,
        io.BytesIO(),
        max_bytes=100,
        cancel_check=None,
    )


def _redirected_drug_session(
    identity: SourceArtifactIdentity,
    location: str,
    *,
    status: int = 302,
) -> _Session:
    return _Session(
        [
            _Response(
                status=status,
                headers={"Location": location},
                url=identity.source_url,
            )
        ]
    )


@pytest.mark.asyncio
async def test_drug_stream_upgrades_exact_http_sibling_without_requesting_it():
    identity = _external_drug_identity()
    unsafe_redirect = "http://www.providerlookuponline.com/moved"
    mirror_url = (
        "https://www.providerlookuponline.com/files/" f"{identity.file_name}"
    )
    raw_bytes = b"[{}]"
    session = _Session(
        [
            _Response(
                status=302,
                headers={"Location": unsafe_redirect},
                url=identity.source_url,
            ),
            _Response(
                chunks=[raw_bytes],
                headers={"Content-Length": str(len(raw_bytes))},
                url=mirror_url,
                content_length=len(raw_bytes),
            ),
        ]
    )

    digest, byte_count = await _stream_drug(session, identity)

    requested_urls = [request_url for request_url, _options in session.requests]
    assert (digest, byte_count) == (hashlib.sha256(raw_bytes).hexdigest(), 4)
    assert requested_urls == [identity.source_url, mirror_url]
    assert unsafe_redirect not in requested_urls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unsafe_redirect",
    [
        "http://mirror.example.invalid/moved",
        "https://www.providerlookuponline.com/moved",
        "http://www.providerlookuponline.com:80/moved",
        "http://www.providerlookuponline.com:8080/moved",
        "http://www.providerlookuponline.com/moved?token=invalid",
        "http://www.providerlookuponline.com/moved#fragment",
        "http://user@www.providerlookuponline.com/moved",
        " http://www.providerlookuponline.com/moved ",
    ],
)
async def test_drug_stream_rejects_other_http_redirects(unsafe_redirect):
    identity = _external_drug_identity()
    session = _redirected_drug_session(identity, unsafe_redirect)

    with pytest.raises(
        drug_transport.UHCDrugArtifactAcquisitionError,
        match="redirect",
    ):
        await _stream_drug(session, identity)

    assert [request_url for request_url, _options in session.requests] == [
        identity.source_url
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_response", ["status", "basename"])
async def test_drug_stream_rejects_unreviewed_signal(invalid_response):
    identity = _external_drug_identity()
    if invalid_response == "basename":
        identity = replace(
            identity,
            source_url="https://legacy.providerlookuponline.com/files/other.json",
        )
    session = _redirected_drug_session(
        identity,
        "http://www.providerlookuponline.com/moved",
        status=301 if invalid_response == "status" else 302,
    )

    with pytest.raises(
        drug_transport.UHCDrugArtifactAcquisitionError,
        match="redirect",
    ):
        await _stream_drug(session, identity)

    assert len(session.requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("mirror_response", ["redirect", "wrong-url"])
async def test_drug_stream_rejects_mirror_response_change(mirror_response):
    identity = _external_drug_identity()
    mirror_url = (
        "https://www.providerlookuponline.com/files/" f"{identity.file_name}"
    )
    if mirror_response == "redirect":
        response = _Response(
            status=302,
            headers={"Location": identity.source_url},
            url=mirror_url,
        )
    else:
        response = _Response(
            chunks=[b"[{}]"],
            url=mirror_url + ".changed",
            content_length=4,
        )
    session = _redirected_drug_session(
        identity,
        "http://www.providerlookuponline.com/moved",
    )
    session.responses.append(response)

    with pytest.raises(
        drug_transport.UHCDrugArtifactAcquisitionError,
        match="redirect",
    ):
        await _stream_drug(session, identity)

    assert len(session.requests) == 2


@pytest.mark.asyncio
async def test_drug_stream_keeps_direct_trusted_https_request_unchanged():
    source_url = catalog_types.CATALOG_URLS["cs"]
    raw_bytes = b"[{}]"
    session = _Session(
        [
            _Response(
                chunks=[raw_bytes],
                url=source_url,
                content_length=len(raw_bytes),
            )
        ]
    )

    digest, byte_count = await drug_transport.stream_uhc_drug_response(
        session,
        SimpleNamespace(source_url=source_url, expected_byte_count=len(raw_bytes)),
        io.BytesIO(),
        max_bytes=100,
        cancel_check=None,
    )

    assert (digest, byte_count) == (hashlib.sha256(raw_bytes).hexdigest(), 4)
    assert [request_url for request_url, _options in session.requests] == [source_url]


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
