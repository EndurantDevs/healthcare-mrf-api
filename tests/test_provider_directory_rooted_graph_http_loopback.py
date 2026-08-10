# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from contextlib import asynccontextmanager
import datetime as dt
import ipaddress
import json
import ssl

import aiohttp
from aiohttp import web
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
import pytest

from process.provider_directory_rooted_graph_http import (
    ProviderDirectoryRootedGraphHTTPError,
    fetch_provider_directory_rooted_graph_query,
)
from process.provider_directory_rooted_graph_query import (
    build_provider_directory_practitioner_role_query,
)
from tests.provider_directory_rooted_graph_acquisition_test_support import (
    bundle,
    claim_for_query,
)


def _tls_contexts(tmp_path) -> tuple[ssl.SSLContext, ssl.SSLContext]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = dt.datetime.now(dt.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .sign(private_key, hashes.SHA256())
    )
    key_path = tmp_path / "loopback-key.pem"
    certificate_path = tmp_path / "loopback-certificate.pem"
    key_path.write_bytes(
        private_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    server_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    server_context.load_cert_chain(certificate_path, key_path)
    client_context = ssl.create_default_context(cafile=certificate_path)
    return server_context, client_context


def _fhir_response(payload: object) -> web.Response:
    return web.Response(
        body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        content_type="application/fhir+json",
        charset="utf-8",
    )


@asynccontextmanager
async def _loopback_server(tmp_path):
    server_context, client_context = _tls_contexts(tmp_path)
    requests: list[tuple[str, str, str]] = []
    first_role_by_field = {
        "resourceType": "PractitionerRole",
        "id": "role.synthetic-1",
        "practitioner": {"reference": "Practitioner/practitioner.synthetic-1"},
    }
    second_role_by_field = {
        **first_role_by_field,
        "id": "role.synthetic-2",
    }

    async def handler(request: web.Request) -> web.Response:
        requests.append(
            (
                request.raw_path,
                request.headers.get("Accept", ""),
                request.headers.get("Accept-Encoding", ""),
            )
        )
        if "cursor" not in request.query:
            next_url = (
                f"https://localhost:{port}/fhir/R4/PractitionerRole?"
                "cursor=opaque%2Bvalue&cursor=second"
            )
            return _fhir_response(
                bundle([first_role_by_field], total=2, next_url=next_url)
            )
        return _fhir_response(bundle([second_role_by_field], total=2))

    application = web.Application()
    application.router.add_get("/fhir/R4/PractitionerRole", handler)
    runner = web.AppRunner(application)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=server_context)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    api_base = f"https://localhost:{port}/fhir/R4"
    try:
        yield api_base, client_context, requests
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_verified_tls_loopback_preserves_exact_query_and_opaque_cursor(
    tmp_path,
) -> None:
    async with _loopback_server(tmp_path) as (
        api_base,
        client_context,
        requests,
    ):
        query = build_provider_directory_practitioner_role_query(
            api_base,
            "practitioner.synthetic-1",
        )
        claim = claim_for_query(query)
        connector = aiohttp.TCPConnector(ssl=client_context)
        async with aiohttp.ClientSession(
            connector=connector,
            auto_decompress=False,
            trust_env=False,
        ) as session:
            fetched_result = await fetch_provider_directory_rooted_graph_query(
                session,
                api_base,
                claim,
            )
    assert [resource["id"] for resource in fetched_result.resources] == [
        "role.synthetic-1",
        "role.synthetic-2",
    ]
    assert requests == [
        (
            "/fhir/R4/PractitionerRole?"
            "practitioner=Practitioner%2Fpractitioner.synthetic-1&_count=100",
            "application/fhir+json",
            "identity",
        ),
        (
            "/fhir/R4/PractitionerRole?cursor=opaque%2Bvalue&cursor=second",
            "application/fhir+json",
            "identity",
        ),
    ]


@pytest.mark.asyncio
async def test_loopback_certificate_is_not_trusted_by_default(tmp_path) -> None:
    async with _loopback_server(tmp_path) as (api_base, _context, _requests):
        query = build_provider_directory_practitioner_role_query(
            api_base,
            "practitioner.synthetic-1",
        )
        claim = claim_for_query(query)
        async with aiohttp.ClientSession(
            auto_decompress=False,
            trust_env=False,
        ) as untrusted_session:
            with pytest.raises(ProviderDirectoryRootedGraphHTTPError) as error_info:
                await fetch_provider_directory_rooted_graph_query(
                    untrusted_session,
                    api_base,
                    claim,
                )
    assert error_info.value.retryable is True
    assert error_info.value.code == "transport_connection"
