# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Local TLS and protocol witnesses for the installed native connector."""

import gzip
import ipaddress
import json
import ssl
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from types import SimpleNamespace

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

SCALAR_QUERY = "SELECT 7 AS integer_value, 12.500000000000::NUMBER(38,12) AS amount, 'example' AS label, TRUE AS active"


def _scalar_result():
    columns = [
        dict(name="INTEGER_VALUE", type="fixed", precision=38, scale=0, nullable=False),
        dict(name="AMOUNT", type="fixed", precision=38, scale=12, nullable=False),
        dict(name="LABEL", type="text", nullable=True, length=16777216),
        dict(name="ACTIVE", type="boolean", nullable=False),
    ]
    return dict(rowtype=columns, rowset=[["7", "12.500000000000", "example", "true"]], total=1, returned=1)


def _authority():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Synthetic Test Authority")])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=100))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
        .sign(key, hashes.SHA256())
    )
    return key, certificate


def _server_certificate(authority, distribution_url):
    key, issuer = authority
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]))
        .issuer_name(issuer.subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=60))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(key.public_key()), critical=False)
    )
    if distribution_url:
        point = x509.DistributionPoint(
            full_name=[x509.UniformResourceIdentifier(distribution_url)],
            relative_name=None,
            reasons=None,
            crl_issuer=None,
        )
        builder = builder.add_extension(x509.CRLDistributionPoints([point]), critical=False)
    return server_key, builder.sign(key, hashes.SHA256())


def _revocations(authority, certificate, revoked):
    key, issuer = authority
    now = datetime.now(timezone.utc)
    builder = (
        x509.CertificateRevocationListBuilder()
        .issuer_name(issuer.subject)
        .last_update(now - timedelta(minutes=1))
        .next_update(now + timedelta(hours=1))
        .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(key.public_key()), critical=False)
        .add_extension(x509.CRLNumber(1), critical=False)
    )
    if revoked:
        builder = builder.add_revoked_certificate(
            x509.RevokedCertificateBuilder()
            .serial_number(certificate.serial_number)
            .revocation_date(now - timedelta(minutes=1))
            .build()
        )
    return builder.sign(key, hashes.SHA256()).public_bytes(serialization.Encoding.DER)


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *_arguments):
        return None

    def do_GET(self):
        self.server.requests.append(("GET", self.path, None))
        content = self.server.revocations
        self.send_response(200 if content is not None else 503)
        self.end_headers()
        if content is not None:
            self.wfile.write(content)

    def do_POST(self):
        contents = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        if self.headers.get("Content-Encoding") == "gzip":
            contents = gzip.decompress(contents)
        request_payload = json.loads(contents or b"{}")
        self.server.requests.append(("POST", self.path, request_payload))
        if "login-request" in self.path:
            response_by_field = dict(
                token="session",
                masterToken="master",
                validityInSeconds=3600,
                masterValidityInSeconds=3600,
                sessionId=1,
                serverVersion="9.0.0",
                sessionInfo=dict(roleName="READER_ROLE", warehouseName="IMPORT_WH"),
                parameters=[],
            )
        else:
            response_by_field = dict(
                queryId="00000000-0000-0000-0000-000000000001",
                queryResultFormat="json",
                rowtype=[],
                rowset=[],
                total=0,
                parameters=[],
            )
            if request_payload.get("sqlText") == SCALAR_QUERY:
                response_by_field.update(_scalar_result())
        content = json.dumps(dict(success=True, data=response_by_field)).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)


@contextmanager
def _server(context=None):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.requests, server.revocations = [], None
    try:
        if context is not None:
            server.socket = context.wrap_socket(server.socket, server_side=True)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server
        finally:
            server.shutdown()
            thread.join(timeout=5)
            assert not thread.is_alive()
    finally:
        server.server_close()


@contextmanager
def local_connector_endpoint(tmp_path, *, revocation):
    """Use only newly created loopback listeners, trust files and CRL caches."""
    authority = _authority()
    root_file = tmp_path / "authority.pem"
    root_file.write_bytes(authority[1].public_bytes(serialization.Encoding.PEM))
    with _server() as distribution:
        address = f"http://127.0.0.1:{distribution.server_port}/revocations.der"
        key, certificate = _server_certificate(authority, None if revocation == "missing" else address)
        if revocation != "unavailable":
            distribution.revocations = _revocations(authority, certificate, revocation == "revoked")
        if revocation == "invalid":
            distribution.revocations = b"invalid"
        cert_file, key_file = tmp_path / "server.pem", tmp_path / "server-key.pem"
        cert_file.write_bytes(
            certificate.public_bytes(serialization.Encoding.PEM) + authority[1].public_bytes(serialization.Encoding.PEM)
        )
        key_file.write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
            )
        )
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert_file, key_file)
        with _server(context) as endpoint:
            yield SimpleNamespace(
                url=f"https://127.0.0.1:{endpoint.server_port}",
                trust_file=root_file,
                requests=endpoint.requests,
                revocation_requests=distribution.requests,
            )
