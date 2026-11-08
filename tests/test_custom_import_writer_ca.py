# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed certificate trust and native TLS verification without sockets."""

import hashlib
import ssl
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
import pytest

from process.custom_import import admission_launch as launch
from process.custom_import import admission_worker as worker
from tests import test_custom_import_admission_worker as admission
from tests import test_custom_import_source_worker as source
from tests.admission_transport_test_support import WRITER_CA_PEM, writer_tls_context
from tests.test_provider_directory_rooted_graph_http_loopback import _tls_contexts


@pytest.fixture(autouse=True)
def fixed_clock(monkeypatch):
    monkeypatch.setattr(worker, "_utcnow", lambda: admission.NOW)


@pytest.mark.parametrize("factory", [admission._transport, source.transport])
@pytest.mark.parametrize("context", [None, True, "/untrusted/ca.pem", object()])
def test_both_transports_require_explicit_native_context(factory, context):
    with pytest.raises(worker.AdmissionTransportError):
        factory(lambda _: admission._reply(), tls_context=context)


@pytest.mark.parametrize("factory", [admission._transport, source.transport])
@pytest.mark.parametrize("disable_certificate_verification", [False, True])
def test_both_transports_reject_weakened_context(factory, disable_certificate_verification):
    context = writer_tls_context()
    context.check_hostname = False
    if disable_certificate_verification:
        context.verify_mode = ssl.CERT_NONE
    with pytest.raises(worker.AdmissionTransportError):
        factory(lambda _: admission._reply(), tls_context=context)


@pytest.mark.parametrize("factory", [admission._transport, source.transport])
async def test_both_clients_receive_the_exact_context_and_no_environment(factory, monkeypatch):
    context = writer_tls_context()
    original, options = httpx.AsyncClient, []

    def client(**kwargs):
        options.append(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", client)
    async with factory(lambda _: admission._reply(), tls_context=context):
        assert options[0]["verify"] is context
        assert options[0]["trust_env"] is False
        assert options[0]["follow_redirects"] is False


def _handshake(server_context, client_context, hostname):
    """Drive actual SSL objects through bounded in-memory TLS records."""

    client_in, client_out, server_in, server_out = ssl.MemoryBIO(), ssl.MemoryBIO(), ssl.MemoryBIO(), ssl.MemoryBIO()
    client = client_context.wrap_bio(client_in, client_out, server_hostname=hostname)
    server = server_context.wrap_bio(server_in, server_out, server_side=True)
    completed_flags = [False, False]
    for _ in range(16):
        for index, (peer, outbound, inbound) in enumerate(
            ((client, client_out, server_in), (server, server_out, client_in))
        ):
            if not completed_flags[index]:
                try:
                    peer.do_handshake()
                    completed_flags[index] = True
                except ssl.SSLWantReadError:
                    completed_flags[index] = False
            data = outbound.read()
            if data:
                inbound.write(data)
        if all(completed_flags):
            return
    pytest.fail("TLS handshake exceeded its finite record budget")


@pytest.mark.parametrize("failure", [None, "untrusted", "hostname"])
def test_native_tls_uses_only_pinned_ca_and_checks_hostname(tmp_path, failure):
    with TemporaryDirectory(dir=tmp_path) as temporary:
        directory = Path(temporary)
        server, _ = _tls_contexts(directory)
        (directory / "loopback-key.pem").chmod(0o600)
        raw = (directory / "loopback-certificate.pem").read_bytes()
        if failure == "untrusted":
            raw = WRITER_CA_PEM
        context = launch._writer_tls_context(raw, hashlib.sha256(raw).hexdigest())
        hostname = "other.example.invalid" if failure == "hostname" else "localhost"
        if failure:
            with pytest.raises(ssl.SSLCertVerificationError):
                _handshake(server, context, hostname)
        else:
            _handshake(server, context, hostname)
    assert not directory.exists()
