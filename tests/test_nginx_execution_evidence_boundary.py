"""Exercise the public route boundary with a native Nginx and synthetic upstream."""

import shutil
import socket
import subprocess
import time
from http.client import HTTPConnection
from itertools import product
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_PATH = "/control/v1/custom-import/execution-evidence"
STOP_REQUEST_PATH = "/control/v1/custom-import/execution-stop-request"
STOP_FINALIZE_PATH = "/control/v1/custom-import/execution-stop-finalize"
ADMISSION_PATH = "/control/v1/custom-import/admission-batch"
SOURCE_PATH = "/control/v1/custom-import/source-batch"
PRIVATE_PATHS = (EVIDENCE_PATH, STOP_REQUEST_PATH, STOP_FINALIZE_PATH, ADMISSION_PATH, SOURCE_PATH)


def _unused_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def _request(port, path, *, no_store=False):
    connection = HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        connection.request("POST", path, headers={"Authorization": "Bearer synthetic"})
        reply = connection.getresponse()
        reply.read()
        if no_store:
            assert reply.getheader("Cache-Control") == "no-store"
        return reply.status, reply.getheader("X-Synthetic-Authorization")
    finally:
        connection.close()


def _configuration(tmp_path, public_port, upstream_port, *, external_port=None):
    configuration = (ROOT / "service/nginx.conf").read_text()
    for original, replacement in (
        ("user nobody nogroup;", ""),
        ("pid /run/nginx.pid;", f"pid {tmp_path}/nginx.pid;"),
        ("include /etc/nginx/mime.types;", "types {}"),
        ("listen       8080 default;", f"listen 127.0.0.1:{public_port} default;"),
        ("listen       8082;", f"listen 127.0.0.1:{external_port or _unused_port()};"),
        ("8080 internal;", f"{public_port} internal;"),
        ("http://127.0.0.1:8081", f"http://127.0.0.1:{upstream_port}"),
        ("/etc/nginx/private/*.conf", f"{tmp_path}/private/*.conf"),
    ):
        assert original in configuration
        configuration = configuration.replace(original, replacement)
    temporary_paths = "\n".join(
        f"{kind}_temp_path {tmp_path}/{kind};" for kind in ("client_body", "proxy", "fastcgi", "uwsgi", "scgi")
    )
    return configuration.replace(
        "http {",
        f"""http {{
            {temporary_paths}
            server {{
                listen 127.0.0.1:{upstream_port};
                add_header X-Synthetic-Authorization $http_authorization always;
                add_header X-Synthetic-Read-Boundary $http_x_plan_release_read_boundary always;
                return 204;
            }}""",
    )


def _wait_for_listener(process, port):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        assert process.poll() is None, process.stderr.read()
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.02)
    pytest.fail("Nginx listener did not become ready")


def _encoded_leaf(path):
    prefix, leaf = path.rsplit("/", 1)
    return prefix + "/%" + format(ord(leaf[0]), "02x") + leaf[1:]


@pytest.mark.parametrize("private_enabled", (False, True))
def test_evidence_is_private_and_other_routes_still_proxy(tmp_path, private_enabled):
    nginx = shutil.which("nginx")
    if nginx is None:
        pytest.skip("native Nginx is required for the routing check")
    public_port = _unused_port()
    external_port = _unused_port()
    upstream_port = _unused_port()
    private_port = _unused_port()
    private_directory = tmp_path / "private"
    if private_enabled:
        private_directory.mkdir()
        (private_directory / "synthetic.conf").write_text(
            f"""server {{
                listen 127.0.0.1:{private_port};
                {"".join(f"location = {path} {{ proxy_pass http://127.0.0.1:{upstream_port}; }}" for path in PRIVATE_PATHS)}
                location / {{ return 404; }}
            }}"""
        )
    else:
        assert not private_directory.exists()
    config_path = tmp_path / "nginx.conf"
    config_path.write_text(_configuration(tmp_path, public_port, upstream_port, external_port=external_port))
    command_parts = [nginx, "-p", str(tmp_path), "-c", str(config_path)]
    subprocess.run([*command_parts, "-t"], check=True, capture_output=True, text=True)
    with subprocess.Popen([*command_parts, "-g", "daemon off;"], stderr=subprocess.PIPE, text=True) as process:
        try:
            _wait_for_listener(process, public_port)
            _wait_for_listener(process, external_port)
            for port, private_path in product((public_port, external_port), PRIVATE_PATHS):
                for path in (
                    private_path,
                    private_path + "/",
                    private_path + "/nested",
                    private_path + "?probe=1",
                    private_path.replace("/control/v1/", "/control//v1/"),
                    _encoded_leaf(private_path),
                    private_path.replace("/custom-import/", "/custom-import%2f"),
                    "/api/../" + private_path.removeprefix("/"),
                ):
                    assert _request(port, path, no_store=private_path in (ADMISSION_PATH, SOURCE_PATH)) == (
                        404,
                        None,
                    ), path
            for path in (
                "/api/v1/healthcheck/live",
                "/control/v1/custom-import/another-route",
                EVIDENCE_PATH + "-status",
            ):
                assert _request(public_port, path) == (204, "Bearer synthetic")
            if private_enabled:
                for private_path in PRIVATE_PATHS:
                    assert _request(private_port, private_path) == (204, "Bearer synthetic")
                assert _request(private_port, "/api/v1/healthcheck/live") == (404, None)
        finally:
            process.terminate()
            process.wait(timeout=5)


def _read_boundary(port, path, forged_values):
    """Inspect the upstream boundary after arbitrary caller headers."""
    connection = HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        connection.putrequest("GET", path + "?%70lan_release_id=example", skip_host=True)
        connection.putheader("Host", "internal.invalid:8080")
        for value in forged_values:
            connection.putheader("X-Plan-Release-Read-Boundary", value)
        connection.endheaders()
        reply = connection.getresponse()
        reply.read()
        return reply.status, reply.getheader("X-Synthetic-Read-Boundary")
    finally:
        connection.close()


def test_listener_overwrites_forged_read_boundary_and_keeps_native_paths(tmp_path):
    nginx = shutil.which("nginx")
    if nginx is None:
        pytest.skip("native Nginx is required for the routing check")
    internal_port = _unused_port()
    external_port = _unused_port()
    upstream_port = _unused_port()
    config_path = tmp_path / "nginx.conf"
    config_path.write_text(
        _configuration(
            tmp_path,
            internal_port,
            upstream_port,
            external_port=external_port,
        )
    )
    command_parts = [nginx, "-p", str(tmp_path), "-c", str(config_path)]
    subprocess.run([*command_parts, "-t"], check=True, capture_output=True, text=True)
    with subprocess.Popen([*command_parts, "-g", "daemon off;"], stderr=subprocess.PIPE, text=True) as process:
        try:
            _wait_for_listener(process, external_port)
            for (port, expected), path, forged_values in product(
                ((external_port, "external"), (internal_port, "internal")),
                ("/api/v1/npi/all", "/api/v1/npi/near/", "/api/v1/pricing/providers/by-procedure"),
                ((), ("internal",), ("external",), ("internal", "external")),
            ):
                assert _read_boundary(port, path, forged_values) == (204, expected)
        finally:
            process.terminate()
            process.wait(timeout=5)
