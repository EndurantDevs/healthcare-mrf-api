"""Exercise the public route boundary with a native Nginx and synthetic upstream."""

import shutil
import socket
import subprocess
import time
from http.client import HTTPConnection
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_PATH = "/control/v1/custom-import/execution-evidence"


def _unused_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def _request(port, path):
    connection = HTTPConnection("127.0.0.1", port, timeout=2)
    try:
        connection.request("POST", path, headers={"Authorization": "Bearer synthetic"})
        reply = connection.getresponse()
        reply.read()
        return reply.status, reply.getheader("X-Synthetic-Authorization")
    finally:
        connection.close()


def _configuration(tmp_path, public_port, upstream_port):
    configuration = (ROOT / "service/nginx.conf").read_text()
    for original, replacement in (
        ("user nobody nogroup;", ""),
        ("pid /run/nginx.pid;", f"pid {tmp_path}/nginx.pid;"),
        ("include /etc/nginx/mime.types;", "types {}"),
        ("listen       8080 default;", f"listen 127.0.0.1:{public_port} default;"),
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


@pytest.mark.parametrize("private_enabled", (False, True))
def test_evidence_is_private_and_other_routes_still_proxy(tmp_path, private_enabled):
    nginx = shutil.which("nginx")
    if nginx is None:
        pytest.skip("native Nginx is required for the routing check")
    public_port = _unused_port()
    upstream_port = _unused_port()
    private_port = _unused_port()
    private_directory = tmp_path / "private"
    if private_enabled:
        private_directory.mkdir()
        (private_directory / "synthetic.conf").write_text(
            f"""server {{
                listen 127.0.0.1:{private_port};
                location = {EVIDENCE_PATH} {{ proxy_pass http://127.0.0.1:{upstream_port}; }}
                location / {{ return 404; }}
            }}"""
        )
    else:
        assert not private_directory.exists()
    config_path = tmp_path / "nginx.conf"
    config_path.write_text(_configuration(tmp_path, public_port, upstream_port))
    command_parts = [nginx, "-p", str(tmp_path), "-c", str(config_path)]
    subprocess.run([*command_parts, "-t"], check=True, capture_output=True, text=True)
    with subprocess.Popen([*command_parts, "-g", "daemon off;"], stderr=subprocess.PIPE, text=True) as process:
        try:
            _wait_for_listener(process, public_port)
            for path in (
                EVIDENCE_PATH,
                EVIDENCE_PATH + "/",
                EVIDENCE_PATH + "/nested",
                EVIDENCE_PATH + "?probe=1",
                "/control//v1/custom-import/execution-evidence",
                "/control/v1/custom-import/%65xecution-evidence",
                "/control/v1/custom-import%2fexecution-evidence",
                "/api/../control/v1/custom-import/execution-evidence",
            ):
                assert _request(public_port, path) == (404, None), path
            for path in (
                "/api/v1/healthcheck/live",
                "/control/v1/custom-import/another-route",
                EVIDENCE_PATH + "-status",
            ):
                assert _request(public_port, path) == (204, "Bearer synthetic")
            if private_enabled:
                assert _request(private_port, EVIDENCE_PATH) == (204, "Bearer synthetic")
                assert _request(private_port, "/api/v1/healthcheck/live") == (404, None)
        finally:
            process.terminate()
            process.wait(timeout=5)
