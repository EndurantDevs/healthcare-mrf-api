# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real native TLS, revocation and key-pair signing over an isolated local protocol."""

from decimal import Decimal

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from snowflake.connector.errors import Error

from process.custom_import import snowflake_python
from process.custom_import.snowflake import SnowflakeKeyPairCredentials
from tests.snowflake_tls_test_support import SCALAR_QUERY, local_connector_endpoint


def _credentials():
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    credentials = SnowflakeKeyPairCredentials(
        account="example",
        user="reader",
        private_key_pem=key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        ),
    )
    return key, credentials


@pytest.mark.parametrize("revocation", ("valid", "unavailable", "invalid", "missing", "revoked"))
def test_native_key_pair_tls_enforces_revocation_and_bounded_session(tmp_path, monkeypatch, revocation):
    monkeypatch.setenv("SNOWFLAKE_HOME", str(tmp_path / "connector-config"))
    key, credentials = _credentials()
    native_connect = snowflake_python.snowflake.connector.connect
    with local_connector_endpoint(tmp_path, revocation=revocation) as endpoint:

        def route_local(**arguments):
            return native_connect(
                **arguments,
                server_url=endpoint.url,
                custom_root_store_path=str(endpoint.trust_file),
                crl_cache_dir=str(tmp_path / "revocation-cache"),
                client_out_of_band_telemetry_enabled=False,
                use_proxy_env=False,
                retry_max_attempts=1,
            )

        monkeypatch.setattr(snowflake_python.snowflake.connector, "connect", route_local)
        adapter = snowflake_python.SnowflakePythonConnectorAdapter(role="READER_ROLE", warehouse="IMPORT_WH")
        if revocation in ("revoked", "unavailable", "invalid"):
            with pytest.raises(Error, match="CRL validation failed"):
                adapter._connect(credentials, timeout_seconds=3)
            assert not endpoint.requests
            assert endpoint.revocation_requests
            return
        connection, cursor = adapter._connect(credentials, timeout_seconds=3)
        try:
            login = next(body["data"] for _, path, body in endpoint.requests if "login-request" in path)
            claims = jwt.decode(login["TOKEN"], key.public_key(), algorithms=["RS256"])
            assert claims["sub"] == "EXAMPLE.READER"
            assert claims["iss"].startswith("EXAMPLE.READER.SHA256:")
            assert claims["exp"] > claims["iat"]
            wrong_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
            with pytest.raises(jwt.InvalidSignatureError):
                jwt.decode(login["TOKEN"], wrong_key.public_key(), algorithms=["RS256"])
            queries = [body["sqlText"] for _, path, body in endpoint.requests if "query-request" in path]
            assert queries == ["USE SECONDARY ROLES NONE"]
            assert connection.config.crl_check_mode == "ENABLED"
            assert connection.config.client_store_temporary_credential is False
            assert connection.config.enable_server_session_keep_alive_auto_detection is True
            assert connection.config.crl_connection_timeout == connection.config.crl_http_timeout == 3
            assert connection.config.query_timeout == connection.config.request_timeout == 3
            assert connection.config.connect_timeout == connection.config.retry_timeout == 3
            cursor.execute(SCALAR_QUERY)
            assert cursor.fetchone() == (7, Decimal("12.500000000000"), "example", True)
            assert cursor.fetchone() is None
            assert [snowflake_python._source_type(column) for column in cursor.description] == [
                "FIXED(38,0)",
                "FIXED(38,12)",
                "TEXT",
                "BOOLEAN",
            ]
        finally:
            cursor.close()
            connection.close()
            assert connection.is_closed()
            assert any("session?delete=true" in path for _, path, _ in endpoint.requests)
