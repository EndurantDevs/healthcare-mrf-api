# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The captured-epoch fixture admits only explicit disposable service custody."""

import pytest
from sqlalchemy.engine import make_url

from tests.test_tiger_captured_epoch_postgres import _capture_admin_url


@pytest.mark.parametrize(
    "changed",
    (
        {"host": "untrusted.invalid"},
        {"host": "localhost"},
        {"port": 5440},
        {"username": "other"},
        {"database": "other"},
        {"query": {"host": "untrusted.invalid"}},
        {"drivername": "postgresql"},
    ),
)
def test_ci_capture_service_refuses_changed_connection_identity(monkeypatch, changed):
    monkeypatch.setenv("HLTHPRT_TIGER_CAPTURE_CI_TEST", "1")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    raw = "postgresql+asyncpg://postgres:synthetic@postgres:5432/postgres"
    with pytest.raises(pytest.fail.Exception, match="explicit disposable"):
        _capture_admin_url(make_url(raw).set(**changed).render_as_string(hide_password=False))


@pytest.mark.parametrize("mode,actions", (("0", "true"), ("true", "true"), ("1", "false"), ("1", "")))
def test_ci_capture_service_requires_literal_mode_and_actions(monkeypatch, mode, actions):
    monkeypatch.setenv("HLTHPRT_TIGER_CAPTURE_CI_TEST", mode)
    monkeypatch.setenv("GITHUB_ACTIONS", actions)
    with pytest.raises(pytest.fail.Exception, match="explicit disposable"):
        _capture_admin_url("postgresql+asyncpg://postgres:synthetic@postgres:5432/postgres")


@pytest.mark.parametrize("host", ("postgres", "127.0.0.1"))
def test_exact_ci_service_and_existing_loopback_boundary(monkeypatch, host):
    monkeypatch.setenv("HLTHPRT_TIGER_CAPTURE_CI_TEST", "1")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    url = make_url("postgresql+asyncpg://postgres:synthetic@postgres:5432/postgres").set(host=host)
    assert _capture_admin_url(url.render_as_string(hide_password=False)) == url
    monkeypatch.delenv("HLTHPRT_TIGER_CAPTURE_CI_TEST")
    assert _capture_admin_url("postgresql+asyncpg://synthetic@localhost:5440/postgres").host == "localhost"
    with pytest.raises(pytest.fail.Exception, match="local disposable"):
        _capture_admin_url("postgresql+asyncpg://postgres:synthetic@postgres:5432/postgres")
