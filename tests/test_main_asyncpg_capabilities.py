# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The server CLI capability override matches the installed asyncpg contract."""

from asyncpg import connection
from asyncpg.connection import ServerCapabilities
from click.testing import CliRunner

import main


def test_server_cli_constructs_real_asyncpg_capabilities(monkeypatch):
    observed_capabilities = []
    run_options_by_name = {}

    def run_server(_app, **options):
        run_options_by_name.update(options)
        observed_capabilities.append(
            connection._detect_server_capabilities((18, 2), {})
        )

    monkeypatch.setenv("HLTHPRT_PTG_WAVE_RECEIPT_AUTHORITY_ROLE", "reader")
    monkeypatch.setattr(
        connection, "_detect_server_capabilities", connection._detect_server_capabilities
    )
    monkeypatch.setattr(type(main.api), "run", run_server)
    monkeypatch.setattr(main.logging.config, "dictConfig", lambda _config: None)
    loop = main._new_event_loop()
    monkeypatch.setattr(main.asyncio, "get_event_loop", lambda: loop)
    try:
        command_result = CliRunner().invoke(
            main.cli,
            ["server", "start", "--host", "127.0.0.1", "--port", "8081", "--workers", "1"],
        )
    finally:
        loop.close()

    assert command_result.exit_code == 0, repr(command_result.exception)
    assert run_options_by_name["host"] == "127.0.0.1"
    assert run_options_by_name["port"] == 8081
    assert run_options_by_name["workers"] == 1
    assert len(observed_capabilities) == 1
    assert isinstance(observed_capabilities[0], ServerCapabilities)
    assert observed_capabilities[0]._asdict() == {
        "advisory_locks": False,
        "notifications": False,
        "plpgsql": False,
        "sql_reset": False,
        "sql_close_all": False,
        "sql_copy_from_where": False,
        "jit": False,
    }
