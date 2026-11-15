# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Structural registration locks remain available for published SELECT-only inputs."""

import pytest

from tests.test_cms_native_input_registration import NativeProtocol, _enroll


@pytest.mark.asyncio
async def test_registration_locks_allow_published_reads_and_read_only_retry():
    native = NativeProtocol()
    await _enroll(native)
    statements = [sql for kind, sql, _args in native.calls if kind == "execute"]
    assert all(sql.endswith(" IN ACCESS SHARE MODE NOWAIT") for sql in statements if sql.startswith("LOCK TABLE"))
    assert any(sql.startswith("CREATE TRIGGER") for sql in statements)
    assert any(sql.startswith("ALTER TABLE") for sql in statements)
    native.calls.clear()
    await _enroll(native)
    statements = [sql for kind, sql, _args in native.calls if kind == "execute"]
    assert statements and all(
        sql.startswith("LOCK TABLE") and sql.endswith(" IN ACCESS SHARE MODE NOWAIT") for sql in statements
    )
