# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read ACL and baseline codecs; synthetic replies never establish native adoption authority."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nucc_protected_bootstrap as bootstrap

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("grantable,role_exists", [(False, True), (True, True), (False, False)])
async def test_preserved_reads_never_replay_delegation_or_missing_roles(grantable, role_exists):
    child_by_field = {
        "acl": [
            [110, 110, "INSERT", False],
            [102, 110, "SELECT", grantable],
            [102, 110, "SELECT", grantable],
            [101, 101, "SELECT", False],
            [0, 110, "SELECT", False],
        ]
    }
    session = SimpleNamespace(
        scalar=AsyncMock(return_value='"legacy_reader"' if role_exists else None), execute=AsyncMock()
    )
    # Reads come from the configured reader policy; the retired heap's ACL is never replayed.
    owner_by_field = {"readers": ("legacy_reader", "api_reader")}
    await bootstrap._preserve_reads(session, child_by_field, owner_by_field, '"archive"."nucc_taxonomy"')
    session.scalar.assert_not_awaited()
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements == [
        'GRANT USAGE ON SCHEMA "archive" TO "legacy_reader"',
        'GRANT SELECT ON "mrf"."nucc_taxonomy","archive"."nucc_taxonomy" TO "legacy_reader"',
        'GRANT USAGE ON SCHEMA "archive" TO "api_reader"',
        'GRANT SELECT ON "mrf"."nucc_taxonomy","archive"."nucc_taxonomy" TO "api_reader"',
    ]
    assert not any("GRANT OPTION" in statement or "PUBLIC" in statement for statement in statements)
