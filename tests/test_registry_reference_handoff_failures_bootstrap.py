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
    if grantable or not role_exists:
        with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError):
            await bootstrap._preserve_reads(session, child_by_field, {"oid": 101}, '"archive"."nucc_taxonomy"')
        session.execute.assert_not_awaited()
        return
    await bootstrap._preserve_reads(session, child_by_field, {"oid": 101}, '"archive"."nucc_taxonomy"')
    assert session.scalar.await_count == 1
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements == [
        'GRANT USAGE ON SCHEMA "archive" TO "legacy_reader"',
        'GRANT SELECT ON "mrf"."nucc_taxonomy","archive"."nucc_taxonomy" TO "legacy_reader"',
        'GRANT USAGE ON SCHEMA "archive" TO PUBLIC',
        'GRANT SELECT ON "mrf"."nucc_taxonomy","archive"."nucc_taxonomy" TO PUBLIC',
    ]
