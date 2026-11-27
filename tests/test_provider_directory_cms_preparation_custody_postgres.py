# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named input reads use native access; they do not retain physical identity evidence."""

import asyncpg
import pytest

from process import provider_directory_generic_source_storage as storage
from tests.cms_native_input_postgres_support import native_case as native_case


async def test_native_named_kernel_supports_a_readonly_caller_snapshot(native_case):
    case = native_case
    names = [relation["relation_name"] for relation in case.inventory["relations"]]
    async with case.admin.transaction(readonly=True):
        observed = await storage._capture(case.admin, case.policy, names)
        assert observed == {name: {"schema": case.policy.schema, "name": name} for name in names}
        with pytest.raises(asyncpg.ReadOnlySQLTransactionError):
            async with case.admin.transaction():
                await case.admin.execute("INSERT INTO private_source.frozen_input_0 VALUES(42)")
