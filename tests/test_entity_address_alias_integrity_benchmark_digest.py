# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from tests.entity_address_alias_integrity_benchmark_lifecycle import _table_digest


def test_table_digest_excludes_evidence_id_from_both_json_values():
    database = SimpleNamespace(scalar=AsyncMock(return_value="digest"))

    assert asyncio.run(_table_digest(database, "mrf", "entity_address_evidence")) == "digest"
    assert database.scalar.await_args.args[0].count("'evidence_id'") == 2
