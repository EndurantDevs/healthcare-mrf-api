# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control callable and acquisition custody cannot confer undeclared authority."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_source_acquisition_custody as acquisitions


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "replaced", "missing"])
async def test_candidate_lock_rechecks_exact_heap_identity_without_changing_incumbent(fault):
    members = [dict(name="incoming_second", oid=202), dict(name="incoming_first", oid=101)]
    before = deepcopy(members)
    catalog_records = [dict(relname=member["name"], oid=member["oid"]) for member in members]
    if fault == "replaced":
        catalog_records[0]["oid"] = 999
    elif fault == "missing":
        catalog_records.pop()
    connection = SimpleNamespace(execute=AsyncMock(), fetch=AsyncMock(return_value=catalog_records))
    if fault is None:
        await acquisitions._lock_candidates(connection, SimpleNamespace(schema="sample"), members)
    else:
        with pytest.raises(ValueError, match="source_acquisition_family_member_changed"):
            await acquisitions._lock_candidates(connection, SimpleNamespace(schema="sample"), members)
    assert members == before
    assert connection.execute.await_args.args == (
        'LOCK TABLE "sample"."incoming_first","sample"."incoming_second" IN ACCESS EXCLUSIVE MODE NOWAIT',
    )
    assert connection.execute.await_count == 1


@pytest.mark.asyncio
async def test_reused_acquisition_identifier_refuses_before_looking_up_other_families():
    acquisition = "pdpa_" + "1" * 40
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[dict(acquisition_id=acquisition, status="loading")] * 2), execute=AsyncMock()
    )
    with pytest.raises(ValueError, match="source_acquisition_acquisition_identity_reused"):
        await acquisitions._acquisitions(connection, "sample")
    assert connection.fetch.await_count == 1
    connection.execute.assert_not_awaited()
