# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Required-proof policy distinguishes CMS NPD serving from legacy CMS education."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import ServiceUnavailable

from api import provider_profile_snapshot as snapshot


@pytest.mark.parametrize(
    "has_history_table,has_profile_table,answers,expected",
    [
        (False, False, [], False),
        (False, True, [False], False),
        (False, True, [True], True),
        (True, True, [False, False], False),
        (True, True, [False, True], True),
        (True, False, [True], True),
        (True, True, [True], True),
    ],
)
async def test_required_proof_survives_profile_withdrawal(has_history_table, has_profile_table, answers, expected):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=answers))
    oid_by_relation = {
        "mrf.provider_directory_cms_serving_receipt": 1 if has_history_table else None,
        "mrf.provider_directory_profile_serving_generation": 2 if has_profile_table else None,
    }
    assert await snapshot._requires_cms_receipt(session, "mrf", oid_by_relation) is expected
    assert session.scalar.await_count == len(answers)


@pytest.mark.parametrize("has_receipt_table", [False, True])
async def test_missing_common_proof_is_unavailable(monkeypatch, has_receipt_table):
    read_receipt = AsyncMock(return_value=None)
    monkeypatch.setattr(snapshot.cms_serving_receipt, "read_serving_receipt", read_receipt)
    oid_by_relation = {"mrf.provider_directory_cms_serving_receipt": 1 if has_receipt_table else None}
    with pytest.raises(ServiceUnavailable, match="temporarily unavailable"):
        await snapshot._read_cms_receipt(SimpleNamespace(), "mrf", oid_by_relation)
    assert read_receipt.await_count == int(has_receipt_table)
