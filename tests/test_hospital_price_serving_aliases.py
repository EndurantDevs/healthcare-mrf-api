# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

from api import hospital_price_serving as serving
from api.hospital_price_serving_sql import VERSION_SQL
from tests.test_hospital_price_serving import _query
from tests.test_hospital_price_serving import _Session


@pytest.mark.asyncio
async def test_canonical_hospital_reads_latest_publication_from_alias_group(monkeypatch):
    session = _Session()
    monkeypatch.setattr(serving, "_NATIVE", session.native)

    page = await serving.read_hospital_price_page(
        session,
        _query(
            hospital_id="hospital-005657", payer_name=None, plan_name=None
        ),
    )

    version_params = next(
        params for statement, params in session.statements if statement is VERSION_SQL
    )
    assert version_params["hospital_ids"] == (
        "hospital-005657", "hospital-005658",
    )
    assert page["hospital_id"] == "hospital-005657"
    with pytest.raises(serving.HospitalPriceInvalidRequestError):
        await serving.read_hospital_price_page(
            session,
            _query(
                hospital_id="hospital-005658", payer_name=None, plan_name=None,
                cursor=page["pagination"]["next_cursor"],
            ),
        )
