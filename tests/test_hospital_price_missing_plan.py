# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact absent-plan queries remain distinct from named plans and no filter."""

from dataclasses import replace
from pathlib import Path
import runpy

import pytest

from api import hospital_price_serving as serving
from api.hospital_price_request import decode_hospital_price_cursor, encode_hospital_price_cursor
from api.hospital_price_request import MISSING_PLAN_SELECTOR, validate_hospital_price_plan
from db.models.hospital_price_facts import HospitalPricePayerCharge
from db.models.hospital_price_header import HospitalPriceVersion
from support.hospital_price_native_validation import HOSPITAL_MRF_PACKED_V6_PARSER_CONTRACT_SHA256
from support.hospital_price_native_validation import HOSPITAL_MRF_PARSER_CONTRACT_SHA256
from tests.test_hospital_price_serving import _query, _Session, VERSION_ID


@pytest.mark.parametrize("values", [
    {"plan_missing": "true"},
    {"plan_missing": "true", "plan_name": None, "payer_name": None},
    {"plan_missing": "false", "plan_name": None},
    {"plan_missing": True, "plan_name": None},
    {"plan_missing": "", "plan_name": None},
])
def test_missing_plan_selector_is_explicit(values):
    with pytest.raises(serving.HospitalPriceInvalidRequestError):
        plan = validate_hospital_price_plan(values.get("plan_name", "Plan"), values["plan_missing"])
        _query(plan_name=plan, payer_name=values.get("payer_name", "Payer"))


def test_missing_plan_cursor_cannot_cross_scope():
    missing = _query(plan_name=validate_hospital_price_plan(None, "true"))
    cursor = encode_hospital_price_cursor(missing, VERSION_ID, 7)
    assert decode_hospital_price_cursor(replace(missing, cursor=cursor), VERSION_ID) == 7
    for other in [_query(), _query(payer_name=None, plan_name=None)]:
        with pytest.raises(serving.HospitalPriceInvalidRequestError):
            decode_hospital_price_cursor(replace(other, cursor=cursor), VERSION_ID)
        with pytest.raises(serving.HospitalPriceInvalidRequestError):
            decode_hospital_price_cursor(replace(missing,
                cursor=encode_hospital_price_cursor(other, VERSION_ID, 7)), VERSION_ID)


@pytest.mark.asyncio
async def test_missing_plan_pages_return_null_only(monkeypatch):
    session = _Session()
    for ordinal in session.native.payer_refs:
        session.native.fact_rows[ordinal]["plan_name"] = None
    monkeypatch.setattr(serving, "_NATIVE", session.native)
    query = _query(plan_name=MISSING_PLAN_SELECTOR)
    first = await serving.read_hospital_price_page(session, query)
    assert first["query"]["plan_missing"] is True
    assert first["items"][0]["negotiated_prices"][0]["plan_name"] is None
    second = await serving.read_hospital_price_page(session,
        replace(query, cursor=first["pagination"]["next_cursor"]))
    assert second["items"][0]["negotiated_prices"][0]["plan_name"] is None
    assert second["pagination"]["next_cursor"] is None
    session.native.fact_rows[0]["plan_name"] = "Named"
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="identity"):
        await serving.read_hospital_price_page(session, query)


def test_missing_plan_admission_preserves_header_guards():
    migration = runpy.run_path(str(Path(__file__).resolve().parents[1] / "alembic" / "versions" /
        "20260907220000_hospital_price_missing_plan.py"))
    _drop, statement = migration["_upgrade_statements"]()
    shape = next(str(constraint.sqltext) for constraint in HospitalPriceVersion.__table__.constraints
        if constraint.name == "hospital_price_version_shape_check")
    assert statement.endswith(f"CHECK ({shape});")
    assert statement.count(HOSPITAL_MRF_PARSER_CONTRACT_SHA256) == 2
    assert statement.count(HOSPITAL_MRF_PACKED_V6_PARSER_CONTRACT_SHA256) == 2
    assert HospitalPricePayerCharge.__table__.c.plan_name.nullable is False
