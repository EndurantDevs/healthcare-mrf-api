# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable complete-input oracle and unpublished candidate rollback regressions."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from api import plan_pricing_code_build as build
from api import plan_pricing_code_calibration as calibration
from api import plan_pricing_code_zip_build as scoped
from api.plan_pricing_code_contract import identity_digest
from api.plan_pricing_code_oracle import CodeInputOracle
from tests import test_plan_pricing_code_postgres as original
from tests import test_plan_pricing_code_zip_postgres as zip_case
from tests.test_plan_pricing_code_zip import _scope


async def assert_calibration_native(monkeypatch, fault):
    """Dense duplicates/nonmatches and original source/codecs cannot yield partial proof."""
    with pytest.raises(ValueError, match="not calibrated"):
        scoped.work_limits()
    monkeypatch.setattr(
        original,
        "_SOURCE_TABLES",
        original._SOURCE_TABLES
        + (
            "entity_address_unified",
            "npi_taxonomy",
            "npi",
            "geo_zip_lookup",
            "npi_address",
            "mrf_address",
            "doctor_clinician_address",
        ),
    )
    async with original._database(monkeypatch) as db:
        await _exercise_native_case(db, monkeypatch, fault)
        with pytest.raises(ValueError, match="not calibrated"):
            scoped.work_limits()


async def _exercise_native_case(db, monkeypatch, fault):
    """Exercise the actual rollback target over independently owned native input."""
    oid = await original._source_database(db)
    base = original._fixture(monkeypatch, db, oid)
    await zip_case._providers(db, monkeypatch)
    identity = zip_case._scoped_inputs(monkeypatch, base, drift=fault == "identity")
    async with db.sessions() as session, session.begin():
        await zip_case._dense_raw_address_oracle(session)

    _install_native_transport(db, monkeypatch)
    checked_cases = []
    monkeypatch.setattr(CodeInputOracle, "verify", _fault_verifier(fault, checked_cases))
    params_by_name = {
        name: identity[name] for name in ("plan_release_id", "serving_revision_id", "code_system", "code")
    }
    params_by_name.update(
        expected_identity_digest=identity_digest(identity),
        operation="calibrate",
        scope_kind="zip_radius",
        zip5=identity["scope"]["zip5"],
        zip_radius_miles=identity["scope"]["zip_radius_miles"],
    )
    context_by_field = {"audit": {}, "phase_timings": {}}
    await _run_calibration_case(params_by_name, context_by_field, fault)
    assert checked_cases == [True]
    assert context_by_field["audit"]["calibration"]["cleanup_verified"]


def _install_native_transport(db, monkeypatch):
    """Keep only fixture release selection synthetic; native lifecycle remains unchanged."""

    @asynccontextmanager
    async def transaction():
        async with db.sessions() as session, session.begin():
            yield session

    async def inputs(session, params, state_by_field):
        # The established fixture supplies the source/release identity; native
        # Publisher custody, stage writes, comparisons and cleanup stay genuine.
        await calibration.native.protected_publisher_owner(session)
        captured, bindings = await build.resolve_inputs(session, None, ("CPT", "27447"), scope=_scope())
        state_by_field.update(
            identity=captured, publication_before=await calibration._publication_state(session, captured)
        )
        return SimpleNamespace(), captured, bindings, _scope()

    monkeypatch.setattr(calibration.db, "transaction", transaction)
    monkeypatch.setattr(calibration, "_resolved_inputs", inputs)


def _fault_verifier(fault, checked_cases):
    """Inject late comparison faults after genuine candidate preparation."""
    original_verify = CodeInputOracle.verify

    async def verify(oracle, audit, ownership):
        checked_cases.append(True)
        if fault == "membership":
            await oracle.session.execute(
                text(f'DELETE FROM "{ownership.schema_name}".plan_pricing_code_membership WHERE npi=1000000001')
            )
        if fault == "work":
            oracle.work["factor"] += 1
        if fault == "cancelled":
            raise asyncio.CancelledError
        return await original_verify(oracle, audit, ownership)

    return verify


async def _run_calibration_case(params, context_by_field, fault):
    """A refused or cancelled comparison must retain verified cleanup and no success."""
    messages_by_fault = {
        "membership": "independent input multiset differs",
        "work": "independent work census differs",
        "identity": "input identity changed during preparation",
    }
    if fault:
        error = asyncio.CancelledError if fault == "cancelled" else ValueError
        with pytest.raises(error, match=messages_by_fault.get(fault)):
            await calibration.calibrate(params, context_by_field)
        assert not context_by_field["audit"]["calibration"]["complete"]
    else:
        receipt = (await calibration.calibrate(params, context_by_field))["calibration"]
        assert receipt["complete"] and receipt["rolled_back"] and receipt["cleanup_verified"]
        assert receipt["oracle"]["rates"]["rate_profiles"]
        assert receipt["oracle"]["work"]["raw_address"] > receipt["oracle"]["work"]["address"]
