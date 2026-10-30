# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native run eligibility uses the maintained disposable snapshot fixture."""

import pytest

from db.models.system import ImportRun
from process import provider_directory_capacity_reservation_snapshot as snapshot
from process import provider_directory_serving_inverse_capacity_contract as inverse
from tests.test_provider_directory_capacity_reservation_snapshot_postgres import _fixture
from tests.test_provider_directory_serving_inverse_snapshot import inverse_run


@pytest.mark.asyncio
@pytest.mark.parametrize("has_envelope", [True, False])
async def test_active_inverse_is_observed_before_any_consumption(monkeypatch, has_envelope):
    async with _fixture(monkeypatch) as (fhir, engine, tables_by_name):
        run_by_field = inverse_run(status="queued")
        if not has_envelope:
            run_by_field["params"].pop(inverse.EXECUTION_FIELD)
        async with engine.begin() as connection:
            await connection.execute(tables_by_name[ImportRun.__tablename__].insert().values(**run_by_field))
        if not has_envelope:
            with pytest.raises(RuntimeError, match="run_purpose_changed"):
                await snapshot.capacity_reservation_snapshot(fhir)
            return
        observed = await snapshot.capacity_reservation_snapshot(fhir)
        assert len(observed["owners"]) == len(observed["reservations"]) == 1
        reservation = observed["reservations"][0]
        assert reservation["admission_purpose"] == inverse.PURPOSE
        assert observed["owners"][0]["reservation_ids"] == [reservation["reservation_id"]]
        assert [entry["kind"] for entry in reservation["observations"]] == ["import_run"]
        assert observed["preflight_receipts"] == [] and reservation["durable_preflight_present"] is False
