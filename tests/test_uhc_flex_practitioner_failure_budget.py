# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""New Flex seals/admissions obey the request budget without rewriting history."""

from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from process import uhc_flex_practitioner_result_store as result_store
from process import uhc_flex_practitioner_twin_store as twin_store
from process.uhc_flex_practitioner_single_root_contract import build_single_root_admission
from process.uhc_flex_practitioner_store_contract import UHCFlexPractitionerStoreError
from process.uhc_flex_practitioner_twin_store_contract import UHCFlexPractitionerTwinStoreError
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import cohort_fixture
from tests.test_uhc_flex_practitioner_storage_boundaries import (
    _Database as SealDatabase,
    _header_row,
    _identity,
)
from tests.test_uhc_flex_practitioner_twin_boundaries import (
    _Database as AdmissionDatabase,
    _single_root,
)
from tests.test_uhc_flex_practitioner_twin_store_contract import (
    OPERATION_KEY,
    PROJECTION_DATE,
    TIMESTAMP,
)


def _partial_header(identity):
    return _header_row(identity, status="sealed") | {
        "matched_count": identity.expected_npi_count - 1,
        "unmatched_count": 0,
        "error_count": 1,
        "cohort_complete": False,
    }


@pytest.mark.asyncio
async def test_new_seal_rejects_over_budget_but_historical_seal_stays_readable():
    identity = _identity()
    partial = _partial_header(identity)
    existing = await result_store.seal_uhc_flex_practitioner_acquisition(
        identity, database=SealDatabase(first_rows=(partial,)),
    )
    assert existing.error_count == 1 and not existing.cohort_complete
    with pytest.raises(UHCFlexPractitionerStoreError):
        await result_store.seal_uhc_flex_practitioner_acquisition(
            identity,
            database=SealDatabase(first_rows=(
                _header_row(identity), _header_row(identity), partial,
            )),
        )


@pytest.mark.asyncio
async def test_new_seal_sql_requires_terminal_conservation_and_strict_budget():
    identity = _identity()
    database = SealDatabase(first_rows=(_header_row(identity, status="sealed"),))
    await result_store._seal_building_header(database, identity.acquisition_id)
    sql, parameters = database.statements[-1]
    assert "census.pending_count = 0 AND census.leased_count = 0" in sql
    assert "census.matched_count + census.unmatched_count + census.error_count" in sql
    assert "census.error_count * :failure_budget_multiplier" in sql
    assert "< acquisition.expected_npi_count" in sql
    assert parameters["failure_budget_multiplier"] == 50


@pytest.mark.asyncio
@pytest.mark.parametrize("has_existing_admission", [False, True])
async def test_over_budget_admission_only_replays_exact_existing_authority(
    monkeypatch, has_existing_admission,
):
    cohort = cohort_fixture()
    candidate = replace(
        _single_root(cohort.cohort_id, cohort.npi_count),
        error_count=1, cohort_complete=False,
    )
    admission = build_single_root_admission(
        candidate, semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY, admitted_at=TIMESTAMP,
    )
    monkeypatch.setattr(twin_store, "_lock_single_root", AsyncMock(return_value=candidate))
    insert = AsyncMock()
    monkeypatch.setattr(twin_store, "_insert_admission", insert)
    read = AsyncMock(return_value=admission) if has_existing_admission else AsyncMock(
        side_effect=UHCFlexPractitionerTwinStoreError("missing")
    )
    monkeypatch.setattr(twin_store, "_read_admission", read)
    admission_arguments_by_name = dict(
        semantic_projection_as_of=PROJECTION_DATE,
        operation_key=OPERATION_KEY, database=AdmissionDatabase(),
    )
    if has_existing_admission:
        assert await twin_store.admit_uhc_flex_practitioner_single_root(
            candidate.acquisition_id, **admission_arguments_by_name,
        ) is admission
    else:
        with pytest.raises(UHCFlexPractitionerTwinStoreError):
            await twin_store.admit_uhc_flex_practitioner_single_root(
                candidate.acquisition_id, **admission_arguments_by_name,
            )
    insert.assert_not_awaited()
