# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed boundaries for Practitioner acquisition and twin storage."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import uhc_flex_practitioner_result_store as result_store
from process import uhc_flex_practitioner_store as store
from process import uhc_flex_practitioner_store_contract as store_contract
from process import uhc_flex_practitioner_store_support as store_support
from process import uhc_flex_practitioner_stage as stage
from tests.test_uhc_flex_practitioner_store import (
    _identity,
    _matched_result,
    _terminal_claim,
    INTENT_ID,
    NPI,
    RUN_ID,
)
from tests.test_uhc_flex_practitioner_twin_store_contract import TIMESTAMP
from tests.provider_directory_uhc_flex_npi_cohort_pg_support import cohort_fixture


class _Database:
    def __init__(self, *, first_rows=(), status_counts=(), all_rows=()) -> None:
        self.first_rows = iter(first_rows)
        self.status_counts = iter(status_counts)
        self.all_rows = list(all_rows)
        self.statements = []

    @asynccontextmanager
    async def transaction(self):
        yield self

    async def scalar(self, statement, **parameters):
        self.statements.append((statement, parameters))
        return TIMESTAMP

    async def status(self, statement, **parameters):
        self.statements.append((statement, parameters))
        return next(self.status_counts, 1)

    async def first(self, statement, **parameters):
        self.statements.append((statement, parameters))
        return next(self.first_rows, None)

    async def all(self, statement, **parameters):
        self.statements.append((statement, parameters))
        return self.all_rows


@pytest.mark.asyncio
@pytest.mark.parametrize("has_transaction, count", ((False, 1), (True, 0), (True, 17)))
async def test_resource_copy_requires_transaction_and_bounded_response(monkeypatch, has_transaction, count):
    """Invalid response admission never prepares or copies a temporary heap."""
    database = SimpleNamespace(scalar=AsyncMock())
    copier = AsyncMock()
    monkeypatch.setattr(stage, "_copy_driver", copier)
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError, match="state"):
        await stage.copy_resource_stage(database, object() if has_transaction else None, [{}] * count)
    database.scalar.assert_not_awaited()
    copier.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", (None, b"{}", "x", "é" * 524289), ids=("missing", "bytes", "short", "utf8_bytes"))
async def test_resource_copy_rejects_malformed_or_oversized_text_before_prepare(monkeypatch, payload):
    """The payload cap measures UTF-8 bytes, including multibyte source text."""
    database = SimpleNamespace(scalar=AsyncMock())
    copier = AsyncMock()
    monkeypatch.setattr(stage, "_copy_driver", copier)
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError, match="state"):
        await stage.copy_resource_stage(database, object(), [{"payload_json_text": payload}])
    database.scalar.assert_not_awaited()
    copier.assert_not_awaited()


@pytest.mark.asyncio
async def test_resource_short_copy_never_admits_manifest(monkeypatch):
    """A driver census mismatch stops before the trusted admission function."""
    database = SimpleNamespace(scalar=AsyncMock())
    driver = SimpleNamespace(copy_records_to_table=AsyncMock(return_value="COPY 0"))
    monkeypatch.setattr(stage, "_copy_driver", AsyncMock(return_value=driver))
    resources = [{"resource_id": "one", "payload_sha256": "a" * 64, "payload_json_text": "{}"}]
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError, match="state"):
        await result_store._insert_resource_manifest(database, _terminal_claim("baseline"), resources, transaction=object())
    database.scalar.assert_awaited_once()
    assert "prepare_pd_uhc_flex_practitioner_stage" in database.scalar.call_args.args[0]
    driver.copy_records_to_table.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("copy_status, expected_count", (("COPY 0", 1), ("COPY 1", 2)))
async def test_work_copy_mismatch_never_finishes_candidate(monkeypatch, copy_status, expected_count):
    """Both per-batch and whole-cohort counts must match before indexed admission."""
    identity = SimpleNamespace(acquisition_id="acquisition", cohort_id="cohort", expected_npi_count=expected_count)
    database = SimpleNamespace(scalar=AsyncMock(return_value="candidate_work"), all=AsyncMock(side_effect=[[(NPI,)], []]))
    driver = SimpleNamespace(copy_records_to_table=AsyncMock(return_value=copy_status))
    monkeypatch.setattr(stage, "_copy_driver", AsyncMock(return_value=driver))
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError, match="state"):
        await store._insert_pending_workset(database, identity, object())
    database.scalar.assert_awaited_once()
    assert "prepare_pd_uhc_flex_practitioner_work" in database.scalar.call_args.args[0]
    assert database.all.await_count == (1 if copy_status == "COPY 0" else 2)


def _header_row(identity, *, status="building"):
    return {
        **store_support.identity_fields(identity),
        "status": status,
        "expected_npi_count": identity.expected_npi_count,
        "matched_count": identity.expected_npi_count,
        "unmatched_count": 0,
        "error_count": 0,
        "resource_count": identity.expected_npi_count,
        "terminal_set_sha256": "7" * 64,
        "cohort_complete": True,
    }


def test_store_support_rejects_ambiguous_schema_and_drifted_headers(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "one")
    monkeypatch.setenv("DB_SCHEMA", "two")
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError):
        store_support.schema_name()
    monkeypatch.delenv("DB_SCHEMA")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "invalid-name")
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError):
        store_support.schema_name()

    identity = _identity()
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError):
        store_support.assert_identity_row(identity, {"status": "building"})
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError):
        store_support.assert_identity_row(
            identity,
            {**store_support.identity_fields(identity), "status": "other"},
        )


def test_store_contract_rejects_invalid_identity_claim_and_payload_boundaries():
    with pytest.raises(ValueError):
        store_contract.strict_identifier(object(), store_contract.RUN_PATTERN, "run")
    with pytest.raises(ValueError):
        store_contract._acquisition_id(
            cohort_id=cohort_fixture().cohort_id,
            acquisition_role="baseline",
            run_id=RUN_ID,
            dataset_intent_id=INTENT_ID,
            expected_npi_count=True,
        )

    identity = _identity()
    for changes in (
        {"expected_npi_count": 0},
        {"acquisition_id": object()},
        {"acquisition_id": "pdufpa_" + "0" * 48},
    ):
        with pytest.raises(ValueError):
            replace(identity, **changes)
    with pytest.raises(ValueError):
        store_contract.build_uhc_flex_practitioner_acquisition_identity(
            object(),
            acquisition_role="baseline",
            run_id=RUN_ID,
            dataset_intent_id=INTENT_ID,
        )

    claim = _terminal_claim("baseline")
    for changes in (
        {"requested_npi": object()},
        {"attempt": 0},
        {"lease_token": "invalid"},
    ):
        with pytest.raises(ValueError):
            replace(claim, **changes)
    with pytest.raises(ValueError):
        store_contract.UHCFlexPractitionerResourceRow(
            requested_npi=NPI,
            resource_id="synthetic",
            payload_sha256="0" * 64,
            payload_json_text=None,
        )

    query_result = _matched_result()

    class _DriftedQueryResult:
        resource_ids = query_result.resource_ids
        resource_sha256_by_id = ((query_result.resource_ids[0], "0" * 64),)

        @staticmethod
        def resource_payloads():
            return query_result.resource_payloads()

    with pytest.raises(store_contract.UHCFlexPractitionerStoreError):
        store_contract.canonical_resource_fields_list(_DriftedQueryResult())


@pytest.mark.asyncio
async def test_store_operation_inputs_and_stale_lease_outcomes_fail_closed():
    identity = _identity()
    claim = _terminal_claim("baseline")
    for operation in (
        store.initialize_uhc_flex_practitioner_acquisition(object()),
        store.heartbeat_uhc_flex_practitioner_work(object()),
        store.release_uhc_flex_practitioner_work(object()),
    ):
        with pytest.raises(ValueError):
            await operation

    with pytest.raises(ValueError):
        await store.claim_uhc_flex_practitioner_work(
            identity.acquisition_id,
            requested_npi=1,
        )
    for operation in (
        store.claim_uhc_flex_practitioner_work(
            identity.acquisition_id,
            lease_seconds=29,
        ),
        store.heartbeat_uhc_flex_practitioner_work(claim, lease_seconds=29),
    ):
        with pytest.raises(ValueError):
            await operation

    with pytest.raises(store_contract.UHCFlexPractitionerStoreError) as error_info:
        await store.heartbeat_uhc_flex_practitioner_work(
            claim,
            database=_Database(status_counts=(0,)),
        )
    assert error_info.value.code == "lease_lost"

    assert store._claim_from_row(None) is None
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError):
        store._claim_from_row({"acquisition_id": "invalid"})


@pytest.mark.asyncio
async def test_claim_rejects_invalid_fresh_only_selection():
    identity = _identity()
    for excluded_npis in (
        [NPI],
        (NPI, NPI),
        tuple(1000000000 + index for index in range(33)),
        (1,),
    ):
        with pytest.raises(ValueError):
            await store.claim_uhc_flex_practitioner_work(
                identity.acquisition_id,
                excluded_npis=excluded_npis,
            )
    with pytest.raises(ValueError):
        await store.claim_uhc_flex_practitioner_work(
            identity.acquisition_id,
            requested_npi=NPI,
            excluded_npis=(NPI,),
        )
    for requested_npi, fresh_only in ((None, 1), (NPI, True)):
        with pytest.raises(ValueError):
            await store.claim_uhc_flex_practitioner_work(
                identity.acquisition_id,
                requested_npi=requested_npi,
                fresh_only=fresh_only,
            )


@pytest.mark.asyncio
async def test_result_store_rejects_invalid_inputs_and_lost_fences():
    claim = _terminal_claim("baseline")
    query_result = _matched_result()
    with pytest.raises(ValueError):
        await result_store.complete_uhc_flex_practitioner_result(
            object(),
            query_result,
        )
    with pytest.raises(ValueError):
        await result_store.complete_uhc_flex_practitioner_result(
            claim,
            object(),
        )
    with pytest.raises(ValueError):
        await result_store.complete_uhc_flex_practitioner_error(
            object(),
            error_code="bounded",
        )
    for invalid_code in (object(), "Invalid"):
        with pytest.raises(ValueError):
            await result_store.complete_uhc_flex_practitioner_error(
                claim,
                error_code=invalid_code,
            )

    resource_fields_list = store_contract.canonical_resource_fields_list(query_result)
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError):
        await result_store._insert_resource_manifest(
            _Database(status_counts=(0,)),
            claim,
            resource_fields_list,
        )
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError) as error_info:
        await result_store.complete_uhc_flex_practitioner_error(
            claim,
            error_code="bounded",
            database=_Database(status_counts=(0,)),
        )
    assert error_info.value.code == "lease_lost"
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError):
        result_store._summary_from_row({})


@pytest.mark.asyncio
async def test_seal_replay_and_manifest_cursor_boundaries():
    identity = _identity()
    sealed_header = _header_row(identity, status="sealed")
    summary = await result_store.seal_uhc_flex_practitioner_acquisition(
        identity,
        database=_Database(first_rows=(sealed_header,)),
    )
    assert summary.cohort_complete is True

    database = _Database(first_rows=(sealed_header,))
    await result_store._seal_building_header(database, identity.acquisition_id)
    seal_sql, parameters = database.statements[-1]
    assert "cohort_complete = (census.error_count = 0)" in seal_sql
    assert "exhausted.attempt_count < :max_attempts" in seal_sql
    assert parameters["max_attempts"] == 8
    assert parameters["retry_exhausted_error_code"] == ("retry_exhausted_transport")

    summary = await result_store.seal_uhc_flex_practitioner_acquisition(
        identity,
        database=_Database(
            first_rows=(_header_row(identity), sealed_header),
            status_counts=(1,),
        ),
    )
    assert summary.cohort_complete is True

    with pytest.raises(ValueError):
        await result_store.seal_uhc_flex_practitioner_acquisition(object())
    with pytest.raises(store_contract.UHCFlexPractitionerStoreError):
        await result_store.seal_uhc_flex_practitioner_acquisition(
            identity,
            database=_Database(
                first_rows=(_header_row(identity), _header_row(identity), None),
                status_counts=(1,),
            ),
        )

    for options in (
        {"after_npi": True},
        {"after_npi": -1},
        {"after_resource_id": object()},
        {"after_resource_id": "x" * 65},
        {"after_resource_id": "synthetic"},
        {"limit": True},
        {"limit": 0},
    ):
        with pytest.raises(ValueError):
            await result_store.read_uhc_flex_practitioner_resource_page(
                identity.acquisition_id,
                **options,
            )
