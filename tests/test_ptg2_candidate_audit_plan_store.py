from __future__ import annotations

from dataclasses import FrozenInstanceError
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_candidate_audit_plan_store as plan_store
from process.ptg_parts.ptg2_shared_audit import persisted_audit_sample_digest


def _rows() -> list[dict]:
    common_by_field = {
        "code_key": 7,
        "provider_set_key": 5,
        "price_key": 8,
        "source_key": 0,
        "npi": 1_234_567_890,
        "atom_key": 9,
        "reported_code_system": " ms-drg ",
        "reported_code": "7",
    }
    return [
        {**common_by_field, "occurrence_id": b"a" * 32, "atom_ordinal": 0},
        {**common_by_field, "occurrence_id": b"b" * 32, "atom_ordinal": 1},
    ]


def _metadata(rows: list[dict]) -> dict:
    return {
        "sample_count": len(rows),
        "sample_digest": persisted_audit_sample_digest(rows),
        "source_count": 1,
    }


@pytest.mark.asyncio
async def test_loads_code_aware_sample_with_one_ordered_query(monkeypatch):
    database_rows = _rows()
    query = AsyncMock(return_value=database_rows)
    monkeypatch.setattr(plan_store.db, "all", query)

    sample = await plan_store.load_persisted_audit_sample(
        schema_name='mrf"shared',
        snapshot_key=41,
        expected_metadata=_metadata(database_rows),
    )

    assert sample.snapshot_key == 41
    assert sample.sample_count == 2
    assert sample.records[0] == plan_store.PersistedAuditSampleRecord(
        occurrence_id=b"a" * 32,
        code_key=7,
        code_system="MS_DRG",
        code="007",
        provider_set_key=5,
        price_key=8,
        source_artifact_key=0,
        npi=1_234_567_890,
        atom_ordinal=0,
        atom_key=9,
    )
    with pytest.raises(FrozenInstanceError):
        sample.records[0].code = "008"

    query.assert_awaited_once()
    sql = query.await_args.args[0]
    assert 'FROM "mrf""shared".ptg2_v3_audit_occurrence audit' in sql
    assert 'LEFT JOIN "mrf""shared".ptg2_v3_code code' in sql
    assert "code.snapshot_key = audit.snapshot_key" in sql
    assert "code.code_key = audit.code_key" in sql
    assert "WHERE audit.snapshot_key = :snapshot_key" in sql
    assert "ORDER BY audit.occurrence_id" in sql
    assert query.await_args.kwargs == {"snapshot_key": 41, "row_limit": 3}


@pytest.mark.asyncio
async def test_rejects_exact_sample_count_mismatch(monkeypatch):
    expected_rows = _rows()
    query = AsyncMock(return_value=expected_rows[:1])
    monkeypatch.setattr(plan_store.db, "all", query)

    with pytest.raises(
        plan_store.CandidateAuditPlanStoreError,
        match="row count.*sealed metadata",
    ):
        await plan_store.load_persisted_audit_sample(
            schema_name="mrf",
            snapshot_key=41,
            expected_metadata=_metadata(expected_rows),
        )

    query.assert_awaited_once()


@pytest.mark.asyncio
async def test_rejects_sample_digest_mismatch(monkeypatch):
    rows = _rows()
    query = AsyncMock(return_value=rows)
    monkeypatch.setattr(plan_store.db, "all", query)
    metadata = {**_metadata(rows), "sample_digest": "0" * 64}

    with pytest.raises(
        plan_store.CandidateAuditPlanStoreError,
        match="digest.*sealed metadata",
    ):
        await plan_store.load_persisted_audit_sample(
            schema_name="mrf",
            snapshot_key=41,
            expected_metadata=metadata,
        )

    query.assert_awaited_once()


@pytest.mark.asyncio
async def test_rejects_duplicate_or_ambiguous_code_join(monkeypatch):
    expected_rows = _rows()
    ambiguous_rows = [
        expected_rows[0],
        {
            **expected_rows[0],
            "reported_code_system": "CPT",
            "reported_code": "99213",
        },
    ]
    query = AsyncMock(return_value=ambiguous_rows)
    monkeypatch.setattr(plan_store.db, "all", query)

    with pytest.raises(
        plan_store.CandidateAuditPlanStoreError,
        match="duplicate or ambiguous",
    ):
        await plan_store.load_persisted_audit_sample(
            schema_name="mrf",
            snapshot_key=41,
            expected_metadata=_metadata(expected_rows),
        )

    query.assert_awaited_once()


@pytest.mark.parametrize(
    ("field_name", "invalid_value", "message"),
    [
        ("occurrence_id", b"short", "occurrence id"),
        ("code_key", True, "coordinate"),
        ("source_key", 1, "source key"),
        ("npi", 999, "NPI"),
        ("reported_code_system", None, "code identity"),
        ("reported_code", " ", "code identity"),
    ],
)
@pytest.mark.asyncio
async def test_rejects_invalid_rows(
    monkeypatch,
    field_name,
    invalid_value,
    message,
):
    valid_rows = _rows()
    invalid_rows = [dict(row) for row in valid_rows]
    invalid_rows[0][field_name] = invalid_value
    query = AsyncMock(return_value=invalid_rows)
    monkeypatch.setattr(plan_store.db, "all", query)

    with pytest.raises(plan_store.CandidateAuditPlanStoreError, match=message):
        await plan_store.load_persisted_audit_sample(
            schema_name="mrf",
            snapshot_key=41,
            expected_metadata=_metadata(valid_rows),
        )

    query.assert_awaited_once()


@pytest.mark.parametrize(
    "metadata",
    [
        {"sample_count": True, "sample_digest": "0" * 64, "source_count": 1},
        {"sample_count": 2, "sample_digest": "A" * 64, "source_count": 1},
        {"sample_count": 2, "sample_digest": "0" * 64, "source_count": -1},
    ],
)
@pytest.mark.asyncio
async def test_rejects_invalid_metadata_before_query(monkeypatch, metadata):
    query = AsyncMock()
    monkeypatch.setattr(plan_store.db, "all", query)

    with pytest.raises(plan_store.CandidateAuditPlanStoreError):
        await plan_store.load_persisted_audit_sample(
            schema_name="mrf",
            snapshot_key=41,
            expected_metadata=metadata,
        )

    query.assert_not_awaited()


@pytest.mark.parametrize(
    "metadata",
    [
        None,
        {"sample_count": 0, "sample_digest": "0" * 64, "source_count": 1},
        {
            "sample_count": plan_store.PTG2_V3_AUDIT_MAX_SAMPLE_ROWS + 1,
            "sample_digest": "0" * 64,
            "source_count": 1,
        },
        {"sample_count": 1, "sample_digest": "0" * 64, "source_count": 0},
    ],
)
def test_rejects_empty_or_out_of_range_sample_contract(metadata):
    with pytest.raises(plan_store.CandidateAuditPlanStoreError):
        plan_store._expected_sample_contract(metadata)


@pytest.mark.parametrize(
    "database_row",
    [object(), {"occurrence_id": b"a" * 32}],
)
def test_rejects_non_mapping_or_incomplete_database_rows(database_row):
    with pytest.raises(plan_store.CandidateAuditPlanStoreError):
        plan_store._row_mapping(database_row)


@pytest.mark.parametrize(
    ("schema_name", "snapshot_key"),
    [("", 41), ("mrf", 0)],
)
@pytest.mark.asyncio
async def test_rejects_invalid_query_scope_before_database_access(
    monkeypatch,
    schema_name,
    snapshot_key,
):
    query = AsyncMock()
    monkeypatch.setattr(plan_store.db, "all", query)

    with pytest.raises(plan_store.CandidateAuditPlanStoreError):
        await plan_store.load_persisted_audit_sample(
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            expected_metadata=_metadata(_rows()),
        )

    query.assert_not_awaited()
