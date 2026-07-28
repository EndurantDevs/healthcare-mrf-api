# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio

import pytest

from process import uhc_semantic_evidence as evidence
from process.uhc_semantic_evidence import (
    UHC_EVIDENCE_CONFLICT_FIELDS,
    summarize_uhc_npi_evidence,
    uhc_evidence_summary_sql,
    validate_uhc_evidence_summary,
)


def _row(**overrides: int) -> dict[str, int]:
    evidence_by_field = {
        "evidence_count": 10,
        "distinct_npis": 8,
        "duplicate_npi_groups": 2,
        "conflicting_npi_groups": 1,
        **{f"conflict_{field}": 0 for field in UHC_EVIDENCE_CONFLICT_FIELDS},
    }
    evidence_by_field["conflict_names"] = 1
    evidence_by_field.update(overrides)
    return evidence_by_field


def test_query_is_one_setwise_group_and_rejects_identifier_injection() -> None:
    query = uhc_evidence_summary_sql("mrf.uhc_stage_abc123")

    assert 'FROM "mrf"."uhc_stage_abc123"' in query
    assert query.count("GROUP BY npi") == 1
    assert "substring(conflict_signature_pack FROM 1 FOR 32)" in query
    assert "substring(conflict_signature_pack FROM 257 FOR 32)" in query
    assert "row_kind = 2" in query
    with pytest.raises(ValueError, match="safe schema.table"):
        uhc_evidence_summary_sql("mrf.stage; DROP TABLE provider")


def test_summary_enforces_exact_evidence_count_and_group_bounds() -> None:
    summary = validate_uhc_evidence_summary(_row(), expected_evidence_count=10)

    assert summary.distinct_npis == 8
    assert summary.duplicate_npi_groups == 2
    assert summary.conflicting_npi_groups == 1
    assert summary.conflict_counts["names"] == 1
    with pytest.raises(RuntimeError, match="sealed provider fact count"):
        validate_uhc_evidence_summary(_row(), expected_evidence_count=11)
    with pytest.raises(RuntimeError, match="invariants"):
        validate_uhc_evidence_summary(
            _row(conflicting_npi_groups=3),
            expected_evidence_count=10,
        )
    with pytest.raises(RuntimeError, match="invariants"):
        validate_uhc_evidence_summary(
            _row(conflict_names=2),
            expected_evidence_count=10,
        )


def test_summary_rejects_boolean_and_negative_counts() -> None:
    with pytest.raises(RuntimeError, match="not nonnegative"):
        validate_uhc_evidence_summary(
            _row(evidence_count=True),
            expected_evidence_count=10,
        )
    with pytest.raises(RuntimeError, match="not nonnegative"):
        validate_uhc_evidence_summary(
            _row(distinct_npis=-1),
            expected_evidence_count=10,
        )


def _evidence_signature(seed: int, *, name_seed: int | None = None) -> bytes:
    values = [bytes([seed]) * 32 for _ in UHC_EVIDENCE_CONFLICT_FIELDS]
    if name_seed is not None:
        values[UHC_EVIDENCE_CONFLICT_FIELDS.index("names")] = (
            bytes([name_seed]) * 32
        )
    return b"".join(values)


class _EvidenceConnection:
    query: str | None = None

    def transaction(self):
        class Transaction:
            async def __aenter__(self):
                return None

            async def __aexit__(self, *_args):
                return None

        return Transaction()

    async def cursor(self, query: str, *, prefetch: int):
        self.query = query
        evidence_rows = [
            {
                "npi": "0000000000",
                "conflict_signature_pack": _evidence_signature(1),
            },
            {
                "npi": "0000000000",
                "conflict_signature_pack": _evidence_signature(
                    1, name_seed=2
                ),
            },
            {
                "npi": "0000000001",
                "conflict_signature_pack": _evidence_signature(3),
            },
            {
                "npi": "0000000001",
                "conflict_signature_pack": _evidence_signature(3),
            },
            *[
                {
                    "npi": f"{index:010d}",
                    "conflict_signature_pack": _evidence_signature(index + 3),
                }
                for index in range(2, 8)
            ],
        ]
        for evidence_row in evidence_rows:
            yield evidence_row


def test_async_summary_uses_validated_setwise_result() -> None:
    """The async path must consume independently validated evidence."""

    connection = _EvidenceConnection()
    summary = asyncio.run(
        summarize_uhc_npi_evidence(
            connection,
            "mrf.uhc_stage_abc123",
            expected_evidence_count=10,
        )
    )

    assert summary.evidence_count == 10
    assert summary.distinct_npis == 8
    assert summary.conflicting_npi_groups == 1
    assert len(summary.proof_sha256) == 64
    assert connection.query is not None
    assert 'FROM "mrf"."uhc_stage_abc123"' in connection.query
    assert "ORDER BY npi" in connection.query
    assert "GROUP BY" not in connection.query


@pytest.mark.parametrize("expected_count", [True, -1])
def test_summary_rejects_invalid_expected_count(expected_count):
    with pytest.raises(ValueError, match="must be nonnegative"):
        validate_uhc_evidence_summary(
            _row(),
            expected_evidence_count=expected_count,
        )


class _RowsConnection:
    def __init__(self, rows):
        self.rows = rows

    def transaction(self):
        return _EvidenceConnection().transaction()

    async def cursor(self, _query, *, prefetch):
        assert prefetch == 512
        for row in self.rows:
            yield row


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [
        {
            "npi": None,
            "conflict_signature_pack": _evidence_signature(1),
        },
        {
            "npi": "bad",
            "conflict_signature_pack": _evidence_signature(1),
        },
        {
            "npi": "0000000000",
            "conflict_signature_pack": b"short",
        },
    ],
)
async def test_stage_evidence_rows_rejects_malformed_rows(row):
    with pytest.raises(RuntimeError, match="row is malformed"):
        async for _ in evidence._stage_evidence_rows(
            _RowsConnection([row]),
            "mrf.stage",
        ):
            continue


def test_accumulator_rejects_merge_order_and_accepts_empty_finish():
    accumulator = evidence._EvidenceSummaryAccumulator()
    accumulator._finish_group()
    accumulator.observe("0000000001", _evidence_signature(1))
    with pytest.raises(RuntimeError, match="merge order"):
        accumulator.observe("0000000000", _evidence_signature(1))


@pytest.mark.asyncio
async def test_summary_requires_stage_when_evidence_is_expected():
    with pytest.raises(RuntimeError, match="stages are missing"):
        await evidence.summarize_uhc_npi_evidence_stages(
            _RowsConnection([]),
            [],
            expected_evidence_count=1,
        )
    summary = await evidence.summarize_uhc_npi_evidence_stages(
        _RowsConnection([]),
        [],
        expected_evidence_count=0,
    )
    assert summary.evidence_count == 0


@pytest.mark.asyncio
async def test_merged_evidence_skips_empty_stage(monkeypatch):
    async def empty_stage(_connection, _stage_ref):
        if False:
            yield "", b""

    monkeypatch.setattr(evidence, "_stage_evidence_rows", empty_stage)
    assert [
        row
        async for row in evidence._merged_evidence_rows(
            object(),
            ["mrf.empty"],
        )
    ] == []
