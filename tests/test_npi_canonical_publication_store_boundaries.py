# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Residual value-safe boundaries for canonical NPI publication storage."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.npi_canonical_publication import NpiCanonicalPublicationError, receipt_metrics
from process.npi_canonical_publication_store import (
    canonical_relation_oids,
    has_settled_npi_publication,
    insert_npi_publication_receipt,
    load_committed_npi_publication,
    mark_npi_publication_succeeded,
)
from tests.test_npi_canonical_publication import (
    RUN_ID,
    TERMINAL_AT,
    _publication_input,
    _receipt,
    _stored_row,
)


@pytest.mark.asyncio
async def test_store_rejects_invalid_schema_before_database_io() -> None:
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(NpiCanonicalPublicationError):
        await canonical_relation_oids(connection, schema="bad-name")
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_receipt_insert_rejects_missing_generation_field() -> None:
    stored_row_by_field = _stored_row()
    stored_row_by_field.pop("publication_generation")
    with pytest.raises(NpiCanonicalPublicationError):
        await insert_npi_publication_receipt(
            SimpleNamespace(fetchrow=AsyncMock(return_value=stored_row_by_field)),
            schema="mrf",
            publication_input=_publication_input(),
        )


@pytest.mark.asyncio
async def test_committed_load_requires_exact_expected_generation() -> None:
    expected_receipt = _receipt()
    stored_row_by_field = _stored_row(sealed=True)
    stored_row_by_field.update(
        publication_generation=expected_receipt.publication_generation + 1,
        snapshot_id=expected_receipt.publication_ref,
        heartbeat_at=TERMINAL_AT,
        finished_at=TERMINAL_AT,
    )
    with pytest.raises(NpiCanonicalPublicationError):
        await load_committed_npi_publication(
            SimpleNamespace(fetchrow=AsyncMock(return_value=stored_row_by_field)),
            schema="mrf",
            receipt=expected_receipt,
            progress_by_name={"phase": "npi published"},
            metrics_by_name={
                "npi_canonical_publication": receipt_metrics(expected_receipt),
            },
        )


@pytest.mark.asyncio
async def test_terminal_update_is_exact_attempt_cas_with_snapshot() -> None:
    receipt = _receipt()
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            return_value={
                "run_id": RUN_ID,
                "snapshot_id": receipt.publication_ref,
                "heartbeat_at": TERMINAL_AT,
                "finished_at": TERMINAL_AT,
            }
        )
    )
    committed = await mark_npi_publication_succeeded(
        connection,
        schema="mrf",
        receipt=receipt,
        progress_by_name={"phase": "npi published"},
        metrics_by_name={"npi_canonical_publication": receipt_metrics(receipt)},
    )
    query = connection.fetchrow.await_args.args[0]
    assert "status='running'" in query
    assert "progress->>'attempt_id'=$2" in query
    assert "snapshot_id=$6" in query
    assert connection.fetchrow.await_args.args[6] == receipt.publication_ref
    assert committed.receipt == receipt
    assert committed.finished_at == "2026-08-09T02:04:05.678901+00:00"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row_or_error",
    (
        None,
        RuntimeError("private terminal detail"),
        {
            "run_id": "wrong",
            "snapshot_id": "wrong",
            "heartbeat_at": TERMINAL_AT,
            "finished_at": TERMINAL_AT,
        },
        {
            "run_id": RUN_ID,
            "snapshot_id": _receipt().publication_ref,
            "heartbeat_at": "not-a-datetime",
            "finished_at": TERMINAL_AT,
        },
        {},
    ),
)
async def test_terminal_update_fails_closed(row_or_error) -> None:
    fetchrow = (
        AsyncMock(side_effect=row_or_error)
        if isinstance(row_or_error, BaseException)
        else AsyncMock(return_value=row_or_error)
    )
    with pytest.raises(NpiCanonicalPublicationError) as caught:
        await mark_npi_publication_succeeded(
            SimpleNamespace(fetchrow=fetchrow),
            schema="mrf",
            receipt=_receipt(),
            progress_by_name={},
            metrics_by_name={},
        )
    assert "private" not in repr(caught.value)


@pytest.mark.asyncio
async def test_committed_load_requires_exact_sealed_terminal_state() -> None:
    receipt = _receipt()
    stored_row_by_field = _stored_row(sealed=True)
    stored_row_by_field.update(
        snapshot_id=receipt.publication_ref,
        heartbeat_at=TERMINAL_AT,
        finished_at=TERMINAL_AT,
    )
    progress_by_name = {"phase": "npi published"}
    metrics_by_name = {"npi_canonical_publication": receipt_metrics(receipt)}
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=stored_row_by_field))

    committed = await load_committed_npi_publication(
        connection,
        schema="mrf",
        receipt=receipt,
        progress_by_name=progress_by_name,
        metrics_by_name=metrics_by_name,
    )

    assert committed is not None
    assert committed.receipt == receipt
    assert committed.heartbeat_at == "2026-08-09T02:04:05.678901+00:00"
    query = connection.fetchrow.await_args.args[0]
    assert "JOIN \"mrf\".\"npi_canonical_publication_receipt_seal\"" in query
    assert "run.progress::jsonb=$2::jsonb" in query
    assert "run.metrics::jsonb=$3::jsonb" in query


@pytest.mark.asyncio
async def test_committed_load_distinguishes_missing_and_mismatch() -> None:
    receipt = _receipt()
    missing_connection = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
    assert await load_committed_npi_publication(
        missing_connection,
        schema="mrf",
        receipt=receipt,
        progress_by_name={},
        metrics_by_name={},
    ) is None

    mismatched_row_by_field = _stored_row(row_counts=(9, 2, 3, 4, 5, 6))
    mismatched_row_by_field.update(
        snapshot_id=receipt.publication_ref,
        heartbeat_at=TERMINAL_AT,
        finished_at=TERMINAL_AT,
    )
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=mismatched_row_by_field))
    with pytest.raises(NpiCanonicalPublicationError):
        await load_committed_npi_publication(
            connection,
            schema="mrf",
            receipt=receipt,
            progress_by_name={},
            metrics_by_name={},
        )


@pytest.mark.asyncio
async def test_committed_load_normalizes_store_failure() -> None:
    connection = SimpleNamespace(
        fetchrow=AsyncMock(side_effect=RuntimeError("private load detail"))
    )
    with pytest.raises(NpiCanonicalPublicationError) as caught:
        await load_committed_npi_publication(
            connection,
            schema="mrf",
            receipt=_receipt(),
            progress_by_name={},
            metrics_by_name={},
        )
    assert "private" not in repr(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("settled_value", "expected"),
    ((RUN_ID, True), (None, False)),
)
async def test_publication_settlement_wait_reports_presence(
    settled_value,
    expected,
) -> None:
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=settled_value))
    assert await has_settled_npi_publication(
        connection,
        schema="mrf",
        run_id=RUN_ID,
    ) is expected
    assert "FOR UPDATE" in connection.fetchval.await_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "settled_value",
    ("wrong-run", RuntimeError("private settlement detail")),
)
async def test_publication_settlement_wait_fails_closed(settled_value) -> None:
    fetchval = (
        AsyncMock(side_effect=settled_value)
        if isinstance(settled_value, BaseException)
        else AsyncMock(return_value=settled_value)
    )
    with pytest.raises(NpiCanonicalPublicationError) as caught:
        await has_settled_npi_publication(
            SimpleNamespace(fetchval=fetchval),
            schema="mrf",
            run_id=RUN_ID,
        )
    assert "private" not in repr(caught.value)
