# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Transactional fail-closed boundaries for PTG source pointers."""

from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_pointers
from tests.ptg_v4_publish_control_support import TransactionDatabase


class _QueryResult:
    def __init__(self, row=None, *, rows=(), scalar=None):
        self._row = row
        self._rows = rows
        self._scalar = scalar

    def first(self):
        return self._row

    def one_or_none(self):
        return self._row

    def scalar_one(self):
        return self._scalar

    def __iter__(self):
        return iter(self._rows)


class _QuerySession:
    def __init__(self, *query_results):
        self._query_results = list(query_results)
        self.calls = []

    async def execute(self, statement, params=None):
        self.calls.append((str(statement), params))
        if not self._query_results:
            raise AssertionError("unexpected database statement")
        return self._query_results.pop(0)


def _candidate_activation_row(**overrides):
    candidate_by_field = {
        "status": source_pointers.PTG2_STATUS_VALIDATED,
        "previous_snapshot_id": "snapshot-previous",
        "manifest": {
            "activation": {
                "contract": (
                    source_pointers.PTG2_CANDIDATE_ACTIVATION_CONTRACT
                ),
                "state": "validated",
                "source_key": "synthetic-source",
                "expected_previous_snapshot_id": "snapshot-previous",
            }
        },
        "snapshot_key": 17,
        "plan_id": "synthetic-plan",
        "plan_market_type": "group",
        "coverage_scope_id": b"c" * 32,
    }
    candidate_by_field.update(overrides)
    return candidate_by_field


def _coverage_scope_arguments(**overrides):
    arguments_by_name = {
        "schema_name": "mrf",
        "snapshot_id": "snapshot-current",
        "coverage_scope_id": b"c" * 32,
        "plan_pointer_entries": [
            {"plan_id": "synthetic-plan", "plan_market_type": "group"}
        ],
    }
    arguments_by_name.update(overrides)
    return arguments_by_name


def _patch_pointer_transaction(monkeypatch, authoritative_snapshot):
    transaction_session = object()
    database = TransactionDatabase(transaction_session)
    monkeypatch.setattr(source_pointers, "resolve_ptg2_schema", lambda: "mrf")
    monkeypatch.setattr(source_pointers.db, "transaction", database.transaction)
    monkeypatch.setattr(
        source_pointers,
        "_acquire_source_pointer_gc_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(source_pointers, "lock_writable_snapshot", AsyncMock())
    monkeypatch.setattr(
        source_pointers,
        "_locked_snapshot_publication_row",
        AsyncMock(return_value=authoritative_snapshot),
    )


def _patch_legacy_publication_writers(monkeypatch, plan_pointer_entries):
    bind_layout = AsyncMock()
    bind_scope = AsyncMock()
    monkeypatch.setattr(
        source_pointers,
        "_source_plan_rows",
        AsyncMock(return_value=plan_pointer_entries),
    )
    monkeypatch.setattr(
        source_pointers,
        "bind_snapshot_to_shared_layout",
        bind_layout,
    )
    monkeypatch.setattr(
        source_pointers,
        "_bind_snapshot_coverage_scope",
        bind_scope,
    )
    monkeypatch.setattr(
        source_pointers,
        "_compare_and_swap_source_pointer",
        AsyncMock(),
    )
    monkeypatch.setattr(
        source_pointers,
        "_publish_snapshot_in_pointer_transaction",
        AsyncMock(),
    )
    monkeypatch.setattr(
        source_pointers,
        "_replace_source_plan_pointers",
        AsyncMock(),
    )
    return bind_layout, bind_scope


@pytest.mark.asyncio
async def test_snapshot_staging_requires_validated_attributes():
    """Reject a staging request that has not completed validation."""

    with pytest.raises(ValueError, match="validated snapshot"):
        await source_pointers._stage_snapshot_in_pointer_transaction(
            object(),
            schema_name="mrf",
            snapshot_attributes={"status": "building"},
        )


@pytest.mark.asyncio
async def test_snapshot_staging_rejects_a_changed_database_row():
    """Fail the transaction when the staging compare-and-swap changes no row."""

    staged_attributes_by_name = {
        "snapshot_id": "snapshot-current",
        "status": source_pointers.PTG2_STATUS_VALIDATED,
        "manifest": {},
    }
    with pytest.raises(RuntimeError, match="could not be staged"):
        await source_pointers._stage_snapshot_in_pointer_transaction(
            _QuerySession(_QueryResult()),
            schema_name="mrf",
            snapshot_attributes=staged_attributes_by_name,
        )


@pytest.mark.asyncio
async def test_absent_snapshot_attributes_perform_no_publication_write():
    """A pointer-only repoint must not rewrite immutable snapshot metadata."""

    query_session = _QuerySession()
    await source_pointers._publish_snapshot_in_pointer_transaction(
        query_session,
        schema_name="mrf",
        snapshot_attributes=None,
    )
    assert query_session.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("strict_candidate", "error_message"),
    [
        (True, "changed during audited"),
        (False, "disappeared"),
    ],
)
async def test_snapshot_publication_rejects_a_changed_database_row(
    strict_candidate,
    error_message,
):
    """Require the selected candidate or legacy publication row to persist."""

    manifest_by_field = {}
    if strict_candidate:
        manifest_by_field["activation"] = {
            "contract": source_pointers.PTG2_CANDIDATE_ACTIVATION_CONTRACT,
            "state": "activated",
        }
    attributes_by_name = {
        "snapshot_id": "snapshot-current",
        "status": source_pointers.PTG2_STATUS_PUBLISHED,
        "published_at": dt.datetime(2026, 7, 27, 12, 0),
        "previous_snapshot_id": "snapshot-previous",
        "manifest": manifest_by_field,
    }
    with pytest.raises(RuntimeError, match=error_message):
        await source_pointers._publish_snapshot_in_pointer_transaction(
            _QuerySession(_QueryResult()),
            schema_name="mrf",
            snapshot_attributes=attributes_by_name,
        )


@pytest.mark.asyncio
async def test_global_pointer_reconciliation_requires_a_published_row():
    """Reject reconciliation when the selected publication vanished."""

    with pytest.raises(RuntimeError, match="global pointer reconciliation"):
        await source_pointers._reconcile_global_snapshot_pointer(
            _QuerySession(_QueryResult()),
            schema_name="mrf",
            snapshot_id="snapshot-current",
            updated_at=dt.datetime(2026, 7, 27, 12, 0),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides_by_name", "error_type", "error_message"),
    [
        (
            {"coverage_scope_id": b"short"},
            ValueError,
            "exactly 32 bytes",
        ),
        (
            {"plan_pointer_entries": []},
            RuntimeError,
            "no logical plan",
        ),
        (
            {
                "coverage_plan_scopes": [
                    {"plan_id": " ", "plan_market_type": "group"}
                ]
            },
            ValueError,
            "requires logical coverage plans",
        ),
        (
            {
                "coverage_plan_scopes": [
                    {
                        "plan_id": "different-plan",
                        "plan_market_type": "group",
                    }
                ]
            },
            RuntimeError,
            "do not match",
        ),
    ],
)
async def test_coverage_scope_rejects_incomplete_or_changed_identity(
    overrides_by_name,
    error_type,
    error_message,
):
    """Reject invalid immutable-scope input before database mutation."""

    with pytest.raises(error_type, match=error_message):
        await source_pointers._bind_snapshot_coverage_scope(
            object(),
            **_coverage_scope_arguments(**overrides_by_name),
        )


@pytest.mark.asyncio
async def test_coverage_scope_rejects_a_conflicting_physical_binding():
    """Reject a snapshot already bound to different physical coverage."""

    with pytest.raises(RuntimeError, match="already bound"):
        await source_pointers._bind_snapshot_coverage_scope(
            _QuerySession(_QueryResult()),
            **_coverage_scope_arguments(),
        )


@pytest.mark.asyncio
async def test_coverage_scope_rejects_stale_logical_plan_mappings():
    """Require the committed logical-plan set to match the sealed scope."""

    query_session = _QuerySession(
        _QueryResult({"coverage_scope_id": b"c" * 32}),
        _QueryResult(),
        _QueryResult(
            rows=[
                {
                    "plan_id": "stale-plan",
                    "plan_market_type": "group",
                }
            ]
        ),
    )
    with pytest.raises(RuntimeError, match="stale logical"):
        await source_pointers._bind_snapshot_coverage_scope(
            query_session,
            **_coverage_scope_arguments(),
        )


@pytest.mark.asyncio
async def test_publication_lock_requires_an_existing_snapshot():
    """Reject pointer publication when the locked snapshot row is absent."""

    with pytest.raises(ValueError, match="unavailable for publication"):
        await source_pointers._locked_snapshot_publication_row(
            _QuerySession(_QueryResult()),
            schema_name="mrf",
            snapshot_id="snapshot-current",
        )


@pytest.mark.asyncio
async def test_candidate_lock_requires_an_existing_sealed_layout():
    """Reject activation when no sealed candidate row can be locked."""

    with pytest.raises(ValueError, match="candidate is unavailable"):
        await source_pointers._locked_candidate_activation_row(
            _QuerySession(_QueryResult()),
            schema_name="mrf",
            snapshot_id="snapshot-current",
        )


@pytest.mark.asyncio
async def test_activation_clock_requires_a_database_timestamp():
    """Reject activation if PostgreSQL returns an unexpected clock shape."""

    with pytest.raises(RuntimeError, match="activation timestamp"):
        await source_pointers._database_utc_timestamp(
            _QuerySession(_QueryResult(scalar="not-a-timestamp"))
        )


@pytest.mark.asyncio
async def test_candidate_activation_requires_logical_plan_mappings():
    """Reject a candidate whose sealed scope has no logical plans."""

    with pytest.raises(ValueError, match="no logical plan mappings"):
        await source_pointers._candidate_plan_pointer_entries(
            _QuerySession(_QueryResult(rows=[])),
            schema_name="mrf",
            source_key="synthetic-source",
            snapshot_id="snapshot-current",
            previous_snapshot_id="snapshot-previous",
            import_month=dt.date(2026, 7, 1),
            activated_at=dt.datetime(2026, 7, 27, 12, 0),
        )


@pytest.mark.asyncio
async def test_candidate_activation_requires_an_immutable_import_month(
    monkeypatch,
):
    """Reject a candidate whose locked import month has changed shape."""

    candidate_by_field = _candidate_activation_row(import_month="2026-07")
    monkeypatch.setattr(
        source_pointers,
        "_locked_candidate_activation_row",
        AsyncMock(return_value=candidate_by_field),
    )
    monkeypatch.setattr(
        source_pointers,
        "_database_utc_timestamp",
        AsyncMock(return_value=dt.datetime(2026, 7, 27, 12, 0)),
    )
    plan_pointer_loader = AsyncMock()
    monkeypatch.setattr(
        source_pointers,
        "_candidate_plan_pointer_entries",
        plan_pointer_loader,
    )

    with pytest.raises(ValueError, match="no import month"):
        await source_pointers._candidate_activation_context(
            object(),
            schema_name="mrf",
            source_key="synthetic-source",
            snapshot_id="snapshot-current",
            expected_current_snapshot_id="snapshot-previous",
        )
    plan_pointer_loader.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_publication_binds_the_immutable_coverage_scope(
    monkeypatch,
):
    """Exercise physical and logical scope binding before pointer promotion."""

    _patch_pointer_transaction(
        monkeypatch,
        {"status": source_pointers.PTG2_STATUS_PUBLISHED, "manifest": {}},
    )
    plan_pointer_entries = [
        {"plan_id": "synthetic-plan", "plan_market_type": "group"}
    ]
    bind_layout, bind_scope = _patch_legacy_publication_writers(
        monkeypatch,
        plan_pointer_entries,
    )

    publication_by_field = await source_pointers._publish_ptg2_source_pointers(
        source_key="synthetic-source",
        snapshot_id="snapshot-current",
        previous_snapshot_id="snapshot-previous",
        import_month=dt.date(2026, 7, 1),
        updated_at=dt.datetime(2026, 7, 27, 12, 0),
        shared_snapshot_key=17,
        coverage_scope_id=b"c" * 32,
        coverage_plan_scopes=[
            {"plan_id": "synthetic-plan", "plan_market_type": "group"}
        ],
    )

    assert publication_by_field["status"] == "promoted"
    bind_layout.assert_awaited_once()
    bind_scope.assert_awaited_once()


@pytest.mark.asyncio
async def test_source_repoint_requires_an_already_published_snapshot(
    monkeypatch,
):
    """Reject a pointer-only repoint to non-public snapshot state."""

    _patch_pointer_transaction(
        monkeypatch,
        {"status": source_pointers.PTG2_STATUS_VALIDATED, "manifest": {}},
    )
    monkeypatch.setattr(
        source_pointers,
        "_source_plan_rows",
        AsyncMock(return_value=[]),
    )
    with pytest.raises(ValueError, match="already published"):
        await source_pointers._publish_ptg2_source_pointers(
            source_key="synthetic-source",
            snapshot_id="snapshot-current",
            previous_snapshot_id="snapshot-previous",
            import_month=dt.date(2026, 7, 1),
            updated_at=dt.datetime(2026, 7, 27, 12, 0),
        )
