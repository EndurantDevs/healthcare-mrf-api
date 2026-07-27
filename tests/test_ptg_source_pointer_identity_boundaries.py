# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed identity boundaries for PTG source-pointer publication."""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_pointers


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


def _candidate_with_activation(**activation_overrides):
    candidate_by_field = _candidate_activation_row()
    activation_by_field = dict(
        candidate_by_field["manifest"]["activation"]
    )
    activation_by_field.update(activation_overrides)
    candidate_by_field["manifest"] = {
        "activation": activation_by_field
    }
    return candidate_by_field


def _allowed_candidate(**allowed_overrides):
    allowed_by_field = {
        "contract": source_pointers.PTG2_ALLOWED_AMOUNT_CONTRACT,
        "arch_version": "postgres_binary_v3",
        "storage": "postgresql",
        "data_domain": source_pointers.PTG2_DOMAIN_ALLOWED_AMOUNT,
        "source_key": "synthetic-source",
        "current_source_key": "synthetic_source_allowed_amounts",
        "snapshot_scoped": True,
        "allowed_amount_payments": 1,
        "allowed_amount_evidence": True,
        "previous_snapshot_id": "allowed-previous",
    }
    allowed_by_field.update(allowed_overrides)
    return {"manifest": {"allowed_amount_index": allowed_by_field}}


def test_pointer_metadata_decoders_accept_only_supported_database_shapes():
    """Normalize trusted mappings while rejecting malformed manifest payloads."""

    assert source_pointers._manifest_mapping('{"state": "validated"}') == {
        "state": "validated"
    }
    assert source_pointers._manifest_mapping("{not-json") == {}
    assert source_pointers._manifest_mapping('["not", "an", "object"]') == {}
    assert source_pointers._manifest_mapping(object()) == {}
    assert source_pointers._row_mapping(None) == {}
    assert source_pointers._row_mapping(
        SimpleNamespace(_mapping={"snapshot_id": "snapshot-current"})
    ) == {"snapshot_id": "snapshot-current"}


def test_candidate_activation_requires_a_sealed_contract():
    """Reject activation metadata without the immutable contract marker."""

    with pytest.raises(ValueError, match="activation contract"):
        source_pointers.activated_snapshot_attributes(
            {"manifest": {}},
            activated_at=dt.datetime(2026, 7, 27, 12, 0),
            activation_mode="audited_control",
        )


def test_plan_pointer_keys_include_network_source_identity():
    """Keep plan pointers for independent network sources distinct."""

    source_without_network = source_pointers._ptg2_plan_source_key(
        "synthetic-plan",
        "group",
        dt.date(2026, 7, 1),
    )
    source_with_network = source_pointers._ptg2_plan_source_key(
        "synthetic-plan",
        "group",
        dt.date(2026, 7, 1),
        "synthetic-source",
    )
    assert source_without_network != source_with_network


@pytest.mark.parametrize(
    ("plan_id", "market_type"),
    [
        ("", "group"),
        ("synthetic-plan", ""),
    ],
)
def test_plan_pointer_entries_require_complete_logical_identity(
    plan_id,
    market_type,
):
    """Reject incomplete logical-plan coordinates before pointer writes."""

    with pytest.raises(ValueError, match="identity is incomplete"):
        source_pointers._plan_pointer_entry(
            plan_id=plan_id,
            plan_market_type=market_type,
            import_month=dt.date(2026, 7, 1),
            source_key="synthetic-source",
            snapshot_id="snapshot-current",
            previous_snapshot_id="snapshot-previous",
            updated_at=dt.datetime(2026, 7, 27, 12, 0),
        )


@pytest.mark.asyncio
async def test_current_source_lookup_distinguishes_absent_null_and_live_rows(
    monkeypatch,
):
    """Preserve the three database states used by predecessor CAS planning."""

    source_pointer_lookup = AsyncMock(
        side_effect=[None, (None,), ("snapshot-current",)]
    )
    monkeypatch.setattr(
        source_pointers.db,
        "first",
        source_pointer_lookup,
    )
    monkeypatch.setattr(
        source_pointers,
        "resolve_ptg2_schema",
        lambda: "mrf",
    )

    assert await source_pointers._current_source_snapshot_id("source-a") is None
    assert await source_pointers._current_source_snapshot_id("source-b") is None
    assert (
        await source_pointers._current_source_snapshot_id("source-c")
        == "snapshot-current"
    )


@pytest.mark.asyncio
async def test_plan_pointer_rows_skip_empty_scope_records_and_map_driver_rows(
    monkeypatch,
):
    """Publish only complete logical-plan mappings returned by the driver."""

    monkeypatch.setattr(
        source_pointers.db,
        "all",
        AsyncMock(
            return_value=[
                {"plan_id": " ", "plan_market_type": "group"},
                SimpleNamespace(
                    _mapping={
                        "plan_id": " synthetic-plan ",
                        "plan_market_type": " GROUP ",
                    }
                ),
            ]
        ),
    )
    monkeypatch.setattr(
        source_pointers,
        "resolve_ptg2_schema",
        lambda: "mrf",
    )

    plan_pointer_entries = await source_pointers._source_plan_rows(
        snapshot_id="snapshot-current",
        source_key="synthetic-source",
        import_month=dt.date(2026, 7, 1),
        previous_snapshot_id="snapshot-previous",
        updated_at=dt.datetime(2026, 7, 27, 12, 0),
    )
    pointer_identities = [
        (
            plan_pointer_entry["plan_id"],
            plan_pointer_entry["plan_market_type"],
        )
        for plan_pointer_entry in plan_pointer_entries
    ]
    assert pointer_identities == [("synthetic-plan", "group")]


@pytest.mark.parametrize(
    ("candidate", "source_key", "expected_current", "error_type", "message"),
    [
        (
            _candidate_activation_row(status="building"),
            "synthetic-source",
            "snapshot-previous",
            ValueError,
            "not a validated candidate",
        ),
        (
            _candidate_with_activation(contract=None),
            "synthetic-source",
            "snapshot-previous",
            ValueError,
            "activation contract",
        ),
        (
            _candidate_with_activation(source_key="different-source"),
            "synthetic-source",
            "snapshot-previous",
            ValueError,
            "source_key",
        ),
        (
            _candidate_activation_row(previous_snapshot_id="different"),
            "synthetic-source",
            "snapshot-previous",
            ValueError,
            "predecessor disagrees",
        ),
        (
            _candidate_activation_row(),
            "synthetic-source",
            "different-current",
            source_pointers.PTG2SourcePointerConflict,
            "requested predecessor",
        ),
        (
            _candidate_activation_row(coverage_scope_id=b"short"),
            "synthetic-source",
            "snapshot-previous",
            ValueError,
            "incomplete immutable scope",
        ),
    ],
)
def test_candidate_activation_identity_rejects_changed_immutable_fields(
    candidate,
    source_key,
    expected_current,
    error_type,
    message,
):
    """Reject every independently mutable candidate identity boundary."""

    with pytest.raises(error_type, match=message):
        source_pointers._validated_activation_identity(
            candidate,
            source_key=source_key,
            expected_current_snapshot_id=expected_current,
        )


@pytest.mark.parametrize(
    ("candidate", "message"),
    [
        (
            {"manifest": {"allowed_amount_index": "not-an-object"}},
            "must be an object",
        ),
        (
            _allowed_candidate(contract="wrong-contract"),
            "invalid contract binding",
        ),
        (
            _allowed_candidate(snapshot_scoped=False),
            "not snapshot scoped",
        ),
        (
            _allowed_candidate(allowed_amount_payments=object()),
            "payment count is invalid",
        ),
        (
            _allowed_candidate(allowed_amount_evidence=False),
            "no payment evidence",
        ),
        (
            _allowed_candidate(allowed_amount_payments=0),
            "no payment evidence",
        ),
    ],
)
def test_allowed_pointer_identity_rejects_unsealed_or_empty_evidence(
    candidate,
    message,
):
    """Reject optional allowed evidence unless every sealed binding agrees."""

    with pytest.raises(ValueError, match=message):
        source_pointers._validated_allowed_activation_identity(
            candidate,
            source_key="synthetic-source",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_key", "snapshot_id"),
    [
        ("", "snapshot-current"),
        ("synthetic-source", ""),
    ],
)
async def test_candidate_activation_requires_both_pointer_coordinates(
    source_key,
    snapshot_id,
):
    """Reject incomplete activation coordinates before opening a transaction."""

    with pytest.raises(ValueError, match="source_key and snapshot_id"):
        await source_pointers.activate_ptg2_source_candidate(
            source_key=source_key,
            snapshot_id=snapshot_id,
        )


@pytest.mark.asyncio
async def test_source_publication_requires_pointer_coordinates():
    """Reject incomplete source coordinates before opening a transaction."""

    with pytest.raises(ValueError, match="source_key and snapshot_id"):
        await source_pointers._publish_ptg2_source_pointers(
            source_key="",
            snapshot_id="snapshot-current",
            previous_snapshot_id="snapshot-previous",
            import_month=dt.date(2026, 7, 1),
            updated_at=dt.datetime(2026, 7, 27, 12, 0),
        )


@pytest.mark.asyncio
async def test_source_publication_rejects_contradictory_snapshot_attributes():
    """Reject caller metadata for a different snapshot before locking."""

    with pytest.raises(ValueError, match="do not match"):
        await source_pointers._publish_ptg2_source_pointers(
            source_key="synthetic-source",
            snapshot_id="snapshot-current",
            previous_snapshot_id="snapshot-previous",
            import_month=dt.date(2026, 7, 1),
            updated_at=dt.datetime(2026, 7, 27, 12, 0),
            snapshot_attributes={"snapshot_id": "different-snapshot"},
        )
