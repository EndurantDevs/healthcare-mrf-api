# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Default-off plan-release loading of billing source publications."""

from __future__ import annotations

import asyncio

from api import plan_release_readiness, plan_release_serving
from tests.test_plan_release_descriptor_reuse import _serving_table_descriptor
from tests.test_plan_release_serving import (
    PLAN_RELEASE_ID,
    _Session,
    _binding_row,
)
from tests.test_plan_release_serving_readiness import _binding


def test_binding_readiness_opts_into_billing_source_metadata(monkeypatch):
    descriptor = _serving_table_descriptor()
    serving_table_calls = []

    async def current_snapshot(_session, **selectors_by_name):
        return selectors_by_name["requested_snapshot_id"]

    async def load_serving_tables(_session, snapshot_id, **options_by_name):
        serving_table_calls.append((snapshot_id, options_by_name))
        return descriptor

    monkeypatch.setattr(
        plan_release_readiness,
        "current_snapshot_id",
        current_snapshot,
    )
    monkeypatch.setattr(
        plan_release_readiness,
        "snapshot_serving_tables",
        load_serving_tables,
    )

    is_ready = asyncio.run(
        plan_release_readiness.is_release_binding_serving_ready(
            object(),
            _binding(),
            include_billing_tax_identity_source=True,
        )
    )

    assert is_ready is True
    assert serving_table_calls == [
        (
            _binding().snapshot_id,
            {"include_billing_tax_identity_source": True},
        )
    ]


def test_binding_readiness_rejects_nonboolean_source_opt_in(monkeypatch):
    async def unexpected(*_args, **_kwargs):
        raise AssertionError("invalid opt-in must fail before snapshot access")

    monkeypatch.setattr(
        plan_release_readiness,
        "current_snapshot_id",
        unexpected,
    )

    is_ready = asyncio.run(
        plan_release_readiness.is_release_binding_serving_ready(
            object(),
            _binding(),
            include_billing_tax_identity_source=1,
        )
    )

    assert is_ready is False


def test_release_resolver_defaults_to_source_metadata_not_loaded(monkeypatch):
    descriptor = _serving_table_descriptor()

    async def is_serving_ready(
        _session,
        binding,
        *,
        validated_serving_tables_by_snapshot_id,
    ):
        validated_serving_tables_by_snapshot_id[binding.snapshot_id] = descriptor
        return True

    monkeypatch.setattr(
        plan_release_serving,
        "is_release_binding_serving_ready",
        is_serving_ready,
    )

    selection = asyncio.run(
        plan_release_serving.resolve_plan_release_serving(
            _Session([_binding_row()]),
            PLAN_RELEASE_ID,
        )
    )

    assert selection is not None
    assert selection.includes_billing_tax_identity_source is False


def test_release_resolver_records_and_forwards_source_opt_in(monkeypatch):
    descriptor = _serving_table_descriptor()
    readiness_calls = []

    async def is_serving_ready(
        _session,
        binding,
        *,
        validated_serving_tables_by_snapshot_id,
        include_billing_tax_identity_source,
    ):
        readiness_calls.append(include_billing_tax_identity_source)
        validated_serving_tables_by_snapshot_id[binding.snapshot_id] = descriptor
        return True

    monkeypatch.setattr(
        plan_release_serving,
        "is_release_binding_serving_ready",
        is_serving_ready,
    )

    selection = asyncio.run(
        plan_release_serving.resolve_plan_release_serving(
            _Session([_binding_row()]),
            PLAN_RELEASE_ID,
            include_billing_tax_identity_source=True,
        )
    )

    assert selection is not None
    assert selection.includes_billing_tax_identity_source is True
    assert readiness_calls == [True]


def test_release_resolver_rejects_nonboolean_source_opt_in() -> None:
    session = _Session([_binding_row()])

    selection = asyncio.run(
        plan_release_serving.resolve_plan_release_serving(
            session,
            PLAN_RELEASE_ID,
            include_billing_tax_identity_source=1,
        )
    )

    assert selection is None
    assert session.calls == []
