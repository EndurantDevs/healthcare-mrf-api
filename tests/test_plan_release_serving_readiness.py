# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Serving-readiness coverage for canonical plan-release bindings."""

import asyncio

import pytest

from api import plan_release_readiness, plan_release_serving
from api.plan_release_serving import PlanReleaseSnapshotBinding
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.allowed_amounts import PTG2_ALLOWED_AMOUNT_CONTRACT
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

from .test_plan_release_serving import (
    PLAN_RELEASE_ID,
    _Session,
    _binding_row,
)


def _binding(
    *,
    snapshot_id="ptg2:release-old",
    source_key="aetna-network-a",
    plan_id="99-0000001",
    market_type="group",
    role="in_network",
):
    return PlanReleaseSnapshotBinding(
        binding_ordinal=0,
        snapshot_id=snapshot_id,
        source_key=source_key,
        plan_id=plan_id,
        plan_market_type=market_type,
        role=role,
        required=True,
    )


def _serving_table_descriptor(**updates):
    fields_by_name = {
        "snapshot_id": "ptg2:release-old",
        "arch_version": "postgres_binary_v3",
        "shared_snapshot_key": 17,
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "shared_block_layout": "dense_shared_blocks_v3",
        "source_count": 1,
        "plan_id": "990000001",
        "plan_market_type": "group",
        "source_key": "aetna-network-a",
    }
    fields_by_name.update(updates)
    return PTG2ServingTables(**fields_by_name)


def _resolve_release(rows, monkeypatch, binding_resolver):
    monkeypatch.setattr(
        plan_release_serving,
        "is_release_binding_serving_ready",
        binding_resolver,
    )
    return asyncio.run(
        plan_release_serving.resolve_plan_release_serving(
            _Session(rows),
            PLAN_RELEASE_ID,
        )
    )


def _install_snapshot_guards(monkeypatch, serving_tables):
    selector_calls = []

    async def current_snapshot(_session, **selectors):
        selector_calls.append(selectors)
        return selectors["requested_snapshot_id"]

    async def load_serving_tables(_session, _snapshot_id):
        return serving_tables

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
    return selector_calls


class _ScalarSession:
    def __init__(self, scalar_value):
        self.scalar_value = scalar_value
        self.calls = []

    async def scalar(self, statement, parameters):
        self.calls.append((str(statement), parameters))
        return self.scalar_value


def _two_binding_rows():
    return [
        _binding_row(expected_binding_count=2),
        _binding_row(
            expected_binding_count=2,
            binding_ordinal=0,
            snapshot_id="ptg2:release-allowed",
            source_key="aetna-allowed",
            role="allowed_amounts",
        ),
    ]


def test_release_resolver_accepts_ready_binding(monkeypatch):
    calls = []

    async def is_serving_ready(_session, binding):
        calls.append(binding.snapshot_id)
        return True

    selection = _resolve_release(
        [_binding_row()],
        monkeypatch,
        is_serving_ready,
    )

    assert selection is not None
    assert calls == ["ptg2:release-old"]


def test_release_resolver_rejects_binding_that_is_not_serving_ready(monkeypatch):
    async def is_binding_ready(_session, _binding):
        return False

    selection = _resolve_release(
        [_binding_row()],
        monkeypatch,
        is_binding_ready,
    )

    assert selection is None


def test_release_resolver_rejects_multi_binding_cut_when_one_is_unready(
    monkeypatch,
):
    calls = []

    async def is_binding_ready(_session, binding):
        snapshot_id = binding.snapshot_id
        calls.append(snapshot_id)
        return snapshot_id != "ptg2:release-allowed"

    selection = _resolve_release(
        _two_binding_rows(),
        monkeypatch,
        is_binding_ready,
    )

    assert selection is None
    assert calls == ["ptg2:release-old", "ptg2:release-allowed"]


def test_release_resolver_catches_malformed_serving_manifest(monkeypatch):
    async def malformed_manifest(_session, _binding):
        raise PTG2ManifestArtifactError("malformed full manifest")

    selection = _resolve_release(
        [_binding_row()],
        monkeypatch,
        malformed_manifest,
    )

    assert selection is None


def test_binding_readiness_uses_exact_snapshot_selectors(monkeypatch):
    binding = _binding()
    selector_calls = _install_snapshot_guards(
        monkeypatch,
        _serving_table_descriptor(),
    )

    is_ready = asyncio.run(
        plan_release_readiness.is_release_binding_serving_ready(
            object(),
            binding,
        )
    )

    assert is_ready is True
    assert selector_calls == [
        {
            "requested_snapshot_id": binding.snapshot_id,
            "requested_source_key": binding.source_key,
            "requested_plan_id": binding.plan_id,
            "requested_plan_market_type": binding.plan_market_type,
        }
    ]


@pytest.mark.parametrize("resolved_snapshot_id", [None, "ptg2:wrong"])
def test_binding_readiness_rejects_snapshot_selector_miss(
    monkeypatch,
    resolved_snapshot_id,
):
    async def current_snapshot(_session, **_selectors):
        return resolved_snapshot_id

    async def fail_serving_table_load(*_args, **_kwargs):
        raise AssertionError("selector miss must stop before table loading")

    monkeypatch.setattr(
        plan_release_readiness,
        "current_snapshot_id",
        current_snapshot,
    )
    monkeypatch.setattr(
        plan_release_readiness,
        "snapshot_serving_tables",
        fail_serving_table_load,
    )

    is_ready = asyncio.run(
        plan_release_readiness.is_release_binding_serving_ready(
            object(),
            _binding(),
        )
    )

    assert is_ready is False


def test_binding_readiness_rejects_non_strict_serving_tables(monkeypatch):
    _install_snapshot_guards(
        monkeypatch,
        _serving_table_descriptor(arch_version="full_json_v1"),
    )

    is_ready = asyncio.run(
        plan_release_readiness.is_release_binding_serving_ready(
            object(),
            _binding(),
        )
    )

    assert is_ready is False


def test_binding_readiness_rejects_unknown_role(monkeypatch):
    async def fail_generic_guard(*_args, **_kwargs):
        raise AssertionError("unknown roles must fail before generic guards")

    monkeypatch.setattr(
        plan_release_readiness,
        "current_snapshot_id",
        fail_generic_guard,
    )
    monkeypatch.setattr(
        plan_release_readiness,
        "snapshot_serving_tables",
        fail_generic_guard,
    )

    is_ready = asyncio.run(
        plan_release_readiness.is_release_binding_serving_ready(
            object(),
            _binding(role="unexpected"),
        )
    )

    assert is_ready is False


@pytest.mark.parametrize(
    "serving_updates",
    [
        pytest.param({"plan_id": "99-9999999"}, id="physical-plan"),
        pytest.param({"plan_market_type": "individual"}, id="market"),
        pytest.param({"source_key": "other-source"}, id="source"),
    ],
)
def test_binding_readiness_rejects_physical_scope_mismatch(
    monkeypatch,
    serving_updates,
):
    _install_snapshot_guards(
        monkeypatch,
        _serving_table_descriptor(**serving_updates),
    )

    is_ready = asyncio.run(
        plan_release_readiness.is_release_binding_serving_ready(
            object(),
            _binding(),
        )
    )

    assert is_ready is False


@pytest.mark.parametrize("coverage_exists", [False, True])
def test_allowed_amount_binding_requires_strict_index_and_plan_coverage(
    coverage_exists,
):
    session = _ScalarSession(coverage_exists)
    binding = _binding(
        snapshot_id="ptg2:release-allowed",
        source_key="aetna-allowed",
        role="allowed_amounts",
    )

    is_covered = asyncio.run(
        plan_release_readiness.has_allowed_amount_binding_coverage(
            session,
            binding,
        )
    )

    assert is_covered is coverage_exists
    readiness_sql, parameters = session.calls[0]
    assert "ptg2_allowed_amount_plan" in readiness_sql
    assert "plan_coverage.plan_id = ANY(CAST(:plan_ids AS text[]))" in readiness_sql
    assert "ptg2_current_source_snapshot" not in readiness_sql
    assert "allowed_amount_index" in readiness_sql
    assert "snapshot_scoped" in readiness_sql
    assert "allowed_amount_evidence" in readiness_sql
    assert "allowed_amount_payments" in readiness_sql
    assert "allowed_amount_provider_payments" in readiness_sql
    assert "ptg2_allowed_amount_item allowed_item" in readiness_sql
    assert "ptg2_allowed_amount_payment allowed_payment" in readiness_sql
    assert "ptg2_allowed_amount_provider_payment" in readiness_sql
    assert "allowed_item.file_id = plan_coverage.file_id" in readiness_sql
    assert "cardinality(provider_payment.npi) > 0" in readiness_sql
    assert "= :source_key" in readiness_sql
    assert parameters == {
        "snapshot_id": binding.snapshot_id,
        "source_key": binding.source_key,
        "plan_ids": ["99-0000001", "990000001"],
        "market_type": "group",
        "allowed_contract": PTG2_ALLOWED_AMOUNT_CONTRACT,
        "allowed_data_domain": "allowed_amounts",
    }


def test_allowed_amount_binding_requires_nonempty_provider_npi_evidence():
    session = _ScalarSession(True)

    asyncio.run(
        plan_release_readiness.has_allowed_amount_binding_coverage(
            session,
            _binding(role="allowed_amounts"),
        )
    )

    readiness_sql = session.calls[0][0]
    assert "cardinality(provider_payment.npi) > 0" in readiness_sql


@pytest.mark.parametrize(
    ("coverage_exists", "expected_readiness"),
    [(False, False), (True, True)],
)
def test_allowed_amount_role_uses_index_coverage_guard(
    monkeypatch,
    coverage_exists,
    expected_readiness,
):
    binding = _binding(
        snapshot_id="ptg2:release-allowed",
        source_key="aetna-allowed",
        role="allowed_amounts",
    )

    async def fail_generic_guard(*_args, **_kwargs):
        raise AssertionError("allowed releases do not use generic V3 guards")

    monkeypatch.setattr(
        plan_release_readiness,
        "current_snapshot_id",
        fail_generic_guard,
    )
    monkeypatch.setattr(
        plan_release_readiness,
        "snapshot_serving_tables",
        fail_generic_guard,
    )

    async def covered(_session, _binding_value):
        return coverage_exists

    monkeypatch.setattr(
        plan_release_readiness,
        "has_allowed_amount_binding_coverage",
        covered,
    )

    is_ready = asyncio.run(
        plan_release_readiness.is_release_binding_serving_ready(
            object(),
            binding,
        )
    )

    assert is_ready is expected_readiness
