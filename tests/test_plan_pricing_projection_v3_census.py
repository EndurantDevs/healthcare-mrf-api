# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed source and rollback proofs for the projection-v3 census."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.research import plan_pricing_projection_v3_census as census
from scripts.research import plan_pricing_projection_v3_census_contract as contract
from scripts.research import plan_pricing_projection_v3_census_support as support

TARGET_CLI_ARGS = (
    "--expected-healthporta-plan-id",
    "hpplan_01ARZ3NDEKTSV4RRFFQ69G5FAA",
    "--expected-serving-revision-id",
    "hpserve_01ARZ3NDEKTSV4RRFFQ69G5FAB",
    "--expected-binding-set-digest",
    "a" * 64,
    "--expected-binding-count",
    "3",
    "--expected-in-network-binding-count",
    "3",
    "--expected-snapshot-count",
    "3",
    "--expected-plan-count",
    "1",
    "--expected-harness-manifest-sha256",
    "b" * 64,
    "--expected-image-digest",
    "sha256:" + "c" * 64,
)


def _target_args(**overrides):
    target_by_field = {
        "expected_healthporta_plan_id": "hpplan_01ARZ3NDEKTSV4RRFFQ69G5FAA",
        "plan_release_id": "hprelease_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        "expected_serving_revision_id": "hpserve_01ARZ3NDEKTSV4RRFFQ69G5FAB",
        "expected_binding_set_digest": "a" * 64,
        "expected_binding_count": 3,
        "expected_in_network_binding_count": 3,
        "expected_snapshot_count": 3,
        "expected_plan_count": 1,
        "expected_harness_manifest_sha256": "b" * 64,
        "expected_image_digest": "sha256:" + "c" * 64,
    }
    target_by_field.update(overrides)
    return SimpleNamespace(**target_by_field)


def _release_input(*, third_role="in_network", third_plan_id="plan_shared"):
    binding_list = [
        {
            "role": role,
            "snapshot_id": f"snapshot_{ordinal}",
            "plan_id": third_plan_id if ordinal == 3 else "plan_shared",
        }
        for ordinal, role in enumerate(
            ("in_network", "in_network", third_role), start=1
        )
    ]
    return census.ReleaseInput(
        {
            "healthporta_plan_id": "hpplan_01ARZ3NDEKTSV4RRFFQ69G5FAA",
            "plan_release_id": "hprelease_01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "serving_revision_id": "hpserve_01ARZ3NDEKTSV4RRFFQ69G5FAB",
            "binding_set_digest": "a" * 64,
        },
        binding_list,
    )


def _empty_staged_counts() -> dict[str, int]:
    return {
        "provider_set_count": 0,
        "provider_membership_count": 0,
        "maximum_provider_set_membership_count": 0,
        "provider_cell_count": 0,
        "provider_fragment_byte_count": 0,
        "provider_npi_count": 0,
        "pending_npi_count": 0,
        "referenced_empty_provider_set_count": 0,
        "price_membership_cached_block_count": 0,
        "price_membership_identity_retained_bytes": 0,
        "price_membership_metadata_fragment_count": 0,
        "price_membership_maximum_fragments_per_block": 0,
        "price_membership_singleton_peak_bytes": 0,
    }


def test_census_accepts_authority_bound_serving_digest() -> None:
    bindings = [
        {
            "role": "in_network",
            "ordinal": 0,
            "required": True,
            "snapshot_id": "snapshot",
            "source_key": "source",
            "plan_id": "plan",
            "market_type": "commercial",
        }
    ]
    release_input = support._release_input_from_rows(
        "hprelease_01ARZ3NDEKTSV4RRFFQ69G5FAV",
        {
            "healthporta_plan_id": "hpplan_01ARZ3NDEKTSV4RRFFQ69G5FAA",
            "serving_revision_id": "hpserve_01ARZ3NDEKTSV4RRFFQ69G5FAB",
            "binding_set_digest": "a" * 64,
            "expected_binding_count": 1,
            "source_manifest": {"bindings": bindings},
            "published_at": "2026-08-29T00:00:00+00:00",
        },
        [
            {
                "role": "in_network",
                "binding_ordinal": 0,
                "required": True,
                "snapshot_id": "snapshot",
                "source_key": "source",
                "plan_id": "plan",
                "plan_market_type": "commercial",
            }
        ],
    )

    assert release_input.identity["binding_set_digest"] == "a" * 64
    assert release_input.binding_manifest == bindings


def test_census_source_drift_writes_a_failed_receipt(
    monkeypatch,
    tmp_path,
) -> None:
    receipt_path = tmp_path / "census.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "plan-pricing-v3-census",
            "--plan-release-id",
            "hprelease_01ARZ3NDEKTSV4RRFFQ69G5FAV",
            *TARGET_CLI_ARGS,
            "--expected-source-sha",
            "0" * 40,
            "--expected-source-manifest-sha256",
            "f" * 64,
            "--receipt",
            str(receipt_path),
            "--source-only",
        ],
    )

    assert census.census_main() == 1
    receipt_by_field = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt_by_field["status"] == "failed"
    assert receipt_by_field["accepted"] is False
    assert receipt_by_field["phase"] == "binding source"
    assert receipt_by_field["error"] == {"type": "RuntimeError"}


def test_census_target_requires_exact_release_and_shape() -> None:
    expected_target = census._expected_target(_target_args())

    assert census._require_expected_target(_target_args(), _release_input()) == (
        expected_target
    )


@pytest.mark.parametrize(
    ("target_args", "release_input"),
    (
        (
            _target_args(expected_healthporta_plan_id="hpplan_changed"),
            _release_input(),
        ),
        (_target_args(plan_release_id="hprelease_changed"), _release_input()),
        (
            _target_args(expected_serving_revision_id="hpserve_changed"),
            _release_input(),
        ),
        (_target_args(expected_binding_set_digest="b" * 64), _release_input()),
        (
            _target_args(),
            _release_input(third_role="out_of_network"),
        ),
        (
            _target_args(),
            _release_input(third_plan_id="plan_changed"),
        ),
        (_target_args(expected_snapshot_count=2), _release_input()),
    ),
)
def test_census_target_rejects_identity_or_shape_drift(
    target_args,
    release_input,
) -> None:
    with pytest.raises(RuntimeError, match="target changed"):
        census._require_expected_target(target_args, release_input)


@pytest.mark.asyncio
async def test_source_only_receipt_is_not_cap_or_resource_proof(
    monkeypatch,
    tmp_path,
) -> None:
    receipt_path = tmp_path / "source-only.json"
    target_args = _target_args(
        expected_source_sha="0" * 40,
        expected_source_manifest_sha256="f" * 64,
        receipt=receipt_path,
        source_only=True,
    )
    monkeypatch.setattr(
        census,
        "_source_identity",
        lambda _args: {"manifest_sha256": "f" * 64},
    )
    receipt_by_field: dict = {}

    assert await census.run_census(target_args, receipt_by_field) == 0
    assert receipt_by_field["status"] == "source_only"
    assert receipt_by_field["mode"] == "source_only"
    assert receipt_by_field["accepted"] is False
    assert receipt_by_field["cap_calibration_admissible"] is False
    assert receipt_by_field["resource_proof_admissible"] is False
    assert receipt_by_field["proof_scope"] == "source_identity_only"


@pytest.mark.asyncio
async def test_census_enforces_one_whole_run_deadline(monkeypatch, tmp_path) -> None:
    target_args = _target_args(
        expected_source_sha="0" * 40,
        expected_source_manifest_sha256="f" * 64,
        receipt=tmp_path / "timeout.json",
        source_only=False,
    )
    monkeypatch.setattr(census, "_source_identity", lambda _args: {})
    monkeypatch.setattr(census, "MAX_CENSUS_RUNTIME_SECONDS", 0.001)
    monkeypatch.setenv(census.OPT_IN_ENV, "1")

    async def never_finishes(_args, _receipt_by_field):
        await census.asyncio.sleep(1)

    monkeypatch.setattr(census, "_execute_census", never_finishes)
    with pytest.raises(TimeoutError):
        await census.run_census(target_args, {})


def test_census_projects_the_four_executable_work_limits() -> None:
    metrics_by_field = census._empty_metrics()
    metrics_by_field["membership_probe_rows"] = {
        "total": 21,
        "maximum_per_code": 13,
    }
    metrics_by_field["member_cell_rows"] = {
        "total": 34,
        "maximum_per_code": 17,
    }

    assert census._observed_work_limits(metrics_by_field) == {
        "maximum_code_membership_probe_rows": 13,
        "maximum_projection_membership_probe_rows": 21,
        "maximum_code_member_cell_rows": 17,
        "maximum_projection_member_cell_rows": 34,
    }


def test_census_acceptance_requires_four_positive_work_limits() -> None:
    from tests.test_plan_pricing_projection_v3_census_contract import (
        _database_receipt,
    )

    receipt_by_field = {
        **_database_receipt(),
        "rollback_complete": True,
        "temporary_relations_after_rollback": [],
        "postflight": {"accepted": True},
    }
    work_by_field = census._empty_metrics()
    work_by_field["membership_probe_rows"] = {
        "total": 2,
        "maximum_per_code": 1,
    }
    work_by_field["member_cell_rows"] = {
        "total": 4,
        "maximum_per_code": 3,
    }
    staged_by_field = _empty_staged_counts()
    measurement_by_field = {
        "work": work_by_field,
        "staged": staged_by_field,
        "fixed_cap_gates": contract.fixed_cap_gates(work_by_field, staged_by_field),
        "observed_work_limits": contract.observed_work_limits(work_by_field),
    }

    assert contract.is_accepted(receipt_by_field, measurement_by_field, True)
    measurement_by_field["observed_work_limits"][
        "maximum_code_membership_probe_rows"
    ] = 0
    assert not contract.is_accepted(receipt_by_field, measurement_by_field, True)


def test_census_runtime_binds_the_expected_image_digest(monkeypatch) -> None:
    image_digest = "sha256:" + "c" * 64
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_V3_CENSUS_JOB_NAME", "census-job")
    monkeypatch.setenv("HOSTNAME", "census-pod")
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_V3_CENSUS_POD_UID", "pod-uid")
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_V3_CENSUS_IMAGE_DIGEST", image_digest)

    runtime_by_field = support.runtime_identity(image_digest)
    assert runtime_by_field["image_digest"] == image_digest
    assert runtime_by_field["external_pod_image_id_attestation_required"] is True
    with pytest.raises(RuntimeError, match="image identity changed"):
        support.runtime_identity("sha256:" + "d" * 64)


def test_census_fixed_gates_bind_the_rate_profile_cardinality_cap() -> None:
    metrics_by_field = census._empty_metrics()
    profile_rates = metrics_by_field["profile_distinct_rate_count_max"]
    profile_rates["maximum_per_code"] = contract.MAX_RATE_PROFILE_RATES * 4 // 5

    gates = census._fixed_cap_gates(metrics_by_field, _empty_staged_counts())
    assert gates["profile_distinct_rate_count_within_cap"]
    profile_rates["maximum_per_code"] += 1
    gates = census._fixed_cap_gates(metrics_by_field, _empty_staged_counts())
    assert not gates["profile_distinct_rate_count_within_cap"]


@pytest.mark.parametrize(
    ("metric_name", "metric_field", "gate_name", "cap"),
    (
        (
            "normalized_occurrence_rows",
            "maximum_per_code",
            "occurrence_code_within_cap",
            contract.MAX_CODE_OCCURRENCES,
        ),
        (
            "staged_price_atom_membership_rows",
            "maximum_per_code",
            "price_atom_code_within_cap",
            contract.MAX_CODE_STAGED_PRICE_ATOMS,
        ),
        (
            "maximum_price_key_atom_membership_rows",
            "maximum_per_code",
            "price_key_hydration_within_cap",
            contract.MAX_PRICE_HYDRATION_ATOMS,
        ),
        (
            "profile_join_rows",
            "total",
            "profile_release_within_cap",
            contract.MAX_PROJECTION_RATE_PROFILE_WORK_ROWS,
        ),
        (
            "aggregate_rate_count_max",
            "maximum_per_code",
            "aggregate_rate_count_within_bigint",
            contract.MAX_BIGINT,
        ),
    ),
)
def test_census_fixed_gates_require_twenty_five_percent_headroom(
    metric_name,
    metric_field,
    gate_name,
    cap,
) -> None:
    metrics_by_field = census._empty_metrics()
    admitted_value = cap * 4 // 5
    metrics_by_field[metric_name][metric_field] = admitted_value
    gates = census._fixed_cap_gates(metrics_by_field, _empty_staged_counts())
    assert gates[gate_name]

    metrics_by_field[metric_name][metric_field] = admitted_value + 1
    gates = census._fixed_cap_gates(metrics_by_field, _empty_staged_counts())
    assert not gates[gate_name]


def test_census_records_exact_staged_occurrence_and_price_atom_counts() -> None:
    metrics_by_field = census._empty_metrics()
    code_work = SimpleNamespace(
        **{
            field_name: 0
            for field_name in census.MEASURED_WORK_FIELDS
            if field_name
            not in {
                "eligible_member_cell_rows",
                "normalized_occurrence_rows",
                "staged_price_atom_membership_rows",
                "maximum_price_key_atom_membership_rows",
            }
        }
    )

    census._record_metric(metrics_by_field, "normalized_occurrence_rows", 5)
    census._record_metric(metrics_by_field, "maximum_price_key_atom_membership_rows", 4)
    census._record_work(metrics_by_field, code_work, 3, 7)

    assert metrics_by_field["normalized_occurrence_rows"] == {
        "total": 5,
        "maximum_per_code": 5,
    }
    assert metrics_by_field["staged_price_atom_membership_rows"] == {
        "total": 7,
        "maximum_per_code": 7,
    }
    assert metrics_by_field["maximum_price_key_atom_membership_rows"] == {
        "total": 4,
        "maximum_per_code": 4,
    }


@pytest.mark.asyncio
async def test_census_counts_declared_occurrences_without_staged_rates(
    monkeypatch,
) -> None:
    from api import ptg2_serving as serving

    code_identity = ("CPT", "27447")
    context = SimpleNamespace(
        state=object(),
        binding_projections=[
            SimpleNamespace(code_rows_by_identity={code_identity: [3, 5]}),
            SimpleNamespace(code_rows_by_identity={code_identity: [7]}),
        ],
    )
    monkeypatch.setattr(serving, "_declared_geo_rate_count", lambda rows: sum(rows))

    async def has_staged_rates(*_args, **_kwargs):
        return False

    monkeypatch.setattr(census.projection, "_has_staged_code_inputs", has_staged_rates)
    metrics_by_field = census._empty_metrics()

    assert not await census._has_measured_code(
        object(), context, code_identity, metrics_by_field
    )
    assert metrics_by_field["normalized_occurrence_rows"] == {
        "total": 15,
        "maximum_per_code": 15,
    }
    assert metrics_by_field["staged_price_atom_membership_rows"] == {
        "total": 0,
        "maximum_per_code": 0,
    }


def test_census_source_overlay_includes_branch_runtime_dependencies() -> None:
    assert {
        "api/plan_pricing_projection_v3.py",
        "api/plan_pricing_projection_v3_price.py",
        "api/plan_pricing_projection_v3_work.py",
        "api/ptg2_db_serving_v3.py",
        "api/ptg2_db_sidecars.py",
        "api/ptg2_serving.py",
        "api/ptg2_snapshot.py",
        "api/ptg2_v4_graph.py",
    } <= set(support.SOURCE_PATHS)


def test_census_rejects_a_changed_harness_manifest(monkeypatch) -> None:
    file_digest = "e" * 64
    hashed_paths = []
    source_manifest = support._canonical_sha256(
        [[source_path, file_digest] for source_path in support.SOURCE_PATHS]
    )

    def hash_file(path):
        hashed_paths.append(Path(path).name)
        return file_digest

    monkeypatch.setattr(support, "_sha256_file", hash_file)
    monkeypatch.setattr(support, "_observed_git_head", lambda _root: "0" * 40)

    with pytest.raises(RuntimeError, match="harness identity changed"):
        support.capture_source_identity(
            Path(census.__file__),
            "0" * 40,
            source_manifest,
            "f" * 64,
        )
    assert "plan_pricing_projection_v3_census_diagnostics.py" in hashed_paths
