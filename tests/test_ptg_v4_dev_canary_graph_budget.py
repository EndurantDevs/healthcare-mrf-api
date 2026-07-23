# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace

import pytest

from scripts import ptg_v4_dev_canary_graph_budget as graph_policy
from scripts.ptg_v4_dev_canary_cli import build_parser
from scripts.ptg_v4_dev_canary_graph_budget import (
    GRAPH_READ_CANARY_CASES,
    UNAPPROVED_GRAPH_READ_CEILING_FAILURE,
    GraphReadApproval,
    evaluate_graph_read_budget,
)


_CASE = GRAPH_READ_CANARY_CASES[0]
_SNAPSHOT_ID = "ptg2:v4:measured"
_IMPORT_RUN_ID = "ptg2:import:measured"
_IMAGE_IDENTITY = "sha256:measured-image"


def _public_graph(
    *,
    database_bytes: int = 1_000,
    database_blocks: int = 10,
    logical_lookups: int = 3,
) -> dict[str, object]:
    return {
        "passed": True,
        "failures": [],
        "deltas": {
            "request_count": 1,
            "database_bytes": database_bytes,
            "database_blocks": database_blocks,
            "logical_lookups": logical_lookups,
        },
        "runtime_identity": {
            "process_identity": "process",
            "process_started_at": "2026-07-23T00:00:00Z",
            "image_identity": _IMAGE_IDENTITY,
        },
    }


def _cold_samples() -> list[dict[str, object]]:
    return [
        {
            "snapshot_id": _SNAPSHOT_ID,
            "reference_snapshot_id": _CASE.reference_snapshot_id,
            "image_identity": _IMAGE_IDENTITY,
            "graph_read_evidence": _public_graph(),
        }
        for _sample_index in range(20)
    ]


def _internal_delta(
    *,
    database_bytes: int = 8_000,
    database_blocks: int = 4,
    logical_lookups: int = 6,
    locator_pages: int = 16,
    member_pages: int = 128,
) -> dict[str, int]:
    return {
        "request_count": 1,
        "database_bytes": database_bytes,
        "database_blocks": database_blocks,
        "logical_lookups": logical_lookups,
        "hot_group_npi_locator_pages": locator_pages,
        "hot_group_npi_member_pages": member_pages,
    }


def _owner(
    role: str,
    *,
    mode: str,
    source_pages: int,
) -> dict[str, object]:
    metric_deltas = [_internal_delta() for _sample_index in range(20)]
    return {
        "role": role,
        "mode": mode,
        "compiler_work": {"observed": {"source_pages": source_pages}},
        "cold": {"metric_deltas": list(metric_deltas)},
        "warm": {"metric_deltas": list(metric_deltas)},
    }


def _internal_evidence() -> dict[str, object]:
    return {
        "snapshot_id": _SNAPSHOT_ID,
        "reference_snapshot_id": _CASE.reference_snapshot_id,
        "image_identity": _IMAGE_IDENTITY,
        "owners": [
            _owner(
                "overall_worst",
                mode="prefix_override",
                source_pages=0,
            ),
            _owner(
                "worst_online_non_override",
                mode="online_source_component",
                source_pages=64,
            ),
        ],
    }


def _evaluate() -> dict[str, object]:
    return evaluate_graph_read_budget(
        reference_snapshot_id=_CASE.reference_snapshot_id,
        snapshot_id=_SNAPSHOT_ID,
        import_run_id=_IMPORT_RUN_ID,
        image_identity=_IMAGE_IDENTITY,
        public_cold_samples=_cold_samples(),
        public_warm_graph_evidence=_public_graph(database_bytes=500),
        internal_owner_evidence=_internal_evidence(),
    )


def _approval() -> GraphReadApproval:
    approval = GraphReadApproval(
        measurement_reference_snapshot_id=_CASE.reference_snapshot_id,
        measurement_snapshot_id=_SNAPSHOT_ID,
        measurement_import_run_id=_IMPORT_RUN_ID,
        measurement_image_identity=_IMAGE_IDENTITY,
        measurement_evidence_sha256="",
        measured_public_database_bytes=1_000,
        measured_public_database_blocks=10,
        measured_public_logical_lookups=3,
        measured_internal_database_bytes=8_000,
        measured_internal_database_blocks=4,
        measured_internal_logical_lookups=6,
        tolerance_basis_points=200,
        approved_public_database_bytes=1_020,
        approved_public_database_blocks=11,
        approved_public_logical_lookups=4,
        approved_internal_database_bytes=8_160,
        approved_internal_database_blocks=5,
        approved_internal_logical_lookups=7,
    )
    return replace(
        approval,
        measurement_evidence_sha256=(
            graph_policy.graph_read_measurement_evidence_sha256(approval)
        ),
    )


def test_first_measurement_reports_all_series_but_blocks_acceptance() -> None:
    report = _evaluate()

    assert report["passed"] is False
    assert report["failures"] == [
        UNAPPROVED_GRAPH_READ_CEILING_FAILURE
    ]
    assert report["promotion_state"] == "measurement_only_pending_review"
    assert report["measurement_evidence"] == {
        "contract": graph_policy.GRAPH_READ_MEASUREMENT_EVIDENCE_CONTRACT,
        "measurement_reference_snapshot_id": _CASE.reference_snapshot_id,
        "measurement_snapshot_id": _SNAPSHOT_ID,
        "measurement_import_run_id": _IMPORT_RUN_ID,
        "measurement_image_identity": _IMAGE_IDENTITY,
        "measured_public_database_bytes": 1_000,
        "measured_public_database_blocks": 10,
        "measured_public_logical_lookups": 3,
        "measured_internal_database_bytes": 8_000,
        "measured_internal_database_blocks": 4,
        "measured_internal_logical_lookups": 6,
    }
    assert report["measurement_evidence_sha256"] == (
        graph_policy.graph_read_measurement_evidence_sha256(_approval())
    )
    assert report["public"]["cold"]["sample_count"] == 20
    assert report["public"]["warm"]["sample_count"] == 1
    assert [owner["role"] for owner in report["internal"]["owners"]] == [
        "overall_worst",
        "worst_online_non_override",
    ]
    assert report["internal"]["hard_maximums"]["graph_pages"] == 208


def test_checked_in_measured_ceiling_uses_exact_two_percent_tolerance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    approval = _approval()
    graph_policy._validate_graph_read_approval(_CASE, approval)
    approved_case = replace(_CASE, graph_read_approval=approval)
    monkeypatch.setitem(
        graph_policy._CASE_BY_REFERENCE_SNAPSHOT_ID,
        _CASE.reference_snapshot_id,
        approved_case,
    )

    report = _evaluate()

    assert report["passed"] is True
    assert report["promotion_state"] == "approved_measured_ceiling"
    assert report["graph_read_approval"]["tolerance_basis_points"] == 200


def test_approval_rejects_extra_headroom_or_non_two_percent_tolerance() -> None:
    with pytest.raises(RuntimeError, match="incomplete or inconsistent"):
        graph_policy._validate_graph_read_approval(
            _CASE,
            replace(_approval(), approved_public_database_bytes=1_021)
        )
    with pytest.raises(RuntimeError, match="incomplete or inconsistent"):
        graph_policy._validate_graph_read_approval(
            _CASE,
            replace(_approval(), tolerance_basis_points=201),
        )
    with pytest.raises(RuntimeError, match="incomplete or inconsistent"):
        graph_policy._validate_graph_read_approval(
            _CASE,
            replace(_approval(), tolerance_basis_points=199),
        )


def test_approval_rejects_cross_case_or_mistyped_measurement_evidence() -> None:
    with pytest.raises(RuntimeError, match="incomplete or inconsistent"):
        graph_policy._validate_graph_read_approval(
            GRAPH_READ_CANARY_CASES[1],
            _approval(),
        )
    with pytest.raises(RuntimeError, match="incomplete or inconsistent"):
        graph_policy._validate_graph_read_approval(
            _CASE,
            replace(_approval(), measurement_evidence_sha256="0" * 64),
        )
    with pytest.raises(RuntimeError, match="incomplete or inconsistent"):
        graph_policy._validate_graph_read_approval(
            _CASE,
            replace(_approval(), measurement_import_run_id="run_mistyped"),
        )


def test_internal_measurement_cannot_exceed_hard_envelope() -> None:
    internal_evidence = _internal_evidence()
    owner = internal_evidence["owners"][1]
    owner["cold"]["metric_deltas"][0]["database_blocks"] = 417

    report = evaluate_graph_read_budget(
        reference_snapshot_id=_CASE.reference_snapshot_id,
        snapshot_id=_SNAPSHOT_ID,
        import_run_id=_IMPORT_RUN_ID,
        image_identity=_IMAGE_IDENTITY,
        public_cold_samples=_cold_samples(),
        public_warm_graph_evidence=_public_graph(),
        internal_owner_evidence=internal_evidence,
    )

    assert report["passed"] is False
    assert any(
        "internal graph-read hard envelope" in failure
        for failure in report["failures"]
    )


def test_graph_measurement_is_bound_to_snapshot_reference_and_image() -> None:
    cold_samples = _cold_samples()
    cold_samples[0]["image_identity"] = "sha256:different"

    report = evaluate_graph_read_budget(
        reference_snapshot_id=_CASE.reference_snapshot_id,
        snapshot_id=_SNAPSHOT_ID,
        import_run_id=_IMPORT_RUN_ID,
        image_identity=_IMAGE_IDENTITY,
        public_cold_samples=cold_samples,
        public_warm_graph_evidence=_public_graph(),
        internal_owner_evidence=_internal_evidence(),
    )

    assert (
        "public cold graph evidence differs from accepted provenance"
        in report["failures"]
    )


def test_cli_has_no_operator_graph_read_ceiling() -> None:
    parser = build_parser()
    subparsers = next(
        action
        for action in parser._actions
        if action.__class__.__name__ == "_SubParsersAction"
    )
    legacy_options = {
        "--maximum-database-bytes",
        "--maximum-database-blocks",
        "--maximum-logical-lookups",
    }

    for command in ("record-cold-sample", "internal-owner-probe", "accept"):
        options = set(subparsers.choices[command]._option_string_actions)
        assert options.isdisjoint(legacy_options)
    assert (
        "--reference-snapshot-id"
        in subparsers.choices["internal-owner-probe"]._option_string_actions
    )
