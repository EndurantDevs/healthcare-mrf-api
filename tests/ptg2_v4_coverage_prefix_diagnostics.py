from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from process.ptg_parts import ptg2_v4_graph_compiler as compiler


def _prefix_context(
    *,
    provider_sets: int,
    simulated_sets: int,
    override_owners: int,
) -> compiler._PrefixDiagnosticContext:
    source_work_by_dimension = {
        "owner": 0,
        "member": 0,
        "page": 0,
        "byte": 0,
    }
    group_npi_work_by_dimension = {
        "member": 0,
        "locator_page": 0,
        "member_page": 0,
        "byte": 0,
        "batch": 0,
    }
    return compiler._PrefixDiagnosticContext(
        provider_set_count=provider_sets,
        simulated_set_count=simulated_sets,
        override_owner_count=override_owners,
        groups_to_target_percentiles=(0, 0, 0, 1 if simulated_sets else 0),
        source_maxima_by_dimension=source_work_by_dimension,
        source_limits_by_dimension=source_work_by_dimension,
        group_npi_maxima_by_dimension=group_npi_work_by_dimension,
        group_npi_limits_by_dimension=group_npi_work_by_dimension,
    )


def _worst_owner_observe(
    *,
    key: Any = None,
    groups_to_target: int = 0,
    uses_override: Any = False,
    uses_component: Any = False,
    member_count: int = 0,
    digest: Any = None,
) -> dict[str, Any]:
    return {
        "npi_prefix_worst_provider_set_key": key,
        "npi_prefix_worst_groups_to_target": groups_to_target,
        "npi_prefix_worst_provider_set_uses_override": uses_override,
        "npi_prefix_worst_uses_component_fallback": uses_component,
        "npi_prefix_worst_member_count": member_count,
        "npi_prefix_worst_member_digest": digest,
        **{
            f"npi_prefix_worst_source_{dimension}_work": 0
            for dimension in ("owner", "member", "page", "byte")
        },
        **{
            f"npi_prefix_worst_group_npi_{dimension}_work": 0
            for dimension in (
                "member",
                "locator_page",
                "member_page",
                "byte",
                "batch",
            )
        },
    }


def _online_owner_observe(
    *,
    key: Any = None,
    groups_to_target: int = 0,
    is_exact: Any = False,
    uses_component: Any = False,
    member_count: int = 0,
    digest: Any = None,
    group_work_bound: int = 0,
) -> dict[str, Any]:
    return {
        "npi_prefix_worst_online_provider_set_key": key,
        "npi_prefix_worst_online_groups_to_target": groups_to_target,
        "npi_prefix_worst_online_groups_to_target_exact": is_exact,
        "npi_prefix_worst_online_uses_component_fallback": uses_component,
        "npi_prefix_worst_online_member_count": member_count,
        "npi_prefix_worst_online_member_digest": digest,
        "npi_prefix_worst_online_group_work_bound": group_work_bound,
        **{
            f"npi_prefix_worst_online_source_{dimension}_work": 0
            for dimension in ("owner", "member", "page", "byte")
        },
        **{
            f"npi_prefix_worst_online_group_npi_{dimension}_work": 0
            for dimension in (
                "member",
                "locator_page",
                "member_page",
                "byte",
                "batch",
            )
        },
    }


def _assert_worst_owner_fail_closed(
    option_by_name,
    empty_context,
    active_context,
) -> None:
    with pytest.raises(RuntimeError, match="mode is invalid"):
        compiler._validate_worst_prefix_owner(
            _worst_owner_observe(uses_override=None),
            option_by_name,
            empty_context,
        )
    with pytest.raises(RuntimeError, match="empty worst-owner"):
        compiler._validate_worst_prefix_owner(
            _worst_owner_observe(key=1),
            option_by_name,
            empty_context,
        )
    assert compiler._validate_worst_prefix_owner(
        _worst_owner_observe(),
        option_by_name,
        empty_context,
    ) == (None, False)
    with pytest.raises(RuntimeError, match="provider-set key is invalid"):
        compiler._validate_worst_prefix_owner(
            _worst_owner_observe(),
            option_by_name,
            active_context,
        )
    with pytest.raises(RuntimeError, match="diagnostics are inconsistent"):
        compiler._validate_worst_prefix_owner(
            _worst_owner_observe(
                key=1,
                groups_to_target=2,
                member_count=1,
                digest="ab" * 32,
            ),
            option_by_name,
            active_context,
        )


def _assert_online_owner_fail_closed(
    option_by_name,
    active_context,
) -> None:
    no_online_context = _prefix_context(
        provider_sets=1,
        simulated_sets=1,
        override_owners=1,
    )
    with pytest.raises(RuntimeError, match="exactness is invalid"):
        compiler._validate_online_prefix_owner(
            _online_owner_observe(is_exact=None),
            option_by_name,
            active_context,
        )
    with pytest.raises(RuntimeError, match="empty worst-online"):
        compiler._validate_online_prefix_owner(
            _online_owner_observe(key=1),
            option_by_name,
            no_online_context,
        )
    assert (
        compiler._validate_online_prefix_owner(
            _online_owner_observe(),
            option_by_name,
            no_online_context,
        )
        is None
    )
    with pytest.raises(RuntimeError, match="diagnostics are inconsistent"):
        compiler._validate_online_prefix_owner(
            _online_owner_observe(),
            option_by_name,
            active_context,
        )


def _assert_pattern_memo_fail_closed() -> None:
    with pytest.raises(RuntimeError, match="pattern-memo diagnostics"):
        compiler._validate_npi_pattern_diagnostics(
            {
                "npi_patterns_per_npi_p50": 2,
                "npi_patterns_per_npi_p95": 1,
                "npi_patterns_per_npi_p99": 3,
                "maximum_patterns_per_npi": 3,
                "group_set_expansion_edge_visits": 0,
                "group_set_incidence_count": 0,
            }
        )


def test_compiler_prefix_owner_diagnostics_fail_closed() -> None:
    option_by_name = {
        "npi_prefix_target": 10,
        "max_online_group_keys_per_set": 10,
    }
    empty_context = _prefix_context(
        provider_sets=0,
        simulated_sets=0,
        override_owners=0,
    )
    active_context = _prefix_context(
        provider_sets=1,
        simulated_sets=1,
        override_owners=0,
    )

    _assert_worst_owner_fail_closed(
        option_by_name,
        empty_context,
        active_context,
    )
    _assert_online_owner_fail_closed(option_by_name, active_context)
    _assert_pattern_memo_fail_closed()


def test_compiler_accepts_risk_ranked_owner_below_group_maximum() -> None:
    option_by_name = {
        "npi_prefix_target": 10,
        "max_online_group_keys_per_set": 10,
    }
    context = compiler._PrefixDiagnosticContext(
        provider_set_count=2,
        simulated_set_count=2,
        override_owner_count=0,
        groups_to_target_percentiles=(1, 3, 5, 5),
        source_maxima_by_dimension={
            "owner": 0,
            "member": 0,
            "page": 0,
            "byte": 512,
        },
        source_limits_by_dimension={
            "owner": 1,
            "member": 1,
            "page": 1,
            "byte": 512,
        },
        group_npi_maxima_by_dimension={
            "member": 0,
            "locator_page": 0,
            "member_page": 0,
            "byte": 0,
            "batch": 0,
        },
        group_npi_limits_by_dimension={
            "member": 0,
            "locator_page": 0,
            "member_page": 0,
            "byte": 0,
            "batch": 0,
        },
    )
    observe = _worst_owner_observe(
        key=7,
        groups_to_target=4,
        member_count=10,
        digest="ab" * 32,
    )
    observe["npi_prefix_worst_source_byte_work"] = 512

    assert compiler._validate_worst_prefix_owner(
        observe,
        option_by_name,
        context,
    ) == (7, False)


def test_compiler_rejects_unavailable_factor_artifact(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="factor artifact is unavailable"):
        compiler._artifact_manifest({"path": str(tmp_path / "missing.bin")})
