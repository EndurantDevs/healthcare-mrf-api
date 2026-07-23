"""Pure metric, digest, work, and report helpers for the V4 owner canary."""

from __future__ import annotations

import hashlib
import struct
from typing import Any, Mapping, Sequence

from scripts.ptg_v4_dev_canary_graph_budget import (
    INTERNAL_MAXIMUM_DATABASE_BLOCKS,
    INTERNAL_MAXIMUM_DATABASE_BYTES,
    INTERNAL_MAXIMUM_GRAPH_PAGES,
    INTERNAL_MAXIMUM_LOCATOR_PAGES,
    INTERNAL_MAXIMUM_LOGICAL_LOOKUPS,
    INTERNAL_MAXIMUM_MEMBER_PAGES,
    INTERNAL_MAXIMUM_SOURCE_PAGES,
)

_PREFIX_DIGEST_DOMAIN = b"PTG2V4NPI-PREFIX\x01"
_DELTA_FIELDS = (
    "request_count",
    "database_bytes",
    "database_blocks",
    "logical_lookups",
    "component_fallback_sets",
    "hot_prefix_requests",
    "cold_exact_requests",
    "npi_prefix_override_sets",
    "hot_group_npi_locator_pages",
    "hot_group_npi_member_pages",
)


def metric_failures(
    *,
    diagnostic: Mapping[str, Any],
    owner_spec: Any,
    phase_name: str,
    metric_deltas: Sequence[Mapping[str, int]],
) -> list[str]:
    """Require one hot request scope and bounded graph I/O for every sample."""

    expected_by_field = {
        "request_count": 1,
        "hot_prefix_requests": 1,
        "cold_exact_requests": 0,
        "npi_prefix_override_sets": int(owner_spec.uses_override),
        "component_fallback_sets": (
            0
            if owner_spec.uses_override
            else int(owner_spec.uses_component_fallback)
        ),
    }
    maximum_by_field = {
        "database_bytes": INTERNAL_MAXIMUM_DATABASE_BYTES,
        "database_blocks": INTERNAL_MAXIMUM_DATABASE_BLOCKS,
        "logical_lookups": INTERNAL_MAXIMUM_LOGICAL_LOOKUPS,
    }
    source_pages = (
        0
        if owner_spec.uses_override
        else compiler_work(diagnostic, owner_spec)["observed"]["source_pages"]
    )
    failures: list[str] = []
    for sample_index, metric_delta_by_field in enumerate(metric_deltas):
        if any(
            int(metric_delta_by_field.get(field_name, -1)) != expected_value
            for field_name, expected_value in expected_by_field.items()
        ):
            failures.append(
                f"{owner_spec.role} {phase_name} sample {sample_index} mode metrics differ"
            )
        if any(
            int(metric_delta_by_field.get(field_name, -1)) < 0
            or int(metric_delta_by_field.get(field_name, -1)) > maximum_value
            for field_name, maximum_value in maximum_by_field.items()
        ):
            failures.append(
                f"{owner_spec.role} {phase_name} sample {sample_index} I/O exceeds bounds"
            )
        if _is_sample_outside_page_envelope(
            metric_delta_by_field,
            source_pages=source_pages,
        ):
            failures.append(
                f"{owner_spec.role} {phase_name} sample {sample_index} "
                "graph pages exceed bounds"
            )
    return failures


def _is_sample_outside_page_envelope(
    metric_delta_by_field: Mapping[str, int],
    *,
    source_pages: int,
) -> bool:
    """Return whether one owner request exceeds any packed-page hard cap."""

    locator_pages = int(
        metric_delta_by_field.get("hot_group_npi_locator_pages", -1)
    )
    member_pages = int(
        metric_delta_by_field.get("hot_group_npi_member_pages", -1)
    )
    return bool(
        locator_pages < 0
        or locator_pages > INTERNAL_MAXIMUM_LOCATOR_PAGES
        or member_pages < 0
        or member_pages > INTERNAL_MAXIMUM_MEMBER_PAGES
        or source_pages + locator_pages + member_pages
        > INTERNAL_MAXIMUM_GRAPH_PAGES
    )


def compiler_work_failures(
    diagnostic: Mapping[str, Any],
    owner_spec: Any,
) -> list[str]:
    """Require the non-override owner to remain within sealed compiler caps."""

    if owner_spec.uses_override:
        return []
    work_by_section = compiler_work(diagnostic, owner_spec)
    maximum_by_field = work_by_section["maximum"]
    observed_by_field = work_by_section["observed"]
    if any(
        observed_by_field[field_name] > maximum_by_field[field_name]
        for field_name in maximum_by_field
    ) or observed_by_field["source_pages"] > INTERNAL_MAXIMUM_SOURCE_PAGES:
        return [f"{owner_spec.role} compiler work exceeds sealed online caps"]
    return []


def compiler_work(
    diagnostic: Mapping[str, Any],
    owner_spec: Any,
) -> dict[str, dict[str, int]]:
    """Return redacted group/source work and the matching sealed maxima."""

    prefix = "worst_online" if owner_spec.role.startswith("worst_online") else "worst"
    observed_by_field = {
        "groups": int(diagnostic[f"{prefix}_groups_to_target"]),
        "source_owners": int(diagnostic[f"{prefix}_source_owner_work"]),
        "source_members": int(diagnostic[f"{prefix}_source_member_work"]),
        "source_pages": int(diagnostic[f"{prefix}_source_page_work"]),
        "source_bytes": int(diagnostic[f"{prefix}_source_byte_work"]),
    }
    maximum_by_field = {
        "groups": int(diagnostic["max_online_group_keys_per_set"]),
        "source_owners": int(diagnostic["max_online_source_owners_per_set"]),
        "source_members": int(diagnostic["max_online_source_members_per_set"]),
        "source_pages": int(diagnostic["max_online_source_pages_per_set"]),
        "source_bytes": int(diagnostic["max_online_source_bytes_per_set"]),
    }
    return {
        "observed": observed_by_field,
        "maximum": maximum_by_field,
    }


def series_report(series: Any, p95_ms: float) -> dict[str, Any]:
    """Return latency and bounded metric deltas without external NPI values."""

    return {
        "sample_count": len(series.latencies_ms),
        "p95_ms": round(float(p95_ms), 3),
        "maximum_ms": round(max(series.latencies_ms), 3),
        "metric_deltas": list(series.metric_deltas),
    }


def owner_mode(owner_spec: Any) -> str:
    """Name the compiler-authenticated serving mode without public ids."""

    if owner_spec.uses_override:
        return "prefix_override"
    if owner_spec.uses_component_fallback:
        return "online_source_component"
    return "online_factor"


def _owner_workload_failures(
    evidence_by_field: Mapping[str, Any],
) -> list[str]:
    """Require both compiler-selected probes to prove exactly 201 members."""

    owners = evidence_by_field.get("owners")
    if not isinstance(owners, list):
        return ["internal worst-owner evidence lacks owner workloads"]
    owners_by_role = {
        str(owner.get("role") or ""): owner
        for owner in owners
        if isinstance(owner, Mapping)
    }
    required_roles = {"overall_worst", "worst_online_non_override"}
    failures: list[str] = []
    if len(owners) != 2 or set(owners_by_role) != required_roles:
        failures.append(
            "internal worst-owner evidence does not contain exactly two selected owners"
        )
    for role in sorted(required_roles):
        owner = owners_by_role.get(role)
        if owner is None:
            failures.append(f"internal {role} workload is missing")
            continue
        if (
            owner.get("expected_member_count") != 201
            or owner.get("actual_member_count") != 201
            or owner.get("passed") is not True
        ):
            failures.append(
                f"internal {role} workload is not an accepted exact 201-member prefix"
            )
    return failures


def validate_internal_evidence(
    evidence_by_field: Mapping[str, Any],
    snapshot_id: str,
    *,
    expected_reference_snapshot_id: str,
    expected_image_identity: str,
) -> dict[str, Any]:
    """Bind a successful internal probe to the accepted snapshot and image."""

    failures = list(evidence_by_field.get("failures") or [])
    if evidence_by_field.get("contract") != "ptg_v4_internal_owner_probe_v1":
        failures.append("internal worst-owner evidence contract is missing")
    if evidence_by_field.get("snapshot_id") != snapshot_id:
        failures.append("internal worst-owner snapshot differs from acceptance")
    if (
        evidence_by_field.get("reference_snapshot_id")
        != expected_reference_snapshot_id
    ):
        failures.append(
            "internal worst-owner reference snapshot differs from acceptance"
        )
    if evidence_by_field.get("image_identity") != expected_image_identity:
        failures.append("internal worst-owner image differs from public evidence")
    for field_name in (
        "process_identity",
        "process_started_at",
        "image_identity",
    ):
        field_value = evidence_by_field.get(field_name)
        if not isinstance(field_value, str) or not field_value.strip():
            failures.append(
                f"internal worst-owner {field_name} is missing"
            )
    if evidence_by_field.get("passed") is not True:
        failures.append("internal worst-owner production probe did not pass")
    failures.extend(_owner_workload_failures(evidence_by_field))
    return {
        **dict(evidence_by_field),
        "passed": not failures,
        "failures": failures,
    }


def metric_delta(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> dict[str, int]:
    """Subtract cumulative V4 counters for one isolated request."""

    return {
        field_name: int(after.get(field_name) or 0)
        - int(before.get(field_name) or 0)
        for field_name in _DELTA_FIELDS
    }


def prefix_digest(npi_keys: Sequence[int]) -> str:
    """Match the Rust compiler's ordered snapshot-local prefix digest."""

    digest = hashlib.sha256()
    digest.update(_PREFIX_DIGEST_DOMAIN)
    digest.update(struct.pack(">Q", len(npi_keys)))
    for npi_key in npi_keys:
        if isinstance(npi_key, bool) or not 0 <= int(npi_key) <= 0xFFFF_FFFF:
            raise RuntimeError("snapshot-local NPI key is outside uint32")
        digest.update(struct.pack(">I", int(npi_key)))
    return digest.hexdigest()
