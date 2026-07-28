# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure decisions for bounded legacy PTG relation cleanup."""

from __future__ import annotations

import hashlib
from typing import Iterable

from process.ptg_parts.ptg2_legacy_orphan_models import (
    CONTROL_TERMINAL_STATUSES,
    INTERNAL_TERMINAL_STATUSES,
    LEGACY_ROOT_PREFIXES,
    LEGACY_SWEEP_AUDIT_TABLE,
    LEGACY_SWEEP_CONTRACT,
    MIRROR_TERMINAL_STATUSES,
    PLACEMENT_TERMINAL_STATUSES,
    SNAPSHOT_TERMINAL_STATUSES,
    _AUTHORITATIVE_OWNER_KINDS,
    _HEX_32_PATTERN,
    _HEX_64_PATTERN,
    LegacyCatalogProgress,
    LegacyBlockedSuffix,
    LegacyRootRelation,
    LegacySuffixOwnership,
    LegacySweepCandidate,
    LegacySweepLimits,
    LegacySweepPlan,
    canonical_sha256,
    embedded_legacy_suffix,
    legacy_relation_suffixes,
    legacy_root_identity,
)

def _status_reasons(
    label: str,
    rows: tuple[tuple[str, str], ...],
    allowed: frozenset[str],
) -> list[str]:
    return [
        f"{label}_status_{status or 'missing'}"
        for _identity, status in rows
        if status not in allowed
    ]


def _validated_run_reasons(
    ownership: LegacySuffixOwnership,
) -> list[str]:
    validated_runs = any(
        status == "validated"
        for _identity, status in ownership.internal_run_statuses
    )
    if not validated_runs:
        return []
    has_published_snapshot = any(
        status == "published"
        for _identity, status in ownership.snapshot_statuses
    )
    existing_snapshot_ids = {
        existing_id for existing_id, _status in ownership.snapshot_statuses
    }
    has_missing_terminal_snapshot = bool(
        ownership.declared_snapshot_ids
    ) and all(
        snapshot_id not in existing_snapshot_ids
        for snapshot_id in ownership.declared_snapshot_ids
    )
    has_succeeded_control = bool(
        ownership.control_import_statuses
    ) and all(
        status == "succeeded"
        for _identity, status in ownership.control_import_statuses
    )
    if has_published_snapshot or (
        has_missing_terminal_snapshot and has_succeeded_control
    ):
        return []
    return ["validated_run_without_terminal_owner"]


def _orphan_proof_reasons(
    relations: tuple[LegacyRootRelation, ...],
    ownership: LegacySuffixOwnership,
) -> list[str]:
    if ownership.evidence_kinds:
        return []
    if any(relation.has_rows is None for relation in relations):
        return ["empty_orphan_proof_missing"]
    if any(relation.has_rows for relation in relations):
        return ["nonempty_orphan"]
    return []


def legacy_suffix_blocking_reasons(
    relations: Iterable[LegacyRootRelation],
    ownership: LegacySuffixOwnership,
) -> tuple[str, ...]:
    """Return every reason that prevents a suffix from being removed."""

    normalized = ownership.normalized()
    reasons = list(normalized.ambiguity_reasons)
    if normalized.active_references:
        reasons.append("serving_or_lifecycle_reference")
    if normalized.fence_states:
        reasons.append("attempt_fence_present")
    reasons.extend(
        _status_reasons(
            "snapshot",
            normalized.snapshot_statuses,
            SNAPSHOT_TERMINAL_STATUSES,
        )
    )
    reasons.extend(
        _status_reasons(
            "control_import",
            normalized.control_import_statuses,
            CONTROL_TERMINAL_STATUSES,
        )
    )
    reasons.extend(
        _status_reasons(
            "mirror_run",
            normalized.mirror_run_statuses,
            MIRROR_TERMINAL_STATUSES,
        )
    )
    reasons.extend(
        _status_reasons(
            "internal_run",
            normalized.internal_run_statuses,
            INTERNAL_TERMINAL_STATUSES,
        )
    )
    for _identity, status in normalized.placement_statuses:
        if status == "active":
            reasons.append("active_file_placement")
        elif status not in PLACEMENT_TERMINAL_STATUSES:
            reasons.append(
                f"file_placement_status_{status or 'missing'}"
            )
    relation_values = tuple(relations)
    reasons.extend(_validated_run_reasons(normalized))
    reasons.extend(_orphan_proof_reasons(relation_values, normalized))
    return tuple(sorted(set(reasons)))


def _validate_candidate_relations(
    suffix: str,
    relations: tuple[LegacyRootRelation, ...],
) -> None:
    if not _HEX_32_PATTERN.fullmatch(str(suffix or "")):
        raise ValueError("legacy cleanup suffix is invalid")
    if not relations:
        raise ValueError("legacy cleanup suffix has no root relations")
    if any(relation.suffix != suffix for relation in relations):
        raise ValueError("legacy cleanup relation suffix mismatch")
    table_names = [relation.table_name for relation in relations]
    root_oids = [relation.relation_oid for relation in relations]
    dependent_oids = [
        dependent_oid
        for relation in relations
        for dependent_oid in relation.dependent_relation_oids
    ]
    if len(set(table_names)) != len(table_names):
        raise ValueError("legacy cleanup root table is duplicated")
    if len(set(root_oids)) != len(root_oids):
        raise ValueError("legacy cleanup root OID is duplicated")
    if len(set(dependent_oids)) != len(dependent_oids):
        raise ValueError("legacy cleanup dependent OID is duplicated")


def classify_legacy_suffix(
    suffix: str,
    relations: Iterable[LegacyRootRelation],
    ownership: LegacySuffixOwnership,
) -> LegacySweepCandidate | LegacyBlockedSuffix:
    """Classify one fully inspected suffix without database side effects."""

    relation_values = tuple(
        sorted(relations, key=lambda relation: relation.table_name)
    )
    _validate_candidate_relations(suffix, relation_values)
    reasons = legacy_suffix_blocking_reasons(
        relation_values,
        ownership,
    )
    normalized_ownership = ownership.normalized()
    authoritative_owner = bool(
        _AUTHORITATIVE_OWNER_KINDS.intersection(
            normalized_ownership.evidence_kinds
        )
    )
    if (
        not authoritative_owner
        and any(relation.has_rows for relation in relation_values)
    ):
        reasons = tuple(
            sorted({*reasons, "authoritative_owner_missing"})
        )
    if reasons:
        return LegacyBlockedSuffix(
            suffix=suffix,
            reasons=reasons,
            table_count=len(relation_values),
            total_bytes=sum(
                relation.total_bytes for relation in relation_values
            ),
        )
    proof_kind = (
        "terminal_non_serving"
        if authoritative_owner
        else "empty_orphan"
    )
    return LegacySweepCandidate(
        suffix=suffix,
        proof_kind=proof_kind,
        relations=relation_values,
        ownership=ownership.normalized(),
    )


def _candidate_limit_reasons(
    candidate: LegacySweepCandidate,
    limits: LegacySweepLimits,
) -> tuple[str, ...]:
    return tuple(
        reason
        for is_exceeded, reason in (
            (
                limits.max_suffixes < 1,
                "candidate_exceeds_max_suffixes",
            ),
            (
                candidate.table_count > limits.max_tables,
                "candidate_exceeds_max_tables",
            ),
            (
                candidate.relation_count > limits.max_relations,
                "candidate_exceeds_max_relations",
            ),
            (
                candidate.total_bytes > limits.max_bytes,
                "candidate_exceeds_max_bytes",
            ),
        )
        if is_exceeded
    )


def _select_candidates_within_limits(
    ordered_candidates: tuple[LegacySweepCandidate, ...],
    limits: LegacySweepLimits,
) -> tuple[
    tuple[LegacySweepCandidate, ...],
    tuple[LegacyBlockedSuffix, ...],
]:
    """Return the deterministic candidate prefix within every bound."""

    selected_candidates: list[LegacySweepCandidate] = []
    oversized_candidates: list[LegacyBlockedSuffix] = []
    selected_tables = 0
    selected_relations = 0
    selected_bytes = 0
    for candidate in ordered_candidates:
        exceeded_limits = _candidate_limit_reasons(candidate, limits)
        if exceeded_limits:
            oversized_candidates.append(
                LegacyBlockedSuffix(
                    suffix=candidate.suffix,
                    reasons=exceeded_limits,
                    table_count=candidate.table_count,
                    total_bytes=candidate.total_bytes,
                )
            )
            continue
        next_suffixes = len(selected_candidates) + 1
        next_tables = selected_tables + candidate.table_count
        next_relations = selected_relations + candidate.relation_count
        next_bytes = selected_bytes + candidate.total_bytes
        if (
            next_suffixes > limits.max_suffixes
            or next_tables > limits.max_tables
            or next_relations > limits.max_relations
            or next_bytes > limits.max_bytes
        ):
            continue
        selected_candidates.append(candidate)
        selected_tables = next_tables
        selected_relations = next_relations
        selected_bytes = next_bytes
    return tuple(selected_candidates), tuple(oversized_candidates)


def _resolved_catalog_progress(
    candidate_count: int,
    blocked_count: int,
    supplied_progress: LegacyCatalogProgress | None,
) -> LegacyCatalogProgress:
    classified_count = candidate_count + blocked_count
    resolved_progress = supplied_progress or LegacyCatalogProgress(
        catalog_suffix_count=classified_count,
        scanned_suffix_count=classified_count,
    )
    resolved_progress.validate(classified_suffix_count=classified_count)
    return resolved_progress


def _is_catalog_digest_set_valid(*digests: str) -> bool:
    return all(_HEX_64_PATTERN.fullmatch(digest) for digest in digests)


def build_bounded_legacy_sweep_plan(
    *,
    schema_name: str,
    control_schema_name: str,
    authority_digest: str,
    catalog_digest: str,
    eligible_candidates: Iterable[LegacySweepCandidate],
    blocked: Iterable[LegacyBlockedSuffix],
    limits: LegacySweepLimits,
    catalog_progress: LegacyCatalogProgress | None = None,
) -> LegacySweepPlan:
    """Select a deterministic prefix that fits every execution bound."""

    limits.validate()
    if not _is_catalog_digest_set_valid(authority_digest, catalog_digest):
        raise ValueError("legacy sweep catalog digest is invalid")
    ordered_candidates = tuple(
        sorted(eligible_candidates, key=lambda candidate: candidate.suffix)
    )
    blocked_suffixes = tuple(blocked)
    resolved_progress = _resolved_catalog_progress(
        len(ordered_candidates), len(blocked_suffixes), catalog_progress
    )
    selected_candidates, oversized_candidates = _select_candidates_within_limits(
        ordered_candidates,
        limits,
    )
    base_payload_by_field = {
        "contract": LEGACY_SWEEP_CONTRACT,
        "schema_name": schema_name,
        "control_schema_name": control_schema_name,
        "authority_digest": authority_digest,
        "catalog_digest": catalog_digest,
        "catalog_suffix_count": resolved_progress.catalog_suffix_count,
        "scanned_suffix_count": resolved_progress.scanned_suffix_count,
        "limits": limits.payload(),
        "candidates": [
            candidate.payload() for candidate in selected_candidates
        ],
    }
    return LegacySweepPlan(
        schema_name=schema_name,
        control_schema_name=control_schema_name,
        authority_digest=authority_digest,
        catalog_digest=catalog_digest,
        candidates=selected_candidates,
        blocked=tuple(
            sorted(
                (*blocked_suffixes, *oversized_candidates),
                key=lambda item: item.suffix,
            )
        ),
        eligible_suffix_count=len(ordered_candidates),
        limits=limits,
        plan_digest=canonical_sha256(base_payload_by_field),
        catalog_suffix_count=resolved_progress.catalog_suffix_count,
        scanned_suffix_count=resolved_progress.scanned_suffix_count,
    )


def legacy_sweep_audit_id(plan_digest: str) -> str:
    """Build an idempotent audit identity from one reviewed plan."""

    if not _HEX_64_PATTERN.fullmatch(plan_digest):
        raise ValueError("legacy sweep plan digest is invalid")
    return hashlib.sha256(
        f"{LEGACY_SWEEP_CONTRACT}\0{plan_digest}".encode("ascii")
    ).hexdigest()
