# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from dataclasses import replace

import pytest

from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LEGACY_ROOT_PREFIXES,
    LegacyBlockedSuffix,
    LegacyRootRelation,
    LegacySuffixOwnership,
    LegacySweepCandidate,
    LegacySweepLimits,
    build_bounded_legacy_sweep_plan,
    canonical_sha256,
    classify_legacy_suffix,
    embedded_legacy_suffix,
    legacy_relation_suffixes,
    legacy_root_identity,
    legacy_sweep_audit_id,
)


SUFFIX_A = "1" * 32
SUFFIX_B = "2" * 32


def _relation(
    suffix: str = SUFFIX_A,
    *,
    prefix: str = "ptg_file",
    oid: int = 11,
    total_bytes: int = 100,
    has_rows: bool | None = False,
) -> LegacyRootRelation:
    return LegacyRootRelation(
        table_name=f"{prefix}_{suffix}",
        relation_oid=oid,
        namespace_oid=7,
        owner_oid=8,
        relkind="r",
        persistence="p",
        total_bytes=total_bytes,
        schema_digest="a" * 64,
        dependent_relation_oids=(oid + 100,),
        dependent_relation_names=(f"{prefix}_{suffix}_pkey",),
        has_rows=has_rows,
    )


def _owned(
    *,
    snapshot_status: str = "failed",
    reference: str | None = None,
    fence: bool = False,
    control_status: str = "failed",
    placement_status: str = "inactive",
    internal_status: str = "failed",
) -> LegacySuffixOwnership:
    return LegacySuffixOwnership(
        snapshot_statuses=(("snapshot-a", snapshot_status),),
        declared_snapshot_ids=("snapshot-a",),
        internal_run_statuses=(("ptg2:owner", internal_status),),
        mirror_run_statuses=(("run-a", "failed"),),
        control_import_statuses=(("owner", control_status),),
        placement_statuses=(("placement-a", placement_status),),
        active_references=((reference,) if reference else ()),
        fence_states=((("fence-a", "active"),) if fence else ()),
        evidence_kinds=("snapshot", "control_import"),
    )


@pytest.mark.parametrize("prefix", LEGACY_ROOT_PREFIXES)
def test_exact_root_allowlist_accepts_every_frozen_family(prefix: str) -> None:
    assert legacy_root_identity(f"{prefix}_{SUFFIX_A}") == (
        prefix,
        SUFFIX_A,
    )


@pytest.mark.parametrize(
    "relation_name",
    (
        f"ptg_file_{SUFFIX_A}_pkey",
        f"unrelated_{SUFFIX_A}",
        f"ptg_file_{'a' * 32}".upper(),
        "ptg_file_short",
        f"public.ptg_file_{SUFFIX_A}",
    ),
)
def test_exact_root_allowlist_rejects_lookalikes(relation_name: str) -> None:
    assert legacy_root_identity(relation_name) is None


def test_embedded_suffix_requires_one_unambiguous_lowercase_identity() -> None:
    assert embedded_legacy_suffix(
        f"ptg_file_{SUFFIX_A}_file_type_idx"
    ) == SUFFIX_A
    assert embedded_legacy_suffix(
        f"ptg_file_{SUFFIX_A}_{SUFFIX_B}_idx"
    ) is None
    assert legacy_relation_suffixes(
        f"ptg_file_{SUFFIX_A}_{SUFFIX_B}_idx"
    ) == (SUFFIX_A, SUFFIX_B)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("relation_oid", 0, "OID"),
        ("namespace_oid", 9, "another schema"),
        ("owner_oid", 10, "owner"),
        ("relkind", "f", "regular table"),
        ("persistence", "t", "persistence"),
        ("schema_digest", "short", "digest"),
    ),
)
def test_relation_catalog_validation_fails_closed(
    field: str,
    value: object,
    message: str,
) -> None:
    relation = replace(_relation(), **{field: value})
    with pytest.raises(ValueError, match=message):
        relation.validate(expected_namespace_oid=7, expected_owner_oid=8)


def test_empty_unowned_suffix_is_removable_but_nonempty_is_not() -> None:
    empty = classify_legacy_suffix(
        SUFFIX_A,
        (_relation(),),
        LegacySuffixOwnership(),
    )
    nonempty = classify_legacy_suffix(
        SUFFIX_A,
        (_relation(has_rows=True),),
        LegacySuffixOwnership(),
    )

    assert isinstance(empty, LegacySweepCandidate)
    assert empty.proof_kind == "empty_orphan"
    assert isinstance(nonempty, LegacyBlockedSuffix)
    assert nonempty.reasons == (
        "authoritative_owner_missing",
        "nonempty_orphan",
    )


def test_terminal_owned_suffix_can_remove_nonempty_non_serving_tables() -> None:
    result = classify_legacy_suffix(
        SUFFIX_A,
        (_relation(has_rows=True),),
        _owned(snapshot_status="published", control_status="succeeded"),
    )

    assert isinstance(result, LegacySweepCandidate)
    assert result.proof_kind == "terminal_non_serving"
    assert result.snapshot_ids == ("snapshot-a",)
    assert result.nonempty_table_count == 1


def test_inactive_placement_alone_never_authorizes_nonempty_cleanup() -> None:
    result = classify_legacy_suffix(
        SUFFIX_A,
        (_relation(has_rows=True),),
        LegacySuffixOwnership(
            placement_statuses=(("placement-a", "inactive"),),
            evidence_kinds=("file_placement",),
        ),
    )

    assert isinstance(result, LegacyBlockedSuffix)
    assert "authoritative_owner_missing" in result.reasons


@pytest.mark.parametrize(
    "relations",
    (
        (),
        (_relation(), _relation()),
        (_relation(), _relation(SUFFIX_B, oid=22)),
        (
            _relation(),
            replace(
                _relation(prefix="ptg_billing_code", oid=22),
                dependent_relation_oids=(111,),
            ),
        ),
    ),
)
def test_destructive_contract_rejects_ambiguous_relation_sets(
    relations: tuple[LegacyRootRelation, ...],
) -> None:
    with pytest.raises(ValueError):
        classify_legacy_suffix(
            SUFFIX_A,
            relations,
            LegacySuffixOwnership(),
        )


@pytest.mark.parametrize(
    ("ownership", "reason"),
    (
        (_owned(snapshot_status="building"), "snapshot_status_building"),
        (_owned(snapshot_status="validated"), "snapshot_status_validated"),
        (_owned(reference="active_route"), "serving_or_lifecycle_reference"),
        (_owned(fence=True), "attempt_fence_present"),
        (_owned(control_status="planned"), "control_import_status_planned"),
        (_owned(placement_status="active"), "active_file_placement"),
        (
            _owned(placement_status="mystery"),
            "file_placement_status_mystery",
        ),
        (
            _owned(internal_status="running"),
            "internal_run_status_running",
        ),
    ),
)
def test_active_or_ambiguous_ownership_is_never_removed(
    ownership: LegacySuffixOwnership,
    reason: str,
) -> None:
    result = classify_legacy_suffix(
        SUFFIX_A,
        (_relation(),),
        ownership,
    )

    assert isinstance(result, LegacyBlockedSuffix)
    assert reason in result.reasons


def test_validated_run_needs_published_or_missing_succeeded_owner() -> None:
    no_terminal_owner = LegacySuffixOwnership(
        internal_run_statuses=(("ptg2:owner", "validated"),),
        evidence_kinds=("internal_run",),
    )
    missing_succeeded_owner = LegacySuffixOwnership(
        declared_snapshot_ids=("missing-snapshot",),
        internal_run_statuses=(("ptg2:owner", "validated"),),
        control_import_statuses=(("owner", "succeeded"),),
        evidence_kinds=("internal_run", "control_import"),
    )

    blocked = classify_legacy_suffix(
        SUFFIX_A,
        (_relation(),),
        no_terminal_owner,
    )
    removable = classify_legacy_suffix(
        SUFFIX_A,
        (_relation(has_rows=True),),
        missing_succeeded_owner,
    )

    assert isinstance(blocked, LegacyBlockedSuffix)
    assert "validated_run_without_terminal_owner" in blocked.reasons
    assert isinstance(removable, LegacySweepCandidate)


def test_bounded_plan_is_sorted_and_stops_before_first_exceeded_bound() -> None:
    candidate_b = classify_legacy_suffix(
        SUFFIX_B,
        (_relation(SUFFIX_B, oid=22, total_bytes=200),),
        LegacySuffixOwnership(),
    )
    candidate_a = classify_legacy_suffix(
        SUFFIX_A,
        (_relation(total_bytes=100),),
        LegacySuffixOwnership(),
    )
    assert isinstance(candidate_a, LegacySweepCandidate)
    assert isinstance(candidate_b, LegacySweepCandidate)
    plan = build_bounded_legacy_sweep_plan(
        schema_name="mrf",
        control_schema_name="control_plane",
        authority_digest="b" * 64,
        catalog_digest="c" * 64,
        eligible_candidates=(candidate_b, candidate_a),
        blocked=(),
        limits=LegacySweepLimits(
            max_suffixes=2,
            max_tables=2,
            max_relations=4,
            max_bytes=150,
        ),
    )

    assert [candidate.suffix for candidate in plan.candidates] == [SUFFIX_A]
    assert plan.total_bytes == 100
    assert plan.remaining_eligible_suffix_count == 1
    assert plan.plan_digest == canonical_sha256(plan.audit_payload())
    assert len(legacy_sweep_audit_id(plan.plan_digest)) == 64


def test_oversized_lexical_prefix_does_not_starve_later_candidate() -> None:
    oversized = classify_legacy_suffix(
        SUFFIX_A,
        (_relation(total_bytes=151),),
        LegacySuffixOwnership(),
    )
    later = classify_legacy_suffix(
        SUFFIX_B,
        (_relation(SUFFIX_B, oid=22, total_bytes=100),),
        LegacySuffixOwnership(),
    )
    assert isinstance(oversized, LegacySweepCandidate)
    assert isinstance(later, LegacySweepCandidate)

    plan = build_bounded_legacy_sweep_plan(
        schema_name="mrf",
        control_schema_name="control_plane",
        authority_digest="b" * 64,
        catalog_digest="c" * 64,
        eligible_candidates=(oversized, later),
        blocked=(),
        limits=LegacySweepLimits(2, 2, 4, 150),
    )

    assert [candidate.suffix for candidate in plan.candidates] == [SUFFIX_B]
    assert plan.blocked == (
        LegacyBlockedSuffix(
            suffix=SUFFIX_A,
            reasons=("candidate_exceeds_max_bytes",),
            table_count=1,
            total_bytes=151,
        ),
    )


def test_plan_digest_binds_relation_oid_and_schema_digest() -> None:
    first = classify_legacy_suffix(
        SUFFIX_A,
        (_relation(),),
        LegacySuffixOwnership(),
    )
    second = classify_legacy_suffix(
        SUFFIX_A,
        (replace(_relation(), relation_oid=12),),
        LegacySuffixOwnership(),
    )
    assert isinstance(first, LegacySweepCandidate)
    assert isinstance(second, LegacySweepCandidate)
    limits = LegacySweepLimits(1, 1, 2, 1000)
    first_plan = build_bounded_legacy_sweep_plan(
        schema_name="mrf",
        control_schema_name="control_plane",
        authority_digest="b" * 64,
        catalog_digest="c" * 64,
        eligible_candidates=(first,),
        blocked=(),
        limits=limits,
    )
    second_plan = build_bounded_legacy_sweep_plan(
        schema_name="mrf",
        control_schema_name="control_plane",
        authority_digest="b" * 64,
        catalog_digest="c" * 64,
        eligible_candidates=(second,),
        blocked=(),
        limits=limits,
    )

    assert first_plan.plan_digest != second_plan.plan_digest


def test_plan_digest_binds_effective_limits_even_when_selection_is_equal() -> None:
    candidate = classify_legacy_suffix(
        SUFFIX_A,
        (_relation(total_bytes=10),),
        LegacySuffixOwnership(),
    )
    assert isinstance(candidate, LegacySweepCandidate)
    plan_parameters_by_field = {
        "schema_name": "mrf",
        "control_schema_name": "control",
        "authority_digest": "1" * 64,
        "catalog_digest": "2" * 64,
        "eligible_candidates": (candidate,),
        "blocked": (),
    }

    narrow_plan = build_bounded_legacy_sweep_plan(
        **plan_parameters_by_field,
        limits=LegacySweepLimits(1, 1, 2, 10),
    )
    wider_plan = build_bounded_legacy_sweep_plan(
        **plan_parameters_by_field,
        limits=LegacySweepLimits(2, 2, 2, 20),
    )

    assert narrow_plan.candidates == wider_plan.candidates
    assert narrow_plan.plan_digest != wider_plan.plan_digest
    assert narrow_plan.audit_payload()["limits"] == {
        "max_suffixes": 1,
        "max_tables": 1,
        "max_relations": 2,
        "max_bytes": 10,
    }


def test_execution_bounds_have_non_overridable_hard_ceilings() -> None:
    with pytest.raises(ValueError, match="hard ceiling"):
        LegacySweepLimits(
            max_suffixes=101,
            max_tables=1,
            max_relations=1,
            max_bytes=1,
        ).validate()
