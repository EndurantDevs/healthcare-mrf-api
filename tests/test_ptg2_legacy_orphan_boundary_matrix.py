# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed unit boundaries for the legacy PTG orphan sweeper."""

from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_legacy_orphan_store_catalog as catalog_store
from process.ptg_parts.ptg2_legacy_orphan_contract import (
    LegacyRootRelation,
    LegacySuffixOwnership,
    LegacySweepCandidate,
    LegacySweepLimits,
    build_bounded_legacy_sweep_plan,
    classify_legacy_suffix,
    legacy_sweep_audit_id,
)
from process.ptg_parts.ptg2_legacy_orphan_store_catalog import (
    _DependencyCatalog,
    _RelationBuildContext,
    _RootSchemaCatalog,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _catalog_json_value,
    _catalog_text,
    _normalized_json,
    _snapshot_manifest_suffixes,
    _walk_manifest_identities,
)
from process.ptg_parts.ptg2_legacy_orphan_store_mutation import (
    LegacySweepAuditRecord,
    delete_legacy_snapshot_metadata,
    insert_legacy_sweep_audit,
    load_legacy_sweep_audit,
    lock_legacy_root_relations,
)


SUFFIX = "1" * 32
OTHER_SUFFIX = "2" * 32


def _relation(
    *,
    prefix: str = "ptg_file",
    suffix: str = SUFFIX,
    relation_oid: int = 11,
    total_bytes: int = 100,
    dependent_oids: tuple[int, ...] = (111,),
    dependent_names: tuple[str, ...] = ("ptg_file_idx",),
) -> LegacyRootRelation:
    return LegacyRootRelation(
        table_name=f"{prefix}_{suffix}",
        relation_oid=relation_oid,
        namespace_oid=7,
        owner_oid=8,
        relkind="r",
        persistence="p",
        total_bytes=total_bytes,
        schema_digest="a" * 64,
        dependent_relation_oids=dependent_oids,
        dependent_relation_names=dependent_names,
        has_rows=False,
    )


@pytest.mark.parametrize(
    ("replacement", "message"),
    (
        ({"table_name": "not_legacy"}, "allowlist"),
        ({"total_bytes": -1}, "bytes"),
        (
            {
                "dependent_relation_oids": (111, 111),
                "dependent_relation_names": ("first", "second"),
            },
            "duplicated",
        ),
        (
            {
                "dependent_relation_oids": (111,),
                "dependent_relation_names": (),
            },
            "incomplete",
        ),
    ),
)
def test_relation_validation_rejects_unsafe_catalog_shapes(
    replacement: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        replace(_relation(), **replacement).validate(
            expected_namespace_oid=7,
            expected_owner_oid=8,
        )


def test_contract_rejects_duplicate_oids_and_unknown_emptiness() -> None:
    with pytest.raises(ValueError, match="root OID is duplicated"):
        classify_legacy_suffix(
            SUFFIX,
            (
                _relation(),
                _relation(prefix="ptg_billing_code"),
            ),
            LegacySuffixOwnership(),
        )
    unproved = classify_legacy_suffix(
        SUFFIX,
        (replace(_relation(), has_rows=None),),
        LegacySuffixOwnership(),
    )
    assert unproved.reasons == ("empty_orphan_proof_missing",)


def test_aggregate_limits_skip_nonfitting_candidate_without_starvation() -> None:
    first = classify_legacy_suffix(
        SUFFIX,
        (_relation(),),
        LegacySuffixOwnership(),
    )
    second = classify_legacy_suffix(
        OTHER_SUFFIX,
        (_relation(suffix=OTHER_SUFFIX, relation_oid=22),),
        LegacySuffixOwnership(),
    )
    assert isinstance(first, LegacySweepCandidate)
    assert isinstance(second, LegacySweepCandidate)

    plan = build_bounded_legacy_sweep_plan(
        schema_name="mrf",
        control_schema_name="control_plane",
        authority_digest="b" * 64,
        catalog_digest="c" * 64,
        eligible_candidates=(first, second),
        blocked=(),
        limits=LegacySweepLimits(2, 1, 4, 1_000),
    )

    assert plan.candidates == (first,)
    assert plan.remaining_eligible_suffix_count == 1


def test_digest_and_limit_inputs_fail_closed() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        LegacySweepLimits(-1, 1, 1, 1).validate()
    with pytest.raises(ValueError, match="catalog digest"):
        build_bounded_legacy_sweep_plan(
            schema_name="mrf",
            control_schema_name="control_plane",
            authority_digest="invalid",
            catalog_digest="c" * 64,
            eligible_candidates=(),
            blocked=(),
            limits=LegacySweepLimits(1, 1, 1, 1),
        )
    with pytest.raises(ValueError, match="plan digest"):
        legacy_sweep_audit_id("invalid")


def test_manifest_helpers_preserve_only_explicit_identifiers() -> None:
    assert _catalog_text(b"r") == "r"
    assert _normalized_json('{"root": ["value"]}') == {"root": ["value"]}
    assert _normalized_json("{") == {}
    assert _normalized_json(7) == {}
    assert _catalog_json_value({b"bytes": (b"\x01",)}) == {
        "b'bytes'": [{"bytea_hex": "01"}]
    }
    assert list(
        _walk_manifest_identities(
            {"nested": [{"value": "one"}], "scalar": "two"}
        )
    ) == [("value", "one"), ("scalar", "two")]
    assert list(_walk_manifest_identities("three")) == [(None, "three")]
    assert _snapshot_manifest_suffixes(
        {"table_name": f"ptg_file_{SUFFIX}"},
        {},
    ) == (SUFFIX,)


class _AllExecutor:
    def __init__(self, result_sets: list[list[dict[str, object]]]) -> None:
        self.result_sets = list(result_sets)
        self.statements: list[str] = []

    async def all(self, statement: str, **_parameters):
        self.statements.append(statement)
        return self.result_sets.pop(0) if self.result_sets else []


@pytest.mark.asyncio
async def test_catalog_empty_and_incomplete_probe_boundaries() -> None:
    empty_schema = await catalog_store._root_schema_catalog(object(), [])
    assert empty_schema == _RootSchemaCatalog((), (), (), (), (), (), (), ())

    with pytest.raises(RuntimeError, match="empty_probe_incomplete"):
        await catalog_store._probe_relation_rows(
            _AllExecutor([[]]),
            schema_name="mrf",
            table_names=[f"ptg_file_{SUFFIX}"],
        )


def _root_row(**replacements: object) -> dict[str, object]:
    row_by_column: dict[str, object] = {
        "relname": f"ptg_file_{SUFFIX}",
        "relation_oid": 11,
        "namespace_oid": 7,
        "owner_oid": 8,
        "relkind": "r",
        "relpersistence": "p",
        "total_bytes": 100,
    }
    row_by_column.update(replacements)
    return row_by_column


def test_catalog_ambiguity_records_every_dependency_boundary() -> None:
    valid_rows, ambiguity_by_suffix = catalog_store._validated_root_rows(
        [_root_row(owner_oid=9)],
        namespace_oid=7,
        owner_oid=8,
    )
    assert valid_rows == []
    assert ambiguity_by_suffix == {SUFFIX: {"root_relation_catalog_invalid"}}

    dependencies = _DependencyCatalog((), (), frozenset(), frozenset({11}))
    schema = _RootSchemaCatalog(
        (),
        (),
        (),
        (),
        (),
        (),
        (),
        ({"root_oid": 11},),
    )
    other_suffix = "2" * 32
    raw_rows = [
        _root_row(),
        _root_row(
            relname=f"ptg_file_{other_suffix}",
            relation_oid=12,
        ),
        {
            "relname": f"unexpected_{SUFFIX}_idx",
            "relation_oid": 99,
        },
        {
            "relname": f"synthetic_{SUFFIX}_{other_suffix}",
            "relation_oid": 100,
        },
    ]
    ambiguity_by_suffix = {}
    catalog_store._record_catalog_ambiguity(
        raw_relation_rows=raw_rows,
        root_relation_rows=raw_rows[:2],
        schema=schema,
        dependencies=dependencies,
        ambiguity_by_suffix=ambiguity_by_suffix,
    )
    assert ambiguity_by_suffix[SUFFIX] == {
        "dependent_relation_catalog_invalid",
        "external_relation_dependency",
        "unexpected_relation_catalog_entry",
    }
    assert ambiguity_by_suffix[other_suffix] == {
        "unexpected_relation_catalog_entry",
    }


def test_catalog_builder_marks_validation_and_inheritance_drift() -> None:
    context = _RelationBuildContext(
        namespace_oid=7,
        owner_oid=8,
        should_probe_rows=False,
        row_presence_by_table={},
        schema=_RootSchemaCatalog(
            (),
            (),
            (),
            (),
            ({"child_oid": 11, "parent_oid": 12},),
            (),
            (),
            (),
        ),
        dependencies=_DependencyCatalog((), (), frozenset(), frozenset()),
    )
    ambiguity_by_suffix: dict[str, set[str]] = {}

    relations = catalog_store._build_relation_contracts(
        [_root_row(namespace_oid=9)],
        context,
        ambiguity_by_suffix,
    )

    assert tuple(relations) == (SUFFIX,)
    assert ambiguity_by_suffix[SUFFIX] == {
        "root_relation_catalog_invalid",
        "root_relation_inheritance_present",
    }


class _MutationExecutor:
    def __init__(
        self,
        *,
        first_result: object | None = None,
        status_result: int = 1,
    ) -> None:
        self.first_result = first_result
        self.status_result = status_result
        self.statements: list[str] = []

    async def first(self, _statement: str, **_parameters):
        return self.first_result

    async def status(self, statement: str, **_parameters):
        self.statements.append(statement)
        return self.status_result


def _audit_record() -> LegacySweepAuditRecord:
    return LegacySweepAuditRecord(
        audit_id="a" * 64,
        actor="test",
        plan_digest="b" * 64,
        authority_digest="c" * 64,
        catalog_digest="d" * 64,
        candidate_suffix_count=1,
        root_table_count=1,
        dependent_relation_count=0,
        snapshot_count=0,
        nonempty_table_count=0,
        total_bytes=0,
        root_relation_oids=[11],
        snapshot_ids=[],
        proof={"contract": "ptg2_legacy_orphan_sweep_v1"},
    )


@pytest.mark.asyncio
async def test_mutations_fail_closed_on_empty_or_conflicting_state() -> None:
    empty_executor = _MutationExecutor()
    await lock_legacy_root_relations(
        empty_executor,
        schema_name="mrf",
        relations=(),
    )
    assert empty_executor.statements == []
    assert await load_legacy_sweep_audit(
        empty_executor,
        schema_name="mrf",
        plan_digest="a" * 64,
    ) is None

    row_executor = _MutationExecutor(first_result=SimpleNamespace(_mapping={"x": 1}))
    assert await load_legacy_sweep_audit(
        row_executor,
        schema_name="mrf",
        plan_digest="a" * 64,
    ) == {"x": 1}

    with pytest.raises(RuntimeError, match="audit_insert_conflict"):
        await insert_legacy_sweep_audit(
            _MutationExecutor(status_result=0),
            schema_name="mrf",
            audit=_audit_record(),
        )


@pytest.mark.asyncio
async def test_metadata_delete_handles_ownerless_and_count_drift() -> None:
    ownerless_executor = _MutationExecutor()
    await delete_legacy_snapshot_metadata(
        ownerless_executor,
        schema_name="mrf",
        snapshot_ids=[],
        internal_run_ids=[],
    )
    assert len(ownerless_executor.statements) == 2

    with pytest.raises(RuntimeError, match="delete_count_mismatch"):
        await delete_legacy_snapshot_metadata(
            _MutationExecutor(status_result=0),
            schema_name="mrf",
            snapshot_ids=["snapshot"],
            internal_run_ids=[],
        )
