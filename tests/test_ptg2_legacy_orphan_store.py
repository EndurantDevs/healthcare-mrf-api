# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_legacy_orphan_store_ownership as ownership_store
from process.ptg_parts import ptg2_legacy_orphan_store_catalog as catalog_store
from process.ptg_parts.ptg2_legacy_orphan_contract import (
    canonical_sha256,
    legacy_sweep_audit_id,
)
from process.ptg_parts.ptg2_legacy_orphan_store import (
    _MRF_REQUIRED_TABLES,
    _bare_control_suffix,
    _internal_run_suffix,
    _snapshot_manifest_suffixes,
    require_legacy_sweep_schema,
    verify_applied_audit_state,
)
from process.ptg_parts.ptg2_legacy_orphan_store_common import (
    _OwnershipAccumulator,
)
from process.ptg_parts.ptg2_legacy_orphan_store_ownership import (
    _internal_run_ownership,
    _snapshot_ownership,
)
from process.ptg_parts.ptg2_legacy_orphan_store_references import (
    _attach_raw_snapshot_owner_conflicts,
)


SUFFIX = "a" * 32
OTHER_ID = "b" * 32


def test_authority_inventory_covers_frozen_v4_attachment_surface() -> None:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260724100000_ptg2_v4_attempt_fence.py"
    )
    spec = importlib.util.spec_from_file_location(
        "legacy_sweep_attachment_contract",
        migration_path,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    assert {
        attachment.table_name
        for attachment in migration.ATTEMPT_ATTACHMENTS
    }.issubset(_MRF_REQUIRED_TABLES)


def test_manifest_owner_ignores_unrelated_32_hex_values() -> None:
    manifest_by_field = {
        "source_file_id": OTHER_ID,
        "nested": {
            "table": f"ptg_file_{SUFFIX}",
        },
    }

    assert _snapshot_manifest_suffixes(
        manifest_by_field,
        {f"ptg_file_{SUFFIX}": SUFFIX},
    ) == (SUFFIX,)


def test_manifest_owner_accepts_only_explicit_suffix_field() -> None:
    manifest_by_field = {
        "source_file_id": OTHER_ID,
        "legacy_table_suffix": SUFFIX,
    }

    assert _snapshot_manifest_suffixes(manifest_by_field, {}) == (SUFFIX,)


def test_lifecycle_identities_are_context_specific() -> None:
    assert _bare_control_suffix(SUFFIX) == SUFFIX
    assert _bare_control_suffix(f"ptg2:{SUFFIX}") is None
    assert _internal_run_suffix(f"ptg2:{SUFFIX}") == SUFFIX
    assert _internal_run_suffix(SUFFIX) is None
    assert _internal_run_suffix(f"prefix:{SUFFIX}") is None
    assert _internal_run_suffix(f"ptg2:{SUFFIX}:{OTHER_ID}") is None


class _SchemaExecutor:
    def __init__(self, rows: list[dict[str, str]]) -> None:
        self.rows = rows

    async def all(self, _statement: str, **_parameters):
        return self.rows


@pytest.mark.asyncio
async def test_required_schema_fails_closed_with_exact_missing_relation() -> None:
    executor = _SchemaExecutor([])

    with pytest.raises(
        RuntimeError,
        match=(
            "legacy_sweep_required_relations_missing:"
            "control_plane.hp_plan_release_binding"
        ),
    ):
        await require_legacy_sweep_schema(
            executor,
            schema_name="mrf",
            control_schema_name="control_plane",
        )


class _ReplayExecutor:
    def __init__(self) -> None:
        self.snapshot_parameters: list[list[str]] = []

    async def scalar(self, statement: str, **parameters):
        if 'FROM "mrf"."ptg2_snapshot"' in statement:
            self.snapshot_parameters.append(parameters["snapshot_ids"])
        return 0


@pytest.mark.asyncio
async def test_replay_verification_extracts_snapshot_identity_pairs() -> None:
    executor = _ReplayExecutor()
    proof_by_field = {
        "contract": "ptg2_legacy_orphan_sweep_v1",
        "schema_name": "mrf",
        "control_schema_name": "control_plane",
        "authority_digest": "b" * 64,
        "catalog_digest": "c" * 64,
        "candidates": [
            {
                "suffix": SUFFIX,
                "relations": [
                    {
                        "table_name": f"ptg_file_{SUFFIX}",
                        "relation_oid": 101,
                        "dependent_relation_oids": [],
                        "total_bytes": 4096,
                        "has_rows": False,
                    }
                ],
                "ownership": {
                    "snapshot_statuses": [
                        ["snapshot-a", "failed"],
                        ["snapshot-b", "published"],
                    ],
                    "internal_run_statuses": [
                        [f"ptg2:{SUFFIX}", "failed"]
                    ],
                },
            }
        ]
    }
    plan_digest = canonical_sha256(proof_by_field)

    counts = await verify_applied_audit_state(
        executor,
        schema_name="mrf",
        control_schema_name="control_plane",
        expected_plan_digest=plan_digest,
        audit_row={
            "audit_id": legacy_sweep_audit_id(plan_digest),
            "contract": "ptg2_legacy_orphan_sweep_v1",
            "plan_digest": bytes.fromhex(plan_digest),
            "authority_digest": bytes.fromhex("b" * 64),
            "catalog_digest": bytes.fromhex("c" * 64),
            "candidate_suffix_count": 1,
            "root_table_count": 1,
            "dependent_relation_count": 0,
            "snapshot_count": 2,
            "nonempty_table_count": 0,
            "total_bytes": 4096,
            "root_relation_oids": [101],
            "snapshot_ids": ["snapshot-a", "snapshot-b"],
            "proof": proof_by_field,
        },
    )

    assert executor.snapshot_parameters == [["snapshot-a", "snapshot-b"]]
    assert counts["total_bytes"] == 4096


class _InternalRunExecutor:
    def __init__(self) -> None:
        self.call_count = 0

    async def all(self, _statement: str, **_parameters):
        self.call_count += 1
        if self.call_count == 1:
            return [{"import_run_id": f"ptg2:{SUFFIX}", "status": "failed"}]
        return [
            {
                "import_job_id": "job-active",
                "import_run_id": f"ptg2:{SUFFIX}",
                "status": "running",
            }
        ]


@pytest.mark.asyncio
async def test_active_import_job_blocks_the_owned_suffix() -> None:
    accumulator = _OwnershipAccumulator()

    await _internal_run_ownership(
        _InternalRunExecutor(),
        "mrf",
        {SUFFIX: accumulator},
    )

    assert accumulator.ambiguity_reasons == {"active_import_job_running"}


class _SnapshotOwnerExecutor:
    async def all(self, statement: str, **_parameters):
        assert "WHERE import_run_id = ANY" in statement
        assert "manifest::text LIKE ANY" in statement
        return [
            {
                "snapshot_id": "snapshot-mismatch",
                "import_run_id": f"ptg2:{OTHER_ID}",
                "status": "failed",
                "manifest": {"legacy_table_suffix": SUFFIX},
            }
        ]


@pytest.mark.asyncio
async def test_snapshot_owner_keeps_foreign_raw_identity_before_filtering() -> None:
    accumulator = _OwnershipAccumulator()

    raw_suffixes_by_snapshot = await _snapshot_ownership(
        _SnapshotOwnerExecutor(),
        "mrf",
        {f"ptg_file_{SUFFIX}": SUFFIX},
        {SUFFIX: accumulator},
    )

    assert raw_suffixes_by_snapshot == {
        "snapshot-mismatch": {SUFFIX, OTHER_ID}
    }
    assert "snapshot_owner_suffix_conflict" in accumulator.ambiguity_reasons


def test_declared_snapshot_must_match_raw_snapshot_owner() -> None:
    accumulator = _OwnershipAccumulator()
    accumulator.declared_snapshot_ids.add("snapshot-foreign")

    _attach_raw_snapshot_owner_conflicts(
        {SUFFIX: accumulator},
        {"snapshot-foreign": {OTHER_ID}},
    )

    assert accumulator.ambiguity_reasons == {
        "declared_snapshot_raw_owner_conflict"
    }


class _UnboundedOwnerExecutor:
    def __init__(self) -> None:
        self.statements: list[str] = []

    async def all(self, statement: str, **_parameters):
        self.statements.append(statement)
        return [
            {"import_run_id": f"ptg2:{SUFFIX}", "status": "failed"},
            {"import_run_id": f"ptg2:{SUFFIX}", "status": "failed"},
        ]


@pytest.mark.asyncio
async def test_candidate_filtered_owner_scan_fails_at_hard_ceiling(
    monkeypatch,
) -> None:
    executor = _UnboundedOwnerExecutor()
    monkeypatch.setattr(
        ownership_store,
        "LEGACY_SWEEP_MAX_OWNERSHIP_ROWS",
        1,
    )

    with pytest.raises(
        RuntimeError,
        match="legacy_sweep_internal_run_scan_limit_exceeded",
    ):
        await _internal_run_ownership(
            executor,
            "mrf",
            {SUFFIX: _OwnershipAccumulator()},
        )

    assert "WHERE import_run_id = ANY" in executor.statements[0]


@pytest.mark.asyncio
async def test_root_discovery_ceiling_fails_before_row_probes(
    monkeypatch,
) -> None:
    async def base_identity(*_arguments, **_parameters):
        return 7, 8

    async def relation_rows(*_arguments, **_parameters):
        return [
            {"relname": f"ptg_file_{SUFFIX}"},
            {"relname": f"ptg_file_{OTHER_ID}"},
        ]

    async def unexpected_probe(*_arguments, **_parameters):
        raise AssertionError("row probes must not run after discovery overflow")

    monkeypatch.setattr(catalog_store, "LEGACY_SWEEP_MAX_TABLES", 1)
    monkeypatch.setattr(catalog_store, "_base_catalog_identity", base_identity)
    monkeypatch.setattr(catalog_store, "_relation_catalog_rows", relation_rows)
    monkeypatch.setattr(
        catalog_store,
        "_relation_build_context",
        unexpected_probe,
    )

    with pytest.raises(
        RuntimeError,
        match="legacy_sweep_root_catalog_limit_exceeded",
    ):
        await catalog_store.load_legacy_relation_catalog(
            object(),
            schema_name="mrf",
            probe_rows=True,
        )
