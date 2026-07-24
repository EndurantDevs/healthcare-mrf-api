from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from typing import Any, Iterable

from process.ptg_parts.ptg2_shared_reuse import (
    SharedPhysicalArtifactIdentity,
    SharedSnapshotSourceAssignment,
)


class QueryResult:
    """Small SQLAlchemy-result substitute for publication control tests."""

    def __init__(self, rows: Iterable[Any] = (), *, scalar: Any = None) -> None:
        self.rows = list(rows)
        self.scalar_value = scalar

    def __iter__(self):
        return iter(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def one_or_none(self):
        if len(self.rows) > 1:
            raise AssertionError("expected at most one row")
        return self.first()

    def scalar_one(self):
        return self.scalar_value


class ScriptedSession:
    """Return queued results while preserving SQL and parameter evidence."""

    def __init__(self, *results: QueryResult) -> None:
        self.results = list(results)
        self.calls: list[tuple[str, Any]] = []

    async def execute(self, statement: Any, parameters: Any = None) -> QueryResult:
        self.calls.append((str(statement), parameters))
        if not self.results:
            raise AssertionError(f"unexpected SQL: {statement}")
        return self.results.pop(0)


class SourcePublicationSession:
    """Model the insert-only source, plan, and source-set persistence protocol."""

    def __init__(
        self,
        *,
        scope: dict[str, Any],
        plans: Iterable[dict[str, Any]],
        sources: Iterable[dict[str, Any]],
        persisted_source_set: dict[str, Any] | None = None,
    ) -> None:
        self.scope = scope
        self.plans = list(plans)
        self.sources = list(sources)
        self.persisted_source_set = persisted_source_set
        self.calls: list[tuple[str, Any]] = []

    async def execute(self, statement: Any, parameters: Any = None) -> QueryResult:
        sql = str(statement)
        self.calls.append((sql, parameters))
        if "SELECT plan_id, plan_market_type, coverage_scope_id" in sql:
            return QueryResult([self.scope])
        if "SELECT plan_id, plan_market_type" in sql:
            return QueryResult(self.plans)
        if "SELECT snapshot_id, source_key" in sql:
            return QueryResult(self.sources)
        if "WITH expected_source_set AS" in sql:
            expected_source_set_by_field = {
                "contract": parameters["source_set_contract"],
                "source_count": parameters["source_set_count"],
                "raw_container_sha256_digest": parameters["source_set_digest"],
            }
            persisted_source_set_by_field = (
                self.persisted_source_set or expected_source_set_by_field
            )
            return QueryResult(
                [{"snapshot_source_set": persisted_source_set_by_field}]
            )
        if self._is_expected_insert(sql):
            return QueryResult()
        raise AssertionError(f"unexpected SQL: {sql}")

    @staticmethod
    def _is_expected_insert(sql: str) -> bool:
        expected_tables = (
            "ptg2_v3_snapshot_scope",
            "ptg2_v3_snapshot_plan_scope",
            "ptg2_v3_snapshot_source",
        )
        return "INSERT INTO" in sql and any(
            table_name in sql for table_name in expected_tables
        )


class TransactionDatabase:
    """Expose a deterministic transaction around one supplied session."""

    def __init__(self, session: Any) -> None:
        self.session = session

    @asynccontextmanager
    async def transaction(self):
        yield self.session

    @staticmethod
    def text(statement: str) -> str:
        return statement


class Operations:
    """Capture migration DDL and optionally expose a synthetic bind."""

    def __init__(self, bind: Any = None, *, expose_bind: bool = True) -> None:
        self.bind = bind
        self.expose_bind = expose_bind
        self.statements: list[str] = []

    def get_bind(self):
        if not self.expose_bind:
            raise AssertionError("bind lookup was not expected")
        return self.bind

    def execute(self, statement: Any) -> None:
        self.statements.append(str(statement))


def assignment(
    source_key: int = 0,
    *,
    raw_hash: str = "b" * 64,
    logical_hash: str | None = "a" * 64,
    deferred: bool = False,
) -> SharedSnapshotSourceAssignment:
    """Return one valid deterministic physical source assignment."""

    identity = SharedPhysicalArtifactIdentity(
        "in_network",
        "logical_json_sha256_v1",
        "a" * 64,
    )
    return SharedSnapshotSourceAssignment(
        source_key=source_key,
        identity=identity,
        source_trace_set_hash="c" * 64,
        source_trace_hashes=("d" * 64,),
        raw_container_sha256=raw_hash,
        logical_json_sha256=logical_hash,
        logical_hash_deferred=deferred,
    )


def source_row(
    snapshot_id: str,
    source_assignment: SharedSnapshotSourceAssignment,
) -> dict[str, Any]:
    """Return the persisted row expected for one assignment."""

    return {
        "snapshot_id": snapshot_id,
        "source_key": source_assignment.source_key,
        **source_assignment.identity.as_dict(),
        "raw_container_sha256": source_assignment.raw_container_sha256,
        "logical_json_sha256": source_assignment.logical_json_sha256,
        "logical_hash_deferred": source_assignment.logical_hash_deferred,
        "source_trace_set_hash": source_assignment.source_trace_set_hash,
    }


def active_stale_context(**overrides: Any):
    """Build a stale, metadata-only context for fail-closed plan tests."""

    from process.ptg_parts.ptg2_v4_stale_metadata_types import (
        PTG2V4StaleMetadataContext,
    )

    observed_at = dt.datetime(2026, 7, 24, 12, 0)
    context_arguments_by_name = {
        "observed_at": observed_at,
        "snapshot_by_field": {
            "snapshot_id": "snapshot",
            "import_run_id": "run",
            "status": "building",
            "created_at": observed_at - dt.timedelta(hours=8),
            "validated_at": None,
            "published_at": None,
            "manifest": {},
        },
        "internal_run_by_field": {
            "import_run_id": "run",
            "status": "running",
            "started_at": observed_at - dt.timedelta(hours=8),
            "heartbeat_at": observed_at - dt.timedelta(hours=7),
            "finished_at": None,
            "error": None,
            "options": {
                "storage_generation": "shared_blocks_v4",
                "source_key": "source",
            },
            "report": {},
        },
        "attempt_fence_by_field": {
            "snapshot_id": "snapshot",
            "internal_run_id": "run",
            "state": "active",
            "target_digest": None,
            "plan_digest": None,
            "marker_digest": None,
            "marker": None,
            "reconciled_at": None,
        },
        "attachment_count_by_name": {},
    }
    context_arguments_by_name.update(overrides)
    return PTG2V4StaleMetadataContext(**context_arguments_by_name)


def prepared_layout_arguments(**overrides: Any) -> dict[str, Any]:
    """Return the minimum prepared-layout arguments needed by early guards."""

    layout_arguments_by_name = {
        "schema_name": "mrf",
        "manifest_stage_table": "manifest_stage",
        "reserved_snapshot_key": 7,
        "build_token": "token",
        "expected_coverage_scope_id": b"c" * 32,
        "logical_snapshot_id": "snapshot",
        "expected_source_identities": (),
        "serving_run_entries": (),
        "code_dictionary_entries": (),
        "provider_set_metadata_entries": (),
        "source_audit_witness_entries": (),
        "expected_raw_source_sha256": (),
        "graph_artifact_entries": (),
        "provider_identifier_quarantine": {},
        "prepared_price": object(),
        "publication_started_at": 0.0,
        "price_prepare_seconds": 0.0,
    }
    layout_arguments_by_name.update(overrides)
    return layout_arguments_by_name


__all__ = [
    "Operations",
    "QueryResult",
    "ScriptedSession",
    "SourcePublicationSession",
    "TransactionDatabase",
    "active_stale_context",
    "assignment",
    "prepared_layout_arguments",
    "source_row",
]
