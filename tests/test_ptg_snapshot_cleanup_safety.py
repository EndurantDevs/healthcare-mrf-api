# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio

from process.ptg_parts import snapshot_cleanup


def _snapshot_row(snapshot_id, status, table_name, previous_snapshot_id=None):
    return {
        "snapshot_id": snapshot_id,
        "previous_snapshot_id": previous_snapshot_id,
        "status": status,
        "manifest": {
            "serving_index": {
                "source_key": "source_a",
                "table": f"mrf.{table_name}",
            }
        },
    }


class _StaticCleanupConnection:
    def __init__(self, current_snapshot_ids, snapshot_rows):
        self.current_snapshot_ids = current_snapshot_ids
        self.snapshot_rows = snapshot_rows
        self.status_calls = []
        self.pointer_sql = ""
        self.manifest_sql = ""

    async def status(self, statement, **params):
        self.status_calls.append((statement, params))
        return 1

    async def all(self, statement, **params):
        if "current_refs" in statement:
            self.pointer_sql = statement
            assert params == {"source_key": "source_a"}
            return [
                {"snapshot_id": snapshot_id}
                for snapshot_id in self.current_snapshot_ids
            ]
        if "ptg2_snapshot" in statement:
            assert "pg_advisory_xact_lock" in self.status_calls[0][0]
            self.manifest_sql = statement
            assert params == {}
            return self.snapshot_rows
        raise AssertionError(statement)


class _AcquireConnection:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _CleanupDB:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _AcquireConnection(self.connection)


def _dropped_table_statements(connection):
    return [statement for statement, _params in connection.status_calls if "DROP TABLE" in statement]


def test_rollback_cleanup_retains_displaced_snapshot(monkeypatch):
    snapshot_rows = [
        _snapshot_row("snap_a", "published", "ptg2_serving_a"),
        _snapshot_row("snap_b", "published", "ptg2_serving_b", "snap_a"),
        _snapshot_row("snap_old", "published", "ptg2_serving_old"),
    ]
    connection = _StaticCleanupConnection(["snap_a", "snap_b"], snapshot_rows)
    monkeypatch.setattr(snapshot_cleanup, "db", _CleanupDB(connection))
    monkeypatch.setenv(snapshot_cleanup.PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV, "1")

    asyncio.run(
        snapshot_cleanup._cleanup_old_ptg2_source_tables(
            "source_a",
            {"snap_a"},
            lock_pointer_state=True,
        )
    )

    assert connection.pointer_sql.count("previous_snapshot_id AS snapshot_id") == 3
    assert _dropped_table_statements(connection) == [
        'DROP TABLE IF EXISTS "mrf"."ptg2_serving_old";'
    ]


def test_locked_cleanup_skips_rebuilding_snapshot(monkeypatch):
    snapshot_rows = [
        _snapshot_row("snap_current", "published", "ptg2_serving_current"),
        _snapshot_row("snap_retry", "building", "ptg2_serving_retry"),
        _snapshot_row("snap_old", "failed", "ptg2_serving_old"),
    ]
    connection = _StaticCleanupConnection(["snap_current"], snapshot_rows)
    monkeypatch.setattr(snapshot_cleanup, "db", _CleanupDB(connection))
    monkeypatch.setenv(snapshot_cleanup.PTG2_SOURCE_SNAPSHOT_RETAIN_LINEAGE_ENV, "1")

    asyncio.run(
        snapshot_cleanup._cleanup_old_ptg2_source_tables(
            "source_a",
            {"snap_current"},
            lock_pointer_state=True,
        )
    )

    assert "status" in connection.manifest_sql
    assert _dropped_table_statements(connection) == [
        'DROP TABLE IF EXISTS "mrf"."ptg2_serving_old";'
    ]


def test_old_v3_graph_contract_cannot_be_repromoted(monkeypatch):
    monkeypatch.setattr(snapshot_cleanup, "_required_snapshot_table_names", lambda _index: set())
    monkeypatch.setattr(snapshot_cleanup, "_snapshot_artifact_references", lambda _index: (set(), set()))

    missing_tables, missing_artifacts = asyncio.run(
        snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "snapshot-old-v3",
            {
                "arch_version": "postgres_binary_v3",
                "provider_membership_graph": {
                    "artifact_version": "provider_membership_graph_v2",
                },
            },
        )
    )

    assert missing_tables == []
    assert missing_artifacts == ["provider_membership_graph_v3"]


def test_current_v3_version_label_without_graph_entries_cannot_be_promoted(monkeypatch):
    monkeypatch.setattr(snapshot_cleanup, "_required_snapshot_table_names", lambda _index: set())
    monkeypatch.setattr(snapshot_cleanup, "_snapshot_artifact_references", lambda _index: (set(), set()))

    missing_tables, missing_artifacts = asyncio.run(
        snapshot_cleanup._missing_snapshot_serving_resources(
            "mrf",
            "snapshot-graphless-v3",
            {
                "arch_version": "postgres_binary_v3",
                "provider_membership_graph": {
                    "artifact_version": "provider_membership_graph_v3",
                    "artifact_names": [
                        "provider_forward",
                        "provider_group_npi",
                        "provider_inverted",
                        "provider_npi_group",
                    ],
                    "storage": "postgresql_chunks_v1",
                },
                "artifacts": {},
            },
        )
    )

    assert missing_tables == []
    assert missing_artifacts == ["provider_membership_graph_v3"]
