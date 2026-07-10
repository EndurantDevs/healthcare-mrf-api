import asyncio

from process.ptg_parts import ptg2_manifest_cleanup as manifest_cleanup
from process.ptg_parts.snapshot_cleanup import _snapshot_manifest_table_names


class _FakeAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeDB:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _FakeAcquire(self.connection)


class _PublishRaceExecutor:
    def __init__(self):
        self.publisher_committed = False
        self.status_calls = []

    async def status(self, statement, **params):
        self.status_calls.append((statement, params))
        if "pg_advisory_xact_lock" in statement:
            self.publisher_committed = True
        return 1

    async def all(self, statement, **_params):
        if "status = 'building'" in statement:
            if not self.publisher_committed:
                return []
            return [
                {
                    "snapshot_id": "snap_published",
                    "status": "published",
                    "manifest": {
                        "serving_index": {
                            "storage": "manifest_snapshot",
                            "table": "mrf.ptg2_serving_abc123def4567890",
                        }
                    },
                }
            ]
        if "FROM pg_class" in statement:
            return [{"table_name": "ptg2_serving_abc123def4567890"}]
        if "status IN ('published', 'failed')" in statement:
            return []
        raise AssertionError(statement)


async def _fake_ensure(_schema_name=None):
    return None


def test_manifest_cleanup_replans_after_current_format_publish(monkeypatch):
    connection = _PublishRaceExecutor()
    dry_run_plan = asyncio.run(
        manifest_cleanup.build_ptg2_manifest_cleanup_plan(executor=connection)
    )
    assert dry_run_plan.tables == ("ptg2_serving_abc123def4567890",)

    monkeypatch.setattr(manifest_cleanup, "db", _FakeDB(connection))
    monkeypatch.setattr(manifest_cleanup, "ensure_ptg2_artifact_blob_table", _fake_ensure)
    executed_plan = asyncio.run(
        manifest_cleanup.execute_ptg2_manifest_cleanup_plan(dry_run_plan)
    )

    assert executed_plan.tables == ()
    statements = [statement for statement, _params in connection.status_calls]
    assert "set_config('lock_timeout'" in statements[0]
    assert "pg_advisory_xact_lock" in statements[1]
    assert "SHARE ROW EXCLUSIVE MODE" in statements[2]
    assert not any("DROP TABLE" in statement for statement in statements)


def test_manifest_cleanup_skips_table_drops_during_active_build():
    class BuildingSnapshotExecutor(_PublishRaceExecutor):
        async def all(self, statement, **params):
            if "status = 'building'" in statement:
                return [
                    {
                        "snapshot_id": "snap_building",
                        "status": "building",
                        "manifest": {},
                    }
                ]
            return await super().all(statement, **params)

    plan = asyncio.run(
        manifest_cleanup.build_ptg2_manifest_cleanup_plan(
            executor=BuildingSnapshotExecutor(),
        )
    )

    assert plan.tables == ()


def test_snapshot_manifest_table_names_allow_price_code_set_family():
    table_names = _snapshot_manifest_table_names(
        {
            "price_code_set_table": "mrf.ptg2_price_code_set_abc123",
        }
    )

    assert table_names == ["ptg2_price_code_set_abc123"]
