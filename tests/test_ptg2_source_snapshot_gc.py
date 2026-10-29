# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import sqlite3
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_source_snapshot_gc as snapshot_gc
from process.ptg_parts.source_snapshot_references import source_binding_schema


def _strict_index(**values):
    return {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        **values,
    }


def _strict_v4_index(**values):
    return {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v4",
        **values,
    }


def _allowed_manifest(source_key, previous_snapshot_id=None):
    return {
        "source_key": source_key,
        "allowed_amount_index": {
            "contract": snapshot_gc.PTG2_ALLOWED_AMOUNT_CONTRACT,
            "arch_version": "postgres_binary_v3",
            "storage": "postgresql",
            "snapshot_scoped": True,
            "current_source_key": f"{source_key}_allowed_amounts",
            "previous_snapshot_id": previous_snapshot_id,
        },
    }


class _Executor:
    def __init__(self, snapshot_rows, current_snapshot_ids=()):
        self.snapshot_rows = snapshot_rows
        self.current_snapshot_ids = current_snapshot_ids
        self.status_calls = []
        self.all_calls = []
        self.present_tables = set(snapshot_gc.PTG2_V3_MIGRATION_OWNED_TABLE_NAMES)

    async def all(self, statement, **params):
        self.all_calls.append((statement, params))
        if "AS bindings_exist" in statement:
            return [{"bindings_exist": False, "source_header_exists": False, "header_exists": False}]
        if "RETURNING snapshot_id" in statement:
            return [{"snapshot_id": value} for value in params["snapshot_ids"]]
        if "FROM information_schema.tables" in statement:
            return [{"table_name": table_name} for table_name in sorted(self.present_tables)]
        if "SELECT DISTINCT snapshot_id" in statement:
            return [{"snapshot_id": value} for value in self.current_snapshot_ids]
        if "SELECT DISTINCT snapshot_key" in statement:
            return [{"snapshot_key": 10}]
        if 'FROM "mrf".ptg2_snapshot' in statement:
            return self.snapshot_rows
        if "WITH eligible_layouts AS MATERIALIZED" in statement:
            return [
                {
                    "logical_layout_count": 0,
                    "candidate_hash_count": 0,
                    "stored_bytes": 0,
                }
            ]
        raise AssertionError(statement)

    async def status(self, statement, **params):
        self.status_calls.append((statement, params))
        return 1


class _Acquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _DB:
    def __init__(self, connection):
        self.connection = connection

    def acquire(self):
        return _Acquire(self.connection)


def test_gc_plan_rejects_non_v3_manifests_without_table_discovery():
    executor = _Executor(
        [
            {
                "snapshot_id": "legacy",
                "status": "failed",
                "source_key": "source_a",
                "serving_index": {"storage": "manifest_snapshot", "table": "ptg2_serving_old"},
            },
        ]
    )

    plan = asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor))

    assert plan.candidate_snapshot_ids == ()
    assert plan.tables == ()


def test_gc_plan_selects_only_unreferenced_strict_v3_snapshots():
    executor = _Executor(
        [
            {
                "snapshot_id": "current",
                "status": "published",
                "source_key": "source_a",
                "serving_index": _strict_index(),
            },
            {
                "snapshot_id": "failed",
                "status": "failed",
                "source_key": "source_a",
                "serving_index": _strict_index(),
            },
            {
                "snapshot_id": "stale",
                "status": "building",
                "stale_building": True,
                "source_key": "source_b",
                "serving_index": _strict_index(),
            },
        ],
        current_snapshot_ids=("current",),
    )

    plan = asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor))

    assert plan.candidate_snapshot_ids == ("failed", "stale")
    assert plan.shared_snapshot_ids == ("failed", "stale")
    assert plan.tables == ()


def test_gc_plan_additively_selects_unreferenced_strict_v4_snapshots():
    executor = _Executor(
        [
            {
                "snapshot_id": "v4-current",
                "status": "published",
                "source_key": "source_a",
                "serving_index": _strict_v4_index(),
            },
            {
                "snapshot_id": "v4-failed",
                "status": "failed",
                "source_key": "source_a",
                "serving_index": _strict_v4_index(),
            },
        ],
        current_snapshot_ids=("v4-current",),
    )

    plan = asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor))

    assert plan.candidate_snapshot_ids == ("v4-failed",)
    assert plan.shared_snapshot_ids == ("v4-failed",)
    assert plan.tables == ()


def test_gc_protected_set_unions_pins_and_direct_release_bindings():
    executor = _Executor(
        [
            {
                "snapshot_id": "release-pinned",
                "status": "published",
                "source_key": "source_a",
                "serving_index": _strict_index(),
            },
            {
                "snapshot_id": "unreferenced",
                "status": "published",
                "source_key": "source_a",
                "serving_index": _strict_index(),
            },
        ],
        current_snapshot_ids=("release-pinned",),
    )

    plan = asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor))

    pointer_sql = next(
        statement for statement, _params in executor.all_calls if "SELECT DISTINCT snapshot_id" in statement
    )
    assert "ptg2_snapshot_pin" in pointer_sql
    assert "plan_release_snapshot_binding" in pointer_sql
    assert plan.current_snapshot_ids == ("release-pinned",)
    assert plan.candidate_snapshot_ids == ("unreferenced",)


def test_gc_plan_tracks_allowed_current_replacement_and_collects_older_snapshot():
    executor = _Executor(
        [
            {
                "snapshot_id": "allowed-current",
                "status": "published",
                "previous_snapshot_id": "allowed-previous",
                "manifest": _allowed_manifest(
                    "source_a",
                    "allowed-previous",
                ),
            },
            {
                "snapshot_id": "allowed-previous",
                "status": "published",
                "previous_snapshot_id": "allowed-old",
                "manifest": _allowed_manifest(
                    "source_a",
                    "allowed-old",
                ),
            },
            {
                "snapshot_id": "allowed-old",
                "status": "published",
                "previous_snapshot_id": None,
                "manifest": _allowed_manifest("source_a"),
            },
        ],
        current_snapshot_ids=("allowed-current", "allowed-previous"),
    )

    plan = asyncio.run(
        snapshot_gc.build_ptg2_source_snapshot_gc_plan(
            executor=executor,
            retain_current_lineage=1,
        )
    )

    assert plan.current_snapshot_ids == (
        "allowed-current",
        "allowed-previous",
    )
    assert plan.candidate_snapshot_ids == ("allowed-old",)
    assert plan.candidate_reasons == (("allowed-old", "terminal"),)
    assert plan.shared_snapshot_ids == ()
    assert plan.tables == ()


def test_gc_plan_never_age_deletes_validated_candidate_without_attestation():
    executor = _Executor(
        [
            {
                "snapshot_id": "validated-awaiting-audit",
                "status": "validated",
                # Exercise the Python admission guard independently of the SQL
                # status predicate used to compute stale_building.
                "stale_building": True,
                "source_key": "source-a",
                "serving_index": _strict_index(),
            }
        ]
    )

    plan = asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor))

    assert plan.candidate_snapshot_ids == ()
    assert plan.shared_snapshot_ids == ()
    assert plan.total_bytes == 0


def test_gc_stale_sql_compares_utc_naive_timestamps_in_utc():
    executor = _Executor([])

    asyncio.run(snapshot_gc.build_ptg2_source_snapshot_gc_plan(executor=executor))

    snapshot_query = next(
        statement for statement, _params in executor.all_calls if 'FROM "mrf".ptg2_snapshot AS snapshot' in statement
    )
    assert snapshot_query.count("timezone('UTC', transaction_timestamp())") == 2
    assert "'-infinity'::timestamp" in snapshot_query
    assert "candidate_audit_attestation" not in snapshot_query


def test_gc_schema_resolution_matches_alembic_alias_rules(monkeypatch):
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setenv("DB_SCHEMA", "legacy_ptg")
    assert snapshot_gc.resolve_ptg2_schema() == "legacy_ptg"

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_ptg")
    with pytest.raises(RuntimeError, match="must identify the same schema"):
        snapshot_gc.resolve_ptg2_schema()


def test_execute_gc_deletes_v3_metadata_with_strict_sql_admission(monkeypatch):
    connection = _Executor(
        [
            {
                "snapshot_id": "failed",
                "status": "failed",
                "source_key": "source_a",
                "serving_index": _strict_index(),
            },
        ]
    )
    monkeypatch.setattr(snapshot_gc, "db", _DB(connection))

    release = AsyncMock()
    monkeypatch.setattr(
        snapshot_gc,
        "release_unbound_ptg2_shared_layouts",
        release,
    )
    plan = asyncio.run(snapshot_gc.execute_ptg2_source_snapshot_gc_plan())

    assert plan.candidate_snapshot_ids == ("failed",)
    statements = [statement for statement, _params in connection.status_calls]
    assert not any("DROP TABLE IF EXISTS" in statement for statement in statements)
    snapshot_delete, delete_parameters = next(
        (statement, parameters)
        for statement, parameters in connection.all_calls
        if 'DELETE FROM "mrf".ptg2_snapshot' in statement
    )
    assert "arch_version" in snapshot_delete
    assert "storage_generation" in snapshot_delete
    assert delete_parameters["shared_generations"] == [
        "shared_blocks_v3",
        "shared_blocks_v4",
    ]
    release.assert_awaited_once_with(
        schema_name="mrf",
        executor=connection,
        require_shared=True,
        layout_keys=(10,),
    )


@pytest.mark.parametrize("deleted_ids", [(), ("other",), ("selected", "selected")])
def test_gc_requires_exact_snapshot_deletion(deleted_ids):
    connection = _Executor([])
    connection.all = AsyncMock(return_value=[{"snapshot_id": value} for value in deleted_ids])
    with pytest.raises(RuntimeError, match="snapshot deletion set changed"):
        asyncio.run(snapshot_gc._delete_snapshot_gc_metadata(connection, "mrf", ["selected"]))


@pytest.mark.parametrize("header_exists", [False, True])
def test_unclaimed_source_storage_is_optional(header_exists):
    catalog_by_field = {"bindings_exist": False, "source_header_exists": False, "header_exists": header_exists}
    executor = SimpleNamespace(all=AsyncMock(side_effect=[[catalog_by_field], [{"claimed": False}]]))
    assert asyncio.run(source_binding_schema(executor, "mrf")) is None


@pytest.mark.parametrize("source_header_exists", [False, True])
def test_missing_claimed_source_storage_refuses(source_header_exists):
    catalog_by_field = {"bindings_exist": False, "source_header_exists": source_header_exists, "header_exists": True}
    executor = SimpleNamespace(all=AsyncMock(side_effect=[[catalog_by_field], [{"claimed": True}]]))
    with pytest.raises(RuntimeError, match="Source release binding storage is missing"):
        asyncio.run(source_binding_schema(executor, "mrf"))


def test_gc_retains_source_bindings_without_current_filter(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", "source_control")
    executor = SimpleNamespace(
        all=AsyncMock(
            side_effect=[
                [{"bindings_exist": True, "source_header_exists": True, "header_exists": True}],
                [{"snapshot_id": "retained"}],
            ]
        )
    )
    assert asyncio.run(snapshot_gc._load_current_snapshot_ids(executor, "mrf")) == ("retained",)
    statement = executor.all.await_args.args[0]
    assert 'FROM "source_control".hp_plan_release_binding' in statement
    assert "is_current" not in statement and "status" not in statement


@pytest.mark.parametrize("consumer", ["direct", "schema_helper", "legacy_loader", "canonical_loader"])
def test_retained_snapshot_sql_executes_for_every_template_consumer(monkeypatch, consumer):
    table_names = (
        "ptg2_current_snapshot",
        "ptg2_current_source_snapshot",
        "ptg2_current_plan_source",
        "ptg2_snapshot_pin",
        "plan_release_snapshot_pin",
        "plan_release_snapshot_binding",
    )
    has_canonical_bindings = consumer == "canonical_loader"
    monkeypatch.setattr(
        snapshot_gc, "source_binding_schema", AsyncMock(return_value="registry" if has_canonical_bindings else None)
    )
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("ATTACH DATABASE ':memory:' AS mrf")
        for table in table_names:
            connection.execute(f"CREATE TABLE mrf.{table} (snapshot_id TEXT, previous_snapshot_id TEXT)")
            connection.executemany(
                f"INSERT INTO mrf.{table} VALUES (?,?)",
                [(table, "previous"), (table, "previous")],
            )
        connection.executescript("""
            ATTACH DATABASE ':memory:' AS registry;
            CREATE TABLE registry.hp_plan_release_binding (snapshot_id TEXT);
            INSERT INTO registry.hp_plan_release_binding VALUES
                ('source-retained'), ('ptg2_current_snapshot'), (NULL);
        """)

        async def execute(statement):
            return [dict(snapshot_row) for snapshot_row in connection.execute(statement)]

        if consumer == "direct":
            statement = snapshot_gc._CURRENT_SNAPSHOT_IDS_SQL.replace("__SCHEMA__", "mrf")
        elif consumer == "schema_helper":
            statement = snapshot_gc._schema_sql(snapshot_gc._CURRENT_SNAPSHOT_IDS_SQL, "mrf")
        else:
            executor = SimpleNamespace(all=execute)
            retained_snapshot_ids = asyncio.run(snapshot_gc._load_current_snapshot_ids(executor, "mrf"))
        if consumer in {"direct", "schema_helper"}:
            retained_snapshot_ids = tuple(snapshot_row["snapshot_id"] for snapshot_row in connection.execute(statement))

    expected_snapshot_ids = set(table_names) | {"previous"}
    if has_canonical_bindings:
        expected_snapshot_ids.add("source-retained")
    assert retained_snapshot_ids == tuple(sorted(expected_snapshot_ids))
