# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


partd_network = importlib.import_module("process.partd_formulary_network")


class _MemoryRedis:
    def __init__(self) -> None:
        self.scalar_by_key: dict[str, str] = {}
        self.members_by_key: dict[str, set[object]] = {}
        self.hash_by_key: dict[str, dict[object, object]] = {}
        self.expired_keys: list[str] = []

    async def delete(self, *keys: str) -> None:
        for key in keys:
            self.scalar_by_key.pop(key, None)
            self.members_by_key.pop(key, None)
            self.hash_by_key.pop(key, None)

    async def set(self, key: str, value: str) -> None:
        self.scalar_by_key[key] = value

    async def get(self, key: str) -> str | None:
        return self.scalar_by_key.get(key)

    async def expire(self, key: str, _seconds: int) -> None:
        self.expired_keys.append(key)

    async def sadd(self, key: str, member: object) -> None:
        self.members_by_key.setdefault(key, set()).add(member)

    async def srem(self, key: str, member: object) -> None:
        self.members_by_key.setdefault(key, set()).discard(member)

    async def smembers(self, key: str) -> set[object]:
        return self.members_by_key.get(key, set())

    async def scard(self, key: str) -> int:
        return len(self.members_by_key.get(key, set()))

    async def hset(self, key: str, field: object, value: object) -> None:
        self.hash_by_key.setdefault(key, {})[field] = value

    async def hgetall(self, key: str) -> dict[object, object]:
        return self.hash_by_key.get(key, {})

    async def hdel(self, key: str, field: object) -> None:
        self.hash_by_key.setdefault(key, {}).pop(field, None)

    async def incrby(self, key: str, value: int) -> None:
        self.scalar_by_key[key] = str(int(self.scalar_by_key.get(key, "0")) + value)


def _catalog_with_partd_releases() -> dict[str, object]:
    return {
        "dataset": [
            {
                "title": partd_network.QUARTERLY_DATASET_TITLE,
                "distribution": [
                    {
                        "downloadURL": "https://example.test/quarterly-20260415.zip",
                        "issued": "2026-04-15",
                    },
                    {
                        "downloadURL": "https://example.test/quarterly-20260115.zip",
                        "issued": "2026-01-15",
                    },
                ],
            },
            {
                "title": partd_network.MONTHLY_DATASET_TITLE,
                "distribution": [
                    {
                        "downloadURL": "https://example.test/monthly-20260430.zip",
                        "issued": "2026-04-30",
                    },
                    {
                        "downloadURL": "https://example.test/monthly-20260531.zip",
                        "issued": "2026-05-31",
                    },
                    {
                        "downloadURL": "https://example.test/monthly-20260630.zip",
                        "issued": "2026-06-30",
                    },
                    {
                        "downloadURL": "https://example.test/monthly-20260731.zip",
                        "issued": "2026-07-31",
                    },
                ],
            },
        ]
    }


def test_resolve_artifacts_selects_quarterly_base_and_later_monthlies():
    catalog_by_name = _catalog_with_partd_releases()

    bounded_artifacts = partd_network._resolve_artifacts(catalog_by_name, True)
    complete_artifacts = partd_network._resolve_artifacts(catalog_by_name, False)

    assert [artifact.source_type for artifact in bounded_artifacts] == [
        "quarterly",
        "monthly",
        "monthly",
    ]
    assert [artifact.release_date.isoformat() for artifact in bounded_artifacts] == [
        "2026-04-15",
        "2026-05-31",
        "2026-06-30",
    ]
    assert complete_artifacts[-1].release_date == datetime.date(2026, 7, 31)
    assert all(
        artifact.cutoff_month == datetime.date(2026, 4, 1)
        for artifact in complete_artifacts
    )


def test_resolve_artifacts_rejects_quarterly_dataset_without_zip():
    catalog_by_name = _catalog_with_partd_releases()
    catalog_by_name["dataset"][0]["distribution"] = []

    with pytest.raises(LookupError, match="does not expose ZIP"):
        partd_network._resolve_artifacts(catalog_by_name, False)


def test_explicit_artifacts_accepts_source_url_forms_and_skips_invalid_entries():
    source_url_artifacts = partd_network._explicit_artifacts(
        {"source_urls": " , /tmp/base.zip, /tmp/monthly.zip "}
    )
    mixed_artifacts = partd_network._explicit_artifacts(
        {
            "artifacts": [
                None,
                " ",
                {"source_url": "/tmp/from-source-url.zip"},
                {"file": "/tmp/from-file.zip", "source_type": ""},
                {},
                42,
            ]
        }
    )

    assert [artifact.source_type for artifact in source_url_artifacts] == [
        "quarterly",
        "monthly",
    ]
    assert [artifact.artifact_name for artifact in mixed_artifacts] == [
        "from-source-url.zip",
        "from-file.zip",
    ]
    assert partd_network._explicit_artifacts({"artifacts": ("not", "a", "list")}) == []


def test_small_scalar_and_file_boundaries_cover_fail_closed_paths(tmp_path, monkeypatch):
    pipe_file = tmp_path / "pipe.txt"
    tab_file = tmp_path / "tab.txt"
    pipe_file.write_text("a|b|c\n", encoding="utf-8")
    tab_file.write_text("a\tb\tc\n", encoding="utf-8")
    downloaded_paths: list[tuple[str, str]] = []
    monkeypatch.setenv("PARTD_BOUNDARY_FLAG", "YES")
    monkeypatch.setattr(
        partd_network,
        "download_it_and_save",
        AsyncMock(side_effect=lambda url, path: downloaded_paths.append((url, path))),
    )

    assert partd_network._is_env_enabled("PARTD_BOUNDARY_FLAG") is True
    assert partd_network._to_bool(True) is True
    assert partd_network._to_bool("indeterminate") is None
    assert partd_network._detect_delimiter(pipe_file) == "|"
    assert partd_network._detect_delimiter(tab_file) == "\t"
    assert partd_network._extract_distribution_release_date({}) is None
    with pytest.raises(FileNotFoundError):
        asyncio.run(partd_network._stage_artifact_file(str(tmp_path / "missing.zip"), "unused"))
    asyncio.run(
        partd_network._stage_artifact_file(
            "https://example.test/archive.zip",
            str(tmp_path / "archive.zip"),
        )
    )
    assert downloaded_paths == [
        ("https://example.test/archive.zip", str(tmp_path / "archive.zip"))
    ]


def test_column_default_sql_covers_server_python_and_scalar_defaults():
    server_default = SimpleNamespace(server_default=SimpleNamespace(arg="now()"), default=None)
    callable_default = SimpleNamespace(
        server_default=None,
        default=SimpleNamespace(arg=lambda: "generated"),
    )

    assert partd_network._column_default_sql(server_default) == "now()"
    assert partd_network._column_default_sql(callable_default) is None
    assert partd_network._column_default_sql(
        SimpleNamespace(server_default=None, default=SimpleNamespace(arg="O'Reilly"))
    ) == "'O''Reilly'"
    assert partd_network._column_default_sql(
        SimpleNamespace(server_default=None, default=SimpleNamespace(arg=True))
    ) == "TRUE"
    assert partd_network._column_default_sql(
        SimpleNamespace(server_default=None, default=SimpleNamespace(arg=False))
    ) == "FALSE"
    assert partd_network._column_default_sql(
        SimpleNamespace(server_default=None, default=SimpleNamespace(arg=17))
    ) == "17"
    assert partd_network._column_default_sql(
        SimpleNamespace(server_default=None, default=None)
    ) is None


def test_index_helpers_cover_primary_optional_and_filtered_indexes(monkeypatch):
    status_statements: list[str] = []
    fake_db = SimpleNamespace(status=AsyncMock(side_effect=lambda statement: status_statements.append(statement)))
    indexed_model = SimpleNamespace(
        __tablename__="covered_table",
        __my_index_elements__=("owner_id", "ordinal"),
        __my_additional_indexes__=(
            {},
            {"index_elements": ()},
            {
                "name": "covered_custom_idx",
                "index_elements": ("search_vector",),
                "using": "gin",
                "where": "search_vector IS NOT NULL",
            },
            {"index_elements": ("status",)},
        ),
    )
    plain_model = SimpleNamespace(
        __tablename__="plain_table",
        __my_index_elements__=(),
        __my_additional_indexes__=(),
    )
    monkeypatch.setattr(partd_network, "db", fake_db)

    async def exercise_indexes() -> None:
        await partd_network._ensure_indexes(indexed_model, "mrf")
        await partd_network._ensure_indexes(indexed_model, "mrf", include_additional=False)
        await partd_network._ensure_indexes(plain_model, "mrf")
        await partd_network._drop_additional_indexes(indexed_model, "mrf")

    asyncio.run(exercise_indexes())

    joined_statements = "\n".join(status_statements)
    assert "covered_table_idx_primary" in joined_statements
    assert "USING gin (search_vector) WHERE search_vector IS NOT NULL" in joined_statements
    assert "covered_table_status_idx" in joined_statements
    assert "DROP INDEX IF EXISTS mrf.covered_custom_idx" in joined_statements


def test_column_helpers_add_only_missing_columns_with_supported_constraints(monkeypatch):
    status_statements: list[str] = []
    fake_db = SimpleNamespace(
        all=AsyncMock(return_value=[("existing",)]),
        status=AsyncMock(side_effect=lambda statement: status_statements.append(statement)),
    )
    columns = (
        SimpleNamespace(name="existing", nullable=False, type_sql="TEXT", default_sql=None),
        SimpleNamespace(name="required", nullable=False, type_sql="INTEGER", default_sql="7"),
        SimpleNamespace(name="optional", nullable=True, type_sql="TEXT", default_sql=None),
        SimpleNamespace(name="optional_default", nullable=True, type_sql="BOOLEAN", default_sql="TRUE"),
    )
    model = SimpleNamespace(
        __table__=SimpleNamespace(name="covered_columns", columns=columns)
    )
    monkeypatch.setattr(partd_network, "db", fake_db)
    monkeypatch.setattr(partd_network, "_column_type_sql", lambda column: column.type_sql)
    monkeypatch.setattr(partd_network, "_column_default_sql", lambda column: column.default_sql)

    async def exercise_columns() -> None:
        await partd_network._ensure_columns(model, "mrf")
        await partd_network._drop_columns(model, "mrf", ("old_one", "old_two"))

    asyncio.run(exercise_columns())

    joined_statements = "\n".join(status_statements)
    assert '"required" INTEGER DEFAULT 7 NOT NULL' in joined_statements
    assert '"optional" TEXT' in joined_statements
    assert '"optional_default" BOOLEAN DEFAULT TRUE' in joined_statements
    assert '"existing"' not in joined_statements
    assert joined_statements.count("DROP COLUMN IF EXISTS") == 2


def test_table_maintenance_orchestrates_models_indexes_and_analyze(monkeypatch):
    status_statements: list[str] = []
    created_tables: list[object] = []
    ensured_models: list[object] = []
    index_options: list[tuple[object, bool]] = []
    dropped_columns: list[tuple[object, tuple[str, ...]]] = []
    fake_db = SimpleNamespace(
        status=AsyncMock(side_effect=lambda statement: status_statements.append(statement)),
        create_table=AsyncMock(side_effect=lambda table, **_kwargs: created_tables.append(table)),
    )
    monkeypatch.setattr(partd_network, "db", fake_db)
    monkeypatch.setattr(
        partd_network,
        "_ensure_columns",
        AsyncMock(side_effect=lambda model, _schema: ensured_models.append(model)),
    )
    monkeypatch.setattr(
        partd_network,
        "_ensure_indexes",
        AsyncMock(
            side_effect=lambda model, _schema, *, include_additional: index_options.append(
                (model, include_additional)
            )
        ),
    )
    monkeypatch.setattr(
        partd_network,
        "_drop_columns",
        AsyncMock(side_effect=lambda model, _schema, columns: dropped_columns.append((model, columns))),
    )
    monkeypatch.setattr(partd_network, "PARTD_DEFER_ADDITIONAL_INDEXES", True)

    schema = asyncio.run(partd_network._ensure_tables())
    asyncio.run(partd_network._analyze_partd_tables(schema))

    assert schema == "mrf"
    assert len(created_tables) == len(ensured_models) == 6
    assert sum(not include_additional for _model, include_additional in index_options) == 2
    assert len(dropped_columns) == 4
    assert sum(statement.startswith("ANALYZE mrf.") for statement in status_statements) == 4


def test_secondary_index_orchestrators_visit_both_canonical_models(monkeypatch):
    dropped_models: list[object] = []
    ensured_models: list[object] = []
    monkeypatch.setattr(
        partd_network,
        "_drop_additional_indexes",
        AsyncMock(side_effect=lambda model, _schema: dropped_models.append(model)),
    )
    monkeypatch.setattr(
        partd_network,
        "_ensure_indexes",
        AsyncMock(side_effect=lambda model, _schema, **_kwargs: ensured_models.append(model)),
    )

    asyncio.run(partd_network._drop_partd_secondary_indexes("mrf"))
    asyncio.run(partd_network._ensure_partd_secondary_indexes("mrf"))

    assert dropped_models == ensured_models == [
        partd_network.PartDPharmacyActivity,
        partd_network.PartDMedicationCost,
    ]


def test_activity_chunk_state_round_trips_bounded_progress_and_completion():
    redis = _MemoryRedis()
    run_id = "run-one"
    snapshot_id = "snapshot-one"

    async def exercise_chunk_state():
        await partd_network._init_activity_chunk_state(
            redis,
            run_id,
            snapshot_id,
            -2,
            total_bytes=100,
        )
        await partd_network._mark_activity_chunk_started(
            redis, run_id, snapshot_id, "chunk-a", total_bytes=0
        )
        await partd_network._mark_activity_chunk_started(
            redis, run_id, snapshot_id, "chunk-b", total_bytes=100
        )
        await partd_network._mark_activity_chunk_progress(
            redis,
            run_id,
            snapshot_id,
            "chunk-a",
            processed_bytes=150,
            accepted_rows=-3,
            total_bytes=100,
        )
        await partd_network._mark_activity_chunk_progress(
            redis,
            run_id,
            snapshot_id,
            "chunk-b",
            processed_bytes=-1,
            accepted_rows=4,
        )
        await partd_network._mark_activity_chunk_done(
            redis, run_id, snapshot_id, "chunk-a", 3, total_bytes=100
        )
        await partd_network._mark_activity_chunk_done(
            redis, run_id, snapshot_id, "chunk-b", 0
        )
        done_key = partd_network._state_key(run_id, snapshot_id, "activity_done")
        redis.members_by_key[done_key].add(b"chunk-c")
        return (
            await partd_network._get_activity_chunk_progress(redis, run_id, snapshot_id),
            await partd_network._activity_done_chunk_ids(redis, run_id, snapshot_id),
            await partd_network._activity_completed_rows(redis, run_id, snapshot_id),
        )

    progress, done_chunk_ids, completed_rows = asyncio.run(exercise_chunk_state())

    assert progress.total_chunks == 0
    assert progress.bytes_total == 100
    assert progress.bytes_done == 100
    assert progress.row_count == 3
    assert done_chunk_ids == {"chunk-a", "chunk-b", "chunk-c"}
    assert completed_rows == 3
    assert redis.expired_keys
