# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reviewer-oriented fail-closed coverage for small infrastructure contracts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import mrf_discovery_catalog as discovery_catalog
from db import json_mixin
from process import clinical_reference_artifacts as reference_artifacts
from process import redis_config
from process.ptg_parts import db_tables, rust_stage, snapshot_tables, table_setup


class _UnlinkFailure:
    def unlink(self, *, missing_ok: bool) -> None:
        assert missing_ok is True
        raise OSError("read-only residue")


def test_reference_artifact_cleanup_is_atomic_and_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reference_artifacts._discard_partial_download(_UnlinkFailure())

    artifact_path = tmp_path / "clinical.json"

    def reject_manifest(*_args, **_kwargs) -> None:
        raise RuntimeError("serialization rejected")

    monkeypatch.setattr(reference_artifacts.json, "dump", reject_manifest)
    with pytest.raises(RuntimeError, match="serialization rejected"):
        reference_artifacts._write_manifest_temporary(
            artifact_path,
            {"byte_count": 1},
        )
    assert not list(tmp_path.glob(".clinical.json.manifest.*.tmp"))
    assert reference_artifacts._is_manifest_current(artifact_path) is False

    def reject_temporary_file(**_kwargs):
        raise OSError("temporary directory unavailable")

    monkeypatch.setattr(
        reference_artifacts.tempfile,
        "NamedTemporaryFile",
        reject_temporary_file,
    )
    with pytest.raises(OSError, match="temporary directory unavailable"):
        reference_artifacts._write_manifest_temporary(artifact_path, {})


def test_reference_artifact_recovery_discards_current_and_empty_rollbacks(
    tmp_path: Path,
) -> None:
    artifact_path = tmp_path / "codes.json"
    artifact_path.write_bytes(b"stable")
    reference_artifacts._manifest_path(artifact_path).write_text(
        json.dumps(
            {
                "byte_count": artifact_path.stat().st_size,
                "sha256": reference_artifacts._sha256_file(artifact_path),
            }
        ),
        encoding="utf-8",
    )
    current_rollback = tmp_path / ".codes.json.rollback-existing.deadbeef.tmp"
    current_rollback.write_bytes(b"old")
    reference_artifacts._recover_interrupted_publication(artifact_path)
    assert artifact_path.read_bytes() == b"stable"
    assert not current_rollback.exists()

    reference_artifacts._manifest_path(artifact_path).unlink()
    empty_rollback = tmp_path / ".codes.json.rollback-empty.cafebabe.tmp"
    empty_rollback.write_bytes(b"")
    reference_artifacts._recover_interrupted_publication(artifact_path)
    assert not artifact_path.exists()
    assert not empty_rollback.exists()


class _JSONFixture(json_mixin.JSONOutputMixin):
    EXCLUDE_FIELDS = ("excluded",)
    EXECUTABLE_FIELDS = {"computed": lambda fixture: fixture.raw_value}
    __table__ = SimpleNamespace(
        columns=(
            SimpleNamespace(name="defaulted", default=SimpleNamespace(arg="fallback")),
            SimpleNamespace(name="raw_value", default=None),
            SimpleNamespace(name="excluded", default=None),
        )
    )

    defaulted = None
    raw_value = datetime(2026, 7, 30, tzinfo=timezone.utc)
    excluded = "private"

    def to_dict(self) -> dict[str, object]:
        return {
            "when": datetime(2026, 7, 30, 1, 2, 3),
            "identifier": UUID("12345678-1234-5678-1234-567812345678"),
            "fallback": object(),
        }


def test_json_mixin_maps_defaults_iterables_and_extended_values() -> None:
    fixture = _JSONFixture()
    iterated_by_field = dict(fixture.__iter__())
    assert set(iterated_by_field) == {"when", "identifier", "fallback"}
    assert json_mixin.JSONOutputMixin.is_iterable(iter((1, 2))) is True
    assert json_mixin.JSONOutputMixin.is_iterable(3) is False
    assert json_mixin.JSONOutputMixin.map_anything(
        (value for value in (1, 2)),
        lambda value: value * 2,
    ) == [2, 4]
    identifier = UUID("12345678-1234-5678-1234-567812345678")
    assert json_mixin.JSONOutputMixin.prepare_for_json(identifier) == str(identifier)

    json_ready = fixture.to_json_dict()
    assert json_ready["defaulted"] == "fallback"
    assert json_ready["computed"] == "2026-07-30T00:00:00Z"
    assert "excluded" not in json_ready
    encoded = json.loads(fixture.to_json())
    assert encoded["when"] == "2026-07-30T01:02:03"
    assert encoded["identifier"] == str(identifier)
    assert encoded["fallback"].startswith("<object object at")


@pytest.mark.asyncio
async def test_discovery_catalog_rejects_empty_source_and_normalizes_edges() -> None:
    with pytest.raises(ValueError, match="source_id is required"):
        await discovery_catalog.list_discovery_source_files_page("  ")

    assert discovery_catalog._normalized_plan_info(
        {},
        {"plan_info": [{"plan_name": "No market"}]},
    ) == []
    assert discovery_catalog._text_list(" Plan A ") == ["Plan A"]
    assert discovery_catalog._text_list("  ") == []
    assert discovery_catalog._text_list(object()) == []
    assert discovery_catalog._text_list([" A ", "", 7]) == ["A", "7"]
    assert discovery_catalog._value_at(["only"], 9) == "only"

    mapped_row = SimpleNamespace(_mapping={"source_id": "source-1"})
    assert discovery_catalog._row_mapping(mapped_row) == {"source_id": "source-1"}
    with pytest.raises(TypeError, match="unsupported row type"):
        discovery_catalog._row_mapping(SimpleNamespace(_mapping=[]))


@pytest.mark.asyncio
async def test_rust_stage_creation_cleans_columns_and_tolerates_advisory_ddl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []

    async def status(statement: str) -> None:
        statements.append(statement)
        if "SET UNLOGGED" in statement or "autovacuum_enabled" in statement:
            raise RuntimeError("advisory setting unavailable")

    async def columns(*_args, **_kwargs):
        return [
            {"column_name": "obsolete"},
            SimpleNamespace(column_name="keep"),
            SimpleNamespace(column_name=None),
        ]

    monkeypatch.setattr(rust_stage.db, "status", status)
    monkeypatch.setattr(rust_stage.db, "all", columns)
    monkeypatch.setattr(rust_stage, "_uses_ptg2_stage_copy_dedupe", lambda _kind: True)
    monkeypatch.setattr(rust_stage, "_env_bool", lambda _name, _default: True)

    await rust_stage._create_one_rust_copy_stage_table(
        kind="provider_set",
        schema_name="mrf",
        storage_mode="UNLOGGED ",
        stage_table="ptg2_stage_provider_set",
        target_table="ptg2_provider_set",
        columns=["keep"],
        conflict_targets=["keep"],
    )

    assert any('DROP COLUMN IF EXISTS "obsolete"' in sql for sql in statements)
    assert any("CREATE UNIQUE INDEX IF NOT EXISTS" in sql for sql in statements)


@pytest.mark.asyncio
async def test_rust_stage_builds_worker_lanes_without_optional_ddl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_kinds: list[str] = []

    async def create_stage(**kwargs) -> None:
        created_kinds.append(kwargs["kind"])

    compact_spec = ("ptg2_serving_rate_compact", ["id"], ["id"])
    procedure_spec = ("ptg2_procedure", ["id"], ["id"])
    monkeypatch.setattr(
        rust_stage,
        "_RUST_COPY_TABLE_SPECS",
        {
            "serving_rate_compact": compact_spec,
            "procedure": procedure_spec,
        },
    )
    monkeypatch.setattr(rust_stage, "_create_one_rust_copy_stage_table", create_stage)
    monkeypatch.setattr(rust_stage, "_env_bool", lambda _name, _default: False)

    stage_tables = await rust_stage._create_rust_copy_stage_tables(
        " Unsafe Token ",
        serving_lanes=3,
    )

    assert rust_stage._rust_copy_stage_table_name(" Unsafe Kind ", " Token ").endswith(
        "unsafe_kind_token"
    )
    assert created_kinds == [
        "serving_rate_compact",
        rust_stage._serving_stage_lane_key(1),
        rust_stage._serving_stage_lane_key(2),
        "procedure",
    ]
    assert len(stage_tables) == 4


@pytest.mark.asyncio
async def test_rust_stage_logged_mode_and_procedure_merge_preserve_source_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []

    async def status(statement: str) -> None:
        statements.append(statement)

    monkeypatch.setattr(rust_stage.db, "status", status)
    monkeypatch.setattr(rust_stage.db, "all", AsyncMock(return_value=[]))
    monkeypatch.setattr(rust_stage, "_env_bool", lambda _name, _default: False)
    await rust_stage._create_one_rust_copy_stage_table(
        kind="procedure",
        schema_name="mrf",
        storage_mode="",
        stage_table="procedure_stage",
        target_table="ptg2_procedure",
        columns=["procedure_hash"],
    )
    assert not any("SET UNLOGGED" in statement for statement in statements)

    statements.clear()
    monkeypatch.setattr(rust_stage, "_env_bool", lambda _name, _default: True)
    await rust_stage._merge_rust_copy_stage_tables(
        {"procedure": "procedure_stage"},
        drop=False,
    )
    assert any("INSERT INTO" in statement for statement in statements)
    assert not any("DROP TABLE" in statement for statement in statements)


@pytest.mark.asyncio
async def test_table_column_maintenance_tolerates_migration_owned_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def reject_ddl(_statement: str) -> None:
        raise RuntimeError("column is migration-owned")

    monkeypatch.setattr(table_setup.db, "status", reject_ddl)
    monkeypatch.setattr(table_setup, "_drop_ptg2_columns", AsyncMock())

    await table_setup._ensure_ptg2_serving_rate_columns("mrf")
    await table_setup._ensure_rate_compact_columns("mrf")
    await table_setup._ensure_ptg2_provider_set_columns("mrf")
    await table_setup._ensure_ptg2_price_set_columns("mrf")
    await table_setup._ensure_ptg2_price_atom_columns("mrf")

    table_setup._drop_ptg2_columns.assert_awaited_once_with(
        "mrf",
        "ptg2_price_atom",
        ("hash_prefix", "canonical_payload", "service_code", "billing_code_modifier"),
    )


@pytest.mark.asyncio
async def test_table_index_maintenance_skips_unavailable_and_redundant_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_model = SimpleNamespace(__tablename__="missing_table")
    monkeypatch.setattr(table_setup, "_is_ptg_table_present", AsyncMock(return_value=False))
    await table_setup._ensure_indexes(missing_model, "mrf")

    monkeypatch.setattr(table_setup, "_is_ptg_table_present", AsyncMock(return_value=True))
    monkeypatch.setattr(table_setup, "_env_bool", lambda _name, _default: True)
    await table_setup._ensure_indexes(table_setup.PTG2PriceSet, "mrf")

    def is_compact_skip_enabled(name: str, _default: bool) -> bool:
        return name == table_setup.PTG2_SKIP_COMPACT_SERVING_INDEX_ENSURE_ENV

    monkeypatch.setattr(table_setup, "_env_bool", is_compact_skip_enabled)
    await table_setup._ensure_indexes(table_setup.PTG2ServingRateCompact, "mrf")

    class PrimaryModel:
        __tablename__ = "primary_model"
        __my_index_elements__ = ["id"]
        __my_additional_indexes__ = [
            {},
            {
                "index_elements": ["payload"],
                "using": "gin",
                "include": ["created_at"],
                "where": "payload IS NOT NULL",
            },
        ]

    create_index = AsyncMock()
    monkeypatch.setattr(table_setup, "_env_bool", lambda _name, _default: False)
    monkeypatch.setattr(table_setup, "_primary_key_column_names", lambda _model: ["id"])
    monkeypatch.setattr(table_setup, "_create_index_if_not_exists", create_index)
    await table_setup._ensure_indexes(PrimaryModel, "mrf")

    assert create_index.await_count == 1
    index_statement = create_index.await_args.args[0]
    assert "USING gin" in index_statement
    assert "INCLUDE (created_at)" in index_statement
    assert "WHERE payload IS NOT NULL" in index_statement

    monkeypatch.setattr(table_setup, "_primary_key_column_names", lambda _model: ["other"])
    await table_setup._ensure_indexes(PrimaryModel, "mrf")
    assert create_index.await_count == 3


@pytest.mark.asyncio
async def test_table_index_maintenance_reraises_non_race_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        table_setup.db,
        "status",
        AsyncMock(side_effect=RuntimeError("permission denied")),
    )
    with pytest.raises(RuntimeError, match="permission denied"):
        await table_setup._create_index_if_not_exists(
            "CREATE INDEX provider_idx ON provider(id)",
            index_name="provider_idx",
        )


@pytest.mark.asyncio
async def test_dynamic_table_setup_falls_back_and_keeps_control_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DynamicRate:
        pass

    class DynamicControl:
        pass

    rate_table = SimpleNamespace(__tablename__="rate_table", __table__=object())
    control_table = SimpleNamespace(__tablename__="control_table", __table__=object())
    statements: list[str] = []

    async def status(statement: str) -> None:
        statements.append(statement)
        if statement.startswith("CREATE SCHEMA") or "DROP TABLE" in statement:
            raise RuntimeError("DDL rejected")

    monkeypatch.setattr(table_setup, "PTG_DYNAMIC_TABLE_CLASSES", (DynamicRate, DynamicControl))
    monkeypatch.setattr(table_setup, "PTG_CONTROL_TABLE_CLASS_NAMES", {"DynamicControl"})
    monkeypatch.setattr(table_setup, "get_import_schema", lambda *_args: "tenant")
    monkeypatch.setattr(table_setup.db, "status", status)
    monkeypatch.setattr(table_setup.db, "create_table", AsyncMock(side_effect=RuntimeError("busy")))
    ensure_indexes = AsyncMock()
    monkeypatch.setattr(table_setup, "_ensure_indexes", ensure_indexes)

    await table_setup._ensure_ptg_dynamic_tables(
        {"DynamicRate": rate_table, "DynamicControl": control_table},
        {"DynamicRate", "DynamicControl"},
        test_mode=False,
    )

    assert ensure_indexes.await_count == 2
    assert any("DROP TABLE IF EXISTS public.rate_table" in sql for sql in statements)
    assert not any("DROP TABLE IF EXISTS public.control_table" in sql for sql in statements)


@pytest.mark.asyncio
async def test_prepare_tables_falls_back_before_building_requested_classes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DynamicRate:
        pass

    async def reject_schema(_statement: str) -> None:
        raise RuntimeError("schema unavailable")

    ensure_dynamic = AsyncMock()
    monkeypatch.setattr(table_setup, "PTG_DYNAMIC_TABLE_CLASSES", (DynamicRate,))
    monkeypatch.setattr(table_setup, "PTG_CONTROL_TABLE_CLASS_NAMES", set())
    monkeypatch.setattr(table_setup, "get_import_schema", lambda *_args: "tenant")
    monkeypatch.setattr(table_setup.db, "status", reject_schema)
    monkeypatch.setattr(table_setup, "make_class", lambda cls, *_args, **_kwargs: cls)
    monkeypatch.setattr(table_setup, "_ensure_ptg_dynamic_tables", ensure_dynamic)

    classes = await table_setup._prepare_ptg_tables(
        "import-1",
        False,
        initial_table_class_names={"DynamicRate"},
    )

    assert classes == {"DynamicRate": DynamicRate}
    ensure_dynamic.assert_awaited_once_with(
        classes,
        {"DynamicRate"},
        test_mode=False,
    )


@pytest.mark.asyncio
async def test_db_table_helpers_handle_empty_and_attribute_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(db_tables.db, "all", AsyncMock(return_value=[]))
    assert await db_tables._is_table_available("mrf", "missing") is False
    assert await db_tables._has_rows_in_table("mrf", "empty") is False
    assert await db_tables._estimated_table_rows("mrf", "empty") == 0

    responses = iter(
        [
            [SimpleNamespace(table_exists=True)],
            [SimpleNamespace(has_rows=True)],
            [SimpleNamespace(row_estimate=17)],
        ]
    )

    async def rows(*_args, **_kwargs):
        return next(responses)

    monkeypatch.setattr(db_tables.db, "all", rows)
    assert await db_tables._is_table_available("mrf", "present") is True
    assert await db_tables._has_rows_in_table("mrf", "present") is True
    assert await db_tables._estimated_table_rows("mrf", "present") == 17
    monkeypatch.setattr(db_tables.db, "scalar", AsyncMock(return_value=None))
    assert await db_tables._exact_table_rows("mrf", "empty") == 0


def test_redis_and_snapshot_helpers_fail_closed_on_malformed_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INVALID_INTEGER", "not-a-number")
    assert redis_config._env_int("INVALID_INTEGER", 4, minimum=6) == 6
    monkeypatch.setenv("INVALID_INTEGER", "-2")
    assert redis_config._env_int("INVALID_INTEGER", 4, minimum=1) == 1
    monkeypatch.setenv("FEATURE_FLAG", "YES")
    assert redis_config._is_environment_enabled("FEATURE_FLAG") is True

    monkeypatch.delenv("HLTHPRT_REDIS_ADDRESS", raising=False)
    monkeypatch.setenv("HLTHPRT_REDIS_MAX_CONNECTIONS", "8")
    settings = redis_config.build_redis_settings()
    assert settings.max_connections == 8

    assert snapshot_tables._normalize_source_key("___") is None
    long_key = "Carrier Name / " + ("x" * 80)
    normalized = snapshot_tables._normalize_source_key(long_key)
    assert normalized is not None and len(normalized) <= 48
    assert snapshot_tables._ptg2_snapshot_table_name(
        "Rate / Group",
        "source-1",
        "snapshot-1",
    ).startswith("ptg2_rate_group_")


def test_index_race_classification_requires_exact_index_identity() -> None:
    matching_error = RuntimeError(
        "duplicate key value violates unique constraint pg_class_relname_nsp_index "
        "for provider_idx"
    )
    assert table_setup._is_concurrent_index_exists_race(
        matching_error,
        "provider_idx",
    ) is True
    assert table_setup._is_concurrent_index_exists_race(
        RuntimeError("provider_idx already exists"),
        "provider_idx",
    ) is True
    assert table_setup._is_concurrent_index_exists_race(
        RuntimeError("another_idx already exists"),
        "provider_idx",
    ) is False
