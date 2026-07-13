# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest


entity_address_unified = importlib.import_module("process.entity_address_unified")


def _shutdown_context(*, refresh_mode: str) -> dict:
    return {
        "import_date": "20260712",
        "context": {
            "run": 1,
            "control_run_id": "run_test",
            "test_mode": True,
            "publish_requested": True,
            "serving_only_refresh": True,
            "refresh_mode": refresh_mode,
            "staged_rows": 100,
        },
    }


def _mock_shutdown_dependencies(monkeypatch, events: list[tuple[str, str]]) -> None:
    async def run_sql(statement, **_kwargs):
        events.append(("sql", statement))
        return 0

    async def validate(_db_schema, table_name, _support_stage_classes, **_kwargs):
        events.append(("validate", table_name))
        return {}

    async def publish(*_args, **_kwargs):
        events.append(("publish", "cutover"))

    async def mark(_run_id, *, status, **_kwargs):
        events.append(("status", status))

    async def create_post_publish_indexes(*_args, **_kwargs):
        events.append(("ddl", "post-publish indexes"))

    monkeypatch.setattr(entity_address_unified, "ensure_database", AsyncMock())
    monkeypatch.setattr(entity_address_unified, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(entity_address_unified.db, "scalar", AsyncMock(return_value=100))
    monkeypatch.setattr(entity_address_unified, "_run_sql_phase", run_sql)
    monkeypatch.setattr(
        entity_address_unified,
        "_inherit_archive_coordinates",
        AsyncMock(return_value={"inherited_rows": 0, "ambiguous_rows": 0}),
    )
    monkeypatch.setattr(entity_address_unified, "_validate_publish_integrity", validate)
    monkeypatch.setattr(entity_address_unified, "_publish_staged_entity_address_tables", publish)
    monkeypatch.setattr(entity_address_unified, "mark_control_run", mark)
    monkeypatch.setattr(
        entity_address_unified,
        "_create_post_publish_indexes",
        create_post_publish_indexes,
    )
    monkeypatch.setattr(entity_address_unified, "print_time_info", lambda _started_at: None)


@pytest.mark.asyncio
async def test_provider_directory_partial_validates_replacement_stage_before_read_only_cutover(
    monkeypatch,
):
    events: list[tuple[str, str]] = []
    _mock_shutdown_dependencies(monkeypatch, events)
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_PUBLISH_VALIDATION", "true")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE", "all")

    ctx = _shutdown_context(
        refresh_mode=entity_address_unified.ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL
    )
    await entity_address_unified.shutdown(ctx)

    publish_index = events.index(("publish", "cutover"))
    validation_events = [(index, value) for index, (kind, value) in enumerate(events) if kind == "validate"]
    assert len(validation_events) == 1
    validation_index, validation_table = validation_events[0]
    assert validation_index < publish_index
    assert validation_table != entity_address_unified.EntityAddressUnified.__main_table__
    assert validation_table.startswith("entity_address_unified_")

    post_cutover_events = events[publish_index + 1 :]
    assert all(kind not in {"sql", "ddl"} for kind, _value in post_cutover_events)
    assert [value for kind, value in events if kind == "status"] == ["succeeded"]
    assert ctx["context"]["post_publish_index_profile"] == "none"


@pytest.mark.asyncio
async def test_deferred_validation_is_read_only_and_precedes_terminal_success(monkeypatch):
    events: list[tuple[str, str]] = []
    _mock_shutdown_dependencies(monkeypatch, events)
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_PUBLISH_VALIDATION", "true")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE", "none")

    ctx = _shutdown_context(refresh_mode=entity_address_unified.ENTITY_ADDRESS_REFRESH_MODE_FULL)
    await entity_address_unified.shutdown(ctx)

    publish_index = events.index(("publish", "cutover"))
    validation_index = events.index(
        ("validate", entity_address_unified.EntityAddressUnified.__main_table__)
    )
    running_index = events.index(("status", "running"))
    succeeded_index = events.index(("status", "succeeded"))

    assert publish_index < running_index < validation_index < succeeded_index
    assert all(kind != "sql" for kind, _value in events[publish_index + 1 :])
    assert ctx["context"]["publish_validation"]["status"] == "complete"


@pytest.mark.asyncio
async def test_archive_coordinate_mismatches_are_informational_but_invalid_coordinates_fail(
    monkeypatch,
):
    monkeypatch.setattr(
        entity_address_unified,
        "_location_key_primary_key_validated",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(entity_address_unified, "_table_exists", AsyncMock(return_value=True))
    monkeypatch.setattr(entity_address_unified.db, "all", AsyncMock(return_value=[]))

    scalar = AsyncMock(side_effect=[0, 1233, 0, 0, 0, 0, 0])
    monkeypatch.setattr(entity_address_unified.db, "scalar", scalar)
    invalid_coordinate_count = AsyncMock(return_value=0)
    monkeypatch.setattr(
        entity_address_unified,
        "_invalid_coordinate_count",
        invalid_coordinate_count,
    )

    metrics = await entity_address_unified._validate_publish_integrity(
        "mrf",
        "entity_address_unified_stage_test",
        {},
        test_mode=False,
    )

    assert metrics["archive_coordinate_mismatch_rows"] == 1233
    assert metrics["invalid_coordinate_rows"] == 0

    scalar.side_effect = [0, 1233, 0, 0, 0, 0, 0]
    invalid_coordinate_count.return_value = 1
    with pytest.raises(RuntimeError, match="1 staged rows have invalid latitude/longitude values"):
        await entity_address_unified._validate_publish_integrity(
            "mrf",
            "entity_address_unified_stage_test",
            {},
            test_mode=False,
        )

    scalar.side_effect = [0, 1233, 1, 0, 0, 0, 0]
    invalid_coordinate_count.return_value = 0
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_ARCHIVE_COORDINATES", "true")
    with pytest.raises(RuntimeError, match="1 staged rows reference archive addresses without coordinates"):
        await entity_address_unified._validate_publish_integrity(
            "mrf",
            "entity_address_unified_stage_test",
            {},
            test_mode=False,
        )
