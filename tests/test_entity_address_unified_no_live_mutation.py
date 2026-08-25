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


def _shutdown_callbacks(events: list[tuple[str, str]]) -> dict[str, object]:
    async def run_sql(statement, **_kwargs):
        events.append(("sql", statement))
        return 0

    async def validate(_db_schema, table_name, _support_stage_classes, **_kwargs):
        events.append(("validate", table_name))
        return {}

    async def validate_geo(_db_schema, table_name):
        events.append(("validate_geo", table_name))
        return 0

    async def materialize_geo(
        db_schema,
        table_name,
        *,
        force,
        context,
        **_kwargs,
    ):
        events.append(
            (
                "sql",
                entity_address_unified._materialize_geo_assurance_sql(
                    db_schema,
                    table_name,
                    force=force,
                ),
            )
        )
        context["invalid_geo_assurance_rows"] = await validate_geo(
            db_schema,
            table_name,
        )
        return 0

    async def publish(*_args, **_kwargs):
        events.append(("publish", "cutover"))

    async def mark(_run_id, *, status, **_kwargs):
        events.append(("status", status))

    async def create_post_publish_indexes(*_args, **_kwargs):
        events.append(("ddl", "post-publish indexes"))

    return {
        "_run_sql_phase": run_sql,
        "_validate_geo_assurance_projection": validate_geo,
        "_materialize_geo_assurance": materialize_geo,
        "_validate_publish_integrity": validate,
        "_publish_staged_entity_address_tables": publish,
        "mark_control_run": mark,
        "_create_post_publish_indexes": create_post_publish_indexes,
    }


def _mock_shutdown_dependencies(monkeypatch, events: list[tuple[str, str]]) -> None:
    monkeypatch.setattr(entity_address_unified.db, "scalar", AsyncMock(return_value=100))
    mocks_by_name = {
        "ensure_database": AsyncMock(),
        "_has_table": AsyncMock(return_value=True),
        "_address_alias_generation": AsyncMock(return_value=0),
        "_drop_stage_secondary_indexes": AsyncMock(return_value=0),
        "_compact_geo_assurance_stage": AsyncMock(return_value="set_logged"),
        "_create_stage_indexes": AsyncMock(),
        "_inherit_archive_coordinates": AsyncMock(
            return_value={"inherited_rows": 0, "ambiguous_rows": 0}
        ),
        "print_time_info": lambda _started_at: None,
        **_shutdown_callbacks(events),
    }
    for name, replacement in mocks_by_name.items():
        monkeypatch.setattr(entity_address_unified, name, replacement)


def _provider_directory_partial_context() -> dict:
    """Build a fenced partial-refresh shutdown context."""

    shutdown_payload = _shutdown_context(
        refresh_mode=(
            entity_address_unified.ENTITY_ADDRESS_REFRESH_MODE_PROVIDER_DIRECTORY_PARTIAL
        )
    )
    shutdown_payload["context"].update(
        {
            "partial_provider_directory_dataset_id": "dataset-current",
            "partial_provider_directory_run_id": "run-overlay",
            "partial_provider_directory_scope": "latest-run",
            "partial_provider_directory_source_ids": ["source-current"],
        }
    )
    return shutdown_payload


def _assert_partial_projection_cutover_order(events: list[tuple[str, str]]) -> None:
    """Require coordinate cleanup, projection, validation, then cutover."""

    publish_index = events.index(("publish", "cutover"))
    validation_events = [
        (index, event_detail)
        for index, (kind, event_detail) in enumerate(events)
        if kind == "validate"
    ]
    assert len(validation_events) == 1
    validation_index, validation_table = validation_events[0]
    projection_index = next(
        index
        for index, (kind, statement) in enumerate(events)
        if kind == "sql" and "WITH projection_targets AS MATERIALIZED" in statement
    )
    coordinate_clear_index = next(
        index
        for index, (kind, statement) in enumerate(events)
        if kind == "sql" and "SET lat = NULL" in statement
    )
    geo_validation_index = next(
        index
        for index, (kind, _event_detail) in enumerate(events)
        if kind == "validate_geo"
    )
    assert coordinate_clear_index < projection_index < geo_validation_index
    assert geo_validation_index < validation_index < publish_index
    assert validation_table != entity_address_unified.EntityAddressUnified.__main_table__
    assert validation_table.startswith("entity_address_unified_")
    assert all(
        kind not in {"sql", "ddl"}
        for kind, _event_detail in events[publish_index + 1 :]
    )
    assert [
        event_detail for kind, event_detail in events if kind == "status"
    ] == ["succeeded"]


@pytest.mark.asyncio
async def test_provider_directory_partial_validates_replacement_stage_before_read_only_cutover(
    monkeypatch,
):
    """Prove partial publication validates the projected replacement stage."""

    events: list[tuple[str, str]] = []
    _mock_shutdown_dependencies(monkeypatch, events)
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_PUBLISH_VALIDATION", "true")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE", "all")

    ctx = _provider_directory_partial_context()
    dataset_fence = AsyncMock()
    monkeypatch.setattr(
        entity_address_unified,
        "_assert_current_provider_directory_dataset",
        dataset_fence,
    )
    await entity_address_unified.shutdown(ctx)

    _assert_partial_projection_cutover_order(events)
    assert ctx["context"]["post_publish_index_profile"] == "none"
    dataset_fence.assert_awaited_once_with(
        "mrf",
        source_id="source-current",
        expected_dataset_id="dataset-current",
        expected_root_run_id="run-overlay",
    )


@pytest.mark.asyncio
async def test_deferred_validation_is_read_only_and_precedes_terminal_success(monkeypatch):
    events: list[tuple[str, str]] = []
    _mock_shutdown_dependencies(monkeypatch, events)
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_PUBLISH_VALIDATION", "true")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE", "none")

    ctx = _shutdown_context(refresh_mode=entity_address_unified.ENTITY_ADDRESS_REFRESH_MODE_FULL)
    ctx["context"]["stage_reused"] = True
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
    assert any(
        kind == "sql"
        and "WITH projection_targets AS MATERIALIZED" in statement
        and "WHERE TRUE" in statement
        for kind, statement in events
    )


@pytest.mark.asyncio
async def test_archive_coordinate_mismatches_are_informational_but_invalid_coordinates_fail(
    monkeypatch,
):
    monkeypatch.setattr(
        entity_address_unified,
        "_is_location_primary_key_validated",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(entity_address_unified, "_has_table", AsyncMock(return_value=True))
    monkeypatch.setattr(
        entity_address_unified,
        "_address_alias_generation",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(entity_address_unified.db, "all", AsyncMock(return_value=[]))

    scalar = AsyncMock(side_effect=[0, 0, 0, 1233, 0, 0, 0, 0, 0])
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

    scalar.side_effect = [0, 0, 0, 1233, 0, 0, 0, 0, 0]
    invalid_coordinate_count.return_value = 1
    with pytest.raises(RuntimeError, match="1 staged rows have invalid latitude/longitude values"):
        await entity_address_unified._validate_publish_integrity(
            "mrf",
            "entity_address_unified_stage_test",
            {},
            test_mode=False,
        )

    scalar.side_effect = [0, 0, 0, 1233, 1, 0, 0, 0, 0]
    invalid_coordinate_count.return_value = 0
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_ARCHIVE_COORDINATES", "true")
    with pytest.raises(RuntimeError, match="1 staged rows reference archive addresses without coordinates"):
        await entity_address_unified._validate_publish_integrity(
            "mrf",
            "entity_address_unified_stage_test",
            {},
            test_mode=False,
        )
