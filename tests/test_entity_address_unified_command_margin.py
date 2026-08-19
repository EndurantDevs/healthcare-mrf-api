# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic coverage for the entity-address command boundary."""

import importlib
from unittest.mock import AsyncMock

import pytest


entity_address_unified = importlib.import_module("process.entity_address_unified")


def test_entity_address_import_id_is_normalized_or_dated() -> None:
    assert entity_address_unified._normalize_import_id(" run-! 123 ") == "run123"
    fallback = entity_address_unified._normalize_import_id(None)
    assert len(fallback) == 8
    assert fallback.isdecimal()
    punctuation_fallback = entity_address_unified._normalize_import_id("!!!")
    assert len(punctuation_fallback) == 8
    assert punctuation_fallback.isdecimal()


@pytest.mark.asyncio
async def test_entity_address_command_forwards_bounded_payload(monkeypatch) -> None:
    redis = AsyncMock()
    monkeypatch.setattr(
        entity_address_unified,
        "create_pool",
        AsyncMock(return_value=redis),
    )
    monkeypatch.setattr(
        entity_address_unified,
        "build_redis_settings",
        lambda: object(),
    )

    await entity_address_unified.run_entity_address_unified_command(
        test_mode=True,
        limit_per_source=-1,
        publish=False,
        refresh_mode="provider-directory-partial",
        serving_only_refresh=True,
        reuse_raw_stage=True,
        provider_directory_run_id="run-one",
        provider_directory_source_ids=(" source-one ", "source-two"),
        provider_directory_partial_scope="partial",
        provider_directory_source_batch_size=-2,
    )

    redis.enqueue_job.assert_awaited_once_with(
        "process_data",
        {
            "test_mode": True,
            "limit_per_source": 0,
            "publish": False,
            "refresh_mode": "provider-directory-partial",
            "serving_only_refresh": True,
            "reuse_raw_stage": True,
            "provider_directory_run_id": "run-one",
            "provider_directory_source_ids": ["source-one", "source-two"],
            "provider_directory_partial_scope": "partial",
            "provider_directory_source_batch_size": 0,
        },
        _queue_name=entity_address_unified.ENTITY_ADDRESS_UNIFIED_QUEUE_NAME,
    )


@pytest.mark.asyncio
async def test_entity_address_command_forwards_exact_dataset_fence(monkeypatch) -> None:
    redis = AsyncMock()
    monkeypatch.setattr(
        entity_address_unified,
        "create_pool",
        AsyncMock(return_value=redis),
    )
    monkeypatch.setattr(
        entity_address_unified,
        "build_redis_settings",
        lambda: object(),
    )

    await entity_address_unified.run_entity_address_unified_command(
        provider_directory_dataset_id="synthetic-dataset",
    )

    redis.enqueue_job.assert_awaited_once_with(
        "process_data",
        {
            "test_mode": False,
            "provider_directory_dataset_id": "synthetic-dataset",
        },
        _queue_name=entity_address_unified.ENTITY_ADDRESS_UNIFIED_QUEUE_NAME,
    )
