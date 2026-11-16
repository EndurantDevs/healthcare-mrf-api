# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""The ordinary physical desired path activates shared group keys."""

import pytest

from tests.entity_address_batch_group_support import (
    assert_cached_raw_collation,
    assert_partial_failure_cleanup,
    assert_partial_groups,
    assert_source_case_parity,
)


@pytest.mark.asyncio
async def test_physical_desired_partial_groups_match_sql_and_retire_cache(monkeypatch):
    await assert_partial_groups(monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("locale,qualified", [("en", True), ("tr", False)])
async def test_actual_column_ascii_behavior_activates_native_or_preserves_locale(monkeypatch, locale, qualified):
    await assert_source_case_parity(monkeypatch, locale, qualified)


@pytest.mark.asyncio
@pytest.mark.parametrize("split", ["false", "true"])
async def test_inline_evidence_and_raw_hash_share_keys_with_both_array_paths(monkeypatch, split):
    """Exercise cached evidence hashes and final inline joins through actual output SQL."""
    from tests.entity_address_batch_postgres_support import compare_builders

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SPLIT_ARRAY_AGGREGATES", split)
    await compare_builders(monkeypatch, "raw_inline", mixed=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["cancel", "collision"])
async def test_partial_cache_cancel_and_name_collision_preserve_exact_cleanup(monkeypatch, failure):
    await assert_partial_failure_cleanup(monkeypatch, failure)


@pytest.mark.asyncio
@pytest.mark.parametrize("has_missing_tuple", [False, True])
async def test_existing_c_cache_never_supplies_turkish_raw_semantics(monkeypatch, has_missing_tuple):
    await assert_cached_raw_collation(monkeypatch, has_missing_tuple)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["consumer", "caller"])
async def test_projection_only_source_readers_drain_before_cleanup(monkeypatch, failure):
    """Cover consumer failure and caller cancellation with actual SQL-only inputs."""
    from tests.entity_address_batch_group_support import assert_projection_reader_drain

    await assert_projection_reader_drain(monkeypatch, failure)
