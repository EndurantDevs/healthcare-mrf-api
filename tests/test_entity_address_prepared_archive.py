# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Staged archive enrichment and alias validation use the desired read relation."""

import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_candidate_preparation as preparation
from tests.provider_directory_profile_delta_test_support import _delta_database
from tests.test_entity_address_candidate_preparation_postgres import _inputs, native


def _archive_inputs():
    return replace(_inputs(), relation_overrides=(("address_archive_v2", "prepared_archive"),))


def test_archive_coordinate_generators_use_internal_preparation_scope():
    inputs = _archive_inputs()
    preparation.validate_preparation_input(inputs)
    token = preparation._PREPARATION.set(inputs)
    try:
        statements = (
            native._backfill_archive_coordinates_sql("sample", "result"),
            native._backfill_archive_coordinates_sql("sample", "result", coordinate_scope_table="scope"),
            native._inherit_archive_coordinates_sql("sample", "result"),
            native._available_archive_enrichment_sql("sample")[1],
        )
        assert all("sample.prepared_archive" in sql and "sample.address_archive_v2" not in sql for sql in statements)
    finally:
        preparation._PREPARATION.reset(token)
    assert "sample.address_archive_v2" in native._backfill_archive_coordinates_sql("sample", "result")


@pytest.mark.asyncio
async def test_alias_probe_uses_prepared_archive(monkeypatch):
    read = AsyncMock(return_value=None)
    monkeypatch.setattr(native.preparation_admission, "read_first", read)
    monkeypatch.setattr(native, "_report_raw_alias_integrity_progress", AsyncMock())
    token = preparation._PREPARATION.set(_archive_inputs())
    try:
        await native._raw_alias_integrity_violation_for_range(
            "sample", "raw", "NULL::uuid", (None, None), asyncio.Semaphore(1), object()
        )
    finally:
        preparation._PREPARATION.reset(token)
    statement = read.call_args.args[1]
    assert "sample.prepared_archive AS target" in statement and "sample.address_archive_v2" not in statement


@pytest.mark.asyncio
async def test_real_coordinate_read_preserves_incumbent_archive(monkeypatch):
    async with _delta_database(monkeypatch) as (database, schema):
        await database.status(
            f"CREATE TABLE {schema}.address_archive_v2 (address_key uuid,lat numeric,long numeric,merged_into uuid)"
        )
        await database.status(
            f"INSERT INTO {schema}.address_archive_v2 VALUES ('00000000-0000-0000-0000-000000000001',1,2,NULL)"
        )
        await database.status(
            f"CREATE VIEW {schema}.prepared_archive AS SELECT address_key,10::numeric lat,20::numeric long,merged_into FROM {schema}.address_archive_v2"
        )
        await database.status(f"CREATE TABLE {schema}.result (address_key uuid,lat numeric,long numeric)")
        await database.status(
            f"INSERT INTO {schema}.result SELECT address_key,NULL,NULL FROM {schema}.address_archive_v2"
        )
        token = preparation._PREPARATION.set(_archive_inputs())
        try:
            await database.status(native._backfill_archive_coordinates_sql(schema, "result"))
        finally:
            preparation._PREPARATION.reset(token)
        assert tuple(await database.first(f"SELECT lat,long FROM {schema}.result")) == (10, 20)
        assert tuple(await database.first(f"SELECT lat,long FROM {schema}.address_archive_v2")) == (1, 2)
