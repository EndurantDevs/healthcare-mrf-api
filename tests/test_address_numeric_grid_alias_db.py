# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL lifecycle tests for reviewed numeric-grid address aliases."""

from __future__ import annotations

import asyncio
from contextlib import suppress
import importlib.util
import json
import os
from pathlib import Path

import asyncpg
import pytest

from db.models import db
from process.address_numeric_grid_alias import _mark_failed, run_numeric_grid_alias
from process.address_numeric_grid_alias_revoke import revoke_numeric_grid_alias
from process.address_strict_source_backfill import run_strict_source_backfill
from process.ext import address_alias_sql
from process.ext import address_canon
from process.ext import address_strict_source_backfill_sql
from process.ext.address_canon import resolve_into_archive, stamp_address_keys


entity_address_unified = __import__(
    "process.entity_address_unified",
    fromlist=["entity_address_unified"],
)
provider_directory = __import__(
    "process.provider_directory_fhir",
    fromlist=["provider_directory_fhir"],
)


ROOT = Path(__file__).resolve().parents[1]


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _requires_test_database() -> None:
    if "test" not in os.getenv("HLTHPRT_DB_DATABASE", ""):
        pytest.skip("numeric-grid alias tests require a disposable test database")


async def _reset_alias_data(schema: str) -> None:
    await db.status(
        f"""
        TRUNCATE TABLE
            {schema}.address_alias_v1,
            {schema}.address_alias_candidate_v1,
            {schema}.address_alias_run_v1
        RESTART IDENTITY CASCADE;
        """
    )
    await db.status(f"TRUNCATE TABLE {schema}.address_archive_v2 CASCADE;")
    await db.status(
        f"""
        UPDATE {schema}.address_alias_state_v1
        SET generation = 0,
            updated_at = now()
        WHERE singleton = true;
        """
    )
    await db.status(
        f"""
        UPDATE {schema}.address_alias_artifact_state_v1
        SET generation = 0,
            updated_at = now();
        """
    )


async def _prepare_serving_fence_schema(
    schema: str,
    *,
    generation: int,
    overlay_generation: int,
    corroboration_generation: int,
) -> None:
    """Create a minimal serving schema with explicit alias receipts."""
    await db.status(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;')
    await db.status(f'CREATE SCHEMA "{schema}";')
    await _seed_serving_fence_state(
        schema,
        generation=generation,
        overlay_generation=overlay_generation,
        corroboration_generation=corroboration_generation,
    )
    await _seed_serving_fence_relations(schema)


async def _seed_serving_fence_state(
    schema: str,
    *,
    generation: int,
    overlay_generation: int,
    corroboration_generation: int,
) -> None:
    """Seed alias singleton and artifact-generation rows."""
    await db.status(
        f"""
        CREATE TABLE "{schema}".address_alias_state_v1 (
            singleton boolean PRIMARY KEY,
            schema_version smallint NOT NULL,
            active_ruleset_version smallint NOT NULL,
            generation bigint NOT NULL
        );
        """
    )
    await db.status(
        f"""
        INSERT INTO "{schema}".address_alias_state_v1
            (singleton, schema_version, active_ruleset_version, generation)
        VALUES (true, 2, 1, :generation);
        """,
        generation=generation,
    )
    await db.status(
        f"""
        CREATE TABLE "{schema}".address_alias_artifact_state_v1 (
            artifact_name varchar(128) PRIMARY KEY,
            generation bigint NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now()
        );
        """
    )
    await db.status(
        f"""
        INSERT INTO "{schema}".address_alias_artifact_state_v1
            (artifact_name, generation)
        VALUES
            ('provider_directory_address_overlay', :overlay_generation),
            ('provider_directory_address_corroboration', :corroboration_generation);
        """,
        overlay_generation=overlay_generation,
        corroboration_generation=corroboration_generation,
    )


async def _seed_serving_fence_relations(schema: str) -> None:
    """Seed overlay, corroboration, and cutover probe relations."""
    await db.status(
        f'CREATE TABLE "{schema}".provider_directory_address_overlay '
        "(marker text NOT NULL);"
    )
    await db.status(
        f'INSERT INTO "{schema}".provider_directory_address_overlay '
        "VALUES ('overlay-old');"
    )
    await db.status(
        f'CREATE TABLE "{schema}".provider_directory_address_corroboration '
        "(marker text NOT NULL);"
    )
    await db.status(
        f'INSERT INTO "{schema}".provider_directory_address_corroboration '
        "VALUES ('corroboration-old');"
    )
    await db.status(
        f'CREATE TABLE "{schema}".cutover_probe (marker text NOT NULL);'
    )
    await db.status(
        f'INSERT INTO "{schema}".cutover_probe VALUES (\'untouched\');'
    )


async def _replace_overlay_relation(schema: str) -> None:
    async with db.transaction():
        await db.scalar(address_alias_sql.alias_advisory_xact_lock_sql())
        await db.status(
            f'ALTER TABLE "{schema}".provider_directory_address_overlay '
            "RENAME TO provider_directory_address_overlay_replaced;"
        )
        await db.status(
            f'CREATE TABLE "{schema}".provider_directory_address_overlay '
            "(marker text NOT NULL);"
        )
        await db.status(
            f'INSERT INTO "{schema}".provider_directory_address_overlay '
            "VALUES ('overlay-new');"
        )


_INSERT_ARCHIVE_ADDRESS_SQL = """
    INSERT INTO {schema}.address_archive_v2 (
        address_key,
        identity_key,
        identity_version,
        precision,
        premise_key,
        line1_norm,
        unit_norm,
        city_norm,
        state_code,
        zip5,
        country_code,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        source_bits,
        strict_source_bits
    )
    SELECT
        {schema}.addr_key_v1(
            :first_line, :second_line, :city, :state, :postal_code, 'US'
        ),
        {schema}.addr_identity_key_v1(
            :first_line, :second_line, :city, :state, :postal_code, 'US'
        ),
        2,
        'street',
        {schema}.addr_premise_key_v1(
            :first_line, :second_line, :city, :state, :postal_code, 'US'
        ),
        {schema}.addr_street_norm_v1(:first_line, :second_line),
        {schema}.addr_unit_norm_v1(:first_line, :second_line),
        {schema}.addr_city_norm_v1(:city),
        {schema}.addr_state_code_v1(:state),
        left(:postal_code, 5),
        'US',
        :first_line,
        :second_line,
        :city,
        :state,
        :postal_code,
        :strict_source_bits,
        :strict_source_bits
    RETURNING address_key::text;
"""


async def _insert_archive_address(
    schema: str,
    *,
    first_line: str,
    second_line: str | None,
    city: str = "Example City",
    state: str = "TX",
    postal_code: str = "75001",
    strict_source_bits: int,
) -> str:
    """Insert one canonical archive row and return its address key."""
    archive_row = await db.first(
        _INSERT_ARCHIVE_ADDRESS_SQL.format(schema=schema),
        first_line=first_line,
        second_line=second_line,
        city=city,
        state=state,
        postal_code=postal_code,
        strict_source_bits=strict_source_bits,
    )
    assert archive_row is not None
    return str(archive_row[0])
