# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actors absent from a capture census are admitted only when wholly outside every capture."""

import os
from unittest.mock import Mock
from uuid import uuid4

import asyncpg
import pytest

from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile
from process.registry_source_recipe_composition import (
    RegistrySourceCompositionError,
    SourceWitnessRuntime,
    require_source_witness_census_outsiders,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def census_case():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    suffix = uuid4().hex
    role_by_kind = {kind: f"census_{kind}_{suffix}" for kind in ("capture", "census", "outsider", "profile")}
    schema = "census_capture_" + suffix
    try:
        for name in role_by_kind.values():
            await connection.execute(f'CREATE ROLE "{name}" NOLOGIN')
        await connection.execute(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{role_by_kind["capture"]}"')
        await connection.execute(f'CREATE TABLE "{schema}".capture_rows (id integer PRIMARY KEY)')
        await connection.execute(f'ALTER TABLE "{schema}".capture_rows OWNER TO "{role_by_kind["capture"]}"')
        await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role_by_kind["census"]}"')
        await connection.execute(f'GRANT SELECT ON "{schema}".capture_rows TO "{role_by_kind["census"]}"')
        runtime = SourceWitnessRuntime(
            Mock(side_effect=AssertionError("Source must not open in a census test")),
            (FHIRCatalogSourceReadProfile("source_example", role_by_kind["profile"], ("source_reader",)),),
            role_by_kind["capture"],
            (role_by_kind["census"],),
            census_outsiders=(role_by_kind["outsider"],),
        )
        yield connection, role_by_kind, schema, runtime
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        for name in role_by_kind.values():
            await connection.execute(f'DROP ROLE IF EXISTS "{name}"')
        await connection.close()


async def test_protected_outsider_without_capture_reach_is_admitted(census_case):
    connection, role_by_kind, _, runtime = census_case
    await require_source_witness_census_outsiders(connection, runtime, ())
    await require_source_witness_census_outsiders(connection, runtime, (role_by_kind["outsider"],))


@pytest.mark.parametrize(
    "grant",
    (
        'ALTER ROLE "{outsider}" LOGIN',
        'GRANT "{capture}" TO "{outsider}" WITH INHERIT FALSE, SET TRUE',
        'GRANT "{profile}" TO "{outsider}"',
        'GRANT USAGE ON SCHEMA "{schema}" TO "{outsider}"',
        'GRANT SELECT ON "{schema}".capture_rows TO "{outsider}"',
        'GRANT "{census}" TO "{outsider}"',
        'GRANT "{census}" TO "{outsider}" WITH INHERIT FALSE, SET TRUE',
    ),
)
async def test_outsider_with_any_capture_reach_or_privilege_is_refused(census_case, grant):
    connection, role_by_kind, schema, runtime = census_case
    await connection.execute(grant.format(schema=schema, **role_by_kind))
    with pytest.raises(RegistrySourceCompositionError, match="^registry_source_witness_runtime_census_differs$"):
        await require_source_witness_census_outsiders(connection, runtime, (role_by_kind["outsider"],))


async def test_census_members_unknown_and_unsorted_outsiders_are_refused(census_case):
    connection, role_by_kind, _, runtime = census_case
    for outsiders in (
        (role_by_kind["census"],),
        (role_by_kind["capture"],),
        ("census_missing_" + uuid4().hex,),
        (role_by_kind["outsider"], role_by_kind["outsider"]),
        tuple(sorted((role_by_kind["outsider"], role_by_kind["profile"]), reverse=True)),
    ):
        with pytest.raises(RegistrySourceCompositionError, match="^registry_source_witness_runtime_census_differs$"):
            await require_source_witness_census_outsiders(connection, runtime, outsiders)
    with pytest.raises(RegistrySourceCompositionError, match="^registry_source_witness_runtime_invalid$"):
        await require_source_witness_census_outsiders(connection, runtime, [role_by_kind["outsider"]])
