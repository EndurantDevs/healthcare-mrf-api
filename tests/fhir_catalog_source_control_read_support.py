# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite registry-reference observations for the native Source publication fixture."""

import asyncpg
import pytest

from process.provider_directory_projection_stage import _copy_driver


async def _grant_source_retention_control_reads(case):
    """Let the Source cutover inspect only the registry's retained address references."""
    connection = case.fixture.connection
    schema, owner = case.fixture.control_schema, case.component.owner
    await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{owner}"')
    for table in ("network_membership_candidate", "network_serving_manifest", "network_serving_control"):
        await connection.execute(f'GRANT SELECT,MAINTAIN ON "{schema}"."{table}" TO "{owner}"')


async def _assert_source_control_mutations_refused(case):
    """The maintenance lock grant permits no registry data mutation or schema creation."""
    component = case.component
    async with component.factory.transaction(component.database, isolation="read committed") as session:
        connection = await _copy_driver(session)
        for table, column in (
            ("network_membership_candidate", "state"),
            ("network_serving_manifest", "eligible"),
            ("network_serving_control", "metadata_generation"),
        ):
            relation = f'"{case.fixture.control_schema}"."{table}"'
            for statement in (
                f"INSERT INTO {relation}({column}) SELECT {column} FROM {relation} WHERE false",
                f"UPDATE {relation} SET {column}={column} WHERE false",
                f"DELETE FROM {relation} WHERE false",
                f"TRUNCATE {relation}",
            ):
                await _assert_mutation_refused(connection, statement)
        await _assert_mutation_refused(
            connection, f'CREATE TABLE "{case.fixture.control_schema}".source_forbidden_probe(id integer)'
        )


async def _assert_mutation_refused(connection, statement):
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction():
            await connection.execute(statement)
