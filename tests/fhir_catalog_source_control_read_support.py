# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite registry-reference observations for the native Source publication fixture."""


async def _grant_source_retention_control_reads(case):
    """Let the Source cutover inspect only the registry's retained address references."""
    connection = case.fixture.connection
    schema, owner = case.fixture.control_schema, case.component.owner
    await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{owner}"')
    tables = ["network_membership_candidate", "network_serving_manifest"]
    if await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relname='network_candidate_custody')",
        schema,
    ):
        tables.append("network_candidate_custody")
    for table in tables:
        await connection.execute(f'GRANT SELECT ON "{schema}"."{table}" TO "{owner}"')
