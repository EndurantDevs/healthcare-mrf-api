# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real independent candidate sealing for admitted source-contribution fixtures."""

import asyncpg

from process.network_address_projection import _identifier
from process.network_candidate_custody import seal_network_candidate_custody
from tests.registry_candidate_custody_postgres_support import install_candidate_custody


async def install_source_contribution_custody(fixture, source_pin, stack):
    """Use a separate native custodian; the producer receives no seal-ledger authority."""
    await install_candidate_custody(fixture, stack)
    source_readers = (
        source_pin.retained_epoch.runtime_roles
        if source_pin.retained_epoch is not None
        else (source_pin.custody_runtime_roles)
    )
    for role in sorted(set(source_readers)):
        await fixture.connection.execute(
            f"GRANT {_identifier(role)} TO {_identifier(fixture.custody_roles['custodian'])} "
            "WITH ADMIN FALSE, INHERIT TRUE, SET FALSE"
        )
    custodian = await asyncpg.connect(
        fixture.custody_url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    stack.push_async_callback(custodian.close)
    fixture.source_custodian = custodian

    async def seal_candidate(target):
        async def authorize(connection, supplied):
            assert connection is custodian and supplied == target
            return await connection.fetchval(
                "SELECT current_user=session_user AND current_user::text=$1", fixture.custody_roles["custodian"]
            )

        return await seal_network_candidate_custody(
            custodian,
            target,
            control_schema=fixture.control_schema,
            runtime_roles=(fixture.roles["reader"],),
            authorize=authorize,
        )

    fixture.seal_candidate = seal_candidate
    return seal_candidate
