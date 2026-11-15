# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configured preparation and administrator freeze for source-contribution fixtures."""

import asyncpg
import pytest

from process.network_address_projection import _identifier
from process.network_membership_pipeline import prepare_and_publish_network_candidate, prepare_network_candidate
from process.network_membership_publication import publish_network_candidate
from tests.cms_registry_recipe_postgres_support import retained_fixture_roles
from tests.test_network_compact_publication_postgres import _named_compact_runtime


async def install_source_contribution_custody(fixture, source_pin, stack, *, source_readers=None):
    """Use the normal configured validator and retain the address preparation actor."""
    monkeypatch = stack.enter_context(pytest.MonkeyPatch.context())
    await _named_compact_runtime(fixture, stack, monkeypatch)
    if source_readers is None:
        source_readers = (
            retained_fixture_roles(source_pin)[1:]
            if source_pin.retained_epoch is not None
            else source_pin.custody_runtime_roles
        )
    for role in sorted(set(source_readers)):
        for recipient in (fixture.custody_roles["custodian"], fixture.executor_role):
            await fixture.connection.execute(
                f"GRANT {_identifier(role)} TO {_identifier(recipient)} WITH ADMIN FALSE, INHERIT TRUE, SET FALSE"
            )
    custodian = await asyncpg.connect(
        fixture.custody_url.set(drivername="postgresql").render_as_string(hide_password=False),
        timeout=fixture.sql_limits.connect_timeout_seconds,
        server_settings=fixture.sql_limits.server_settings(),
    )
    stack.push_async_callback(custodian.close)
    fixture.source_custodian = custodian
    validate = fixture.seal_candidate

    async def seal_candidate(target):
        if target.candidate_id not in fixture.preparation_ids:
            fixture.preparation_ids.append(target.candidate_id)
        return await validate(target)

    fixture.seal_candidate = seal_candidate
    return seal_candidate


async def publish_source_contribution_candidate(
    fixture, connection, target, address, *, writer_roles, seal_candidate=None
):
    """Publish through the real preparation callback with its explicit SDK role profile."""
    candidate = await prepare_network_candidate(
        connection, target, address, **writer_roles, control_schema=fixture.control_schema
    )
    if candidate["state"] == "published":
        return await prepare_and_publish_network_candidate(
            connection, target, address, **writer_roles, control_schema=fixture.control_schema
        )
    validate = seal_candidate or fixture.seal_candidate
    if target.candidate_id not in fixture.preparation_ids:
        fixture.preparation_ids.append(target.candidate_id)
    await validate(target)
    async with connection.transaction(isolation="repeatable_read"):
        return await publish_network_candidate(
            connection, target, control_schema=fixture.control_schema, preparation_roles=fixture.preparation_roles
        )
