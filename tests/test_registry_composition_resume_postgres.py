# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native registered-contract lookup catches partial scope and sibling candidates."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from process import registry_composition_resume as resume
from process.network_membership_candidate_lifecycle import create_network_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.registry_source_contribution_store import registry_source_contribution_receipt
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_composition_resume import _registered
from tests.test_registry_source_contributions import _descriptor, _recipe

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _registration(connection, schema, request_id, revision, *, altered_target=None):
    expected, candidate = _registered(request_id, revision)
    copy_target = expected if altered_target is None else altered_target
    receipt = registry_source_contribution_receipt(
        SimpleNamespace(contribution_receipts=(_descriptor(_recipe()),), contribution_revision=revision)
    )
    try:
        async with connection.transaction(isolation="repeatable_read"):
            await create_network_candidate(
                connection,
                copy_target,
                source_generations=candidate["source_generations"],
                source_recipes=(_recipe(),),
                source_contributions=receipt,
                approved_custom_revision=7,
                expected_head=9,
                expected_rows=1,
                control_schema=schema,
            )
        yield copy_target
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
        await connection.execute(
            f'DELETE FROM "{schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )


@pytest.mark.parametrize("revision", [1, 2])
async def test_native_pending_contract_returns_the_original_target_and_revision(serving_schema, revision):
    connection, schema, _ = serving_schema
    request_id = uuid4()
    async with _registration(connection, schema, request_id, revision) as copy_target:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            assert await resume.resolve_registered_contribution_resume(
                connection, request_id, 7, 9, control_schema=schema
            ) == resume.RegisteredContributionResume(copy_target, revision)
            with pytest.raises(resume.RegistryCompositionResumeError, match="approval_scope_changed"):
                await resume.resolve_registered_contribution_resume(connection, request_id, 8, 9, control_schema=schema)


async def test_native_partial_or_multiple_matching_scopes_never_fall_through(serving_schema):
    connection, schema, _ = serving_schema
    request_id = uuid4()
    copy_target, _ = _registered(request_id)
    identity = uuid4()
    partial = MembershipCopyTarget(
        copy_target.dataset_id, str(uuid4()), str(uuid4()), str(identity), "network_candidate_" + identity.hex
    )
    async with _registration(connection, schema, request_id, 1, altered_target=partial):
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            with pytest.raises(resume.RegistryCompositionResumeError, match="request_scope_changed"):
                await resume.resolve_registered_contribution_resume(connection, request_id, 7, 9, control_schema=schema)
        async with _registration(connection, schema, request_id, 1):
            async with connection.transaction(isolation="repeatable_read", readonly=True):
                with pytest.raises(resume.RegistryCompositionResumeError, match="ambiguous"):
                    await resume.resolve_registered_contribution_resume(
                        connection, request_id, 7, 9, control_schema=schema
                    )


async def test_native_final_guard_preserves_unversioned_manual_target(serving_schema):
    from process.registry_candidate_composition import _target

    connection, schema, _ = serving_schema
    request_id = uuid4()
    copy_target = _target(request_id, 7, 9, {"custom_membership": "b" * 64})
    try:
        async with connection.transaction(isolation="repeatable_read"):
            await create_network_candidate(
                connection,
                copy_target,
                source_generations={"custom_membership": "b" * 64},
                approved_custom_revision=7,
                expected_head=9,
                expected_rows=0,
                control_schema=schema,
            )
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            assert (
                await resume.require_registered_composition_target(
                    connection, request_id, 7, 9, copy_target, control_schema=schema
                )
                is None
            )
            sibling = _target(request_id, 7, 9, {"custom_membership": "c" * 64})
            with pytest.raises(resume.RegistryCompositionResumeError, match="target_changed"):
                await resume.require_registered_composition_target(
                    connection, request_id, 7, 9, sibling, control_schema=schema
                )
            with pytest.raises(resume.RegistryCompositionResumeError, match="unversioned"):
                await resume.resolve_registered_contribution_resume(connection, request_id, 7, 9, control_schema=schema)
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')
        await connection.execute(
            f'DELETE FROM "{schema}".network_membership_candidate WHERE candidate_id=$1', UUID(copy_target.candidate_id)
        )
