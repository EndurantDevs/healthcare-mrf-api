# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native private-only allocation data; no canonical finisher or authority proof."""

from copy import deepcopy

import pytest

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_prepared_allocation as allocation
from process import provider_directory_generic_source_storage as storage
from process.provider_directory_source_storage_binding import FHIRSourceCandidateHandoff
from tests.test_provider_directory_cms_prepared_candidate_postgres import (
    canonical_snapshot,
    unallocated_candidate_case,
)
from tests.test_provider_directory_cms_prepared_identity_publication_postgres import publication_case
from tests.test_provider_directory_cms_prepared_outputs_postgres import (
    full_private_case,
    native_case,
    native_derived_case,
    native_frozen_case,
    observe,
    prepare,
    publisher,
)
from tests.test_provider_directory_cms_prepared_payload_publication_postgres import payload_case

pytestmark = pytest.mark.asyncio


async def _canonical_state(case, connection):
    objects = await connection.fetch(
        "SELECT c.oid::bigint,c.relname,c.relkind::text,c.relowner::bigint,c.relfilenode::bigint,"
        "c.relpartbound::text,ARRAY(SELECT inhparent::bigint FROM pg_inherits WHERE inhrelid=c.oid ORDER BY inhparent) "
        "AS parents FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 ORDER BY c.oid",
        case.storage.catalog_schema,
    )
    return await canonical_snapshot(case, connection), objects


async def test_private_record_preserves_canonical_storage_and_original_publisher(unallocated_candidate_case):
    case = unallocated_candidate_case
    before = await _canonical_state(case, case.admin)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, _report):
            binding = workspace.binding
            transaction = await driver.fetchval("SELECT pg_current_xact_id()::text")
            result = await allocation.retain_prepared_cms_workspace(workspace, case.handoff)
            assert result == dict(
                contract=allocation.PRIVATE_ALLOCATION_CONTRACT, workspace=binding, transaction=transaction
            )
            assert await allocation.retain_prepared_cms_workspace(workspace, case.handoff) == result
            assert await workspace.verify_current() == binding["tables"]
            assert await driver.fetchval(
                "SELECT session_user=current_user AND current_user=$1", case.policy.publisher_role
            )
            assert await driver.fetchval("SELECT pg_current_xact_id()::text") == transaction
            assert await _canonical_state(case, driver) == before
            result["workspace"]["tables"].clear()
            assert workspace.binding == binding
    assert await _canonical_state(case, case.admin) == before


@pytest.mark.parametrize("invalid", ["unindexed", "handoff", "assumed_owner"])
async def test_private_record_rejects_invalid_inputs_without_canonical_writes(unallocated_candidate_case, invalid):
    case = unallocated_candidate_case
    before = await _canonical_state(case, case.admin)
    async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
        async with prepare(case, session, driver, inputs) as (workspace, _report):
            handoff = case.handoff
            if invalid == "unindexed":
                workspace._indexed = False
            elif invalid == "handoff":
                document = deepcopy(handoff.document)
                document["identity"]["dataset_id"] = "another-dataset"
                handoff = FHIRSourceCandidateHandoff(storage._encoded(document))
            else:
                await driver.execute("SET LOCAL ROLE " + storage_identifier(case.policy.owner_role))
            try:
                with pytest.raises(ValueError, match="handoff_changed|original_publisher_changed"):
                    await allocation.retain_prepared_cms_workspace(workspace, handoff)
            finally:
                workspace._indexed = True
                await driver.execute("RESET ROLE")
            assert await _canonical_state(case, driver) == before
    assert await _canonical_state(case, case.admin) == before


async def test_private_record_does_not_commit_outer_preparation(unallocated_candidate_case):
    case = unallocated_candidate_case
    before = await _canonical_state(case, case.admin)
    binding = None
    with pytest.raises(RuntimeError, match="rollback-private-preparation"):
        async with publisher(case) as (session, driver), observe(case, session, driver) as inputs:
            async with prepare(case, session, driver, inputs) as (workspace, _report):
                binding = workspace.binding
                retained = await allocation.retain_prepared_cms_workspace(workspace, case.handoff)
                assert retained["workspace"] == binding and driver.is_in_transaction()
                raise RuntimeError("rollback-private-preparation")
    assert binding is not None
    for receipt in binding["tables"].values():
        assert (
            await case.admin.fetchval(
                "SELECT to_regclass($1)", storage_relation(case.storage.storage_schema, receipt["name"])
            )
            is None
        )
    assert await _canonical_state(case, case.admin) == before
