# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Connected metadata registration; 101 recipes do not imply admitted Sources or publication readiness."""

from dataclasses import replace
from uuid import UUID, uuid4

import pytest

from process import registry_source_inventory_store as store
from process.network_membership_candidate_lifecycle import MembershipCandidateError, create_network_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_publication import NetworkPublicationError, _publication_readiness
from process.registry_source_inventory_contract import RegistrySourceInventoryError, source_inventory_root
from process.registry_source_inventory_reference import SourceInventoryReference, verify_source_recipe_metadata
from process.registry_source_recipe_store import RECIPE_DIGEST_KEY
from tests.test_registry_source_inventory_contract import _entries
from tests.test_registry_source_inventory_store_postgres import (
    _asynchronous_entries,
    _new_target,
    _verify,
    _write,
)
from tests.test_registry_source_inventory_store_postgres import (
    custom_db as custom_db,
)
from tests.test_registry_source_inventory_store_postgres import (
    inventory_db as inventory_db,
)
from tests.test_registry_source_inventory_store_postgres import (
    serving_schema as serving_schema,
)
from tests.test_registry_source_recipe_store_postgres import _example


def _unregistered_target(context):
    identity = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(identity), "network_candidate_" + identity.hex
    )
    context.targets.append(copy_target)
    return copy_target


async def _register(context, copy_target, reference, generations=None):
    async with context.fixture.connection.transaction():
        return await create_network_candidate(
            context.fixture.connection,
            copy_target,
            source_generations={} if generations is None else generations,
            approved_custom_revision=0,
            expected_head=0,
            expected_rows=0,
            control_schema=context.fixture.control_schema,
            source_inventory=reference,
        )


async def _candidate(context, copy_target):
    return dict(
        await context.fixture.connection.fetchrow(
            f'SELECT * FROM "{context.fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1',
            UUID(copy_target.candidate_id),
        )
    )


async def _physical_state(context, copy_target):
    return tuple(
        await context.fixture.connection.fetchrow(
            f"SELECT c.xmin::text,c.ctid::text,to_jsonb(c)::text,to_regclass($2)::oid::bigint,"
            f'to_regclass($3)::oid::bigint FROM "{context.fixture.control_schema}".network_membership_candidate c '
            "WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            copy_target.schema_name + ".network_membership",
            copy_target.schema_name + "." + store.INVENTORY_TABLE,
        )
    )


async def _assert_early_refusals(context, copy_target, entries, reference):
    before = await _physical_state(context, copy_target)
    assert before[-1] is None
    changed_root = replace(reference.root, entries_sha256="0" * 64)
    assert changed_root != reference.root
    with pytest.raises(MembershipCandidateError, match="immutable"):
        await _register(context, copy_target, SourceInventoryReference(changed_root))
    with pytest.raises(RegistrySourceInventoryError, match="root_changed"):
        async with context.fixture.connection.transaction():
            await store.write_source_inventory(
                context.fixture.connection,
                copy_target,
                _asynchronous_entries(entries),
                expected_root=changed_root,
                control_schema=context.fixture.control_schema,
            )
    assert await _physical_state(context, copy_target) == before
    rejected = _unregistered_target(context)
    with pytest.raises(MembershipCandidateError, match="source recipes"):
        await _register(context, rejected, reference, {RECIPE_DIGEST_KEY: "0" * 64})
    assert not await context.fixture.connection.fetchval(
        f'SELECT EXISTS(SELECT FROM "{context.fixture.control_schema}".network_membership_candidate WHERE candidate_id=$1)',
        UUID(rejected.candidate_id),
    )
    assert await context.fixture.connection.fetchval("SELECT to_regnamespace($1)", rejected.schema_name) is None


async def _assert_exact_create_replay(context, copy_target, reference):
    before = await _physical_state(context, copy_target)
    assert before[-1] is not None
    await _register(context, copy_target, reference)
    assert await _physical_state(context, copy_target) == before


async def _assert_unpublished_boundary(context, copy_target):
    candidate = await _candidate(context, copy_target)
    assert candidate["state"] == "open"
    with pytest.raises(NetworkPublicationError, match="pinned validation report"):
        _publication_readiness(candidate)
    for table in ("network_serving_manifest", "network_candidate_custody"):
        assert (
            await context.fixture.connection.fetchval(
                f'SELECT count(*) FROM "{context.fixture.control_schema}".{table} WHERE candidate_id=$1',
                UUID(copy_target.candidate_id),
            )
            == 0
        )


@pytest.mark.asyncio
async def test_inventory_registration_copy_reader_and_exact_replay_preserve_metadata_only_boundary(inventory_db):
    context = inventory_db
    entries = _entries(_example(index) for index in range(101))
    root = source_inventory_root(entries)
    assert root.entry_count == 101 and root.contribution_count == root.derivation_count == 0
    reference = SourceInventoryReference(root)
    copy_target = await _new_target(context, entries)
    metadata = verify_source_recipe_metadata(await _candidate(context, copy_target))
    assert metadata.recipe_count == 101 and metadata.recipes_sha256 == root.recipes_sha256
    assert metadata.inventory == reference
    await _assert_early_refusals(context, copy_target, entries, reference)
    receipt = await _write(context, copy_target, entries, row_limit=7, byte_limit=4096)
    assert await _verify(context, copy_target, receipt, row_limit=13, byte_limit=4096) == root
    await _assert_exact_create_replay(context, copy_target, reference)
    await _assert_unpublished_boundary(context, copy_target)
