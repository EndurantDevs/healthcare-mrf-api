"""Existing typed identities seed missing drafts without allocating or publishing."""

import hashlib
import json
from dataclasses import replace
from uuid import uuid4

import pytest

from process.network_catalog_store import (
    ExistingPlanTierCatalogDraftRow,
    NetworkCatalogPreimage,
    NetworkCatalogSeedError,
    NetworkCatalogSourceContext,
    seed_network_catalog_drafts,
)
from process.network_registry_identity import allocate_network_ids
from process.registry_fhir_catalog_identity import ALIAS_SCOPE, FHIRNetworkIdentityInput, _allocation_key
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from tests.test_network_legacy_alias_adoption_postgres import (
    _CountedConnection,
    _ready,
    _row,
    _state,
)
from tests.test_network_legacy_alias_adoption_postgres import (
    permissions_db as permissions_db,
)
from tests.test_network_legacy_alias_adoption_postgres import (
    provider_db as provider_db,
)
from tests.test_network_legacy_alias_adoption_postgres import (
    serving_schema as serving_schema,
)
from tests.test_registry_management_permissions_postgres import _arguments
from tests.test_registry_record_store_postgres import _actor

pytestmark = pytest.mark.asyncio


async def _existing_rows(fixture, count=700):
    endpoint, generation = "a" * 64, "b" * 64
    keys = [hashlib.sha256(f"synthetic-tier-{number}".encode()).hexdigest() for number in range(count)]
    allocations = [
        _allocation_key(FHIRNetworkIdentityInput(key, endpoint, (UHC_PROVIDER_FILE_SOURCE_ID,), "proof"), "plan_tier")
        for key in keys
    ]
    async with fixture.sessions() as session, session.begin():
        ids = await allocate_network_ids(session, allocations, schema=fixture.schema)
    draft_rows = [
        ExistingPlanTierCatalogDraftRow(
            ids[allocation],
            allocation,
            endpoint,
            key,
            generation + ":UHCPlanTier/" + key,
            f"Synthetic Tier {number}",
            (f"Synthetic Alias {number}",),
        )
        for number, (key, allocation) in enumerate(zip(keys, allocations, strict=True))
    ]
    await fixture.connection.copy_records_to_table(
        "network_registry_alias",
        schema_name=fixture.schema,
        columns=("source_system", "source_id", "alias_type", "alias_value", "scope_key", "network_id", "evidence_id"),
        records=[
            (
                "fhir",
                UHC_PROVIDER_FILE_SOURCE_ID,
                "fhir_plan_tier",
                draft_row.network_key_id,
                ALIAS_SCOPE,
                draft_row.network_id,
                draft_row.evidence_id,
            )
            for draft_row in draft_rows
        ],
    )
    await fixture.connection.execute(
        f'UPDATE "{fixture.schema}".network_serving_control SET metadata_generation=$1', "c" * 64
    )
    return draft_rows


async def _context(fixture):
    revision = await fixture.connection.fetchrow(
        f'SELECT draft_revision,approved_revision FROM "{fixture.schema}".registry_revision_control'
    )
    return NetworkCatalogSourceContext(
        "fhir",
        UHC_PROVIDER_FILE_SOURCE_ID,
        ALIAS_SCOPE,
        "Reviewed synthetic typed names",
        "typed-catalog",
        NetworkCatalogPreimage(revision["draft_revision"], revision["approved_revision"], 0, "c" * 64),
    )


async def _seed(fixture, draft_rows, actor, context, connection=None):
    caller = await fixture.connection.fetchval("SELECT current_user")
    async with fixture.connection.transaction():
        receipt = await seed_network_catalog_drafts(
            connection or fixture.connection, draft_rows, actor, context, **_arguments(fixture)
        )
        assert await fixture.connection.fetchval("SELECT current_user") == caller
    return receipt


async def test_all_700_native_batch_parity_history_and_idempotency(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    draft_rows = await _existing_rows(fixture)
    context = await _context(fixture)
    before = await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".network_registry_identity')
    counted = _CountedConnection(fixture.connection)
    first = await _seed(fixture, draft_rows, actor, context, counted)
    assert (first["requested_records"], first["created_records"], first["preserved_records"]) == (700, 700, 0)
    assert counted.calls.count("copy_records_to_table") == 1
    assert first["draft_revision"] == 1 and first["approved_revision"] == 0
    heads = await fixture.connection.fetch(
        f'SELECT network_id,display_name,aliases::text FROM "{fixture.schema}".network_registry_record'
    )
    assert {
        draft_row["network_id"]: (draft_row["display_name"], json.loads(draft_row["aliases"])) for draft_row in heads
    } == {draft_row.network_id: (draft_row.display_name, list(draft_row.aliases)) for draft_row in draft_rows}
    histories = await fixture.connection.fetch(
        f'SELECT custom_revision,revision,request_sha256,actor_json::text FROM "{fixture.schema}".registry_record_history'
    )
    assert len(histories) == 700 and all(
        draft_row["custom_revision"] == draft_row["revision"] == 1 for draft_row in histories
    )
    assert {draft_row["request_sha256"] for draft_row in histories} == {first["request_sha256"]}
    assert all(json.loads(draft_row["actor_json"])["user_id"] == str(actor.user_id) for draft_row in histories)
    retained = await _state(fixture)
    replay = await _seed(fixture, draft_rows, actor, context)
    assert replay == {**first, "created_records": 0, "preserved_records": 700}
    assert await _state(fixture) == retained
    with pytest.raises(NetworkCatalogSeedError):
        await _seed(fixture, draft_rows, actor, replace(context, reason="Changed retry"))
    assert await _state(fixture) == retained
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".network_registry_identity')
        == before
    )


async def _invalidate_typed_row(fixture, draft_rows, invalid):
    if invalid == "missing_alias":
        await fixture.connection.execute(
            f'DELETE FROM "{fixture.schema}".network_registry_alias WHERE network_id=$1', draft_rows[1].network_id
        )
        return
    if invalid == "alias_evidence":
        draft_rows[1] = replace(draft_rows[1], evidence_id="d" * 64 + ":UHCPlanTier/" + draft_rows[1].network_key_id)
        return
    if invalid == "identity_key":
        await fixture.connection.execute(
            f'UPDATE "{fixture.schema}".network_registry_identity SET allocation_key=$1 WHERE network_id=$2',
            uuid4(),
            draft_rows[1].network_id,
        )
        return
    if invalid == "mixed":
        draft_rows[1] = _row()
        return
    if invalid == "wrong_key":
        draft_rows[1] = replace(draft_rows[1], allocation_key=draft_rows[0].allocation_key)
        return
    draft_rows[1] = draft_rows[0]


@pytest.mark.parametrize(
    "invalid", ["missing_alias", "alias_evidence", "identity_key", "mixed", "wrong_key", "duplicate"]
)
async def test_one_bad_typed_row_refuses_whole_native_batch(permissions_db, invalid):
    fixture = permissions_db
    await _ready(fixture)
    draft_rows = await _existing_rows(fixture, 3)
    await _invalidate_typed_row(fixture, draft_rows, invalid)
    retained = await _state(fixture)
    with pytest.raises(NetworkCatalogSeedError):
        await _seed(fixture, draft_rows, _actor(), await _context(fixture))
    assert await _state(fixture) == retained


@pytest.mark.parametrize(
    "changed", ["draft_revision", "approved_revision", "metadata_generation", "serving_generation"]
)
async def test_stale_native_control_preimage_refuses(permissions_db, changed):
    fixture = permissions_db
    await _ready(fixture)
    draft_rows = await _existing_rows(fixture, 2)
    context = await _context(fixture)
    if changed == "serving_generation":
        context = replace(context, expected_preimage=replace(context.expected_preimage, serving_generation=1))
    elif changed == "metadata_generation":
        await fixture.connection.execute(
            f'UPDATE "{fixture.schema}".network_serving_control SET metadata_generation=$1', "d" * 64
        )
    else:
        await fixture.connection.execute(
            f'UPDATE "{fixture.schema}".registry_revision_control SET draft_revision=1,approved_revision=$1',
            int(changed == "approved_revision"),
        )
    retained = await _state(fixture)
    with pytest.raises(NetworkCatalogSeedError):
        await _seed(fixture, draft_rows, _actor(), context)
    assert await _state(fixture) == retained


async def test_typed_seed_outer_rollback_and_constant_statement_count(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    draft_rows = await _existing_rows(fixture, 701)
    retained = await _state(fixture)
    with pytest.raises(RuntimeError, match="outer rollback"):
        async with fixture.connection.transaction():
            await seed_network_catalog_drafts(
                fixture.connection, draft_rows, actor, await _context(fixture), **_arguments(fixture)
            )
            raise RuntimeError("outer rollback")
    assert await _state(fixture) == retained
    calls = []
    for subset, key in ((draft_rows[:1], "typed-one"), (draft_rows[1:], "typed-seven-hundred")):
        counted = _CountedConnection(fixture.connection)
        context = replace(await _context(fixture), idempotency_key=key)
        receipt = await _seed(fixture, subset, actor, context, counted)
        assert receipt["created_records"] == len(subset)
        calls.append(counted.calls)
    assert calls[0] == calls[1] and calls[1].count("copy_records_to_table") == 1


async def test_fresh_typed_seed_preserves_heads_and_refuses_dangling_history(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _ready(fixture)
    draft_rows = await _existing_rows(fixture, 3)
    await _seed(fixture, draft_rows[:2], actor, await _context(fixture))
    await fixture.connection.execute(
        f'UPDATE "{fixture.schema}".network_registry_record SET display_name=$1,archived=true WHERE network_id=$2',
        "Reviewed manual name",
        draft_rows[0].network_id,
    )
    context = replace(await _context(fixture), idempotency_key="next-catalog")
    receipt = await _seed(fixture, draft_rows, actor, context)
    assert receipt["created_records"] == 1 and receipt["preserved_records"] == 2
    preserved = await fixture.connection.fetchrow(
        f'SELECT display_name,archived FROM "{fixture.schema}".network_registry_record WHERE network_id=$1',
        draft_rows[0].network_id,
    )
    assert dict(preserved) == {"display_name": "Reviewed manual name", "archived": True}
    await fixture.connection.execute(
        f'DELETE FROM "{fixture.schema}".network_registry_record WHERE network_id=$1', draft_rows[1].network_id
    )
    retained = await _state(fixture)
    context = replace(await _context(fixture), idempotency_key="dangling-catalog")
    with pytest.raises(NetworkCatalogSeedError):
        await _seed(fixture, draft_rows, actor, context)
    assert await _state(fixture) == retained
