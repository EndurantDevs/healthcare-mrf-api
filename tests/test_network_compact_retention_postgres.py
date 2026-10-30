# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real independent seals retain address/edge artifacts through rollback and rotation."""

import asyncio
import json
from uuid import UUID

import pytest

from process import network_candidate_custody as custody
from process import network_compact_retention as retention
from tests.test_network_address_artifact_postgres import address_db
from tests.test_network_compact_publication_postgres import (
    _candidate,
    _prepare,
    _publish,
    _seal,
    compact_publication_db,
)


@pytest.fixture
async def sealed_compact(compact_publication_db):
    fixture = compact_publication_db
    target = await _candidate(fixture, (42, 88), 0)
    await _prepare(fixture, target)
    seal = await _seal(fixture, target)
    return fixture, target, seal


async def _guard(fixture, oids):
    async with fixture.connection.transaction(isolation="read_committed"):
        return await retention.require_network_compact_artifacts_unreferenced(
            fixture.connection, oids, control_schema=fixture.control_schema
        )


@pytest.mark.parametrize("state", ["ready", "published", "ineligible"])
async def test_sealed_candidates_retain_rollback_dependencies(sealed_compact, state):
    fixture, target, seal = sealed_compact
    if state != "ready":
        publication = await _publish(fixture, target)
        if state == "ineligible":
            await fixture.connection.execute(
                f'UPDATE "{fixture.control_schema}".network_serving_manifest SET eligible=false WHERE generation_id=$1',
                publication["generation_id"],
            )
    source_oid = fixture.address.descriptor["table"]["oid"]
    edge_oid = seal["receipt"]["closure"]["relation_oids"]["network_office_edge"]
    for oid in (source_oid, edge_oid, fixture.address.descriptor["authority_table_oid"]):
        with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
            await _guard(fixture, [oid])
    unrelated = await fixture.connection.fetchval("SELECT 'pg_catalog.pg_class'::regclass::oid::bigint")
    assert await _guard(fixture, [unrelated]) is None


async def _alter_candidate_metadata(fixture, copy_target, damage, original):
    connection = fixture.connection
    namespace = '"' + fixture.control_schema + '"'
    if damage in {"erased_descriptor", "erased_component"}:
        report = json.loads(original["report"])
        if damage == "erased_descriptor":
            report["serving_readiness"].pop("address_artifact")
        else:
            report["serving_readiness"]["component"] = "legacy"
        await connection.execute(
            f"UPDATE {namespace}.network_membership_candidate SET validation_json=$2::jsonb WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            json.dumps(report),
        )
    elif damage == "seal_digest":
        await connection.execute(
            f"UPDATE {namespace}.network_candidate_custody SET seal_sha256=$2 WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            "0" * 64,
        )
    elif damage == "ledger_owner":
        await connection.execute(
            f'ALTER TABLE {namespace}.network_candidate_custody OWNER TO "{fixture.address.roles["outsider"]}"'
        )
    elif damage == "missing_ready_seal":
        await connection.execute(
            f"DELETE FROM {namespace}.network_candidate_custody WHERE candidate_id=$1", UUID(copy_target.candidate_id)
        )
    else:
        await connection.execute(
            f"UPDATE {namespace}.network_membership_candidate SET state='rejected' WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
        )


@pytest.mark.parametrize(
    "damage",
    ["erased_descriptor", "erased_component", "seal_digest", "ledger_owner", "missing_ready_seal", "demoted_state"],
)
async def test_mutable_metadata_cannot_erase_a_sealed_pin(sealed_compact, damage):
    fixture, copy_target, _ = sealed_compact
    connection = fixture.connection
    namespace = '"' + fixture.control_schema + '"'
    original = await connection.fetchrow(
        f"SELECT state,validation_json::text AS report FROM {namespace}.network_membership_candidate WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
    )
    stored = await connection.fetchrow(
        f"SELECT * FROM {namespace}.network_candidate_custody WHERE candidate_id=$1", UUID(copy_target.candidate_id)
    )
    owner = await connection.fetchval(
        "SELECT quote_ident(r.rolname) FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner WHERE c.oid=to_regclass($1)",
        namespace + ".network_candidate_custody",
    )
    try:
        await _alter_candidate_metadata(fixture, copy_target, damage, original)
        with pytest.raises((retention.NetworkCompactRetentionError, custody.NetworkCandidateCustodyError)):
            await _guard(fixture, [fixture.address.descriptor["table"]["oid"]])
    finally:
        await connection.execute(f"ALTER TABLE {namespace}.network_candidate_custody OWNER TO {owner}")
        await connection.execute(
            f"UPDATE {namespace}.network_membership_candidate SET state=$2,validation_json=$3::jsonb WHERE candidate_id=$1",
            UUID(copy_target.candidate_id),
            original["state"],
            original["report"],
        )
        await connection.execute(
            f"INSERT INTO {namespace}.network_candidate_custody(candidate_id,seal_sha256,receipt_json,created_at) VALUES($1,$2,$3::jsonb,$4) ON CONFLICT(candidate_id) DO UPDATE SET seal_sha256=excluded.seal_sha256,receipt_json=excluded.receipt_json",
            stored["candidate_id"],
            stored["seal_sha256"],
            stored["receipt_json"],
            stored["created_at"],
        )


async def test_missing_published_seal_is_not_retirement(sealed_compact):
    fixture, target, _ = sealed_compact
    await _publish(fixture, target)
    namespace = '"' + fixture.control_schema + '"'
    row = await fixture.connection.fetchrow(
        f"SELECT * FROM {namespace}.network_candidate_custody WHERE candidate_id=$1", UUID(target.candidate_id)
    )
    try:
        await fixture.connection.execute(
            f"DELETE FROM {namespace}.network_candidate_custody WHERE candidate_id=$1", UUID(target.candidate_id)
        )
        with pytest.raises(retention.NetworkCompactRetentionError, match="published_seal_missing"):
            await _guard(fixture, [fixture.address.descriptor["table"]["oid"]])
    finally:
        await fixture.connection.execute(
            f"INSERT INTO {namespace}.network_candidate_custody(candidate_id,seal_sha256,receipt_json,created_at) VALUES($1,$2,$3::jsonb,$4)",
            row["candidate_id"],
            row["seal_sha256"],
            row["receipt_json"],
            row["created_at"],
        )


async def test_retained_name_cannot_release_a_physical_pin(sealed_compact):
    fixture, _, _ = sealed_compact
    oid = fixture.address.descriptor["table"]["oid"]
    retained = f"entity_address_unified_retained_{oid:x}"
    await fixture.connection.execute(
        f'ALTER TABLE "{fixture.address.schema}".entity_address_unified RENAME TO "{retained}"'
    )
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint", f'"{fixture.address.schema}"."{retained}"'
        )
        == oid
    )
    with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
        await _guard(fixture, [oid])


async def test_second_check_sees_a_concurrently_committed_seal(compact_publication_db):
    fixture = compact_publication_db
    copy_target = await _candidate(fixture, (42, 88), 0)
    await _prepare(fixture, copy_target)
    oid = fixture.address.descriptor["table"]["oid"]
    first_check, committed = asyncio.Event(), asyncio.Event()

    async def cleanup():
        async with fixture.connection.transaction(isolation="read_committed"):
            await retention.require_network_compact_artifacts_unreferenced(
                fixture.connection, [oid], control_schema=fixture.control_schema
            )
            first_check.set()
            await committed.wait()
            await fixture.connection.execute(
                f'LOCK TABLE ONLY "{fixture.address.schema}".entity_address_unified IN ACCESS EXCLUSIVE MODE NOWAIT'
            )
            await retention.require_network_compact_artifacts_unreferenced(
                fixture.connection, [oid], control_schema=fixture.control_schema
            )
            await fixture.connection.execute(f'DROP TABLE "{fixture.address.schema}".entity_address_unified RESTRICT')

    operation = asyncio.create_task(cleanup())
    try:
        await asyncio.wait_for(first_check.wait(), 10)
        await _seal(fixture, copy_target)
    finally:
        committed.set()
    with pytest.raises(retention.NetworkCompactRetentionError, match="artifact_referenced"):
        await operation
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint", f'"{fixture.address.schema}".entity_address_unified'
        )
        == oid
    )


async def test_repeatable_read_cannot_hide_a_new_seal(compact_publication_db):
    fixture = compact_publication_db
    async with fixture.connection.transaction(isolation="repeatable_read"):
        with pytest.raises(retention.NetworkCompactRetentionError, match="fresh_snapshot_required"):
            await retention.require_network_compact_artifacts_unreferenced(
                fixture.connection, [fixture.address.descriptor["table"]["oid"]], control_schema=fixture.control_schema
            )


async def test_legacy_cleanup_without_registry_stays_available(address_db):
    async with address_db.admin.transaction(isolation="repeatable_read"):
        assert (
            await retention.require_network_compact_artifacts_unreferenced(
                address_db.admin, [address_db.descriptor["table"]["oid"]], control_schema="absent_registry"
            )
            is None
        )
