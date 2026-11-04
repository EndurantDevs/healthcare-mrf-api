# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent slice checks reject stale hashes, misbound reuse and missing indexes."""

import asyncio
from uuid import UUID, uuid4

import pytest

from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_compact_readiness import NetworkCompactReadinessError, compact_index_digest
from process.network_compact_slice_validation import verify_compact_slice_contents
from process.network_compact_validation import _actual_memberships, _membership_sets_differ
from tests.test_network_membership_slices_postgres import _candidate, _pin, _prepare, slice_db


async def _layout(db, target, receipt, artifacts=None):
    oid = await db.connection.fetchval("SELECT to_regclass($1)::oid", db.source_schema + ".retained_addresses")
    own = CompactEdgeArtifact(target.candidate_id, target.schema_name, receipt.edge_heap_oid)
    return CompactNetworkLayout(
        CompactAddressRelation(target.schema_name, "network_slice_ref", receipt.ref_map_oid),
        CompactAddressRelation(db.source_schema, "retained_addresses", oid),
        (own,) if artifacts is None else tuple(sorted(artifacts, key=lambda artifact: artifact.candidate_id)),
    )


@pytest.mark.parametrize("corruption", ["digest", "count", "key", "extra", "missing", "bucket"])
async def test_independent_new_slice_verification_rejects_corruption(slice_db, corruption):
    db = slice_db
    target = await _candidate(db, [(42, 0), (7, 0)])
    async with db.connection.transaction():
        receipt = await _prepare(db, target)
        layout = await _layout(db, target, receipt)
        await verify_compact_slice_contents(db.connection, target, layout)
    ns = '"' + target.schema_name + '"'
    mutation_sql_by_kind = {
        "digest": f"UPDATE {ns}.network_slice_ref SET content_sha256=repeat('a',64)",
        "count": f"UPDATE {ns}.network_slice_ref SET row_count=row_count+1",
        "key": f"UPDATE {ns}.network_office_edge SET location_key='different-office'",
        "missing": f"DELETE FROM {ns}.network_office_edge",
        "bucket": f"UPDATE {ns}.network_slice_ref SET office_bucket=(office_bucket+1)%64",
    }
    if corruption == "extra":
        await db.connection.execute(f"INSERT INTO {ns}.network_office_edge VALUES($1,'extra-office')", uuid4())
    else:
        await db.connection.execute(mutation_sql_by_kind[corruption])
    async with db.connection.transaction():
        with pytest.raises(NetworkCompactReadinessError):
            await verify_compact_slice_contents(db.connection, target, layout)


async def test_reused_slice_metadata_must_match_its_sealed_origin(slice_db):
    db = slice_db
    first = await _candidate(db, [(42, 0)])
    async with db.connection.transaction():
        first_receipt = await _prepare(db, first)
    second = await _candidate(db, [(42, 0), (7, 1)])
    async with db.connection.transaction():
        receipt = await _prepare(db, second, previous=_pin(first, first_receipt))
        layout = await _layout(
            db,
            second,
            receipt,
            (
                CompactEdgeArtifact(first.candidate_id, first.schema_name, first_receipt.edge_heap_oid),
                CompactEdgeArtifact(second.candidate_id, second.schema_name, receipt.edge_heap_oid),
            ),
        )
        await verify_compact_slice_contents(db.connection, second, layout)
    await db.connection.execute(
        f"UPDATE \"{second.schema_name}\".network_slice_ref SET content_sha256=repeat('a',64) WHERE artifact_candidate_id=$1",
        UUID(first.candidate_id),
    )
    async with db.connection.transaction():
        with pytest.raises(NetworkCompactReadinessError):
            await verify_compact_slice_contents(db.connection, second, layout)


async def test_compact_index_identity_requires_native_inverse_lookup(slice_db):
    db = slice_db
    target = await _candidate(db, [])
    async with db.connection.transaction():
        await _prepare(db, target)
    first = await compact_index_digest(db.connection, target.schema_name)
    assert len(first) == 64
    await db.connection.execute(f'DROP INDEX "{target.schema_name}".network_office_edge_location_idx')
    with pytest.raises(NetworkCompactReadinessError):
        await compact_index_digest(db.connection, target.schema_name)


async def _set_candidate(db, pairs):
    target = await _candidate(db, pairs)
    namespace = '"' + target.schema_name + '"'
    await db.connection.execute(
        f"UPDATE {namespace}.provider_location_binding SET location_key=$1 WHERE location_id=$2",
        db.keys[0],
        db.location_ids[1],
    )
    await db.connection.execute(f"CREATE INDEX ON {namespace}.provider_location_binding USING hash(location_key)")
    await db.connection.execute(
        f"CREATE INDEX ON {namespace}.network_membership(provider_system,provider_id,location_id,network_id)"
    )
    return target


async def _reference_set_difference(connection, namespace, layout):
    return await connection.fetchval(
        f"""WITH expected AS (
          SELECT DISTINCT m.network_id,b.location_key FROM {namespace}.network_membership m
          JOIN {namespace}.provider_location_binding b USING(provider_system,provider_id,location_id)
        ), actual AS ({_actual_memberships(layout)})
        SELECT EXISTS((SELECT * FROM expected EXCEPT SELECT * FROM actual)
          UNION ALL (SELECT * FROM actual EXCEPT SELECT * FROM expected))""",
        *(UUID(artifact.candidate_id) for artifact in layout.artifacts),
    )


@pytest.mark.parametrize("reuse", [False, True], ids=["generic", "retained-and-new"])
@pytest.mark.parametrize("corruption", [None, "missing", "extra", "network"])
async def test_indexed_compact_set_parity_keeps_aliases_evidence_and_selected_reuse(slice_db, reuse, corruption):
    db = slice_db
    pairs = [(42, 0), (42, 0), (42, 1), (7, 1), (88, 2)]
    prior, artifacts = None, []
    if reuse:
        first = await _set_candidate(db, [*pairs, (99, 3)])
        async with db.connection.transaction():
            first_receipt = await _prepare(db, first)
        prior = _pin(first, first_receipt)
        artifacts.append(CompactEdgeArtifact(first.candidate_id, first.schema_name, first_receipt.edge_heap_oid))
    copy_target = await _set_candidate(db, pairs + ([(21, 3)] if reuse else []))
    async with db.connection.transaction():
        receipt = await _prepare(db, copy_target, previous=prior)
        artifacts.append(CompactEdgeArtifact(copy_target.candidate_id, copy_target.schema_name, receipt.edge_heap_oid))
        layout = await _layout(db, copy_target, receipt, artifacts)
        await verify_compact_slice_contents(db.connection, copy_target, layout)
    namespace = '"' + copy_target.schema_name + '"'
    selected = await db.connection.fetchrow(
        f"SELECT artifact_candidate_id,slice_id FROM {namespace}.network_slice_ref WHERE network_id=42"
    )
    edge = '"network_candidate_' + selected["artifact_candidate_id"].hex + '".network_office_edge'
    if corruption == "missing":
        await db.connection.execute(f"DELETE FROM {edge} WHERE slice_id=$1", selected["slice_id"])
    elif corruption == "extra":
        await db.connection.execute(f"INSERT INTO {edge} VALUES($1,'extra-office')", selected["slice_id"])
    elif corruption == "network":
        await db.connection.execute(f"UPDATE {namespace}.network_slice_ref SET network_id=43 WHERE network_id=42")
    async with db.connection.transaction(isolation="repeatable_read", readonly=True):
        is_different = corruption is not None
        assert await _reference_set_difference(db.connection, namespace, layout) is is_different
        assert await _membership_sets_differ(db.connection, namespace, layout) is is_different


async def test_indexed_compact_set_parity_accepts_empty_artifacts(slice_db):
    db = slice_db
    target = await _set_candidate(db, [])
    async with db.connection.transaction():
        receipt = await _prepare(db, target)
        layout = await _layout(db, target, receipt, ())
        await verify_compact_slice_contents(db.connection, target, layout)
    namespace = '"' + target.schema_name + '"'
    async with db.connection.transaction(isolation="repeatable_read", readonly=True):
        assert await _reference_set_difference(db.connection, namespace, layout) is False
        assert await _membership_sets_differ(db.connection, namespace, layout) is False


async def test_compact_set_parity_preserves_readonly_without_reverse_index(slice_db):
    db = slice_db
    target = await _candidate(db, [(42, 0), (42, 0), (7, 1)])
    async with db.connection.transaction():
        receipt = await _prepare(db, target)
        layout = await _layout(db, target, receipt)
    namespace = '"' + target.schema_name + '"'
    for has_extra in (False, True):
        if has_extra:
            await db.connection.execute(f"""INSERT INTO {namespace}.network_office_edge
                SELECT slice_id,'extra-office' FROM {namespace}.network_slice_ref WHERE network_id=42""")
        async with db.connection.transaction(isolation="repeatable_read", readonly=True):
            assert await _reference_set_difference(db.connection, namespace, layout) is has_extra
            queries = []
            db.connection.add_query_logger(queries.append)
            try:
                assert await _membership_sets_differ(db.connection, namespace, layout) is has_extra
                await asyncio.sleep(0)
            finally:
                db.connection.remove_query_logger(queries.append)
            assert any(" EXCEPT " in query.query for query in queries)
