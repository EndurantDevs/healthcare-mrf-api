# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent slice checks reject stale hashes, misbound reuse and missing indexes."""

from uuid import UUID, uuid4

import pytest

from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_compact_readiness import NetworkCompactReadinessError, compact_index_digest
from process.network_compact_slice_validation import verify_compact_slice_contents
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
