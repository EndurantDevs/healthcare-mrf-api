# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native component parity for typed slices; synthetic dispatch grants no authority."""

from uuid import UUID

import pytest

from process import registry_typed_membership_parity as parity
from process.network_compact_readiness import NetworkCompactReadinessError
from process.network_compact_slice_validation import verify_compact_slice_contents
from tests.test_network_compact_slice_validation_postgres import _layout
from tests.test_network_membership_slices_postgres import _candidate, _prepare, _refs, slice_db


async def _seed_memberships(db, copy_target, count):
    namespace = '"' + copy_target.schema_name + '"'
    await parity.create_order_index(
        db.connection, namespace + ".network_membership", "typed_effective_membership_order_idx"
    )
    await db.connection.execute(
        f"""INSERT INTO {namespace}.provider_location_binding
        SELECT 'npi','provider-extra-'||g,md5('office-extra-'||g)::uuid,'office-extra-'||g,'npi','provider-extra-'||g
        FROM generate_series(1,$1::integer) g""",
        count,
    )
    await db.connection.execute(
        f"""INSERT INTO {namespace}.network_membership
        SELECT 7+g%3,'npi','provider-extra-'||g,md5('office-extra-'||g)::uuid,'evidence-'||g
        FROM generate_series(1,$1::integer) g""",
        count,
    )
    # A full-key duplicate run crosses a native page boundary and must all be counted.
    await db.connection.execute(
        f"INSERT INTO {namespace}.network_membership SELECT 7,'npi','provider-extra-3',md5('office-extra-3')::uuid,'duplicate' FROM generate_series(1,7001)"
    )
    await db.connection.execute(
        f'UPDATE "{db.source_schema}".network_membership_candidate SET accepted_rows=$2,expected_rows=$2 WHERE candidate_id=$1',
        UUID(copy_target.candidate_id),
        count + 7001,
    )


@pytest.mark.asyncio
async def test_native_typed_slice_parity_duplicates_and_corruption(slice_db, monkeypatch):
    db, count = slice_db, 20001
    assert await db.connection.fetchval("SELECT current_setting('work_mem')") == "4MB"
    assert await db.connection.fetchval("SELECT current_setting('temp_file_limit')") == "256MB"
    await db.connection.execute(
        f"""INSERT INTO "{db.source_schema}".retained_addresses
        SELECT 'office-extra-'||g,'npi','provider-extra-'||g,'synthetic address' FROM generate_series(1,$1::integer) g""",
        count,
    )
    legacy, typed = await _candidate(db, []), await _candidate(db, [])
    for copy_target in (legacy, typed):
        await _seed_memberships(db, copy_target, count)
    async with db.connection.transaction():
        legacy_receipt = await _prepare(db, legacy)
    monkeypatch.setattr(parity, "typed_candidate", lambda _candidate: True)
    async with db.connection.transaction():
        typed_receipt = await _prepare(db, typed)
        layout = await _layout(db, typed, typed_receipt)
        await verify_compact_slice_contents(db.connection, typed, layout, candidate={})
    fields = ("network_id", "office_bucket", "row_count", "content_sha256")
    expected_by_key = {
        key: tuple(slice_ref[name] for name in fields) for key, slice_ref in (await _refs(db, legacy)).items()
    }
    actual_by_key = {
        key: tuple(slice_ref[name] for name in fields) for key, slice_ref in (await _refs(db, typed)).items()
    }
    assert actual_by_key == expected_by_key
    for receipt in (legacy_receipt, typed_receipt):
        assert receipt.membership_rows == count + 7001 and receipt.distinct_memberships == count
        assert receipt.matched_locations == count and receipt.copied_edge_rows == count
    namespace = '"' + typed.schema_name + '"'
    mutations = (
        f"UPDATE {namespace}.network_slice_ref SET content_sha256=repeat('a',64)",
        f"UPDATE {namespace}.network_slice_ref SET row_count=row_count+1",
        f"UPDATE {namespace}.network_slice_ref SET network_id=99999,office_bucket=(office_bucket+1)%64 WHERE (network_id,office_bucket)=(SELECT network_id,office_bucket FROM {namespace}.network_slice_ref ORDER BY network_id,office_bucket LIMIT 1)",
        f"DELETE FROM {namespace}.network_office_edge WHERE location_key='office-extra-1'",
        f"INSERT INTO {namespace}.network_office_edge VALUES('11111111-1111-4111-8111-111111111111','extra-office')",
    )
    for mutation in mutations:
        transaction = db.connection.transaction()
        await transaction.start()
        try:
            await db.connection.execute(mutation)
            with pytest.raises(NetworkCompactReadinessError):
                await verify_compact_slice_contents(db.connection, typed, layout, candidate={})
        finally:
            await transaction.rollback()
    async with db.connection.transaction():
        await verify_compact_slice_contents(db.connection, typed, layout, candidate={})
