# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native component parity for typed slices; synthetic dispatch grants no authority."""

import re
from uuid import UUID

import pytest

from process import network_fhir_typed_sql as typed_sql
from process import network_typed_membership_accounting as accounting
from process import registry_typed_membership_parity as parity
from process.network_compact_readiness import NetworkCompactReadinessError
from process.network_compact_slice_validation import verify_compact_slice_contents
from process.provider_directory_projection_fhir_values import is_valid_npi
from tests.test_network_compact_slice_validation_postgres import _layout
from tests.test_network_membership_slices_postgres import _candidate, _prepare, _refs
from tests.test_network_membership_slices_postgres import slice_db as slice_db


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
    monkeypatch.setattr(parity, "is_typed_candidate", lambda _candidate: True)
    async with db.connection.transaction():
        typed_receipt = await _prepare(db, typed)
        layout = await _layout(db, typed, typed_receipt)
        await verify_compact_slice_contents(db.connection, typed, layout)
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
        f"DELETE FROM {namespace}.network_office_edge WHERE location_key='office-extra-1'",
        f"INSERT INTO {namespace}.network_office_edge VALUES('11111111-1111-4111-8111-111111111111','extra-office')",
    )
    for mutation in mutations:
        transaction = db.connection.transaction()
        await transaction.start()
        try:
            await db.connection.execute(mutation)
            with pytest.raises(NetworkCompactReadinessError):
                await verify_compact_slice_contents(db.connection, typed, layout)
        finally:
            await transaction.rollback()
    async with db.connection.transaction():
        await verify_compact_slice_contents(db.connection, typed, layout)
    lookup_ready = await db.connection.fetchval(
        """SELECT indisvalid AND indisready AND indislive FROM pg_index
        WHERE indexrelid=to_regclass($1)""",
        f'"{typed.schema_name}".network_slice_ref_slice_lookup_idx',
    )
    assert lookup_ready is True


@pytest.mark.asyncio
async def test_location_key_binding_audit_catches_multipage_orphans_and_duplicates(slice_db, monkeypatch):
    """Location-key-ordered binding audits count multi-page aliased orphans and flag duplicate arms."""
    db = slice_db
    monkeypatch.setattr(accounting, "_ORPHAN_KEY_PAGE_ROWS", 2)
    copy_target = await _candidate(db, [(7, 0), (7, 1), (42, 2), (88, 3)])
    namespace = '"' + copy_target.schema_name + '"'
    bindings = namespace + ".provider_location_binding"
    addresses = '"' + db.source_schema + '".retained_addresses'
    await db.connection.execute(f"ALTER TABLE {bindings} ADD PRIMARY KEY (provider_system,provider_id,location_id)")
    await db.connection.execute(
        f"CREATE INDEX typed_office_location_idx ON {bindings}(location_key,provider_system,provider_id,location_id)"
    )
    # Add a second alias pointing at an existing office key to test deduplicated alias counting.
    await db.connection.execute(
        f"""INSERT INTO {bindings}
        SELECT 'npi','alias-provider',md5('alias-office')::uuid,location_key,entity_type,entity_id
        FROM {bindings} ORDER BY location_key LIMIT 1"""
    )
    assert await accounting._orphan_bindings_audit(db.connection, bindings, addresses) == (0, True)
    duplicate_addresses = f"(SELECT * FROM {addresses} UNION ALL SELECT * FROM {addresses})"
    assert await accounting._orphan_bindings_audit(db.connection, bindings, duplicate_addresses) == (0, False)
    first_key = await db.connection.fetchval(f"SELECT min(location_key) FROM {bindings}")
    await db.connection.execute(f"DELETE FROM {addresses} WHERE location_key=$1", first_key)
    assert await accounting._orphan_bindings_audit(db.connection, bindings, addresses) == (2, False)


@pytest.mark.asyncio
async def test_inline_npi_luhn_sql_matches_python_validator(slice_db):
    samples = [
        "1234567893",
        "1000000004",
        "1000000491",
        "2000000002",
        "1999999994",
        "2999999992",
        "1234567890",
        "1234567891",
        "1234567892",
        "1234567894",
        "0123456789",
        "3234567893",
        "9999999999",
        "123456789",
        "12345678930",
        "123456789a",
        "",
        None,
    ]
    samples.extend(f"10000000{digit}{check}" for digit in range(10) for check in range(10))
    samples.extend(f"29876543{digit}{check}" for digit in range(10) for check in range(10))
    sql_results = await slice_db.connection.fetch(
        f"""SELECT candidate.npi,({typed_sql._NPI.replace("p.npi", "candidate.npi")}) AS is_valid
        FROM unnest($1::text[]) AS candidate(npi)""",
        samples,
    )
    for sample, entry in zip(samples, sql_results, strict=True):
        expected = (
            isinstance(sample, str) and is_valid_npi(sample) and re.fullmatch(r"[12][0-9]{9}", sample) is not None
        )
        assert bool(entry["is_valid"]) is expected, f"mismatch for {sample!r}"
