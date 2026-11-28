# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Independent slice checks reject stale hashes, misbound reuse and missing indexes."""

import asyncio
from uuid import UUID, uuid4

import pytest

from process import network_compact_validation as compact_validation
from process.network_compact_dependencies import compact_slice_ref_digest
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_compact_readiness import NetworkCompactReadinessError, compact_index_digest
from process.network_compact_slice_validation import verify_compact_slice_contents
from process.network_compact_validation import _actual_memberships, _membership_sets_differ, _typed_memberships_match
from process.network_typed_slice_native import _MATCH_ROWS, build_matched_facts, verify_work_refs
from process.registry_typed_membership_parity import ORDER
from tests.test_network_membership_slices_postgres import _candidate, _pin, _prepare, slice_db


async def _layout(db, target, receipt, artifacts=None):
    oid = await db.connection.fetchval("SELECT to_regclass($1)::oid", db.source_schema + ".retained_addresses")
    own = CompactEdgeArtifact(target.candidate_id, target.schema_name)
    return CompactNetworkLayout(
        CompactAddressRelation(target.schema_name, "network_slice_ref"),
        CompactAddressRelation(db.source_schema, "retained_addresses"),
        (own,) if artifacts is None else tuple(sorted(artifacts, key=lambda artifact: artifact.candidate_id)),
    )


@pytest.mark.parametrize("damage", ["extra", "missing", "duplicate-owner"])
async def test_new_slice_validation_rejects_unowned_or_missing_edges(slice_db, damage):
    db = slice_db
    target = await _candidate(db, [(42, 0), (7, 0)])
    async with db.connection.transaction():
        receipt = await _prepare(db, target)
        layout = await _layout(db, target, receipt)
        await verify_compact_slice_contents(db.connection, target, layout)
    ns = '"' + target.schema_name + '"'
    if damage == "extra":
        await db.connection.execute(f"INSERT INTO {ns}.network_office_edge VALUES($1,'extra-office')", uuid4())
    elif damage == "missing":
        await db.connection.execute(
            f"DELETE FROM {ns}.network_office_edge WHERE ctid=(SELECT min(ctid) FROM {ns}.network_office_edge)"
        )
    else:
        await db.connection.execute(f"""INSERT INTO {ns}.network_slice_ref
            SELECT 45,office_bucket,artifact_candidate_id,slice_id,row_count,content_sha256
            FROM {ns}.network_slice_ref WHERE network_id=42""")
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
                CompactEdgeArtifact(first.candidate_id, first.schema_name),
                CompactEdgeArtifact(second.candidate_id, second.schema_name),
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


async def test_exact_slice_reuse_metadata_detects_digest_change(slice_db):
    db = slice_db
    target = await _candidate(db, [(42, 0), (7, 1)])
    async with db.connection.transaction():
        await _prepare(db, target)
    namespace = '"' + target.schema_name + '"'
    counts = await compact_slice_ref_digest(db.connection, target.schema_name)
    exact = await compact_slice_ref_digest(db.connection, target.schema_name, include_reuse_keys=True)
    await db.connection.execute(
        f"UPDATE {namespace}.network_slice_ref SET content_sha256=repeat('a',64) WHERE network_id=42"
    )
    assert await compact_slice_ref_digest(db.connection, target.schema_name) == counts
    assert await compact_slice_ref_digest(db.connection, target.schema_name, include_reuse_keys=True) != exact


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
@pytest.mark.parametrize("corruption", [None, "missing", "extra", "network", "same-count"])
async def test_indexed_compact_set_parity_keeps_aliases_evidence_and_selected_reuse(slice_db, reuse, corruption):
    db = slice_db
    pairs = [(42, 0), (42, 0), (42, 1), (7, 1), (88, 2)]
    prior, artifacts = None, []
    if reuse:
        first = await _set_candidate(db, [*pairs, (99, 3)])
        async with db.connection.transaction():
            first_receipt = await _prepare(db, first)
        prior = _pin(first, first_receipt)
        artifacts.append(CompactEdgeArtifact(first.candidate_id, first.schema_name))
    copy_target = await _set_candidate(db, pairs + ([(21, 3)] if reuse else []))
    async with db.connection.transaction():
        receipt = await _prepare(db, copy_target, previous=prior)
        artifacts.append(CompactEdgeArtifact(copy_target.candidate_id, copy_target.schema_name))
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
    elif corruption == "same-count":
        await db.connection.execute(
            f"UPDATE {edge} SET location_key='wrong-office' WHERE slice_id=$1", selected["slice_id"]
        )
    async with db.connection.transaction(isolation="repeatable_read", readonly=True):
        is_different = corruption is not None
        assert await _reference_set_difference(db.connection, namespace, layout) is is_different
        assert await _membership_sets_differ(db.connection, namespace, layout) is is_different
        assert (
            await _typed_memberships_match(db.connection, namespace, layout, len(pairs) + int(reuse))
            is not is_different
        )


async def test_typed_compact_validation_rejects_unbound_alias_with_same_serving_set(slice_db):
    db = slice_db
    target = await _set_candidate(db, [(42, 0), (42, 1)])
    async with db.connection.transaction():
        receipt = await _prepare(db, target)
        layout = await _layout(db, target, receipt)
    namespace = '"' + target.schema_name + '"'
    await db.connection.execute(
        f"DELETE FROM {namespace}.provider_location_binding WHERE location_id=$1", db.location_ids[1]
    )
    async with db.connection.transaction(isolation="repeatable_read", readonly=True):
        assert await _membership_sets_differ(db.connection, namespace, layout) is False
        assert await _typed_memberships_match(db.connection, namespace, layout, 2) is False


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
        assert await _typed_memberships_match(db.connection, namespace, layout, 0) is True
        assert await _typed_memberships_match(db.connection, namespace, layout, 1) is False


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


async def _independent_native_work(db, target):
    """Preserve genuine native facts for the subsequent read-only comparison."""
    namespace = '"' + target.schema_name + '"'
    async with db.connection.transaction():
        await db.connection.execute(f"CREATE INDEX ON {namespace}.network_membership({ORDER})")
        counts = await build_matched_facts(
            db.connection, namespace, '"' + db.source_schema + '".retained_addresses', '"source_facts"', None
        )
        assert counts["invalid_memberships"] == counts["invalid_location_keys"] == 0
        await db.connection.execute('CREATE TEMP TABLE independent_facts AS SELECT * FROM "source_facts"')
        await db.connection.execute(
            "ALTER TABLE independent_facts ADD PRIMARY KEY(network_id,office_bucket,location_key)"
        )
    return '"independent_facts"'


@pytest.mark.parametrize(("select_bucket", "bindings_verified"), [(False, False), (True, False), (False, True)])
async def test_native_facts_keep_all_evidence_at_page_boundary(slice_db, select_bucket, bindings_verified):
    db = slice_db
    copy_target = await _candidate(db, [(7, 0), (42, 1), (99, 2)])
    namespace = '"' + copy_target.schema_name + '"'
    async with db.connection.transaction():
        await db.connection.execute(f"CREATE INDEX ON {namespace}.network_membership({ORDER})")
        await db.connection.execute(f"""INSERT INTO {namespace}.network_membership
            SELECT network_id,provider_system,provider_id,location_id,evidence_id
            FROM {namespace}.network_membership CROSS JOIN generate_series(1,{_MATCH_ROWS}) WHERE network_id=42""")
        await db.connection.execute(f"DELETE FROM {namespace}.provider_location_binding WHERE location_key='office-a'")
        affected = None
        if select_bucket:
            await db.connection.execute("""CREATE TEMP TABLE changed_buckets ON COMMIT DROP AS
                SELECT 42 AS network_id,get_byte(sha256(convert_to('office-b','UTF8')),0)&63 AS office_bucket""")
            affected = '"changed_buckets"'
        # A verified full pass groups everything in one statement and must count exactly what the pages count.
        counts = await build_matched_facts(
            db.connection,
            namespace,
            '"' + db.source_schema + '".retained_addresses',
            '"source_facts"',
            affected,
            bindings_verified=bindings_verified,
        )
        assert counts["invalid_memberships"] == (0 if select_bucket else 1)
        assert counts["invalid_location_keys"] == 0
        assert counts["membership_rows"] == _MATCH_ROWS + (1 if select_bucket else 3)
        actual = await db.connection.fetch("SELECT network_id,membership_rows FROM source_facts ORDER BY network_id")
        expected_rows = [(42, _MATCH_ROWS + 1)]
        assert list(map(tuple, actual)) == (expected_rows if select_bucket else [(7, 1), *expected_rows, (99, 1)])


def _reference_mutations(namespace):
    refs = namespace + ".network_slice_ref"
    tail = f"(SELECT network_id,office_bucket FROM {refs} ORDER BY network_id DESC,office_bucket DESC LIMIT 1)"
    return (
        f"DELETE FROM {refs} WHERE (network_id,office_bucket)={tail}",
        f"INSERT INTO {refs} SELECT 2147483647,office_bucket,artifact_candidate_id,slice_id,row_count,content_sha256 FROM {refs} ORDER BY network_id,office_bucket LIMIT 1",
        f"UPDATE {refs} SET network_id=2147483646 WHERE (network_id,office_bucket)={tail}",
        f"UPDATE {refs} SET row_count=row_count+1 WHERE (network_id,office_bucket)={tail}",
        f"UPDATE {refs} SET content_sha256=repeat('a',64) WHERE (network_id,office_bucket)={tail}",
        "DELETE FROM independent_facts WHERE network_id=(SELECT max(network_id) FROM independent_facts)",
    )


@pytest.mark.parametrize("reuse", [False, True], ids=["new", "retained-and-new"])
async def test_independent_work_requires_exact_reference_tails(slice_db, reuse):
    db = slice_db
    prior, artifacts = None, []
    if reuse:
        first = await _set_candidate(db, [(network, 0) for network in range(1, 258)])
        async with db.connection.transaction():
            first_receipt = await _prepare(db, first)
        prior = _pin(first, first_receipt)
        artifacts.append(CompactEdgeArtifact(first.candidate_id, first.schema_name))
    copy_target = await _set_candidate(db, [(network, 0) for network in range(1, 302)] + [(42, 0), (42, 1)])
    async with db.connection.transaction():
        receipt = await _prepare(db, copy_target, previous=prior)
        artifacts.append(CompactEdgeArtifact(copy_target.candidate_id, copy_target.schema_name))
        layout = await _layout(db, copy_target, receipt, artifacts)
        await verify_compact_slice_contents(db.connection, copy_target, layout)
    assert receipt.distinct_memberships == 301 and receipt.membership_rows == 303
    assert receipt.reused_slices == (257 if reuse else 0)
    work = await _independent_native_work(db, copy_target)
    async with db.connection.transaction(isolation="repeatable_read", readonly=True):
        await verify_work_refs(db.connection, copy_target, work)
    for statement in _reference_mutations('"' + copy_target.schema_name + '"'):
        savepoint = db.connection.transaction()
        await savepoint.start()
        try:
            await db.connection.execute(statement)
            with pytest.raises(NetworkCompactReadinessError):
                await verify_work_refs(db.connection, copy_target, work)
        finally:
            await savepoint.rollback()
    await verify_work_refs(db.connection, copy_target, work)


async def test_independent_work_checks_empty_reference_domain(slice_db):
    db = slice_db
    target = await _candidate(db, [])
    async with db.connection.transaction():
        await _prepare(db, target)
    work = await _independent_native_work(db, target)
    async with db.connection.transaction(isolation="repeatable_read", readonly=True):
        await verify_work_refs(db.connection, target, work)
    await db.connection.execute(
        f"INSERT INTO \"{target.schema_name}\".network_slice_ref VALUES(1,0,$1,$2,1,repeat('a',64))",
        UUID(target.candidate_id),
        uuid4(),
    )
    with pytest.raises(NetworkCompactReadinessError):
        await verify_work_refs(db.connection, target, work)


def test_validation_chunks_partition_entire_network_domain(monkeypatch):
    """Network ranges cover every ID once and a network over the row budget is checked alone."""
    monkeypatch.setattr(compact_validation, "_VALIDATION_CHUNK_ROWS", 10)
    assert compact_validation._validation_chunks([]) == [(None, None)]
    chunks = compact_validation._validation_chunks([(1, 6), (2, 6), (5, 40), (6, 25), (10, 4)])
    assert chunks == [(None, 1), (1, 2), (2, 5), (5, 6), (6, None)]


async def _apply_chunked_corruption(db, namespace, selected, corruption, expected_rows):
    """Apply one compact-slice or membership mutation for chunked validation tests."""
    edge_table = namespace + ".network_office_edge"
    ref_table = namespace + ".network_slice_ref"
    statement_by_corruption = {
        "missing": (f"DELETE FROM {edge_table} WHERE slice_id=$1", (selected["slice_id"],)),
        "extra": (f"INSERT INTO {edge_table} VALUES($1,'extra-office')", (selected["slice_id"],)),
        "wrong-network": (f"UPDATE {ref_table} SET network_id=43 WHERE network_id=88", ()),
        "same-count": (
            f"UPDATE {edge_table} SET location_key='wrong-office' WHERE slice_id=$1",
            (selected["slice_id"],),
        ),
        "unbound-alias": (
            f"DELETE FROM {namespace}.provider_location_binding WHERE location_id=$1",
            (db.location_ids[1],),
        ),
    }
    if corruption in statement_by_corruption:
        statement, arguments = statement_by_corruption[corruption]
        await db.connection.execute(statement, *arguments)
        return expected_rows
    if corruption == "wrong-bucket":
        wrong_bucket = (selected["office_bucket"] + 1) % 64
        await db.connection.execute(f"DELETE FROM {ref_table} WHERE network_id=42 AND office_bucket=$1", wrong_bucket)
        await db.connection.execute(
            f"UPDATE {ref_table} SET office_bucket=$1 WHERE network_id=42 AND office_bucket=$2",
            wrong_bucket,
            selected["office_bucket"],
        )
        return expected_rows
    membership_by_corruption = {"extra-high-network": (500, "extra-ev"), "nonpositive-network": (0, "zero-ev")}
    if corruption in membership_by_corruption:
        network_id, evidence_id = membership_by_corruption[corruption]
        await db.connection.execute(
            f"INSERT INTO {namespace}.network_membership VALUES($1,'npi','1000000004',$2,$3)",
            network_id,
            db.location_ids[0],
            evidence_id,
        )
        return expected_rows + 1
    return expected_rows + int(corruption == "wrong-expected-rows")


@pytest.mark.parametrize(
    "corruption",
    [
        None,
        "missing",
        "extra",
        "wrong-bucket",
        "wrong-network",
        "same-count",
        "unbound-alias",
        "wrong-expected-rows",
        "extra-high-network",
        "nonpositive-network",
    ],
)
async def test_chunked_typed_compact_validation_handles_mega_networks_and_corruptions(
    slice_db, monkeypatch, corruption
):
    """Chunked order-indexed validation accepts valid mega-networks and rejects every corruption."""
    db = slice_db
    monkeypatch.setattr(compact_validation, "_VALIDATION_CHUNK_ROWS", 2)
    pairs = [(7, 0), (42, 0), (42, 1), (42, 2), (42, 3), (88, 2), (99, 3)]
    copy_target = await _candidate(db, pairs)
    namespace = '"' + copy_target.schema_name + '"'
    await db.connection.execute(
        f"CREATE INDEX typed_effective_membership_order_idx ON {namespace}.network_membership({ORDER})"
    )
    async with db.connection.transaction():
        receipt = await _prepare(db, copy_target)
        layout = await _layout(db, copy_target, receipt)
        await verify_compact_slice_contents(db.connection, copy_target, layout)
    selected = await db.connection.fetchrow(
        f"SELECT slice_id,office_bucket FROM {namespace}.network_slice_ref WHERE network_id=42 ORDER BY office_bucket LIMIT 1"
    )
    expected_rows = await _apply_chunked_corruption(db, namespace, selected, corruption, len(pairs))
    async with db.connection.transaction(isolation="repeatable_read", readonly=True):
        assert await _typed_memberships_match(db.connection, namespace, layout, expected_rows) is (corruption is None)
