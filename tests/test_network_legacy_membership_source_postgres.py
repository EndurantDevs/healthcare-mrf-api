# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual retained ACA models, archive validation, reviewed aliases and COPY.

Synthetic archive admission is component evidence; no source fetch or complete
source publication is simulated by these tests.
"""

import asyncio
import hashlib
import json
from dataclasses import asdict, replace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from process import reference_family_archive as archive
from process.network_approved_membership_source import pin_approved_membership_source
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_legacy_membership_source import (
    LegacyMembershipSourceError,
    PinnedACAMembershipSource,
    aca_checksum_scope,
    capture_aca_reviewed_alias_digest,
    copy_aca_membership_batch,
    read_aca_membership_batch,
)
from process.network_membership_candidate_lifecycle import _locked_candidate, create_network_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_source_binding_store import NetworkSourceBindingBatchCommand, apply_network_source_binding_batch
from tests.test_network_serving_schema_postgres import serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _CountedConnection, _create, _draft

pytestmark = pytest.mark.asyncio


async def _reviewed_alias(connection, schema, checksum, network_id, *, source_id="source-example", issuer_id=7):
    await connection.execute(
        f'INSERT INTO "{schema}".network_registry_alias VALUES '
        "('aca',$1,'legacy_checksum',$2,$3,$4,'reviewed-example',now())",
        source_id,
        str(checksum),
        aca_checksum_scope(issuer_id, 2026, "medical"),
        network_id,
    )


async def _insert_evidence(session, schema, entries):
    """Land synthetic exact evidence in one native set statement before sealing."""
    evidence_items = [
        {"key": checksum, "network": network_checksum, "site": str(site_id)}
        for checksum, network_checksum, site_id in entries
    ]
    await session.execute(
        text(
            f'INSERT INTO "{schema}".mrf_address_evidence '
            "(evidence_checksum,npi,type,checksum,issuer_id,year,checksum_network,import_id,source_url,"
            "source_record_id,address_key) SELECT key,1000000491,'primary',key,7,2026,network,"
            "'import-one','https://source.example.test/providers','provider:'||key,site "
            "FROM jsonb_to_recordset(CAST(:items AS jsonb)) item(key bigint,network bigint,site uuid)"
        ),
        {"items": json.dumps(evidence_items)},
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema}".mrf_address_evidence '
            f'SELECT (jsonb_populate_record(NULL::"{schema}".mrf_address_evidence,to_jsonb(evidence)||changes)).* '
            f'FROM "{schema}".mrf_address_evidence evidence CROSS JOIN '
            "jsonb_array_elements(CAST(:changes AS jsonb)) excluded(changes) WHERE evidence_checksum=1"
        ),
        {
            "changes": json.dumps(
                [
                    {"evidence_checksum": -10, "issuer_id": 8},
                    {"evidence_checksum": -11, "import_id": "other-import"},
                    {"evidence_checksum": -12, "source_url": "https://other.example.test/providers"},
                ]
            )
        },
    )


async def _damage_evidence(session, schema, damage):
    """Admit unresolved synthetic input before archive validation and sealing."""
    assignment_by_damage = {
        "missing_alias": "checksum_network=123456",
        "missing_site": "address_key=NULL",
        "nil_site": "address_key='00000000-0000-0000-0000-000000000000'",
        "wrong_origin": "source_table='unrelated'",
        "bad_npi": "npi=42",
        "invalid_luhn": "npi=1000000000",
    }
    await session.execute(
        text(f'UPDATE "{schema}".mrf_address_evidence SET {assignment_by_damage[damage]} WHERE evidence_checksum=1')
    )


async def _freeze_archive(engine, source_coordinates_map, entries, damage=None):
    """Create real model heaps and mint the existing native validation receipt."""
    dataset_id = uuid4()
    schema = archive.reference_family_stage_schema(dataset_id)
    owner = "aca_owner_" + uuid4().hex
    async with AsyncSession(engine) as session, session.begin():
        await archive._create_model_family(session, archive.reference_family_spec("mrf-address"), schema)
        await _insert_evidence(session, schema, entries)
        if damage:
            await _damage_evidence(session, schema, damage)
        manifest = await archive._family_manifest(
            session,
            spec=archive.reference_family_spec("mrf-address"),
            schema_name=schema,
            source_metadata={"network_membership": source_coordinates_map},
        )
        ownership = await archive.capture_reference_family_stage_ownership(
            session,
            importer_id="mrf-address",
            dataset_id=dataset_id,
        )
        await session.execute(text(f'CREATE ROLE "{owner}" NOLOGIN'))
        for role_name in source_coordinates_map["runtime_roles"]:
            await session.execute(text(f'CREATE ROLE "{role_name}" NOLOGIN'))
        owner_oid = await session.scalar(text("SELECT oid FROM pg_roles WHERE rolname=:name"), {"name": owner})
        await session.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{owner}"'))
        for table_name, _ in ownership.relation_oids:
            await session.execute(text(f'ALTER TABLE "{schema}"."{table_name}" OWNER TO "{owner}"'))
        for role_name in source_coordinates_map["runtime_roles"]:
            await session.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role_name}"'))
            await session.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role_name}"'))
        validation = await archive.prepare_reference_family_activation(
            session,
            ownership=ownership,
            manifest=manifest,
            package_id=hashlib.sha256(json.dumps(entries, default=str).encode()).hexdigest(),
            profile_contract=archive.CONTRACT,
            sealed_owner_oid=owner_oid,
        )
    return archive.ReferenceFamilyPreparedSource(manifest, ownership), validation, owner


@pytest.fixture
async def aca_source(serving_schema, request):
    connection, schema, engine = serving_schema
    for network_id in (77, 78, 99):
        await connection.execute(
            f'INSERT INTO "{schema}".network_registry_identity(network_id,allocation_key) '
            "OVERRIDING SYSTEM VALUE VALUES($1,$2)",
            network_id,
            uuid4(),
        )
    await _reviewed_alias(connection, schema, 42, 77)
    await _reviewed_alias(connection, schema, 43, 78)
    await _reviewed_alias(connection, schema, 42, 99, source_id="another-source")
    await _reviewed_alias(connection, schema, 42, 99, issuer_id=8)
    async with connection.transaction():
        alias_digest = await capture_aca_reviewed_alias_digest(
            connection,
            source_id="source-example",
            issuer_id=7,
            year=2026,
            alias_scope="medical",
            registry_schema=schema,
        )
    source_coordinates_map = {
        "source_id": "source-example",
        "release_id": "release-one",
        "import_id": "import-one",
        "issuer_id": 7,
        "year": 2026,
        "source_url": "https://source.example.test/providers",
        "alias_scope": "medical",
        "reviewed_alias_sha256": alias_digest,
        "runtime_roles": ("aca_reader_" + uuid4().hex,),
    }
    sites = (uuid4(), uuid4())
    fixture_case = getattr(request, "param", 2)
    count = fixture_case if type(fixture_case) is int else 2
    entries = [(index, 42 if index % 2 else 43, sites[(index - 1) % 2]) for index in range(1, count + 1)]
    prepared, validation, owner = await _freeze_archive(
        engine, source_coordinates_map, entries, fixture_case if type(fixture_case) is str else None
    )
    source_pin = PinnedACAMembershipSource(prepared, validation, **source_coordinates_map)
    try:
        yield connection, schema, source_pin, sites, owner
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{prepared.ownership.schema_name}" CASCADE')
        await connection.execute(f'DROP ROLE "{owner}"')
        for role_name in source_pin.runtime_roles:
            await connection.execute(f'DROP ROLE "{role_name}"')
        assert await connection.fetchval("SELECT to_regnamespace($1)", prepared.ownership.schema_name) is None
        assert not await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", owner)


async def _read(fixture, **options):
    connection, schema, source_pin, *_ = fixture
    async with connection.transaction():
        return await read_aca_membership_batch(connection, source_pin, registry_schema=schema, **options)


async def test_exact_sites_and_checksum_namespaces_are_independent(aca_source):
    batch = await _read(aca_source)
    assert (batch.source_rows, batch.membership_rows, batch.unresolved_rows) == (2, 2, 0)
    memberships = json.loads(batch.input_bytes)
    assert {(entry["network_id"], entry["location_id"]) for entry in memberships} == {
        (77, str(aca_source[3][0])),
        (78, str(aca_source[3][1])),
    }
    assert all(entry["provider_id"] == "1000000491" for entry in memberships)
    first = await _read(aca_source, limit=1)
    second = await _read(aca_source, limit=1, after_evidence_checksum=first.next_evidence_checksum)
    assert json.loads(first.input_bytes)[0]["location_id"] != json.loads(second.input_bytes)[0]["location_id"]
    assert first.next_evidence_checksum == 1 and second.next_evidence_checksum == 2
    assert (await _read(aca_source, after_evidence_checksum=2)).source_rows == 0
    assert (await _read(aca_source)).input_bytes == batch.input_bytes


async def test_copy_actual_candidate_native_codec_and_whole_caller_rollback(aca_source):
    connection, schema, source_pin, *_ = aca_source
    batch = await _read(aca_source)
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    try:
        async with connection.transaction():
            await create_network_candidate(
                connection,
                copy_target,
                source_generations={"aca": source_pin.generation_id},
                approved_custom_revision=0,
                expected_head=0,
                expected_rows=2,
                control_schema=schema,
            )

        async def authority(connection, requested):
            """Use actual control ownership and the complete pinned source generation."""
            candidate = await _locked_candidate(connection, requested, '"' + schema + '"')
            assert candidate["state"] == "open"
            assert json.loads(candidate["source_generations"])["aca"] == source_pin.generation_id
            return requested

        transaction = connection.transaction()
        await transaction.start()
        receipt = await copy_aca_membership_batch(connection, batch, copy_target, require_candidate_authority=authority)
        assert receipt.row_count == 2
        with pytest.raises(LegacyMembershipSourceError, match="accounting mismatch"):
            await copy_aca_membership_batch(
                connection, replace(batch, membership_rows=3), copy_target, require_candidate_authority=authority
            )
        assert await connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership') == 2
        await transaction.rollback()
        assert await connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership') == 0
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')


@pytest.mark.parametrize("damage", ["alias", "source", "release", "package", "manifest", "oid", "writable", "owner"])
async def test_pinned_scope_receipts_catalog_and_reviewed_binding_drift_fail_closed(aca_source, damage):
    connection, schema, source_pin, _, owner = aca_source
    if damage == "alias":
        await connection.execute(
            f"UPDATE \"{schema}\".network_registry_alias SET evidence_id='altered-review' "
            "WHERE source_id='source-example'"
        )
    elif damage in {"source", "release"}:
        with pytest.raises(LegacyMembershipSourceError):
            replace(source_pin, **{("source_id" if damage == "source" else "release_id"): "changed"})
        return
    elif damage in {"package", "manifest", "oid"}:
        field = {"package": "package_id", "manifest": "manifest_sha256", "oid": "stage_schema_oid"}[damage]
        with pytest.raises(LegacyMembershipSourceError):
            replace(
                source_pin, validation=replace(source_pin.validation, **{field: 1 if damage == "oid" else "a" * 64})
            )
        return
    elif damage == "writable":
        await connection.execute(
            f'GRANT INSERT ON "{source_pin.prepared.ownership.schema_name}".mrf_address_evidence TO PUBLIC'
        )
    else:
        await connection.execute(f'ALTER ROLE "{owner}" LOGIN')
    async with connection.transaction():
        with pytest.raises(LegacyMembershipSourceError):
            await read_aca_membership_batch(connection, source_pin, registry_schema=schema)


@pytest.mark.parametrize(
    "aca_source", ["missing_alias", "missing_site", "nil_site", "wrong_origin", "bad_npi"], indirect=True
)
async def test_unresolved_source_rows_are_rejected_before_copy(aca_source):
    connection, *_ = aca_source
    batch = await _read(aca_source)
    assert batch.unresolved_rows == 1 and batch.membership_rows == 1
    authority_calls = []

    async def authority(*_):
        """Record any unexpected authority access before native COPY."""
        authority_calls.append(True)

    async with connection.transaction():
        with pytest.raises(LegacyMembershipSourceError, match="unresolved"):
            await copy_aca_membership_batch(connection, batch, None, require_candidate_authority=authority)
    assert not authority_calls


@pytest.mark.parametrize("aca_source", [5000], indirect=True)
async def test_native_queries_are_constant_for_one_and_five_thousand_rows(aca_source):
    connection, *_ = aca_source
    queries = []
    connection.add_query_logger(queries.append)
    try:
        observed_query_counts = []
        for limit in (1, 5000):
            async with connection.transaction():
                await asyncio.sleep(0)
                queries.clear()
                batch = await read_aca_membership_batch(
                    connection, aca_source[2], registry_schema=aca_source[1], limit=limit
                )
                await asyncio.sleep(0)
                observed_query_counts.append(len(queries))
                assert batch.source_rows == batch.membership_rows == limit
        assert observed_query_counts == [2, 2]
    finally:
        connection.remove_query_logger(queries.append)


@pytest.mark.parametrize(
    "options",
    [
        {"limit": True},
        {"limit": 5001},
        {"limit": 0},
        {"after_evidence_checksum": True},
        {"after_evidence_checksum": 2**63},
    ],
)
async def test_native_reads_reject_invalid_page_inputs(aca_source, options):
    with pytest.raises(LegacyMembershipSourceError):
        await _read(aca_source, **options)


async def test_current_head_changes_cannot_replace_retained_pin_and_scope_is_exact(aca_source):
    connection, schema, source_pin, *_ = aca_source
    original = await _read(aca_source)
    await connection.execute(f'CREATE TABLE "{schema}".mrf_address_evidence(npi bigint,checksum_network bigint)')
    await connection.execute(f'INSERT INTO "{schema}".mrf_address_evidence VALUES(1000000491,42)')
    assert (await _read(aca_source)).input_bytes == original.input_bytes
    assert source_pin.generation_id == original.source.generation_id


async def test_physical_relation_identity_is_rechecked_and_reader_executes_no_dml(aca_source):
    connection, schema, source_pin, _, owner = aca_source
    queries = []
    connection.add_query_logger(queries.append)
    namespace = '"' + source_pin.prepared.ownership.schema_name + '"'
    try:
        async with connection.transaction():
            await asyncio.sleep(0)
            queries.clear()
            await read_aca_membership_batch(connection, source_pin, registry_schema=schema)
            await asyncio.sleep(0)
            assert len(queries) == 2 and all(query.query.startswith("WITH") for query in queries)
        transaction = connection.transaction()
        await transaction.start()
        try:
            await connection.execute(f"ALTER TABLE {namespace}.mrf_address_evidence RENAME TO prior_evidence")
            await connection.execute(f"CREATE TABLE {namespace}.mrf_address_evidence (LIKE {namespace}.prior_evidence)")
            await connection.execute(f'ALTER TABLE {namespace}.mrf_address_evidence OWNER TO "{owner}"')
            with pytest.raises(LegacyMembershipSourceError, match="unavailable"):
                await read_aca_membership_batch(connection, source_pin, registry_schema=schema)
        finally:
            await transaction.rollback()
        assert (await _read(aca_source)).source_rows == 2
    finally:
        connection.remove_query_logger(queries.append)


async def test_reviewed_binding_digest_is_stable_across_session_timezones(aca_source):
    connection, schema, source_pin, *_ = aca_source
    original = await _read(aca_source)
    async with connection.transaction():
        await connection.execute("SET LOCAL TIME ZONE 'America/New_York'")
        assert (
            await capture_aca_reviewed_alias_digest(
                connection,
                source_id=source_pin.source_id,
                issuer_id=7,
                year=2026,
                alias_scope="medical",
                registry_schema=schema,
            )
            == source_pin.reviewed_alias_sha256
        )
        assert (
            await read_aca_membership_batch(connection, source_pin, registry_schema=schema)
        ).input_bytes == original.input_bytes


@pytest.mark.parametrize("damage", ["unallocated", "overflow", "invalid_checksum"])
async def test_alias_admission_rejects_unallocated_or_unbounded_maps(aca_source, damage):
    connection, schema, source_pin, *_ = aca_source
    if damage == "unallocated":
        await connection.execute(
            f"UPDATE \"{schema}\".network_registry_alias SET network_id=12345 WHERE source_id='source-example'"
        )
    else:
        await connection.execute(
            f'INSERT INTO "{schema}".network_registry_alias SELECT '
            "'aca','source-example','legacy_checksum',(100000+sequence_id)::text,$1,77,'reviewed-example',now() "
            "FROM generate_series(1,5001) sequence_id",
            aca_checksum_scope(7, 2026, "medical"),
        )
    if damage == "invalid_checksum":
        await connection.execute(
            f"UPDATE \"{schema}\".network_registry_alias SET alias_value='042' "
            "WHERE source_id='source-example' AND alias_value='42'"
        )
    async with connection.transaction():
        with pytest.raises(LegacyMembershipSourceError):
            await capture_aca_reviewed_alias_digest(
                connection,
                source_id=source_pin.source_id,
                issuer_id=7,
                year=2026,
                alias_scope="medical",
                registry_schema=schema,
            )
        with pytest.raises(LegacyMembershipSourceError):
            await read_aca_membership_batch(connection, source_pin, registry_schema=schema)


@pytest.mark.parametrize(
    "changes", [{"year": True}, {"issuer_id": True}, {"reviewed_alias_sha256": "A" * 64}, {"source_id": ""}]
)
async def test_closed_coordinates_and_caller_transaction_are_required(aca_source, changes):
    connection, schema, source_pin, *_ = aca_source
    with pytest.raises(LegacyMembershipSourceError):
        replace(source_pin, **changes)
    with pytest.raises(LegacyMembershipSourceError, match="caller transaction"):
        await read_aca_membership_batch(connection, source_pin, registry_schema=schema)


@pytest.mark.parametrize("aca_source", ["invalid_luhn"], indirect=True)
async def test_native_npi_validation_rolls_back_the_complete_copy_batch(aca_source):
    connection, schema, source_pin, *_ = aca_source
    batch = await _read(aca_source)
    assert batch.membership_rows == 2 and batch.unresolved_rows == 0
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    try:
        async with connection.transaction():
            await create_network_candidate(
                connection,
                copy_target,
                source_generations={"aca": source_pin.generation_id},
                approved_custom_revision=0,
                expected_head=0,
                expected_rows=2,
                control_schema=schema,
            )

        async def authority(connection, requested):
            """Require real native candidate control ownership before encoding."""
            candidate = await _locked_candidate(connection, requested, '"' + schema + '"')
            assert json.loads(candidate["source_generations"])["aca"] == source_pin.generation_id
            return requested

        async with connection.transaction():
            with pytest.raises(ValueError, match="NPI"):
                await copy_aca_membership_batch(connection, batch, copy_target, require_candidate_authority=authority)
            assert (
                await connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership') == 0
            )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')


async def test_explicit_runtime_role_owner_path_is_rejected_without_direct_acls(aca_source):
    connection, schema, source_pin, _, owner = aca_source
    reader_role = source_pin.runtime_roles[0]
    namespace = '"' + source_pin.prepared.ownership.schema_name + '"'
    await connection.execute(f'REVOKE SELECT ON ALL TABLES IN SCHEMA {namespace} FROM "{reader_role}"')
    await connection.execute(f'REVOKE USAGE ON SCHEMA {namespace} FROM "{reader_role}"')
    await connection.execute(f'GRANT "{owner}" TO "{reader_role}"')
    async with connection.transaction():
        with pytest.raises(LegacyMembershipSourceError, match="writable"):
            await read_aca_membership_batch(connection, source_pin, registry_schema=schema)


async def test_actual_configured_reader_can_pin_without_source_writer_privileges(aca_source):
    connection, schema, source_pin, *_ = aca_source
    reader_role = source_pin.runtime_roles[0]
    await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{reader_role}"')
    await connection.execute(
        f'GRANT SELECT ON "{schema}".network_registry_alias,"{schema}".network_registry_identity TO "{reader_role}"'
    )
    try:
        async with connection.transaction():
            await connection.execute(f'SET LOCAL ROLE "{reader_role}"')
            assert (
                await read_aca_membership_batch(connection, source_pin, registry_schema=schema)
            ).membership_rows == 2
            assert not await connection.fetchval(
                "SELECT has_table_privilege(current_user,$1,'INSERT,UPDATE,DELETE,TRUNCATE')",
                source_pin.prepared.ownership.schema_name + ".mrf_address_evidence",
            )
    finally:
        await connection.execute(
            f'REVOKE SELECT ON "{schema}".network_registry_alias,"{schema}".network_registry_identity FROM "{reader_role}"'
        )
        await connection.execute(f'REVOKE USAGE ON SCHEMA "{schema}" FROM "{reader_role}"')


async def _insert_reviewed_lineage(session, schema, *, extra_plan=False, damage=None):
    await session.execute(
        text(f"UPDATE \"{schema}\".mrf_address_evidence SET network_tier='tier-'||checksum_network::text")
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema}".plan(plan_id,year,issuer_id,state) VALUES '
            "('00007CA0000001',2026,7,'CA'),('00007CA0000002',2026,7,'CA')"
        )
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema}".plan_networktier VALUES '
            "('00007CA0000001','tier-42',7,2026,42),('00007CA0000002','tier-43',7,2026,43)"
        )
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema}".plan_npi_raw(npi,checksum_network,network_tier,issuer_id,year) VALUES '
            "(1000000491,42,'tier-42',7,2026),(1000000491,43,'tier-43',7,2026)"
        )
    )
    if extra_plan:
        await session.execute(
            text(f"INSERT INTO \"{schema}\".plan(plan_id,year,issuer_id,state) VALUES ('00007CA0000003',2026,7,'CA')")
        )
        await session.execute(
            text(f"INSERT INTO \"{schema}\".plan_networktier VALUES ('00007CA0000003','tier-42',7,2026,42)")
        )
    assignment_by_damage = {
        "state": "UPDATE {ns}.plan SET state='NY' WHERE plan_id='00007CA0000001'",
        "tier": "UPDATE {ns}.plan_npi_raw SET network_tier=' tier-42' WHERE checksum_network=42",
        "provider": "DELETE FROM {ns}.plan_npi_raw WHERE checksum_network=42",
        "plan": "DELETE FROM {ns}.plan WHERE plan_id='00007CA0000001'",
        "checksum": "UPDATE {ns}.mrf_address_evidence SET checksum_network=2147483648 WHERE evidence_checksum=1",
    }
    if damage:
        await session.execute(text(assignment_by_damage[damage].format(ns='"' + schema + '"')))


async def _seal_reviewed_archive(session, ownership, owner, reader):
    schema = ownership.schema_name
    await session.execute(text(f'CREATE ROLE "{owner}" NOLOGIN'))
    await session.execute(text(f'CREATE ROLE "{reader}" NOLOGIN'))
    owner_oid = await session.scalar(text("SELECT oid FROM pg_roles WHERE rolname=:name"), {"name": owner})
    await session.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{owner}"'))
    relations = ownership.relation_oids + ((archive.STAGE_TABLE, ownership.auxiliary_oid),)
    for table_name, _ in relations:
        await session.execute(text(f'ALTER TABLE "{schema}"."{table_name}" OWNER TO "{owner}"'))
    await session.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{reader}"'))
    await session.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{reader}"'))
    return owner_oid


async def _freeze_reviewed_archive(engine, coordinates, source_metadata, entries, owner, case):
    schema = coordinates.dataset_schema
    async with AsyncSession(engine) as session, session.begin():
        await archive._create_model_family(session, archive.reference_family_spec("mrf"), schema)
        await _insert_evidence(session, schema, entries)
        await _insert_reviewed_lineage(
            session,
            schema,
            extra_plan=case in {"expansion", "bounded_expansion"},
            damage=case
            if case
            in {
                "state",
                "tier",
                "provider",
                "plan",
                "checksum",
            }
            else None,
        )
        if case == "last_site":
            await session.execute(
                text(f'UPDATE "{schema}".mrf_address_evidence SET address_key=NULL WHERE evidence_checksum=2')
            )
        manifest = await archive._family_manifest(
            session,
            spec=archive.reference_family_spec("mrf"),
            schema_name=schema,
            source_metadata={
                "network_membership": source_metadata,
                "network_bindings": {**asdict(coordinates), "source_key_kind": "hios_plan_id"},
            },
            dependencies={"plan-attributes": "b" * 64},
            auxiliary={"archive_name": archive.archive_table_name(), "publication_sha256": "a" * 64},
        )
        ownership = await archive.capture_reference_family_stage_ownership(
            session,
            importer_id="mrf",
            dataset_id=UUID(coordinates.dataset_id),
        )
        owner_oid = await _seal_reviewed_archive(session, ownership, owner, source_metadata["runtime_roles"][0])
        validation = await archive.prepare_reference_family_activation(
            session,
            ownership=ownership,
            manifest=manifest,
            package_id=hashlib.sha256(json.dumps(entries, default=str).encode()).hexdigest(),
            profile_contract=archive.CONTRACT,
            sealed_owner_oid=owner_oid,
        )
    return archive.ReferenceFamilyPreparedSource(manifest, ownership), validation


def _aca_binding(coordinates, network_id, index=1, **changes):
    return {
        "binding_id": str(uuid4()),
        **asdict(coordinates),
        "source_key": f"00007CA000000{index}",
        "source_scope_json": {
            "issuer_id": "00007",
            "state": "CA",
            "plan_year": 2026,
            "plan_id": f"00007CA000000{index}",
            "checksum_network": 43 if index == 2 else 42,
        },
        "network_id": network_id,
        "evidence_id": "reviewed-example",
        "evidence_sha256": "e" * 64,
        "operation": "bind",
        "expected_revision": 0,
        "expected_network_id": None,
        **changes,
    }


async def _write_aca_bindings(connection, schema, actor, rows):
    command = NetworkSourceBindingBatchCommand(json.dumps(rows).encode(), "Reviewed exact ACA lineage", uuid4().hex)
    async with connection.transaction():
        return await apply_network_source_binding_batch(connection, command, actor, control_schema=schema)


async def _approved_aca_pin(connection, schema):
    revision = await connection.fetchval(f'SELECT approved_revision FROM "{schema}".registry_revision_control')
    async with connection.transaction(isolation="repeatable_read"):
        return await pin_approved_membership_source(connection, approved_revision=revision, control_schema=schema)


@pytest.fixture
async def reviewed_aca_source(serving_schema, request):
    connection, schema, engine = serving_schema
    dataset_id = uuid4()
    coordinates = RegistryNetworkSourceCoordinates(
        "aca",
        "source-example",
        archive.reference_family_stage_schema(dataset_id),
        str(dataset_id),
        "producer-example",
        "edition-example",
    )
    owner, reader = "aca_owner_" + uuid4().hex, "aca_reader_" + uuid4().hex
    source_metadata_dict = {
        "source_id": "source-example",
        "release_id": "release-one",
        "import_id": "import-one",
        "issuer_id": 7,
        "year": 2026,
        "source_url": "https://source.example.test/providers",
        "alias_scope": "medical",
        "reviewed_alias_sha256": "a" * 64,
        "runtime_roles": (reader,),
    }
    case = getattr(request, "param", 2)
    count = case if type(case) is int else (5000 if case == "bounded_expansion" else 2)
    sites = (uuid4(), uuid4())
    entries = [(index, 42 if index % 2 else 43, sites[(index - 1) % 2]) for index in range(1, count + 1)]
    try:
        prepared, validation = await _freeze_reviewed_archive(
            engine, coordinates, source_metadata_dict, entries, owner, case
        )
        source_pin = PinnedACAMembershipSource(prepared, validation, **source_metadata_dict)
        actor = _actor()
        networks = [await _draft(engine, schema, _create("network"), actor) for _ in range(2)]
        binding_rows = [_aca_binding(coordinates, networks[0]["record_id"], index) for index in (1, 2)]
        if case in {"expansion", "bounded_expansion"}:
            binding_rows.append(_aca_binding(coordinates, networks[1]["record_id"], 3))
        receipt = await _write_aca_bindings(connection, schema, actor, binding_rows)
        await _approve(connection, schema, await _command(connection, schema, *networks, *receipt["records"]), actor)
        yield (
            (connection, schema, source_pin, sites),
            actor,
            networks,
            binding_rows,
            coordinates,
            await _approved_aca_pin(connection, schema),
        )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{coordinates.dataset_schema}" CASCADE')
        for role in (reader, owner):
            if await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role):
                await connection.execute(f'DROP OWNED BY "{role}"')
                await connection.execute(f'DROP ROLE "{role}"')
        assert await connection.fetchval("SELECT to_regnamespace($1)", coordinates.dataset_schema) is None


async def _read_reviewed_aca(fixture, *, pin=None, coordinates=None, **options):
    source_fixture, _, _, _, original_coordinates, original_pin = fixture
    connection, schema, source, _ = source_fixture
    async with connection.transaction(isolation="repeatable_read"):
        return await read_aca_membership_batch(
            connection,
            source,
            registry_schema=schema,
            approved_source=pin or original_pin,
            binding_coordinates=coordinates or original_coordinates,
            **options,
        )


@pytest.mark.parametrize("approved_only", [None, 0, 1, "true", [], {}])
async def test_selection_policy_requires_exact_boolean(reviewed_aca_source, approved_only):
    with pytest.raises(LegacyMembershipSourceError, match="coordinates are invalid"):
        await _read_reviewed_aca(reviewed_aca_source, approved_only=approved_only)


async def test_legacy_archive_cannot_select_approved_only(aca_source):
    with pytest.raises(LegacyMembershipSourceError, match="coordinates are invalid"):
        await _read(aca_source, approved_only=True)


async def test_approved_only_preserves_strict_generation_and_empty(reviewed_aca_source):
    strict = await _read_reviewed_aca(reviewed_aca_source)
    expected_recipe_dict = {
        "source_generation": strict.source.generation_id,
        "approved_source": asdict(strict.approved_source),
        "binding_coordinates": asdict(strict.binding_coordinates),
    }
    assert strict.generation_id == hashlib.sha256(archive._canonical_json(expected_recipe_dict)).hexdigest()
    selected = await _read_reviewed_aca(reviewed_aca_source, approved_only=True)
    assert selected.approved_only and selected.generation_id != strict.generation_id
    assert (selected.source_rows, selected.membership_rows, selected.unresolved_rows, selected.omitted_rows) == (
        2,
        2,
        0,
        0,
    )
    assert [row["network_id"] for row in json.loads(selected.input_bytes)] == [
        row["network_id"] for row in json.loads(strict.input_bytes)
    ]
    empty = await _read_reviewed_aca(
        reviewed_aca_source, approved_only=True, after_evidence_checksum=selected.next_evidence_checksum
    )
    assert (empty.source_rows, empty.membership_rows, empty.unresolved_rows, empty.omitted_rows) == (0, 0, 0, 0)
    assert empty.next_evidence_checksum is None and empty.input_bytes == b"[]"


async def test_approved_rebind_and_close_preserve_observational_alias(reviewed_aca_source):
    fixture, actor, networks, binding_rows, _, _ = reviewed_aca_source
    connection, schema, _, sites = fixture
    await _reviewed_alias(connection, schema, 42, networks[0]["record_id"])
    rebind_dict = {
        **binding_rows[0],
        "operation": "rebind",
        "expected_revision": 1,
        "expected_network_id": binding_rows[0]["network_id"],
        "network_id": networks[1]["record_id"],
    }
    pending = (await _write_aca_bindings(connection, schema, actor, [rebind_dict]))["records"][0]
    await _approve(connection, schema, await _command(connection, schema, pending), actor)
    selected = await _read_reviewed_aca(
        reviewed_aca_source, pin=await _approved_aca_pin(connection, schema), approved_only=True
    )
    assert (selected.membership_rows, selected.unresolved_rows, selected.omitted_rows) == (2, 0, 0)
    assert (networks[1]["record_id"], str(sites[0])) in {
        (membership_record["network_id"], membership_record["location_id"])
        for membership_record in json.loads(selected.input_bytes)
    }
    closed = (
        await _write_aca_bindings(
            connection,
            schema,
            actor,
            [
                {
                    **rebind_dict,
                    "operation": "close",
                    "expected_revision": 2,
                    "expected_network_id": rebind_dict["network_id"],
                }
            ],
        )
    )["records"][0]
    await _approve(connection, schema, await _command(connection, schema, closed), actor)
    omitted = await _read_reviewed_aca(
        reviewed_aca_source, pin=await _approved_aca_pin(connection, schema), approved_only=True
    )
    assert (omitted.source_rows, omitted.membership_rows, omitted.unresolved_rows, omitted.omitted_rows) == (2, 1, 0, 1)
    assert json.loads(omitted.input_bytes)[0]["location_id"] == str(sites[1])
    assert (
        await connection.fetchval(f'SELECT network_id FROM "{schema}".network_registry_alias')
        == networks[0]["record_id"]
    )


@pytest.mark.parametrize("reviewed_aca_source", ["state", "tier", "provider", "plan", "checksum"], indirect=True)
async def test_missing_or_bad_raw_scope_is_not_intentional_omission(reviewed_aca_source):
    batch = await _read_reviewed_aca(reviewed_aca_source, approved_only=True)
    assert (batch.source_rows, batch.membership_rows, batch.unresolved_rows, batch.omitted_rows) == (2, 1, 1, 0)
    connection, schema, _, _ = reviewed_aca_source[0]
    with pytest.raises(LegacyMembershipSourceError, match="unresolved"):
        await copy_aca_membership_batch(
            connection, batch, None, require_candidate_authority=None, control_schema=schema
        )


@pytest.mark.parametrize("reviewed_aca_source", ["last_site"], indirect=True)
@pytest.mark.parametrize("approved_only", [False, True])
async def test_last_bad_mapped_site_rejects_whole_batch(reviewed_aca_source, approved_only):
    batch = await _read_reviewed_aca(reviewed_aca_source, approved_only=approved_only)
    assert (batch.source_rows, batch.membership_rows, batch.unresolved_rows, batch.omitted_rows) == (2, 1, 1, 0)
    connection, schema, _, _ = reviewed_aca_source[0]
    with pytest.raises(LegacyMembershipSourceError, match="unresolved"):
        await copy_aca_membership_batch(
            connection, batch, None, require_candidate_authority=None, control_schema=schema
        )


@pytest.mark.parametrize("reviewed_aca_source", ["expansion"], indirect=True)
async def test_valid_unmapped_plan_is_omitted_and_accounted(reviewed_aca_source):
    fixture, actor, _, rows, _, _ = reviewed_aca_source
    connection, schema, _, _ = fixture
    closed = (
        await _write_aca_bindings(
            connection,
            schema,
            actor,
            [{**rows[2], "operation": "close", "expected_revision": 1, "expected_network_id": rows[2]["network_id"]}],
        )
    )["records"][0]
    await _approve(connection, schema, await _command(connection, schema, closed), actor)
    batch = await _read_reviewed_aca(
        reviewed_aca_source, pin=await _approved_aca_pin(connection, schema), approved_only=True
    )
    assert (batch.source_rows, batch.membership_rows, batch.unresolved_rows, batch.omitted_rows) == (2, 2, 0, 1)


async def test_reviewed_exact_aca_lineage_and_keyset(reviewed_aca_source):
    fixture, _, networks, _, _, pin = reviewed_aca_source
    batch = await _read_reviewed_aca(reviewed_aca_source)
    assert (batch.source_rows, batch.membership_rows, batch.unresolved_rows) == (2, 2, 0)
    assert batch.generation_id != fixture[2].generation_id and batch.approved_source == pin
    assert {(row["network_id"], row["location_id"]) for row in json.loads(batch.input_bytes)} == {
        (networks[0]["record_id"], str(site)) for site in fixture[3]
    }
    first = await _read_reviewed_aca(reviewed_aca_source, limit=1)
    second = await _read_reviewed_aca(reviewed_aca_source, after_evidence_checksum=first.next_evidence_checksum)
    assert first.next_evidence_checksum == 1 and second.next_evidence_checksum == 2
    terminal = await _read_reviewed_aca(reviewed_aca_source, after_evidence_checksum=2)
    assert terminal.source_rows == terminal.membership_rows == terminal.unresolved_rows == 0


@pytest.mark.parametrize("reviewed_aca_source", ["expansion"], indirect=True)
async def test_reviewed_same_checksum_expands_only_actual_retained_plans(reviewed_aca_source):
    fixture, _, networks, _, _, _ = reviewed_aca_source
    batch = await _read_reviewed_aca(reviewed_aca_source)
    assert (batch.source_rows, batch.membership_rows, batch.unresolved_rows) == (2, 3, 0)
    assert {(row["network_id"], row["location_id"]) for row in json.loads(batch.input_bytes)} == {
        (networks[0]["record_id"], str(fixture[3][0])),
        (networks[0]["record_id"], str(fixture[3][1])),
        (networks[1]["record_id"], str(fixture[3][0])),
    }


@pytest.mark.parametrize("reviewed_aca_source", ["state", "tier", "provider", "plan", "checksum"], indirect=True)
async def test_reviewed_retained_lineage_drift_is_unresolved(reviewed_aca_source):
    batch = await _read_reviewed_aca(reviewed_aca_source)
    assert (batch.source_rows, batch.membership_rows, batch.unresolved_rows) == (2, 1, 1)
    async with reviewed_aca_source[0][0].transaction():
        with pytest.raises(LegacyMembershipSourceError, match="unresolved"):
            await copy_aca_membership_batch(
                reviewed_aca_source[0][0],
                batch,
                None,
                require_candidate_authority=lambda *_: None,
            )


async def test_reviewed_rebind_close_and_alias_never_change_old_extraction(reviewed_aca_source):
    fixture, actor, networks, binding_rows, _, _ = reviewed_aca_source
    connection, schema, source_pin, _ = fixture
    original = await _read_reviewed_aca(reviewed_aca_source)
    rebind_dict = {
        **binding_rows[0],
        "operation": "rebind",
        "expected_revision": 1,
        "expected_network_id": binding_rows[0]["network_id"],
        "network_id": networks[1]["record_id"],
    }
    pending = (await _write_aca_bindings(connection, schema, actor, [rebind_dict]))["records"][0]
    assert await _read_reviewed_aca(reviewed_aca_source) == original
    await _approve(connection, schema, await _command(connection, schema, pending), actor)
    second_pin = await _approved_aca_pin(connection, schema)
    updated = await _read_reviewed_aca(reviewed_aca_source, pin=second_pin)
    assert updated.generation_id != original.generation_id
    assert {membership_record["network_id"] for membership_record in json.loads(updated.input_bytes)} == {
        network["record_id"] for network in networks
    }
    with pytest.raises(LegacyMembershipSourceError, match="bindings are unavailable"):
        await _read_reviewed_aca(reviewed_aca_source)
    closed = (
        await _write_aca_bindings(
            connection,
            schema,
            actor,
            [
                {
                    **rebind_dict,
                    "operation": "close",
                    "expected_revision": 2,
                    "expected_network_id": rebind_dict["network_id"],
                }
            ],
        )
    )["records"][0]
    assert await _read_reviewed_aca(reviewed_aca_source, pin=second_pin) == updated
    await _approve(connection, schema, await _command(connection, schema, closed), actor)
    await _reviewed_alias(connection, schema, 42, rebind_dict["network_id"])
    closed_batch = await _read_reviewed_aca(reviewed_aca_source, pin=await _approved_aca_pin(connection, schema))
    assert (closed_batch.membership_rows, closed_batch.unresolved_rows) == (1, 1)
    assert {membership_record["network_id"] for membership_record in json.loads(original.input_bytes)} == {
        binding_rows[0]["network_id"]
    }
    assert source_pin.prepared.manifest.importer_id == "mrf"


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_system", "fhir"),
        ("source_id", "foreign"),
        ("dataset_schema", "foreign"),
        ("dataset_id", "foreign"),
        ("producer_id", "foreign"),
        ("edition_id", "foreign"),
    ],
)
async def test_reviewed_every_admitted_coordinate_is_exact(reviewed_aca_source, field, value):
    coordinates = reviewed_aca_source[4]
    with pytest.raises(LegacyMembershipSourceError, match="coordinates"):
        await _read_reviewed_aca(reviewed_aca_source, coordinates=replace(coordinates, **{field: value}))


@pytest.mark.parametrize("metadata", [None, {}, {"source_key_kind": "other"}, {"extra": True}])
async def test_reviewed_descriptor_requires_exact_seven_sealed_keys(reviewed_aca_source, metadata):
    source = reviewed_aca_source[0][2]
    descriptor_dict = {**asdict(reviewed_aca_source[4]), "source_key_kind": "hios_plan_id"}
    source.prepared.manifest.source_metadata["network_bindings"] = (
        metadata if metadata in (None, {}) else {**descriptor_dict, **metadata}
    )
    with pytest.raises(LegacyMembershipSourceError, match="admission coordinates differ"):
        await _read_reviewed_aca(reviewed_aca_source)


@pytest.mark.parametrize("reviewed_aca_source", [5000], indirect=True)
@pytest.mark.parametrize("approved_only", [False, True])
async def test_reviewed_fixed_query_count_for_one_and_five_thousand(reviewed_aca_source, approved_only):
    fixture, _, _, _, coordinates, pin = reviewed_aca_source
    connection, schema, source, _ = fixture
    for limit in (1, 5000):
        counted = _CountedConnection(connection)
        async with connection.transaction(isolation="repeatable_read"):
            result = await read_aca_membership_batch(
                counted,
                source,
                registry_schema=schema,
                approved_source=pin,
                binding_coordinates=coordinates,
                limit=limit,
                approved_only=approved_only,
            )
        assert counted.statements == 4
        assert result.source_rows == result.membership_rows == limit and result.unresolved_rows == 0


@pytest.mark.parametrize("reviewed_aca_source", ["bounded_expansion"], indirect=True)
@pytest.mark.parametrize("approved_only", [False, True])
async def test_reviewed_plan_expansion_is_bounded_before_return(reviewed_aca_source, approved_only):
    with pytest.raises(LegacyMembershipSourceError, match="batch exceeds bounds"):
        await _read_reviewed_aca(reviewed_aca_source, limit=5000, approved_only=approved_only)


@pytest.mark.parametrize("isolation", ["read_committed", "repeatable_read"])
async def test_reviewed_requires_snapshot_and_exact_approved_fingerprint(reviewed_aca_source, isolation):
    fixture, _, _, _, coordinates, pin = reviewed_aca_source
    connection, schema, source, _ = fixture
    selected_pin = replace(pin, generation_id="f" * 64) if isolation == "repeatable_read" else pin
    async with connection.transaction(isolation=isolation):
        with pytest.raises(LegacyMembershipSourceError, match="bindings are unavailable"):
            await read_aca_membership_batch(
                connection,
                source,
                registry_schema=schema,
                approved_source=selected_pin,
                binding_coordinates=coordinates,
            )


@pytest.mark.parametrize("approved_only", [False, True])
@pytest.mark.parametrize("damage", [None, "revision", "fingerprint", "generation", "after_copy", "policy"])
async def test_reviewed_native_copy_requires_exact_candidate_pins(reviewed_aca_source, damage, approved_only):
    fixture, _, _, _, _, pin = reviewed_aca_source
    connection, schema, _, _ = fixture
    batch = await _read_reviewed_aca(reviewed_aca_source, approved_only=approved_only)
    candidate_id = uuid4()
    copy_target = MembershipCopyTarget(
        str(uuid4()), str(uuid4()), str(uuid4()), str(candidate_id), "network_candidate_" + candidate_id.hex
    )
    generations_by_source = {"aca": batch.generation_id, "custom_membership": pin.generation_id}
    if damage in {"fingerprint", "generation"}:
        generations_by_source["custom_membership" if damage == "fingerprint" else "aca"] = "a" * 64
    if damage == "policy":
        generations_by_source["aca"] = replace(batch, approved_only=not approved_only).generation_id

    async def authority(connection, requested):
        candidate = await _locked_candidate(connection, requested, '"' + schema + '"')
        assert candidate["state"] == "open"
        if damage == "after_copy":
            await connection.execute(
                f'UPDATE "{schema}".network_membership_candidate SET approved_custom_revision=0 WHERE candidate_id=$1',
                candidate_id,
            )
        return requested

    try:
        async with connection.transaction():
            await create_network_candidate(
                connection,
                copy_target,
                source_generations=generations_by_source,
                approved_custom_revision=pin.approved_revision + (damage == "revision"),
                expected_head=0,
                expected_rows=2,
                control_schema=schema,
            )
        async with connection.transaction():
            if damage:
                with pytest.raises(LegacyMembershipSourceError, match="candidate pin does not match"):
                    await copy_aca_membership_batch(
                        connection,
                        batch,
                        copy_target,
                        require_candidate_authority=authority,
                        control_schema=schema,
                    )
            else:
                receipt = await copy_aca_membership_batch(
                    connection,
                    batch,
                    copy_target,
                    require_candidate_authority=authority,
                    control_schema=schema,
                )
                assert receipt.row_count == 2
        assert await connection.fetchval(f'SELECT count(*) FROM "{copy_target.schema_name}".network_membership') == (
            0 if damage else 2
        )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{copy_target.schema_name}" CASCADE')


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_system", "ptg"),
        ("source_id", "other-source"),
        ("dataset_schema", "other_schema"),
        ("dataset_id", "other-dataset"),
        ("producer_id", "other-producer"),
        ("edition_id", "other-edition"),
        ("source_key", "other-key"),
        ("checksum_network", 41),
        ("plan_year", 2025),
        ("state", "NY"),
        ("issuer_id", "00008"),
    ],
)
async def test_reviewed_approved_foreign_namespace_and_scope_never_match(reviewed_aca_source, field, value):
    fixture, actor, _, rows, _, _ = reviewed_aca_source
    connection, schema, _, _ = fixture
    foreign_dict = {**rows[0], "binding_id": str(uuid4()), "source_scope_json": dict(rows[0]["source_scope_json"])}
    if field in foreign_dict["source_scope_json"]:
        foreign_dict["source_scope_json"][field] = value
        if field in {"state", "issuer_id"}:
            foreign_dict["source_scope_json"]["plan_id"] = "00007NY0000001" if field == "state" else "00008CA0000001"
    else:
        foreign_dict[field] = value
    if field == "source_system":
        foreign_dict["source_scope_json"] = {
            "cohort_id": "cohort-example",
            "snapshot_id": "snapshot-example",
            "company_key": "company-example",
        }
    closed_dict = {
        **rows[0],
        "operation": "close",
        "expected_revision": 1,
        "expected_network_id": rows[0]["network_id"],
    }
    receipt = await _write_aca_bindings(connection, schema, actor, [foreign_dict, closed_dict])
    await _approve(connection, schema, await _command(connection, schema, *receipt["records"]), actor)
    result = await _read_reviewed_aca(reviewed_aca_source, pin=await _approved_aca_pin(connection, schema))
    assert (result.source_rows, result.membership_rows, result.unresolved_rows) == (2, 1, 1)


async def test_reviewed_requires_both_pins_and_full_plan_archive(aca_source):
    connection, schema, source, *_ = aca_source
    pin = await _approved_aca_pin(connection, schema)
    coordinates = RegistryNetworkSourceCoordinates(
        "aca",
        source.source_id,
        source.prepared.ownership.schema_name,
        str(source.prepared.ownership.dataset_id),
        "producer-example",
        "edition-example",
    )
    async with connection.transaction(isolation="repeatable_read"):
        for approved_source, binding_coordinates in ((pin, None), (None, coordinates), (pin, coordinates)):
            with pytest.raises(LegacyMembershipSourceError, match="source coordinates are invalid"):
                await read_aca_membership_batch(
                    connection,
                    source,
                    registry_schema=schema,
                    approved_source=approved_source,
                    binding_coordinates=binding_coordinates,
                )
