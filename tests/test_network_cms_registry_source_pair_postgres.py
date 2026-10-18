# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual eight-file admission paired with the native seven-table capture contract."""

import asyncio
import json
from contextlib import asynccontextmanager, contextmanager
from dataclasses import asdict, replace
from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.schema import CreateTable

from api import provider_directory_cms_network_scope as scoped
from api.provider_directory_cms_entities import read_cms_entities
from api.provider_directory_entities_contract import CURSOR_KEY_ENV, DirectoryRead, DirectoryReadError
from db.connection import Base
from process import network_cms_registry_source_pair as capture
from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_serving_receipt as serving
from process import provider_directory_profile as profile
from process.entity_address_snapshot_source import (
    EntityAddressSourceCopy,
    entity_address_archive_relations,
    entity_address_unified,
    native_entity_address_source_copy,
)
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_source_pair import (
    RegistryCMSPublicationProof,
    capture_registry_cms_source_pair,
    decode_registry_cms_source_pair,
    require_registry_cms_source_pair,
    require_registry_cms_source_pair_connection,
)
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, read_fhir_membership_batch
from process.network_serving_read import PinnedNetworkServingManifest
from process.provider_directory_mrf_payer_binding import (
    ReviewedCMSPayerDecision,
    record_reviewed_cms_payer_decision,
)
from process.reference_family_archive import native_copy_projection
from tests import cms_npd_admission_postgres_support as support
from tests import test_provider_directory_cms_serving_receipt_postgres as native
from tests.test_entity_address_snapshot_stage_postgres import _copy_source_index_shapes, _install_copy_source_indexes
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template


def _retained_office_resources(monkeypatch):
    """Acquire two explicit practitioner/office assertions through all eight source files."""
    original = support._resource_rows

    def resources(revision):
        rows = original(revision)
        rows["Practitioner"][0]["identifier"] = [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1000000491"}]
        rows["Location"][0]["name"] = "Office A " + revision
        rows["Location"].append(
            {
                "id": "site-2",
                "identifier": [{"system": "https://example.test/site", "value": "site-source-2"}],
                "name": "Office B " + revision,
                "address": {"id": "address-source-2", "line": ["2 Sample Street"], "city": "Example City"},
            }
        )
        role = rows["PractitionerRole"][0]
        rows["InsurancePlan"][0]["network"] = [{"reference": "Organization/network-1"}]
        role["network"] = [{"reference": "Organization/network-1"}]
        role["insurancePlan"] = [{"reference": "InsurancePlan/plan-1"}]
        role["location"] = [{"reference": "Location/site-1"}, {"reference": "Location/site-2"}]
        rows["PractitionerRole"].append({**role, "id": "role-2", "location": [{"reference": "Location/site-2"}]})
        return rows

    monkeypatch.setattr(support, "_resource_rows", resources)


async def _create_result_models(connection, *, physical=False):
    """Install the real seven-table family and native Profile model definitions."""
    metadata = MetaData(schema="mrf")
    for model in (entity_address_unified.EntityAddressUnified, *entity_address_unified.SUPPORT_TABLE_MODELS):
        model.__table__.to_metadata(metadata, schema="mrf")
    for name in serving._NATIVE_RELATIONS:
        if name not in {table.name for table in metadata.tables.values()}:
            original = next((table for table in Base.metadata.tables.values() if table.name == name), None)
            if original is not None:
                original.to_metadata(metadata, schema="mrf")
    if physical:
        main = metadata.tables["mrf.entity_address_unified"]
        statement = str(CreateTable(main).compile(dialect=connection.dialect)).replace(
            "(\n", "(\n discarded_column integer,\n", 1
        )
        await connection.execute(text(statement))
        await connection.execute(text("ALTER TABLE mrf.entity_address_unified DROP COLUMN discarded_column"))
    await connection.run_sync(metadata.create_all)
    await _install_copy_source_indexes(connection, "mrf")
    if physical:
        await connection.execute(
            text("ALTER SEQUENCE mrf.entity_address_evidence_evidence_id_seq RENAME TO promoted_evidence_id_seq")
        )
    for definition in (
        profile.profile_table_sql("mrf", logged=True),
        profile.profile_evidence_table_sql("mrf", logged=True),
        support.fhir.provider_directory_address_overlay_table_sql("mrf"),
    ):
        await connection.execute(text(definition))


async def _set_profile_selection(connection, selection_proof_id):
    """Bind the actual fixture selection before capturing its receipt snapshot."""
    await connection.execute(
        text("UPDATE mrf.provider_directory_profile_serving_generation SET selection_proof_id=:proof"),
        {"proof": selection_proof_id},
    )


async def _publish_pair(database, initial, monkeypatch, *, physical=False, selection_proof_id=None, profile_as_of=None):
    """Commit real sealed source scalars and native result authorities together."""
    handoff = initial["registry_source_admission"]
    cms_pin_by_field = {field: initial["cms_serving_candidate"]["desired_cms_dataset"][field] for field in native._PIN}
    monkeypatch.setattr(native, "_PIN", cms_pin_by_field)
    profile_custody = await _prepare_pair_results(database, physical)
    async with _pair_publication_transaction(database) as connection:
        await _assert_pair_profile_reads(database, connection, profile_custody)
        await connection.execute(
            text("""INSERT INTO mrf.entity_address_unified
          (location_key,entity_type,entity_id,npi,checksum,type,first_line,second_line,city_name,state_name,postal_code,country_code,
           plans_network_array,procedures_array,medications_array,canonical_network_ids)
          SELECT repeat(unit::text,64),'npi','1000000004',1000000004,42,'practice','123 Example Street','Suite '||unit,
            'Sample City','CA','90210','US','{}','{}','{}','{}' FROM generate_series(2,3) unit""")
        )
        await connection.execute(text("DELETE FROM mrf.provider_directory_profile_serving_generation"))
        await native._seed_profile(connection, "mrf")
        await connection.execute(
            text(
                "UPDATE mrf.provider_directory_endpoint_dataset SET status='published',is_current=true,published_at=now() WHERE dataset_id=:dataset"
            ),
            {"dataset": handoff["network_bindings"]["dataset_id"]},
        )
        await connection.execute(
            text(
                "INSERT INTO mrf.provider_directory_cms_serving_coverage(dataset_id,release_id,dataset_hash,proof_version,published_at,created_at) VALUES(:dataset,:release,:hash,2,now(),now())"
            ),
            {
                "dataset": cms_pin_by_field["dataset_id"],
                "release": handoff["release_id"],
                "hash": cms_pin_by_field["dataset_hash"],
            },
        )
        await native._advance_native(connection, "mrf", doctors=True)
        await native._advance_native(connection, "mrf")
        await connection.execute(
            text("UPDATE mrf.provider_directory_profile_serving_generation SET profile_as_of=:as_of"),
            {"as_of": profile_as_of or handoff["semantic_projection_as_of"]},
        )
        if selection_proof_id is not None:
            await _set_profile_selection(connection, selection_proof_id)
        receipt_payload = await native._payload(connection, "mrf")
        receipt_payload["cms"]["release_id"] = handoff["release_id"]
        if selection_proof_id is not None:
            receipt_payload["selection"]["proof_id"] = selection_proof_id
        receipt_id = await serving.append_serving_receipt(connection, "mrf", receipt_payload)
    async with database.session() as session:
        receipt_row = (
            await session.execute(
                text(
                    "SELECT publication_xid::text,profile_generation_id FROM mrf.provider_directory_cms_serving_receipt WHERE receipt_id=:receipt"
                ),
                {"receipt": receipt_id},
            )
        ).one()
    return _publication_proof(handoff, cms_pin_by_field, receipt_payload, receipt_id, receipt_row)


async def _prepare_pair_results(database, physical):
    """Create the native result family before its separately authenticated control cut."""
    if getattr(database, "_source_component", None) is None:
        async with database.engine.begin() as connection:
            await connection.run_sync(lambda sync: native._apply(sync, "20260920100000"))
    async with database.engine.begin() as connection:
        if getattr(database, "_source_component", None) is None:
            await connection.run_sync(lambda sync: native._apply(sync, "20260929000000"))
        await _create_result_models(connection, physical=physical)
        await _own_pair_result_tables(database, connection)
        return await _pair_profile_custody(connection)


async def _assert_pair_profile_reads(database, connection, expected):
    """Check the exact prepared heaps through the original authenticated Source owner."""
    component = getattr(database, "_source_component", None)
    if component is not None:
        actor = (await connection.execute(text("SELECT session_user::text,current_user::text"))).one()
        assert tuple(actor) == (component.publisher, component.owner)
        assert all(owner == component.owner for _name, _oid, owner in expected)
        assert await _pair_profile_custody(connection) == expected


async def _pair_profile_custody(connection):
    """Pin the three exact native result heaps and require current-actor reads."""
    names = ("provider_directory_profile", "provider_directory_profile_evidence", "provider_directory_address_overlay")
    rows = (
        (
            await connection.execute(
                text(
                    "SELECT c.relname,c.oid::bigint AS oid,c.relkind::text AS kind,r.rolname AS owner_name,"
                    "has_table_privilege(current_user,c.oid,'SELECT') AS can_read FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_roles r ON r.oid=c.relowner "
                    "WHERE n.nspname='mrf' AND c.relname=ANY(CAST(:names AS text[])) ORDER BY c.relname"
                ),
                {"names": list(names)},
            )
        )
        .mappings()
        .all()
    )
    assert {row["relname"] for row in rows} == set(names)
    assert all(
        row["kind"] == "r" and type(row["oid"]) is int and row["oid"] > 0 and row["can_read"] is True for row in rows
    )
    return tuple((row["relname"], row["oid"], row["owner_name"]) for row in rows)


async def _own_pair_result_tables(database, connection):
    """Assign only newly-created native result heaps and their sequences to SourceOwner."""
    component = getattr(database, "_source_component", None)
    if component is None:
        return
    names = set(serving._NATIVE_RELATIONS) | {"provider_directory_profile_serving_generation"}
    for name in sorted(names):
        oid = await connection.scalar(text("SELECT to_regclass(:name)::oid"), {"name": "mrf." + name})
        if oid is not None:
            await connection.execute(text(f'ALTER TABLE mrf."{name}" OWNER TO "{component.owner}"'))
    sequences = await connection.execute(
        text(
            "SELECT DISTINCT seq.relname FROM pg_class seq JOIN pg_depend dep ON dep.objid=seq.oid "
            "JOIN pg_class heap ON heap.oid=dep.refobjid WHERE seq.relkind='S' AND heap.relnamespace='mrf'::regnamespace "
            "AND heap.relname=ANY(:names) AND dep.deptype IN ('a','i')"
        ),
        {"names": sorted(names)},
    )
    for name in sequences.scalars():
        await connection.execute(text(f'ALTER SEQUENCE mrf."{name}" OWNER TO "{component.owner}"'))


@asynccontextmanager
async def _pair_publication_transaction(database):
    """Keep legacy receipt proofs and genuine SOURCE final owner contexts distinct."""
    if getattr(database, "_source_component", None) is None:
        async with database.engine.begin() as connection:
            yield connection
    else:
        async with support.original_source_transaction(database, isolation="read committed") as session:
            yield session


def _publication_proof(handoff, cms_pin_by_field, receipt_payload, receipt_id, receipt_row):
    """Bind the actual admission handoff to its committed native receipt values."""
    coordinates = RegistryNetworkSourceCoordinates(
        **{field: handoff["network_bindings"][field] for field in RegistryNetworkSourceCoordinates.__dataclass_fields__}
    )
    source_pin = PinnedFHIRMembershipSource(
        "mrf",
        "cms-npd",
        handoff["endpoint_id"],
        cms_pin_by_field["dataset_id"],
        handoff["dataset_sha256"],
        handoff["release_id"],
        handoff["resource_table_oid"],
        "cms-npd",
        handoff["semantic_projection_as_of"],
    )
    return RegistryCMSPublicationProof(
        source_pin,
        coordinates,
        receipt_id,
        tuple(
            receipt_payload["cms"][field]
            for field in (
                "source_id",
                "endpoint_id",
                "dataset_id",
                "dataset_hash",
                "acquisition_root_run_id",
                "release_id",
                "proof_version",
            )
        ),
        receipt_row[0],
        receipt_row[1],
        receipt_payload["selection"]["proof_id"],
        handoff["expected_admission_sha256"],
        handoff["expected_metadata_sha256"],
    )


def _mismatched_proofs(proof):
    for field, changed in (
        ("publication_xid", str(int(proof.publication_xid) + 1)),
        ("profile_generation_id", "pdprofile_" + "0" * 32),
        ("selection_proof_id", "0" * 64),
        ("expected_admission_sha256", "0" * 64),
        ("expected_metadata_sha256", "0" * 64),
    ):
        yield replace(proof, **{field: changed})
    authority_fields = list(proof.cms_authority)
    authority_fields[4] = "different-root"
    yield replace(proof, cms_authority=tuple(authority_fields))
    for field, authority_index, changed in (
        ("release_id", 5, "different-release"),
        ("dataset_sha256", 3, "0" * 64),
    ):
        authority_fields = list(proof.cms_authority)
        authority_fields[authority_index] = changed
        yield replace(
            proof, source_pin=replace(proof.source_pin, **{field: changed}), cms_authority=tuple(authority_fields)
        )
    yield replace(proof, binding_coordinates=replace(proof.binding_coordinates, producer_id="different-producer"))


async def _assert_absent(database, capture_id):
    async with database.session_factory() as session:
        for schema in ("registry_cms_epoch_" + capture_id.hex, "entity_address_archive_" + capture_id.hex):
            assert (
                await session.scalar(text("SELECT oid FROM pg_namespace WHERE nspname=:schema"), {"schema": schema})
                is None
            )


async def _reject_mismatches(database, proof, roles):
    for mismatch in _mismatched_proofs(proof):
        capture_id = uuid4()
        with pytest.raises(ValueError):
            await capture_registry_cms_source_pair(
                database.session_factory,
                mismatch,
                capture_id=capture_id,
                owner_role=roles[0],
                runtime_roles=tuple(sorted(roles[1:])),
                source_copy=_bounded_source_copy(),
            )
        await _assert_absent(database, capture_id)


async def _cancel_capture(database, proof, roles, monkeypatch):
    capture_id = uuid4()

    async def cancelled(*_args):
        raise asyncio.CancelledError

    with monkeypatch.context() as patch:
        patch.setattr(capture, "_freeze_pair", cancelled)
        with pytest.raises(asyncio.CancelledError):
            await capture_registry_cms_source_pair(
                database.session_factory,
                proof,
                capture_id=capture_id,
                owner_role=roles[0],
                runtime_roles=tuple(sorted(roles[1:])),
                source_copy=_bounded_source_copy(),
            )
    await _assert_absent(database, capture_id)


async def _reject_copy_budget(database, proof, roles):
    """A real native byte-cap failure rolls back the whole UUID-owned candidate."""
    capture_id = uuid4()
    with pytest.raises(RuntimeError, match="COPY byte cap exceeded"):
        await capture_registry_cms_source_pair(
            database.session_factory,
            proof,
            capture_id=capture_id,
            owner_role=roles[0],
            runtime_roles=tuple(sorted(roles[1:])),
            source_copy=_bounded_source_copy(max_bytes=1),
        )
    await _assert_absent(database, capture_id)
    capture_id = uuid4()
    with pytest.raises(TimeoutError):
        await capture_registry_cms_source_pair(
            database.session_factory,
            proof,
            capture_id=capture_id,
            owner_role=roles[0],
            runtime_roles=tuple(sorted(roles[1:])),
            source_copy=replace(_bounded_source_copy(), timeout=0.000001),
        )
    await _assert_absent(database, capture_id)


async def _capture_bounded_pair(database, proof, capture_id, roles):
    """Verify actual native per-table accounting and exact replay without recopy."""
    copies = []
    capture_options_by_field = dict(
        capture_id=capture_id,
        owner_role=roles[0],
        runtime_roles=tuple(sorted(roles[1:])),
        source_copy=_bounded_source_copy(copies),
    )
    pair = await capture_registry_cms_source_pair(database.session_factory, proof, **capture_options_by_field)
    assert [name for name, _size, _remaining in copies] == [
        relation.table_name for relation in entity_address_archive_relations()
    ]
    assert all(size <= remaining for _name, size, remaining in copies)
    assert [remaining for _name, _size, remaining in copies] == [
        16 * 1024**2 - sum(size for _name, size, _remaining in copies[:index]) for index in range(7)
    ]
    assert await capture_registry_cms_source_pair(database.session_factory, proof, **capture_options_by_field) == pair
    assert len(copies) == 7
    return pair


async def _verify_current_pair(session, pair, monkeypatch, roles, physical, proof):
    """Check both wire versions, native closure and admitted CMS authority."""
    assert json.loads(pair.cms_receipt_payload)["profile"]["profile_as_of"] != proof.source_pin.as_of
    await _verify_closed_pair(session, pair, monkeypatch)
    source_index = await _copy_source_index_shapes(session, proof.source_pin.schema_name)
    clone_index = await _copy_source_index_shapes(session, pair.address_ownership.schema_name)
    assert len(source_index) == 2 and source_index == clone_index
    if physical:
        await _reject_v2_tampering(session, pair)
    else:
        await _assert_v1_wire(session, pair)
    assert json.loads(pair.cms_receipt_payload)["cms"] == dict(
        zip(capture._CMS_FIELDS, proof.cms_authority, strict=True)
    )
    for field, value in (
        ("generation_id", "pdprofile_" + "0" * 32),
        ("selection_proof_id", "0" * 64),
        ("profile_as_of", "2026-10-15T00:00:00"),
    ):
        changed = json.loads(pair.cms_receipt_payload)
        changed["profile"][field] = value
        with pytest.raises(ValueError, match="source_pair_invalid"):
            replace(pair, cms_receipt_payload=json.dumps(changed))
    await _reject_address_write_grant(session, pair, roles[1])


@pytest.mark.asyncio
@pytest.mark.parametrize("physical", [False, True], ids=["v1", "v2"])
async def test_native_pair_capture_replay_and_closed_custody(monkeypatch, tmp_path, physical):
    """Verify retained custody and replay with independent source and Profile dates."""
    _retained_office_resources(monkeypatch)
    directory, acquired = support.retained_release(tmp_path)
    capture_id = uuid4()
    roles = tuple("pair_" + uuid4().hex for _ in range(2))
    async with support.protected_admission_database(monkeypatch) as database:
        with support.release_probe_client(directory) as client:
            initial = await support.run_protected_admission(
                database, monkeypatch, "pair-admission", directory, acquired, client
            )
        profile_as_of = "2026-10-15"
        proof = await _publish_pair(database, initial, monkeypatch, physical=physical, profile_as_of=profile_as_of)
        try:
            async with database.engine.begin() as connection:
                for role in roles:
                    await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
            await _reject_mismatches(database, proof, roles)
            await _cancel_capture(database, proof, roles, monkeypatch)
            await _reject_copy_budget(database, proof, roles)
            pair = await _capture_bounded_pair(database, proof, capture_id, roles)
            assert (pair.witness_json is not None) is physical
            assert len(capture._json(pair.as_dict()).encode()) <= 65536
            assert pair.recipe.source_pin.schema_name == "mrf"
            assert len(pair.address_receipt.tables) == 7
            assert pair.address_receipt.tables[0].row_count == 2
            assert decode_registry_cms_source_pair(pair.as_dict()) == pair
            await _verify_read_pair(database, pair, monkeypatch, published=True)
            await _verify_retained_entity_read(database, pair, monkeypatch)
            await _reject_read_pair_comments(database, pair)
            await _reject_read_pair_write_grant(database, pair)
            async with database.session_factory() as session, session.begin():
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                assert await require_registry_cms_source_pair(session, pair) == pair
                assert (
                    await require_registry_cms_source_pair_connection(await capture.native_driver(session), pair)
                    == pair
                )
                await _verify_current_pair(session, pair, monkeypatch, roles, physical, proof)
            if physical:
                await _verify_physical_pair_replay(database, pair, proof, roles, monkeypatch)
        finally:
            async with database.engine.begin() as connection:
                for schema in ("registry_cms_epoch_" + capture_id.hex, "entity_address_archive_" + capture_id.hex):
                    await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
                for role in roles:
                    await connection.execute(text(f'DROP OWNED BY "{role}" CASCADE'))
                    await connection.execute(text(f'DROP ROLE "{role}"'))
            await _assert_absent(database, capture_id)


async def _verify_physical_pair_replay(database, pair, proof, roles, monkeypatch):
    await capture._cleanup_capture(database.session_factory, pair.address_ownership)
    async with database.session_factory() as session:
        assert (
            await session.scalar(
                text("SELECT oid FROM pg_namespace WHERE nspname=:schema"),
                {"schema": pair.address_ownership.schema_name},
            )
            == pair.address_ownership.schema_oid
        )
    await _prove_lost_acknowledgement(database, proof, roles, monkeypatch)


async def _verify_closed_pair(session, pair, monkeypatch):
    """The final closure seam performs no semantic content rescans."""
    with monkeypatch.context() as patch:

        async def no_content_scan(*_args, **_kwargs):
            raise AssertionError("final publication must not rescan address contents")

        patch.setattr(capture, "capture_entity_address_archive_receipt", no_content_scan)
        patch.setattr(
            "process.network_cms_registry_address_capture.capture_entity_address_archive_receipt",
            no_content_scan,
        )
        assert (
            await require_registry_cms_source_pair_connection(
                await capture.native_driver(session), pair, verify_content=False
            )
            == pair
        )


async def _read_settings(session):
    return tuple(
        (
            await session.execute(
                text("""SELECT current_setting('work_mem'),current_setting('statement_timeout'),
                current_setting('search_path'),current_setting('transaction_isolation'),
                current_setting('transaction_read_only'),current_setting('TimeZone'),
                current_setting('DateStyle'),current_setting('extra_float_digits')""")
            )
        ).one()
    )


async def _read_transaction(session):
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
    await session.execute(text("SET LOCAL statement_timeout='2s'"))
    await session.execute(text("SET LOCAL work_mem='4MB'"))
    await session.execute(text("SET LOCAL search_path=pg_catalog,public"))


async def _verify_read_pair(database, pair, monkeypatch, *, published):
    """Real custody checks keep every caller setting on success and caught refusal."""
    async with database.session_factory() as session, session.begin():
        await _read_transaction(session)
        before = await _read_settings(session)
        assert before[:3] == ("4MB", "2s", "pg_catalog, public")
        with monkeypatch.context() as patch:

            async def forbidden_scan(*_args, **_kwargs):
                raise AssertionError("read resolver must not normalize or scan source content")

            patch.setattr(capture, "_normalize_receipt_session", forbidden_scan)
            patch.setattr(
                "process.network_cms_registry_address_equivalence.receipt._normalize_receipt_session", forbidden_scan
            )
            patch.setattr(
                "process.network_cms_registry_address_capture.capture_entity_address_archive_receipt", forbidden_scan
            )
            patch.setattr("process.network_fhir_source_epoch._content", forbidden_scan)
            resolved = await capture.resolve_committed_registry_cms_source_pair(session, pair.recipe)
            assert resolved.pair == pair
            assert (resolved.source_published_at is not None) is published
            assert await _read_settings(session) == before
            wrong_coordinates = replace(pair.recipe.binding_coordinates, producer_id="different-producer")
            with pytest.raises(ValueError, match="registry_cms_source_pair_invalid"):
                await capture.resolve_committed_registry_cms_source_pair(
                    session, replace(pair.recipe, binding_coordinates=wrong_coordinates)
                )
            assert await _read_settings(session) == before


async def _reject_read_pair_comments(database, pair):
    """An epoch-derived name cannot authenticate prepared, oversized or forged evidence."""
    original = capture._pair_comment(pair) + capture._json(pair.as_dict())
    forged = pair.as_dict()
    forged["proof"]["serving_receipt_id"] = "0" * 64
    comments = [
        capture._STAGE_COMMENT + "{}",
        capture._COMMENT + " " * 65537,
        capture._COMMENT + "{}",
        capture._pair_comment(pair) + capture._json(forged),
    ]
    if pair.witness_json is not None:
        forged_witness = pair.as_dict()
        forged_witness["address_copy_witness"]["clone"]["catalog_sha256"] = "0" * 64
        comments.append(capture._pair_comment(pair) + capture._json(forged_witness))
    try:
        for comment in comments:
            async with database.session_factory() as session, session.begin():
                escaped = comment.replace("'", "''")
                await (await capture.native_driver(session)).execute(
                    f"COMMENT ON SCHEMA \"{pair.address_source.schema_name}\" IS '{escaped}'"
                )
            async with database.session_factory() as session, session.begin():
                await _read_transaction(session)
                before = await _read_settings(session)
                with pytest.raises(ValueError):
                    await capture.resolve_committed_registry_cms_source_pair(session, pair.recipe)
                assert await _read_settings(session) == before
    finally:
        async with database.session_factory() as session, session.begin():
            escaped = original.replace("'", "''")
            await (await capture.native_driver(session)).execute(
                f"COMMENT ON SCHEMA \"{pair.address_source.schema_name}\" IS '{escaped}'"
            )


async def _reject_read_pair_write_grant(database, pair):
    """Actual opened native custody is refused without changing reader settings."""
    schema, role = pair.address_source.schema_name, pair.runtime_roles[0]
    try:
        async with database.session_factory() as session, session.begin():
            await session.execute(text(f'GRANT UPDATE ON "{schema}".entity_address_unified TO "{role}"'))
        async with database.session_factory() as session, session.begin():
            await _read_transaction(session)
            before = await _read_settings(session)
            with pytest.raises(ValueError):
                await capture.resolve_committed_registry_cms_source_pair(session, pair.recipe)
            assert await _read_settings(session) == before
    finally:
        async with database.session_factory() as session, session.begin():
            await session.execute(text(f'REVOKE UPDATE ON "{schema}".entity_address_unified FROM "{role}"'))


async def _reject_address_write_grant(session, pair, runtime_role):
    """Both semantic and cheap final validators reject newly opened write custody."""
    await session.execute(
        text(f'GRANT UPDATE ON "{pair.address_source.schema_name}".entity_address_unified TO "{runtime_role}"')
    )
    with pytest.raises(ValueError):
        await require_registry_cms_source_pair(session, pair)
    with pytest.raises(ValueError):
        await require_registry_cms_source_pair_connection(
            await capture.native_driver(session), pair, verify_content=False
        )


async def _assert_v1_wire(session, pair):
    """Preserve the exact historical nine-key encoder, hash and native comment."""
    from dataclasses import asdict

    from process.registry_source_recipe_store import canonical_registry_source_recipes

    legacy_by_field = {
        "proof": pair.proof.as_dict(),
        "recipes": json.loads(canonical_registry_source_recipes((pair.recipe,))),
        "address_source": asdict(pair.address_source),
        "address_ownership": pair.address_ownership.as_dict(),
        "address_receipt": pair.address_receipt.as_dict(),
        "address_catalog_sha256": pair.address_catalog_sha256,
        "cms_receipt_payload": json.loads(pair.cms_receipt_payload),
        "owner_role": pair.owner_role,
        "runtime_roles": list(pair.runtime_roles),
    }
    assert pair.as_dict() == legacy_by_field
    assert pair.pair_sha256 == capture._digest(legacy_by_field)
    comment = await session.scalar(
        text("SELECT obj_description(:oid,'pg_namespace')"), {"oid": pair.address_ownership.schema_oid}
    )
    assert comment == capture._COMMENT + capture._json(legacy_by_field)
    assert decode_registry_cms_source_pair(json.loads(capture._json(legacy_by_field))) == pair


async def _reject_v2_tampering(session, pair):
    """Counterfeit checkpoint evidence never bypasses the protected native comment."""
    from copy import deepcopy

    original = pair.as_dict()
    witness = original["address_copy_witness"]
    assert witness["source"]["receipt"] != witness["clone"]["receipt"]
    changes = (
        ((), "unknown", 1),
        (("source",), "schema_name", "foreign_schema"),
        (("clone",), "schema_oid", pair.address_ownership.schema_oid + 1),
        (("source",), "catalog_sha256", "0" * 64),
        (("clone",), "catalog_sha256", "0" * 64),
        (("clone", "sequence"), "sequence_oid", witness["clone"]["sequence"]["sequence_oid"] + 1),
        (("source", "sequence", "settings"), "seqincrement", 2),
        (("source", "receipt", "tables", 0), "row_count", 99),
        (("source", "relation_oids", 0), 1, witness["source"]["relation_oids"][0][1] + 1),
        (("clone", "relation_oids", 0), 1, witness["clone"]["relation_oids"][0][1] + 1),
        (("source", "ordinals", 0, 1, 0), 2, 99),
    )
    for path, key, changed in changes:
        document = deepcopy(original)
        destination = document["address_copy_witness"]
        for part in path:
            destination = destination[part]
        destination[key] = changed
        try:
            counterfeit = decode_registry_cms_source_pair(document)
        except ValueError:
            continue
        for full in (True, False):
            with pytest.raises(ValueError):
                await require_registry_cms_source_pair(session, counterfeit, verify_content=full)
    assert await require_registry_cms_source_pair(session, pair) == pair


async def _prove_lost_acknowledgement(database, proof, roles, monkeypatch):
    """A v2 commit survives acknowledgement loss and deterministic replay."""
    capture_id = uuid4()
    original = capture._freeze_pair

    async def lost_ack(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("synthetic_lost_acknowledgement")

    try:
        with monkeypatch.context() as patch:
            patch.setattr(capture, "_freeze_pair", lost_ack)
            with pytest.raises(RuntimeError, match="synthetic_lost_acknowledgement"):
                await capture_registry_cms_source_pair(
                    database.session_factory,
                    proof,
                    capture_id=capture_id,
                    owner_role=roles[0],
                    runtime_roles=tuple(sorted(roles[1:])),
                    source_copy=_bounded_source_copy(),
                )
        retained = await capture_registry_cms_source_pair(
            database.session_factory,
            proof,
            capture_id=capture_id,
            owner_role=roles[0],
            runtime_roles=tuple(sorted(roles[1:])),
            source_copy=_bounded_source_copy(),
        )
        assert retained.witness_json is not None
    finally:
        async with database.engine.begin() as connection:
            for schema in ("registry_cms_epoch_" + capture_id.hex, "entity_address_archive_" + capture_id.hex):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


async def _prepare_retained_entity_controls(database, pair):
    recipe = pair.recipe
    async with database.session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        if await session.scalar(text("SELECT to_regclass('mrf.network_registry_identity')")) is None:
            connection = await session.connection()
            await connection.run_sync(lambda sync: native._apply(sync, "20261007010000"))
        await session.execute(
            text(
                "CREATE TABLE IF NOT EXISTS mrf.registry_approved_record (approved_revision bigint,record_kind text,record_key text,record_revision bigint,custom_revision bigint,record_json jsonb)"
            )
        )
        for definition in (
            "CREATE TABLE IF NOT EXISTS mrf.network_membership_source (network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)",
            "CREATE TABLE IF NOT EXISTS mrf.network_membership (LIKE mrf.network_membership_source)",
            "CREATE TABLE IF NOT EXISTS mrf.provider_location_binding (provider_system text,provider_id text,location_id uuid)",
        ):
            await session.execute(text(definition))
        legacy_id = await session.scalar(
            text(
                f"SELECT network_id FROM \"{recipe.source_pin.read_schema_name}\".provider_directory_insurance_network_source_binding WHERE resource_id='network-1'"
            )
        )
        for kind, key, document in (
            ("network", "1001", {"network_id": 1001, "archived": False}),
            (
                "network_binding",
                "test-binding",
                {
                    **asdict(recipe.binding_coordinates),
                    "network_id": 1001,
                    "archived": False,
                    "source_key": "network-1",
                    "source_scope_json": {
                        "organization_id": "network-1",
                        "legacy_uuid": str(legacy_id),
                        "alias_scope": recipe.source_pin.alias_scope,
                    },
                },
            ),
        ):
            await session.execute(
                text("INSERT INTO mrf.registry_approved_record VALUES(7,:kind,:key,1,7,CAST(:value AS jsonb))"),
                {"kind": kind, "key": key, "value": json.dumps(document)},
            )
        await _retained_office_membership(session, recipe, legacy_id)


async def _retained_office_membership(session, recipe, legacy_id):
    """Use real retained extraction facts; only the canonical approval snapshot is a test seam."""
    await session.execute(
        text(
            "INSERT INTO mrf.network_registry_identity(network_id,allocation_key) OVERRIDING SYSTEM VALUE VALUES(1001,:key) ON CONFLICT DO NOTHING"
        ),
        {"key": uuid4()},
    )
    await session.execute(
        text(
            "INSERT INTO mrf.network_registry_alias VALUES('fhir','cms-npd','legacy_fhir_uuid',:legacy,:scope,1001,'test-approved',now()) ON CONFLICT DO NOTHING"
        ),
        {"legacy": str(legacy_id), "scope": recipe.source_pin.alias_scope},
    )
    driver = await capture.native_driver(session)
    batch = await read_fhir_membership_batch(
        driver, recipe.source_pin, registry_schema="mrf", after_resource=("OrganizationAffiliation", "affiliation-1")
    )
    assert batch.unresolved_rows == 0 and batch.membership_rows == 3
    assert batch.source_rows == batch.accepted_source_rows == 2 and batch.complete_raw_facts
    facts = json.loads(batch.input_bytes)
    site_id_by_resource = dict(
        await driver.fetch(
            f"SELECT resource_id,site_id FROM \"{recipe.source_pin.read_schema_name}\".provider_directory_entity_source_binding WHERE resource_type='Location' AND source_id='cms-npd'"
        )
    )
    assert {membership_by_field["location_id"] for membership_by_field in facts} == {
        str(site_id_by_resource["site-1"]),
        str(site_id_by_resource["site-2"]),
    }
    assert {membership_by_field["provider_system"] for membership_by_field in facts} == {"npi"}
    assert {membership_by_field["provider_id"] for membership_by_field in facts} == {"1000000491"}
    for table in ("network_membership_source", "network_membership", "provider_location_binding"):
        await session.execute(text("DELETE FROM mrf." + table))
    for fact in facts:
        membership_by_field = {**fact, "location_id": UUID(fact["location_id"])}
        await session.execute(
            text(
                "INSERT INTO mrf.network_membership_source VALUES(:network_id,:provider_system,:provider_id,:location_id,:evidence_id)"
            ),
            membership_by_field,
        )
        await session.execute(
            text("""INSERT INTO mrf.provider_location_binding
              SELECT :provider_system,:provider_id,:location_id WHERE NOT EXISTS(
              SELECT 1 FROM mrf.provider_location_binding WHERE provider_system=:provider_system
                AND provider_id=:provider_id AND location_id=:location_id)"""),
            membership_by_field,
        )
        if membership_by_field["location_id"] == site_id_by_resource["site-1"]:
            await session.execute(
                text(
                    "INSERT INTO mrf.network_membership VALUES(:network_id,:provider_system,:provider_id,:location_id,:evidence_id)"
                ),
                membership_by_field,
            )


@contextmanager
def _retained_entity_scope(pair, monkeypatch):
    recipe = pair.recipe

    async def manifest(*_args, **_kwargs):
        return PinnedNetworkServingManifest(9, str(uuid4()), "mrf", 1, {}, 7, "c" * 64, 1)

    async def recipes(*_args, **_kwargs):
        return (recipe,)

    async def approved(*_args, **_kwargs):
        return ApprovedMembershipSource(7, "d" * 64)

    async def no_current(*_args, **_kwargs):
        raise AssertionError("retained read must not consult the current source selector")

    with monkeypatch.context() as patch:
        patch.setattr(scoped, "resolve_network_serving_manifest", manifest)
        patch.setattr(scoped, "resolve_retained_registry_source_recipes", recipes)
        patch.setattr(scoped, "pin_retained_approved_membership_source", approved)
        patch.setattr(scoped, "registry_schema", lambda: "mrf")
        patch.setattr(scoped, "accepted_cms_generation", no_current)
        patch.setattr("api.provider_directory_cms_entities.accepted_cms_generation", no_current)

        yield


async def _read_retained_entity(database, query):
    headers_by_name = {}
    async with database.session_factory() as session:
        page = await read_cms_entities(session, query, response_headers=headers_by_name)
        assert not session.in_transaction()
    assert headers_by_name["X-CMS-Retained-Publication"] == "committed/v1"
    return page


async def _verify_retained_payer_closure(database, recipe, query, page):
    async with database.session_factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO mrf.mrf_payer(payer_id,canonical_name,lifecycle) VALUES('selected-payer','Example Payer','active') ON CONFLICT DO NOTHING"
            )
        )
        payload_hash = await session.scalar(
            text(
                "SELECT payload_sha256 FROM mrf.provider_directory_entity_release_evidence WHERE resource_id='insurer-1' AND release_id=:release"
            ),
            {"release": recipe.source_pin.release_id},
        )
        decision = ReviewedCMSPayerDecision(
            uuid4(),
            "bind",
            "cms-npd",
            "insurer-1",
            recipe.source_pin.release_id,
            payload_hash,
            "selected-payer",
            None,
            "test-review",
            "f" * 64,
            "test-reviewer",
            datetime.now(timezone.utc),
        )
        await record_reviewed_cms_payer_decision(session, decision)
    payers = await _read_retained_entity(database, replace(query, kind="payers"))
    assert [payer_entry["id"] for payer_entry in payers["items"]] == ["selected-payer"]
    async with database.session_factory() as session, session.begin():
        await record_reviewed_cms_payer_decision(
            session, replace(decision, decision_id=uuid4(), action="close", prior_decision_id=decision.decision_id)
        )
    assert (await _read_retained_entity(database, replace(query, kind="payers")))["items"] == []
    with pytest.raises(DirectoryReadError) as stale:
        await _read_retained_entity(database, replace(query, generation_id=page["generation_id"]))
    assert stale.value.status == 409


async def _verify_retained_entity_read(database, pair, monkeypatch, *, revision="first", unknown=False):
    """Ordinary entity reads authenticate actual custody, source bytes and current revocation."""
    monkeypatch.setenv(CURSOR_KEY_ENV, "A" * 43)
    await _prepare_retained_entity_controls(database, pair)
    query = DirectoryRead("organizations", "cms-npd", "entities", None, None, 100, None, (1001,), 9, "e" * 64)
    with _retained_entity_scope(pair, monkeypatch):
        page = await _read_retained_entity(database, query)
        assert {row["display_name"] for row in page["items"]} == {"Example Network " + revision, "Example Insurer"}
        assert all((item["evidence"][0]["observed_at"] is None) is unknown for item in page["items"])
        plans = await _read_retained_entity(database, replace(query, kind="plans"))
        assert len(plans["items"]) == 1
        links = await _read_retained_entity(
            database, replace(query, kind="plans", shape="relationships", entity_id=plans["items"][0]["id"])
        )
        assert links["items"] and all(row["target_id"] is not None for row in links["items"])
        office_generations = await _verify_retained_offices(database, pair, query, revision, unknown)
        await _verify_retained_payer_closure(database, pair.recipe, query, page)
        refreshed = await _verify_retained_offices(database, pair, query, revision, unknown)
        assert all(refreshed[kind] != generation for kind, generation in office_generations.items())
        for kind, generation in office_generations.items():
            with pytest.raises(DirectoryReadError) as stale:
                await _read_retained_entity(database, replace(query, kind=kind, generation_id=generation))
            assert stale.value.status == 409


async def _verify_retained_offices(database, pair, query, revision, unknown):
    """The complete retained source proves both offices; effective approval permits only A."""
    async with database.session_factory() as session:
        site_id_by_resource = dict(
            (
                await session.execute(
                    text(
                        f"SELECT resource_id,site_id FROM \"{pair.recipe.source_pin.read_schema_name}\".provider_directory_entity_source_binding WHERE resource_type='Location' AND source_id='cms-npd'"
                    )
                )
            ).all()
        )
    generation_by_kind = {}
    for kind in ("sites", "practitioner-roles"):
        office_page = await _read_retained_entity(database, replace(query, kind=kind, limit=1))
        assert len(office_page["items"]) == 1 and office_page["next_cursor"] is None
        office_entry = office_page["items"][0]
        generation_by_kind[kind] = office_page["generation_id"]
        assert (office_entry["evidence"][0]["observed_at"] is None) is unknown
        if kind == "sites":
            assert (
                office_entry["id"] == str(site_id_by_resource["site-1"])
                and office_entry["display_name"] == "Office A " + revision
            )
            hidden = str(site_id_by_resource["site-2"])
        else:
            from process.provider_directory_resource_identity import source_resource_uuid

            assert office_entry["id"] == str(source_resource_uuid("cms-npd", "PractitionerRole", "role-1"))
            hidden = str(source_resource_uuid("cms-npd", "PractitionerRole", "role-2"))
        detail = await _read_retained_entity(
            database, replace(query, kind=kind, shape="entity", entity_id=office_entry["id"])
        )
        assert detail["item"] == office_entry
        for shape in ("entity", "relationships"):
            with pytest.raises(DirectoryReadError) as denied:
                await _read_retained_entity(database, replace(query, kind=kind, shape=shape, entity_id=hidden))
            assert denied.value.status == 404
        if kind == "practitioner-roles":
            links = await _read_retained_entity(
                database, replace(query, kind=kind, shape="relationships", entity_id=office_entry["id"])
            )
            assert [
                relationship_entry["target_id"]
                for relationship_entry in links["items"]
                if relationship_entry["target_kind"] == "sites"
            ] == [str(site_id_by_resource["site-1"])]
            assert [
                relationship_entry["provider_npi"]
                for relationship_entry in links["items"]
                if relationship_entry["target_kind"] == "providers"
            ] == ["1000000491"]
    return generation_by_kind


def _bounded_source_copy(copies=None, *, max_bytes=16 * 1024**2):
    """Use the real bounded native driver with explicit disposable-fixture limits."""

    async def copy_rows(session, query, **kwargs):
        size = await native_copy_projection(session, query, **kwargs)
        if copies is not None:
            copies.append((kwargs["table_name"], size, kwargs["max_bytes"]))
        return size

    return EntityAddressSourceCopy(copy_rows, max_bytes, 30)


@pytest.mark.asyncio
async def test_compatibility_capture_requires_explicit_copy_before_session():
    """Missing trusted bounds cannot open a transaction or select the legacy fallback."""
    from unittest.mock import Mock

    sessions = Mock(side_effect=AssertionError("database must not be opened"))
    with pytest.raises(ValueError, match="bounded native COPY"):
        await capture_registry_cms_source_pair(
            sessions, None, capture_id=uuid4(), owner_role="synthetic_owner", runtime_roles=("synthetic_reader",)
        )
    sessions.assert_not_called()
