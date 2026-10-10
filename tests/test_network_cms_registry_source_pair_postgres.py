# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual eight-file admission paired with the native seven-table capture contract."""

import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.schema import CreateTable

from db.connection import Base
from process import network_cms_registry_source_pair as capture
from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_serving_receipt as serving
from process import provider_directory_profile as profile
from process.entity_address_snapshot_source import entity_address_unified
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_cms_registry_source_pair import (
    RegistryCMSPublicationProof,
    capture_registry_cms_source_pair,
    decode_registry_cms_source_pair,
    require_registry_cms_source_pair,
    require_registry_cms_source_pair_connection,
)
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from tests import cms_npd_admission_postgres_support as support
from tests import test_provider_directory_cms_serving_receipt_postgres as native
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template


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


async def _publish_pair(database, initial, monkeypatch, *, physical=False, selection_proof_id=None):
    """Commit real sealed source scalars and native result authorities together."""
    handoff = initial["registry_source_admission"]
    cms_pin_by_field = {field: initial["cms_serving_candidate"]["desired_cms_dataset"][field] for field in native._PIN}
    monkeypatch.setattr(native, "_PIN", cms_pin_by_field)
    async with database.engine.begin() as connection:
        await connection.run_sync(lambda sync: native._apply(sync, "20260920100000"))
    async with database.engine.begin() as connection:
        await connection.run_sync(lambda sync: native._apply(sync, "20260929000000"))
        await _create_result_models(connection, physical=physical)
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
            {"as_of": handoff["semantic_projection_as_of"]},
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
                runtime_roles=(roles[1],),
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
                database.session_factory, proof, capture_id=capture_id, owner_role=roles[0], runtime_roles=(roles[1],)
            )
    await _assert_absent(database, capture_id)


async def _verify_current_pair(session, pair, monkeypatch, roles, physical, proof):
    """Check both wire versions, native closure and admitted CMS authority."""
    await _verify_closed_pair(session, pair, monkeypatch)
    if physical:
        await _reject_v2_tampering(session, pair)
    else:
        await _assert_v1_wire(session, pair)
    assert json.loads(pair.cms_receipt_payload)["cms"] == dict(
        zip(capture._CMS_FIELDS, proof.cms_authority, strict=True)
    )
    await _reject_address_write_grant(session, pair, roles[1])


@pytest.mark.asyncio
@pytest.mark.parametrize("physical", [False, True], ids=["v1", "v2"])
async def test_native_pair_capture_replay_and_closed_custody(monkeypatch, tmp_path, physical):
    directory, acquired = support.retained_release(tmp_path)
    capture_id = uuid4()
    roles = tuple("pair_" + uuid4().hex for _ in range(2))
    async with support.admission_database(monkeypatch) as database:
        with support.release_probe_client(directory) as client:
            initial = await cms._run_acquired({"context": {}}, {}, "pair-admission", directory, acquired, client)
        proof = await _publish_pair(database, initial, monkeypatch, physical=physical)
        try:
            async with database.engine.begin() as connection:
                for role in roles:
                    await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
            await _reject_mismatches(database, proof, roles)
            await _cancel_capture(database, proof, roles, monkeypatch)
            capture_options_by_field = dict(capture_id=capture_id, owner_role=roles[0], runtime_roles=(roles[1],))
            pair = await capture_registry_cms_source_pair(database.session_factory, proof, **capture_options_by_field)
            assert (pair.witness_json is not None) is physical
            assert len(capture._json(pair.as_dict()).encode()) <= 65536
            assert pair.recipe.source_pin.schema_name == "mrf"
            assert len(pair.address_receipt.tables) == 7
            assert pair.address_receipt.tables[0].row_count == 2
            assert decode_registry_cms_source_pair(pair.as_dict()) == pair
            assert (
                await capture_registry_cms_source_pair(database.session_factory, proof, **capture_options_by_field)
                == pair
            )
            async with database.session_factory() as session, session.begin():
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                assert await require_registry_cms_source_pair(session, pair) == pair
                assert (
                    await require_registry_cms_source_pair_connection(await capture.native_driver(session), pair)
                    == pair
                )
                await _verify_current_pair(session, pair, monkeypatch, roles, physical, proof)
            if physical:
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
        finally:
            async with database.engine.begin() as connection:
                for schema in ("registry_cms_epoch_" + capture_id.hex, "entity_address_archive_" + capture_id.hex):
                    await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
                for role in roles:
                    await connection.execute(text(f'DROP OWNED BY "{role}" CASCADE'))
                    await connection.execute(text(f'DROP ROLE "{role}"'))
            await _assert_absent(database, capture_id)


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
                    runtime_roles=(roles[1],),
                )
        retained = await capture_registry_cms_source_pair(
            database.session_factory, proof, capture_id=capture_id, owner_role=roles[0], runtime_roles=(roles[1],)
        )
        assert retained.witness_json is not None
    finally:
        async with database.engine.begin() as connection:
            for schema in ("registry_cms_epoch_" + capture_id.hex, "entity_address_archive_" + capture_id.hex):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
