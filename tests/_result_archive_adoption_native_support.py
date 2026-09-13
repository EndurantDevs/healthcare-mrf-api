# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reusable native fixture support for result-archive adoption tests."""

from __future__ import annotations

import json
import uuid
from contextlib import suppress
from pathlib import Path

import pytest
from sqlalchemy import text

from api import ptg2_billing_entity_refs as billing_refs
from api import ptg2_billing_entity_source_resolution as source_resolution
from api.ptg2_shared_blocks import fetch_shared_blocks
from db.connection import Database
from process.ptg_parts import ptg2_v4_finalizer_publish as finalizer_publish
from process.ptg_parts import result_archive_adoption as adoption
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    tax_identity_source_publication_from_metadata,
)
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
    has_valid_finalizer_map,
)
from scripts.research import ptg2_packed_finalizer_abba_lifecycle as finalizer_lifecycle
from scripts.research.ptg2_packed_finalizer_abba_artifacts import generate_artifacts
from tests import test_ptg2_v4_postgres_e2e as v4_e2e
from tests.test_ptg2_packed_finalizer_wrapper_postgres import _tiny_shape

_TAX_SOURCE_RECEIPT_TABLES = (
    "ptg2_provider_tax_identity_manifest",
    "ptg2_provider_tax_identity",
    "ptg2_provider_group_tax_identity",
    "ptg2_provider_tax_identity_source_manifest",
    "ptg2_provider_tax_identity_source_binding",
    "ptg2_provider_group_tax_identity_source",
)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


async def _publish_native_finalizer_fixture(
    database: Database,
    *,
    schema_name: str,
    source_snapshot_key: int,
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Attach a producer-created packed finalizer map to the sealed source layout."""

    source_build_token, finalizer_build_token = await _enter_finalizer_build(
        database,
        schema_name=schema_name,
        source_snapshot_key=source_snapshot_key,
    )
    manifest = await _publish_finalizer_artifacts(
        database,
        schema_name=schema_name,
        source_snapshot_key=source_snapshot_key,
        finalizer_build_token=finalizer_build_token,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )
    await _seal_finalizer_fixture(
        database,
        schema_name=schema_name,
        source_snapshot_key=source_snapshot_key,
        source_build_token=source_build_token,
        manifest=manifest,
    )


async def _enter_finalizer_build(
    database: Database,
    *,
    schema_name: str,
    source_snapshot_key: int,
) -> tuple[str, str]:
    """Reopen the disposable native layout for its producer-backed finalizer."""

    schema = _quoted(schema_name)
    source_build_token = await database.scalar(
        f"SELECT build_token FROM {schema}.ptg2_v3_snapshot_layout WHERE snapshot_key = :snapshot_key",
        snapshot_key=source_snapshot_key,
    )
    finalizer_build_token = f"receiver-finalizer-{uuid.uuid4().hex[:16]}"
    await database.status(
        f"""
        UPDATE {schema}.ptg2_v3_snapshot_layout
           SET state = 'building', build_token = :build_token
         WHERE snapshot_key = :snapshot_key
        """,
        build_token=finalizer_build_token,
        snapshot_key=source_snapshot_key,
    )
    return str(source_build_token), finalizer_build_token


async def _publish_finalizer_artifacts(
    database: Database,
    *,
    schema_name: str,
    source_snapshot_key: int,
    finalizer_build_token: str,
    tmp_path: Path,
    monkeypatch,
) -> dict[str, object]:
    """Run the production finalizer producer against generated tiny artifacts."""

    work_directory = tmp_path / f"finalizer-work-{uuid.uuid4().hex[:8]}"
    artifacts = generate_artifacts(tmp_path / f"finalizer-artifacts-{uuid.uuid4().hex[:8]}", _tiny_shape())
    work_directory.mkdir()
    monkeypatch.setenv("HLTHPRT_PTG2_V3_FINALIZER_WORKERS", "1")
    monkeypatch.setenv("HLTHPRT_PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES", "67108864")
    monkeypatch.setenv("HLTHPRT_PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES", "16777216")
    monkeypatch.setattr(finalizer_lifecycle, "db", database)
    monkeypatch.setattr(finalizer_publish, "db", database)
    try:
        request = finalizer_lifecycle.ArmRequest(
            "receiver",
            True,
            schema_name,
            source_snapshot_key,
            finalizer_build_token,
            work_directory,
            artifacts,
        )
        publication_result, _elapsed_seconds, _timeline = await finalizer_lifecycle._publish_finalizer(request)
        manifest = publication_result.publication.manifest()
        assert not any(work_directory.iterdir())
    finally:
        artifacts.cleanup()
        with suppress(OSError):
            work_directory.rmdir()
    return manifest


async def _seal_finalizer_fixture(
    database: Database,
    *,
    schema_name: str,
    source_snapshot_key: int,
    source_build_token: str,
    manifest: dict[str, object],
) -> None:
    """Attach the producer receipt and verify it through the native map reader."""

    schema = _quoted(schema_name)
    await database.status(
        f"""
        UPDATE {schema}.ptg2_v3_snapshot_layout
           SET state = 'sealed', build_token = :source_build_token,
               layout_manifest = jsonb_set(
                   layout_manifest, '{{serving_index}}',
                   COALESCE(layout_manifest->'serving_index', '{{}}'::jsonb)
                       || jsonb_build_object('finalizer_mapping', CAST(:manifest AS jsonb))
               )
         WHERE snapshot_key = :snapshot_key
        """,
        source_build_token=source_build_token,
        manifest=json.dumps(manifest, sort_keys=True),
        snapshot_key=source_snapshot_key,
    )
    async with database.transaction() as session:
        assert await has_valid_finalizer_map(
            session,
            schema_name=schema_name,
            snapshot_key=source_snapshot_key,
            layout_manifest={"serving_index": {"finalizer_mapping": manifest}},
        )


async def _assert_destination_finalizer_reader(
    database: Database,
    *,
    stage_schema_name: str,
    destination_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Read producer receipts and packed finalizer payloads through destination APIs."""

    source_schema = _quoted(stage_schema_name)
    destination_schema = _quoted(destination_schema_name)
    await _assert_finalizer_receipts_match(
        database,
        source_schema=source_schema,
        destination_schema=destination_schema,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )
    await _assert_finalizer_maps_are_populated(
        database,
        source_schema=source_schema,
        destination_schema=destination_schema,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )
    await _assert_destination_finalizer_payload_reads(
        database,
        destination_schema=destination_schema,
        destination_schema_name=destination_schema_name,
        destination_snapshot_key=destination_snapshot_key,
    )


async def _assert_finalizer_receipts_match(
    database: Database,
    *,
    source_schema: str,
    destination_schema: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Require the destination root to preserve every producer completion receipt."""

    finalizer_fields = (
        "map_digest, canonical_mapping_digest, canonical_byte_count, "
        "target_identity_digest, object_kind_count, map_pack_count, "
        "coordinate_count, entry_count, logical_byte_count, "
        "stored_map_byte_count, target_block_count"
    )
    source_receipt = await database.first(
        f"SELECT {finalizer_fields} FROM {source_schema}.ptg2_v4_finalizer_map_root WHERE snapshot_key = :snapshot_key",
        snapshot_key=source_snapshot_key,
    )
    destination_receipt = await database.first(
        f"SELECT {finalizer_fields} FROM {destination_schema}.ptg2_v4_finalizer_map_root "
        "WHERE snapshot_key = :snapshot_key",
        snapshot_key=destination_snapshot_key,
    )
    assert source_receipt is not None
    assert destination_receipt == source_receipt


async def _assert_finalizer_maps_are_populated(
    database: Database,
    *,
    source_schema: str,
    destination_schema: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Reject a fixture that merely carries an empty finalizer table family."""

    for schema, snapshot_key in (
        (source_schema, source_snapshot_key),
        (destination_schema, destination_snapshot_key),
    ):
        assert (
            await database.scalar(
                f"SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_pack WHERE snapshot_key = :snapshot_key",
                snapshot_key=snapshot_key,
            )
            > 0
        )
        assert (
            await database.scalar(
                f"SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_target WHERE snapshot_key = :snapshot_key",
                snapshot_key=snapshot_key,
            )
            > 0
        )


async def _assert_destination_finalizer_payload_reads(
    database: Database,
    *,
    destination_schema: str,
    destination_schema_name: str,
    destination_snapshot_key: int,
) -> None:
    """Validate the copied map and every finalizer object kind via native readers."""

    async with database.transaction() as session:
        layout_manifest = (
            await session.execute(
                text(
                    f"SELECT layout_manifest FROM {destination_schema}.ptg2_v3_snapshot_layout "
                    "WHERE snapshot_key = :snapshot_key"
                ),
                {"snapshot_key": destination_snapshot_key},
            )
        ).scalar_one()
        assert await has_valid_finalizer_map(
            session,
            schema_name=destination_schema_name,
            snapshot_key=destination_snapshot_key,
            layout_manifest=layout_manifest,
        )
        for object_kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS:
            blocks = await fetch_shared_blocks(
                session,
                schema_name=destination_schema_name,
                snapshot_key=destination_snapshot_key,
                object_kind=object_kind,
                block_keys=(0,),
                require_all=True,
            )
            assert tuple(blocks) == (0,)
            assert len(blocks[0]) == 1


async def _prepare_tax_source_destination(
    database: Database,
    *,
    destination_schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
):
    """Reserve unrelated local keys before preparing the selected tax layout."""

    async with database.transaction() as session:
        for fingerprint, build_token in (
            (b"u" * 32, "unrelated-layout"),
            (b"v" * 32, "second-unrelated-layout"),
        ):
            await v4_e2e.reserve_v4_shared_layout(
                session,
                schema_name=destination_schema_name,
                semantic_fingerprint=fingerprint,
                build_token=build_token,
            )
        return await adoption.prepare_result_archive_layout(
            session,
            schema_name=destination_schema_name,
            staging_schema_name=staging_schema_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_id="destination-tax-snapshot",
            build_token=f"receiver-{uuid.uuid4().hex}",
        )


async def _assert_destination_tax_source_reader(
    database: Database,
    *,
    stage_schema_name: str,
    destination_schema_name: str,
    source_snapshot_key: int,
    unrelated_snapshot_key: int,
    destination_snapshot_key: int,
    source_publication_metadata: dict[str, object],
) -> None:
    """Authenticate copied source records with the destination billing resolver."""

    publication = tax_identity_source_publication_from_metadata(source_publication_metadata)
    stage = _quoted(stage_schema_name)
    destination = _quoted(destination_schema_name)
    await _assert_selected_tax_source_rows(
        database,
        stage=stage,
        destination=destination,
        source_snapshot_key=source_snapshot_key,
        unrelated_snapshot_key=unrelated_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )
    await _assert_destination_tax_source_resolution(
        database,
        destination=destination,
        destination_schema_name=destination_schema_name,
        destination_snapshot_key=destination_snapshot_key,
        publication=publication,
    )


async def _assert_selected_tax_source_rows(
    database: Database,
    *,
    stage: str,
    destination: str,
    source_snapshot_key: int,
    unrelated_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Prove selected tax receipt rows were copied without unrelated layout rows."""

    for table_name in _TAX_SOURCE_RECEIPT_TABLES:
        source_count = await database.scalar(
            f"SELECT COUNT(*) FROM {stage}.{table_name} WHERE snapshot_key = :snapshot_key",
            snapshot_key=source_snapshot_key,
        )
        destination_count = await database.scalar(
            f"SELECT COUNT(*) FROM {destination}.{table_name} WHERE snapshot_key = :snapshot_key",
            snapshot_key=destination_snapshot_key,
        )
        assert source_count > 0
        assert destination_count == source_count
        assert (
            await database.scalar(
                f"SELECT COUNT(*) FROM {stage}.{table_name} WHERE snapshot_key = :snapshot_key",
                snapshot_key=unrelated_snapshot_key,
            )
            > 0
        )
        assert (
            await database.scalar(
                f"SELECT COUNT(*) FROM {destination}.{table_name} WHERE snapshot_key = :snapshot_key",
                snapshot_key=unrelated_snapshot_key,
            )
            == 0
        )


async def _assert_destination_tax_source_resolution(
    database: Database,
    *,
    destination: str,
    destination_schema_name: str,
    destination_snapshot_key: int,
    publication,
) -> None:
    """Resolve a destination-local opaque tax reference through persisted records."""

    tax_identity = await database.first(
        f"SELECT tin_id_128, tin_hmac_sha256 FROM {destination}.ptg2_provider_tax_identity "
        "WHERE snapshot_key = :snapshot_key ORDER BY tin_key LIMIT 1",
        snapshot_key=destination_snapshot_key,
    )
    assert tax_identity is not None
    billing_entity_ref = billing_refs.encode_billing_entity_ref(
        snapshot_key=destination_snapshot_key,
        tin_id_128=bytes(tax_identity.tin_id_128),
        tin_hmac_sha256=bytes(tax_identity.tin_hmac_sha256),
    )
    async with database.transaction() as session:
        resolved = await source_resolution.resolve_billing_entity_ref_source_scope(
            session,
            schema_name=destination_schema_name,
            snapshot_key=destination_snapshot_key,
            billing_entity_ref=billing_entity_ref,
            source_publication=publication,
        )
    assert resolved is not None
    assert resolved.publication == publication
    assert resolved.witnesses


async def _prepare_layout(
    database: Database,
    *,
    destination_schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_id: str,
):
    """Run local layout preparation inside one caller-owned transaction."""

    async with database.transaction() as session:
        return await adoption.prepare_result_archive_layout(
            session,
            schema_name=destination_schema_name,
            staging_schema_name=staging_schema_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_id=destination_snapshot_id,
            build_token=f"receiver-{uuid.uuid4().hex}",
        )


async def _assert_reuse_rejects_support_mismatch(
    database: Database,
    *,
    stage: str,
    destination_schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
) -> None:
    """Require a reused reservation to match the staged support receipt."""

    source_support_digest = await database.scalar(
        f"SELECT support_digest FROM {stage}.ptg2_v3_snapshot_layout WHERE snapshot_key = :snapshot_key",
        snapshot_key=source_snapshot_key,
    )
    await database.status(
        f"UPDATE {stage}.ptg2_v3_snapshot_layout SET support_digest = :support_digest WHERE snapshot_key = :snapshot_key",
        support_digest=b"s" * 32,
        snapshot_key=source_snapshot_key,
    )
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="metadata differs"):
        await _prepare_layout(
            database,
            destination_schema_name=destination_schema_name,
            staging_schema_name=staging_schema_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_id="destination-three",
        )
    await database.status(
        f"UPDATE {stage}.ptg2_v3_snapshot_layout SET support_digest = :support_digest WHERE snapshot_key = :snapshot_key",
        support_digest=source_support_digest,
        snapshot_key=source_snapshot_key,
    )


async def _corrupt_staged_target_payload(database: Database, *, stage: str) -> None:
    """Change one same-length target payload while retaining its claimed hash."""

    await database.status(
        f"""
        UPDATE {stage}.ptg2_v3_block
           SET payload = set_byte(payload, 0, (get_byte(payload, 0) + 1) % 256)
         WHERE block_hash = (
             SELECT block_hash
               FROM {stage}.ptg2_v3_block
              WHERE object_kind <> 'snapshot_coordinate_map_v1'
              ORDER BY block_hash
              LIMIT 1
         )
        """
    )


async def _assert_reuse_rejects_payload_mismatch(
    database: Database,
    *,
    stage: str,
    destination: str,
    destination_schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
) -> None:
    """Reject same-length staged payload corruption before a third bind occurs."""

    await _corrupt_staged_target_payload(database, stage=stage)
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="CAS hash collides"):
        await _prepare_layout(
            database,
            destination_schema_name=destination_schema_name,
            staging_schema_name=staging_schema_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_id="destination-four",
        )
    for snapshot_id in ("destination-three", "destination-four"):
        assert (
            await database.scalar(
                f"SELECT COUNT(*) FROM {destination}.ptg2_v3_snapshot_binding WHERE snapshot_id = :snapshot_id",
                snapshot_id=snapshot_id,
            )
            == 0
        )


async def _prepare_reuse_layouts(
    database: Database,
    *,
    destination_schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
):
    """Prepare once and then bind a second local candidate to the reused key."""

    destination = _quoted(destination_schema_name)
    await database.status(
        f"""
        INSERT INTO {destination}.ptg2_snapshot (snapshot_id)
        VALUES ('destination-two'), ('destination-three'), ('destination-four')
        """
    )
    first = await _prepare_layout(
        database,
        destination_schema_name=destination_schema_name,
        staging_schema_name=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_id="destination-one",
    )
    reused = await _prepare_layout(
        database,
        destination_schema_name=destination_schema_name,
        staging_schema_name=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_id="destination-two",
    )
    return destination, first, reused
