# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic scoped clinical source clone and native dump/restore proof."""

import subprocess
from dataclasses import replace
from time import perf_counter
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import clinical_reference_result_archive as scoped
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.test_reference_family_archive_postgres import _database_url


async def _seed_clinical_source(session, live):
    spec = archive.reference_family_spec("clinical-reference")
    await archive._create_model_family(session, spec, live)
    await session.execute(
        text(
            f'CREATE TABLE "{live}".reference_family_result_generation ('
            "importer_id text PRIMARY KEY,local_lineage_id uuid NOT NULL,local_generation bigint NOT NULL,"
            "origin_lineage_id uuid,origin_generation bigint,published_at timestamptz,relation_oids bigint[])"
        )
    )
    await session.execute(
        text(
            f'INSERT INTO "{live}".reference_family_result_generation '
            "VALUES ('clinical-reference',:lineage,0,NULL,NULL,NULL,NULL)"
        ),
        {"lineage": uuid4()},
    )
    await session.execute(
        text(
            f'INSERT INTO "{live}".code_catalog (code_system,code,source) VALUES '
            "('TEST','owned','cdc_icd10cm'),('TEST','foreign','other_source')"
        )
    )
    await session.execute(
        text(
            f'INSERT INTO "{live}".code_catalog (code_system,code,source) '
            "SELECT 'TEST', 'synthetic-' || ordinal::text, 'cdc_icd10cm' "
            "FROM generate_series(1,10000) AS ordinal"
        )
    )
    initial = await generation.publish_local_reference_family_generation(
        session, importer_id="clinical-reference", schema_name=live
    )
    await generation.publish_local_reference_family_generation(
        session, importer_id="clinical-reference", schema_name=live
    )
    return initial


async def _roundtrip_clinical_stage(sessions, url, live, stage, dataset_id, tmp_path):
    async def persist(_session, _prepared):
        return None

    prepared = await archive.prepare_reference_family_archive_source(
        sessions,
        importer_id="clinical-reference",
        schema_name=live,
        source_metadata={"release": "synthetic"},
        dataset_id=dataset_id,
        on_prepared=persist,
    )
    assert next(table.row_count for table in prepared.manifest.tables if table.table_name == "code_catalog") == 10001
    async with sessions.begin() as session:
        assert await session.scalar(text(f'SELECT count(*) FROM "{stage}".code_catalog')) == 10001

    dump = tmp_path / "clinical.dump"
    dsn = url.replace("+asyncpg", "")
    dump_command_parts = ["pg_dump", "-Fc", "--data-only", "--file", str(dump)]
    for name in archive.reference_family_spec("clinical-reference").table_names:
        dump_command_parts.extend(["--table", f"{stage}.{name}"])
    subprocess.run([*dump_command_parts, dsn], check=True, capture_output=True)
    async with sessions.begin() as session:
        await archive.cleanup_reference_family_stage(session, prepared.ownership)
        await archive.precreate_reference_family_restore(
            session, importer_id="clinical-reference", dataset_id=dataset_id
        )
    subprocess.run(["pg_restore", "--data-only", "--dbname", dsn, str(dump)], check=True, capture_output=True)
    return prepared


async def _validate_restored_stage(session, live, stage, dataset_id, prepared):
    witness = await scoped.prepare_shared_before_image(session, destination=live, stage=stage)
    restored = replace(
        await archive.capture_reference_family_stage_ownership(
            session, importer_id="clinical-reference", dataset_id=dataset_id
        ),
        effect_witness=witness,
    )
    await archive.complete_reference_family_restore(session, restored)
    await session.execute(
        text(
            f'INSERT INTO "{stage}".code_catalog (code_system,code,source) '
            "VALUES ('TEST','contaminant','other_source')"
        )
    )
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="foreign sources"):
        await archive.validate_reference_family_stage(session, ownership=restored, manifest=prepared.manifest)
    await session.execute(text(f"DELETE FROM \"{stage}\".code_catalog WHERE code='contaminant'"))
    await archive.validate_reference_family_stage(session, ownership=restored, manifest=prepared.manifest)
    incumbent = await archive.capture_reference_family_incumbent(
        session, importer_id="clinical-reference", schema_name=live
    )
    await session.execute(text(f"UPDATE \"{live}\".code_catalog SET display_name='drift' WHERE code='owned'"))
    with pytest.raises(scoped.ClinicalReferenceScopeError, match="scoped rows changed"):
        await scoped.verify_shared_before_image_witness(session, destination=live, stage=stage, witness=witness)
    await session.execute(text(f"UPDATE \"{live}\".code_catalog SET display_name=NULL WHERE code='owned'"))
    return restored, incumbent


async def _activate_clinical_stage(session, live, dataset_id, restored, incumbent, prepared):
    spec = archive.reference_family_spec("clinical-reference")
    cutover_started = perf_counter()
    predecessor, live_pairs = await archive._complete_validated_stage_activation(
        session, spec, restored, incumbent, prepared.manifest
    )
    assert perf_counter() - cutover_started < 8
    assert predecessor == archive.reference_family_predecessor_schema(dataset_id)
    assert dict(live_pairs)["code_catalog"] == dict(incumbent.relation_oids)["code_catalog"]
    assert dict(live_pairs)["clinical_area"] == dict(restored.relation_oids)["clinical_area"]
    assert await session.scalar(text(f'SELECT count(*) FROM "{live}".code_catalog')) == 10002
    assert await session.scalar(text(f"SELECT count(*) FROM \"{live}\".code_catalog WHERE code='foreign'")) == 1
    await generation.publish_adopted_reference_family_generation(
        session,
        importer_id="clinical-reference",
        schema_name=live,
        source_generation=prepared.manifest.source_serving_generation,
    )
    activation = await archive._activation_receipt(
        session,
        spec,
        restored,
        incumbent,
        prepared.manifest,
        prepared.manifest.tables,
        predecessor,
    )
    assert dict(activation.relation_oids) == dict(live_pairs)
    return predecessor, live_pairs


async def _rollback_clinical_stage(session, live, rollback_id, predecessor, live_pairs, incumbent, prepared, initial):
    before_image = await scoped.shared_effect_receipt(session, schema=predecessor)
    outgoing = archive.reference_family_predecessor_schema(rollback_id)
    rollback_image = await scoped.rollback_clinical_result(
        session,
        destination=live,
        predecessor=predecessor,
        outgoing=outgoing,
        current_relation_oids=dict(live_pairs),
        target_relation_oids={
            name: oid
            for name, oid in incumbent.relation_oids
            if name in {"clinical_area", "clinical_area_condition", "clinical_area_treatment"}
        },
        generations=(prepared.manifest.source_serving_generation, initial.serving_generation),
        before_image=before_image,
    )
    assert rollback_image["schema_name"] == outgoing
    assert await session.scalar(text(f'SELECT count(*) FROM "{live}".code_catalog')) == 10002


@pytest.mark.asyncio
async def test_clinical_source_dump_restore_is_scoped(tmp_path):
    """Dump, restore, activate and roll back only the owned clinical source rows."""
    url = _database_url()
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    live = "clinical_source_" + uuid4().hex
    dataset_id = uuid4()
    rollback_id = uuid4()
    stage = archive.reference_family_stage_schema(dataset_id)
    try:
        async with sessions.begin() as session:
            initial = await _seed_clinical_source(session, live)
        prepared = await _roundtrip_clinical_stage(sessions, url, live, stage, dataset_id, tmp_path)
        async with sessions.begin() as session:
            restored, incumbent = await _validate_restored_stage(session, live, stage, dataset_id, prepared)
            predecessor, live_pairs = await _activate_clinical_stage(
                session, live, dataset_id, restored, incumbent, prepared
            )
            await _rollback_clinical_stage(
                session, live, rollback_id, predecessor, live_pairs, incumbent, prepared, initial
            )
    finally:
        async with engine.begin() as connection:
            for schema in (
                archive.reference_family_predecessor_schema(rollback_id),
                archive.reference_family_predecessor_schema(dataset_id),
                stage,
                live,
            ):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
