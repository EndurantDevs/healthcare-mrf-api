# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real model completion and closed ownership for newly acquired NPI output."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import npi_owned_publication as owned
from process import npi_result_archive as archive
from tests.npi_native_source_support import native_npi_runtime


@pytest.fixture
async def native_runtime():
    """Keep direct and registered collection independent of module import order."""
    from tests.test_npi_result_archive_postgres import _database_url

    async with native_npi_runtime(_database_url()) as runtime:
        yield runtime


async def _builder_output(runtime):
    """Bind new compiled heaps to an independent builder without namespace creation."""
    dataset_id = uuid4()
    runtime.schemas.add(archive.npi_stage_schema(dataset_id))
    async with runtime.administrator.begin() as connection:
        await connection.execute(text(f'GRANT "{runtime.builder}" TO "{runtime.publisher}"'))
        owner_oid = await connection.scalar(text("SELECT CAST(:role AS regrole)::oid"), {"role": runtime.owner})
        builder_oid = await connection.scalar(text("SELECT CAST(:role AS regrole)::oid"), {"role": runtime.builder})
    async with runtime.sessions() as session, session.begin():
        output = await owned.precreate_npi_output(session, dataset_id=dataset_id)
        assert len(output.relation_oids) == 7 and output.sequence_oids == ()
        schema = output.schema_name
        await session.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{runtime.owner}"'))
        await session.execute(text(f'GRANT USAGE,CREATE ON SCHEMA "{schema}" TO "{runtime.builder}"'))
        for name, _ in output.relation_oids:
            await session.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{runtime.builder}"'))
        await session.execute(text(f'REVOKE CREATE ON SCHEMA "{schema}" FROM "{runtime.builder}"'))
    return output, owner_oid, builder_oid


async def _fill_output(runtime, output):
    """Produce native rows and their genuine canonical contribution independently."""
    schema, address_key = output.schema_name, uuid4()
    async with runtime.builders() as session, session.begin():
        await session.execute(text(f'INSERT INTO "{schema}".npi(npi) VALUES (1000000001)'))
        await session.execute(
            text(
                f"INSERT INTO \"{schema}\".npi_address(npi,type,checksum,address_key) VALUES (1000000001,'primary',1,:key)"
            ),
            {"key": address_key},
        )
        await session.execute(
            text(
                f"INSERT INTO \"{schema}\".npi_canonical_address(address_key,identity_key,source_bits) VALUES (:key,'synthetic-output',1)"
            ),
            {"key": address_key},
        )


async def _output_manifest(session, output):
    """Observe real completed model schemas and counts, without serving-history claims."""
    for name, _ in output.relation_oids:
        await session.execute(text(f'ANALYZE "{output.schema_name}"."{name}"'))
    tables = await archive._manifest_tables(session, output.schema_name, canonical=True)
    metadata, metadata_sha = archive._source_metadata({"run_id": "synthetic-run", "source_evidence": "a" * 64})
    return archive.NpiResultManifest(
        tables,
        metadata,
        metadata_sha,
        archive._schema_digest(tables),
        "legacy-manual",
        None,
        None,
        archive.MODEL_CONTRACT,
    )


@pytest.mark.asyncio
async def test_native_output_custody(native_runtime):
    runtime = native_runtime
    output, owner_oid, builder_oid = await _builder_output(runtime)
    schema = output.schema_name
    await _fill_output(runtime, output)
    async with runtime.sessions() as session, session.begin():
        assert await owned.require_npi_output_heap_catalog(
            session,
            output,
            executable_roles=(runtime.builder, runtime.reader),
            protected_owner_oid=owner_oid,
            builder_oid=builder_oid,
        )
        await archive.complete_npi_restore(session, output)
        manifest = await _output_manifest(session, output)
        assert await owned.require_npi_output_catalog(
            session,
            output,
            executable_roles=(runtime.builder, runtime.reader),
            protected_owner_oid=owner_oid,
            expected_owner_oid=builder_oid,
        )
        for name, _ in output.relation_oids:
            await session.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{runtime.owner}"'))
        await session.execute(text(f'REVOKE ALL ON SCHEMA "{schema}" FROM "{runtime.builder}"'))
        owner_oid = await session.scalar(text("SELECT CAST(:role AS regrole)::oid"), {"role": runtime.owner})
        await owned._require_inert_owned_stage(session, output, owner_oid)
        validation = await archive.prepare_npi_activation(
            session,
            ownership=output,
            manifest=manifest,
            package_id=archive._manifest_digest(manifest),
            sealed_owner_oid=owner_oid,
        )
        assert await owned.require_npi_output_catalog(
            session,
            output,
            executable_roles=(runtime.builder, runtime.reader),
            protected_owner_oid=owner_oid,
            expected_owner_oid=owner_oid,
        )
        assert validation.tables == manifest.tables
        assert await session.scalar(
            text("SELECT NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=ANY(CAST(:oids AS oid[])))"),
            {"oids": [oid for _, oid in output.relation_oids]},
        )
        assert await session.scalar(
            text(
                "SELECT NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid=ANY(CAST(:oids AS oid[])) AND contype='f')"
            ),
            {"oids": [oid for _, oid in output.relation_oids]},
        )
    async with runtime.builders() as session, session.begin():
        with pytest.raises(DBAPIError):
            await session.execute(text(f"UPDATE \"{schema}\".npi SET provider_first_name='late'"))


@pytest.mark.asyncio
async def test_native_output_rejects_incomplete_canonical_closure(native_runtime):
    runtime, dataset_id = native_runtime, uuid4()
    runtime.schemas.add(archive.npi_stage_schema(dataset_id))
    async with runtime.sessions() as session, session.begin():
        output = await owned.precreate_npi_output(session, dataset_id=dataset_id)
        schema = output.schema_name
        await session.execute(text(f'INSERT INTO "{schema}".npi(npi) VALUES (1000000001)'))
        await session.execute(
            text(
                f"INSERT INTO \"{schema}\".npi_address(npi,type,checksum,address_key) VALUES (1000000001,'primary',1,:key)"
            ),
            {"key": uuid4()},
        )
        with pytest.raises(RuntimeError, match="closure is incomplete"):
            await archive.complete_npi_restore(session, output)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["owner", "enum", "index", "default", "trigger", "rule"])
async def test_native_catalog_proof_rejects_drift(native_runtime, drift):
    runtime, dataset_id = native_runtime, uuid4()
    runtime.schemas.add(archive.npi_stage_schema(dataset_id))
    async with runtime.sessions() as session, session.begin():
        output = await owned.precreate_npi_output(session, dataset_id=dataset_id)
        schema = output.schema_name
        await archive.complete_npi_restore(session, output)
        await session.execute(text(f'ALTER SCHEMA "{schema}" OWNER TO "{runtime.owner}"'))
        for name, _ in output.relation_oids:
            await session.execute(text(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{runtime.owner}"'))
        owner_oid = await session.scalar(text("SELECT CAST(:role AS regrole)::oid"), {"role": runtime.owner})
        changes_by_name = {
            "owner": f'ALTER TABLE "{schema}".npi OWNER TO "{runtime.publisher}"',
            "enum": f'ALTER TABLE "{schema}".npi_canonical_address ALTER COLUMN geo_source TYPE text USING geo_source::text',
            "index": f'CREATE INDEX unexpected_index ON "{schema}".npi(provider_first_name)',
            "default": f"ALTER TABLE \"{schema}\".npi ALTER COLUMN provider_first_name SET DEFAULT 'unexpected'",
            "trigger": f'CREATE TRIGGER unexpected_trigger BEFORE UPDATE ON "{schema}".npi FOR EACH ROW EXECUTE FUNCTION pg_catalog.suppress_redundant_updates_trigger()',
            "rule": f'CREATE RULE unexpected_rule AS ON DELETE TO "{schema}".npi DO INSTEAD NOTHING',
        }
        await session.execute(text(changes_by_name[drift]))
        with pytest.raises((RuntimeError, archive.NpiResultArchiveError)):
            await owned.require_npi_output_catalog(
                session,
                output,
                executable_roles=(runtime.builder, runtime.reader),
                protected_owner_oid=owner_oid,
                expected_owner_oid=owner_oid,
            )
