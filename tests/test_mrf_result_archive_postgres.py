# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import re
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import reference_family_archive as archive
from process import reference_family_result_generation as generation


_DSN_ENV = "HLTHPRT_MRF_RESULT_ARCHIVE_TEST_DSN"
_LOCAL_DATABASE = re.compile(r"hc_mrf_archive_[0-9a-f]{32}\Z")
_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260914130000_mrf_result_generation.py"
)


def _database_url() -> str:
    raw_dsn = os.getenv(_DSN_ENV, "")
    if not raw_dsn:
        pytest.skip(f"{_DSN_ENV} is not set")
    database_url = make_url(raw_dsn)
    if (
        not database_url.drivername.startswith("postgresql")
        or database_url.host not in {"127.0.0.1", "localhost", "postgres"}
        or _LOCAL_DATABASE.fullmatch(str(database_url.database or "")) is None
    ):
        pytest.fail(f"{_DSN_ENV} must identify a UUID-owned PostgreSQL test database")
    return database_url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _create_family(session, schema_name: str) -> None:
    await session.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gin"))
    await session.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    metadata = archive.MetaData(schema=schema_name)
    for model_type in archive.reference_family_spec("mrf").model_types:
        table = model_type.__table__.to_metadata(metadata, schema=schema_name)
        statement = str(archive.CreateTable(table).compile(dialect=archive.postgresql.dialect()))
        await session.execute(text(statement))
        indexes = tuple(getattr(model_type, "__my_initial_indexes__", ()) or ()) + tuple(
            getattr(model_type, "__my_additional_indexes__", ()) or ()
        )
        for index in indexes:
            await session.execute(text(archive._additional_index_sql(schema_name, model_type, index)))
    await session.execute(
        text(
            f'CREATE TABLE "{schema_name}".reference_family_result_generation ('
            "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, "
            "local_generation bigint NOT NULL, origin_lineage_id uuid, "
            "origin_generation bigint, published_at timestamptz, relation_oids bigint[])"
        )
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema_name}".reference_family_result_generation '
            "(importer_id, local_lineage_id, local_generation) VALUES ('mrf', :lineage_id, 0)"
        ),
        {"lineage_id": uuid4()},
    )


async def _insert_family_rows(session, schema_name: str, marker: str) -> None:
    plan_id = "00000000000001"
    statements = (
        ("issuer", "(issuer_id, issuer_name) VALUES (1, :marker)"),
        ("plan", "(plan_id, year, marketing_name) VALUES (:plan_id, 2026, :marker)"),
        (
            "plan_formulary",
            "(plan_id, year, drug_tier, pharmacy_type) VALUES (:plan_id, 2026, 'generic', 'retail')",
        ),
        (
            "plan_benefits_marketplace",
            "(plan_id, year, checksum, benefit_name) VALUES (:plan_id, 2026, 1, :marker)",
        ),
        ("plan_transparency", "(plan_id, year, issuer_name) VALUES (:plan_id, 2026, :marker)"),
        (
            "plan_drug_raw",
            "(plan_id, rxnorm_id, drug_name) VALUES (:plan_id, '1', :marker)",
        ),
        (
            "plan_drug_stats",
            "(plan_id, total_drugs, auth_required, auth_not_required, step_required, "
            "step_not_required, quantity_limit, quantity_no_limit) "
            "VALUES (:plan_id, 1, 0, 1, 0, 1, 0, 1)",
        ),
        (
            "plan_drug_tier_stats",
            "(plan_id, drug_tier, drug_count) VALUES (:plan_id, 'generic', 1)",
        ),
        ("log", "(issuer_id, checksum, text) VALUES (1, 1, :marker)"),
        (
            "plan_npi_raw",
            "(npi, checksum_network, name_or_facility_name) VALUES (1000000001, 1, :marker)",
        ),
        (
            "plan_networktier",
            "(plan_id, checksum_network, network_tier) VALUES (:plan_id, 1, :marker)",
        ),
        (
            "mrf_address",
            "(checksum, npi, type, first_line) VALUES (1, 1000000001, 'practice', :marker)",
        ),
        (
            "mrf_address_evidence",
            "(evidence_checksum, npi, type, checksum, import_id, source_record_id, first_line) "
            "VALUES (1, 1000000001, 'practice', 1, 'synthetic-import', 'record-1', :marker)",
        ),
    )
    assert tuple(table_name for table_name, _ in statements) == generation.RELATION_NAMES_BY_IMPORTER["mrf"]
    for table_name, values_sql in statements:
        await session.execute(
            text(f'INSERT INTO "{schema_name}"."{table_name}" {values_sql}'),
            {"marker": marker, "plan_id": plan_id},
        )


async def _copy_prepared_stage(session, prepared, restored) -> None:
    for table_name in generation.RELATION_NAMES_BY_IMPORTER["mrf"]:
        await session.execute(
            text(
                f'INSERT INTO "{restored.schema_name}"."{table_name}" '
                f'SELECT * FROM "{prepared.ownership.schema_name}"."{table_name}"'
            )
        )
    await archive._rebase_mrf_sequences(session, restored.schema_name)


async def _prepare_restored_candidate(sessions, source_schema: str, prepared_dataset_id, restored_dataset_id):
    prepared_records = []

    async def retain_prepared(_session, prepared) -> None:
        prepared_records.append(prepared)

    prepared = await archive.prepare_reference_family_archive_source(
        sessions,
        importer_id="mrf",
        schema_name=source_schema,
        source_metadata={"release": "synthetic-mrf-2"},
        dataset_id=prepared_dataset_id,
        on_prepared=retain_prepared,
    )
    assert prepared_records == [prepared]
    async with sessions() as session, session.begin():
        await session.execute(text(f"UPDATE \"{source_schema}\".issuer SET issuer_name = 'later'"))
        restored = await archive.precreate_reference_family_restore(
            session,
            importer_id="mrf",
            dataset_id=restored_dataset_id,
        )
        await _copy_prepared_stage(session, prepared, restored)
        await archive.validate_reference_family_stage(
            session,
            ownership=restored,
            manifest=prepared.manifest,
        )
        await archive.cleanup_reference_family_stage(session, prepared.ownership)
    return prepared.manifest, restored


async def _prepare_activation(sessions, destination_schema, manifest, ownership):
    async with sessions() as session, session.begin():
        incumbent = await archive.capture_reference_family_incumbent(
            session,
            importer_id="mrf",
            schema_name=destination_schema,
        )
        owner_oid = await session.scalar(text("SELECT current_user::regrole::oid"))
        validation = await archive.prepare_reference_family_activation(
            session,
            ownership=ownership,
            manifest=manifest,
            package_id="a" * 64,
            profile_contract=archive.CONTRACT,
            sealed_owner_oid=owner_oid,
        )
    return incumbent, validation, owner_oid


async def _activate(session, ownership, manifest, incumbent, validation, owner_oid, source_generation):
    return await archive.activate_validated_reference_family_stage(
        session,
        ownership=ownership,
        manifest=manifest,
        expected_incumbent=incumbent,
        validation_receipt=validation,
        cutover=archive.ReferenceFamilyCutoverAuthority(
            "a" * 64,
            archive.CONTRACT,
            owner_oid,
            owner_oid,
            "automatic",
            source_generation.as_dict(),
        ),
    )


async def _assert_replaced_sequence_rejected(sessions, ownership) -> None:
    async with sessions() as session:
        transaction = await session.begin()
        await session.execute(text(f'DROP SEQUENCE "{ownership.schema_name}".issuer_issuer_id_seq CASCADE'))
        await session.execute(text(f'CREATE SEQUENCE "{ownership.schema_name}".issuer_issuer_id_seq'))
        await session.execute(
            text(
                f'ALTER SEQUENCE "{ownership.schema_name}".issuer_issuer_id_seq '
                f'OWNED BY "{ownership.schema_name}".issuer.issuer_id'
            )
        )
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="ownership differs"):
            await archive.verify_reference_family_stage_ownership(session, ownership)
        await transaction.rollback()


async def _assert_wrong_sequence_column_rejected(sessions, ownership) -> None:
    async with sessions() as session:
        transaction = await session.begin()
        await session.execute(
            text(
                f'ALTER SEQUENCE "{ownership.schema_name}".issuer_issuer_id_seq '
                f'OWNED BY "{ownership.schema_name}".issuer.issuer_name'
            )
        )
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="sequence set is invalid"):
            await archive.verify_reference_family_stage_ownership(session, ownership)
        await transaction.rollback()


async def _assert_stale_incumbent_rejected(
    sessions,
    ownership,
    manifest,
    incumbent,
    validation,
    owner_oid,
    source_generation,
) -> None:
    async with sessions() as session:
        transaction = await session.begin()
        await session.execute(text(f'ALTER TABLE "{incumbent.schema_name}".issuer RENAME TO issuer_stale'))
        await session.execute(
            text(
                f'CREATE TABLE "{incumbent.schema_name}".issuer '
                f'(LIKE "{incumbent.schema_name}".issuer_stale INCLUDING ALL)'
            )
        )
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="incumbent changed"):
            await _activate(
                session,
                ownership,
                manifest,
                incumbent,
                validation,
                owner_oid,
                source_generation,
            )
        await transaction.rollback()


@pytest.mark.asyncio
async def test_mrf_model_family_roundtrip_retains_predecessor_and_rolls_back():
    """Transfer all 13 relations with frozen data, CAS, and atomic generation."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:10]
    source_schema = f"mrf_source_{token}"
    destination_schema = f"mrf_destination_{token}"
    unrelated_schema = f"mrf_unrelated_{token}"
    prepared_dataset_id, restored_dataset_id = uuid4(), uuid4()
    owned_schemas = {
        source_schema,
        destination_schema,
        unrelated_schema,
        archive.reference_family_stage_schema(prepared_dataset_id),
        archive.reference_family_stage_schema(restored_dataset_id),
        archive.reference_family_predecessor_schema(restored_dataset_id),
    }
    try:
        async with sessions() as session, session.begin():
            await _create_family(session, source_schema)
            await _create_family(session, destination_schema)
            await _insert_family_rows(session, source_schema, "source-v1")
            await _insert_family_rows(session, destination_schema, "destination-v1")
            await session.execute(text(f'CREATE TABLE "{destination_schema}".history (marker text PRIMARY KEY)'))
            await session.execute(text(f"INSERT INTO \"{destination_schema}\".history VALUES ('retained-history')"))
            await session.execute(text(f'CREATE TABLE "{destination_schema}".account_state (marker text PRIMARY KEY)'))
            await session.execute(
                text(f"INSERT INTO \"{destination_schema}\".account_state VALUES ('retained-account')")
            )
            await session.execute(text(f'CREATE SCHEMA "{unrelated_schema}"'))
            await session.execute(text(f'CREATE TABLE "{unrelated_schema}".keep_me (marker text PRIMARY KEY)'))
            await session.execute(text(f"INSERT INTO \"{unrelated_schema}\".keep_me VALUES ('keep')"))
            first_source = await generation.publish_local_reference_family_generation(
                session, importer_id="mrf", schema_name=source_schema
            )
            await generation.publish_adopted_reference_family_generation(
                session,
                importer_id="mrf",
                schema_name=destination_schema,
                source_generation=first_source.serving_generation,
            )
            await session.execute(text(f"UPDATE \"{source_schema}\".issuer SET issuer_name = 'source-v2'"))
            source_authority = await generation.publish_local_reference_family_generation(
                session, importer_id="mrf", schema_name=source_schema
            )
        manifest, ownership = await _prepare_restored_candidate(
            sessions,
            source_schema,
            prepared_dataset_id,
            restored_dataset_id,
        )
        incumbent, validation, owner_oid = await _prepare_activation(sessions, destination_schema, manifest, ownership)
        await _assert_replaced_sequence_rejected(sessions, ownership)
        await _assert_wrong_sequence_column_rejected(sessions, ownership)
        await _assert_stale_incumbent_rejected(
            sessions,
            ownership,
            manifest,
            incumbent,
            validation,
            owner_oid,
            source_authority.serving_generation,
        )
        async with sessions() as session:
            transaction = await session.begin()
            await _activate(
                session,
                ownership,
                manifest,
                incumbent,
                validation,
                owner_oid,
                source_authority.serving_generation,
            )
            assert await session.scalar(text(f'SELECT issuer_name FROM "{destination_schema}".issuer')) == "source-v2"
            await transaction.rollback()
        async with sessions() as session, session.begin():
            assert (
                await session.scalar(text(f'SELECT issuer_name FROM "{destination_schema}".issuer')) == "destination-v1"
            )
            receipt = await _activate(
                session,
                ownership,
                manifest,
                incumbent,
                validation,
                owner_oid,
                source_authority.serving_generation,
            )
            assert receipt.predecessor_schema_name is not None
        async with sessions() as session, session.begin():
            adopted = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf", schema_name=destination_schema
            )
            assert adopted.serving_generation == source_authority.serving_generation
            assert len(adopted.relation_oids) == 13
            assert await session.scalar(text(f'SELECT issuer_name FROM "{destination_schema}".issuer')) == "source-v2"
            assert (
                await session.scalar(text(f'SELECT issuer_name FROM "{receipt.predecessor_schema_name}".issuer'))
                == "destination-v1"
            )
            assert await session.scalar(text(f"SELECT nextval('\"{destination_schema}\".issuer_issuer_id_seq')")) == 2
            assert (
                await session.scalar(
                    text(f"SELECT nextval('\"{destination_schema}\".mrf_address_evidence_evidence_checksum_seq')")
                )
                == 2
            )
            assert await session.scalar(text(f'SELECT marker FROM "{unrelated_schema}".keep_me')) == "keep"
            assert (
                await session.scalar(text(f'SELECT marker FROM "{destination_schema}".history')) == "retained-history"
            )
            assert (
                await session.scalar(text(f'SELECT marker FROM "{destination_schema}".account_state'))
                == "retained-account"
            )
            assert (
                await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": ownership.schema_name}) is None
            )
    finally:
        async with engine.begin() as connection:
            for schema_name in owned_schemas:
                if schema_name:
                    await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


def _migration_module():
    module_spec = importlib.util.spec_from_file_location(
        "mrf_result_generation_postgres_proof",
        _MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _run_migration(connection, schema_name: str, operation: str) -> None:
    migration = _migration_module()

    def apply(sync_connection) -> None:
        migration.op = Operations(MigrationContext.configure(sync_connection))
        with pytest.MonkeyPatch.context() as monkeypatch:
            monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
            monkeypatch.delenv("DB_SCHEMA", raising=False)
            getattr(migration, operation)()

    await connection.run_sync(apply)


@pytest.mark.asyncio
async def test_mrf_generation_migration_adds_row_and_refuses_evidence_downgrade():
    """Extend the prior ledger without inventing or erasing MRF history."""

    engine = create_async_engine(_database_url())
    schema_name = f"mrf_migration_{uuid4().hex[:10]}"
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            await connection.execute(
                text(
                    f'CREATE TABLE "{schema_name}".reference_family_result_generation ('
                    "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, "
                    "local_generation bigint NOT NULL, origin_lineage_id uuid, "
                    "origin_generation bigint, published_at timestamptz, relation_oids bigint[], "
                    "CONSTRAINT reference_family_result_generation_shape_check CHECK ("
                    "importer_id IN ('plan-attributes', 'places-zcta', 'lodes', 'medicare-enrollment')))"
                )
            )
            await _run_migration(connection, schema_name, "upgrade")
            assert (
                await connection.scalar(
                    text(
                        f'SELECT local_generation FROM "{schema_name}".'
                        "reference_family_result_generation WHERE importer_id='mrf'"
                    )
                )
                == 0
            )
            await connection.execute(
                text(
                    f'UPDATE "{schema_name}".reference_family_result_generation '
                    "SET local_generation=1 WHERE importer_id='mrf'"
                )
            )
            with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
                await _run_migration(connection, schema_name, "downgrade")
            await connection.execute(
                text(
                    f'UPDATE "{schema_name}".reference_family_result_generation '
                    "SET local_generation=0 WHERE importer_id='mrf'"
                )
            )
            await _run_migration(connection, schema_name, "downgrade")
            assert (
                await connection.scalar(
                    text(
                        f'SELECT count(*) FROM "{schema_name}".'
                        "reference_family_result_generation WHERE importer_id='mrf'"
                    )
                )
                == 0
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()
