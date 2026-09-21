# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib.util
import os
import re
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import initial, plan_summary
from process import mrf_publication_receipt as publication_receipt
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.reference_family_generation_fixture import generation_shape_check

_DSN_ENV = "HLTHPRT_MRF_RESULT_ARCHIVE_TEST_DSN"
_LOCAL_DATABASE = re.compile(r"hc_mrf_archive_[0-9a-f]{32}\Z")
_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260914130000_mrf_result_generation.py"
)
_TIGER_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260920110000_tiger_result_generation.py"
)
_MRF_ADDRESS_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260920120000_mrf_address_result_generation.py"
)


async def _prepare_synthetic_publication_schema(connection, schema):
    """Add the receipt migration and canonical-address coverage for a fixture."""

    from tests.test_reference_family_result_generation_postgres import _run_migration

    migration = Path(__file__).resolve().parents[1] / "alembic/versions/20260920170000_mrf_publication_receipt.py"

    await _run_migration(connection, migration, "upgrade")
    await connection.execute(
        text(f'''CREATE TABLE "{schema}".address_archive_v2 (
        address_key uuid PRIMARY KEY, merged_into uuid, source_bits integer NOT NULL)''')
    )
    synthetic_key = uuid4()
    for name in ("mrf_address", "mrf_address_evidence"):
        await connection.execute(
            text(f'UPDATE "{schema}".{name} SET address_key=:key WHERE address_key IS NULL'),
            {"key": synthetic_key},
        )
    await connection.execute(
        text(f'''
        INSERT INTO "{schema}".address_archive_v2 (address_key, source_bits)
        SELECT address_key, 16 FROM "{schema}".mrf_address
        UNION SELECT address_key, 16 FROM "{schema}".mrf_address_evidence
    ''')
    )
    assert await connection.scalar(text(f'SELECT count(*) FROM "{schema}".address_archive_v2')) > 0


async def _configure_synthetic_plan_summary_tables(connection, schema, patch):
    """Bind plan-summary tables to the synthetic schema and create missing ones."""

    metadata = MetaData()
    for attribute in (
        "plan_table",
        "plan_attributes_table",
        "plan_benefits_table",
        "plan_prices_table",
        "summary_table",
    ):
        table = getattr(plan_summary, attribute).to_metadata(metadata, schema=schema)
        patch.setattr(plan_summary, attribute, table)
        if table.name not in {"plan", "plan_search_summary"}:
            await connection.run_sync(lambda sync, table=table: table.create(sync))


async def _complete_synthetic_publication(monkeypatch, engine, sessions, schema):
    """Build the real summary and receipt after the fixture's family rotation."""

    database = Database(engine=engine, session_factory=sessions)

    async def ready(_test_mode):
        return None

    with monkeypatch.context() as patch:
        patch.setenv("HLTHPRT_DB_SCHEMA", schema)
        patch.setenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_v2")
        patch.delenv("DB_SCHEMA", raising=False)
        patch.setattr(publication_receipt, "db", database)
        patch.setattr(plan_summary, "db", database)
        patch.setattr(plan_summary, "ensure_database", ready)
        async with engine.begin() as connection:
            await _prepare_synthetic_publication_schema(connection, schema)
            await _configure_synthetic_plan_summary_tables(connection, schema, patch)
        patch.setattr(
            plan_summary,
            "PRICE_RATE_COLUMNS",
            tuple(plan_summary.plan_prices_table.c[column.name] for column in plan_summary.PRICE_RATE_COLUMNS),
        )
        attempt = await publication_receipt.begin_publication(schema, "synthetic")
        async with sessions() as session, session.begin():
            authority = await generation.read_reference_family_result_generation_authority(
                session,
                importer_id="mrf",
                schema_name=schema,
            )
        await plan_summary.rebuild_plan_search_summary(publication=(attempt, authority, False))


class _PublisherDatabase:
    """Run the normal importer publication against one disposable session."""

    def __init__(self, session, schema_name):
        self.session = session
        self.schema_name = schema_name

    def transaction(self):
        return self.session.begin()

    def in_transaction(self):
        return self.session.in_transaction()

    async def execute(self, statement, params=None):
        return await self.session.execute(statement, params or {})

    async def status(self, statement):
        return await self.session.execute(text(statement))

    async def create_table(self, table, *, checkfirst=False):
        if table.schema != self.schema_name:
            table = table.to_metadata(MetaData(), schema=self.schema_name)
        connection = await self.session.connection()
        await connection.run_sync(lambda sync: table.create(sync, checkfirst=checkfirst))


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
    await archive._create_model_family(session, archive.reference_family_spec("mrf"), schema_name)
    await session.execute(
        text(
            f'CREATE TABLE "{schema_name}".reference_family_result_generation ('
            "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, "
            "local_generation bigint NOT NULL, origin_lineage_id uuid, "
            "origin_generation bigint, published_at timestamptz, relation_oids bigint[], "
            "CONSTRAINT reference_family_result_generation_shape_check CHECK ("
            f"{generation_shape_check()}))"
        )
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema_name}".reference_family_result_generation '
            "(importer_id, local_lineage_id, local_generation) VALUES "
            "('mrf', :mrf_lineage_id, 0), ('mrf-address', :address_lineage_id, 0)"
        ),
        {"mrf_lineage_id": uuid4(), "address_lineage_id": uuid4()},
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
    for table_name in archive.reference_family_spec("mrf").archive_names:
        await session.execute(
            text(
                f'INSERT INTO "{restored.schema_name}"."{table_name}" '
                f'SELECT * FROM "{prepared.ownership.schema_name}"."{table_name}"'
            )
        )
    await archive._rebase_owned_sequences(session, restored.schema_name, "mrf")


async def _prepare_restored_candidate(sessions, source_schema: str, prepared_dataset_id, restored_dataset_id):
    prepared_records = []

    async def retain_prepared(_session, prepared) -> None:
        prepared_records.append(prepared)

    async def dependencies(_session):
        return {"plan-attributes": "b" * 64}

    prepared = await archive.prepare_reference_family_archive_source(
        sessions,
        importer_id="mrf",
        schema_name=source_schema,
        source_metadata={"release": "synthetic-mrf-2"},
        dataset_id=prepared_dataset_id,
        on_prepared=retain_prepared,
        dependency_factory=dependencies,
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
        await archive.complete_reference_family_restore(session, restored)
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


async def _seed_mrf_roundtrip(session, source_schema, destination_schema, unrelated_schema):
    await _create_family(session, source_schema)
    await _create_family(session, destination_schema)
    await _insert_family_rows(session, source_schema, "source-v1")
    await _insert_family_rows(session, destination_schema, "destination-v1")
    await session.execute(
        text(
            f'INSERT INTO "{destination_schema}".plan_search_summary '
            "(plan_id, year, marketing_name) VALUES ('00000000000001', 2026, 'destination-v1')"
        )
    )
    await session.execute(
        text(f'''CREATE TABLE "{destination_schema}".address_archive_v2 (
        address_key uuid PRIMARY KEY, merged_into uuid, source_bits integer NOT NULL,
        destination_note text)''')
    )
    for table_name, marker in (("history", "retained-history"), ("account_state", "retained-account")):
        await session.execute(text(f'CREATE TABLE "{destination_schema}".{table_name} (marker text PRIMARY KEY)'))
        await session.execute(text(f"INSERT INTO \"{destination_schema}\".{table_name} VALUES ('{marker}')"))
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
    await session.execute(text(f"UPDATE \"{source_schema}\".plan SET marketing_name = 'source-v2'"))
    return await generation.publish_local_reference_family_generation(
        session, importer_id="mrf", schema_name=source_schema
    )


async def _assert_rolled_back_activation(
    sessions, destination_schema, ownership, manifest, incumbent, validation, owner_oid, source_generation
) -> None:
    async with sessions() as session:
        transaction = await session.begin()
        await _activate(session, ownership, manifest, incumbent, validation, owner_oid, source_generation)
        assert await session.scalar(text(f'SELECT issuer_name FROM "{destination_schema}".issuer')) == "source-v2"
        await transaction.rollback()
    async with sessions() as session, session.begin():
        assert await session.scalar(text(f'SELECT issuer_name FROM "{destination_schema}".issuer')) == "destination-v1"
        assert (
            await session.scalar(text(f'SELECT marketing_name FROM "{destination_schema}".plan_search_summary'))
            == "destination-v1"
        )
        assert (
            await session.scalar(
                text(f'SELECT count(*) FROM "{destination_schema}".address_archive_v2 WHERE source_bits=16')
            )
            == 0
        )


async def _assert_committed_activation(sessions, destination_schema, unrelated_schema, activation_by_field) -> None:
    async with sessions() as session, session.begin():
        receipt = await _activate(session, **activation_by_field)
        assert receipt.predecessor_schema_name is not None
    async with sessions() as session, session.begin():
        adopted = await generation.read_reference_family_result_generation_authority(
            session, importer_id="mrf", schema_name=destination_schema
        )
        assert adopted.serving_generation == activation_by_field["source_generation"]
        assert len(adopted.relation_oids) == 13
        assert await session.scalar(text(f'SELECT issuer_name FROM "{destination_schema}".issuer')) == "source-v2"
        assert (
            await session.scalar(text(f'SELECT marketing_name FROM "{destination_schema}".plan_search_summary'))
            == "source-v2"
        )
        assert (
            await session.scalar(text(f'SELECT issuer_name FROM "{receipt.predecessor_schema_name}".issuer'))
            == "destination-v1"
        )
        assert (
            await session.scalar(
                text(f'SELECT marketing_name FROM "{receipt.predecessor_schema_name}".plan_search_summary')
            )
            == "destination-v1"
        )
        summary_indexes = set(
            await session.scalars(
                text(
                    "SELECT indexname FROM pg_catalog.pg_indexes "
                    "WHERE schemaname=:schema AND tablename='plan_search_summary'"
                ),
                {"schema": destination_schema},
            )
        )
        assert {
            "plan_search_summary_pkey",
            "plan_search_summary_idx_plan_search_summary_state_year_idx",
            "plan_search_summary_idx_plan_search_summary_issuer_year_idx",
        } <= summary_indexes
        assert await session.scalar(text(f"SELECT nextval('\"{destination_schema}\".issuer_issuer_id_seq')")) == 2
        assert (
            await session.scalar(
                text(f"SELECT nextval('\"{destination_schema}\".mrf_address_evidence_evidence_checksum_seq')")
            )
            == 2
        )
        assert await session.scalar(text(f'SELECT marker FROM "{unrelated_schema}".keep_me')) == "keep"
        for table_name, marker in (("history", "retained-history"), ("account_state", "retained-account")):
            assert await session.scalar(text(f'SELECT marker FROM "{destination_schema}".{table_name}')) == marker
        assert (
            await session.scalar(
                text("SELECT to_regnamespace(:schema)"), {"schema": activation_by_field["ownership"].schema_name}
            )
            is None
        )


async def _prepare_destination_address_merge(sessions, destination_schema, ownership):
    """Insert one local address contribution and return its canonical key."""

    async with sessions() as session, session.begin():
        key = await session.scalar(
            text(
                f'SELECT address_key FROM "{ownership.schema_name}".mrf_canonical_address ORDER BY address_key LIMIT 1'
            )
        )
        await session.execute(
            text(
                f'INSERT INTO "{destination_schema}".address_archive_v2 '
                "(address_key, source_bits, destination_note) VALUES (:key, 1, :note)"
            ),
            {"key": key, "note": "keep-local"},
        )
    return key


async def _assert_destination_address_conflict_rejected(sessions, destination_schema, key, activation_by_field):
    """Reject activation when an incumbent local address conflicts with the stage."""

    async with sessions() as session:
        transaction = await session.begin()
        await session.execute(
            text(f'UPDATE "{destination_schema}".address_archive_v2 SET merged_into=:other WHERE address_key=:key'),
            {"key": key, "other": uuid4()},
        )
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="key conflicts"):
            await _activate(session, **activation_by_field)
        await transaction.rollback()


async def _assert_destination_address_merge_retained(sessions, destination_schema, key):
    """Confirm activation preserves and combines the local address contribution."""

    async with sessions() as session, session.begin():
        assert (
            await session.scalar(
                text(f'SELECT destination_note FROM "{destination_schema}".address_archive_v2 WHERE address_key=:key'),
                {"key": key},
            )
            == "keep-local"
        )
        assert (
            await session.scalar(
                text(f'SELECT source_bits FROM "{destination_schema}".address_archive_v2 WHERE address_key=:key'),
                {"key": key},
            )
            == 17
        )


async def _exercise_mrf_roundtrip(
    sessions,
    source_schema,
    destination_schema,
    unrelated_schema,
    prepared_dataset_id,
    restored_dataset_id,
    source_generation,
) -> None:
    """Exercise address conflicts, rollbacks, and successful MRF archive activation."""

    manifest, ownership = await _prepare_restored_candidate(
        sessions, source_schema, prepared_dataset_id, restored_dataset_id
    )
    key = await _prepare_destination_address_merge(sessions, destination_schema, ownership)
    incumbent, validation, owner_oid = await _prepare_activation(sessions, destination_schema, manifest, ownership)
    activation_by_field = {
        "ownership": ownership,
        "manifest": manifest,
        "incumbent": incumbent,
        "validation": validation,
        "owner_oid": owner_oid,
        "source_generation": source_generation,
    }
    await _assert_destination_address_conflict_rejected(sessions, destination_schema, key, activation_by_field)
    await _assert_replaced_sequence_rejected(sessions, ownership)
    await _assert_wrong_sequence_column_rejected(sessions, ownership)
    await _assert_stale_incumbent_rejected(sessions, **activation_by_field)
    await _assert_rolled_back_activation(sessions, destination_schema, **activation_by_field)
    await _assert_committed_activation(sessions, destination_schema, unrelated_schema, activation_by_field)
    await _assert_destination_address_merge_retained(sessions, destination_schema, key)


@pytest.mark.asyncio
async def test_mrf_model_family_roundtrip_retains_predecessor_and_rolls_back(monkeypatch):
    """Transfer all 14 serving relations with frozen data, CAS, and atomic generation."""

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
            source_authority = await _seed_mrf_roundtrip(session, source_schema, destination_schema, unrelated_schema)
        await _complete_synthetic_publication(monkeypatch, engine, sessions, source_schema)
        await _exercise_mrf_roundtrip(
            sessions,
            source_schema,
            destination_schema,
            unrelated_schema,
            prepared_dataset_id,
            restored_dataset_id,
            source_authority.serving_generation,
        )
    finally:
        async with engine.begin() as connection:
            for schema_name in owned_schemas:
                if schema_name:
                    await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


async def _initialize_published_mrf_schema(session, schema_name):
    """Create only the ordinary importer's generation authority prerequisites."""

    await session.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gin"))
    await session.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    await session.execute(
        text(
            f'CREATE TABLE "{schema_name}".reference_family_result_generation ('
            "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, "
            "local_generation bigint NOT NULL, origin_lineage_id uuid, "
            "origin_generation bigint, published_at timestamptz, relation_oids bigint[], "
            "CONSTRAINT reference_family_result_generation_shape_check CHECK ("
            f"{generation_shape_check()}))"
        )
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema_name}".reference_family_result_generation '
            "(importer_id, local_lineage_id, local_generation) VALUES "
            "('mrf', :mrf_lineage_id, 0), ('mrf-address', :address_lineage_id, 0)"
        ),
        {"mrf_lineage_id": uuid4(), "address_lineage_id": uuid4()},
    )
    await session.commit()


async def _stage_normal_mrf_family(session, schema_name, import_date, address_key):
    """Build the ordinary importer's indexed synthetic family without publishing it."""

    await initial._prepare_import_tables(import_date, True)
    address_stage = initial.make_class(initial.MRFAddress, import_date, schema_override=schema_name)
    evidence_stage = initial.make_class(initial.MRFAddressEvidence, import_date, schema_override=schema_name)
    await session.execute(
        text(
            f'INSERT INTO "{schema_name}"."{address_stage.__tablename__}" '
            "(checksum, npi, type, first_line, phone_number, address_key) "
            "VALUES (1, 1000000001, 'practice', 'Synthetic', '5550100', :address_key)"
        ),
        {"address_key": address_key},
    )
    await session.execute(
        text(
            f'INSERT INTO "{schema_name}"."{evidence_stage.__tablename__}" '
            "(npi, type, checksum, import_id, source_record_id, first_line) "
            "VALUES (1000000001, 'practice', 1, 'synthetic-import', 'record-1', 'Synthetic')"
        )
    )
    await initial._create_named_indexes(address_stage, schema_name)
    await initial._create_named_indexes(evidence_stage, schema_name)
    await session.commit()


async def _publish_normal_mrf_stage(session, schema_name, import_date, address_key):
    """Use the production staging and table-swap functions with synthetic rows."""

    await _stage_normal_mrf_family(session, schema_name, import_date, address_key)
    await initial._publish_mrf_table_generation(import_date, schema_name)


async def _activate_manual_archive(sessions, destination_schema, manifest, ownership):
    async with sessions() as session, session.begin():
        incumbent = await archive.capture_reference_family_incumbent(
            session,
            importer_id="mrf",
            schema_name=destination_schema,
        )
        return await archive.activate_reference_family_stage(
            session,
            ownership=ownership,
            manifest=manifest,
            expected_incumbent=incumbent,
            authority="manual",
        )


async def _run_interleaved_archive_cycle(
    sessions,
    monkeypatch,
    source_schema,
    destination_schema,
    dataset_ids,
):
    first_prepared, first_restored, second_prepared, second_restored = dataset_ids
    manifest, ownership = await _prepare_restored_candidate(
        sessions,
        source_schema,
        first_prepared,
        first_restored,
    )
    await _activate_manual_archive(sessions, destination_schema, manifest, ownership)

    async with sessions() as session:
        monkeypatch.setattr(initial, "db", _PublisherDatabase(session, destination_schema))
        monkeypatch.setattr(initial, "get_import_schema", lambda *_args: destination_schema)
        await _publish_normal_mrf_stage(session, destination_schema, "20260921", uuid4())
        await session.execute(text(f"UPDATE \"{source_schema}\".issuer SET issuer_name = 'source-v3'"))
        await session.commit()

    manifest, ownership = await _prepare_restored_candidate(
        sessions,
        source_schema,
        second_prepared,
        second_restored,
    )
    await _activate_manual_archive(sessions, destination_schema, manifest, ownership)


async def _command(*args):
    process = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    assert process.returncode == 0, stderr.decode()
    return stdout.decode()


@pytest.mark.asyncio
async def test_second_address_generation_failure_rolls_back_normal_mrf_rotation(monkeypatch):
    """The address generation is part of the same real table-swap transaction."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema_name = f"mrf_atomic_address_{uuid4().hex[:10]}"
    real_generation_writer = initial.publish_local_reference_family_generation
    try:
        async with sessions() as session:
            monkeypatch.setattr(initial, "db", _PublisherDatabase(session, schema_name))
            monkeypatch.setattr(initial, "get_import_schema", lambda *_args: schema_name)
            await _initialize_published_mrf_schema(session, schema_name)
            await _publish_normal_mrf_stage(session, schema_name, "20260920", uuid4())
            before_mrf = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf", schema_name=schema_name
            )
            before_address = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf-address", schema_name=schema_name
            )
            await session.commit()
            await _stage_normal_mrf_family(session, schema_name, "20260921", uuid4())

            async def fail_after_address_write(database, *, importer_id, schema_name):
                result = await real_generation_writer(
                    database,
                    importer_id=importer_id,
                    schema_name=schema_name,
                )
                if importer_id == "mrf-address":
                    raise RuntimeError("synthetic address generation failure")
                return result

            monkeypatch.setattr(initial, "publish_local_reference_family_generation", fail_after_address_write)
            with pytest.raises(RuntimeError, match="address generation failure"):
                await initial._publish_mrf_table_generation("20260921", schema_name)

            after_mrf = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf", schema_name=schema_name
            )
            after_address = await generation.read_reference_family_result_generation_authority(
                session, importer_id="mrf-address", schema_name=schema_name
            )
            assert after_mrf == before_mrf
            assert after_address == before_address
            assert (
                await generation.current_reference_family_relation_oids(
                    session, importer_id="mrf", schema_name=schema_name
                )
                == before_mrf.relation_oids
            )
            assert await session.scalar(text(f"SELECT to_regclass('{schema_name}.mrf_address_20260921')"))
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


async def _retain_prepared(_session, _prepared):
    return None


async def _synthetic_dependencies(_session):
    return {"plan-attributes": "b" * 64}


async def _assert_full_mrf_stage(sessions, schema_name, dataset_id, address_key):
    prepared = await archive.prepare_reference_family_archive_source(
        sessions,
        importer_id="mrf",
        schema_name=schema_name,
        source_metadata={"release": "synthetic-published-mrf"},
        dataset_id=dataset_id,
        on_prepared=_retain_prepared,
        dependency_factory=_synthetic_dependencies,
    )
    async with sessions() as session, session.begin():
        await archive.validate_reference_family_stage(
            session,
            ownership=prepared.ownership,
            manifest=prepared.manifest,
        )
        assert (
            await session.scalar(text(f'SELECT address_key FROM "{prepared.ownership.schema_name}".mrf_address'))
            == address_key
        )
        assert (
            await session.scalar(text(f'SELECT phone_number FROM "{prepared.ownership.schema_name}".mrf_address'))
            == "5550100"
        )
        assert prepared.manifest.auxiliary["table_name"] == "mrf_canonical_address"
        assert prepared.manifest.auxiliary["row_count"] >= 1
        assert prepared.ownership.auxiliary_oid is not None
        assert (
            await session.scalar(
                text(
                    f'SELECT count(*) FROM "{prepared.ownership.schema_name}".mrf_canonical_address '
                    "WHERE address_key=:key AND payload->>'address_key'=:key_text"
                ),
                {"key": address_key, "key_text": str(address_key)},
            )
            == 1
        )
        await session.execute(
            text(
                f'UPDATE "{prepared.ownership.schema_name}".mrf_canonical_address '
                "SET payload=jsonb_set(payload, '{source_bits}', '1'::jsonb)"
            )
        )
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="restored stage differs"):
            await archive.validate_reference_family_stage(
                session,
                ownership=prepared.ownership,
                manifest=prepared.manifest,
            )
    async with sessions() as session, session.begin():
        await archive.cleanup_reference_family_stage(session, prepared.ownership)


async def _dump_prepared_archive(sessions, prepared, path):
    database_url = make_url(_database_url()).set(drivername="postgresql")

    async def dump(capture):
        await _command(
            "pg_dump",
            "--dbname",
            database_url.render_as_string(hide_password=False),
            "--format=custom",
            "--no-owner",
            "--no-acl",
            "--schema",
            capture.ownership.schema_name,
            "--snapshot",
            capture.postgres_snapshot,
            "--file",
            str(path),
        )

    await archive.export_prepared_reference_family_archive(sessions, prepared=prepared, archive_copy=dump)
    return await _command("pg_restore", "--list", str(path))


async def _restore_prepared_address_archive(sessions, prepared, dataset_id, path):
    async with sessions() as session, session.begin():
        await archive.cleanup_reference_family_stage(session, prepared.ownership)
        restored = await archive.precreate_reference_family_restore(
            session,
            importer_id="mrf-address",
            dataset_id=dataset_id,
        )
    database_url = make_url(_database_url()).set(drivername="postgresql")
    await _command(
        "pg_restore",
        "--dbname",
        database_url.render_as_string(hide_password=False),
        "--data-only",
        "--no-owner",
        "--no-acl",
        "--exit-on-error",
        "--single-transaction",
        str(path),
    )
    return restored


async def _assert_restored_address_archive(sessions, prepared, restored, address_key):
    async with sessions() as session, session.begin():
        await archive.complete_reference_family_restore(session, restored)
        await archive.validate_reference_family_stage(session, ownership=restored, manifest=prepared.manifest)
        assert tuple(table.table_name for table in prepared.manifest.tables) == (
            "mrf_address",
            "mrf_address_evidence",
        )
        assert restored.sequence_oids[0][0] == "mrf_address_evidence_evidence_checksum_seq"
        assert (
            await session.scalar(text(f'SELECT address_key FROM "{restored.schema_name}".mrf_address')) == address_key
        )
        assert (
            await session.scalar(text(f'SELECT first_line FROM "{restored.schema_name}".mrf_address_evidence'))
            == "Synthetic"
        )
        assert (
            await session.scalar(
                text(f"SELECT nextval('\"{restored.schema_name}\".mrf_address_evidence_evidence_checksum_seq')")
            )
            == 2
        )
        await archive.cleanup_reference_family_stage(session, restored)


async def _publish_synthetic_mrf_family(monkeypatch, sessions, schema_name, address_key):
    async with sessions() as session:
        monkeypatch.setattr(initial, "db", _PublisherDatabase(session, schema_name))
        monkeypatch.setattr(initial, "get_import_schema", lambda *_args: schema_name)
        await _initialize_published_mrf_schema(session, schema_name)
        await _publish_normal_mrf_stage(session, schema_name, "20260920", address_key)
        mrf = await generation.read_reference_family_result_generation_authority(
            session,
            importer_id="mrf",
            schema_name=schema_name,
        )
        address = await generation.read_reference_family_result_generation_authority(
            session,
            importer_id="mrf-address",
            schema_name=schema_name,
        )
        assert mrf.local_generation == address.local_generation == 1
        assert address.relation_oids == mrf.relation_oids[-2:]


async def _assert_unfinished_mrf_source_rejected(sessions, schema_name, dataset_id):
    with pytest.raises(RuntimeError, match="completion is unavailable"):
        async with sessions() as session, session.begin():
            await archive.capture_reference_family_source(
                session,
                importer_id="mrf",
                schema_name=schema_name,
                source_metadata={"release": "synthetic-rotation-only"},
                dependencies={"plan-attributes": "b" * 64},
            )
    with pytest.raises(RuntimeError, match="completion is unavailable"):
        await archive.prepare_reference_family_archive_source(
            sessions,
            importer_id="mrf",
            schema_name=schema_name,
            source_metadata={"release": "synthetic-rotation-only"},
            dataset_id=dataset_id,
            on_prepared=_retain_prepared,
            dependency_factory=_synthetic_dependencies,
        )


async def _assert_address_archive_round_trip(sessions, schema_name, dataset_id, address_key, archive_path):
    prepared = await archive.prepare_reference_family_archive_source(
        sessions,
        importer_id="mrf-address",
        schema_name=schema_name,
        source_metadata={"release": "synthetic-published-mrf-address"},
        dataset_id=dataset_id,
        on_prepared=_retain_prepared,
    )
    listing = await _dump_prepared_archive(sessions, prepared, archive_path)
    assert "mrf_address" in listing and "mrf_address_evidence" in listing
    assert f" {prepared.ownership.schema_name} issuer " not in listing
    assert f" {prepared.ownership.schema_name} plan_npi_raw " not in listing
    restored = await _restore_prepared_address_archive(sessions, prepared, dataset_id, archive_path)
    await _assert_restored_address_archive(sessions, prepared, restored, address_key)


@pytest.mark.asyncio
async def test_mrf_archive_accepts_normal_published_staging_tables(monkeypatch, tmp_path):
    """The archive must accept the importer's real table shape, not a model-only fixture."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    schema_name = f"mrf_published_{uuid4().hex[:10]}"
    dataset_id, address_dataset_id, address_key = uuid4(), uuid4(), uuid4()
    owned_schemas = (
        archive.reference_family_stage_schema(dataset_id),
        archive.reference_family_stage_schema(address_dataset_id),
        schema_name,
    )
    try:
        await _publish_synthetic_mrf_family(monkeypatch, sessions, schema_name, address_key)
        await _assert_unfinished_mrf_source_rejected(sessions, schema_name, dataset_id)
        await _complete_synthetic_publication(monkeypatch, engine, sessions, schema_name)
        await _assert_full_mrf_stage(sessions, schema_name, dataset_id, address_key)
        await _assert_address_archive_round_trip(
            sessions,
            schema_name,
            address_dataset_id,
            address_key,
            tmp_path / "mrf-address.dump",
        )
    finally:
        async with engine.begin() as connection:
            for owned_schema in owned_schemas:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{owned_schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
async def test_mrf_archive_rotation_survives_an_interleaved_ordinary_import(monkeypatch):
    """An ordinary table swap must not leave names that block the next archive."""

    engine = create_async_engine(_database_url())
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    token = uuid4().hex[:10]
    source_schema = f"mrf_interleave_source_{token}"
    destination_schema = f"mrf_interleave_destination_{token}"
    first_prepared, first_restored = uuid4(), uuid4()
    second_prepared, second_restored = uuid4(), uuid4()
    dataset_ids = first_prepared, first_restored, second_prepared, second_restored
    owned_schemas = {
        source_schema,
        destination_schema,
        archive.reference_family_stage_schema(first_prepared),
        archive.reference_family_stage_schema(first_restored),
        archive.reference_family_predecessor_schema(first_restored),
        archive.reference_family_stage_schema(second_prepared),
        archive.reference_family_stage_schema(second_restored),
        archive.reference_family_predecessor_schema(second_restored),
    }
    try:
        async with sessions() as session, session.begin():
            await _create_family(session, source_schema)
            await _create_family(session, destination_schema)
            await _insert_family_rows(session, source_schema, "source-v1")
            await _insert_family_rows(session, destination_schema, "destination-v1")

        await _run_interleaved_archive_cycle(
            sessions,
            monkeypatch,
            source_schema,
            destination_schema,
            dataset_ids,
        )

        async with sessions() as session, session.begin():
            assert await session.scalar(text(f'SELECT issuer_name FROM "{destination_schema}".issuer')) == "source-v3"
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_catalog.pg_class AS relation "
                        "JOIN pg_catalog.pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
                        "WHERE namespace.nspname=:schema_name AND relation.relname LIKE '%\\_old' ESCAPE '\\'"
                    ),
                    {"schema_name": destination_schema},
                )
                == 0
            )
    finally:
        async with engine.begin() as connection:
            for schema_name in owned_schemas:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()


def _migration_module(path=_MIGRATION_PATH):
    module_spec = importlib.util.spec_from_file_location(
        f"{path.stem}_postgres_proof",
        path,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _run_migration(connection, schema_name: str, operation: str, path=_MIGRATION_PATH) -> None:
    migration = _migration_module(path)

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


async def _install_prior_generation_ledger(connection, schema_name):
    tiger_migration = _migration_module(_TIGER_MIGRATION_PATH)
    prior_shape = tiger_migration._shape_check(
        {"tiger": tiger_migration._TIGER_CARDINALITY, **tiger_migration._REFERENCE_CARDINALITY}
    )
    prior_importers = tuple({"tiger": tiger_migration._TIGER_CARDINALITY, **tiger_migration._REFERENCE_CARDINALITY})
    await connection.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    await connection.execute(
        text(
            f'CREATE TABLE "{schema_name}".reference_family_result_generation ('
            "importer_id text PRIMARY KEY, local_lineage_id uuid NOT NULL, "
            "local_generation bigint NOT NULL, origin_lineage_id uuid, "
            "origin_generation bigint, published_at timestamptz, relation_oids bigint[], "
            "CONSTRAINT reference_family_result_generation_shape_check CHECK ("
            f"{prior_shape}))"
        )
    )
    for importer_id in prior_importers:
        await connection.execute(
            text(
                f'INSERT INTO "{schema_name}".reference_family_result_generation '
                "(importer_id, local_lineage_id, local_generation) VALUES (:importer_id, :lineage_id, 0)"
            ),
            {"importer_id": importer_id, "lineage_id": uuid4()},
        )
    return (
        await connection.execute(
            text(f'SELECT * FROM "{schema_name}".reference_family_result_generation ORDER BY importer_id')
        )
    ).all()


async def _assert_address_generation_upgrade(connection, schema_name, before):
    await _run_migration(connection, schema_name, "upgrade", _MRF_ADDRESS_MIGRATION_PATH)
    after = (
        await connection.execute(
            text(
                f'SELECT * FROM "{schema_name}".reference_family_result_generation '
                "WHERE importer_id <> 'mrf-address' ORDER BY importer_id"
            )
        )
    ).all()
    assert after == before
    authority = await generation.read_reference_family_result_generation_authority(
        connection,
        importer_id="mrf-address",
        schema_name=schema_name,
    )
    assert authority.local_generation == 0 and authority.serving_generation is None
    with pytest.raises(RuntimeError, match="unavailable"):
        await generation.capture_reference_family_serving_generation(
            connection,
            importer_id="mrf-address",
            schema_name=schema_name,
        )


async def _assert_address_adoption_blocks_downgrade(connection, schema_name):
    await connection.execute(
        text(
            f'UPDATE "{schema_name}".reference_family_result_generation '
            "SET origin_lineage_id=:lineage_id, origin_generation=1, published_at=now(), "
            "relation_oids=ARRAY[1,2]::bigint[] WHERE importer_id='mrf-address'"
        ),
        {"lineage_id": uuid4()},
    )
    with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
        await _run_migration(connection, schema_name, "downgrade", _MRF_ADDRESS_MIGRATION_PATH)
    await connection.execute(
        text(
            f'UPDATE "{schema_name}".reference_family_result_generation '
            "SET origin_lineage_id=NULL, origin_generation=NULL, published_at=NULL, relation_oids=NULL "
            "WHERE importer_id='mrf-address'"
        )
    )


@pytest.mark.asyncio
async def test_mrf_address_generation_migration_requires_new_publication_before_export():
    """Seed generation zero and preserve every prior family while refusing evidence loss."""

    engine = create_async_engine(_database_url())
    schema_name = f"mrf_address_migration_{uuid4().hex[:10]}"
    try:
        async with engine.begin() as connection:
            before = await _install_prior_generation_ledger(connection, schema_name)
            await _assert_address_generation_upgrade(connection, schema_name, before)
            await _assert_address_adoption_blocks_downgrade(connection, schema_name)
            await _run_migration(connection, schema_name, "downgrade", _MRF_ADDRESS_MIGRATION_PATH)
            assert (
                await connection.scalar(
                    text(
                        f'SELECT count(*) FROM "{schema_name}".'
                        "reference_family_result_generation WHERE importer_id='mrf-address'"
                    )
                )
                == 0
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await engine.dispose()
