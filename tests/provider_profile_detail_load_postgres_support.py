# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Full-shape, bounded provider detail data for native publication load checks."""

import importlib
import json
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import UUID

from sqlalchemy import MetaData, text

from api import provider_profile_snapshot as snapshot
from db import models
from process import entity_address_result_generation as address_generation
from process import provider_directory_profile as profile
from tests import test_provider_directory_cms_serving_receipt_postgres as receipt_fixture
from tests.cms_doctors_preparation_postgres_support import doctors_database, native, preparation, stage_family
from tests.public_evidence_storage_postgres_support import connect
from tests.test_npi_canonical_publication_rotation_postgres import (
    ATTEMPT_ID,
    ATTEMPT_STARTED_AT,
    RUN_ID,
    _admitted_chain,
    _finalize_publication,
    _lock_attempt,
)

npi_api = importlib.import_module("api.endpoint.npi")
NPI = 1000000004
STAMP = datetime(2026, 1, 1)
RELATIONS = tuple(
    dict.fromkeys(
        (
            *snapshot._DOCTORS_TABLES,
            *snapshot._PROFILE_TABLES,
            "provider_directory_address_overlay",
            *address_generation.RELATION_NAMES,
            *snapshot._DETAIL_TABLES,
            snapshot._PROFILE_GENERATION_TABLE,
            snapshot.reference_generation.TABLE_NAME,
            address_generation.TABLE_NAME,
            snapshot._CMS_RECEIPT_TABLE,
        )
    )
)


async def _install_detail_tables(connection, schema, monkeypatch, create_scalars):
    """Replace scalar stand-ins before the Doctors fixture installs its real guards."""
    await create_scalars(connection, schema)
    existing_names = {
        *receipt_fixture.receipts._NATIVE_RELATIONS,
        "provider_directory_source",
        "provider_directory_endpoint_dataset",
    }
    later_names = {"entity_address_result_generation", "provider_directory_profile_serving_generation"}
    wanted = (set(RELATIONS) | {"provider_directory_api_endpoint", "import_run"}) - later_names
    tables_by_name = {table.name: table for table in models.db.metadata.tables.values() if table.name in wanted}
    metadata = MetaData(schema=schema)
    for table in tables_by_name.values():
        table.to_metadata(metadata, schema=schema)
    for name in existing_names:
        await connection.execute(text(f'DROP TABLE "{schema}"."{name}"'))
    await connection.run_sync(metadata.create_all)
    for table in tables_by_name.values():
        monkeypatch.setattr(table, "schema", schema)
    await connection.execute(text(profile.profile_table_sql(schema, logged=True)))
    await connection.execute(text(profile.profile_evidence_table_sql(schema, logged=True)))
    for prefix in (
        "20260701090000",
        "20260701110000",
        "20260808090000",
        "20260808100000",
        "20260808170000",
        "20260808220000",
        "20260809020000",
        "20260808230000",
        "20260914120000",
    ):
        await connection.run_sync(lambda sync, prefix=prefix: receipt_fixture._apply(sync, prefix))


async def _insert(session, model, **values):
    await session.execute(model.__table__.insert().values(**values))


async def _seed_identity_and_locations(session):
    """Populate actual NPI identity and related address models."""
    await _insert(
        session,
        models.NPIData,
        npi=NPI,
        entity_type_code=1,
        provider_first_name="Synthetic",
        provider_last_name="Clinician",
        provider_credential_text="MD",
        search_taxonomy_codes=["207Q00000X"],
    )
    await _insert(
        session,
        models.NPIDataTaxonomy,
        npi=NPI,
        checksum=1,
        healthcare_provider_taxonomy_code="207Q00000X",
        healthcare_provider_primary_taxonomy_switch="Y",
    )
    await _insert(
        session, models.NPIDataTaxonomyGroup, npi=NPI, checksum=1, healthcare_provider_taxonomy_group="193400000X"
    )
    await _insert(
        session,
        models.NPIDataOtherIdentifier,
        npi=NPI,
        checksum=1,
        other_provider_identifier="Synthetic Practice",
        other_provider_identifier_type_code="3",
    )
    await _insert(session, models.NPIPhoneStaffing, state_name="CA", telephone_number="2025550100")
    await _seed_locations(session)


async def _seed_locations(session):
    """Keep canonical and native address evidence aligned for the same provider."""
    for ordinal in range(1, 5):
        address_by_field = dict(
            npi=NPI,
            type="primary" if ordinal == 1 else "secondary",
            checksum=ordinal,
            first_line=f"{ordinal} Example Street",
            city_name="Example City",
            state_name="CA",
            postal_code="90210",
            country_code="US",
            telephone_number="2025550100",
            phone_number="2025550100",
            address_key=UUID(int=ordinal),
            lat=34.09,
            long=-118.4,
        )
        await _insert(session, models.NPIAddress, **address_by_field)
        await _insert(
            session,
            models.EntityAddressUnified,
            **address_by_field,
            entity_type="npi",
            entity_id=str(NPI),
            location_key=f"synthetic-location-{ordinal}",
            address_precision="street",
            address_sources=["nppes"],
            source_record_ids=[f"nppes:{NPI}:{ordinal}"],
            source_count=1,
            independent_source_count=1,
            formatted_address=f"{ordinal} Example Street, Example City, CA 90210",
        )
        await _insert(
            session,
            models.EntityAddressEvidence,
            evidence_id=ordinal,
            location_key=f"synthetic-location-{ordinal}",
            address_key=UUID(int=ordinal),
            entity_type="npi",
            entity_id=str(NPI),
            npi=NPI,
            source_id=1,
            source_run_id="synthetic-nppes",
            source_record_key=f"nppes:{NPI}:{ordinal}",
        )


async def _seed_enrichment(session):
    await _insert(
        session,
        models.ProviderEnrichmentSummary,
        npi=NPI,
        latest_reporting_year=2026,
        has_any_enrollment=True,
        has_ffs_enrollment=True,
        total_enrollment_rows=1,
        dataset_keys=["ffs_public"],
        ffs_enrollment_ids=["synthetic-enrollment"],
        primary_state="CA",
    )
    await _insert(
        session,
        models.ProviderEnrollmentFFS,
        npi=NPI,
        record_hash=1,
        enrollment_id="synthetic-enrollment",
        provider_type_code="14",
        provider_type_text="Family Practice",
        reporting_year=2026,
        state="CA",
        city="Example City",
        zip_code="90210",
        imported_at=STAMP,
    )
    common_by_field = dict(record_hash=1, enrollment_id="synthetic-enrollment", reporting_year=2026, imported_at=STAMP)
    await _insert(session, models.ProviderEnrollmentFFSAdditionalNPI, **common_by_field, additional_npi=NPI)
    await _insert(
        session,
        models.ProviderEnrollmentFFSAddress,
        **common_by_field,
        city="Example City",
        state="CA",
        zip_code="90210",
        address_key=UUID(int=1),
    )
    await _insert(
        session,
        models.ProviderEnrollmentFFSSecondarySpecialty,
        **common_by_field,
        provider_type_code="08",
        provider_type_text="Family Practice",
    )
    await _insert(
        session,
        models.ProviderEnrollmentFFSReassignment,
        record_hash=1,
        reassigning_enrollment_id="synthetic-enrollment",
        receiving_enrollment_id="synthetic-group",
        reporting_year=2026,
        imported_at=STAMP,
    )


async def _seed_directory(session, schema):
    """Populate a real source, dataset, resource, and projected profile."""
    await _insert(
        session,
        models.ProviderDirectoryAPIEndpoint,
        endpoint_id="synthetic-endpoint",
        canonical_api_base="https://directory.example/fhir",
        credential_descriptor_hash="a" * 64,
        endpoint_signature_hash="b" * 64,
    )
    await _insert(
        session,
        models.ProviderDirectorySource,
        source_id="synthetic-directory",
        org_name="Synthetic Directory",
        endpoint_id="synthetic-endpoint",
        canonical_api_base="https://directory.example/fhir",
    )
    await _insert(
        session,
        models.ProviderDirectoryEndpointDataset,
        dataset_id="synthetic-dataset",
        endpoint_id="synthetic-endpoint",
        status="published",
        is_current=True,
        acquisition_root_run_id="synthetic-directory-run",
        published_at=STAMP,
    )
    await _insert(
        session,
        models.ProviderDirectoryDatasetResource,
        dataset_id="synthetic-dataset",
        resource_type="Practitioner",
        resource_id="synthetic-practitioner",
        payload_hash="c" * 64,
        payload_json={
            "resourceType": "Practitioner",
            "id": "synthetic-practitioner",
            "npi": str(NPI),
            "active": True,
        },
    )
    await _seed_projected_profile(session, schema)


async def _seed_projected_profile(session, schema):
    """Retain real projected fields and evidence used by the full detail loader."""
    await session.execute(
        text(
            f'INSERT INTO "{schema}".provider_directory_profile '
            "(npi,profile_json,evidence_json,source_ids,endpoint_ids,dataset_ids,source_count,"
            "independent_source_count,fact_count,generation_id,published_at) "
            "VALUES (:npi,CAST(:profile AS jsonb),CAST(:evidence AS jsonb),"
            "ARRAY['synthetic-directory'],ARRAY['synthetic-endpoint'],ARRAY['synthetic-dataset'],"
            "1,1,1,:generation,:published)"
        ),
        {
            "npi": NPI,
            "profile": json.dumps({"npi": NPI, "languages": [{"code": "en", "display": "English"}]}),
            "evidence": json.dumps(
                {"npi": NPI, "source_id": "synthetic-directory", "resource_id": "synthetic-practitioner"}
            ),
            "generation": "pdprofile_" + "1" * 32,
            "published": STAMP,
        },
    )


@asynccontextmanager
async def _enrollment_schema_alias(fixture):
    """Bind legacy literal-schema enrollment reads to the same owned relation OIDs."""
    async with fixture.engine.begin() as connection:
        assert await connection.scalar(text("SELECT to_regnamespace('mrf')")) is None
        await connection.execute(text("CREATE SCHEMA mrf"))
    try:
        async with fixture.engine.begin() as connection:
            for name in ("provider_enrollment_ffs", "provider_enrollment_ffs_reassignment"):
                await connection.execute(text(f'CREATE VIEW mrf.{name} AS SELECT * FROM "{fixture.schema}".{name}'))
        yield
    finally:
        async with fixture.engine.begin() as connection:
            await connection.execute(text("DROP SCHEMA mrf CASCADE"))
            assert await connection.scalar(text("SELECT to_regnamespace('mrf')")) is None


@asynccontextmanager
async def detail_database(monkeypatch, tmp_path):
    """Install all detail families and seal the genuine NPI receipt on populated tables."""
    assert len(RELATIONS) == 57
    create_scalars = receipt_fixture._create_scalar_tables

    async def install(connection, schema):
        await _install_detail_tables(connection, schema, monkeypatch, create_scalars)

    monkeypatch.setattr(receipt_fixture, "_create_scalar_tables", install)
    async with doctors_database(monkeypatch, cms_active=False) as fixture, _enrollment_schema_alias(fixture):
        monkeypatch.setattr(npi_api, "db", fixture.database)
        monkeypatch.setattr(npi_api, "_NPI_DETAIL_RESPONSE_CACHE", npi_api.OrderedDict())
        monkeypatch.setenv(npi_api.ADDRESS_SERVING_SOURCE_ENV, npi_api.ADDRESS_SERVING_SOURCE_UNIFIED)
        async with fixture.database.transaction() as session:
            await _seed_identity_and_locations(session)
            await _seed_enrichment(session)
            await _seed_directory(session, fixture.schema)
            await address_generation.publish_local_entity_address_generation(
                fixture.database, schema_name=fixture.schema
            )
            await _insert(
                session,
                models.ImportRun,
                run_id=RUN_ID,
                importer="npi",
                status="running",
                phase_detail="process_data running",
                heartbeat_at=STAMP,
                progress={"attempt_id": ATTEMPT_ID, "attempt_started_at": ATTEMPT_STARTED_AT},
                metrics={},
            )
        raw = await connect(fixture.engine.url)
        try:
            chain = await _admitted_chain(raw, fixture.schema, tmp_path)
            async with raw.transaction():
                await _lock_attempt(raw, fixture.schema)
                fixture.npi_receipt = await _finalize_publication(
                    raw, fixture.schema, chain.chain_ref, (1, 4, 1, 1, 1, 1)
                )
        finally:
            await raw.close()
        async with fixture.database.transaction() as session:
            oids = await snapshot._relation_oids(session, fixture.schema, RELATIONS)
            assert len(oids) == 57 and all(oids.values())
            fixture.npi_identity = await npi_api._npi_canonical_publication_identity(session=session)
            assert fixture.npi_identity is not None
            assert (await npi_api._fetch_provider_enrichment_detail(NPI, session=session))["summary"] is not None
        fixture.stage = await stage_family(fixture.database, fixture.schema)
        for model in preparation._models():
            stage = native.make_class(model, fixture.stage["import_date"])
            await native._create_stage_indexes(model, fixture.schema)
            await native._create_stage_indexes(stage, fixture.schema)
            await fixture.database.status(f'ALTER TABLE "{fixture.schema}"."{stage.__tablename__}" SET LOGGED')
            await fixture.database.status(f'ANALYZE "{fixture.schema}"."{stage.__tablename__}"')
        yield fixture
