# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Full-shape, bounded provider detail data for native publication load checks."""

import importlib
import json
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from uuid import UUID

from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from api import provider_profile_snapshot as snapshot
from db import models
from db.connection import Database
from db.plan_release_storage import storage_identifier
from process import entity_address_result_generation as address_generation
from process import npi_result_generation as npi_generation
from process import nppes_public_evidence_storage as nppes_storage
from process import provider_directory_generic_source_policy as source
from process import provider_directory_profile as profile
from process.provider_directory_source_local_publication import publish_validated_source_local_dataset
from tests import test_provider_directory_cms_serving_receipt_postgres as receipt_fixture
from tests.cms_doctors_component_postgres_support import prepare_doctors_component
from tests.cms_doctors_preparation_postgres_support import _bind_doctors_database, native, preparation, stage_family
from tests.nppes_candidate_postgres_support import composed_nppes_component
from tests.provider_directory_source_postgres_support import source_component_database
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
fhir = importlib.import_module("process.provider_directory_fhir")
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
            "address_alias_state_v1",
        )
    )
)


async def _install_detail_tables(component, monkeypatch):
    """Add full detail relations before Source provision without replacing existing roots."""
    schema = component.policy.schema
    async with component.admin_engine.begin() as connection:
        assert await connection.scalar(text("SELECT session_user=current_user")) is True
        before_by_name = await snapshot._relation_oids(connection, schema, RELATIONS)
        later_names = {
            "npi_result_generation",
            "npi_canonical_publication_receipt",
            "npi_canonical_publication_receipt_seal",
        }
        wanted = (set(RELATIONS) | {"provider_directory_api_endpoint", "import_run"}) - later_names
        tables_by_name = {table.name: table for table in models.db.metadata.tables.values() if table.name in wanted}
        metadata = MetaData(schema=schema)
        for table in tables_by_name.values():
            table.to_metadata(metadata, schema=schema)
            monkeypatch.setattr(table, "schema", schema)
        await connection.run_sync(metadata.create_all)
        for name, sql in (
            (profile.PROFILE_TABLE, profile.profile_table_sql(schema, logged=True)),
            (profile.PROFILE_EVIDENCE_TABLE, profile.profile_evidence_table_sql(schema, logged=True)),
        ):
            if await connection.scalar(text("SELECT to_regclass(:name)"), {"name": f"{schema}.{name}"}) is None:
                await connection.execute(text(sql))
        await _install_detail_migrations(connection)
        await npi_generation.install_npi_stage_mutation_guards(
            connection, schema_name=schema, stage_tables=npi_generation.RELATION_NAMES
        )
        await _align_npi_receipt_validator_owner(connection, component.policy)
        await _grant_nppes_fixture_inputs(connection, component.policy)
        after_by_name = await snapshot._relation_oids(connection, schema, RELATIONS)
        assert len(after_by_name) == 59
        assert all(
            oid is not None
            for name, oid in after_by_name.items()
            if not name.endswith((".npi_canonical_publication_receipt", ".npi_canonical_publication_receipt_seal"))
        )
        assert all(after_by_name[name] == oid for name, oid in before_by_name.items() if oid is not None)
        await _grant_detail_reads(connection, component, before_by_name, after_by_name)


async def _install_detail_migrations(connection):
    """Install supplemental detail relations; the complete bootstrap owns NPI state."""
    for prefix in (
        "20260701090000",
        "20260701110000",
    ):
        await connection.run_sync(lambda sync, prefix=prefix: receipt_fixture._apply(sync, prefix))


async def _grant_nppes_fixture_inputs(connection, policy):
    """Expose legacy clone templates and append-only metadata to the actual Source owner."""
    namespace, owner = storage_identifier(policy.schema), storage_identifier(policy.owner_role)
    for table, *_fields in nppes_storage.ROOTS.values():
        await connection.execute(text(f"GRANT SELECT ON TABLE {namespace}.{storage_identifier(table)} TO {owner}"))
    for table in ("public_evidence_source_identity", "public_evidence_source_release"):
        await connection.execute(
            text(f"GRANT SELECT,INSERT ON TABLE {namespace}.{storage_identifier(table)} TO {owner}")
        )
    foundation = receipt_fixture._migration("20260808090000")
    for function, arguments in (
        ("public_evidence_source_identity_ref", "text,text,bytea"),
        ("public_evidence_source_release_valid", foundation._RELEASE_VALIDATOR_ARGUMENT_TYPES),
        ("public_evidence_nppes_archive_period", "text"),
        ("public_evidence_nppes_primary_period", "text"),
        ("public_evidence_record_digest", "text,text"),
        ("public_evidence_record_ref", "text,text,text"),
    ):
        await connection.execute(
            text(f"GRANT EXECUTE ON FUNCTION {namespace}.{storage_identifier(function)}({arguments}) TO {owner}")
        )


async def _align_npi_receipt_validator_owner(connection, policy):
    """Match the administrator actor used by the original canonical receipt fixture."""
    migration = receipt_fixture._migration("20260808230000")
    function = f"{storage_identifier(policy.schema)}.{storage_identifier(migration._VALIDATOR)}()"
    query = text("""SELECT p.oid::bigint AS oid,p.prosrc,p.prosecdef,p.proconfig,
        pg_get_userbyid(p.proowner)::text AS owner,current_user::text AS administrator,
        coalesce((SELECT jsonb_agg(jsonb_build_array(a.grantee::bigint,a.privilege_type,a.is_grantable)
            ORDER BY a.grantee,a.privilege_type,a.is_grantable)
            FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
            WHERE a.grantee<>p.proowner),'[]'::jsonb) AS explicit_grants
        FROM pg_proc p WHERE p.oid=to_regprocedure(:function)""")
    before_by_field = dict((await connection.execute(query, {"function": function})).mappings().one())
    assert before_by_field["owner"] == policy.control_runtime_roles[0]
    assert before_by_field["prosecdef"] is True and before_by_field["proconfig"] == ["search_path=pg_catalog"]
    await connection.execute(text(f"ALTER FUNCTION {function} OWNER TO CURRENT_USER"))
    after_by_field = dict((await connection.execute(query, {"function": function})).mappings().one())
    assert after_by_field["owner"] == before_by_field["administrator"]
    assert {key: value for key, value in before_by_field.items() if key != "owner"} == {
        key: value for key, value in after_by_field.items() if key != "owner"
    }


async def _grant_detail_reads(connection, component, before_by_name, after_by_name):
    """Keep new supplemental models on the Source owner and expose only Reader SELECT."""
    policy = component.policy
    model_names = {table.name for table in models.db.metadata.tables.values()}
    model_names.update((profile.PROFILE_TABLE, profile.PROFILE_EVIDENCE_TABLE))
    for qualified, oid in after_by_name.items():
        if oid is None:
            continue
        namespace, name = qualified.split(".")
        relation = f"{storage_identifier(namespace)}.{storage_identifier(name)}"
        if before_by_name[qualified] is None and name in model_names:
            await connection.execute(text(f"ALTER TABLE {relation} OWNER TO {storage_identifier(policy.owner_role)}"))
        for role in (policy.owner_role, *policy.runtime_roles):
            await connection.execute(text(f"GRANT SELECT ON TABLE {relation} TO {storage_identifier(role)}"))


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


async def _seed_directory_source(session):
    """Install the source identity in its required READ COMMITTED owner transaction."""
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


async def _seed_directory():
    """Persist and validate the real resource in its repeatable-read Source transaction."""
    candidate = fhir.EndpointDatasetCandidate(
        dataset_id="synthetic-dataset",
        endpoint_id="synthetic-endpoint",
        acquisition_root_run_id="synthetic-directory-run",
        import_run_id=None,
        source_ids=("synthetic-directory",),
        selected_resources=("Practitioner",),
        previous_dataset_id=None,
        resource_hash_contract=fhir.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )
    await fhir._ensure_endpoint_dataset_candidate(candidate)
    resource_by_field = {
        "resourceType": "Practitioner",
        "id": "synthetic-practitioner",
        "npi": str(NPI),
        "active": True,
        "identifier": [{"system": "http://hl7.org/fhir/sid/us-npi", "value": str(NPI)}],
        "communication": [{"coding": [{"system": "urn:ietf:bcp:47", "code": "en", "display": "English"}]}],
    }
    parsed = fhir.parse_fhir_resource("synthetic-directory", resource_by_field, run_id="synthetic-directory-run")
    assert parsed is not None and parsed[1]["npi"] == NPI
    await fhir._persist_endpoint_dataset_rows(
        parsed[0],
        [parsed[1]],
        candidate.dataset_id,
        resource_hash_contract=candidate.resource_hash_contract,
        semantic_projection_as_of=None,
    )
    summary = await fhir._finalize_endpoint_dataset_candidate(
        candidate, {"Practitioner": {"complete": True, "bounded": False, "error": None, "next_url_remaining": False}}
    )
    assert summary is not None and summary["status"] == fhir.ENDPOINT_DATASET_VALIDATED
    return candidate


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


async def _seed_detail_incumbents(fixture):
    """Seed synthetic incumbents through administration without changing application actors."""
    administrator = Database(
        engine=fixture.engine,
        session_factory=async_sessionmaker(fixture.engine, expire_on_commit=False),
    )
    async with administrator.transaction() as session:
        assert await session.scalar(text("SELECT session_user=current_user")) is True
        await _seed_identity_and_locations(session)
        await _seed_enrichment(session)
        await _seed_projected_profile(session, fixture.schema)
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


@asynccontextmanager
async def _enrollment_schema_alias(fixture):
    """Bind legacy literal-schema enrollment reads to the same owned relation OIDs."""
    if fixture.schema == "mrf":
        async with fixture.engine.begin() as connection:
            for name in ("provider_enrollment_ffs", "provider_enrollment_ffs_reassignment"):
                assert await connection.scalar(text("SELECT to_regclass(:name)"), {"name": f"mrf.{name}"}) is not None
        yield
        return
    async with fixture.engine.begin() as connection:
        assert await connection.scalar(text("SELECT to_regnamespace('mrf')")) is None
        await connection.execute(text("CREATE SCHEMA mrf"))
    try:
        async with fixture.engine.begin() as connection:
            for name in ("provider_enrollment_ffs", "provider_enrollment_ffs_reassignment"):
                await connection.execute(text(f'CREATE VIEW mrf.{name} AS SELECT * FROM "{fixture.schema}".{name}'))
            role = fixture.database._reader_database._reader_login[0]
            await connection.execute(text(f'GRANT USAGE ON SCHEMA mrf TO "{role}"'))
            for name in ("provider_enrollment_ffs", "provider_enrollment_ffs_reassignment"):
                await connection.execute(text(f'GRANT SELECT ON mrf.{name} TO "{role}"'))
        yield
    finally:
        async with fixture.engine.begin() as connection:
            await connection.execute(text("DROP SCHEMA mrf CASCADE"))
            assert await connection.scalar(text("SELECT to_regnamespace('mrf')")) is None


@asynccontextmanager
async def _detail_source_fixture(monkeypatch):
    """Compose genuine NPPES custody before the first Doctors authority capture."""

    async def install(component):
        await _install_detail_tables(component, monkeypatch)

    from tests import provider_directory_source_postgres_support as source_fixture
    from tests.cms_registry_complete_publication_postgres_support import (
        _bootstrap_native_source,
        source_component_native_migrations,
    )

    migrations = source_component_native_migrations()
    monkeypatch.setattr(source_fixture, "bootstrap_source_component", _bootstrap_native_source)
    async with source_component_database(
        monkeypatch, before_provision=install, migration_prefixes=migrations
    ) as component:
        async with composed_nppes_component(component, migration_prefixes=migrations):
            await prepare_doctors_component(component)
            _bind_doctors_database(monkeypatch, component.database)
            monkeypatch.setattr(fhir, "db", component.database)
            async with component.admin_engine.begin() as connection:
                after_by_name = await snapshot._relation_oids(connection, component.policy.schema, RELATIONS)
                assert len(after_by_name) == 59 and all(after_by_name.values())
                await _grant_detail_reads(connection, component, after_by_name, after_by_name)
            yield SimpleNamespace(
                engine=component.admin_engine,
                schema=component.policy.schema,
                database=component.database,
                component=component,
                initial=None,
            )


@asynccontextmanager
async def detail_database(monkeypatch, tmp_path):
    """Install all detail families and seal the genuine NPI receipt on populated tables."""
    assert len(RELATIONS) == 59

    async with (
        _detail_source_fixture(monkeypatch) as fixture,
        _enrollment_schema_alias(fixture),
    ):
        monkeypatch.setattr(npi_api, "db", fixture.database)
        monkeypatch.setattr(npi_api, "_NPI_DETAIL_RESPONSE_CACHE", npi_api.OrderedDict())
        monkeypatch.setenv(npi_api.ADDRESS_SERVING_SOURCE_ENV, npi_api.ADDRESS_SERVING_SOURCE_UNIFIED)
        await _seed_detail_incumbents(fixture)
        async with source.source_transaction(fixture.database) as session:
            await _seed_directory_source(session)
        async with fixture.database.transaction():
            directory_candidate = await _seed_directory()
            await address_generation.publish_local_entity_address_generation(
                fixture.database, schema_name=fixture.schema
            )
        async with source.source_transaction(fixture.database, isolation="read committed"):
            await publish_validated_source_local_dataset(fhir, directory_candidate, "synthetic-directory")
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
            assert len(oids) == 59 and all(oids.values())
            fixture.npi_identity = await npi_api._npi_canonical_publication_identity(session=session)
            assert fixture.npi_identity is not None
            assert (await npi_api._fetch_provider_enrichment_detail(NPI, session=session))["summary"] is not None
        async with _prepared_detail_stage(fixture):
            yield fixture


@asynccontextmanager
async def _prepared_detail_stage(fixture):
    """Retain real identity maps through publication and clean only their owned tables."""
    fixture.stage = await stage_family(fixture.database, fixture.schema)
    try:
        stage_cls = native.make_class(native.DoctorClinicianAddress, fixture.stage["import_date"])
        await native._prepare_cms_doctors_sources(fixture.stage, stage_cls, fixture.schema, 1)
        async with fixture.database.transaction():
            for model in preparation._models():
                stage = native.make_class(model, fixture.stage["import_date"])
                await native._create_stage_indexes(model, fixture.schema)
                await native._create_stage_indexes(stage, fixture.schema)
                await fixture.database.status(f'ALTER TABLE "{fixture.schema}"."{stage.__tablename__}" SET LOGGED')
                await fixture.database.status(f'ANALYZE "{fixture.schema}"."{stage.__tablename__}"')
        yield fixture
    finally:
        await native.discard_doctors_identities(fixture.database, fixture.stage)
