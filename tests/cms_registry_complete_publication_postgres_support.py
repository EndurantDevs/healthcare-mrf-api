# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Task-local authority enrollment around actual native CMS builders.

The public synthetic signing key enrolls this isolated test database only. These
helpers do not establish production admission or an image deployment witness.
"""

import datetime
import json
import os
import re
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict
from functools import partial
from pathlib import Path
from uuid import uuid4

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import Index, MetaData, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.connection import Base, Database
from db.tiger_models import Zip_zcta5, ZipState
from process import cms_doctors_immutable as immutable_doctors
from process import cms_doctors_preparation as doctors
from process import entity_address_result_generation as addresses
from process import geo_source_storage as immutable_geo
from process import npi_result_generation as npi
from process import provider_directory_cms_native_inputs as inputs
from process import provider_directory_cms_nonprofile_capacity as nonprofile
from process import provider_directory_cms_serving_receipt as serving_receipt
from process import provider_directory_generic_source_policy as source_policy
from process import provider_directory_profile as profile
from process import provider_directory_profile_capacity_attestation as leases
from process import provider_directory_profile_capacity_control_projection as control_projection
from process import provider_directory_profile_runtime_observation as runtime
from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_selection_snapshot as selection_snapshot
from process import reference_family_result_generation as reference
from process.entity_address_snapshot_source import entity_address_unified
from process.ext import address_canon
from process.network_cms_registry_source_pair import capture_registry_cms_source_pair, require_registry_cms_source_pair
from tests import cms_npd_admission_postgres_support as source
from tests import cms_registry_wal_support as wal_support
from tests import provider_directory_profile_capacity_signing_guard_test_support as signing
from tests import test_provider_directory_import_run_guards as run_guards
from tests.test_network_cms_registry_source_pair_postgres import _bounded_source_copy, _publication_proof
from tests.test_provider_directory_cms_storage_continuation import _envelope as sign_storage
from tests.test_provider_directory_profile_capacity_attestation import _signed_envelope
from tests.test_provider_directory_profile_selection_attestation import _variant_registry_rows

NATIVE_INPUT_MIGRATIONS = (
    "20260615170000",
    "20260616090000",
    "20260628143000",
    "20260629160000",
    "20260729120000",
    "20260808090000",
    "20260808100000",
    "20260808170000",
    "20260811110000_address_formatted_display",
    "20260815010000",
    "20260816020000",
    "20260808220000",
    "20260808230000",
    "20260809020000",
    "20260825090000",
    "20260914120000_npi_result_generation",
    "20260920100000",
    "20260920110000",
    "20260920120000",
    "20260920130000",
    "20260923000000",
    "20260929000000",
    "20260929040000",
    "20261001110000",
    "20261004000000",
    "20261019030000",
)


def source_component_native_migrations():
    """Merge fixed fixture prerequisites in the real migration ancestor order."""
    from tests.provider_directory_source_postgres_support import source_component_migrations

    baseline = source_component_migrations()
    assert len(baseline) == len(set(baseline)) == 99
    assert len(NATIVE_INPUT_MIGRATIONS) == len(set(NATIVE_INPUT_MIGRATIONS)) == 26
    from tests.nppes_candidate_postgres_support import nppes_source_component_migrations

    callable_migrations = nppes_source_component_migrations()
    selected = set(baseline) | set(NATIVE_INPUT_MIGRATIONS) | set(callable_migrations)
    assert set(baseline) & set(NATIVE_INPUT_MIGRATIONS) == {"20260920100000", "20260929000000", "20260929040000"}
    assert len(selected) == 130
    root = Path(__file__).resolve().parents[1]
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))
    script = ScriptDirectory.from_config(config)
    nodes = tuple(reversed(tuple(script.walk_revisions())))
    nodes_by_prefix = {
        prefix: [node for node in nodes if Path(node.path).name.startswith(prefix)] for prefix in selected
    }
    nodes_by_prefix = {
        prefix: ([node for node in found if "provider_directory" in Path(node.path).name] if len(found) > 1 else found)
        for prefix, found in nodes_by_prefix.items()
    }
    assert all(len(found) == 1 for found in nodes_by_prefix.values())
    prefix_by_revision = {found[0].revision: prefix for prefix, found in nodes_by_prefix.items()}
    assert len(prefix_by_revision) == len(selected)
    ordered_prefixes = tuple(prefix_by_revision[node.revision] for node in nodes if node.revision in prefix_by_revision)
    assert set(ordered_prefixes) == selected and len(ordered_prefixes) == 130
    assert tuple(prefix for prefix in ordered_prefixes if prefix in set(baseline)) == baseline
    return ordered_prefixes


def office_resources(original, revision):
    """Seal two separate units and one exact CMS office membership per unit."""
    resources = original(revision)
    resources["Practitioner"][0]["identifier"] = [{"system": "http://hl7.org/fhir/sid/us-npi", "value": "1234567893"}]
    resources["Location"] = []
    resources["PractitionerRole"] = []
    for unit in (2, 3):
        location = source._site_fixture()
        location.update(id=f"site-{unit}", name=f"Example Office {unit}")
        location["address"] = {
            "use": "work",
            "type": "physical",
            "line": ["123 Example Street", f"Suite {unit}"],
            "city": "Sample City",
            "state": "CA",
            "postalCode": "90210",
            "country": "US",
        }
        resources["Location"].append(location)
        resources["PractitionerRole"].append(
            {
                "id": f"role-{unit}",
                "practitioner": {"reference": "Practitioner/1234567893"},
                "organization": {"reference": "Organization/network-1"},
                "location": [{"reference": f"Location/site-{unit}"}],
            }
        )
    return resources


async def _create_native_relations(database, *, owner_role):
    """Create native model storage and migrated ImportRun guards before publication."""
    async with database.engine.begin() as connection:
        await connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        await connection.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
        await connection.execute(text("CREATE EXTENSION IF NOT EXISTS intarray"))
        await connection.execute(text("CREATE EXTENSION IF NOT EXISTS btree_gin"))
        await connection.execute(text("CREATE SCHEMA tiger"))
        metadata = MetaData()
        names = {name for namespace, name in inputs._relations("mrf") if namespace == "mrf"}
        names.update(reference.RELATION_NAMES_BY_IMPORTER["facility-anchors"])
        names.update(reference.RELATION_NAMES_BY_IMPORTER["mrf-address"])
        for table in Base.metadata.tables.values():
            if table.name in names:
                table.to_metadata(metadata, schema="mrf")
        for model in (
            *doctors._models(),
            entity_address_unified.EntityAddressUnified,
            *entity_address_unified.SUPPORT_TABLE_MODELS,
        ):
            if "mrf." + model.__tablename__ not in metadata.tables:
                model.__table__.to_metadata(metadata, schema="mrf")
        for model in (ZipState, Zip_zcta5):
            model.__table__.to_metadata(metadata, schema="tiger")
        await connection.run_sync(metadata.create_all)
        for model in (entity_address_unified.EntityAddressUnified, *entity_address_unified.SUPPORT_TABLE_MODELS):
            relation = f'"mrf"."{model.__tablename__}"'
            original_oid = await connection.scalar(text("SELECT to_regclass(:name)::oid::bigint"), {"name": relation})
            assert type(original_oid) is int and original_oid > 0
            await connection.execute(text(f'ALTER TABLE {relation} OWNER TO "{owner_role}"'))
            assert (
                await connection.scalar(
                    text(
                        "SELECT oid=:oid AND relowner=(SELECT oid FROM pg_roles WHERE rolname=:owner) "
                        "FROM pg_class WHERE oid=to_regclass(:name)"
                    ),
                    {"oid": original_oid, "owner": owner_role, "name": relation},
                )
                is True
            )
        for name, sql in (
            ("provider_directory_profile", profile.profile_table_sql("mrf", logged=True)),
            ("provider_directory_profile_evidence", profile.profile_evidence_table_sql("mrf", logged=True)),
            ("provider_directory_address_overlay", source.fhir.provider_directory_address_overlay_table_sql("mrf")),
            ("provider_directory_network_catalog", source.fhir.provider_directory_network_catalog_table_sql("mrf")),
        ):
            if not await connection.scalar(text("SELECT to_regclass(:name)"), {"name": "mrf." + name}):
                await connection.execute(text(sql))
            assert await connection.scalar(text(f'SELECT count(*) FROM "mrf"."{name}"')) == 0
            await connection.execute(text(f'ALTER TABLE "mrf"."{name}" OWNER TO "{owner_role}"'))
        await connection.execute(text("CREATE TABLE mrf.alembic_version(version_num varchar(128) NOT NULL)"))
        await connection.execute(
            text("INSERT INTO mrf.alembic_version VALUES('20261001110000_profile_initial_publication')")
        )
        await connection.run_sync(install_import_run_guards)


async def _initialize_native_inputs(database, monkeypatch, tmp_path, source_commit, *, component):
    """Use the same native models, producers and runtime observation for both fixture lifecycles."""
    assert database.engine is component.admin_engine
    await _create_native_relations(database, owner_role=component.policy.owner_role)
    await _seed_native_doctors(database)
    async with database.session_factory() as session, session.begin():
        await npi.install_npi_result_revision_guards(session, schema_name="mrf", stage_tables=npi.RELATION_NAMES)
        await npi.bootstrap_npi_result_generation(session, schema_name="mrf")
        for family in ("facility-anchors", "mrf-address", "tiger"):
            await reference.publish_local_reference_family_generation(
                session, importer_id=family, schema_name="tiger" if family == "tiger" else "mrf"
            )
    await _publish_fixed_native_families(database, component)
    monkeypatch.setattr(selection, "db", database)
    monkeypatch.setattr(selection_snapshot, "db", database)
    monkeypatch.setattr(entity_address_unified, "db", database)
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    identity = tmp_path / "native-test-source-commit"
    identity.write_text(source_commit + "\n")
    identity.chmod(0o444)
    monkeypatch.setattr(runtime, "PROFILE_RUNTIME_SOURCE_COMMIT_FILE", identity)
    monkeypatch.setattr(runtime, "_BUILD_IDENTITY_UID", os.getuid())
    monkeypatch.setattr(runtime, "_BUILD_IDENTITY_GID", os.getgid())
    observe = partial(runtime.observe_profile_runtime, database=database)
    monkeypatch.setattr(source.fhir, "observe_profile_runtime", runtime.observe_profile_runtime)
    return observe


async def _publish_fixed_native_families(database, component):
    """Provision fixed heaps as administrator; publish through the original Source Owner."""
    from db.models import GeoZipLookup
    from tests.cms_doctors_component_postgres_support import _provision_component
    from tests.geo_source_postgres_support import provision_geo
    from tests.provider_directory_source_postgres_support import verify_native_source_profile

    assert database.engine is component.admin_engine
    async with database.engine.begin() as connection:
        for model in (*doctors._models(), GeoZipLookup):
            table = model.__table__.to_metadata(MetaData(), schema=component.policy.schema)
            for spec in getattr(model, "__my_additional_indexes__", ()):
                index = Index(table.name + "_idx_" + spec["name"], *(table.c[name] for name in spec["index_elements"]))
                await connection.run_sync(lambda sync, index=index: index.create(sync, checkfirst=True))
        header = '"' + component.policy.schema + '".provider_directory_endpoint_dataset'
        original = await connection.scalar(text("SELECT to_regclass(:header)::oid::bigint"), {"header": header})
        assert type(original) is int and original > 0
        await connection.execute(text(f'ALTER TABLE {header} OWNER TO "{component.owner}"'))
        assert (
            await connection.scalar(
                text(
                    "SELECT oid=:oid AND relowner=(SELECT oid FROM pg_roles WHERE rolname=:owner) "
                    "FROM pg_class WHERE oid=to_regclass(:header)"
                ),
                {"oid": original, "owner": component.owner, "header": header},
            )
            is True
        )
    await _provision_component(component.admin_engine, component.policy)
    await provision_geo(component.admin_engine, component.policy, verify_native_source_profile)
    async with source_policy.bind_generic_source_transaction_factory(component.factory):
        async with source_policy.source_transaction(component.database) as session:
            for family, publish in (
                ("cms-doctors", immutable_doctors.publish_doctors_generation),
                ("geo", immutable_geo.publish_geo_generation),
            ):
                current = await reference.read_reference_family_result_generation_authority(
                    session, importer_id=family, schema_name=component.policy.schema, lock=True
                )
                assert current.local_generation == 0 and current.serving_generation is None
                await publish(session, schema_name=component.policy.schema, expected_authority=current)


async def _source_native_control_catalog(connection):
    """Pin control identities/shapes and ledger indexes across the checked custody cut."""
    result = await connection.scalar(
        text("""SELECT jsonb_agg(jsonb_build_array(
        c.oid,c.relname,
        (SELECT jsonb_agg(jsonb_build_array(a.attnum,a.attname,a.atttypid,a.atttypmod,a.attnotnull,
        a.attcollation,a.attidentity,a.attgenerated,a.attisdropped) ORDER BY a.attnum)
        FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0),
        (SELECT jsonb_agg(to_jsonb(k) ORDER BY k.oid) FROM pg_constraint k WHERE k.conrelid=c.oid),
        CASE WHEN c.relname='reference_family_result_generation' THEN
        (SELECT jsonb_agg(pg_get_indexdef(i.indexrelid) ORDER BY i.indexrelid) FROM pg_index i WHERE i.indrelid=c.oid) END
        ) ORDER BY c.relname)
        FROM pg_class c WHERE c.relnamespace='mrf'::regnamespace
        AND c.relname IN ('reference_family_result_generation','cms_doctor_group_site')""")
    )
    assert type(result) is list and len(result) == 2
    revision_function = await connection.scalar(
        text(
            "SELECT jsonb_build_array(p.oid,p.proowner,p.proacl,pg_get_functiondef(p.oid)) FROM pg_proc p "
            "WHERE p.oid=to_regprocedure('mrf.advance_reference_source_generation()')"
        )
    )
    assert type(revision_function) is list and len(revision_function) == 4
    return result, revision_function


async def prepare_source_native_inputs(component, monkeypatch, tmp_path, source_commit, *, metadata_read_roles=()):
    """Finish genuine native prerequisites before the protected Source cut closes writers."""
    from process.provider_directory_cms_control_privileges import cms_origin_metadata_read_roles

    assert component.policy.schema == "mrf" and not source.fhir.db._transaction_binding()
    if metadata_read_roles:
        metadata_read_roles = cms_origin_metadata_read_roles(component.policy, metadata_read_roles)
    database = Database(engine=component.admin_engine, session_factory=async_sessionmaker(component.admin_engine))
    async with component.admin_engine.begin() as connection:
        for role in metadata_read_roles:
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{component.policy.schema}" TO "{role}"'))
        original = await _source_native_control_catalog(connection)
    await _initialize_native_inputs(database, monkeypatch, tmp_path, source_commit, component=component)
    from tests.cms_output_snapshot_postgres_support import provision_native_outputs

    await provision_native_outputs(component)
    async with component.admin_engine.begin() as connection:
        assert await _source_native_control_catalog(connection) == original
        owner = '"' + component.owner + '"'
        await connection.execute(text(f"GRANT SET ON PARAMETER temp_file_limit TO {owner}"))
        await connection.execute(text(f"GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO {owner}"))
        names = tuple(sorted(set(inputs._relations("mrf")) | {("mrf", "alembic_version")}))
        for namespace, name in names:
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{namespace}" TO {owner}'))
            rights = "SELECT" if name == "alembic_version" else "SELECT,MAINTAIN"
            await connection.execute(text(f'GRANT {rights} ON TABLE "{namespace}"."{name}" TO {owner}'))
        from process.provider_directory_cms_control_privileges import grant_cms_control_privileges

        raw = await connection.get_raw_connection()
        await grant_cms_control_privileges(raw.driver_connection, component.policy)
        assert await connection.scalar(
            text("SELECT has_parameter_privilege(:owner,'temp_file_limit','SET')"), {"owner": component.owner}
        )
        assert await connection.scalar(
            text("SELECT has_function_privilege(:owner,'pg_catalog.pg_control_system()','EXECUTE')"),
            {"owner": component.owner},
        )
    component.factory.enroll_ordinary_native_inputs()


async def _bootstrap_native_source(engine, policy, *, migration_prefixes):
    """Install canonical primitives between the real Builder migration stages."""
    from sqlalchemy.ext.asyncio import create_async_engine

    from tests.nppes_candidate_postgres_support import (
        _bootstrap_nppes_parent_component,
        install_source_canonical_functions,
    )

    assert migration_prefixes == source_component_native_migrations()
    boundary = migration_prefixes.index("20261016040000")
    await _bootstrap_nppes_parent_component(engine, policy, migration_prefixes[:boundary])
    historical = create_async_engine(engine.url.set(username=policy.control_runtime_roles[0], password=None))
    try:
        await install_source_canonical_functions(historical, policy.schema)
        async with historical.connect() as connection:
            await connection.run_sync(source._run_migrations, migration_prefixes[boundary:])
    finally:
        await historical.dispose()


@asynccontextmanager
async def source_native_publication_database(monkeypatch, tmp_path, source_commit, *, metadata_read_roles=()):
    """Compose the original protected acquisition fixture with the verified combined bootstrap."""
    from tests import provider_directory_source_postgres_support as native_source

    migrations = source_component_native_migrations()
    with monkeypatch.context() as settings:
        settings.setattr(native_source, "bootstrap_source_component", _bootstrap_native_source)
        prepare = partial(
            prepare_source_native_inputs,
            monkeypatch=settings,
            tmp_path=tmp_path,
            source_commit=source_commit,
            metadata_read_roles=metadata_read_roles,
        )
        async with source.protected_admission_database(
            settings, before_provision=prepare, migration_prefixes=migrations, publisher_pool_size=6
        ) as administrator:
            component = administrator._source_component
            await _register_native_heaps(administrator, component)
            namespace, migrator = "native_namespace_" + uuid4().hex, "native_migrator_" + uuid4().hex
            from db.migration_nppes_evidence_candidates import upgrade_nppes_evidence_candidates
            from tests import nppes_candidate_postgres_support as nppes_fixture

            try:
                async with component.admin_engine.begin() as connection:
                    await connection.run_sync(lambda sync: upgrade_nppes_evidence_candidates(sync, "mrf"))
                await nppes_fixture._enroll_native_custody(
                    component, namespace, migrator, native_inputs=True, metadata_read_roles=metadata_read_roles
                )
                await _adopt_native_counter(component)
                await _grant_profile_controls(administrator, component.policy)
                for module in (selection, selection_snapshot, entity_address_unified, address_canon):
                    settings.setattr(module, "db", component.database)
                settings.setattr(source.fhir, "observe_profile_runtime", runtime.observe_profile_runtime)
                yield administrator
            finally:
                await nppes_fixture._drop_nppes_component_roles(component.admin_engine, (migrator, namespace))


async def _counter_inventory(driver, policy, custody_by_role):
    """Retain the complete native payload, roles and namespace across only custody DDL."""
    from process import provider_directory_native_counter_catalog as counter
    from process import provider_directory_source_callable_custody as callables

    graph, roles, namespace, reach = await callables._capture_roles(driver, policy, **custody_by_role)
    census_by_field = dict(await driver.fetchrow(counter._CENSUS_SQL, namespace["oid"], list(counter.FUNCTIONS)))
    catalog = await counter._capture_counter_catalog(driver, policy, roles, namespace, census_by_field)
    defaults = callables._decoded(
        await driver.fetchval(
            "SELECT COALESCE(jsonb_agg(to_jsonb(d) ORDER BY d.oid),'[]'::jsonb) FROM pg_default_acl d"
        )
    )
    return dict(
        graph=graph,
        roles=roles,
        namespace=namespace,
        reach=reach,
        census_by_field=census_by_field,
        catalog=catalog,
        defaults=defaults,
    )


def _counter_immutable_inventory(snapshot):
    """Project only the ownership/ACL fields independently checked by the native contract."""
    inventory = deepcopy(snapshot)
    payload = inventory["catalog"]
    catalog = payload["ledger"]["catalog"]
    for relation in catalog["classes"]:
        relation["relowner"] = relation["relacl"] = None
    for attribute in catalog["attributes"]:
        attribute["attacl"] = None
    for field in ("table_acl", "column_acl", "access"):
        catalog[field] = None
    payload["ledger"]["composite"]["typowner"] = None
    for function in payload["functions"].values():
        function["definition"]["proowner"] = function["definition"]["proacl"] = None
        function["acl"] = None
    return inventory


async def _adopt_native_counter(component):
    """Transfer the genuine fixed counter under complete before/after callable proof."""
    from process.provider_directory_cms_callable_custody import _admin_identity
    from process.provider_directory_native_counter_catalog import _CENSUS_SQL, FUNCTIONS, ROOT

    policy = component.policy
    table = f'"{policy.schema}"."{ROOT}"'
    custody_by_role = dict(
        migration_role=component.factory.migration_role, namespace_owner_role=component.factory.namespace_owner_role
    )
    from process.provider_directory_source_callable_custody import attest_source_callable_capability

    async with component.admin_engine.begin() as connection:
        await connection.execute(text("SELECT current_user=session_user"))
        driver = (await connection.get_raw_connection()).driver_connection
        await driver.fetchval("SELECT pg_current_xact_id()::text")
        actor = await _admin_identity(driver, policy)
        await driver.execute(f"LOCK TABLE ONLY {table} IN ACCESS EXCLUSIVE MODE NOWAIT")
        namespace = await driver.fetchval("SELECT to_regnamespace($1)::oid", policy.schema)
        census_by_field = dict(await driver.fetchrow(_CENSUS_SQL, namespace, list(FUNCTIONS)))
        before = await attest_source_callable_capability(driver, policy, **custody_by_role)
        previous = await _counter_inventory(driver, policy, custody_by_role)
        counter_rows = await driver.fetch(f"SELECT * FROM {table} ORDER BY relation_oid")
        advance = await driver.fetchval(
            "SELECT to_regprocedure($1)::oid::bigint", f'"{policy.schema}"."{FUNCTIONS[0]}"()'
        )
        assert type(advance) is int and advance not in before
        owner, builder = f'"{policy.owner_role}"', f'"{policy.control_runtime_roles[0]}"'
        await driver.execute(f"ALTER TABLE {table} OWNER TO {owner}")
        await driver.execute(f"GRANT SELECT ON TABLE {table} TO {builder}")
        await driver.execute(f"GRANT INSERT(relation_oid,schema_name,table_name) ON TABLE {table} TO {builder}")
        for name in FUNCTIONS:
            await driver.execute(f'ALTER FUNCTION "{policy.schema}"."{name}"() OWNER TO {owner}')
        await driver.execute(f'GRANT EXECUTE ON FUNCTION "{policy.schema}"."{FUNCTIONS[0]}"() TO {builder}')
        after = await attest_source_callable_capability(driver, policy, **custody_by_role)
        assert after == tuple(sorted((*before, advance)))
        current = await _counter_inventory(driver, policy, custody_by_role)
        assert _counter_immutable_inventory(current) == _counter_immutable_inventory(previous)
        assert dict(await driver.fetchrow(_CENSUS_SQL, namespace, list(FUNCTIONS))) == census_by_field
        assert await driver.fetch(f"SELECT * FROM {table} ORDER BY relation_oid") == counter_rows
        assert await _admin_identity(driver, policy) == actor


async def _grant_profile_controls(database, policy):
    """Grant only the existing selection and Profile control protocols."""
    rights_by_table = {
        "provider_directory_profile_selection_proof": "SELECT,INSERT,MAINTAIN",
        "provider_directory_profile_selection_authority": "SELECT,INSERT,UPDATE",
        "provider_directory_profile_selection_observation": "SELECT,INSERT",
        "provider_directory_profile_build_checkpoint": "SELECT,INSERT,UPDATE,DELETE",
        "provider_directory_profile_capacity_lease_consumption": "SELECT,INSERT,MAINTAIN",
        "provider_directory_profile_serving_generation": "SELECT,INSERT,UPDATE",
        "provider_directory_profile_capacity_preflight_receipt": "SELECT,INSERT,UPDATE",
        "provider_directory_profile_initial_receipt": "SELECT,INSERT,MAINTAIN",
        "provider_directory_profile_failed_cleanup_claim": "SELECT",
        "provider_directory_profile_delta_receipt": "SELECT,INSERT",
        "npi_result_generation": "SELECT,MAINTAIN",
    }
    async with database.engine.begin() as connection:
        for name, rights in rights_by_table.items():
            await connection.execute(text(f'GRANT {rights} ON "{policy.schema}"."{name}" TO "{policy.owner_role}"'))
        for name in addresses.RELATION_NAMES:
            await connection.execute(text(f'GRANT SELECT ON "{policy.schema}"."{name}" TO "{policy.owner_role}"'))
        from process.provider_directory_cms_control_privileges import grant_cms_control_privileges

        raw = await connection.get_raw_connection()
        await grant_cms_control_privileges(raw.driver_connection, policy)
        assert (
            await connection.scalar(
                text("SELECT NOT has_table_privilege(:owner,:table,'SELECT,UPDATE')"),
                {"owner": policy.owner_role, "table": f'"{policy.schema}"."import_run"'},
            )
            is True
        )
        await connection.execute(text(f'GRANT USAGE ON SCHEMA pg_toast TO "{policy.owner_role}"'))
        toast_query = await connection.execute(
            text("""SELECT toast.relname FROM pg_catalog.pg_class relation
                JOIN pg_catalog.pg_class toast ON toast.oid=relation.reltoastrelid
                JOIN pg_catalog.pg_namespace namespace ON namespace.oid=toast.relnamespace
                WHERE relation.relnamespace=to_regnamespace(:schema)
                AND relation.relname=ANY(:names) AND namespace.nspname='pg_toast'
                AND toast.relowner<>(SELECT oid FROM pg_catalog.pg_roles WHERE rolname=:owner)
                ORDER BY relation.relname"""),
            {"schema": policy.schema, "names": ["import_run"], "owner": policy.owner_role},
        )
        for toast_relation in toast_query.all():
            assert re.fullmatch(r"pg_toast_[1-9][0-9]*", toast_relation.relname)
            await connection.execute(
                text(f'GRANT SELECT(chunk_id) ON "pg_toast"."{toast_relation.relname}" TO "{policy.owner_role}"')
            )


async def _register_native_heaps(database, component):
    """Enroll ordinary inputs after the Source cut proves its partition parents."""
    from process import provider_directory_cms_native_registration as registration
    from process.provider_directory_projection_stage import _copy_driver

    async with database.session_factory() as session, session.begin():
        await session.execute(text("SET LOCAL lock_timeout='5s'"))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        driver = await _copy_driver(session)
        captured = await registration.capture_current_native_input_candidates(
            driver, ledger_schema="mrf", source_policy=component.policy
        )
        assert captured["protected_fhir"] is not None
        from tests.cms_archive_writer_postgres_support import enroll_archive_writer

        candidates = []
        for candidate in captured["candidates"]:
            if candidate.target_table == "address_archive_v2":
                candidates.append(await enroll_archive_writer(driver, component.policy, candidate))
                continue
            candidates.append(candidate)
            relation = f'"{candidate.stage_schema}"."{candidate.stage_table}"'
            await driver.execute(f'REVOKE ALL ON TABLE {relation} FROM "{component.policy.owner_role}"')
            await driver.execute(f'GRANT SELECT,MAINTAIN ON TABLE {relation} TO "{component.policy.owner_role}"')
        actor_oid = await driver.fetchval("SELECT current_user::regrole::oid::bigint")
        await registration.register_native_input_candidates(
            driver, ledger_schema="mrf", candidates=tuple(candidates), expected_actor_oid=actor_oid
        )


async def _seed_native_doctors(database):
    """Publish a genuine incumbent NPI and Doctors family for the two office units."""
    async with database.session_factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO mrf.npi(npi,entity_type_code,provider_first_name,provider_last_name) "
                "VALUES(1234567893,1,'Example','Clinician')"
            )
        )
        await session.execute(
            text("""
            INSERT INTO mrf.doctor_clinician_address(
                npi,address_checksum,address_line1,address_line2,city,state,zip_code,provider_type,address_key,updated_at
            ) SELECT 1234567893,unit,'123 Example Street','Suite '||unit,'SampleCity','CA','90210','Internal Medicine',
                mrf.addr_key_from_identity_v1(mrf.addr_identity_key_v1(
                    '123 Example Street','Suite '||unit,'SampleCity','CA','90210','US')),now()
              FROM generate_series(2,3) unit
        """)
        )


def install_import_run_guards(connection):
    """Reuse the migration SQL and native model setup of the existing guard proof."""
    functions, triggers = run_guards._migration_statements("mrf")
    models_by_name = {table.name: table for table in Base.metadata.tables.values()}
    needed = set().union(*(set(re.findall(r'"mrf"\."([^\"]+)"', sql)) for sql in functions.values()))
    needed = (needed - set(functions)) & set(models_by_name)
    while True:
        expanded = needed | {key.column.table.name for name in needed for key in models_by_name[name].foreign_keys}
        if expanded == needed:
            break
        needed = expanded
    metadata = MetaData()
    for name in sorted(needed):
        models_by_name[name].to_metadata(metadata, schema="mrf", referred_schema_fn=lambda *_: "mrf")
    metadata.create_all(connection)
    for sql in run_guards._support_tables("mrf"):
        name = re.search(r'CREATE TABLE "mrf"\."([^\"]+)"', sql)[1]
        if not connection.scalar(text("SELECT to_regclass(:name)"), {"name": "mrf." + name}):
            connection.execute(text(sql))
    for sql in functions.values():
        connection.execute(text(sql.replace("CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)))
    for sql in triggers:
        name = re.search(r'CREATE TRIGGER "?([^"\s]+)"?', sql)[1]
        if not connection.scalar(
            text("SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='mrf.import_run'::regclass AND tgname=:name)"),
            {"name": name},
        ):
            connection.execute(text(sql))
        connection.execute(text(f'ALTER TABLE mrf.import_run ENABLE ALWAYS TRIGGER "{name}"'))


async def _seed_selection_row(database, model, fields):
    """Seed synthetic registry configuration through the supplied independent administrator."""
    assert database.engine is database._source_component.admin_engine
    statement = insert(model.__table__).values(**fields)
    keys = [column.name for column in model.__table__.primary_key]
    statement = statement.on_conflict_do_update(
        index_elements=keys, set_={name: statement.excluded[name] for name in fields if name not in keys}
    )
    async with database.session_factory() as session, session.begin():
        await session.execute(statement)


async def attested_execution(database, initial, monkeypatch, *, generation=1):
    """Register the actual sealed candidate's desired selection in PostgreSQL."""
    catalog_by_field = {
        "catalog_digest": "a" * 64,
        "items": [{"runnable": True, "profile_enabled": True, "source_ids": ["cms-npd"]}],
    }
    # Successors retain the fixed registry protected by the rooted dependency guard.
    registry_rows = _variant_registry_rows() if generation == 1 else ()
    for source_by_field in registry_rows:
        await _seed_selection_row(
            database,
            source.fhir.ProviderDirectoryAPIEndpoint,
            {
                "endpoint_id": source_by_field["endpoint_id"],
                "canonical_api_base": "https://example.test/" + source_by_field["endpoint_id"],
                "credential_descriptor_hash": source_by_field["endpoint_id"],
                "endpoint_signature_hash": source_by_field["endpoint_id"],
                "credential_descriptor_json": {},
                "endpoint_signature_json": {},
                "first_seen_at": source.fhir._now(),
                "last_seen_at": source.fhir._now(),
                "created_at": source.fhir._now(),
                "updated_at": source.fhir._now(),
            },
        )
        source_by_field.update(
            requires_registration=False,
            requires_api_key=False,
            auth_type="none",
            created_at=source.fhir._now(),
            updated_at=source.fhir._now(),
            metadata_json={
                source.fhir.PROVIDER_DIRECTORY_CONFIGURED_ENDPOINT_METADATA_KEY: source_by_field["endpoint_id"]
            },
        )
        await _seed_selection_row(database, source.fhir.ProviderDirectorySource, source_by_field)
    monkeypatch.setattr(source.fhir, "_provider_directory_profile_selection_catalog", lambda: catalog_by_field)
    async with database.session_factory() as session:
        incumbent = await serving_receipt.read_current_receipt(session, "mrf")
    pin_fields = ("source_id", "endpoint_id", "dataset_id", "dataset_hash", "acquisition_root_run_id")
    incumbent_pin = (
        None
        if incumbent is None
        else selection_snapshot._selection_pair(*(incumbent["payload"]["cms"][field] for field in pin_fields))
    )
    desired_pin = initial["cms_serving_candidate"]["desired_cms_dataset"]
    if incumbent_pin is not None and all(desired_pin[field] == incumbent_pin[field] for field in pin_fields):
        desired_pin = incumbent_pin
    request = await selection.current_profile_selection_request(
        catalog_by_field,
        desired_cms_dataset=desired_pin,
        expected_cms_incumbent=incumbent_pin,
        desired_profile_as_of=initial["registry_source_admission"]["semantic_projection_as_of"],
    )
    attestation = selection.validated_profile_selection_attestation(
        await selection.attest_profile_selection(request, catalog_by_field)
    )
    return selection.ProviderDirectoryProfileExecution(attestation, generation, capacity_attestation={})


async def committed_publication_proof(database, admitted):
    """Read the real committed pair; reuse only the closed proof DTO conversion."""
    async with database.session_factory() as session:
        receipt = await serving_receipt.read_current_receipt(session, "mrf")
        assert receipt is not None
        payload = receipt["payload"]
        row = (
            await session.execute(
                text(
                    "SELECT publication_xid::text,profile_generation_id "
                    "FROM mrf.provider_directory_cms_serving_receipt WHERE receipt_id=:receipt"
                ),
                {"receipt": receipt["receipt_id"]},
            )
        ).one()
        return _publication_proof(
            admitted["registry_source_admission"], payload["cms"], payload, receipt["receipt_id"], row
        ), payload


async def assert_native_offices(database, receipt_payload):
    """Check actual office conservation and all native receipt relation identities."""
    async with database.session_factory() as session:
        offices = (
            await session.execute(
                text(
                    "SELECT second_line,address_key,address_sources,source_record_ids,npi "
                    "FROM mrf.entity_address_unified ORDER BY second_line"
                )
            )
        ).all()
        assert len(offices) == 2
        assert {office[0] for office in offices} == {"Suite 2", "Suite 3"}
        assert len({office[1] for office in offices}) == 2
        assert all("provider_directory_fhir" in office[2] for office in offices)
        assert {(office[0], office[4], tuple(office[3])) for office in offices} == {
            (
                f"Suite {unit}",
                1234567893,
                (f"provider_directory_fhir:practitioner_role:cms-npd:role-{unit}:site-{unit}",),
            )
            for unit in (2, 3)
        }
        overlay = (
            await session.execute(
                text(
                    "SELECT second_line,address_key,source_id,resource_type,resource_id,npi "
                    "FROM mrf.provider_directory_address_overlay ORDER BY second_line"
                )
            )
        ).all()
        assert [(member[0], member[1]) for member in overlay] == [(office[0], office[1]) for office in offices]
        assert [(member[2], member[3], member[4], member[5]) for member in overlay] == [
            ("cms-npd", "PractitionerRole", f"role-{unit}", 1234567893) for unit in (2, 3)
        ]
        assert await session.scalar(text("SELECT count(*) FROM mrf.doctor_clinician_address")) == 2
        assert receipt_payload["profile"]["profile_rows"] > 0
        assert receipt_payload["profile"]["evidence_rows"] > 0
        for family, names in (
            ("address", addresses.RELATION_NAMES),
            ("doctors", reference.RELATION_NAMES_BY_IMPORTER["cms-doctors"]),
        ):
            for name, oid in zip(names, receipt_payload[family]["relation_oids"], strict=True):
                assert (
                    await session.scalar(text("SELECT to_regclass(:name)::oid::bigint"), {"name": "mrf." + name}) == oid
                )


def observe_geometry(monkeypatch, database, *, private_capture=None):
    """Emit neutral status; an explicit private sink may retain measured metadata."""

    def report(label, payload):
        if label == "NATIVE_WAL_WINDOW":
            counters_by_field = {
                name: payload.get(name)
                if type(payload) is dict and type(payload.get(name)) is int and 0 <= payload[name] < 2**64
                else None
                for name in ("actual_wal", "pending_control", "pending_relation")
            }
            print(label, json.dumps(counters_by_field, sort_keys=True, separators=(",", ":")))
        else:
            print(label)
        if private_capture is not None:
            try:
                private_capture(label, payload)
            except BaseException:  # Diagnostics must preserve the original result.
                print("NATIVE_PRIVATE_CAPTURE_UNAVAILABLE")

    _observe_admission_geometry(monkeypatch, report)
    _observe_mutation_window(monkeypatch, database, report)


def _observe_admission_geometry(monkeypatch, report):
    """Observe original geometry changes without changing the computed result."""
    original, observed, plans = source.fhir._profile_admission_geometry, [], []

    def observe(workload, inputs):
        result = original(workload, inputs)
        geometry_by_field = asdict(result.geometry)
        plan_by_field = asdict(workload.control_wal_plan_input)
        if observed:
            changed_by_field = {
                name: [observed[-1][name], value]
                for name, value in geometry_by_field.items()
                if value != observed[-1][name]
            }
            if changed_by_field:
                report("NATIVE_GEOMETRY_CHANGED", changed_by_field)
            changed_plan_by_field = {
                name: [plans[-1][name], value] for name, value in plan_by_field.items() if value != plans[-1][name]
            }
            if changed_plan_by_field:
                report("NATIVE_CONTROL_PLAN_CHANGED", changed_plan_by_field)
        observed.append(geometry_by_field)
        plans.append(plan_by_field)
        return result

    monkeypatch.setattr(source.fhir, "_profile_admission_geometry", observe)


def _observe_mutation_window(monkeypatch, database, report):
    """Capture only the first original settlement failure and preserve its identity."""
    original_settle = control_projection._settle_mutation_window
    captured_failures = []

    async def settle(fhir, admission, owner, relation_name, wal_before, data_before):
        try:
            return await original_settle(fhir, admission, owner, relation_name, wal_before, data_before)
        except RuntimeError as error:
            if captured_failures:
                raise
            captured_failures.append(True)
            wal_after = wal_support.failed_window_counter(error, admission, wal_before)
            try:
                owners = wal_support.failed_window_owners(fhir, admission, owner, relation_name)
                report("NATIVE_WAL_OWNERS", owners)
            except BaseException as diagnostic_error:  # Retain the original failure.
                report(
                    "NATIVE_WAL_OWNERS_UNAVAILABLE",
                    {"error_type": type(diagnostic_error).__name__},
                )
            report(
                "NATIVE_WAL_WINDOW",
                {
                    "relation_name": relation_name,
                    "actual_wal": wal_after - wal_before if type(wal_after) is int else None,
                    "pending_control": admission.wal_tracker.pending_control_wal_bytes.get(owner, 0),
                    "pending_relation": admission.wal_tracker.pending_relation_wal_bytes.get(relation_name, 0),
                    "pending_metadata": admission.wal_tracker.pending_metadata_wal_bytes,
                    "signed_reservation": dict(admission.geometry.reservation_bytes_by_storage_class),
                },
            )
            if type(wal_after) is int and wal_after > wal_before:
                try:
                    interval = wal_support.wal_interval(admission, wal_before, wal_after)
                    wal_totals_by_field = await wal_support.failed_window_records(database, interval)
                    report("NATIVE_WAL_RECORDS", wal_totals_by_field)
                except BaseException as diagnostic_error:  # Retain the original failure.
                    report(
                        "NATIVE_WAL_RECORDS_UNAVAILABLE",
                        {"error_type": type(diagnostic_error).__name__},
                    )
            raise

    monkeypatch.setattr(control_projection, "_settle_mutation_window", settle)


def verify_resume_geometry(database, monkeypatch):
    """Exercise unchanged resume guards before and inside its actual transaction."""
    from process import provider_directory_cms_preparation as preparation

    original = nonprofile.resume_profile_capacity
    verification_results = []

    async def verify(fhir, paused, execution, fence, resource_fence, types):
        if verification_results:
            return await original(fhir, paused, execution, fence, resource_fence, types)
        if not source_policy.is_source_storage_required():
            await _reject_changed_resume_inputs(
                database, original, fhir, paused, execution, fence, resource_fence, types
            )
        override_by_name = dict(fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get())
        resumed = await original(fhir, paused, execution, fence, resource_fence, types)
        assert resumed.geometry == paused.admission.geometry
        assert resumed.control_wal_projection == paused.admission.control_wal_projection
        assert fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get() == override_by_name
        verification_results.append(True)
        return resumed

    original_scope = preparation._profile_scope_from_inputs

    @asynccontextmanager
    async def source_scope(fhir, execution, fence, resource_fence, run_id, control_run_id, metrics, admission):
        if not verification_results:
            types = fhir._provider_directory_artifact_resource_types({"profile"}, publish_corroboration=False)
            await _reject_changed_resume_inputs(
                database, original, fhir, admission.profile_admission, execution, fence, resource_fence, types
            )
        async with original_scope(
            fhir, execution, fence, resource_fence, run_id, control_run_id, metrics, admission
        ) as prepared:
            yield prepared

    if source_policy.is_source_storage_required():
        monkeypatch.setattr(preparation, "_profile_scope_from_inputs", source_scope)
    monkeypatch.setattr(nonprofile, "resume_profile_capacity", verify)
    return verification_results


@asynccontextmanager
async def _resume_probe_transaction(fhir):
    """Open the original Source actor after mutation so each rejection can abort safely."""
    from process.provider_directory_profile_scope_copy import input_transaction

    if source_policy.is_source_storage_required():
        async with input_transaction(fhir):
            yield
    else:
        yield


async def _reject_changed_resume_inputs(database, original, fhir, paused, execution, fence, resource_fence, types):
    async with _resume_probe_transaction(fhir):
        await _verify_resume_projection(
            fhir, paused.admission, paused.admission.admitted_identity, fence, resource_fence, types
        )
    arguments = (fhir, paused, execution, fence, resource_fence, types)
    await _reject_resume_source_change(database, original, arguments)
    await _reject_resume_target_change(database, original, arguments)


async def _reject_resume_source_change(database, original, arguments):
    async with database.transaction():
        old_name = await database.scalar("SELECT org_name FROM mrf.provider_directory_source WHERE source_id='cms-npd'")
        await database.status(
            "UPDATE mrf.provider_directory_source SET org_name=org_name||' changed original' WHERE source_id='cms-npd'"
        )
    try:
        async with _resume_probe_transaction(arguments[0]):
            with pytest.raises(RuntimeError, match="provider_directory_nonprofile_capacity_profile_geometry_changed"):
                await original(*arguments)
    finally:
        async with database.transaction():
            await database.status(
                "UPDATE mrf.provider_directory_source SET org_name=:name WHERE source_id='cms-npd'", name=old_name
            )


async def _reject_resume_target_change(database, original, arguments):
    async with database.transaction():
        original_oid = await database.scalar("SELECT 'mrf.provider_directory_profile'::regclass::oid::bigint")
        await database.status("ALTER TABLE mrf.provider_directory_profile RENAME TO changed_profile_target")
        await database.status(profile.profile_table_sql("mrf", logged=True))
        replacement_oid = await database.scalar("SELECT 'mrf.provider_directory_profile'::regclass::oid::bigint")
    try:
        async with _resume_probe_transaction(arguments[0]):
            with pytest.raises(RuntimeError, match="provider_directory_profile_capacity_target_identity_changed"):
                await original(*arguments)
    finally:
        async with database.transaction():
            assert (
                await database.scalar("SELECT 'mrf.provider_directory_profile'::regclass::oid::bigint")
                == replacement_oid
            )
            assert await database.scalar("SELECT 'mrf.changed_profile_target'::regclass::oid::bigint") == original_oid
            await database.status("DROP TABLE mrf.provider_directory_profile")
            await database.status("ALTER TABLE mrf.changed_profile_target RENAME TO provider_directory_profile")


async def _verify_resume_projection(fhir, admission, identity, fence, resource_fence, types):
    """Compare actual selected SOURCE inputs and retain the legacy smaller-scope check."""
    scoped = await fhir._profile_admission_workload(identity, fence, resource_fence, types)
    with nonprofile.original_profile_input_scope(fhir):
        original = await fhir._profile_admission_workload(identity, fence, resource_fence, types)
    assert original.artifact_projection.projected_rows > 0
    assert original.artifact_projection.projection_hash == admission.geometry.artifact_scope_projection_hash
    if source_policy.is_source_storage_required():
        assert scoped.artifact_projection == original.artifact_projection
    else:
        assert scoped.artifact_projection.projected_logical_bytes < original.artifact_projection.projected_logical_bytes


def profile_preflight_inputs(execution, limits, storage, timing):
    """Assemble the normal closed successor guard around actual execution inputs."""
    from tests import provider_directory_profile_initial_test_support as initial

    payload = signing._execution_by_field(execution)
    if execution.generation == 1:
        return initial._signed_preflight_inputs(execution, payload, limits, storage, timing)
    control_request = signing._control_plane_request(payload, limits, storage, timing)
    request = signing._execution_request(payload, limits, timing)
    identity = signing.preflight.profile_execution_identity_payload(request)
    followup = signing._held_followup(execution, timing)
    control = signing._control_plane_receipt(control_request, request, identity, storage, followup, timing)
    healthcare_request = signing._healthcare_request(payload, limits, control, timing)
    validated = signing.preflight.validated_capacity_preflight_request(healthcare_request)
    return control_request, control, healthcare_request, validated, identity, followup


@asynccontextmanager
async def retained_pair(database, proof, *, expected_address_rows=2):
    """Capture real publication authority and clean only this fixture's UUID custody."""
    capture_id, roles = uuid4(), tuple("pair_" + uuid4().hex for _ in range(2))
    created_roles = []
    try:
        for role in roles:
            async with database.engine.begin() as connection:
                await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
            created_roles.append(role)
        pair = await capture_registry_cms_source_pair(
            database.session_factory,
            proof,
            capture_id=capture_id,
            owner_role=roles[0],
            runtime_roles=(roles[1],),
            source_copy=_bounded_source_copy(),
        )
        assert len(pair.address_receipt.tables) == 7
        assert pair.address_receipt.tables[0].row_count == expected_address_rows
        async with database.session_factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            assert await require_registry_cms_source_pair(session, pair) == pair
        yield pair
    finally:
        schemas = ("registry_cms_epoch_" + capture_id.hex, "entity_address_archive_" + capture_id.hex)
        async with database.engine.begin() as connection:
            for schema in schemas:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            for role in created_roles:
                await connection.execute(text(f'DROP OWNED BY "{role}" CASCADE'))
                await connection.execute(text(f'DROP ROLE "{role}"'))
            assert (
                await connection.scalar(
                    text("SELECT count(*) FROM pg_namespace WHERE nspname=ANY(:names)"), {"names": list(schemas)}
                )
                == 0
            )
            assert (
                await connection.scalar(
                    text("SELECT count(*) FROM pg_roles WHERE rolname=ANY(:names)"), {"names": created_roles}
                )
                == 0
            )


def sign_native_receipt(guard, receipt, timing, *, reservation):
    """Enroll actual database, geometry and runtime values with the synthetic key."""
    binding = receipt.get("database_binding", receipt["capacity_geometry"])
    required = receipt["required_reservation_bytes_by_storage_class"]
    storage_by_class = {
        volume["volume_class"]: volume for volume in guard["control_plane_request"]["storage_observation"]["volumes"]
    }

    def mutate(body):
        body.update(
            **{field: binding[field] for field in ("database_system_identifier", "database_oid", "database_name")},
            capacity_geometry_hash=receipt["capacity_geometry_hash"],
            nonce=receipt["receipt_sha256"],
            observed_at=signing._utc(timing.observed_at),
            issued_at=signing._utc(timing.issued_at),
            expires_at=signing._utc(timing.expires_at),
            max_build_deadline=signing._utc(timing.max_build_deadline),
            reservation_id=reservation,
            signing_preflight_guard=deepcopy(guard),
            signing_preflight_guard_sha256=signing.preflight.preflight_domain_sha256(
                signing.guard_contract.CAPACITY_SIGNING_PREFLIGHT_GUARD_DIGEST_DOMAIN, guard
            ),
        )
        body["runtime_witness"].update(
            **{
                field: receipt["runtime_observation"][field]
                for field in runtime.CAPACITY_LEASE_LOCALLY_VERIFIED_RUNTIME_FIELDS
            }
        )
        body["runtime_witness_sha256"] = leases.capacity_runtime_witness_sha256(
            body["runtime_witness"], body["deployment_witness"]
        )
        for tablespace in body["tablespaces"]:
            tablespace.update(tablespace_oid=binding["tablespace_oid"], tablespace_name=binding["tablespace_name"])
        for volume in body["volumes"]:
            volume.update(storage_by_class[volume["volume_class"]])
            volume["reserved_bytes"] = required[volume["volume_class"]]

    return _signed_envelope(body_mutator=mutate)


def storage_authority(envelope):
    """Return fresh test authority signatures over each actual engine phase nonce."""

    async def continuation(request):
        now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        storage = deepcopy(envelope["lease"]["signing_preflight_guard"]["control_plane_request"]["storage_observation"])
        storage.update(observed_at=signing._utc(now), issued_at=signing._utc(now))
        return sign_storage(
            {
                **request.binding_by_field,
                "issued_at": signing._utc(now),
                "expires_at": signing._utc(now + datetime.timedelta(seconds=30)),
                "storage_observation": storage,
            }
        )

    return continuation
