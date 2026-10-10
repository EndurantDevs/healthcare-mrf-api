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
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text

from db.connection import Base
from db.tiger_models import Zip_zcta5, ZipState
from process import cms_doctors_preparation as doctors
from process import entity_address_result_generation as addresses
from process import npi_result_generation as npi
from process import provider_directory_cms_native_inputs as inputs
from process import provider_directory_cms_nonprofile_capacity as nonprofile
from process import provider_directory_cms_serving_receipt as serving_receipt
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
from tests.test_network_cms_registry_source_pair_postgres import _publication_proof
from tests.test_provider_directory_cms_storage_continuation import _envelope as sign_storage
from tests.test_provider_directory_profile_capacity_attestation import _signed_envelope
from tests.test_provider_directory_profile_selection_attestation import _variant_registry_rows


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


async def _create_native_relations(database):
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
        for name, sql in (
            ("provider_directory_profile", profile.profile_table_sql("mrf", logged=True)),
            ("provider_directory_profile_evidence", profile.profile_evidence_table_sql("mrf", logged=True)),
            ("provider_directory_address_overlay", source.fhir.provider_directory_address_overlay_table_sql("mrf")),
        ):
            if not await connection.scalar(text("SELECT to_regclass(:name)"), {"name": "mrf." + name}):
                await connection.execute(text(sql))
        await connection.execute(text("CREATE TABLE mrf.alembic_version(version_num varchar(128) NOT NULL)"))
        await connection.execute(
            text("INSERT INTO mrf.alembic_version VALUES('20261001110000_profile_initial_publication')")
        )
        await connection.run_sync(install_import_run_guards)


async def install_native_inputs(database, monkeypatch, tmp_path, source_commit):
    """Create actual empty native families and publish their native generations."""
    monkeypatch.setattr(address_canon, "db", database)
    for prefix in (
        "20260615170000",
        "20260616090000",
        "20260808090000",
        "20260808100000",
        "20260808170000",
        "20260811110000_address_formatted_display",
        "20260815010000",
        "20260816020000",
        "20260808220000",
        "20260808230000",
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
    ):
        async with database.engine.begin() as connection:
            await connection.run_sync(source._run_migrations, (prefix,))
    await _create_native_relations(database)
    await _seed_native_doctors(database)
    async with database.session_factory() as session, session.begin():
        await npi.install_npi_result_revision_guards(session, schema_name="mrf", stage_tables=npi.RELATION_NAMES)
        await npi.bootstrap_npi_result_generation(session, schema_name="mrf")
        for family in ("cms-doctors", "facility-anchors", "geo", "mrf-address", "tiger"):
            await reference.publish_local_reference_family_generation(
                session, importer_id=family, schema_name="tiger" if family == "tiger" else "mrf"
            )
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


async def attested_execution(database, initial, monkeypatch, *, generation=1):
    """Register the actual sealed candidate's desired selection in PostgreSQL."""
    catalog_by_field = {
        "catalog_digest": "a" * 64,
        "items": [{"runnable": True, "profile_enabled": True, "source_ids": ["cms-npd"]}],
    }
    for source_by_field in _variant_registry_rows():
        await source.fhir._upsert_rows(
            source.fhir.ProviderDirectoryAPIEndpoint,
            [
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
                }
            ],
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
        await source.fhir._upsert_rows(source.fhir.ProviderDirectorySource, [source_by_field])
    monkeypatch.setattr(source.fhir, "_provider_directory_profile_selection_catalog", lambda: catalog_by_field)
    async with database.session_factory() as session:
        incumbent = await serving_receipt.read_current_receipt(session, "mrf")
    incumbent_pin = (
        None
        if incumbent is None
        else {
            field: incumbent["payload"]["cms"][field]
            for field in ("source_id", "endpoint_id", "dataset_id", "dataset_hash", "acquisition_root_run_id")
        }
    )
    request = await selection.current_profile_selection_request(
        catalog_by_field,
        desired_cms_dataset=initial["cms_serving_candidate"]["desired_cms_dataset"],
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
    """Exercise exact original workload checks inside the actual shorter CMS scope."""
    original = nonprofile.resume_profile_capacity
    verification_results = []

    async def verify(fhir, paused, execution, fence, resource_fence, types):
        if verification_results:
            return await original(fhir, paused, execution, fence, resource_fence, types)
        identity = paused.admission.admitted_identity
        scoped = await fhir._profile_admission_workload(identity, fence, resource_fence, types)
        assert (
            scoped.artifact_projection.projected_logical_bytes
            < paused.admission.geometry.artifact_scope_projected_logical_bytes
        )
        override_by_name = dict(fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get())
        with pytest.raises(RuntimeError, match="provider_directory_nonprofile_capacity_profile_geometry_changed"):
            async with database.transaction():
                await database.status(
                    "UPDATE mrf.provider_directory_source SET org_name=org_name||' changed original' "
                    "WHERE source_id='cms-npd'"
                )
                await original(fhir, paused, execution, fence, resource_fence, types)
        with pytest.raises(RuntimeError, match="provider_directory_profile_capacity_target_identity_changed"):
            async with database.transaction():
                await database.status("ALTER TABLE mrf.provider_directory_profile RENAME TO changed_profile_target")
                await database.status(profile.profile_table_sql("mrf", logged=True))
                await original(fhir, paused, execution, fence, resource_fence, types)
        resumed = await original(fhir, paused, execution, fence, resource_fence, types)
        assert resumed.geometry == paused.admission.geometry
        assert resumed.control_wal_projection == paused.admission.control_wal_projection
        assert fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get() == override_by_name
        verification_results.append(True)
        return resumed

    monkeypatch.setattr(nonprofile, "resume_profile_capacity", verify)
    return verification_results


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
async def retained_pair(database, proof):
    """Capture real publication authority and clean only this fixture's UUID custody."""
    capture_id, roles = uuid4(), tuple("pair_" + uuid4().hex for _ in range(2))
    created_roles = []
    try:
        for role in roles:
            async with database.engine.begin() as connection:
                await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
            created_roles.append(role)
        pair = await capture_registry_cms_source_pair(
            database.session_factory, proof, capture_id=capture_id, owner_role=roles[0], runtime_roles=(roles[1],)
        )
        assert len(pair.address_receipt.tables) == 7
        assert pair.address_receipt.tables[0].row_count == 2
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
