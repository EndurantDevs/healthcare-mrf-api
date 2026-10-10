# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native prepared retention and atomic receipt binding with component controls.

Capacity signatures are genuine synthetic fixture signatures; phase checks use
actual PostgreSQL relation bytes and WAL. This is component evidence, not the
complete signed producer/storage-continuation pipeline.
"""

import asyncio
import contextvars
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.schema import CreateIndex, CreateTable

from process import network_cms_registry_address_capture as address_capture
from process import network_cms_registry_address_equivalence as address_equivalence
from process import network_fhir_source_epoch as epoch_capture
from process import network_registry_cms_prepared_address as prepared_address
from process import network_registry_cms_prepared_pair as retention
from process import provider_directory_cms_native_layout as native_layout
from process import provider_directory_cms_npd as cms
from process import provider_directory_cms_serving_receipt as serving
from process.entity_address_candidate_preparation import PreparedEntityAddressGeneration
from process.entity_address_result_generation import ENTITY_ADDRESS_RESULT_MODELS, RELATION_NAMES
from process.entity_address_snapshot_source import entity_address_unified
from process.network_cms_registry_source_pair import require_registry_cms_source_pair
from process.network_registry_cms_prepared_pair import RegistryCMSRetentionRequest
from process.provider_directory_cms_address import CMSAddressPreparation
from process.provider_directory_cms_preparation import (
    NonprofileAdmission,
    NonprofileAdmissionPlan,
    NonprofileAdmissionReceipt,
    PreparedServingArtifacts,
)
from tests import cms_npd_admission_postgres_support as support
from tests import test_provider_directory_cms_serving_receipt_postgres as native
from tests.provider_directory_cms_capacity_test_support import cms_execution, paired_profile_envelope, signed_cms_plan
from tests.test_network_cms_registry_source_pair_postgres import _publish_pair
from tests.test_network_custom_address_source_postgres import BootstrapCopyConnection
from tests.test_provider_directory_cms_preparation import VALIDATION_TIME
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template
from tests.test_provider_directory_profile_capacity_attestation import _trust, _verify


class NativePreparationDB:
    """Use real caller-owned SQL transactions for native admission measurements."""

    def __init__(self, sessions):
        self.sessions = sessions
        self.active = contextvars.ContextVar("retention_native_session", default=None)

    @asynccontextmanager
    async def transaction(self):
        async with self.sessions() as session, session.begin():
            token = self.active.set(session)
            try:
                yield session
            finally:
                self.active.reset(token)

    async def first(self, query, **parameters):
        if self.active.get() is not None:
            return (await self.active.get().execute(text(query), parameters)).mappings().one_or_none()
        async with self.sessions() as session:
            return (await session.execute(text(query), parameters)).mappings().one_or_none()

    async def scalar(self, query, **parameters):
        if self.active.get() is not None:
            return await self.active.get().scalar(text(query), parameters)
        async with self.sessions() as session:
            return await session.scalar(text(query), parameters)

    async def all(self, query, **parameters):
        if self.active.get() is not None:
            return (await self.active.get().execute(text(query), parameters)).mappings().all()
        async with self.sessions() as session:
            return (await session.execute(text(query), parameters)).mappings().all()


async def _staged_address(database, monkeypatch):
    """Create finalized native models, declarations and owned sequences with exact stage OIDs."""
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE", "all")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES", "1")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_ADDITIONAL_INDEXES", "0")
    stages = []
    suffix = "_cms" + uuid4().hex[:20]
    async with database.engine.begin() as connection:
        live_sequence = await connection.scalar(
            text("SELECT pg_get_serial_sequence('mrf.entity_address_evidence','evidence_id')")
        )
        live_sequence_values = tuple(
            (await connection.execute(text(f"SELECT last_value,is_called FROM {live_sequence}"))).one()
        )
        for extension in ("postgis", "intarray", "btree_gin"):
            await connection.execute(text(f"CREATE EXTENSION IF NOT EXISTS {extension}"))
        for model in ENTITY_ADDRESS_RESULT_MODELS:
            logical, stage = model.__tablename__, model.__tablename__ + suffix
            table = model.__table__.to_metadata(MetaData(), schema="mrf", name=stage)
            statement = str(CreateTable(table).compile(dialect=connection.dialect))
            statement = statement.replace("(\n", "(\n discarded_column integer,\n", 1)
            await connection.execute(text(statement))
            await connection.execute(text(f'ALTER TABLE mrf."{stage}" DROP COLUMN discarded_column'))
            for index in table.indexes:
                await connection.execute(text(str(CreateIndex(index).compile(dialect=connection.dialect))))
            stage_model = SimpleNamespace(__tablename__=stage, __main_table__=logical)
            for _label, statement in entity_address_unified._stage_index_statements(
                stage_model, "mrf", model.__my_additional_indexes__, {}
            ):
                await connection.execute(text(statement))
            if logical == "entity_address_unified":
                assert (
                    await connection.scalar(
                        text(entity_address_unified._required_geo_taxonomy_stage_index_sql("mrf", stage))
                    )
                    is True
                )
            await connection.execute(text(entity_address_unified._disable_autovacuum_sql("mrf", stage)))
            columns = ",".join(column.name for column in model.__table__.columns)
            await connection.execute(text(f"INSERT INTO mrf.{stage} ({columns}) SELECT {columns} FROM mrf.{logical}"))
            oid = await connection.scalar(
                text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": "mrf." + stage}
            )
            stages.append((logical, stage, oid))
        await _seed_support_stages(connection, {logical: stage for logical, stage, _oid in stages})
        assert (
            tuple((await connection.execute(text(f"SELECT last_value,is_called FROM {live_sequence}"))).one())
            == live_sequence_values
        )
    return PreparedEntityAddressGeneration(
        "mrf", [], [], [], [], tuple(stages), {"publication_state": "prepared"}, {}, None, None
    )


async def _seed_support_stages(connection, stages_by_logical):
    """Exercise all seven COPY imports, including equal compound-key prefixes."""
    values_by_table = {
        "entity_address_evidence": (
            "evidence_id,location_key,entity_type,entity_id,source_id,source_run_id",
            "100+unit,location_key,'npi','1000000004',1,'synthetic-run'",
        ),
        "entity_address_plan_bridge": (
            "location_key,entity_type,entity_id,plan_id,market_type",
            "location_key,'npi','1000000004','synthetic-plan-'||unit,NULL",
        ),
        "entity_address_network_bridge": (
            "location_key,entity_type,entity_id,network_id",
            "location_key,'npi','1000000004','synthetic-network-'||unit",
        ),
        "entity_address_procedure_bridge": (
            "location_key,npi,code_system,code",
            "location_key,1000000004,'synthetic','procedure-'||unit",
        ),
        "entity_address_medication_bridge": (
            "location_key,npi,code_system,code",
            "location_key,1000000004,'synthetic','medication-'||unit",
        ),
        "facility_anchor_npi_candidate": (
            "candidate_id,location_key,facility_anchor_id,source_run_id",
            "'synthetic-candidate-'||unit,location_key,'synthetic-anchor','synthetic-run'",
        ),
    }
    for logical, (columns, value_sql) in values_by_table.items():
        await connection.execute(
            text(f"""INSERT INTO mrf.{stages_by_logical[logical]} ({columns}) SELECT {value_sql}
            FROM (SELECT min(location_key) AS location_key FROM mrf.{stages_by_logical["entity_address_unified"]}) source
            CROSS JOIN generate_series(1,2) unit""")
        )


async def _prepared_input(database, proof, capture_id, roles, monkeypatch, batch_rows=32):
    """Bind a genuine synthetic signature to the explicit retained-copy policy."""
    address = await _staged_address(database, monkeypatch)

    async def signed_fixture_clock():
        return VALIDATION_TIME

    fhir = support.fhir
    monkeypatch.setattr(fhir, "db", NativePreparationDB(database.session_factory))
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_clock", signed_fixture_clock)
    execution = cms_execution(day=proof.source_pin.as_of)
    assert execution.attestation.proof_id == proof.selection_proof_id
    database_identity = await _native_database_identity(fhir)
    profile_lease = _verify(
        paired_profile_envelope(execution, database_identity=database_identity),
        trust=replace(_trust(), **database_identity),
        **{"expected_" + field: field_value for field, field_value in database_identity.items()},
    )
    request = RegistryCMSRetentionRequest(
        proof.source_pin,
        proof.binding_coordinates,
        proof.selection_proof_id,
        proof.expected_admission_sha256,
        proof.expected_metadata_sha256,
        32 * 1024**2,
        32 * 1024**2,
    )
    factory = CMSAddressPreparation(
        fhir,
        execution,
        "retention-component",
        json.dumps({"registry_source_retention": request.policy(capture_id, roles[0], (roles[1],))}),
    )
    plan = NonprofileAdmissionPlan(
        proof.selection_proof_id,
        proof.source_pin.as_of,
        "a" * 64,
        "b" * 64,
        ("address_overlay",),
        ("Practitioner",),
        batch_rows,
        1,
        tuple((kind, 128 * 1024**2) for kind in ("data", "temp", "wal")),
        1000,
        60,
        tuple(sorted(RELATION_NAMES)),
        factory.input_hash,
        4096,
        64 * 1024**2,
        1024**2,
        profile_lease.lease_digest,
    )
    admission = await _native_admission(fhir, plan, request.policy(capture_id, roles[0], (roles[1],)), execution)
    prepared = PreparedServingArtifacts(fhir, None, execution, admission, None, None, {}, {}, None, address)
    await _admit_address_stages(prepared)
    return prepared, factory, request


async def _admit_address_stages(prepared):
    """Include the actual finalized candidate heaps in native aggregate accounting."""
    for _logical, stage, oid in prepared.address.stage_oids:
        await prepared.nonprofile_admission.register_external_relation(prepared.fhir, "mrf", stage, oid)


async def _native_admission(fhir, plan, policy, execution):
    """Observe exact native aggregate heap bytes and WAL under signed fixture policy."""
    start = await fhir.db.scalar("SELECT pg_current_wal_insert_lsn()::text")

    async def check_phase(check):
        native_by_coordinate = {(entry.schema, entry.relation): entry for entry in check.native_relations}
        for relation in check.relations:
            assert (
                await fhir.db.scalar("SELECT pg_total_relation_size(CAST(:oid AS oid))", oid=relation.oid)
                == relation.total_bytes
            )
            annotation = native_by_coordinate.get((relation.schema, relation.relation))
            if annotation is not None:
                captured = await native_layout.capture_retained_native_layout(
                    fhir, relation, annotation.source_layout, check.plan.native_address_targets
                )
                assert captured.relation_oid == relation.oid
        assert (
            await fhir.db.scalar(
                "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),CAST(CAST(:start AS text) AS pg_lsn))::bigint",
                start=start,
            )
            < 128 * 1024**2
        )
        return NonprofileAdmissionReceipt(
            check.phase,
            check.lease.lease_digest,
            check.lease.reservation_id,
            check.plan.capacity_geometry_hash,
            check.relations,
            check.logging_relations,
        )

    admission = NonprofileAdmission(
        signed_cms_plan(
            plan, retention_policy=policy, execution=execution, database_identity=await _native_database_identity(fhir)
        ),
        plan,
        check_phase,
    )
    admission._started = True
    return admission


async def _native_database_identity(fhir):
    """Sign the genuine fixture lease for its exact current ephemeral database."""
    return dict(
        await fhir.db.first(
            """SELECT database_row.oid::bigint AS database_oid,database_row.datname AS database_name,
        control.system_identifier::text AS database_system_identifier
        FROM pg_database database_row CROSS JOIN pg_control_system() control
        WHERE database_row.datname=current_database()"""
        )
    )


async def _publish_stages(session, prepared_pair, predecessor):
    """Apply an actual native OID swap and append its guarded common receipt."""
    for logical, stage, _oid in prepared_pair.address_stages:
        await session.execute(text(f"ALTER TABLE mrf.{logical} RENAME TO retained_old_{logical}"))
        await session.execute(text(f"ALTER TABLE mrf.{stage} RENAME TO {logical}"))
    await native._advance_native(session, "mrf")
    await session.execute(
        text(
            "UPDATE mrf.provider_directory_profile_serving_generation SET generation_id=:generation,control_generation=control_generation+1"
        ),
        {"generation": "pdprofile_" + uuid4().hex},
    )
    payload = await native._payload(session, "mrf", predecessor)
    payload["selection"]["proof_id"] = predecessor["payload"]["selection"]["proof_id"]
    payload["cms"]["release_id"] = predecessor["payload"]["cms"]["release_id"]
    receipt_id = await serving.append_serving_receipt(session, "mrf", payload)
    return receipt_id, payload


async def _reject_preparation(database, prepared, factory, request, roles, capture_id, monkeypatch):
    """Missing signed policy and cancellation leave no retained copy namespaces."""
    unsigned_id = uuid4()
    options_by_field = dict(capture_id=unsigned_id, owner_role=roles[0], runtime_roles=(roles[1],))
    with pytest.raises(ValueError, match="retention_admission_required"):
        await retention.prepare_registry_cms_source_pair(prepared, factory, request, **options_by_field)
    with monkeypatch.context() as patch:

        async def cancelled(*_args, **_kwargs):
            raise asyncio.CancelledError

        patch.setattr(retention, "prepare_retained_registry_address", cancelled)
        with pytest.raises(asyncio.CancelledError):
            await retention.prepare_registry_cms_source_pair(
                prepared,
                factory,
                request,
                capture_id=capture_id,
                owner_role=roles[0],
                runtime_roles=(roles[1],),
            )
    async with database.session_factory() as session:
        for identity in (unsigned_id, capture_id):
            for prefix in ("registry_cms_epoch_", "entity_address_archive_"):
                assert (
                    await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": prefix + identity.hex})
                    is None
                )


@pytest.mark.asyncio
@pytest.mark.parametrize("copy_bounds", [(1, 64 * 1024**2), (32, 64 * 1024)])
async def test_prepared_retention_native_binding(monkeypatch, tmp_path, copy_bounds):
    """Close before publication, roll binding back, then retain through a successor."""
    directory, acquired = support.retained_release(tmp_path)
    monkeypatch.setattr(
        retention.RegistryCMSRawEpochAdmission, "copy_batch_bytes", property(lambda _self: copy_bounds[1])
    )
    roles = tuple("retention_" + uuid4().hex for _ in range(2))
    capture_id = uuid4()
    async with support.admission_database(monkeypatch) as database:
        with support.release_probe_client(directory) as client:
            initial = await cms._run_acquired({"context": {}}, {}, "retention-component", directory, acquired, client)
        execution = cms_execution(day=initial["registry_source_admission"]["semantic_projection_as_of"])
        proof = await _publish_pair(database, initial, monkeypatch, selection_proof_id=execution.attestation.proof_id)
        try:
            async with database.engine.begin() as connection:
                for role in roles:
                    await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
            prepared, factory, request = await _prepared_input(
                database, proof, capture_id, roles, monkeypatch, copy_bounds[0]
            )
            await _reject_preparation(database, prepared, factory, request, roles, capture_id, monkeypatch)
            await _reject_copy_budget(prepared, factory, request, capture_id, roles, monkeypatch)
            await _reject_raw_oid(database, prepared, factory, request, roles, capture_id, monkeypatch)
            retained = await _prepare_observed(prepared, factory, request, roles, capture_id, monkeypatch)
            await _assert_retained_raw_parity(database, retained)
            await _assert_retained_address_parity(database, retained, prepared)
            async with database.session_factory() as session:
                predecessor = await serving.read_current_receipt(session, "mrf")
            await _bind_rollback(database, retained, predecessor)
            async with database.session_factory() as session, session.begin():
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                with pytest.raises(ValueError, match="prepared_receipt_invalid"):
                    await retention.bind_prepared_registry_cms_source_pair(
                        session, retained, receipt_id=predecessor["receipt_id"], receipt_payload=predecessor["payload"]
                    )
                receipt_id, publication_document = await _publish_stages(session, retained, predecessor)
                await _reject_open_custody(session, retained, roles, receipt_id, publication_document)
                with monkeypatch.context() as patch:

                    async def forbidden_scan(*_args, **_kwargs):
                        raise AssertionError("full content scan in atomic binder")

                    patch.setattr(epoch_capture, "_content", forbidden_scan)
                    patch.setattr(address_capture, "capture_entity_address_archive_receipt", forbidden_scan)
                    pair = await retention.bind_prepared_registry_cms_source_pair(
                        session, retained, receipt_id=receipt_id, receipt_payload=publication_document
                    )
            async with database.session_factory() as session, session.begin():
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                assert await require_registry_cms_source_pair(session, pair) == pair
            await _successor_retention(database, pair)
        finally:
            async with database.engine.begin() as connection:
                for role in roles:
                    await connection.execute(text(f'DROP OWNED BY "{role}" CASCADE'))
                    await connection.execute(text(f'DROP ROLE "{role}"'))
                    assert not await connection.scalar(
                        text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
                    )


async def _reject_copy_budget(prepared, factory, request, capture_id, roles, monkeypatch):
    """A genuinely signed but insufficient extra copy bound rolls every copy back."""
    bounded_request = replace(request, extra_data_upper_bound_bytes=1)
    bounded_factory = replace(
        factory,
        input_json=json.dumps({"registry_source_retention": bounded_request.policy(capture_id, roles[0], (roles[1],))}),
    )
    bounded_plan = replace(prepared.nonprofile_admission.plan, native_address_input_hash=bounded_factory.input_hash)
    bounded_prepared = replace(
        prepared,
        nonprofile_admission=await _native_admission(
            prepared.fhir, bounded_plan, bounded_request.policy(capture_id, roles[0], (roles[1],)), prepared.execution
        ),
    )
    await _admit_address_stages(bounded_prepared)
    budget_events = []
    original = retention.RegistryCMSRawEpochAdmission.__call__

    async def observe_denial(adapter, phase, schema, logical, oid):
        budget_events.append((phase, await adapter.connection.fetchval(f'SELECT count(*) FROM "{schema}"."{logical}"')))
        await original(adapter, phase, schema, logical, oid)

    with monkeypatch.context() as patch:
        patch.setattr(retention.RegistryCMSRawEpochAdmission, "__call__", observe_denial)
        with pytest.raises(epoch_capture.FHIRSourceEpochError, match="copy_admission_refused"):
            await retention.prepare_registry_cms_source_pair(
                bounded_prepared,
                bounded_factory,
                bounded_request,
                capture_id=capture_id,
                owner_role=roles[0],
                runtime_roles=(roles[1],),
            )
    assert budget_events == [("created", 0)]
    assert (
        bounded_prepared.nonprofile_admission._external_relations == prepared.nonprofile_admission._external_relations
    )
    for prefix in ("registry_cms_epoch_", "entity_address_archive_"):
        assert await prepared.fhir.db.scalar("SELECT to_regnamespace(:schema)", schema=prefix + capture_id.hex) is None


async def _reject_raw_oid(database, prepared, factory, request, roles, capture_id, monkeypatch):
    """A substituted original OID fails at registration with exact rollback retirement."""
    before_oid_by_relation = dict(prepared.nonprofile_admission._relations)
    original = retention.RegistryCMSRawEpochAdmission.__call__

    async def substitute(adapter, phase, schema, logical, oid):
        await original(adapter, phase, schema, logical, oid + 1 if phase == "created" else oid)

    with monkeypatch.context() as patch:
        patch.setattr(retention.RegistryCMSRawEpochAdmission, "__call__", substitute)
        with pytest.raises(epoch_capture.FHIRSourceEpochError, match="copy_admission_refused"):
            await retention.prepare_registry_cms_source_pair(
                prepared, factory, request, capture_id=capture_id, owner_role=roles[0], runtime_roles=(roles[1],)
            )
    assert prepared.nonprofile_admission._relations == before_oid_by_relation
    async with database.session_factory() as session:
        assert (
            await session.scalar(
                text("SELECT to_regnamespace(:schema)"), {"schema": "registry_cms_epoch_" + capture_id.hex}
            )
            is None
        )


def _address_copy_observer(prepared, created_relations, address_inputs, address_phases):
    """Retain real COPY while checking complete native enrollment at every import."""
    original_address_copy = prepared_address._copy_native_batch

    async def observe_address_copy(connection, query, arguments, schema, table, expected_count, **options):
        """Keep real admission checks and observe each seven-family import boundary."""
        assert {logical for created_schema, logical, _oid in created_relations if created_schema == schema} == set(
            RELATION_NAMES
        )
        assert {
            logical
            for native_schema, logical in prepared.nonprofile_admission._native_relations
            if native_schema == schema
        } == set(RELATION_NAMES)
        admit = options["import_admission"]

        async def observe_admission(phase):
            await admit(phase)
            count = await connection.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"')
            address_phases.append((table, phase, count))

        options["import_admission"] = observe_admission
        proxy = BootstrapCopyConnection(connection)
        await original_address_copy(proxy, query, arguments, schema, table, expected_count, **options)
        assert len(proxy.imports) == 1 and "OFFSET" not in query and "ctid" not in query
        assert 0 < expected_count <= min(prepared.nonprofile_admission.plan.batch_size, 4096)
        assert all(0 < byte_count <= options["byte_limit"] <= 64 * 1024**2 for byte_count in proxy.import_bytes)
        address_inputs.append((table, expected_count, proxy.import_bytes[0]))

    return observe_address_copy


def _retained_creation_observer(prepared, capture_id, created_relations):
    """Keep creator annotations intact while observing original empty retained heaps."""
    original_create = NonprofileAdmission.register_external_relation

    async def observe_creation(admission, fhir, schema, logical, oid, *, raw_relation=None, native_relation=None):
        if schema in {"registry_cms_epoch_" + capture_id.hex, "entity_address_archive_" + capture_id.hex}:
            assert await fhir.db.scalar(f'SELECT count(*) FROM "{schema}"."{logical}"') == 0
            created_relations.append((schema, logical, oid))
        if schema == "entity_address_archive_" + capture_id.hex:
            assert raw_relation is None and native_relation is not None
            assert (native_relation.schema, native_relation.relation, native_relation.oid) == (schema, logical, oid)
            assert native_relation.source_layout.oid == next(
                source_oid for target, _stage, source_oid in prepared.address.stage_oids if target == logical
            )
        await original_create(
            admission, fhir, schema, logical, oid, raw_relation=raw_relation, native_relation=native_relation
        )

    return observe_creation


async def _prepare_observed(prepared, factory, request, roles, capture_id, monkeypatch):
    """Observe real callbacks and registration while preserving actual admission checks."""
    source_sequence_state = await _source_evidence_sequence_state(prepared)
    growth_events, created_relations, binary_inputs = [], [], []
    address_inputs, address_phases = [], []
    await _reject_copy_batch_interruptions(prepared, factory, request, roles, capture_id, monkeypatch)
    await _reject_address_copy_interruptions(prepared, factory, request, roles, capture_id, monkeypatch)
    original_growth = retention.RegistryCMSRawEpochAdmission.__call__
    original_copy = epoch_capture._copy_native_batch

    observe_address_copy = _address_copy_observer(prepared, created_relations, address_inputs, address_phases)

    async def observe_binary_copy(connection, query, arguments, schema, table, expected_count, **options):
        """Observe real COPY input sizes without replacing either driver operation."""
        proxy = BootstrapCopyConnection(connection)
        await original_copy(proxy, query, arguments, schema, table, expected_count, **options)
        assert len(proxy.imports) == 1
        assert "OFFSET" not in query and "ORDER BY" in query
        assert all(0 < byte_count <= options["byte_limit"] for byte_count in proxy.import_bytes)
        binary_inputs.extend(proxy.import_bytes)

    async def observe_growth(adapter, phase, schema, logical, oid):
        row_count = await adapter.connection.fetchval(f'SELECT count(*) FROM "{schema}"."{logical}"')
        indexes = await adapter.connection.fetchval("SELECT count(*) FROM pg_index WHERE indrelid=$1", oid)
        growth_events.append((phase, logical, oid, row_count, indexes))
        await original_growth(adapter, phase, schema, logical, oid)

    observe_creation = _retained_creation_observer(prepared, capture_id, created_relations)

    with monkeypatch.context() as patch:
        patch.setattr(retention.RegistryCMSRawEpochAdmission, "__call__", observe_growth)
        patch.setattr(NonprofileAdmission, "register_external_relation", observe_creation)
        patch.setattr(epoch_capture, "_copy_native_batch", observe_binary_copy)
        patch.setattr(prepared_address, "_copy_native_batch", observe_address_copy)
        retained = await retention.prepare_registry_cms_source_pair(
            prepared, factory, request, capture_id=capture_id, owner_role=roles[0], runtime_roles=(roles[1],)
        )
    _assert_original_heap_growth(growth_events, created_relations, retained, prepared)
    assert binary_inputs
    assert {table for table, _count, _bytes in address_inputs} == set(prepared_address._COPY_KEYS)
    assert len(address_phases) == 2 * len(address_inputs)
    for index, (table, count, _bytes) in enumerate(address_inputs):
        before, after = address_phases[index * 2 : index * 2 + 2]
        assert before[:2] == (table, "before_import") and after[:2] == (table, "after_import")
        assert after[2] - before[2] == count
    assert await _source_evidence_sequence_state(prepared) == source_sequence_state
    return retained


async def _source_evidence_sequence_state(prepared):
    """Native explicit-ID imports must neither change nor advance the source default."""
    stage = next(stage for logical, stage, _oid in prepared.address.stage_oids if logical == "entity_address_evidence")
    default = await prepared.fhir.db.first(
        """SELECT pg_get_expr(d.adbin,d.adrelid) AS default_expr,s.oid::regclass::text AS sequence
        FROM pg_attrdef d JOIN pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum
        JOIN pg_depend dep ON dep.classid='pg_attrdef'::regclass AND dep.objid=d.oid
        JOIN pg_class s ON s.oid=dep.refobjid AND s.relkind='S'
        WHERE d.adrelid=to_regclass(:relation) AND a.attname='evidence_id'""",
        relation=prepared.address.db_schema + "." + stage,
    )
    assert default is not None
    state = await prepared.fhir.db.first("SELECT last_value,is_called FROM " + default["sequence"])
    return dict(default), dict(state)


def _assert_original_heap_growth(growth_events, created_relations, retained, prepared):
    """All eighteen retained heaps enroll once; each native index growth is bracketed."""
    assert len(created_relations) == len(set(created_relations)) == 18
    assert len({oid for _schema, _logical, oid in created_relations}) == 18
    epoch = retained.recipe.source_pin.retained_epoch
    for logical, oid in epoch.relation_oids:
        events = [event for event in growth_events if event[1] == logical]
        assert all(event[2] == oid for event in events)
        for phase in ("created", "before_insert"):
            assert [event[3:] for event in events if event[0] == phase] == [(0, 0)]
        assert len([event for event in events if event[0] == "after_insert"]) == 1
        before_batches = [event for event in events if event[0] == "before_copy_batch"]
        after_batches = [event for event in events if event[0] == "after_copy_batch"]
        for before, after in zip(before_batches, after_batches, strict=True):
            assert 0 < after[3] - before[3] <= prepared.nonprofile_admission.plan.batch_size
            assert before[4] == after[4] == 0
        if events[-1][3] > prepared.nonprofile_admission.plan.batch_size:
            assert len(after_batches) > 1
        before_indexes = [event for event in events if event[0] == "before_index"]
        after_indexes = [event for event in events if event[0] == "after_index"]
        assert (
            len(before_indexes)
            == len(after_indexes)
            == 1 + sum(table == logical for _name, table, _keys in epoch_capture._LOOKUP_INDEXES)
        )
        for before, after in zip(before_indexes, after_indexes, strict=True):
            assert before[3] == after[3]
            assert before[4] + 1 == after[4]
    native_relations = prepared.nonprofile_admission._relations
    for schema, logical, oid in created_relations:
        assert native_relations[(schema, logical)] == oid
    assert len(native_relations) == 7 + 18


async def _reject_copy_batch_interruptions(prepared, factory, request, roles, capture_id, monkeypatch):
    """Actual completed COPY failures retire heaps and preserve an exact retry."""
    original = retention.RegistryCMSRawEpochAdmission.__call__
    for interruption in (RuntimeError, asyncio.CancelledError):
        completed_batches = []

        async def refuse_after_import(adapter, phase, schema, logical, oid, interruption=interruption):
            """Fail only after the real batch and its native admission checks."""
            await original(adapter, phase, schema, logical, oid)
            if phase == "after_copy_batch":
                completed_batches.append((schema, logical, oid))
                raise interruption("synthetic completed COPY interruption")

        expected = epoch_capture.FHIRSourceEpochError if interruption is RuntimeError else interruption
        with monkeypatch.context() as patch:
            patch.setattr(retention.RegistryCMSRawEpochAdmission, "__call__", refuse_after_import)
            with pytest.raises(expected):
                await retention.prepare_registry_cms_source_pair(
                    prepared, factory, request, capture_id=capture_id, owner_role=roles[0], runtime_roles=(roles[1],)
                )
        assert len(completed_batches) == 1
        for schema in ("registry_cms_epoch_" + capture_id.hex, "entity_address_archive_" + capture_id.hex):
            assert await prepared.fhir.db.scalar("SELECT to_regnamespace(:schema)", schema=schema) is None
        assert len(prepared.nonprofile_admission._relations) == 7


async def _reject_address_copy_interruptions(prepared, factory, request, roles, capture_id, monkeypatch):
    """Completed address COPY refusal and cancellation retire the complete candidate."""
    original_copy = prepared_address._copy_native_batch
    previous_oid_by_relation = dict(prepared.nonprofile_admission._relations)
    previous_external_coordinates = set(prepared.nonprofile_admission._external_relations)
    for interruption in (RuntimeError, asyncio.CancelledError):
        completed_copies = []

        async def refuse_copy(connection, query, arguments, schema, table, count, **options):
            admit = options["import_admission"]

            async def refuse_after(phase):
                await admit(phase)
                if phase == "after_import":
                    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"') == count
                    completed_copies.append((schema, table))
                    raise interruption("synthetic completed address COPY interruption")

            options["import_admission"] = refuse_after
            await original_copy(connection, query, arguments, schema, table, count, **options)

        with monkeypatch.context() as patch:
            patch.setattr(prepared_address, "_copy_native_batch", refuse_copy)
            with pytest.raises(interruption):
                await retention.prepare_registry_cms_source_pair(
                    prepared, factory, request, capture_id=capture_id, owner_role=roles[0], runtime_roles=(roles[1],)
                )
        assert len(completed_copies) == 1
        for prefix in ("registry_cms_epoch_", "entity_address_archive_"):
            assert (
                await prepared.fhir.db.scalar("SELECT to_regnamespace(:schema)", schema=prefix + capture_id.hex) is None
            )
        assert prepared.nonprofile_admission._relations == previous_oid_by_relation
        assert prepared.nonprofile_admission._external_relations == previous_external_coordinates


async def _assert_retained_address_parity(database, retained, prepared):
    """Compare full native rows, portable source receipts and unchanged sequence semantics."""
    async with database.session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        source_receipt = await prepared_address.capture_entity_address_stage_integrity_receipt(
            session,
            schema_name=prepared.address.db_schema,
            stage_table_names={logical: stage for logical, stage, _oid in prepared.address.stage_oids},
        )
        source_capture = await address_equivalence.capture_registry_cms_address_copy_source(
            session,
            source_schema=prepared.address.db_schema,
            expected_relation_oids=tuple(
                sorted((logical, oid) for logical, _stage, oid in prepared.address.stage_oids)
            ),
            stage_table_names={logical: stage for logical, stage, _oid in prepared.address.stage_oids},
        )
        assert (
            prepared_address.portable_prepared_receipt(source_receipt, prepared.address.stage_oids)
            == source_capture.semantic_receipt
        )
        witness = await address_equivalence.validate_registry_cms_address_copy(
            session, source_capture=source_capture, clone_ownership=retained.address_ownership
        )
        assert witness.clone_receipt == retained.address_receipt
        assert source_capture.semantic_receipt.schema_sha256 != retained.address_receipt.schema_sha256
        assert source_capture.semantic_receipt.main_input_sha256 == retained.address_receipt.main_input_sha256
        for logical in RELATION_NAMES:
            assert dict(source_capture.ordinals)[logical][0][1:] == (2, 1)
            assert dict(witness.clone_ordinals)[logical][0][1:] == (1, 1)
        driver = await address_capture.native_driver(session)
        schema = retained.address_ownership.schema_name
        await _assert_prepared_native_copy_key(driver, prepared.address)
        for logical, stage, oid in prepared.address.stage_oids:
            source_ref = f'"{prepared.address.db_schema}"."{stage}"'
            target_ref = f'"{schema}"."{logical}"'
            await prepared_address._require_prepared_copy_key(
                driver, source_ref, oid, prepared_address._COPY_KEYS[logical]
            )
            source_options = await _native_storage_options(driver, source_ref)
            assert source_options["heap_options"] == ["autovacuum_enabled=false"]
            assert source_options["toast_options"] == ["autovacuum_enabled=false"]
            assert await _native_storage_options(driver, target_ref) == source_options
            assert await driver.fetchval(
                f"""SELECT NOT EXISTS((SELECT to_jsonb(head) FROM {source_ref} head
                EXCEPT ALL SELECT to_jsonb(copied) FROM {target_ref} copied)
                UNION ALL (SELECT to_jsonb(copied) FROM {target_ref} copied
                EXCEPT ALL SELECT to_jsonb(head) FROM {source_ref} head))"""
            )
        sequence = await driver.fetchval(
            "SELECT pg_get_serial_sequence($1,'evidence_id')", schema + ".entity_address_evidence"
        )
        assert sequence == schema + ".entity_address_evidence_evidence_id_seq"
        assert tuple(await driver.fetchrow(f"SELECT last_value,is_called FROM {sequence}")) == (1, False)


async def _native_storage_options(driver, relation):
    """Observe exact native heap/TOAST options retained by the clone caller."""
    return dict(
        await driver.fetchrow(
            """SELECT source.reloptions AS heap_options,toast.reloptions AS toast_options
        FROM pg_class source LEFT JOIN pg_class toast ON toast.oid=source.reltoastrelid
        WHERE source.oid=to_regclass($1)""",
            relation,
        )
    )


async def _assert_prepared_native_copy_key(driver, address):
    """Refuse missing, substituted and deferred native ordering keys without source edits."""
    logical, stage, _oid = next(entry for entry in address.stage_oids if entry[0] == "entity_address_unified")
    source = f'"{address.db_schema}"."{stage}"'
    probe = "pg_temp.prepared_address_key_probe"
    await driver.execute(
        f"CREATE TEMP TABLE prepared_address_key_probe (LIKE {source} INCLUDING STORAGE) ON COMMIT DROP"
    )
    oid = await driver.fetchval("SELECT to_regclass($1)::oid::bigint", probe)
    keys = prepared_address._COPY_KEYS[logical]
    with pytest.raises(ValueError, match="copy_key_invalid"):
        await prepared_address._require_prepared_copy_key(driver, probe, oid, keys)
    await driver.execute(f"ALTER TABLE {probe} ADD CONSTRAINT probe_key PRIMARY KEY(entity_id)")
    with pytest.raises(ValueError, match="copy_key_invalid"):
        await prepared_address._require_prepared_copy_key(driver, probe, oid, keys)
    await driver.execute(f"ALTER TABLE {probe} DROP CONSTRAINT probe_key")
    await driver.execute(f"ALTER TABLE {probe} ADD CONSTRAINT probe_key PRIMARY KEY(location_key) DEFERRABLE")
    with pytest.raises(ValueError, match="copy_key_invalid"):
        await prepared_address._require_prepared_copy_key(driver, probe, oid, keys)
    await driver.execute(f"ALTER TABLE {probe} DROP CONSTRAINT probe_key")
    await driver.execute(f"ALTER TABLE {probe} ADD CONSTRAINT probe_key PRIMARY KEY(location_key)")
    await prepared_address._require_prepared_copy_key(driver, probe, oid, keys)


async def _assert_retained_raw_parity(database, retained):
    """Compare complete actual native rows against every exact source scope."""
    pin = retained.recipe.source_pin
    async with database.session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        driver = await address_capture.native_driver(session)
        await _assert_native_source_copy_keys(driver, pin)
        for table in epoch_capture._TABLES:
            predicate, parameters = epoch_capture._epoch_copy_range(pin, table, None)
            source = f'"{pin.schema_name}"."{table}"'
            target = f'"{pin.retained_epoch.schema_name}"."{table}"'
            assert await driver.fetchval(
                f"""SELECT NOT EXISTS(
                (SELECT to_jsonb(head) FROM {source} head WHERE {predicate}
                 EXCEPT ALL SELECT to_jsonb(copied) FROM {target} copied)
                UNION ALL
                (SELECT to_jsonb(copied) FROM {target} copied
                 EXCEPT ALL SELECT to_jsonb(head) FROM {source} head WHERE {predicate}))""",
                *parameters,
            )


async def _assert_native_source_copy_keys(driver, pin):
    """Actual model columns require the exact existing native compound primary key."""
    table = epoch_capture._TABLES[1]
    await driver.execute(
        f'CREATE TEMP TABLE "{table}" (LIKE "{pin.schema_name}"."{table}" INCLUDING STORAGE) ON COMMIT DROP'
    )
    temporary_pin = SimpleNamespace(schema_name="pg_temp")
    with pytest.raises(epoch_capture.FHIRSourceEpochError):
        await epoch_capture._require_epoch_copy_key(driver, temporary_pin, table)
    await driver.execute(
        f'ALTER TABLE pg_temp."{table}" ADD CONSTRAINT wrong_copy_key PRIMARY KEY(dataset_id,resource_type)'
    )
    with pytest.raises(epoch_capture.FHIRSourceEpochError):
        await epoch_capture._require_epoch_copy_key(driver, temporary_pin, table)
    await driver.execute(f'ALTER TABLE pg_temp."{table}" DROP CONSTRAINT wrong_copy_key')
    await driver.execute(
        f'ALTER TABLE pg_temp."{table}" ADD CONSTRAINT exact_copy_key PRIMARY KEY(dataset_id,resource_type,resource_id)'
    )
    await epoch_capture._require_epoch_copy_key(driver, temporary_pin, table)


async def _reject_open_custody(session, retained, roles, receipt_id, payload):
    """Actual opened clone ACLs fail before the common commit."""
    schema = retained.address_ownership.schema_name
    await session.execute(text(f'GRANT UPDATE ON {schema}.entity_address_unified TO "{roles[1]}"'))
    with pytest.raises(ValueError):
        await retention.bind_prepared_registry_cms_source_pair(
            session, retained, receipt_id=receipt_id, receipt_payload=payload
        )
    await session.execute(text(f'REVOKE UPDATE ON {schema}.entity_address_unified FROM "{roles[1]}"'))


async def _bind_rollback(database, retained, predecessor):
    """Native receipt append and pair binding remain one rollback boundary."""
    with pytest.raises(RuntimeError, match="synthetic bind rollback"):
        async with database.session_factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            receipt_id, payload = await _publish_stages(session, retained, predecessor)
            with pytest.raises(ValueError):
                await retention.bind_prepared_registry_cms_source_pair(
                    session,
                    replace(retained, capacity_geometry_hash="0" * 64),
                    receipt_id=receipt_id,
                    receipt_payload=payload,
                )
            await retention.bind_prepared_registry_cms_source_pair(
                session, retained, receipt_id=receipt_id, receipt_payload=payload
            )
            raise RuntimeError("synthetic bind rollback")
    async with database.session_factory() as session:
        assert (await serving.read_current_receipt(session, "mrf"))["receipt_id"] == predecessor["receipt_id"]


async def _successor_retention(database, pair):
    """A later real native address swap cannot change the closed retained family."""
    async with database.session_factory() as session, session.begin():
        predecessor = await serving.read_current_receipt(session, "mrf")
        for logical in RELATION_NAMES:
            await session.execute(text(f"DROP TABLE mrf.retained_old_{logical} CASCADE"))
            await session.execute(text(f"ALTER TABLE mrf.{logical} RENAME TO retained_old_{logical}"))
            await session.execute(text(f"CREATE TABLE mrf.{logical} (LIKE mrf.retained_old_{logical} INCLUDING ALL)"))
        await native._advance_native(session, "mrf")
        await session.execute(
            text(
                "UPDATE mrf.provider_directory_profile_serving_generation SET generation_id=:generation,control_generation=control_generation+1"
            ),
            {"generation": "pdprofile_" + uuid4().hex},
        )
        payload = await native._payload(session, "mrf", predecessor)
        payload["cms"]["release_id"] = predecessor["payload"]["cms"]["release_id"]
        payload["selection"]["proof_id"] = predecessor["payload"]["selection"]["proof_id"]
        await serving.append_serving_receipt(session, "mrf", payload)
    async with database.session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        assert await require_registry_cms_source_pair(session, pair) == pair
        assert pair.address_receipt.tables[0].row_count == 2
