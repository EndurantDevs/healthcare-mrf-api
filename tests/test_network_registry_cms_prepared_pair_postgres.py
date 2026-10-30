# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native retained pairs through the original signed Source publication pipeline."""

import asyncio
import contextvars
import json
import os
import re
from contextlib import aclosing, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import MetaData, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import create_async_engine
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
from process.network_cms_registry_source_pair import (
    capture_registry_cms_source_pair,
    require_registry_cms_source_pair,
    resolve_committed_registry_cms_source_pair,
)
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
from tests.retained_wal_diagnostics import RetainedWalDiagnostic, retained_bound_operands, retained_wal_attribution
from tests.test_network_cms_registry_source_pair_postgres import (
    _bounded_source_copy,
    _publish_pair,
    _read_transaction,
    _retained_office_resources,
    _verify_read_pair,
    _verify_retained_entity_read,
)
from tests.test_network_custom_address_source_postgres import BootstrapCopyConnection
from tests.test_provider_directory_cms_preparation import VALIDATION_TIME
from tests.test_provider_directory_cms_resource_batch_postgres import cms_source_template as cms_source_template
from tests.test_provider_directory_profile_capacity_attestation import _trust, _verify

pytestmark = pytest.mark.usefixtures(cms_source_template.__name__)


def _retention_catalog_url(configured):
    """Whole-database maintenance is restricted to the runner-owned UUID database."""
    try:
        url = make_url(configured)
    except ArgumentError:
        raise ValueError("retention_catalog_database_not_disposable") from None
    allowed_hosts = {"127.0.0.1", "localhost"}
    if os.getenv("CI") == "true":
        allowed_hosts.add("postgres")
    if (
        url.drivername not in {"postgresql", "postgresql+asyncpg"}
        or url.query
        or url.host not in allowed_hosts
        or url.port is None
        or not 1 <= url.port <= 65535
        or not re.fullmatch(r"hc_network_registry_[0-9a-f]{32}", url.database or "")
    ):
        raise ValueError("retention_catalog_database_not_disposable")
    return url.set(drivername="postgresql+asyncpg")


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def cms_resource_template(request, cms_source_template):
    """Keep the ordinary admission baseline explicit for the tests that use it."""
    async with aclosing(support.cms_admission_template.__wrapped__(request)) as baseline:
        await anext(baseline)
        yield


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def retained_catalog_baseline(cms_resource_template):
    """Finish earlier fixture catalog cleanup before measuring cluster-wide WAL."""
    url = _retention_catalog_url(os.environ["NETWORK_REGISTRY_TEST_DSN"])
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
            await connection.exec_driver_sql("VACUUM")
        yield
    finally:
        await engine.dispose()


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
            assert await _native_storage_options(
                (await connection.get_raw_connection()).driver_connection, f'"mrf"."{stage}"'
            ) == {
                "heap_options": ["autovacuum_enabled=false"],
                "toast_options": ["autovacuum_enabled=false"],
            }
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


async def _transfer_address_publication_custody(database, address):
    """Transfer only exact fixture live/stage OIDs and their owned native sequences."""
    owner = database._source_component.owner
    expected_by_name = {stage: oid for _logical, stage, oid in address.stage_oids}
    async with database.engine.begin() as connection:
        for logical, _stage, _oid in address.stage_oids:
            expected_by_name[logical] = await connection.scalar(
                text("SELECT to_regclass(:relation)::oid::bigint"), {"relation": "mrf." + logical}
            )
        assert all(type(oid) is int and oid > 0 for oid in expected_by_name.values())
        heaps = (
            (
                await connection.execute(
                    text(
                        "SELECT c.relname,c.oid::bigint,c.relkind::text AS relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                        "WHERE n.nspname='mrf' AND c.oid=ANY(CAST(:oids AS oid[])) ORDER BY c.relname"
                    ),
                    {"oids": list(expected_by_name.values())},
                )
            )
            .mappings()
            .all()
        )
        assert {entry["relname"]: entry["oid"] for entry in heaps} == expected_by_name
        assert all(entry["relkind"] == "r" for entry in heaps)
        sequences = (
            (
                await connection.execute(
                    text(
                        "SELECT s.relname,s.oid::bigint,n.nspname AS schema_name FROM pg_depend d JOIN pg_class s ON s.oid=d.objid JOIN pg_namespace n ON n.oid=s.relnamespace "
                        "WHERE d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass "
                        "AND d.refobjid=ANY(CAST(:oids AS oid[])) AND d.deptype IN ('a','i') AND s.relkind='S'"
                    ),
                    {"oids": list(expected_by_name.values())},
                )
            )
            .mappings()
            .all()
        )
        assert all(entry["schema_name"] == "mrf" for entry in sequences)
        for name in expected_by_name:
            await connection.execute(text(f'ALTER TABLE mrf."{name}" OWNER TO "{owner}"'))
        for sequence in sequences:
            await connection.execute(text(f'ALTER SEQUENCE mrf."{sequence["relname"]}" OWNER TO "{owner}"'))
        owned_oids = [*expected_by_name.values(), *(entry["oid"] for entry in sequences)]
        assert await connection.scalar(
            text(
                "SELECT count(*) FROM pg_class WHERE oid=ANY(CAST(:oids AS oid[])) "
                "AND relowner=(SELECT oid FROM pg_roles WHERE rolname=:owner)"
            ),
            {"oids": owned_oids, "owner": owner},
        ) == len(owned_oids)
        assert not await connection.scalar(
            text(
                "SELECT EXISTS(SELECT FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                "WHERE i.indrelid=ANY(CAST(:oids AS oid[])) AND c.relowner<>(SELECT oid FROM pg_roles WHERE rolname=:owner))"
            ),
            {"oids": list(expected_by_name.values()), "owner": owner},
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
    await _transfer_address_publication_custody(database, address)

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
        json.dumps({"registry_source_retention": request.policy(capture_id, roles[0], tuple(sorted(roles[1:])))}),
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
    admission = await _native_admission(
        fhir, plan, request.policy(capture_id, roles[0], tuple(sorted(roles[1:]))), execution
    )
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
        wal_bytes = await fhir.db.scalar(
            "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),CAST(CAST(:start AS text) AS pg_lsn))::bigint",
            start=start,
        )
        diagnostic = getattr(admission, "_retained_wal_diagnostic", None)
        if diagnostic is not None:
            diagnostic.record(start, wal_bytes)
        assert wal_bytes < 128 * 1024**2
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
    database_identity = await fhir.db.first(
        """SELECT database_row.oid::bigint AS database_oid,database_row.datname AS database_name,
        control.system_identifier::text AS database_system_identifier
        FROM pg_database database_row CROSS JOIN pg_control_system() control
        WHERE database_row.datname=current_database()"""
    )
    assert database_identity is not None
    return dict(support.fhir._pagination_checkpoint_row_mapping(database_identity))


async def _publish_stages(session, prepared_pair, predecessor):
    """Apply an actual native OID swap and append its guarded common receipt."""
    await _publish_retained_candidate(session, prepared_pair)
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
    publication_payload = await native._payload(session, "mrf", predecessor)
    publication_payload["selection"]["proof_id"] = predecessor["payload"]["selection"]["proof_id"]
    publication_payload["cms"]["release_id"] = prepared_pair.request.source_pin.release_id
    publication_payload["expected_incumbent"] = {field: predecessor["payload"]["cms"][field] for field in native._PIN}
    receipt_id = await serving.append_serving_receipt(
        session, "mrf", publication_payload, registry_source_pair=prepared_pair
    )
    slot = (
        (
            await session.execute(
                text(
                    "SELECT registry_source_pair_json,publication_xid::text FROM mrf.provider_directory_cms_serving_receipt "
                    "WHERE receipt_id=:receipt"
                ),
                {"receipt": receipt_id},
            )
        )
        .mappings()
        .one()
    )
    assert serving.decode_registry_source_pair_document(slot["registry_source_pair_json"]) == prepared_pair
    assert slot["publication_xid"] == await session.scalar(text("SELECT pg_current_xact_id()::text"))
    assert (
        await serving.read_registry_source_pair_receipt(
            session, "mrf", prepared_pair.recipe.source_pin.retained_epoch.epoch_id
        )
        is None
    )
    return receipt_id, publication_payload


async def _publish_retained_candidate(session, prepared_pair):
    """Advance the genuine validated successor through the existing guarded transition."""
    pin = prepared_pair.request.source_pin
    parameters_by_field = {
        "dataset": pin.dataset_id,
        "release": pin.release_id,
        "hash": pin.dataset_sha256,
        "vector": json.dumps([{"source_id": pin.source_id, "dataset_id": pin.dataset_id}]),
    }
    await session.execute(
        text(
            "UPDATE mrf.provider_directory_endpoint_dataset SET status='superseded',is_current=false,superseded_at=now() WHERE status='published' AND endpoint_id=:endpoint AND dataset_id<>:dataset"
        ),
        {**parameters_by_field, "endpoint": pin.endpoint_id},
    )
    await session.execute(
        text(
            "UPDATE mrf.provider_directory_endpoint_dataset SET status='published',is_current=true,published_at=now() WHERE dataset_id=:dataset AND status='validated'"
        ),
        parameters_by_field,
    )
    await session.execute(
        text(
            "INSERT INTO mrf.provider_directory_cms_serving_coverage(dataset_id,release_id,dataset_hash,proof_version,published_at,created_at) VALUES(:dataset,:release,:hash,2,now(),now()) ON CONFLICT DO NOTHING"
        ),
        parameters_by_field,
    )
    await session.execute(
        text("UPDATE mrf.provider_directory_profile_serving_generation SET source_vector_json=:vector"),
        parameters_by_field,
    )
    await native._advance_native(session, "mrf", doctors=True)


async def _reject_preparation(database, prepared, factory, request, roles, capture_id, monkeypatch):
    """Missing signed policy and cancellation leave no retained copy namespaces."""
    unsigned_id = uuid4()
    options_by_field = dict(capture_id=unsigned_id, owner_role=roles[0], runtime_roles=tuple(sorted(roles[1:])))
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
                runtime_roles=tuple(sorted(roles[1:])),
            )
    async with database.session_factory() as session:
        for identity in (unsigned_id, capture_id):
            for prefix in ("registry_cms_epoch_", "entity_address_archive_"):
                assert (
                    await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": prefix + identity.hex})
                    is None
                )


async def _verify_current_office_successor(database, original_pair, pair):
    async with database.session_factory() as session:
        current_dataset = await session.scalar(
            text(
                "SELECT dataset_id FROM mrf.provider_directory_endpoint_dataset WHERE status='published' AND is_current"
            )
        )
        assert current_dataset == pair.recipe.source_pin.dataset_id
        assert current_dataset != original_pair.recipe.source_pin.dataset_id
        assert (
            await session.scalar(
                text(
                    "SELECT payload_json->>'name' FROM mrf.provider_directory_dataset_resource WHERE dataset_id=:dataset AND resource_type='Location' AND resource_id='site-1'"
                ),
                {"dataset": current_dataset},
            )
            == "Office A second"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("copy_bounds", [(1, 64 * 1024**2), (32, 64 * 1024)])
async def test_prepared_retention_native_binding(monkeypatch, tmp_path, copy_bounds):
    """Publish the signed successor with full retention enrollment, parity and binder checks."""
    from tests.cms_registry_native_producer_postgres_support import genuine_prepared_publication

    async with genuine_prepared_publication(monkeypatch, tmp_path, copy_bounds, observe_preparation=True) as state:
        await _verify_genuine_retained_state(state, monkeypatch)


async def _origin_source_authority(database):
    """Observe original Source namespace, role and relation custody without changing it."""
    from process.provider_directory_source_namespace_custody import capture_source_namespace_custody

    component = database._source_component
    component.factory._require_current_policy()
    assert component.factory.policy is component.policy
    async with component.admin_engine.connect() as connection, connection.begin():
        await connection.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        driver = (await connection.get_raw_connection()).driver_connection
        namespace = await capture_source_namespace_custody(
            driver, component.policy, namespace_owner_role=component.factory.namespace_owner_role
        )
        assert await driver.fetchval("SELECT to_regclass('mrf.provider_directory_dataset_resource')") is not None
        publisher = next(
            role["oid"] for role in namespace["role_graph"]["roles"] if role["rolname"] == component.retained_publisher
        )
        actor = await driver.fetchrow(
            "SELECT r.oid::bigint AS oid,pg_has_role(r.oid,$2::regrole,'USAGE') AS owner_usage,"
            "pg_has_role(r.oid,$2::regrole,'SET') AS owner_set FROM pg_roles r WHERE r.rolname=$1::name",
            component.retained_publisher,
            component.policy.owner_role,
        )
        assert actor is not None and actor["oid"] == int(publisher)
        assert actor["owner_usage"] is False and actor["owner_set"] is False
        relations = await driver.fetch(
            "SELECT c.oid,c.relowner,c.relacl,"
            "COALESCE((SELECT jsonb_agg(jsonb_build_array(a.attnum,a.attacl) ORDER BY a.attnum) "
            "FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attnum>0),'[]'::jsonb) AS columns "
            "FROM pg_class c WHERE c.relnamespace=$1::oid ORDER BY c.oid",
            int(namespace["namespace"]["oid"]),
        )
        return component.policy, namespace, relations


async def _origin_pair_read(connection, pair, *, granted):
    from process import network_cms_registry_source_pair as pair_reader

    session = address_capture.NativeReceiptSession(connection)
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        if not granted:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await pair_reader._publication(session, pair.proof, current=False)
            return
        resolved, oid = await pair_reader._read_committed_pair(connection, pair.address_source.schema_name)
        assert resolved == pair and oid == pair.address_ownership.schema_oid
        assert (
            await pair_reader.require_registry_cms_source_pair_connection(connection, pair, verify_content=False)
            == pair
        )
        with pytest.raises(epoch_capture.FHIRSourceEpochError):
            await epoch_capture.require_retained_cms_fhir_source_epoch(
                connection,
                pair.proof.source_pin,
                replace(pair.recipe.source_pin.retained_epoch, metadata_sha256="0" * 64),
                verify_content=False,
            )
        for proof in (
            replace(pair.proof, serving_receipt_id="0" * 64),
            replace(pair.proof, publication_xid=str(int(pair.proof.publication_xid) + 1)),
            replace(pair.proof, profile_generation_id="pdprofile_" + "0" * 32),
        ):
            with pytest.raises(ValueError, match="publication_unavailable"):
                await pair_reader._publication(session, proof, current=False)
        assert await serving.read_registry_source_pair_receipt(session, "mrf", uuid4()) is None


async def _origin_wider_reads_refused(connection, owner):
    for statement in (
        "SELECT created_at FROM mrf.provider_directory_cms_serving_receipt",
        "SELECT * FROM mrf.provider_directory_cms_serving_receipt",
        "SELECT * FROM mrf.provider_directory_dataset_resource",
        "UPDATE mrf.provider_directory_cms_serving_receipt SET payload=payload WHERE false",
        f'SET ROLE "{owner}"',
    ):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(statement)


async def _origin_native_acl_refusals(database, roles, *, metadata_read_roles=None):
    from process import provider_directory_cms_receipt_custody as custody

    component = database._source_component
    custody_options_by_name = dict(
        migration_role=component.factory.migration_role,
        metadata_read_roles=roles[:2] if metadata_read_roles is None else metadata_read_roles,
    )
    async with component.admin_engine.begin() as connection:
        await connection.exec_driver_sql("SELECT 1")
        driver = (await connection.get_raw_connection()).driver_connection
        before = await custody.capture_cms_serving_receipt_custody(driver, component.policy, **custody_options_by_name)
        for privilege, role, suffix in (
            ("SELECT", roles[0], ""),
            ("SELECT(created_at)", roles[0], ""),
            ("SELECT(receipt_id)", roles[0], " WITH GRANT OPTION"),
            ("SELECT(receipt_id)", roles[2], ""),
            ("UPDATE(payload)", roles[0], ""),
        ):
            savepoint = driver.transaction()
            await savepoint.start()
            try:
                await driver.execute(
                    f'GRANT {privilege} ON mrf.provider_directory_cms_serving_receipt TO "{role}"{suffix}'
                )
                with pytest.raises(ValueError, match="acl_changed"):
                    await custody.capture_cms_serving_receipt_custody(
                        driver, component.policy, **custody_options_by_name
                    )
            finally:
                await savepoint.rollback()
            assert (
                await custody.capture_cms_serving_receipt_custody(driver, component.policy, **custody_options_by_name)
                == before
            )


async def _origin_set_authority_refusals(database, roles, *, metadata_read_roles=None):
    from process import provider_directory_cms_receipt_custody as custody

    component = database._source_component
    custody_options_by_name = dict(
        migration_role=component.factory.migration_role,
        metadata_read_roles=roles[:2] if metadata_read_roles is None else metadata_read_roles,
    )
    async with component.admin_engine.begin() as connection:
        await connection.exec_driver_sql("SELECT 1")
        driver = (await connection.get_raw_connection()).driver_connection
        before = await custody.capture_cms_serving_receipt_custody(driver, component.policy, **custody_options_by_name)
        for reachable, setup in (
            (component.factory.namespace_owner_role, None),
            (roles[2], f'GRANT CREATE ON SCHEMA mrf TO "{roles[2]}"'),
            (roles[2], f'ALTER ROLE "{roles[2]}" CREATEROLE'),
        ):
            savepoint = driver.transaction()
            await savepoint.start()
            try:
                if setup is not None:
                    await driver.execute(setup)
                await driver.execute(f'GRANT "{reachable}" TO "{roles[0]}" WITH INHERIT FALSE, SET TRUE')
                assert not await driver.fetchval("SELECT has_schema_privilege($1::name,'mrf'::name,'CREATE')", roles[0])
                assert await driver.fetchval("SELECT pg_has_role($1::name,$2::name,'SET')", roles[0], reachable)
                with pytest.raises(ValueError, match="metadata_reader_authority_changed"):
                    await custody.capture_cms_serving_receipt_custody(
                        driver, component.policy, **custody_options_by_name
                    )
            finally:
                await savepoint.rollback()
            assert (
                await custody.capture_cms_serving_receipt_custody(driver, component.policy, **custody_options_by_name)
                == before
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("copy_bounds", [(1, 64 * 1024**2), (32, 64 * 1024)])
async def test_native_retained_pair_survives_receipt_head_refresh(monkeypatch, tmp_path, copy_bounds):
    """Keep retained readers pinned before and after an independent guarded receipt refresh."""
    from tests.cms_registry_native_producer_postgres_support import (
        genuine_prepared_publication,
        refresh_native_receipt_head,
    )

    async with genuine_prepared_publication(monkeypatch, tmp_path, copy_bounds) as state:
        await _verify_genuine_retained_state(state, monkeypatch)
        await refresh_native_receipt_head(state, monkeypatch)
        await _verify_genuine_retained_state(state, monkeypatch)


@pytest.mark.asyncio
@pytest.mark.parametrize("copy_bounds", [(1, 64 * 1024**2), (32, 64 * 1024)])
async def test_native_retention_preparation_failures_preserve_source(monkeypatch, tmp_path, copy_bounds):
    """Isolate raw preparation failures from successful publication and late rollback."""
    from tests.cms_registry_native_producer_postgres_support import verify_native_preparation_failures

    await verify_native_preparation_failures(monkeypatch, tmp_path, copy_bounds)


@pytest.mark.asyncio
@pytest.mark.parametrize("copy_bounds", [(1, 64 * 1024**2), (32, 64 * 1024)])
async def test_native_retention_address_copy_interruptions_preserve_source(monkeypatch, tmp_path, copy_bounds):
    """Retain genuine address COPY refusal and cancellation with fresh signed preparation."""
    from tests.cms_registry_native_producer_postgres_support import verify_native_preparation_failures

    await verify_native_preparation_failures(monkeypatch, tmp_path, copy_bounds, address_copy=True)


async def _verify_genuine_retained_state(state, monkeypatch):
    database, pair = state.database, state.pair
    await _verify_read_pair(database, pair, monkeypatch, published=False)
    async with database.session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        assert await require_registry_cms_source_pair(session, pair) == pair
    await _verify_current_office_successor(database, state.original_pair, pair)
    await _verify_retained_entity_read(database, state.original_pair, monkeypatch)
    await _verify_retained_entity_read(database, pair, monkeypatch, revision="second", unknown=True)


async def _reject_historical_capture_during_source(database, proof, roles):
    """A completed real SOURCE input does not waive the historical actor boundary."""
    from process.provider_directory_generic_source_policy import bind_generic_source_transaction_factory

    async with bind_generic_source_transaction_factory(database._source_component.factory):
        async with database.session_factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
            driver = await address_capture.native_driver(session)
            with pytest.raises(epoch_capture.FHIRSourceEpochError, match="fhir_source_epoch_unavailable"):
                await epoch_capture.capture_retained_cms_fhir_source_epoch(
                    driver,
                    proof.source_pin,
                    epoch_id=uuid4(),
                    owner_role=roles[0],
                    runtime_roles=tuple(sorted(roles[1:])),
                    expected_admission_sha256=proof.expected_admission_sha256,
                    expected_metadata_sha256=proof.expected_metadata_sha256,
                )


async def _can_install_wal_inspector(database):
    """Optional fixture setup cannot replace an original admission outcome."""
    try:
        async with database.engine.begin() as connection:
            if not await connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_available_extensions WHERE name='pg_walinspect')")
            ):
                return False
            await connection.exec_driver_sql("CREATE EXTENSION IF NOT EXISTS pg_walinspect WITH SCHEMA public")
        return True
    except Exception:
        return False


@pytest.mark.asyncio
@pytest.mark.usefixtures("retained_catalog_baseline")
async def test_native_wal_attribution_preserves_original_refusal(monkeypatch, record_property):
    """Classify real foreign WAL and FPI without changing the original 32MiB guard."""
    from tests import retained_wal_diagnostics as diagnostics

    original_inspection = diagnostics._inspect_original_interval

    async def flushed_inspection(connection, start_lsn, end_lsn):
        """Flush this disposable cluster only after the failed interval is frozen."""
        await connection.fetchval("SELECT pg_catalog.pg_switch_wal()")
        return await original_inspection(connection, start_lsn, end_lsn)

    monkeypatch.setattr(diagnostics, "_inspect_original_interval", flushed_inspection)
    async with support.admission_database(monkeypatch) as database:
        await _seed_wal_attribution(database)
        original_errors = []
        with pytest.raises(ValueError, match="registry_cms_retention_reservation_exceeded") as captured:
            async with database.session_factory() as session, session.begin():
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                driver = await address_capture.native_driver(session)
                before = await driver.fetchval("SELECT pg_total_relation_size('public.admitted_wal_fixture')")
                start = await driver.fetchval("SELECT pg_current_wal_insert_lsn()::text")
                await driver.execute("UPDATE public.admitted_wal_fixture SET counter=counter+1 WHERE id=1")
                async with database.engine.begin() as foreign_writer:
                    await foreign_writer.exec_driver_sql("""INSERT INTO public.foreign_wal_fixture
                        SELECT unit,repeat(md5(unit::text),256) FROM generate_series(1,5000) unit""")
                state = _retained_diagnostic_observers()
                state.diagnostic.begin(start)
                state.diagnostic.enter("readiness")
                prepared = await _wal_attribution_prepared(driver)
                request = SimpleNamespace(
                    extra_data_upper_bound_bytes=32 * 1024**2, extra_wal_upper_bound_bytes=32 * 1024**2
                )
                physical_ids = await driver.fetchval("""SELECT ARRAY[pg_relation_filenode(oid),
                    pg_relation_filenode(reltoastrelid)] FROM pg_class
                    WHERE oid='public.foreign_wal_fixture'::regclass""")
                settings_before = await _wal_attribution_settings(driver)
                try:
                    await state.observers["_assert_retained_bounds"](driver, prepared, request, before, start)
                except ValueError as error:
                    original_errors.append(error)
                    assert await _wal_attribution_settings(driver) == settings_before
                    assert await driver.fetchval("SELECT 1") == 1
                    _assert_native_attribution(error, record_property, physical_ids)
                    await _reject_native_diagnostic_failure(driver, start, error, settings_before, monkeypatch)
                    raise
        assert captured.value is original_errors[0]
        assert captured.value.__cause__ is None and captured.value.__context__ is None
        async with database.engine.connect() as connection:
            assert await connection.scalar(text("SELECT count(*) FROM public.foreign_wal_fixture")) == 5000
            assert await connection.scalar(text("SELECT counter FROM public.admitted_wal_fixture WHERE id=1")) == 0


async def _reject_native_diagnostic_failure(driver, start, original_error, settings_before, monkeypatch):
    """A genuine diagnostic SQL error rolls its savepoint back without poisoning the refusal."""
    from tests import retained_wal_diagnostics as diagnostics

    async def failed_inspection(connection, *_args):
        await connection.execute("SET LOCAL work_mem='8MB'")
        await connection.execute("SELECT 1/0")

    operands = json.loads(original_error.__notes__[0].split(" ", 1)[1])
    before_notes = tuple(original_error.__notes__)
    with monkeypatch.context() as patch:
        patch.setattr(diagnostics, "_inspect_original_interval", failed_inspection)
        assert await retained_wal_attribution(driver, start, operands["wal_bytes"]) is None
    assert await _wal_attribution_settings(driver) == settings_before
    assert await driver.fetchval("SELECT 1") == 1
    assert tuple(original_error.__notes__) == before_notes


async def _seed_wal_attribution(database):
    """Install native inspection and seed logged fixtures before the measured interval."""
    assert await _can_install_wal_inspector(database), "native attribution proof requires pg_walinspect"
    async with database.engine.begin() as connection:
        await connection.exec_driver_sql("CREATE TABLE public.admitted_wal_fixture(id bigint PRIMARY KEY,counter int)")
        await connection.exec_driver_sql("INSERT INTO public.admitted_wal_fixture VALUES(1,0)")
        await connection.exec_driver_sql("CREATE TABLE public.foreign_wal_fixture(id bigint PRIMARY KEY,payload text)")
        await connection.exec_driver_sql(
            "ALTER TABLE public.foreign_wal_fixture ALTER COLUMN payload SET STORAGE EXTERNAL"
        )
    async with database.engine.connect() as connection:
        connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
        await connection.exec_driver_sql("CHECKPOINT")


async def _wal_attribution_prepared(driver):
    async def measure(_fhir, _schema):
        size = await driver.fetchval("SELECT pg_total_relation_size('public.admitted_wal_fixture')")
        return (SimpleNamespace(total_bytes=size),)

    return SimpleNamespace(
        nonprofile_admission=SimpleNamespace(measure=measure), fhir=None, address=SimpleNamespace(db_schema="public")
    )


async def _wal_attribution_settings(driver):
    return tuple(
        await driver.fetchrow("""SELECT current_setting('work_mem'),current_setting('statement_timeout'),
        current_setting('temp_file_limit'),current_setting('search_path')""")
    )


def _assert_native_attribution(error, record_property, physical_ids):
    notes = error.__notes__
    operands = json.loads(notes[0].split(" ", 1)[1])
    attribution = json.loads(notes[2].split(" ", 1)[1])
    assert operands["wal_limit_bytes"] == 32 * 1024**2 < operands["wal_bytes"]
    assert operands["data_delta_bytes"] < operands["data_limit_bytes"] == 32 * 1024**2
    assert attribution["lsn_span_bytes"] == operands["wal_bytes"]
    assert attribution["status"] == "complete" and attribution["accounting_authority"] is False
    assert sum(entry["fpi_bytes"] for entry in attribution["record_types"]) > 0
    assert sum(entry["record_bytes"] for entry in attribution["record_types"]) > 32 * 1024**2
    assert 0 < len(attribution["top_physical_relations"]) <= 16
    attributed_ids = {entry["relfilenode"] for entry in attribution["top_physical_relations"]}
    assert set(physical_ids).issubset(attributed_ids)
    record_property("fixture_physical_ids", json.dumps(physical_ids))
    record_property("original_bound_operands", json.dumps(operands, sort_keys=True))
    record_property("wal_attribution", json.dumps(attribution, sort_keys=True))


async def _prepare_successor_pair(database, tmp_path, execution, roles, capture_id, monkeypatch, copy_bounds):
    """Seal a genuine successor while its dataset is still validated and unpublished."""
    (tmp_path / "successor").mkdir()
    directory, acquired = support.retained_release(tmp_path / "successor", revision="second")
    with support.release_probe_client(directory) as client:
        successor = await support.run_protected_admission(
            database, monkeypatch, "retention-successor", directory, acquired, client
        )
    proof = _prepublication_inputs(successor, execution.attestation.proof_id)
    monkeypatch.setattr(
        native,
        "_PIN",
        {field: successor["cms_serving_candidate"]["desired_cms_dataset"][field] for field in native._PIN},
    )
    prepared, factory, request = await _prepared_input(database, proof, capture_id, roles, monkeypatch, copy_bounds[0])
    await _reject_preparation(database, prepared, factory, request, roles, capture_id, monkeypatch)
    await _reject_copy_budget(prepared, factory, request, capture_id, roles, monkeypatch)
    await _reject_raw_oid(database, prepared, factory, request, roles, capture_id, monkeypatch)
    retained = await _prepare_observed(prepared, factory, request, roles, capture_id, monkeypatch)
    await _assert_retained_raw_parity(database, retained)
    await _assert_retained_address_parity(database, retained, prepared)
    async with database.session_factory() as session, session.begin():
        await _read_transaction(session)
        with pytest.raises(ValueError, match="registry_cms_source_pair_unavailable"):
            await resolve_committed_registry_cms_source_pair(session, retained.recipe)
    return retained


def _prepublication_inputs(initial, selection_proof_id):
    """Use genuine sealed candidate coordinates before any publication receipt exists."""
    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
    from process.network_fhir_membership_source import PinnedFHIRMembershipSource

    handoff = initial["registry_source_admission"]
    return SimpleNamespace(
        source_pin=PinnedFHIRMembershipSource(
            "mrf",
            "cms-npd",
            handoff["endpoint_id"],
            handoff["network_bindings"]["dataset_id"],
            handoff["dataset_sha256"],
            handoff["release_id"],
            handoff["resource_table_oid"],
            "cms-npd",
            handoff["semantic_projection_as_of"],
        ),
        binding_coordinates=RegistryNetworkSourceCoordinates(
            **{
                field: handoff["network_bindings"][field]
                for field in RegistryNetworkSourceCoordinates.__dataclass_fields__
            }
        ),
        selection_proof_id=selection_proof_id,
        expected_admission_sha256=handoff["expected_admission_sha256"],
        expected_metadata_sha256=handoff["expected_metadata_sha256"],
    )


async def _reject_copy_budget(prepared, factory, request, capture_id, roles, monkeypatch):
    """A genuinely signed but insufficient extra copy bound rolls every copy back."""
    bounded_request = replace(request, extra_data_upper_bound_bytes=1)
    bounded_factory = replace(
        factory,
        input_json=json.dumps(
            {"registry_source_retention": bounded_request.policy(capture_id, roles[0], tuple(sorted(roles[1:])))}
        ),
    )
    bounded_plan = replace(prepared.nonprofile_admission.plan, native_address_input_hash=bounded_factory.input_hash)
    bounded_prepared = replace(
        prepared,
        nonprofile_admission=await _native_admission(
            prepared.fhir,
            bounded_plan,
            bounded_request.policy(capture_id, roles[0], tuple(sorted(roles[1:]))),
            prepared.execution,
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
                runtime_roles=tuple(sorted(roles[1:])),
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
                prepared,
                factory,
                request,
                capture_id=capture_id,
                owner_role=roles[0],
                runtime_roles=tuple(sorted(roles[1:])),
            )
    assert prepared.nonprofile_admission._relations == before_oid_by_relation
    async with database.session_factory() as session:
        assert (
            await session.scalar(
                text("SELECT to_regnamespace(:schema)"), {"schema": "registry_cms_epoch_" + capture_id.hex}
            )
            is None
        )


def _copy_import_connection(connection):
    """Observe imported rows on the retained writer, never its pinned export Reader."""
    from process.network_fhir_source_epoch_pin import _CopyConnections

    return connection.target if isinstance(connection, _CopyConnections) else connection


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
            count = await _copy_import_connection(connection).fetchval(f'SELECT count(*) FROM "{schema}"."{table}"')
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


def _retained_bounds_note(error, bounds_check=None):
    """Preserve the original four-operand note without any additional database read."""
    operands = retained_bound_operands(
        error,
        bounds_check or retention._assert_retained_bounds,
        relation_limit=len(epoch_capture._TABLES) + 2 * len(RELATION_NAMES),
    )
    return "retention_native_bounds " + json.dumps(operands, sort_keys=True) if operands is not None else None


def _retained_diagnostic_observers():
    """Wrap only existing test boundaries and carry closed notes across the FHIR wrapper."""
    diagnostic, failure_notes = RetainedWalDiagnostic(), []
    original_admission = retention._raw_copy_admission
    original_bounds = retention._assert_retained_bounds
    original_address = retention.prepare_retained_registry_address
    original_stamp = retention._stamp_prepared

    async def observe_admission(*args, **kwargs):
        adapter = await original_admission(*args, **kwargs)
        diagnostic.begin(adapter.wal_start)
        return adapter

    async def observe_bounds(*args, **kwargs):
        if diagnostic.phase == "readiness":
            diagnostic.enter("final_bounds")
        try:
            return await original_bounds(*args, **kwargs)
        except ValueError as error:
            note = _retained_bounds_note(error, original_bounds)
            if note is not None:
                operands = json.loads(note.split(" ", 1)[1])
                diagnostic.record(diagnostic.start_lsn, operands["wal_bytes"])
                failure_notes.extend(value for value in (note, diagnostic.note(operands)) if value is not None)
                try:
                    attribution = await retained_wal_attribution(args[0], diagnostic.start_lsn, operands["wal_bytes"])
                except Exception, asyncio.CancelledError:
                    attribution = None
                if attribution is not None:
                    failure_notes.append(attribution)
                for value in failure_notes:
                    error.add_note(value)
            raise

    async def observe_address(*args, **kwargs):
        diagnostic.enter("address_prepare")
        return await original_address(*args, **kwargs)

    async def observe_stamp(*args, **kwargs):
        diagnostic.enter("stamp")
        result = await original_stamp(*args, **kwargs)
        diagnostic.enter("readiness")
        return result

    return SimpleNamespace(
        diagnostic=diagnostic,
        failure_notes=failure_notes,
        observers={
            "_raw_copy_admission": observe_admission,
            "_assert_retained_bounds": observe_bounds,
            "prepare_retained_registry_address": observe_address,
            "_stamp_prepared": observe_stamp,
        },
    )


def _install_retained_diagnostic_observers(patch, admission, diagnostic_state):
    """Install only the four existing test-boundary observers in this scoped patch."""
    patch.setattr(admission, "_retained_wal_diagnostic", diagnostic_state.diagnostic, raising=False)
    for name, observer in diagnostic_state.observers.items():
        patch.setattr(retention, name, observer)


async def _prepare_with_retained_notes(prepared, factory, request, capture_id, roles, failure_notes):
    """Keep closed original-bound notes when admission hides its exception context."""
    try:
        return await retention.prepare_registry_cms_source_pair(
            prepared,
            factory,
            request,
            capture_id=capture_id,
            owner_role=roles[0],
            runtime_roles=tuple(sorted(roles[1:])),
        )
    except (ValueError, epoch_capture.FHIRSourceEpochError) as error:
        for note in failure_notes:
            if note not in getattr(error, "__notes__", ()):
                error.add_note(note)
        raise


async def _prepare_observed(prepared, factory, request, roles, capture_id, monkeypatch):
    """Observe real callbacks and registration while preserving actual admission checks."""
    source_sequence_state = await _source_evidence_sequence_state(prepared)
    previous_oid_by_relation = dict(prepared.nonprofile_admission._relations)
    growth_events, created_relations, binary_inputs = [], [], []
    address_inputs, address_phases = [], []
    original_growth = retention.RegistryCMSRawEpochAdmission.__call__
    original_copy = epoch_capture._copy_native_batch
    diagnostic_state = _retained_diagnostic_observers()

    async def observe_binary_copy(connection, query, arguments, schema, table, expected_count, **options):
        """Observe real COPY input sizes without replacing either driver operation."""
        proxy = BootstrapCopyConnection(connection)
        await original_copy(proxy, query, arguments, schema, table, expected_count, **options)
        assert len(proxy.imports) == 1
        assert "OFFSET" not in query and "ORDER BY" in query
        assert all(0 < byte_count <= options["byte_limit"] for byte_count in proxy.import_bytes)
        binary_inputs.extend(proxy.import_bytes)

    async def observe_growth(adapter, phase, schema, logical, oid):
        diagnostic_state.diagnostic.enter(
            "raw_create" if phase == "created" else "raw_index" if "index" in phase else "raw_copy"
        )
        row_count = await adapter.connection.fetchval(f'SELECT count(*) FROM "{schema}"."{logical}"')
        indexes = await adapter.connection.fetchval("SELECT count(*) FROM pg_index WHERE indrelid=$1", oid)
        growth_events.append((phase, logical, oid, row_count, indexes))
        await original_growth(adapter, phase, schema, logical, oid)

    with monkeypatch.context() as patch:
        _install_retained_diagnostic_observers(patch, prepared.nonprofile_admission, diagnostic_state)
        patch.setattr(retention.RegistryCMSRawEpochAdmission, "__call__", observe_growth)
        patch.setattr(
            NonprofileAdmission,
            "register_external_relation",
            _retained_creation_observer(prepared, capture_id, created_relations),
        )
        patch.setattr(epoch_capture, "_copy_native_batch", observe_binary_copy)
        patch.setattr(
            prepared_address,
            "_copy_native_batch",
            _address_copy_observer(prepared, created_relations, address_inputs, address_phases),
        )
        retained = await _prepare_with_retained_notes(
            prepared, factory, request, capture_id, roles, diagnostic_state.failure_notes
        )
    _assert_original_heap_growth(growth_events, created_relations, retained, prepared, previous_oid_by_relation)
    assert binary_inputs
    _assert_address_copy_counts(address_inputs, retained.address_receipt)
    assert len(address_phases) == 2 * len(address_inputs)
    for index, (table, count, _bytes) in enumerate(address_inputs):
        before, after = address_phases[index * 2 : index * 2 + 2]
        assert before[:2] == (table, "before_import") and after[:2] == (table, "after_import")
        assert after[2] - before[2] == count
    assert await _source_evidence_sequence_state(prepared) == source_sequence_state
    return retained


def _assert_address_copy_counts(address_inputs, receipt):
    """Match exact batch totals to the receipt checked against the complete source family."""
    expected_by_table = {entry.table_name: entry.row_count for entry in receipt.tables}
    assert set(expected_by_table) == set(prepared_address._COPY_KEYS)
    copied_by_table = dict.fromkeys(expected_by_table, 0)
    for table, count, _bytes in address_inputs:
        assert table in copied_by_table and count > 0
        copied_by_table[table] += count
    assert copied_by_table == expected_by_table
    assert {table for table, _count, _bytes in address_inputs} == {
        table for table, count in expected_by_table.items() if count > 0
    }


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
    default = support.fhir._pagination_checkpoint_row_mapping(default)
    state = await prepared.fhir.db.first("SELECT last_value,is_called FROM " + default["sequence"])
    assert state is not None
    return dict(default), dict(support.fhir._pagination_checkpoint_row_mapping(state))


def _assert_original_heap_growth(growth_events, created_relations, retained, prepared, previous_oid_by_relation):
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
    assert native_relations == {
        **previous_oid_by_relation,
        **{(schema, logical): oid for schema, logical, oid in created_relations},
    }


async def _reject_copy_batch_interruptions(prepared, factory, request, roles, capture_id, monkeypatch):
    """Actual completed COPY failures retire heaps and preserve an exact retry."""
    original = retention.RegistryCMSRawEpochAdmission.__call__
    previous_oid_by_relation = dict(prepared.nonprofile_admission._relations)
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
                    prepared,
                    factory,
                    request,
                    capture_id=capture_id,
                    owner_role=roles[0],
                    runtime_roles=tuple(sorted(roles[1:])),
                )
        assert len(completed_batches) == 1
        for schema in ("registry_cms_epoch_" + capture_id.hex, "entity_address_archive_" + capture_id.hex):
            assert await prepared.fhir.db.scalar("SELECT to_regnamespace(:schema)", schema=schema) is None
        assert prepared.nonprofile_admission._relations == previous_oid_by_relation


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
                    assert (
                        await _copy_import_connection(connection).fetchval(f'SELECT count(*) FROM "{schema}"."{table}"')
                        == count
                    )
                    completed_copies.append((schema, table))
                    raise interruption("synthetic completed address COPY interruption")

            options["import_admission"] = refuse_after
            await original_copy(connection, query, arguments, schema, table, count, **options)

        with monkeypatch.context() as patch:
            patch.setattr(prepared_address, "_copy_native_batch", refuse_copy)
            with pytest.raises(interruption):
                await retention.prepare_registry_cms_source_pair(
                    prepared,
                    factory,
                    request,
                    capture_id=capture_id,
                    owner_role=roles[0],
                    runtime_roles=tuple(sorted(roles[1:])),
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
        assert witness.source == source_capture
        assert witness.clone_receipt == retained.address_receipt
        assert set(dict(source_capture.relation_oids).values()).isdisjoint(
            dict(retained.address_ownership.relation_oids).values()
        )
        assert source_capture.schema_name != retained.address_ownership.schema_name
        assert source_capture.semantic_receipt.main_input_sha256 == retained.address_receipt.main_input_sha256
        driver = await address_capture.native_driver(session)
        schema = retained.address_ownership.schema_name
        await _assert_live_retained_ordinals(driver, prepared.address, source_capture, witness)
        await _assert_prepared_native_copy_key(driver, prepared.address)
        for logical, stage, oid in prepared.address.stage_oids:
            source_ref = f'"{prepared.address.db_schema}"."{stage}"'
            target_ref = f'"{schema}"."{logical}"'
            await prepared_address._require_prepared_copy_key(
                driver, source_ref, oid, prepared_address._COPY_KEYS[logical]
            )
            source_options = await _native_storage_options(driver, source_ref)
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


async def _assert_live_retained_ordinals(driver, address, source_capture, witness):
    """Compare physical source holes and packed clone columns with original live OIDs."""
    for logical, _stage, oid in address.stage_oids:
        columns = await driver.fetch(
            "SELECT attname,attnum FROM pg_attribute WHERE attrelid=$1 AND attnum>0 AND NOT attisdropped ORDER BY attnum",
            oid,
        )
        actual_ordinals = tuple(
            (column["attname"], column["attnum"], ordinal) for ordinal, column in enumerate(columns, 1)
        )
        assert dict(source_capture.ordinals)[logical] == actual_ordinals
        assert dict(witness.clone_ordinals)[logical] == tuple(
            (name, ordinal, ordinal) for name, _physical, ordinal in actual_ordinals
        )


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


async def _reject_nonoriginal_receipt_sessions(database, retained):
    """Neither fixture administrator nor a foreign session is the original publisher."""
    from process.provider_directory_generic_candidate import GenericCandidateError

    async with database.session_factory() as session, session.begin():
        with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
            await serving.require_registry_receipt_owner(session, "mrf")
        assert (
            await serving.read_registry_source_pair_receipt(
                session, "mrf", retained.recipe.source_pin.retained_epoch.epoch_id
            )
            is None
        )
    async with support.original_source_transaction(database, isolation="repeatable read"):
        async with database.session_factory() as foreign, foreign.begin():
            with pytest.raises(ValueError, match="original_session_required"):
                await serving.require_registry_receipt_owner(foreign, "mrf")


async def _reject_open_custody(database, retained, roles, predecessor):
    """Admin ACL corruption is visible to a fresh original Source publication snapshot."""
    schema = retained.address_ownership.schema_name
    async with database.engine.begin() as connection:
        await connection.execute(text(f'GRANT UPDATE ON {schema}.entity_address_unified TO "{roles[1]}"'))
    try:
        with pytest.raises(ValueError, match="privileges are not closed"):
            async with support.original_source_transaction(database, isolation="repeatable read") as session:
                receipt_id, payload = await _publish_stages(session, retained, predecessor)
                await retention.bind_prepared_registry_cms_source_pair(
                    session, retained, receipt_id=receipt_id, receipt_payload=payload
                )
    finally:
        async with database.engine.begin() as connection:
            await connection.execute(text(f'REVOKE UPDATE ON {schema}.entity_address_unified FROM "{roles[1]}"'))


async def _bind_rollback(database, retained, predecessor):
    """Native receipt append and pair binding remain one rollback boundary."""
    with pytest.raises(RuntimeError, match="synthetic bind rollback"):
        async with support.original_source_transaction(database, isolation="repeatable read") as session:
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
    async with support.original_source_transaction(database, isolation="repeatable read") as session:
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


async def _bind_native_retained_pair(database, retained, roles, predecessor, monkeypatch):
    """Keep receipt append and committed pair binding in the same real transaction."""
    await _reject_open_custody(database, retained, roles, predecessor)
    async with support.original_source_transaction(database, isolation="repeatable read") as session:
        with pytest.raises(ValueError, match="prepared_receipt_invalid"):
            await retention.bind_prepared_registry_cms_source_pair(
                session, retained, receipt_id=predecessor["receipt_id"], receipt_payload=predecessor["payload"]
            )
        receipt_id, publication_document = await _publish_stages(session, retained, predecessor)
        with monkeypatch.context() as patch:

            async def forbidden_scan(*_args, **_kwargs):
                raise AssertionError("full content scan in atomic binder")

            patch.setattr(epoch_capture, "_content", forbidden_scan)
            patch.setattr(address_capture, "capture_entity_address_archive_receipt", forbidden_scan)
            pair = await retention.bind_prepared_registry_cms_source_pair(
                session, retained, receipt_id=receipt_id, receipt_payload=publication_document
            )
    return pair


async def _notification_migration(connection, operation, *, schema):
    """Execute the actual frozen migration using Alembic's real operations context."""
    import importlib.util
    from pathlib import Path

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    path = Path(__file__).parents[1] / "alembic/versions/20261017010000_registry_cms_source_notifications.py"
    specification = importlib.util.spec_from_file_location("notification_migration", path)
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    migration._schema = lambda: schema

    def apply(sync_connection):
        with Operations.context(MigrationContext.configure(sync_connection)):
            getattr(migration, operation)()

    await connection.run_sync(apply)


async def _install_notification_inbox(database, retained, attempt, publisher_roles):
    """Run the actual migration and explicit operator plan before publication."""
    from process import registry_cms_source_notifications as notifications
    from process.network_cms_registry_address_capture import native_driver

    publisher = database._source_component.publisher
    schema = "cms_notify_control_" + uuid4().hex
    owner = "cms_notify_owner_" + uuid4().hex
    foreign = "cms_notify_foreign_" + uuid4().hex
    publisher_roles.extend((owner, foreign))
    store = notifications.RegistryCMSSourceNotificationStore(schema, owner, publisher, attempt)
    async with database.engine.begin() as connection:
        await connection.execute(text(f'CREATE ROLE "{owner}" NOLOGIN'))
        await connection.execute(text(f'CREATE ROLE "{foreign}" NOLOGIN'))
        await connection.execute(text(f'CREATE SCHEMA "{schema}" AUTHORIZATION "{owner}"'))
        await connection.execute(text(f'REVOKE ALL ON SCHEMA "{schema}" FROM PUBLIC'))
        for operation in ("upgrade", "downgrade", "upgrade"):
            await _notification_migration(connection, operation, schema=schema)
        assert not await connection.scalar(
            text("SELECT has_schema_privilege(CAST(:role AS name),:schema,'CREATE')"),
            {"role": publisher, "schema": schema},
        )
    async with database.session_factory() as session, session.begin():
        await session.execute(text("SET LOCAL lock_timeout='1s'"))
        await _verify_notification_operator_refusals(session, store, foreign)
        driver = await native_driver(session)
        statements = await notifications.plan_registry_cms_source_notification_grants(
            driver, control_schema=schema, owner_role=owner, publisher_role=publisher
        )
        for statement in statements:
            await session.execute(text(statement))
    return store


async def _verify_notification_operator_refusals(session, store, foreign_role):
    """Refuse unknown ACLs and created-at insertion without clearing either grant."""
    from process.network_cms_registry_address_capture import native_driver
    from process.registry_cms_source_notifications import plan_registry_cms_source_notification_grants

    driver = await native_driver(session)

    async def grant_plan():
        return await plan_registry_cms_source_notification_grants(
            driver,
            control_schema=store.control_schema,
            owner_role=store.owner_role,
            publisher_role=store.publisher_role,
        )

    await session.execute(
        text(f'GRANT SELECT ON {store.control_schema}.registry_cms_source_notification TO "{foreign_role}"')
    )
    with pytest.raises(ValueError, match="source_notification_invalid"):
        await grant_plan()
    assert await session.scalar(
        text(
            f"SELECT has_table_privilege(CAST(:role AS name),'{store.control_schema}.registry_cms_source_notification','SELECT')"
        ),
        {"role": foreign_role},
    )
    await session.execute(
        text(f'REVOKE SELECT ON {store.control_schema}.registry_cms_source_notification FROM "{foreign_role}"')
    )
    await session.execute(
        text(
            f'GRANT INSERT(created_at) ON {store.control_schema}.registry_cms_source_notification TO "{store.publisher_role}"'
        )
    )
    with pytest.raises(ValueError, match="source_notification_invalid"):
        await grant_plan()
    assert await session.scalar(
        text(
            f"SELECT has_column_privilege(CAST(:role AS name),'{store.control_schema}.registry_cms_source_notification','created_at','INSERT')"
        ),
        {"role": store.publisher_role},
    )
    await session.execute(
        text(
            f'REVOKE INSERT(created_at) ON {store.control_schema}.registry_cms_source_notification FROM "{store.publisher_role}"'
        )
    )


async def _refuse_elevated_notification_role(database, retained, predecessor, store):
    """Grant the forbidden parameter as admin before opening the genuine Source snapshot."""
    async with database.engine.begin() as connection:
        await connection.execute(text(f'GRANT SET ON PARAMETER session_replication_role TO "{store.publisher_role}"'))
    try:
        with pytest.raises(ValueError, match="source_notification_invalid"):
            async with support.original_source_transaction(database, isolation="repeatable read") as session:
                receipt_id, payload = await _publish_stages(session, retained, predecessor)
                await retention.bind_prepared_registry_cms_source_pair(
                    session, retained, receipt_id=receipt_id, receipt_payload=payload
                )
    finally:
        async with database.engine.begin() as connection:
            await connection.execute(
                text(f'REVOKE SET ON PARAMETER session_replication_role FROM "{store.publisher_role}"')
            )


def _notification_callback(original_bind, stores, recorded_notifications):
    async def notify_bound(session, prepared, *, receipt_id, receipt_payload):
        from process import registry_cms_source_notifications as notifications
        from process.provider_directory_generic_source_policy import source_publisher_body

        pair = await original_bind(session, prepared, receipt_id=receipt_id, receipt_payload=receipt_payload)
        store = stores[-1]
        assert store.owner_role != pair.owner_role
        before_actor = (await session.execute(text("SELECT pg_backend_pid(),pg_current_xact_id()::text"))).one()
        with pytest.raises(ValueError, match="source_notification_invalid"):
            await store.notify_receipt(session, receipt_id=receipt_id, pair=pair)
        async with source_publisher_body(session):
            assert await session.scalar(text("SELECT current_user")) == store.publisher_role
            assert (
                await session.execute(text("SELECT pg_backend_pid(),pg_current_xact_id()::text"))
            ).one() == before_actor
            notice = await store.notify_receipt(session, receipt_id=receipt_id, pair=pair)
            assert await store.notify_receipt(session, receipt_id=receipt_id, pair=pair) == notice
            assert notifications.decode_registry_cms_source_notification(notice.canonical_json, notice.sha256) == notice
            with pytest.raises(ValueError, match="source_notification_invalid"):
                await store.notify_receipt(session, receipt_id="0" * 64, pair=pair)
            foreign = replace(
                store, source_attempt=replace(store.source_attempt, attempt_id="source-run:" + uuid4().hex)
            )
            with pytest.raises(ValueError, match="source_notification_invalid"):
                await foreign.notify_receipt(session, receipt_id=receipt_id, pair=pair)
            assert (
                await session.scalar(
                    text(f"SELECT count(*) FROM {store.control_schema}.registry_cms_source_notification")
                )
                == 1
            )
        recorded_notifications.append(notice)
        return pair

    return notify_bound


async def _verify_committed_notification(database, pair, store, notice):
    async with database.session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await session.execute(text(f'SET LOCAL ROLE "{store.publisher_role}"'))
        loaded = await store.read(session, source_schema="mrf", receipt_id=pair.proof.serving_receipt_id)
        assert loaded == notice and loaded.as_dict()["capture_id"] == str(
            pair.recipe.source_pin.retained_epoch.epoch_id
        )
    async with database.session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await session.execute(text(f'SET LOCAL ROLE "{store.publisher_role}"'))
        with pytest.raises(ValueError, match="source_notification_invalid"):
            await store.notify_receipt(session, receipt_id=pair.proof.serving_receipt_id, pair=pair)


async def _verify_notification_rc_refusal(database, retained, predecessor, store):
    from process.network_custom_address_source import NetworkCustomAddressSourceError

    with pytest.raises(RuntimeError, match="synthetic RC rollback"):
        async with support.original_source_transaction(database, isolation="read committed") as session:
            receipt_id, payload = await _publish_stages(session, retained, predecessor)
            with pytest.raises(NetworkCustomAddressSourceError, match="repeatable-read transaction"):
                await retention.bind_prepared_registry_cms_source_pair(
                    session, retained, receipt_id=receipt_id, receipt_payload=payload
                )
            raise RuntimeError("synthetic RC rollback")
    async with database.session_factory() as session:
        assert (await serving.read_current_receipt(session, "mrf"))["receipt_id"] == predecessor["receipt_id"]
        assert (
            await session.scalar(text(f"SELECT count(*) FROM {store.control_schema}.registry_cms_source_notification"))
            == 0
        )


async def _verify_notification_populated_downgrade(database, store):
    async with database.engine.connect() as connection:
        before = (
            await connection.execute(
                text(
                    f"SELECT notification_json,notification_sha256 FROM {store.control_schema}.registry_cms_source_notification"
                )
            )
        ).all()
    with pytest.raises(RuntimeError, match="retained-data migration"):
        async with database.engine.begin() as connection:
            await _notification_migration(connection, "downgrade", schema=store.control_schema)
    async with database.engine.connect() as connection:
        assert (
            await connection.execute(
                text(
                    f"SELECT notification_json,notification_sha256 FROM {store.control_schema}.registry_cms_source_notification"
                )
            )
        ).all() == before


@pytest.mark.asyncio
@pytest.mark.parametrize("copy_bounds", [(1, 64 * 1024**2), (32, 64 * 1024)])
async def test_native_signed_retention_budget_refuses_before_copy(monkeypatch, tmp_path, copy_bounds):
    """Keep both original COPY profiles' actual signed refusal and predecessor rollback."""
    from tests.cms_registry_native_producer_postgres_support import verify_native_budget_refusal

    await verify_native_budget_refusal(monkeypatch, tmp_path, copy_bounds)


async def _verify_native_notification_publication(state, monkeypatch):
    from tests.cms_registry_native_producer_postgres_support import (
        reject_elevated_notification_role,
        reject_unscoped_notification,
    )

    await _verify_notification_populated_downgrade(state.database, state.store)
    await reject_elevated_notification_role(state)
    await reject_unscoped_notification(state)
    await _verify_genuine_retained_state(state, monkeypatch)
    await _verify_committed_notification(state.database, state.pair, state.store, state.notices[-1])


async def verify_committed_source_publication(monkeypatch, tmp_path):
    """Exercise genuine publication and retained replay across a guarded receipt refresh."""
    from tests.cms_registry_native_producer_postgres_support import (
        genuine_prepared_publication,
        refresh_native_receipt_head,
    )

    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "mrf")
    async with genuine_prepared_publication(monkeypatch, tmp_path, (32, 64 * 1024)) as state:
        assert len(state.notices) == 1
        await _verify_native_notification_publication(state, monkeypatch)
        await refresh_native_receipt_head(state, monkeypatch)
        await _verify_genuine_retained_state(state, monkeypatch)
        await _verify_committed_notification(state.database, state.pair, state.store, state.notices[-1])


@pytest.mark.asyncio
async def test_native_source_notification_rollback_preserves_incumbent(monkeypatch, tmp_path):
    """Keep the predecessor and charged capacity after the real late notification rollback."""
    from tests.cms_registry_native_producer_postgres_support import verify_native_notification_rollback

    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", "mrf")
    await verify_native_notification_rollback(monkeypatch, tmp_path, (32, 64 * 1024))
