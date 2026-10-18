# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual native publication using the component's issued Source actors and pools."""

import asyncio
import json
from contextlib import asynccontextmanager
from functools import partial
from uuid import uuid4

from sqlalchemy import text

from process import provider_directory_cms_native_inputs as inputs
from process import provider_directory_cms_serving as serving
from process import provider_directory_generic_source_policy as source
from process.live_progress import reset_live_progress_context, set_live_progress_context
from process.provider_directory_cms_source_runtime import CMSRegistrySourceInvocation, CMSRegistrySourceRuntime
from tests import cms_registry_complete_publication_postgres_support as native
from tests.test_network_cms_registry_complete_publication_postgres import _paired_native_execution


async def _issued_invocation(database, execution, job):
    """Authenticate the exact native control attempt before issuing its trusted handles."""
    component, task = database._source_component, asyncio.current_task()
    factory = component.factory
    assert source._FACTORY.get() is factory
    factory._require_current_policy()
    assert job.source_session_factory is await factory.publication_session_factory()
    assert job.retention_session_factory.kw["bind"].url.username == component.retained_publisher

    def require_original(arguments):
        assert asyncio.current_task() is task and source._FACTORY.get() is factory
        factory._require_current_policy()
        assert arguments["execution"] is execution and arguments["context"] is context_by_field
        assert context_by_field["control_run_id"] == job.source_attempt.run_id
        assert context_by_field["cms_registry_source_runtime"] is runtime
        assert arguments["task"] == {"run_id": job.source_attempt.run_id}

    async def authenticate(**arguments):
        require_original(arguments)
        async with source.source_transaction(component.database, isolation="repeatable read") as session:
            before = await inputs._capture_bound_fhir(session, component.policy.schema)
            attempt = (
                await session.execute(
                    text(
                        "SELECT run_id,importer,status,progress->>'attempt_id',progress->>'attempt_started_at' "
                        "FROM mrf.import_run WHERE run_id=:run"
                    ),
                    {"run": job.source_attempt.run_id},
                )
            ).one()
            assert tuple(attempt) == (
                job.source_attempt.run_id,
                "provider-directory-fhir",
                "running",
                job.source_attempt.attempt_id,
                job.source_attempt.attempt_started_at,
            )
            assert await inputs._capture_bound_fhir(session, component.policy.schema) == before
            require_original(arguments)
        require_original(arguments)
        return job

    runtime = CMSRegistrySourceRuntime(authenticate)
    context_by_field = {"cms_registry_source_runtime": runtime, "control_run_id": job.source_attempt.run_id}
    return CMSRegistrySourceInvocation.from_context(context_by_field, {"run_id": job.source_attempt.run_id})


async def publish_native_execution(database, execution, monkeypatch, *, job=None, batch_size=100_000):
    """Use real preflight, preparation, publication and committed receipt readback."""
    factory = database._source_component.factory
    assert source._FACTORY.get() is factory
    factory._require_current_policy()
    run_id = job.source_attempt.run_id if job is not None else "run_" + uuid4().hex
    progress = (
        {}
        if job is None
        else {
            "attempt_id": job.source_attempt.attempt_id,
            "attempt_started_at": job.source_attempt.attempt_started_at,
        }
    )
    async with database.session_factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO mrf.import_run(run_id,engine,importer,status,params,metrics,progress) "
                "VALUES(:run,'test-engine','provider-directory-fhir','running','{}','{}',CAST(:progress AS jsonb))"
            ),
            {"run": run_id, "progress": json.dumps(progress)},
        )
    policy = None if job is None else job.retention_request.policy(job.capture_id, job.owner_role, job.runtime_roles)
    execution = await _paired_native_execution(monkeypatch, execution, retention_policy=policy, batch_size=batch_size)
    invocation = None if job is None else await _issued_invocation(database, execution, job)
    native.observe_geometry(monkeypatch, database)
    token = (
        set_live_progress_context(run_id=run_id, importer="provider-directory-fhir", status="running", **progress)
        if job is not None
        else None
    )
    try:
        return await serving.publish_current_attested_profile(
            native.source.fhir,
            execution=execution,
            run_id=run_id,
            control_run_id=run_id,
            metrics={},
            registry_source_invocation=invocation,
        )
    finally:
        if token is not None:
            reset_live_progress_context(token)


def install_native_office_seed(monkeypatch):
    """Use the same exact synthetic offices for acquisition and the native Doctors input."""
    original = native.source._resource_rows

    def resources(revision):
        resources_by_kind = original(revision)
        for location in resources_by_kind["Location"]:
            address = location["address"]
            for field, value in (("state", "CA"), ("postalCode", "90210"), ("country", "US")):
                address.setdefault(field, value)
        return resources_by_kind

    async def seed(database):
        resources_by_kind = resources("first")
        npi = int(resources_by_kind["Practitioner"][0]["identifier"][0]["value"])
        async with database.session_factory() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO mrf.npi(npi,entity_type_code,provider_first_name,provider_last_name) "
                    "VALUES(:npi,1,'Example','Clinician')"
                ),
                {"npi": npi},
            )
            for index, location in enumerate(resources_by_kind["Location"], 1):
                address = location["address"]
                lines = address.get("line", [])
                await session.execute(
                    text(
                        "INSERT INTO mrf.doctor_clinician_address(npi,address_checksum,address_line1,address_line2,"
                        "city,state,zip_code,provider_type,address_key,updated_at) VALUES(:npi,:checksum,"
                        "CAST(:first AS text),CAST(:second AS text),CAST(:city AS text),CAST(:state AS text),"
                        "CAST(:postal AS text),'Internal Medicine',mrf.addr_key_from_identity_v1("
                        "mrf.addr_identity_key_v1(CAST(:first AS text),CAST(:second AS text),CAST(:city AS text),"
                        "CAST(:state AS text),CAST(:postal AS text),CAST(:country AS text))),now())"
                    ),
                    dict(
                        npi=npi,
                        checksum=index,
                        first=lines[0],
                        second=lines[1] if len(lines) > 1 else None,
                        city=address.get("city"),
                        state=address["state"],
                        postal=address["postalCode"],
                        country=address["country"],
                    ),
                )

    monkeypatch.setattr(native.source, "_resource_rows", resources)
    monkeypatch.setattr(native, "_seed_native_doctors", seed)


def retention_request(acquired, execution, *, extra_data_bytes=32 * 1024**2):
    """Use the admitted candidate's actual OIDs, hashes and signed selection proof."""
    from process.network_registry_cms_prepared_pair import RegistryCMSRetentionRequest
    from tests.test_network_registry_cms_prepared_pair_postgres import _prepublication_inputs

    proof = _prepublication_inputs(acquired, execution.attestation.proof_id)
    return RegistryCMSRetentionRequest(
        proof.source_pin,
        proof.binding_coordinates,
        proof.selection_proof_id,
        proof.expected_admission_sha256,
        proof.expected_metadata_sha256,
        extra_data_bytes,
        32 * 1024**2,
    )


async def issued_job(database, acquired, execution, roles, sessions, store, *, extra_data_bytes=32 * 1024**2):
    """Issue the existing typed job from the actual original Source and retained pools."""
    from process.provider_directory_cms_source_runtime import BoundCMSRegistrySourceJob

    return BoundCMSRegistrySourceJob(
        retention_request(acquired, execution, extra_data_bytes=extra_data_bytes),
        uuid4(),
        roles[0],
        tuple(sorted(roles[1:])),
        await database._source_component.factory.publication_session_factory(),
        store.notify_receipt,
        store.source_attempt,
        sessions,
        store.read,
    )


async def acquire_native_source(database, monkeypatch, tmp_path, *, revision, run_id):
    """Admit a fresh closed source release through the original protected acquisition."""
    tmp_path.mkdir(exist_ok=True)
    directory, acquired = native.source.retained_release(tmp_path, revision=revision)
    with native.source.release_probe_client(directory) as client:
        return await native.source.run_protected_admission(database, monkeypatch, run_id, directory, acquired, client)


async def _observe_preparation(module, original, prepared, factory, request, options, monkeypatch, state):
    """Observe original admitted COPY and custody without replacing an authority object."""
    database = state.database

    with monkeypatch.context() as patch:
        patch.setattr(module.retention, "prepare_registry_cms_source_pair", partial(original, **options))
        retained = await module._prepare_observed(prepared, factory, request, state.roles, options["capture_id"], patch)
    await module._assert_retained_raw_parity(database, retained)
    await module._assert_retained_address_parity(database, retained, prepared)
    await _reject_nonoriginal_receipt_sessions(module, database, retained)
    await _reject_prepared_custody(module, database, retained, state)
    state.prepared, state.retained = prepared, retained
    return retained


async def _prepared_source_identity(module, database, prepared):
    """Read the exact original SOURCE fence, stage identities, content and sequence."""
    schema = prepared.address.db_schema
    async with source.source_transaction(database._source_component.database, isolation="repeatable read") as session:
        source_fence = await inputs._capture_bound_fhir(session, schema)
    async with database.session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        stage_receipt = await module.prepared_address.capture_entity_address_stage_integrity_receipt(
            session,
            schema_name=schema,
            stage_table_names={logical: stage for logical, stage, _oid in prepared.address.stage_oids},
        )
        observed_oids = tuple(
            [
                (
                    logical,
                    await session.scalar(
                        text("SELECT to_regclass(:relation)::oid"), {"relation": schema + "." + stage}
                    ),
                )
                for logical, stage, _oid in prepared.address.stage_oids
            ]
        )
    assert observed_oids == tuple((logical, oid) for logical, _stage, oid in prepared.address.stage_oids)
    return source_fence, observed_oids, stage_receipt, await module._source_evidence_sequence_state(prepared)


async def _observe_preparation_failures(module, original, prepared, factory, request, options, monkeypatch, state):
    """Run the selected independent failure probes, then stop before any successful capture."""
    before = await _prepared_source_identity(module, state.database, prepared)
    with monkeypatch.context() as patch:
        patch.setattr(module.retention, "prepare_registry_cms_source_pair", partial(original, **options))
        if state.address_copy_interruptions:
            await module._reject_address_copy_interruptions(
                prepared, factory, request, state.roles, options["capture_id"], patch
            )
        else:
            await module._reject_preparation(
                state.database, prepared, factory, request, state.roles, options["capture_id"], patch
            )
            await module._reject_raw_oid(
                state.database, prepared, factory, request, state.roles, options["capture_id"], patch
            )
            await module._reject_copy_batch_interruptions(
                prepared, factory, request, state.roles, options["capture_id"], patch
            )
    assert await _prepared_source_identity(module, state.database, prepared) == before
    state.preparation_failures_verified = True
    raise RuntimeError("synthetic preparation probes complete")


def _notification_sender(module, store, state, *, fail):
    async def notify(session, *, receipt_id, pair):
        from dataclasses import replace

        import pytest

        from process import registry_cms_source_notifications as notifications

        before = (await session.execute(text("SELECT current_user,pg_backend_pid(),pg_current_xact_id()::text"))).one()
        assert before[0] == store.publisher_role
        notice = await store.notify_receipt(session, receipt_id=receipt_id, pair=pair)
        assert await store.notify_receipt(session, receipt_id=receipt_id, pair=pair) == notice
        assert notifications.decode_registry_cms_source_notification(notice.canonical_json, notice.sha256) == notice
        with pytest.raises(ValueError, match="source_notification_invalid"):
            await store.notify_receipt(session, receipt_id="0" * 64, pair=pair)
        foreign = replace(
            store,
            source_attempt=replace(store.source_attempt, attempt_id=store.source_attempt.run_id + ":" + uuid4().hex),
        )
        with pytest.raises(ValueError, match="cms_registry_source_publication_invalid"):
            await foreign.notify_receipt(session, receipt_id=receipt_id, pair=pair)
        assert (
            await session.execute(text("SELECT current_user,pg_backend_pid(),pg_current_xact_id()::text"))
        ).one() == before
        assert (
            await session.scalar(text(f"SELECT count(*) FROM {store.control_schema}.registry_cms_source_notification"))
            == 1
        )
        state.notices.append(notice)
        state.pair = pair
        if fail:
            state.rollback_consumptions = await _capacity_consumptions(state.database)
            raise RuntimeError("synthetic bind rollback")

    return notify


async def _publish_successor(
    module, database, execution, sessions, state, monkeypatch, *, fail=False, preparation_observer=None
):
    from dataclasses import replace

    from process import network_registry_cms_prepared_pair as retention

    job = await issued_job(database, state.acquired, execution, state.roles, sessions, state.store)
    state.captures.append(job.capture_id)
    job = replace(job, notify_receipt=_notification_sender(module, state.store, state, fail=fail))
    original = retention.prepare_registry_cms_source_pair
    original_bind = retention.bind_prepared_registry_cms_source_pair

    async def observe(prepared, factory, request, **options):
        retained = await preparation_observer(module, original, prepared, factory, request, options, monkeypatch, state)
        patch.setattr(
            retention,
            "bind_prepared_registry_cms_source_pair",
            _original_binder_observer(module, original_bind, state, patch),
        )
        return retained

    with monkeypatch.context() as patch:
        if preparation_observer is not None:
            patch.setattr(retention, "prepare_registry_cms_source_pair", observe)
        await publish_native_execution(database, execution, patch, job=job, batch_size=state.copy_bounds[0])
    return job


async def _capacity_consumptions(database):
    async with database.session_factory() as session:
        return (
            (
                await session.execute(
                    text(
                        "SELECT to_jsonb(c) FROM mrf.provider_directory_profile_capacity_lease_consumption c ORDER BY attestation_id"
                    )
                )
            )
            .scalars()
            .all()
        )


async def _new_notification_store(module, database, state):
    import datetime

    from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt

    run_id = "run_" + uuid4().hex
    attempt = RegistryCMSSourceAttempt(
        run_id, run_id + ":" + uuid4().hex, datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
    state.store = await module._install_notification_inbox(database, None, attempt, state.notification_roles)
    state.notification_schemas.append(state.store.control_schema)


async def _assert_failed_publication(database, state, predecessor, *, prepared_captures=False):
    """Separate common rollback from independently committed retained preparation."""
    from process import provider_directory_cms_serving_receipt as receipts

    async with database.session_factory() as session:
        assert (await receipts.read_current_receipt(session, "mrf"))["receipt_id"] == predecessor["receipt_id"]
        assert (
            await session.scalar(
                text(f"SELECT count(*) FROM {state.store.control_schema}.registry_cms_source_notification")
            )
            == 0
        )
        for capture in state.captures[1:]:
            for prefix in ("registry_cms_epoch_", "entity_address_archive_"):
                namespace = await session.scalar(
                    text("SELECT to_regnamespace(:schema)"), {"schema": prefix + capture.hex}
                )
                assert (namespace is not None) is prepared_captures
            assert await native.serving_receipt.read_registry_source_pair_receipt(session, "mrf", capture) is None


async def _record_failed_fixture_attempt(state):
    """Authorize only this fixture's exact failed attempt after its invocation returned."""
    assert state.publication_returned is True
    assert state.publication_task is asyncio.current_task()
    assert state.retention_sessions.kw["bind"].pool.checkedout() == 0
    attempt = state.store.source_attempt
    async with state.database.session_factory() as session, session.begin():
        recorded = await session.scalar(
            text(
                "UPDATE mrf.import_run SET status='failed',finished_at=clock_timestamp() "
                "WHERE run_id=:run AND importer='provider-directory-fhir' AND status='running' "
                "AND progress->>'attempt_id'=:attempt AND progress->>'attempt_started_at'=:started RETURNING run_id"
            ),
            {"run": attempt.run_id, "attempt": attempt.attempt_id, "started": attempt.attempt_started_at},
        )
        assert recorded == attempt.run_id


async def _cleanup_failed_fixture_capture(state):
    """Apply existing native cleanup under this stopped fixture attempt's terminal authority."""
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from process import network_registry_cms_prepared_cleanup as cleanup
    from process.network_registry_cms_capture_lock import registry_cms_capture_transaction
    from process.network_registry_cms_prepared_pair import PreparedRegistryCMSSourcePair

    await _record_failed_fixture_attempt(state)
    capture = state.captures[-1]
    runtime_roles = tuple(sorted(state.roles[1:]))
    engine = create_async_engine(state.database.engine.url, pool_size=1, max_overflow=0)
    try:
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with registry_cms_capture_transaction(sessions, capture_id=capture) as session:
            assert await session.scalar(text("SELECT session_user")) == state.database.engine.url.username
            prepared = await cleanup.probe_registry_cms_source_capture(
                session, capture_id=capture, owner_role=state.roles[0], runtime_roles=runtime_roles
            )
            assert type(prepared) is PreparedRegistryCMSSourcePair
            assert prepared.source_attempt == state.store.source_attempt
            assert prepared.recipe == state.pair.recipe
            assert await cleanup.cleanup_prepared_registry_cms_source_pair(session, prepared) == "removed"
            assert (
                await cleanup.probe_registry_cms_source_capture(
                    session, capture_id=capture, owner_role=state.roles[0], runtime_roles=runtime_roles
                )
                is None
            )
    finally:
        await engine.dispose()


async def refresh_native_receipt_head(state, monkeypatch):
    """Refresh native receipt authority without claiming another signed full build."""
    from process import entity_address_result_generation as addresses
    from process import provider_directory_cms_serving_receipt as receipts

    database = state.database
    async with native.source.original_source_transaction(database, isolation="read committed") as session:
        predecessor = await receipts.read_current_receipt(session, "mrf")
        assert predecessor["receipt_id"] == state.pair.proof.serving_receipt_id
        await addresses.publish_local_entity_address_generation(database._source_component.database, schema_name="mrf")
        snapshot = await receipts.capture_native_dependencies(session, "mrf")
        payload_by_field = {
            **predecessor["payload"],
            "predecessor_receipt_id": predecessor["receipt_id"],
            "expected_incumbent": {
                name: predecessor["payload"]["cms"][name] for name in native.serving_receipt._PIN_FIELDS
            },
            **snapshot,
        }
        for name in ("profile", "doctors", "overlay_oid", "alias_generation"):
            assert payload_by_field[name] == predecessor["payload"][name]
        receipt_id = await receipts.append_serving_receipt(session, "mrf", payload_by_field)

    async with database.session_factory() as session:
        assert (await receipts.read_current_receipt(session, "mrf"))["receipt_id"] == receipt_id
        assert receipt_id != state.pair.proof.serving_receipt_id


async def _cleanup_producer(database, state, engine):
    await engine.dispose()
    async with database.engine.begin() as connection:
        for capture in state.captures:
            for prefix in ("registry_cms_epoch_", "entity_address_archive_"):
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{prefix + capture.hex}" CASCADE'))
        for schema in state.notification_schemas:
            await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        for role in (*state.notification_roles, state.roles[1]):
            await connection.execute(text(f'DROP OWNED BY "{role}" CASCADE'))
            await connection.execute(text(f'DROP ROLE "{role}"'))
            assert not await connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:role)"), {"role": role}
            )


async def _publish_budget_refusal(module, database, acquired, execution, sessions, state, monkeypatch):
    import pytest

    from process import network_fhir_source_epoch as epochs
    from process import network_registry_cms_prepared_pair as retention

    job = await issued_job(database, acquired, execution, state.roles, sessions, state.store, extra_data_bytes=1)
    state.captures.append(job.capture_id)
    events = []
    original = retention.RegistryCMSRawEpochAdmission.__call__

    async def observe(adapter, phase, schema, logical, oid):
        events.append((phase, await adapter.connection.fetchval(f'SELECT count(*) FROM "{schema}"."{logical}"')))
        await original(adapter, phase, schema, logical, oid)

    with monkeypatch.context() as patch:
        patch.setattr(retention.RegistryCMSRawEpochAdmission, "__call__", observe)
        with pytest.raises(epochs.FHIRSourceEpochError, match="copy_admission_refused"):
            await publish_native_execution(database, execution, patch, job=job, batch_size=state.copy_bounds[0])
    assert events == [("created", 0)]


def _historical_profile_values(execution, contexts, targets):
    """Describe the seeded legacy incumbent with current canonical lineage fields."""
    fhir, attestation = native.source.fhir, execution.attestation
    vector = fhir._provider_directory_profile_source_vector(
        (pair["source_id"], pair["dataset_id"]) for pair in attestation.pairs
    )
    context_vector = fhir._provider_directory_profile_source_context_vector(contexts)
    return {
        "operation": "publish",
        "control_generation": execution.generation,
        "generation_id": "pdprofile_" + uuid4().hex,
        "selection_proof_id": attestation.proof_id,
        "authority_revision": attestation.authority_revision,
        "profile_schema_version": attestation.profile_schema_version,
        "profile_strategy_version": attestation.profile_strategy_version,
        "source_vector_hash": fhir._provider_directory_profile_source_vector_hash(vector),
        "source_vector_json": json.dumps(fhir._provider_directory_profile_source_vector_json(vector)),
        "source_context_vector_hash": fhir._provider_directory_profile_source_context_vector_hash(context_vector),
        "source_context_vector_json": json.dumps(
            fhir._provider_directory_profile_source_context_vector_json(context_vector)
        ),
        "executable_plan_hash": fhir._identity_hash({"fixture": "legacy-profile", "selection": attestation.proof_id}),
        "profile_as_of": attestation.desired_profile_as_of,
        "published_at": native.datetime.datetime.now(native.datetime.timezone.utc) - native.datetime.timedelta(days=1),
        **targets,
    }


async def _seed_historical_profile(session, execution, contexts):
    """Insert legacy serving metadata without claiming a current capacity admission."""
    fhir = native.source.fhir
    target_by_field = (
        (
            await session.execute(
                text(
                    "SELECT 'mrf.provider_directory_profile'::regclass::oid::bigint AS profile_target_oid,"
                    "'mrf.provider_directory_profile_evidence'::regclass::oid::bigint AS evidence_target_oid,"
                    "(SELECT count(*) FROM mrf.provider_directory_profile) AS profile_rows,"
                    "(SELECT count(*) FROM mrf.provider_directory_profile_evidence) AS evidence_rows"
                )
            )
        )
        .mappings()
        .one()
    )
    profile_by_field = _historical_profile_values(execution, contexts, target_by_field)
    assert profile_by_field["profile_rows"] == profile_by_field["evidence_rows"] == 0
    await session.execute(
        text(
            fhir._INSERT_PROFILE_ADOPTION_SQL.replace(
                "__SERVING_GENERATION__", '"mrf"."provider_directory_profile_serving_generation"'
            )
        ),
        profile_by_field,
    )
    serving_by_field = (
        (await session.execute(text("SELECT * FROM mrf.provider_directory_profile_serving_generation")))
        .mappings()
        .one()
    )
    state = fhir._profile_serving_state_from_row(serving_by_field)
    assert (
        state.capacity_geometry_status == "legacy_unavailable"
        and state.generation_id == profile_by_field["generation_id"]
    )


def _historical_office_rows(doctors):
    """Retain the actual two Doctor offices with their complete canonical identities."""
    assert len(doctors) == 2
    offices = []
    canonicalizer = native.address_canon
    for doctor in doctors:
        raw_address = (
            doctor["address_line1"],
            doctor["address_line2"],
            doctor["city"],
            doctor["state"],
            doctor["zip_code"],
            "US",
        )
        identity = canonicalizer.identity_key_v1(*raw_address)
        assert identity is not None and identity.endswith("|street")
        address_key = canonicalizer.key_from_identity(identity)
        assert doctor["address_key"] == address_key
        offices.append(
            dict(
                location_key=doctor["location_key"],
                entity_id=str(doctor["npi"]),
                npi=doctor["npi"],
                checksum=doctor["address_checksum"],
                first_line=raw_address[0],
                second_line=raw_address[1],
                city_name=raw_address[2],
                state_name=raw_address[3],
                postal_code=raw_address[4],
                country_code=raw_address[5],
                address_key=address_key,
                premise_key=canonicalizer.key_from_identity(canonicalizer.premise_identity_key_v1(*raw_address)),
                zip5=canonicalizer.zip5_norm(raw_address[4]),
                state_code=canonicalizer.state_code(raw_address[3]),
                city_norm=canonicalizer.city_norm(raw_address[2]),
            )
        )
    assert len({office["address_key"] for office in offices}) == 2
    return offices


async def _seed_historical_offices(session):
    unified = native.entity_address_unified
    location_key = unified._location_key_expr(
        dict(
            entity_type="'npi'::text",
            entity_id="npi::text",
            inferred_npi="NULL::bigint",
            address_role_id=unified._address_role_id_expr("'practice'"),
            row_origin="'base'::text",
            source_id=unified._source_id_expr("'cms_doctors'"),
            source_record_id="('cms_doctors:'||npi::text||':'||address_checksum::text)",
            zip5=unified._zip5_norm_expr("zip_code"),
            state_code=unified._state_norm_expr("state"),
            city_norm=unified._alnum_norm_expr("city"),
        )
    )
    doctors = (
        (
            await session.execute(
                text(
                    f"SELECT {location_key} AS location_key,"
                    "npi,address_checksum,address_line1,address_line2,city,state,zip_code,address_key "
                    "FROM mrf.doctor_clinician_address ORDER BY npi,address_checksum"
                )
            )
        )
        .mappings()
        .all()
    )
    await session.execute(
        text(
            "INSERT INTO mrf.entity_address_unified "
            "(location_key,entity_type,entity_id,npi,checksum,type,first_line,second_line,city_name,state_name,postal_code,country_code,"
            "address_key,address_precision,premise_key,zip5,state_code,city_norm,"
            "plans_network_array,procedures_array,medications_array,canonical_network_ids) "
            "VALUES(:location_key,'npi',:entity_id,:npi,:checksum,'practice',:first_line,:second_line,:city_name,"
            ":state_name,:postal_code,:country_code,:address_key,'street',:premise_key,:zip5,:state_code,:city_norm,"
            "'{}','{}','{}','{}')"
        ),
        _historical_office_rows(doctors),
    )


async def _seed_historical_publication(database, acquired, execution, contexts):
    """Seed a legacy predecessor under enabled guards; its build is not exercised here."""
    from process import provider_directory_cms_publication as publication

    handoff = acquired["registry_source_admission"]
    candidate_proof_by_field = {
        **acquired["cms_serving_candidate"]["desired_cms_dataset"],
        "release_id": handoff["release_id"],
        "proof_version": 2,
    }
    assert database.engine is database._source_component.admin_engine
    async with database.transaction() as session:
        assert (
            await session.scalar(text("SELECT count(*) FROM mrf.provider_directory_profile_capacity_lease_consumption"))
            == 0
        )
        await _seed_historical_offices(session)
        await _seed_historical_profile(session, execution, contexts)
        assert await session.scalar(text("SELECT count(*) FROM mrf.provider_directory_address_overlay")) == 0
        await session.execute(
            text(
                "INSERT INTO mrf.address_alias_artifact_state_v1(artifact_name,generation,updated_at) "
                "SELECT 'provider_directory_address_overlay',generation,now() FROM mrf.address_alias_state_v1 WHERE singleton "
                "ON CONFLICT(artifact_name) DO UPDATE SET generation=EXCLUDED.generation,updated_at=EXCLUDED.updated_at"
            )
        )
        promoted = await session.execute(
            text(
                "UPDATE mrf.provider_directory_endpoint_dataset SET status='published',is_current=true,published_at=now() "
                "WHERE dataset_id=:dataset_id AND endpoint_id=:endpoint_id AND dataset_hash=:dataset_hash AND status='validated' AND NOT is_current"
            ),
            candidate_proof_by_field,
        )
        assert promoted.rowcount == 1
        await session.execute(
            text(
                "INSERT INTO mrf.provider_directory_cms_serving_coverage "
                "(dataset_id,release_id,dataset_hash,proof_version,published_at,created_at) "
                "SELECT dataset_id,:release_id,dataset_hash,2,published_at,now() FROM mrf.provider_directory_endpoint_dataset WHERE dataset_id=:dataset_id"
            ),
            candidate_proof_by_field,
        )
        await native.addresses.publish_local_entity_address_generation(database, schema_name="mrf")
        snapshot = await native.serving_receipt.capture_native_dependencies(session, "mrf")
        receipt_payload_by_field = publication._receipt_payload(execution, candidate_proof_by_field, None, snapshot)
        await native.serving_receipt.append_serving_receipt(session, "mrf", receipt_payload_by_field)


async def _seed_historical_pair(module, database, acquired, state, monkeypatch):
    async with source.bind_generic_source_transaction_factory(database._source_component.factory):
        execution = await native.attested_execution(database, acquired, monkeypatch)
        async with source.source_transaction(database._source_component.database):
            _selected, _retained, contexts = await native.source.fhir._provider_directory_profile_scope_source_ids(
                "mrf", {pair["source_id"] for pair in execution.attestation.pairs}
            )
        await _seed_historical_publication(database, acquired, execution, contexts)
    proof, _payload = await native.committed_publication_proof(database, acquired)
    await module._reject_historical_capture_during_source(database, proof, state.roles)
    capture = uuid4()
    state.captures.append(capture)
    state.original_pair = await module.capture_registry_cms_source_pair(
        database.session_factory,
        proof,
        capture_id=capture,
        owner_role=state.roles[0],
        runtime_roles=tuple(sorted(state.roles[1:])),
        source_copy=module._bounded_source_copy(),
    )


async def _successor_execution(database, acquired, state, monkeypatch):
    from process import provider_directory_cms_serving_receipt as receipts

    async with database.session_factory() as session:
        state.predecessor = await receipts.read_current_receipt(session, "mrf")
    return await native.attested_execution(database, acquired, monkeypatch, generation=2)


async def _publish_successor_pair(
    module, database, acquired, sessions, state, monkeypatch, *, preparation_observer=None
):
    async with source.bind_generic_source_transaction_factory(database._source_component.factory):
        execution = await _successor_execution(database, acquired, state, monkeypatch)
        await _new_notification_store(module, database, state)
        await _publish_successor(
            module,
            database,
            execution,
            sessions,
            state,
            monkeypatch,
            preparation_observer=preparation_observer,
        )
    assert len(state.notices) == 1 and state.notices[-1].pair == state.pair


async def verify_native_preparation_failures(monkeypatch, tmp_path, copy_bounds, *, address_copy=False):
    """Give each interruption proof fresh Source custody and its own real signed preparation."""
    import pytest

    from tests import test_network_registry_cms_prepared_pair_postgres as module

    async with _native_incumbent(monkeypatch, tmp_path, copy_bounds) as state:
        state.address_copy_interruptions = address_copy
        database = state.database
        async with source.bind_generic_source_transaction_factory(database._source_component.factory):
            execution = await _successor_execution(database, state.acquired, state, monkeypatch)
            await _new_notification_store(module, database, state)
            with pytest.raises(RuntimeError, match="^synthetic preparation probes complete$"):
                await _publish_successor(
                    module,
                    database,
                    execution,
                    state.retention_sessions,
                    state,
                    monkeypatch,
                    preparation_observer=_observe_preparation_failures,
                )
            assert state.preparation_failures_verified and not state.notices
            await _assert_failed_publication(database, state, state.predecessor)
        await module._verify_retained_entity_read(database, state.original_pair, monkeypatch)


async def verify_native_notification_rollback(monkeypatch, tmp_path, copy_bounds):
    """Prove one late atomic rollback and the real refusal of immediate fresh admission."""
    import pytest

    from tests import test_network_registry_cms_prepared_pair_postgres as module

    async with _native_incumbent(monkeypatch, tmp_path, copy_bounds) as state:
        database = state.database
        async with source.bind_generic_source_transaction_factory(database._source_component.factory):
            execution = await _successor_execution(database, state.acquired, state, monkeypatch)
            await _new_notification_store(module, database, state)
            state.publication_task = asyncio.current_task()
            with pytest.raises(RuntimeError, match="^synthetic bind rollback$"):
                await _publish_successor(
                    module, database, execution, state.retention_sessions, state, monkeypatch, fail=True
                )
            state.publication_returned = True
            await _assert_failed_publication(database, state, state.predecessor, prepared_captures=True)
            assert len(state.notices) == 1
            assert {consumed["admission_purpose"] for consumed in state.rollback_consumptions} == {
                "profile",
                "cms_nonprofile",
            }
            assert len(state.rollback_consumptions) == 2
            assert await _capacity_consumptions(database) == state.rollback_consumptions
            with pytest.raises(
                native.source.fhir.ProviderDirectoryArtifactBuildStale,
                match="^provider_directory_profile_capacity_preflight_not_quiescent$",
            ):
                await _paired_native_execution(monkeypatch, execution)
            assert await _capacity_consumptions(database) == state.rollback_consumptions
        await _cleanup_failed_fixture_capture(state)
        await _assert_failed_publication(database, state, state.predecessor)
        assert await _capacity_consumptions(database) == state.rollback_consumptions
        await module._verify_retained_entity_read(database, state.original_pair, monkeypatch)


async def verify_native_budget_refusal(monkeypatch, tmp_path, copy_bounds):
    """Retain the actual signed pre-COPY refusal without repeating it in every consumer."""
    from tests import test_network_registry_cms_prepared_pair_postgres as module

    async with _native_incumbent(monkeypatch, tmp_path, copy_bounds) as state:
        database = state.database
        async with source.bind_generic_source_transaction_factory(database._source_component.factory):
            execution = await _successor_execution(database, state.acquired, state, monkeypatch)
            await _new_notification_store(module, database, state)
            await _publish_budget_refusal(
                module, database, state.acquired, execution, state.retention_sessions, state, monkeypatch
            )
            await _assert_failed_publication(database, state, state.predecessor)
        assert not state.notices


def _configure_native_producer(monkeypatch, module, copy_bounds):
    """Keep the small real fixture options and its nonempty-publication refusal together."""
    module._retained_office_resources(monkeypatch)
    install_native_office_seed(monkeypatch)
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_UNLOGGED_STAGE", "true")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES", "true")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MIN_ROWS", "2")
    import pytest

    with pytest.raises(RuntimeError, match="stage row count 0 below minimum 2"):
        native.entity_address_unified._validate_publish_row_count(
            stage_rows=0, previous_rows=0, test_mode=False, min_rows_required=2
        )
    monkeypatch.setattr(
        module.retention.RegistryCMSRawEpochAdmission, "copy_batch_bytes", property(lambda _self: copy_bounds[1])
    )


@asynccontextmanager
async def _native_incumbent(monkeypatch, tmp_path, copy_bounds):
    """Keep a guarded legacy incumbent and the actual successor actors alive."""
    import subprocess
    from types import SimpleNamespace

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from tests import test_network_registry_cms_prepared_pair_postgres as module

    _configure_native_producer(monkeypatch, module, copy_bounds)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    async with native.source_native_publication_database(monkeypatch, tmp_path, commit) as database:
        component = database._source_component
        reader = "a_retention_reader_" + uuid4().hex
        state = SimpleNamespace(
            database=database,
            roles=(component.retention_owner, reader, component.owner, component.publisher),
            captures=[],
            notification_roles=[],
            notification_schemas=[],
            notices=[],
            copy_bounds=copy_bounds,
        )
        engine = create_async_engine(
            database.engine.url.set(username=component.retained_publisher), pool_size=1, max_overflow=0
        )
        try:
            async with database.engine.begin() as connection:
                await connection.execute(text(f'CREATE ROLE "{reader}" NOLOGIN'))
                await connection.execute(
                    text(f'GRANT SET ON PARAMETER temp_file_limit TO "{component.retained_publisher}"')
                )
                assert await connection.scalar(
                    text("SELECT has_parameter_privilege(:role,'temp_file_limit','SET')"),
                    {"role": component.retained_publisher},
                )
                assert not await connection.scalar(
                    text("SELECT has_parameter_privilege(:role,'session_replication_role','SET')"),
                    {"role": component.retained_publisher},
                )
            initial = await acquire_native_source(
                database, monkeypatch, tmp_path / "initial", revision="first", run_id="retention-incumbent"
            )
            await _seed_historical_pair(module, database, initial, state, monkeypatch)
            state.acquired = await acquire_native_source(
                database, monkeypatch, tmp_path / "successor", revision="second", run_id="retention-successor"
            )
            state.retention_sessions = async_sessionmaker(engine, expire_on_commit=False)
            yield state
        finally:
            await _cleanup_producer(database, state, engine)


@asynccontextmanager
async def genuine_prepared_publication(monkeypatch, tmp_path, copy_bounds, *, observe_preparation=False):
    """Build and retain a real signed successor over a separately seeded legacy incumbent."""
    from tests import test_network_registry_cms_prepared_pair_postgres as module

    async with _native_incumbent(monkeypatch, tmp_path, copy_bounds) as state:
        await _publish_successor_pair(
            module,
            state.database,
            state.acquired,
            state.retention_sessions,
            state,
            monkeypatch,
            preparation_observer=_observe_preparation if observe_preparation else None,
        )
        yield state


async def _publish_first_pair(module, state, monkeypatch):
    """Retain and notify the first actual publication without manufacturing an incumbent."""
    from dataclasses import replace

    from process import network_registry_cms_prepared_pair as retention

    database = state.database
    async with source.bind_generic_source_transaction_factory(database._source_component.factory):
        execution = await native.attested_execution(database, state.acquired, monkeypatch)
        await _new_notification_store(module, database, state)
        job = await issued_job(database, state.acquired, execution, state.roles, state.retention_sessions, state.store)
        state.captures.append(job.capture_id)
        job = replace(job, notify_receipt=_notification_sender(module, state.store, state, fail=False))
        original = retention.prepare_registry_cms_source_pair

        async def observe(prepared, factory, request, **options):
            retained = await original(prepared, factory, request, **options)
            await module._assert_retained_raw_parity(database, retained)
            await module._assert_retained_address_parity(database, retained, prepared)
            state.prepared, state.retained = prepared, retained
            return retained

        with monkeypatch.context() as patch:
            patch.setattr(retention, "prepare_registry_cms_source_pair", observe)
            await publish_native_execution(database, execution, patch, job=job, batch_size=state.copy_bounds[0])
    proof, receipt_payload_by_field = await native.committed_publication_proof(database, state.acquired)
    assert (
        receipt_payload_by_field["predecessor_receipt_id"] is None
        and receipt_payload_by_field["expected_incumbent"] is None
    )
    assert len(state.notices) == 1 and state.notices[0].pair == state.pair
    assert state.pair.proof.serving_receipt_id == proof.serving_receipt_id
    await module._verify_committed_notification(database, state.pair, state.store, state.notices[0])


async def _first_publication_reader(database, state):
    """Use the same isolated reader and bounded retained Publisher privileges."""
    publisher = database._source_component.retained_publisher
    async with database.engine.begin() as connection:
        await connection.execute(text(f'CREATE ROLE "{state.roles[1]}" NOLOGIN'))
        await connection.execute(text(f'GRANT SET ON PARAMETER temp_file_limit TO "{publisher}"'))
        assert await connection.scalar(
            text("SELECT has_parameter_privilege(:role,'temp_file_limit','SET')"), {"role": publisher}
        )
        assert not await connection.scalar(
            text("SELECT has_parameter_privilege(:role,'session_replication_role','SET')"), {"role": publisher}
        )


@asynccontextmanager
async def genuine_first_prepared_publication(monkeypatch, tmp_path, copy_bounds):
    """Keep one genuine first-publication pair live for finalized-capture cleanup."""
    import subprocess
    from types import SimpleNamespace

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from tests import test_network_registry_cms_prepared_pair_postgres as module

    _configure_native_producer(monkeypatch, module, copy_bounds)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    async with native.source_native_publication_database(monkeypatch, tmp_path, commit) as database:
        component = database._source_component
        reader = "a_retention_reader_" + uuid4().hex
        state = SimpleNamespace(
            database=database,
            roles=(component.retention_owner, reader, component.owner, component.publisher),
            captures=[],
            notification_roles=[],
            notification_schemas=[],
            notices=[],
            copy_bounds=copy_bounds,
        )
        engine = create_async_engine(
            database.engine.url.set(username=component.retained_publisher), pool_size=1, max_overflow=0
        )
        try:
            await _first_publication_reader(database, state)
            state.retention_sessions = async_sessionmaker(engine, expire_on_commit=False)
            state.acquired = await acquire_native_source(
                database, monkeypatch, tmp_path / "first", revision="first", run_id="retention-first"
            )
            await _publish_first_pair(module, state, monkeypatch)
            yield state
        finally:
            await _cleanup_producer(database, state, engine)


async def _reject_nonoriginal_receipt_sessions(module, database, retained):
    """Neither the administrator nor another native session is the original Publisher."""
    import pytest

    from process.provider_directory_generic_candidate import GenericCandidateError

    async with database.session_factory() as session, session.begin():
        with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
            await module.serving.require_registry_receipt_owner(session, "mrf")
        assert (
            await module.serving.read_registry_source_pair_receipt(
                session, "mrf", retained.recipe.source_pin.retained_epoch.epoch_id
            )
            is None
        )
    async with source.source_transaction(database._source_component.database, isolation="repeatable read"):
        async with database.session_factory() as foreign, foreign.begin():
            with pytest.raises(ValueError, match="original_session_required"):
                await module.serving.require_registry_receipt_owner(foreign, "mrf")


async def _reject_prepared_custody(module, database, retained, state):
    import pytest

    from process.network_custom_address_source import NetworkCustomAddressSourceError

    schema = retained.address_ownership.schema_name
    async with database.engine.begin() as connection:
        await connection.execute(text(f'GRANT UPDATE ON {schema}.entity_address_unified TO "{state.roles[1]}"'))
    try:
        async with source.source_transaction(
            database._source_component.database, isolation="repeatable read"
        ) as session:
            with pytest.raises(ValueError, match="privileges are not closed"):
                await module.retention.bind_prepared_registry_cms_source_pair(
                    session,
                    retained,
                    receipt_id=state.predecessor["receipt_id"],
                    receipt_payload=state.predecessor["payload"],
                )
    finally:
        async with database.engine.begin() as connection:
            await connection.execute(text(f'REVOKE UPDATE ON {schema}.entity_address_unified FROM "{state.roles[1]}"'))
    async with source.source_transaction(database._source_component.database, isolation="read committed") as session:
        with pytest.raises(NetworkCustomAddressSourceError, match="repeatable-read transaction"):
            await module.retention.bind_prepared_registry_cms_source_pair(
                session,
                retained,
                receipt_id=state.predecessor["receipt_id"],
                receipt_payload=state.predecessor["payload"],
            )


def _original_binder_observer(module, original, state, monkeypatch):
    async def bind(session, prepared, *, receipt_id, receipt_payload):
        from dataclasses import replace

        import pytest

        with pytest.raises(ValueError):
            await original(
                session,
                replace(prepared, capacity_geometry_hash="0" * 64),
                receipt_id=receipt_id,
                receipt_payload=receipt_payload,
            )
        with pytest.raises(ValueError, match="prepared_receipt_invalid"):
            await original(
                session,
                prepared,
                receipt_id=state.predecessor["receipt_id"],
                receipt_payload=state.predecessor["payload"],
            )

        async def forbidden_scan(*_arguments, **_options):
            raise AssertionError("full content scan in atomic binder")

        with monkeypatch.context() as patch:
            patch.setattr(module.epoch_capture, "_content", forbidden_scan)
            patch.setattr(module.address_capture, "capture_entity_address_archive_receipt", forbidden_scan)
            return await original(session, prepared, receipt_id=receipt_id, receipt_payload=receipt_payload)

    return bind


async def reject_elevated_notification_role(state):
    """A genuine committed notification remains unavailable to an elevated Publisher."""
    import pytest

    database, store = state.database, state.store
    sessions = await database._source_component.factory.publication_session_factory()
    async with database.engine.begin() as connection:
        await connection.execute(text(f'GRANT SET ON PARAMETER session_replication_role TO "{store.publisher_role}"'))
    try:
        async with sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            with pytest.raises(ValueError, match="source_notification_invalid"):
                await store.read(session, source_schema="mrf", receipt_id=state.pair.proof.serving_receipt_id)
    finally:
        async with database.engine.begin() as connection:
            await connection.execute(
                text(f'REVOKE SET ON PARAMETER session_replication_role FROM "{store.publisher_role}"')
            )


async def reject_unscoped_notification(state):
    import pytest

    database = state.database
    async with source.bind_generic_source_transaction_factory(database._source_component.factory):
        async with source.source_transaction(
            database._source_component.database, isolation="read committed"
        ) as session:
            async with source.source_publisher_body(session):
                with pytest.raises(ValueError, match="cms_registry_source_publication_invalid"):
                    await state.store.notify_receipt(
                        session, receipt_id=state.pair.proof.serving_receipt_id, pair=state.pair
                    )
