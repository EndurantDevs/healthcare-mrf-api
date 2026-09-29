# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Sealed archive readiness, common transaction ownership, and commit recovery."""

import asyncio
import datetime
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.connection import Database
from process import provider_directory_cms_archive as archive
from process import provider_directory_cms_preparation as preparation
from process import provider_directory_cms_publication as publication
from process import provider_directory_cms_serving_receipt as receipts
from tests import test_cms_serving_publication_postgres as common
from tests import test_provider_directory_cms_serving_receipt_postgres as native
from tests.test_provider_directory_cms_preparation import _admission

fhir = importlib.import_module("process.provider_directory_fhir")


class _CommitAcknowledgementDatabase(Database):
    """Inject one acknowledgement fault after the real owning Session commits."""

    acknowledgement_failure = None

    @property
    def connection(self):
        """Let the existing scalar family applications use the actual owner Session."""
        return self._transaction_binding().session

    @asynccontextmanager
    async def transaction(self):
        is_owner = self._transaction_binding() is None
        async with super().transaction() as session:
            yield session
        if is_owner and self.acknowledgement_failure is not None:
            failure, self.acknowledgement_failure = self.acknowledgement_failure, None
            raise failure


async def _prepare_sealed_archive(database, schema, admission):
    """Reuse the real logging/seal/view builder around one small registered canonical heap."""
    target_identity = await native._install_archive(database.engine, schema)
    async with database.engine.begin() as connection:
        await connection.run_sync(lambda sync: native._apply(sync, "20260930130000"))
    prefix = "cms_archive_" + uuid4().hex
    delta_name, effective_name = prefix + "_delta", prefix + "_effective"
    identities = []
    async with preparation.nonprofile_sql_transaction(fhir, admission):
        await database.status(
            f"CREATE UNLOGGED TABLE {schema}.{delta_name} (LIKE {schema}.address_archive_v2 INCLUDING DEFAULTS)"
        )
        delta_oid = await archive._capture_heap(fhir, admission, schema, delta_name, identities)
        await database.status(f"INSERT INTO {schema}.{delta_name} VALUES (1,'new')")
        await database.status(f"ALTER TABLE {schema}.{delta_name} ADD PRIMARY KEY (address_key)")
        filenode, effective_oid, definition = await archive._seal_delta(
            fhir, admission, schema, delta_name, effective_name, identities
        )
        row_count, digest = await archive._digest(fhir, schema, delta_name)
        columns = await archive._columns(database, target_identity["target_oid"])
    return archive.PreparedArchiveDelta(
        schema,
        delta_name,
        delta_oid,
        effective_name,
        effective_oid,
        target_identity["target_oid"],
        0,
        admission.plan.native_address_input_hash,
        columns,
        row_count,
        digest,
        definition,
        filenode,
        admission,
    )


def _bind_publication_seams(monkeypatch, database, schema, prepared, address, outcome):
    """Keep source/native family fixture seams separate from actual archive and readiness code."""
    monkeypatch.setattr(fhir, "_provider_directory_artifact_transaction_timeout_seconds", lambda *_args, **_kw: 5)
    monkeypatch.setattr(fhir, "_provider_directory_artifact_bundle_context", lambda *_args: (schema, (), "5s", "5s"))

    async def configure_settings(*_args):
        await database.status("SET LOCAL statement_timeout='5s'")
        await database.status("SET LOCAL lock_timeout='5s'")

    def admitted_input_fence(prepared_family, address_family, native_dependencies):
        assert prepared_family is prepared and address_family is address
        assert address.native_dependencies.archive is prepared.archive_delta
        assert prepared.archive_delta.admission is prepared.nonprofile_admission
        assert isinstance(native_dependencies, dict)
        return prepared.archive_delta

    async def assert_input_fence(session, namespace, expected):
        assert session is database.connection and namespace == schema and expected is prepared.archive_delta
        assert await archive._revision(database, schema, expected.target_oid) == expected.from_revision
        assert await session.scalar(
            text(
                "SELECT EXISTS (SELECT 1 FROM pg_locks "
                "WHERE pid=pg_backend_pid() AND relation=:oid AND granted AND mode='ShareRowExclusiveLock')"
            ),
            {"oid": expected.target_oid},
        )

    monkeypatch.setattr(fhir, "_configure_provider_directory_artifact_promotion", configure_settings)
    monkeypatch.setattr(publication, "admitted_native_input_fence", admitted_input_fence)
    monkeypatch.setattr(publication, "assert_native_address_input_fence", assert_input_fence)
    common._bind_family_applications(monkeypatch, database, schema, outcome, prepared, address)
    apply_address = publication.publish_prepared_entity_address_generation

    async def guarded_address_apply(prepared_address):
        await prepared.archive_delta.assert_applied_backend(database)
        settings = await database.first("SELECT current_setting('statement_timeout'),current_setting('lock_timeout')")
        assert tuple(settings) == ("5s", "5s")
        await apply_address(prepared_address)

    monkeypatch.setattr(publication, "publish_prepared_entity_address_generation", guarded_address_apply)


async def _prepared_serving(monkeypatch, database, schema, outcome, phase_checks):
    """Use real admission measurement, archive seal, bundles, and serving readiness."""
    admission = _admission()
    admission._started = True

    async def approve(request):
        phase_checks.append(request)
        for relation in request.relations:
            await archive.capture_archive_layout(fhir, relation)
        return preparation.NonprofileAdmissionReceipt(
            request.phase,
            request.lease.lease_digest,
            request.lease.reservation_id,
            request.plan.capacity_geometry_hash,
            request.relations,
            request.logging_relations,
        )

    async def clock():
        return admission.lease.max_build_deadline - datetime.timedelta(minutes=5)

    admission.check_phase = approve

    @asynccontextmanager
    async def cutover_operation():
        assert database._transaction_binding() is None
        yield

    async def local_check(relations):
        assert database._transaction_binding() is not None
        for relation in relations or ():
            await archive.capture_archive_layout(fhir, relation)

    admission.cutover_operation, admission.check_cutover = cutover_operation, local_check
    monkeypatch.setattr(fhir, "_profile_capacity_preflight_clock", clock)
    delta = await _prepare_sealed_archive(database, schema, admission)
    execution = common._execution()
    execution.attestation.desired_profile_as_of = "2026-01-02"
    full_bundle = fhir.ProviderDirectoryArtifactBundle(archive_delta=delta)
    profile_bundle = fhir.ProviderDirectoryArtifactBundle(
        profile_delta=SimpleNamespace(generation_id="pdprofile_" + "2" * 32)
    )
    prepared = preparation.PreparedServingArtifacts(
        fhir,
        SimpleNamespace(datasets=()),
        execution,
        admission,
        full_bundle,
        profile_bundle,
        {"profile": {}},
        delta.relation_overrides,
        None,
    )
    address = SimpleNamespace(committed=False, context={}, swaps=[], native_dependencies=SimpleNamespace(archive=delta))
    prepared.address = address
    _bind_publication_seams(monkeypatch, database, schema, prepared, address, outcome)
    return prepared, address


async def _assert_publication_result(database, schema, prepared, address, predecessor, before, outcome):
    """Read durable state independently after both the owner Session and recovery finish."""
    is_committed = outcome in {"success", "lost_ack", "cancel_after"}
    delta = prepared.archive_delta
    assert delta.committed is is_committed
    assert prepared.nonprofile_bundle.promoted is is_committed and prepared.profile_bundle.promoted is is_committed
    assert address.committed is is_committed and database._transaction_binding() is None
    assert await database.scalar(f"SELECT count(*) FROM {schema}.address_archive_v2") == int(is_committed)
    assert await archive._oid(database, schema, "address_archive_v2") == delta.target_oid
    assert await archive._revision(database, schema, delta.target_oid) == (2 if is_committed else 0)
    if not is_committed:
        assert await common._serving_state(database.engine, schema) == before
        return
    async with database.session() as session:
        current = await receipts.read_current_receipt(session, schema)
        assert current["receipt_id"] == prepared.metrics["cms_serving"]["receipt_id"]
        assert current["payload"]["predecessor_receipt_id"] == predecessor["receipt_id"]
        assert current["payload"]["archive"] == {
            "target_oid": delta.target_oid,
            "from_revision": 0,
            "to_revision": 2,
            "native_input_hash": delta.native_input_hash,
            "delta_rows": delta.delta_rows,
            "delta_sha256": delta.delta_sha256,
        }
        assert await receipts.verify_historical_receipt(session, schema, current["receipt_id"], current["payload"])
    assert prepared.metrics["cms_serving"]["recovered_commit"] is (outcome != "success")


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "profile", "lost_ack", "cancel_after", "seal"])
async def test_actual_archive_readiness_and_common_publication(monkeypatch, outcome):
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await common._enable_candidate_checks(engine, schema)
        before = await common._serving_state(engine, schema)
        database = _CommitAcknowledgementDatabase(engine, async_sessionmaker(engine, expire_on_commit=False))
        monkeypatch.setattr(fhir, "db", database)
        monkeypatch.setenv("DB_SCHEMA", schema)
        phase_checks = []
        prepared, address = await _prepared_serving(monkeypatch, database, schema, outcome, phase_checks)
        delta = prepared.archive_delta
        cms_token = preparation._ACTIVE.set(None)
        profile_token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(object())
        try:

            async def no_rescan(*_args):
                raise AssertionError("Profile and cutover readiness must use sealed metadata")

            monkeypatch.setattr(archive, "_digest", no_rescan)
            await prepared.assert_ready()
            assert phase_checks[-1].phase == "readiness"
            assert not any(check.phase == "cutover" for check in phase_checks)
            await _run_publication(database, schema, prepared, address, predecessor, before[0], outcome)
            await _assert_publication_result(database, schema, prepared, address, predecessor, before, outcome)
            assert [check.phase for check in phase_checks].count("cutover") == 1
        finally:
            fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(profile_token)
            preparation._ACTIVE.reset(cms_token)
            await delta.cleanup(fhir)
        assert not prepared.nonprofile_admission._relations
        assert await archive._oid(database, schema, delta.delta_table) is None
        assert await archive._oid(database, schema, delta.effective_relation) is None


async def _run_publication(database, schema, prepared, address, predecessor, dependencies, outcome):
    """Fault only owned seal metadata, family application, or acknowledgement delivery."""
    if outcome == "seal":
        await database.status(
            f"ALTER TABLE {schema}.{prepared.archive_delta.delta_table} DISABLE TRIGGER {archive._SEAL}"
        )
    if outcome == "lost_ack":
        database.acknowledgement_failure = OSError("synthetic acknowledgement lost")
    if outcome == "cancel_after":
        database.acknowledgement_failure = asyncio.CancelledError("synthetic cancellation after commit")
    candidate_proof_by_field = {
        key: predecessor["payload"]["cms"][key]
        for key in ("dataset_id", "endpoint_id", "dataset_hash", "release_id", "proof_version")
    }

    async def publish():
        return await publication.commit_prepared_serving_generation(
            fhir,
            prepared.execution,
            prepared,
            address=address,
            candidate_proof=candidate_proof_by_field,
            native_dependencies=dependencies,
            predecessor=predecessor,
        )

    if outcome in {"profile", "seal"}:
        with pytest.raises(RuntimeError, match="synthetic profile failure|write_seal_changed"):
            await publish()
    elif outcome == "cancel_after":
        with pytest.raises(asyncio.CancelledError):
            await publish()
    else:
        await publish()
