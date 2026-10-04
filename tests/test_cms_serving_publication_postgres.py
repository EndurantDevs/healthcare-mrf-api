# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real transaction and receipt recovery, using sealed scalar source fixtures."""

import asyncio
import datetime
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from process import provider_directory_cms_preparation as preparation
from process import provider_directory_cms_publication as publication
from process import provider_directory_cms_serving_receipt as receipts
from process import provider_directory_profile_initial as profile_initial
from tests import test_provider_directory_cms_serving_receipt_postgres as native
from tests.test_provider_directory_cms_nonprofile_capacity import _cutover_producer, _signed_plan
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME


class _Database:
    def __init__(self, engine, *, lose_ack=False):
        self.engine, self.connection, self.lose_ack = engine, None, lose_ack
        self.after_commit = None
        self.binding = None

    def _transaction_binding(self):
        return self.binding

    @asynccontextmanager
    async def transaction(self):
        try:
            async with self.engine.begin() as connection:
                self.connection = connection
                self.binding = SimpleNamespace(session=connection)
                yield connection
        finally:
            self.connection = None
            self.binding = None
        if self.after_commit is not None:
            await self.after_commit()
        if self.lose_ack == "cancel":
            raise asyncio.CancelledError("synthetic cancellation after commit")
        if self.lose_ack:
            raise OSError("synthetic commit acknowledgement lost")

    @asynccontextmanager
    async def session(self):
        async with self.engine.connect() as connection:
            yield connection


def _execution():
    pair_map = {**native._PIN, "publication_status": "published", "is_current": True, "lineage_authority": "synthetic"}
    return SimpleNamespace(
        attestation=SimpleNamespace(
            pairs=(pair_map,),
            operation="publish",
            desired_cms_dataset=pair_map,
            expected_cms_incumbent=pair_map,
            proof_id="a" * 64,
            selection_fingerprint="b" * 64,
            catalog_digest="c" * 64,
        )
    )


async def _enable_candidate_checks(engine, schema):
    """Add the real candidate-seal scalars to the narrower receipt fixture."""
    async with engine.begin() as connection:
        await connection.execute(
            text(f"""ALTER TABLE {schema}.provider_directory_endpoint_dataset
            ADD content_proof_admission_version integer DEFAULT 1,
            ADD content_proof_admission_kind text DEFAULT 'generic',
            ADD content_proof_resource_types varchar[] DEFAULT ARRAY['Endpoint','HealthcareService','InsurancePlan',
                'Location','Organization','OrganizationAffiliation','Practitioner','PractitionerRole']::varchar[]""")
        )
        await connection.execute(
            text(f"""ALTER TABLE {schema}.provider_directory_cms_candidate_coverage
            ADD endpoint_id text DEFAULT 'endpoint'""")
        )


async def _serving_state(engine, schema):
    async with engine.connect() as connection:
        snapshot = await receipts.capture_native_dependencies(connection, schema)
        counts_by_table = {}
        for table in ("provider_directory_profile", "entity_address_unified", "provider_directory_cms_serving_receipt"):
            counts_by_table[table] = await connection.scalar(text(f"SELECT count(*) FROM {schema}.{table}"))
        return snapshot, counts_by_table


def _publication_backend(database, schema):
    """Bind the real initial protocol and transaction settings to the native fixture."""
    fhir = SimpleNamespace(db=database, _schema=lambda: schema, _qt=lambda schema, table: f'"{schema}"."{table}"')
    fhir.profile_initial = profile_initial
    fhir._provider_directory_artifact_transaction_timeout_seconds = lambda _fence, **_kwargs: 5
    fhir._ordered_provider_directory_artifact_bundle = lambda stages: stages
    fhir._provider_directory_artifact_bundle_context = lambda _stages, _delta: (schema, (), "5s", "5s")

    async def configure_settings(lock_timeout, statement_timeout):
        assert (lock_timeout, statement_timeout) == ("5s", "5s")
        await database.connection.execute(text("SET LOCAL statement_timeout='5s'"))

    fhir._configure_provider_directory_artifact_promotion = configure_settings
    return fhir


async def _prepare_publication(monkeypatch, engine, schema, failure, *, lose_ack=False):
    """Bind a sealed fixture to real transaction ownership and deferred guards."""
    database = _Database(engine, lose_ack=lose_ack)
    fhir = _publication_backend(database, schema)
    address = SimpleNamespace(committed=False, context={}, swaps=[])

    async def assert_ready(*, cutover):
        assert cutover is True
        if failure == "readiness":
            raise RuntimeError("synthetic readiness failure")

    async def mark_committed(*, profile_result):
        assert profile_result["generation_id"] == "pdprofile_" + "2" * 32
        prepared.committed = True

    prepared = SimpleNamespace(
        stages=(),
        fence=SimpleNamespace(datasets=()),
        profile_delta=object(),
        metrics={},
        committed=False,
        assert_ready=assert_ready,
        mark_committed=mark_committed,
        nonprofile_admission=None,
    )
    expected_input_fence_by_field = {"fixture_schema": schema}

    def admitted_input_fence(prepared_family, address_family, native_dependencies):
        """Supply the signed-input seam; its actual native proofs have a separate fixture."""
        assert prepared_family is prepared and address_family is address
        assert isinstance(native_dependencies, dict)
        return expected_input_fence_by_field

    async def assert_input_fence(session, namespace, expected):
        """Retain real cutover ownership and verify the fence precedes family writes."""
        assert session is database.connection and namespace == schema and expected == expected_input_fence_by_field
        if failure == "native-input":
            raise RuntimeError("synthetic native input changed")

    monkeypatch.setattr(publication, "admitted_native_input_fence", admitted_input_fence)
    monkeypatch.setattr(publication, "assert_native_address_input_fence", assert_input_fence)

    _bind_family_applications(monkeypatch, database, schema, failure, prepared, address)
    proof_by_field = {
        "dataset_id": "dataset",
        "endpoint_id": "endpoint",
        "dataset_hash": "a" * 64,
        "release_id": "b" * 64,
        "proof_version": 2,
    }
    return fhir, prepared, address, proof_by_field


async def _bind_archive_publication(engine, schema, fhir, prepared):
    """Use actual revisioned archive writes at the sealed-preparation protocol seam."""
    proof = await native._install_archive(engine, schema)
    archive = SimpleNamespace(committed=False)

    async def before_lock(backend, session):
        assert backend is fhir and session is fhir.db.connection
        await session.execute(text(f"LOCK TABLE {schema}.address_archive_v2 IN SHARE ROW EXCLUSIVE MODE NOWAIT"))

    async def apply(backend, session):
        assert backend is fhir and session is fhir.db.connection
        await native._merge_archive(session, schema)
        return proof

    async def mark_committed(backend, result):
        assert backend is fhir and fhir.db._transaction_binding() is None and result == proof
        archive.committed = True

    archive.before_lock, archive.apply, archive.mark_committed = before_lock, apply, mark_committed
    prepared.fhir, prepared.archive_delta = fhir, archive
    return archive, proof


def _inject_archive_receipt_failure(monkeypatch):
    """Raise after the real common receipt insert to test transaction rollback."""
    append = receipts.append_serving_receipt

    async def fail_after_append(*args):
        await append(*args)
        raise RuntimeError("synthetic archive receipt failure")

    monkeypatch.setattr(receipts, "append_serving_receipt", fail_after_append)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "profile", "receipt", "lost_ack", "cancel_after"])
async def test_archive_and_native_families_share_commit_and_recovery(monkeypatch, outcome):
    """Archive and native family writes commit or roll back together, including acknowledgement recovery."""
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await _enable_candidate_checks(engine, schema)
        before, counts = await _serving_state(engine, schema)
        fhir, prepared, address, candidate = await _prepare_publication(
            monkeypatch,
            engine,
            schema,
            "profile" if outcome == "profile" else None,
            lose_ack="cancel" if outcome == "cancel_after" else outcome == "lost_ack",
        )
        archive, proof = await _bind_archive_publication(engine, schema, fhir, prepared)
        if outcome == "receipt":
            _inject_archive_receipt_failure(monkeypatch)

        async def publish():
            return await publication.commit_prepared_serving_generation(
                fhir,
                _execution(),
                prepared,
                address=address,
                candidate_proof=candidate,
                native_dependencies=before,
                predecessor=predecessor,
            )

        is_committed = outcome in {"success", "lost_ack", "cancel_after"}
        if outcome in {"profile", "receipt"}:
            with pytest.raises(RuntimeError, match="synthetic .*failure"):
                await publish()
            assert await _serving_state(engine, schema) == (before, counts)
        elif outcome == "cancel_after":
            with pytest.raises(asyncio.CancelledError):
                await publish()
        else:
            await publish()
        assert (
            archive.committed is is_committed
            and prepared.committed is is_committed
            and address.committed is is_committed
        )
        async with engine.connect() as connection:
            assert await connection.scalar(text(f"SELECT count(*) FROM {schema}.address_archive_v2")) == int(
                is_committed
            )
            revision = await connection.scalar(
                text(f"SELECT revision FROM {schema}.cms_native_input_revision WHERE relation_oid=:oid"),
                {"oid": proof["target_oid"]},
            )
            assert revision == (2 if is_committed else 0)
            if is_committed:
                current = await receipts.read_current_receipt(connection, schema)
                assert current["payload"]["archive"] == proof


@pytest.mark.asyncio
async def test_retained_fence_precedes_native_input_checks(monkeypatch):
    """The shared endpoint-first fence must precede native checks and family writes."""
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        fhir, prepared, address, _proof = await _prepare_publication(monkeypatch, engine, schema, None)
        calls = []
        original = publication._lock_retained_relations

        async def lock_relations(backend, session, fence):
            assert calls == []
            await original(backend, session, fence)
            calls.append("retained-fence")

        async def assert_input_fence(*_arguments):
            assert calls == ["retained-fence"]
            calls.append("native-input")

        monkeypatch.setattr(publication, "_lock_retained_relations", lock_relations)
        monkeypatch.setattr(publication, "assert_native_address_input_fence", assert_input_fence)
        snapshot = (await _serving_state(engine, schema))[0]
        async with publication._publication_transaction(
            fhir, _execution(), prepared, snapshot, predecessor, address.context
        ):
            assert calls == ["retained-fence", "native-input"]


def _bind_family_applications(monkeypatch, database, schema, failure, prepared, address):
    """Exercise the owning transaction with writes to both native family ledgers."""

    async def apply_profile(_fhir, _stages, *, profile_delta, cutover_timeout, settings_configured, before_swaps):
        assert profile_delta is prepared.profile_delta
        assert cutover_timeout is None and settings_configured is True
        await database.connection.execute(text(f"INSERT INTO {schema}.provider_directory_profile VALUES (2)"))
        await before_swaps()
        await database.connection.execute(
            text(f"""UPDATE {schema}.provider_directory_profile_serving_generation
                SET generation_id=:generation,control_generation=2,profile_rows=1,profile_as_of='2026-01-02'
                WHERE singleton_key='global'"""),
            {"generation": "pdprofile_" + "2" * 32},
        )
        if failure == "profile":
            raise RuntimeError("synthetic profile failure")
        if failure == "cancel":
            raise asyncio.CancelledError("synthetic cancellation before commit")

    async def apply_address(prepared_address):
        assert prepared_address is address
        await database.connection.execute(text(f"INSERT INTO {schema}.entity_address_unified VALUES (3)"))
        await native._advance_native(database.connection, schema)
        if failure == "address":
            raise RuntimeError("synthetic address failure")

    monkeypatch.setattr(publication, "apply_prepared_artifact_bundle", apply_profile)
    monkeypatch.setattr(publication, "publish_prepared_entity_address_generation", apply_address)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["native-input", "readiness", "profile", "address", "receipt"])
async def test_common_publication_failure_preserves_complete_native_result(monkeypatch, failure):
    """Failures before/after either family leave every native pointer and serving row intact."""
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await _enable_candidate_checks(engine, schema)
        before, counts_by_table = await _serving_state(engine, schema)
        fhir, prepared, address, proof_by_field = await _prepare_publication(monkeypatch, engine, schema, failure)
        execution = _execution()
        if failure == "receipt":
            execution.attestation.proof_id = "d" * 64
        with pytest.raises(Exception):
            await publication.commit_prepared_serving_generation(
                fhir,
                execution,
                prepared,
                address=address,
                candidate_proof=proof_by_field,
                native_dependencies=before,
                predecessor=predecessor,
            )
        assert await _serving_state(engine, schema) == (before, counts_by_table)
        assert not prepared.committed and not address.committed


@pytest.mark.asyncio
@pytest.mark.parametrize("lose_ack", [False, True])
async def test_common_publication_commit_and_lost_ack_have_one_durable_result(monkeypatch, lose_ack):
    """Acknowledgement loss reuses immutable commit proof instead of publishing a second result."""
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await _enable_candidate_checks(engine, schema)
        before, _counts = await _serving_state(engine, schema)
        fhir, prepared, address, proof_by_field = await _prepare_publication(
            monkeypatch, engine, schema, None, lose_ack=lose_ack
        )
        metrics = await publication.commit_prepared_serving_generation(
            fhir,
            _execution(),
            prepared,
            address=address,
            candidate_proof=proof_by_field,
            native_dependencies=before,
            predecessor=predecessor,
        )
        after, counts_by_table = await _serving_state(engine, schema)
        assert prepared.committed and address.committed
        assert counts_by_table == {
            "provider_directory_profile": 1,
            "entity_address_unified": 1,
            "provider_directory_cms_serving_receipt": 2,
        }
        assert after["profile"]["profile_as_of"] == "2026-01-02"
        assert after["address"]["local_generation"] == before["address"]["local_generation"] + 1
        assert after["doctors"] == before["doctors"]
        assert metrics["cms_serving"]["recovered_commit"] is lose_ack
        async with engine.connect() as connection:
            current = await receipts.read_current_receipt(connection, schema)
            assert current["receipt_id"] == metrics["cms_serving"]["receipt_id"]
            assert current["payload"]["predecessor_receipt_id"] == predecessor["receipt_id"]


async def _prepare_cutover_capacity_producer(monkeypatch, fhir):
    """Bind the signed synthetic capacity plan to the real local WAL and clock."""
    producer = _cutover_producer(monkeypatch)
    for name in (
        "_PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION",
        "_profile_capacity_preflight_clock",
        "_assert_provider_directory_profile_wal_budget",
    ):
        setattr(fhir, name, getattr(producer.fhir, name))
    producer.fhir = fhir
    producer.plan = replace(
        producer.plan,
        cutover_wal_upper_bound_bytes=2_000_000,
        reservation_bytes=(("data", 100_000), ("temp", 100_000), ("wal", 2_050_000)),
    )
    producer.lease = _signed_plan(producer.plan)
    async with fhir.db.engine.connect() as connection:
        producer.initial_wal_lsn = await connection.scalar(text("SELECT pg_current_wal_insert_lsn()::text"))
        actual_start = await connection.scalar(text("SELECT clock_timestamp()"))
    clock_by_field = {"offset": VALIDATION_TIME - actual_start, "failure": None}

    return producer, clock_by_field


async def _bind_cutover_capacity(monkeypatch, fhir, prepared, schema, failure):
    """Run real local WAL and clock statements with a signed synthetic authority clock."""
    producer, clock_by_field = await _prepare_cutover_capacity_producer(monkeypatch, fhir)

    async def scalar(query, **params):
        if fhir.db.connection is None:
            async with fhir.db.session() as connection:
                result = await connection.scalar(text(query), params)
        else:
            result = await fhir.db.connection.scalar(text(query), params)
        return (
            (clock_by_field["failure"] or result + clock_by_field["offset"]) if "clock_timestamp" in query else result
        )

    async def local_check(relations):
        if relations is None:
            assert (
                await fhir.db.connection.scalar(
                    text(f"SELECT count(*) FROM {schema}.provider_directory_cms_serving_receipt")
                )
                == 2
            )
            if failure == "expiry":
                clock_by_field["failure"] = producer._cutover_witness.expires_at
            elif failure == "deadline":
                clock_by_field["failure"] = producer.profile_lease.max_build_deadline
            elif failure == "wal":
                await fhir.db.connection.execute(
                    text(
                        f"CREATE TABLE {schema}.synthetic_wal_cost AS "
                        "SELECT string_agg(md5(n::text),'') FROM generate_series(1,70000) n"
                    )
                )
        await producer.assert_cutover(relations)

    fhir.db.scalar = scalar
    prepared.fhir = fhir
    admission = preparation.NonprofileAdmission(
        producer.lease,
        producer.plan,
        producer.check_phase,
        paired_profile_lease=producer.profile_lease,
        cutover_operation=producer.cutover_operation,
        check_cutover=local_check,
    )
    admission._started = True
    prepared.nonprofile_admission = admission

    async def assert_ready(*, cutover):
        await admission.assert_ready(fhir, schema, cutover=cutover)

    prepared.assert_ready = assert_ready
    return producer


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "expiry", "deadline", "wal"])
async def test_final_nonprofile_budget_check_owns_complete_cutover(monkeypatch, failure):
    """One prelock witness authorizes local postwrite checks; failures roll back all families."""
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await _enable_candidate_checks(engine, schema)
        before, before_counts = await _serving_state(engine, schema)
        fhir, prepared, address, proof = await _prepare_publication(monkeypatch, engine, schema, None)
        producer = await _bind_cutover_capacity(monkeypatch, fhir, prepared, schema, failure)
        if failure is not None:
            with pytest.raises(RuntimeError, match="expired|deadline|wal_budget_exceeded"):
                await publication.commit_prepared_serving_generation(
                    fhir,
                    _execution(),
                    prepared,
                    address=address,
                    candidate_proof=proof,
                    native_dependencies=before,
                    predecessor=predecessor,
                )
            assert await _serving_state(engine, schema) == (before, before_counts)
            assert not prepared.committed and not address.committed
        else:
            await publication.commit_prepared_serving_generation(
                fhir,
                _execution(),
                prepared,
                address=address,
                candidate_proof=proof,
                native_dependencies=before,
                predecessor=predecessor,
            )
            assert prepared.committed and address.committed
        assert producer.fresh_storage_envelope.await_count == 1
        assert producer._cutover_witness is None and not prepared.nonprofile_admission._cutover_active


@pytest.mark.asyncio
@pytest.mark.parametrize("after_commit", [False, True])
async def test_cancellation_proves_commit_before_releasing_stage_ownership(monkeypatch, after_commit):
    """A cancelled owner keeps cancellation while distinguishing rollback from durable publication."""
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await _enable_candidate_checks(engine, schema)
        before, counts_by_table = await _serving_state(engine, schema)
        fhir, prepared, address, proof_by_field = await _prepare_publication(
            monkeypatch,
            engine,
            schema,
            None if after_commit else "cancel",
            lose_ack="cancel" if after_commit else False,
        )
        with pytest.raises(asyncio.CancelledError):
            await publication.commit_prepared_serving_generation(
                fhir,
                _execution(),
                prepared,
                address=address,
                candidate_proof=proof_by_field,
                native_dependencies=before,
                predecessor=predecessor,
            )
        assert prepared.committed is after_commit and address.committed is after_commit
        if after_commit:
            assert prepared.metrics["cms_serving"]["recovered_commit"] is True
            _snapshot, actual_counts = await _serving_state(engine, schema)
            assert actual_counts["provider_directory_cms_serving_receipt"] == 2
        else:
            assert await _serving_state(engine, schema) == (before, counts_by_table)


@pytest.mark.parametrize("abort", [False, True])
async def test_profile_final_budget_runs_after_common_receipt_and_rolls_back_owner(monkeypatch, abort):
    """The Profile terminal check must include late native publication and the common proof."""
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await _enable_candidate_checks(engine, schema)
        before, before_counts = await _serving_state(engine, schema)
        fhir, prepared, address, proof = await _prepare_publication(monkeypatch, engine, schema, None)
        apply_bundle = publication.apply_prepared_artifact_bundle
        forecast, admission = object(), object()
        validations = []

        async def apply_with_forecast(*args, **kwargs):
            await apply_bundle(*args, **kwargs)
            return forecast

        async def validate_total(observed_admission, observed_forecast):
            assert observed_admission is admission and observed_forecast is forecast
            assert fhir.db._transaction_binding().session is fhir.db.connection
            assert (
                await fhir.db.connection.scalar(
                    text(f'SELECT count(*) FROM "{schema}".provider_directory_cms_serving_receipt')
                )
                == 2
            )
            current = await receipts.capture_native_dependencies(fhir.db.connection, schema)
            assert current["address"] != before["address"] and current["profile"] != before["profile"]
            validations.append("all-writes-present")
            if abort:
                raise RuntimeError("synthetic final WAL overrun")

        fhir._validate_profile_delta_total_wal = validate_total
        fhir._provider_directory_profile_capacity_admission = lambda: admission
        monkeypatch.setattr(publication, "apply_prepared_artifact_bundle", apply_with_forecast)
        try:
            await publication.commit_prepared_serving_generation(
                fhir,
                _execution(),
                prepared,
                address=address,
                candidate_proof=proof,
                native_dependencies=before,
                predecessor=predecessor,
            )
        except RuntimeError as error:
            assert abort and str(error) == "synthetic final WAL overrun"
            assert await _serving_state(engine, schema) == (before, before_counts)
            assert not prepared.committed and not address.committed
        else:
            assert not abort and prepared.committed and address.committed
        assert validations == ["all-writes-present"]


@pytest.mark.asyncio
async def test_cancellation_during_commit_verification_finishes_owned_recovery(monkeypatch):
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await _enable_candidate_checks(engine, schema)
        before, _counts = await _serving_state(engine, schema)
        fhir, prepared, address, proof_by_field = await _prepare_publication(monkeypatch, engine, schema, None)
        entered, release = asyncio.Event(), asyncio.Event()
        verify_commit = publication._verify_commit

        async def delayed_verify(*args):
            entered.set()
            await release.wait()
            return await verify_commit(*args)

        monkeypatch.setattr(publication, "_verify_commit", delayed_verify)
        owner = asyncio.create_task(
            publication.commit_prepared_serving_generation(
                fhir,
                _execution(),
                prepared,
                address=address,
                candidate_proof=proof_by_field,
                native_dependencies=before,
                predecessor=predecessor,
            )
        )
        async with asyncio.timeout(5):
            await entered.wait()
            owner.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await owner
        assert prepared.committed and address.committed
        assert prepared.metrics["cms_serving"]["recovered_commit"] is True


@pytest.mark.asyncio
async def test_lost_ack_after_successor_consumes_original_historical_result(monkeypatch):
    """A successor does not erase evidence that the original stages committed."""
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await _enable_candidate_checks(engine, schema)
        before, _counts = await _serving_state(engine, schema)
        fhir, prepared, address, proof_by_field = await _prepare_publication(
            monkeypatch, engine, schema, None, lose_ack=True
        )

        async def publish_successor():
            async with engine.begin() as connection:
                current = await receipts.read_current_receipt(connection, schema)
                await connection.execute(
                    text(f"""UPDATE {schema}.provider_directory_profile_serving_generation
                    SET generation_id=:generation,control_generation=3,profile_as_of='2026-01-03'
                    WHERE singleton_key='global'"""),
                    {"generation": "pdprofile_" + "3" * 32},
                )
                await native._advance_native(connection, schema)
                payload = await native._payload(connection, schema, current)
                await receipts.append_serving_receipt(connection, schema, payload)

        fhir.db.after_commit = publish_successor
        metrics = await publication.commit_prepared_serving_generation(
            fhir,
            _execution(),
            prepared,
            address=address,
            candidate_proof=proof_by_field,
            native_dependencies=before,
            predecessor=predecessor,
        )
        assert prepared.committed and address.committed
        assert metrics["cms_serving"]["profile_generation_id"] == "pdprofile_" + "2" * 32
        after, counts_by_table = await _serving_state(engine, schema)
        assert after["profile"]["generation_id"] == "pdprofile_" + "3" * 32
        assert counts_by_table["provider_directory_cms_serving_receipt"] == 3


@pytest.mark.asyncio
async def test_new_publication_cannot_reuse_old_addresses_without_preparation(monkeypatch):
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await _enable_candidate_checks(engine, schema)
        before, counts_by_table = await _serving_state(engine, schema)
        fhir, prepared, _address, proof_by_field = await _prepare_publication(monkeypatch, engine, schema, None)
        with pytest.raises(RuntimeError, match="cms_serving_prepared_address_required"):
            await publication.commit_prepared_serving_generation(
                fhir,
                _execution(),
                prepared,
                address=None,
                candidate_proof=proof_by_field,
                native_dependencies=before,
                predecessor=predecessor,
            )
        assert await _serving_state(engine, schema) == (before, counts_by_table)


@pytest.mark.asyncio
async def test_cutover_wall_bounds_readiness_before_any_serving_change(monkeypatch):
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await _enable_candidate_checks(engine, schema)
        before, counts_by_table = await _serving_state(engine, schema)
        fhir, prepared, address, proof_by_field = await _prepare_publication(monkeypatch, engine, schema, None)
        fhir._provider_directory_artifact_transaction_timeout_seconds = lambda _fence, **_kwargs: 0.25
        entered = asyncio.Event()

        async def blocked_readiness(*, cutover):
            assert cutover is True
            entered.set()
            await asyncio.Event().wait()

        prepared.assert_ready = blocked_readiness
        with pytest.raises(TimeoutError):
            await publication.commit_prepared_serving_generation(
                fhir,
                _execution(),
                prepared,
                address=address,
                candidate_proof=proof_by_field,
                native_dependencies=before,
                predecessor=predecessor,
            )
        assert entered.is_set()
        assert await _serving_state(engine, schema) == (before, counts_by_table)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "address"])
async def test_doctors_participates_in_common_commit_and_rollback(monkeypatch, failure):
    """A native Doctors advance shares the serving owner's exact transaction and receipt."""
    async with native._database(monkeypatch) as (engine, schema):
        predecessor = await native._publish_initial(engine, schema)
        await _enable_candidate_checks(engine, schema)
        before, counts_by_table = await _serving_state(engine, schema)
        fhir, prepared, address, proof_by_field = await _prepare_publication(monkeypatch, engine, schema, failure)
        doctors = SimpleNamespace(committed=False, native_receipt=None)

        async def apply_doctors(owned):
            assert owned is doctors and fhir.db.connection is not None
            await native._advance_native(fhir.db.connection, schema, doctors=True)
            snapshot = await receipts.capture_native_dependencies(fhir.db.connection, schema)
            doctors.native_receipt = snapshot["doctors"]

        async def mark_committed(receipt_id, receipt_payload):
            assert fhir.db.connection is None
            assert receipt_payload["doctors"] == doctors.native_receipt
            async with engine.connect() as connection:
                assert await receipts.verify_historical_receipt(connection, schema, receipt_id, receipt_payload)
            doctors.committed = True

        doctors.mark_committed = mark_committed
        monkeypatch.setattr(publication, "apply_prepared_cms_doctors_generation", apply_doctors)
        arguments_by_name = dict(
            address=address,
            candidate_proof=proof_by_field,
            native_dependencies=before,
            predecessor=predecessor,
            doctors=doctors,
        )
        if failure:
            with pytest.raises(RuntimeError, match="synthetic address failure"):
                await publication.commit_prepared_serving_generation(fhir, _execution(), prepared, **arguments_by_name)
            assert await _serving_state(engine, schema) == (before, counts_by_table)
            assert not doctors.committed
        else:
            metrics = await publication.commit_prepared_serving_generation(
                fhir, _execution(), prepared, **arguments_by_name
            )
            after, _counts = await _serving_state(engine, schema)
            assert doctors.committed and prepared.committed and address.committed
            assert after["doctors"]["local_generation"] == before["doctors"]["local_generation"] + 1
            assert metrics["cms_serving"]["doctors_generation"] == after["doctors"]["local_generation"]
