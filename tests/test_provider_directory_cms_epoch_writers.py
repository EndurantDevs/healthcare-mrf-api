# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""SOURCE receipt writers keep original-session, native-control and seal fences."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_npd_relationship as relationship
from process import provider_directory_cms_publication as publication
from process import provider_directory_cms_serving_coverage as coverage
from process import provider_directory_cms_serving_receipt as receipts
from process import provider_directory_generic_source_auxiliary as auxiliary
from process import provider_directory_generic_source_policy as policy
from process import provider_directory_generic_source_storage as storage
from process import provider_directory_source_cms_epoch_control as controls
from process.provider_directory_generic_candidate import GenericCandidateError

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("extra_rows", [0, 1, None])
async def test_relationship_completion_reconciles_verified_families_with_stored_rows(monkeypatch, extra_rows):
    session = object()

    @asynccontextmanager
    async def connection():
        yield session

    expected = len(relationship.FIELDS)
    actual = None if extra_rows is None else expected + extra_rows
    directory = SimpleNamespace(
        db=SimpleNamespace(session=connection, scalar=AsyncMock(return_value=actual)),
        _schema=lambda: "synthetic",
        _qt=lambda schema, root: f'"{schema}"."{root}"',
    )
    family = AsyncMock(return_value=1)
    restored = AsyncMock()
    monkeypatch.setattr(relationship, "_family_complete", family)
    monkeypatch.setattr(relationship, "_raised_work_mem", AsyncMock(return_value="4MB"))
    monkeypatch.setattr(relationship, "_restore_work_mem", restored)
    if extra_rows == 0:
        assert await relationship.assert_complete(directory, "candidate", "a" * 64) == expected
    else:
        with pytest.raises(RuntimeError, match="cms_npd_relationship_projection_incomplete"):
            await relationship.assert_complete(directory, "candidate", "a" * 64)
    assert [call.args[-1] for call in family.await_args_list] == list(relationship.FIELDS)
    restored.assert_awaited_once_with(session, "4MB")


@pytest.fixture
def source_body(monkeypatch):
    events = []
    session = object()
    publisher = SimpleNamespace(connection=object(), policy=SimpleNamespace(schema="synthetic"))

    @asynccontextmanager
    async def transaction(database, *, isolation=None):
        assert database is directory.db and isolation in {None, "repeatable read"}
        events.append("transaction")
        try:
            yield session
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("release")

    async def authenticate(original, schema):
        assert original is session and schema == "synthetic"
        events.append("original owner")

    async def capture(connection, configured_policy):
        assert connection is publisher.connection and configured_policy is publisher.policy
        events.append("native controls")

    directory = SimpleNamespace(
        db=SimpleNamespace(first=AsyncMock(return_value={"dataset_id": "candidate"})),
        _schema=lambda: "synthetic",
        _qt=lambda schema, root: f'"{schema}"."{root}"',
        ENDPOINT_DATASET_PUBLISHED="published",
    )
    monkeypatch.setattr(policy, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(policy, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(policy, "source_copy_session", lambda: session)
    monkeypatch.setattr(policy, "source_transaction", transaction)
    monkeypatch.setattr(receipts, "require_registry_receipt_owner", authenticate)
    monkeypatch.setattr(controls, "capture_source_cms_epoch_controls", capture)
    return SimpleNamespace(directory=directory, session=session, publisher=publisher, events=events)


def _proof():
    return dict(
        dataset_id="candidate", endpoint_id="endpoint", release_id="a" * 64, dataset_hash="b" * 64, proof_version=2
    )


async def test_source_sealer_borrows_original_owner_and_captures_native_controls(source_body):
    proof = _proof()
    await coverage.seal_cms_candidate_coverage(
        source_body.directory, SimpleNamespace(**proof), proof["release_id"], proof["dataset_hash"]
    )
    assert source_body.events == ["original owner", "transaction", "native controls", "release"]
    assert source_body.directory.db.first.await_count == 1


async def test_already_covered_source_replay_authenticates_before_reads(source_body, monkeypatch):
    async def is_matched(*args):
        source_body.events.append("coverage read")
        return True

    monkeypatch.setattr(publication, "_published_coverage_matches", is_matched)
    await publication._seal_published_coverage(source_body.directory, source_body.session, _proof())
    assert source_body.events == [
        "original owner",
        "transaction",
        "native controls",
        "coverage read",
        "coverage read",
        "release",
    ]
    source_body.directory.db.first.assert_not_awaited()


@pytest.mark.parametrize("substitution", ["session", "schema"])
async def test_source_coverage_refuses_substitution_before_auth_or_target_reads(source_body, monkeypatch, substitution):
    session = object() if substitution == "session" else source_body.session
    if substitution == "schema":
        source_body.publisher.policy.schema = "another_schema"
    read = AsyncMock()
    monkeypatch.setattr(publication, "_published_coverage_matches", read)
    with pytest.raises(RuntimeError, match="original_session_required"):
        await publication._seal_published_coverage(source_body.directory, session, _proof())
    assert source_body.events == []
    read.assert_not_awaited()
    source_body.directory.db.first.assert_not_awaited()


@pytest.mark.parametrize("gate", ["owner", "controls"])
async def test_source_coverage_authority_failure_prevents_insert(source_body, monkeypatch, gate):
    if gate == "owner":
        monkeypatch.setattr(receipts, "require_registry_receipt_owner", AsyncMock(side_effect=RuntimeError("denied")))
    else:
        monkeypatch.setattr(
            controls, "capture_source_cms_epoch_controls", AsyncMock(side_effect=RuntimeError("denied"))
        )
    with pytest.raises(RuntimeError, match="denied"):
        await coverage.seal_cms_candidate_coverage(
            source_body.directory, SimpleNamespace(**_proof()), "a" * 64, "b" * 64
        )
    source_body.directory.db.first.assert_not_awaited()
    assert source_body.events == ([] if gate == "owner" else ["original owner", "transaction", "rollback"])


async def test_source_coverage_cancellation_rolls_back_borrowed_body_without_retry(source_body):
    source_body.directory.db.first.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await coverage.seal_cms_candidate_coverage(
            source_body.directory, SimpleNamespace(**_proof()), "a" * 64, "b" * 64
        )
    assert source_body.events == ["original owner", "transaction", "native controls", "rollback"]
    assert source_body.directory.db.first.await_count == 1


@pytest.mark.parametrize("entrypoint", ["seal", "replay", "build"])
async def test_factory_alone_cannot_open_a_new_final_publication_transaction(entrypoint):
    factory = SimpleNamespace(transaction=AsyncMock())
    directory = SimpleNamespace(db=SimpleNamespace(first=AsyncMock()))
    session = SimpleNamespace(scalar=AsyncMock(), begin=AsyncMock())
    async with policy.bind_generic_source_transaction_factory(factory):
        with pytest.raises(
            (RuntimeError, GenericCandidateError),
            match="publisher_connection_required|candidate_coverage_preparation_required",
        ):
            if entrypoint == "seal":
                await coverage.seal_cms_candidate_coverage(directory, SimpleNamespace(**_proof()), "a" * 64, "b" * 64)
            elif entrypoint == "replay":
                await publication._seal_published_coverage(directory, session, _proof())
            else:
                await coverage.build_cms_coverage(session)
    factory.transaction.assert_not_called()
    session.scalar.assert_not_awaited()
    session.begin.assert_not_called()
    directory.db.first.assert_not_awaited()


async def test_inherited_publisher_task_cannot_reuse_the_final_source_body(monkeypatch):
    directory = SimpleNamespace(db=SimpleNamespace(first=AsyncMock()))
    transaction = AsyncMock(side_effect=AssertionError("transaction reached"))
    monkeypatch.setattr(policy, "source_transaction", transaction)
    token = policy._PUBLISHER.set(SimpleNamespace(task=asyncio.current_task()))
    try:
        with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
            await asyncio.create_task(
                coverage.seal_cms_candidate_coverage(directory, SimpleNamespace(**_proof()), "a" * 64, "b" * 64)
            )
    finally:
        policy._PUBLISHER.reset(token)
    transaction.assert_not_called()
    directory.db.first.assert_not_awaited()


async def test_legacy_coverage_sealer_keeps_existing_sql_without_source_authority(monkeypatch):
    monkeypatch.setattr(policy, "is_source_storage_required", lambda: False)
    authenticate = AsyncMock(side_effect=AssertionError("source authority reached"))
    monkeypatch.setattr(receipts, "require_registry_receipt_owner", authenticate)
    directory = SimpleNamespace(
        db=SimpleNamespace(first=AsyncMock(return_value={"dataset_id": "candidate"})),
        _schema=lambda: "synthetic",
        _qt=lambda schema, root: f'"{schema}"."{root}"',
        ENDPOINT_DATASET_PUBLISHED="published",
    )
    await coverage.seal_cms_candidate_coverage(directory, SimpleNamespace(**_proof()), "a" * 64, "b" * 64)
    statement = directory.db.first.await_args.args[0]
    assert 'INSERT INTO "synthetic"."provider_directory_cms_serving_coverage"' in statement
    assert "is_current=true" in statement and "RETURNING dataset_id" in statement
    assert directory.db.first.await_args.kwargs == {
        **{key: value for key, value in _proof().items() if key != "proof_version"},
        "published": "published",
        "proof_version": 2,
    }
    authenticate.assert_not_awaited()


@pytest.fixture
def receipt_body(monkeypatch):
    events = []
    pending_receipts = []
    publisher = SimpleNamespace(connection=object(), policy=object())

    @asynccontextmanager
    async def transaction(database, *, isolation=None):
        assert database is directory.db and isolation in {None, "repeatable read"}
        events.append("transaction")
        try:
            yield
        except BaseException:
            pending_receipts.clear()
            events.append("rollback")
            raise
        else:
            events.append("release")

    async def capture(connection, configured_policy):
        assert connection is publisher.connection and configured_policy is publisher.policy
        events.append("native controls")

    async def sealed(*args):
        events.append("seal")

    async def complete(*args):
        events.append("indexed complete")
        return 5

    async def insert(statement, **parameters):
        assert "ON CONFLICT (dataset_id) DO NOTHING" in statement
        events.append("insert")
        pending_receipts.append(parameters)

    async def count(*args):
        events.append("receipt read")
        return 5

    directory = SimpleNamespace(
        db=SimpleNamespace(status=insert), _schema=lambda: "synthetic", _qt=lambda schema, root: f'"{schema}"."{root}"'
    )
    monkeypatch.setattr(policy, "source_transaction", transaction)
    monkeypatch.setattr(policy, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(controls, "capture_source_cms_epoch_controls", capture)
    monkeypatch.setattr(relationship, "_assert_relationship_sealed", sealed)
    monkeypatch.setattr(relationship, "assert_complete", complete)
    monkeypatch.setattr(relationship, "completed_receipt_count", count)
    return SimpleNamespace(directory=directory, events=events, pending_receipts=pending_receipts)


async def test_relationship_receipt_is_native_attested_and_rechecked_in_one_transaction(receipt_body):
    await relationship._insert_relationship_receipt(
        receipt_body.directory, SimpleNamespace(dataset_id="candidate"), "a" * 64, 5
    )
    assert receipt_body.events == [
        "transaction",
        "native controls",
        "seal",
        "indexed complete",
        "insert",
        "seal",
        "receipt read",
        "release",
    ]
    assert receipt_body.pending_receipts == [
        dict(dataset_id="candidate", release_id="a" * 64, contract=relationship.CONTRACT, count=5)
    ]


@pytest.mark.parametrize("gate", ["controls", "seal", "complete", "late_seal", "late_count", "cancel"])
async def test_relationship_receipt_failure_or_cancel_rolls_back_without_retry(receipt_body, monkeypatch, gate):
    if gate == "controls":
        monkeypatch.setattr(
            controls, "capture_source_cms_epoch_controls", AsyncMock(side_effect=RuntimeError("denied"))
        )
    elif gate in {"seal", "late_seal", "cancel"}:
        error = asyncio.CancelledError() if gate == "cancel" else RuntimeError("denied")
        outcomes = [error] if gate == "seal" else [None, error]
        monkeypatch.setattr(relationship, "_assert_relationship_sealed", AsyncMock(side_effect=outcomes))
    elif gate == "complete":
        monkeypatch.setattr(relationship, "assert_complete", AsyncMock(return_value=4))
    else:
        monkeypatch.setattr(relationship, "completed_receipt_count", AsyncMock(return_value=4))
    with pytest.raises(asyncio.CancelledError if gate == "cancel" else RuntimeError):
        await relationship._insert_relationship_receipt(
            receipt_body.directory, SimpleNamespace(dataset_id="candidate"), "a" * 64, 5
        )
    assert receipt_body.pending_receipts == []
    assert receipt_body.events[-1] == "rollback"
    assert receipt_body.events.count("insert") == int(gate in {"late_seal", "late_count", "cancel"})


@pytest.mark.parametrize("mutation", [None, "release", "seal", "auxiliary", "override", "native_layout"])
async def test_relationship_recheck_requires_the_complete_native_finished_binding(monkeypatch, mutation):
    identity_by_field = dict(dataset_id="candidate", metadata={"source_release": {"vector_sha256": "a" * 64}})
    proof_by_field = {"name": "private_relationship", "oid": 17}
    auxiliary_by_root = {"fixed_identity_root": {"name": "private_identity", "oid": 18}}
    binding_by_field = dict(
        tables={storage.RELATIONSHIP: proof_by_field},
        auxiliary=dict(
            phase="sealed",
            tables=auxiliary_by_root,
            sealed_sha256=auxiliary._seal_digest(identity_by_field, auxiliary_by_root),
        ),
        relationship_sha256=auxiliary.relationship_seal(identity_by_field, proof_by_field),
    )
    changed = deepcopy(binding_by_field)
    if mutation == "seal":
        changed["relationship_sha256"] = None
    elif mutation == "auxiliary":
        changed["auxiliary"]["phase"] = "loading"
    publisher = SimpleNamespace(connection=object(), policy=SimpleNamespace(schema="synthetic"))
    directory = SimpleNamespace(
        _schema=lambda: "synthetic",
        _qt=lambda schema, root: (
            '"synthetic"."other"' if mutation == "override" else '"synthetic"."private_relationship"'
        ),
    )
    monkeypatch.setattr(policy, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(storage, "candidate_identity", lambda *args: identity_by_field)
    locked = AsyncMock(return_value=changed)
    monkeypatch.setattr(storage, "_locked", locked)
    verify = AsyncMock(side_effect=RuntimeError("native layout changed") if mutation == "native_layout" else None)
    monkeypatch.setattr(storage, "_verify_tables", verify)
    if mutation is None:
        await relationship._assert_relationship_sealed(directory, object(), "a" * 64)
        verify.assert_awaited_once_with(publisher.connection, publisher.policy, changed)
    else:
        with pytest.raises((RuntimeError, GenericCandidateError)):
            await relationship._assert_relationship_sealed(
                directory, object(), "b" * 64 if mutation == "release" else "a" * 64
            )
        if mutation == "release":
            locked.assert_not_awaited()


def _replay_candidate():
    return SimpleNamespace(
        dataset_id="candidate",
        endpoint_id="endpoint",
        acquisition_root_run_id="root",
        selected_resources=("Location",),
        source_release={"vector_sha256": "a" * 64},
    )


def _replay_row():
    from process.provider_directory_admission_seal import ADMISSION_GENERIC_PROOF_SUMMARY_KEY

    return dict(
        endpoint_id="endpoint",
        acquisition_root_run_id="root",
        status="validated",
        seal_valid=True,
        dataset_hash="b" * 64,
        resource_count=1,
        publication_metadata_summary_json={
            ADMISSION_GENERIC_PROOF_SUMMARY_KEY: dict(dataset_hash="b" * 64, resource_count=1)
        },
        publication_metadata_sha256="c" * 64,
        content_proof_admission_version=1,
        content_proof_admission_kind="generic",
        content_proof_admission_sha256="d" * 64,
        content_proof_resource_types=["Location"],
    )


@pytest.mark.parametrize("mutation", [None, "seal", "endpoint", "root", "status", "resources", "hash", "count"])
async def test_source_replay_requires_exact_cached_seal_and_candidate_identity(mutation):
    from process import provider_directory_cms_npd as cms

    row_by_field = _replay_row()
    changes_by_kind = {
        "seal": ("seal_valid", False),
        "endpoint": ("endpoint_id", "another-endpoint"),
        "root": ("acquisition_root_run_id", "another-root"),
        "status": ("status", "acquiring"),
        "resources": ("content_proof_resource_types", ["Organization"]),
        "hash": ("dataset_hash", "e" * 64),
        "count": ("resource_count", 2),
    }
    if mutation is None:
        seal = cms._source_registry_replay_seal(_replay_candidate(), row_by_field)
        assert seal.metadata_sha256 == "c" * 64 and seal.proof_sha256 == "d" * 64
    else:
        field, changed = changes_by_kind[mutation]
        row_by_field[field] = changed
        with pytest.raises(GenericCandidateError, match="admission_replay_changed"):
            cms._source_registry_replay_seal(_replay_candidate(), row_by_field)


@pytest.mark.parametrize("changed_custody", [False, True])
async def test_source_replay_borrows_owner_and_native_custody_without_legacy_rehash(
    source_body, monkeypatch, changed_custody
):
    from process import network_fhir_source_custody as native
    from process import provider_directory_cms_npd as cms

    legacy = importlib.import_module("process.provider_directory_admission_backfill")
    directory = source_body.directory
    directory.db.acquire = AsyncMock(side_effect=AssertionError("ordinary acquire reached"))
    directory._endpoint_dataset_content_proof = AsyncMock(side_effect=AssertionError("payload rehash reached"))
    directory._endpoint_dataset_admission_handoff = lambda seal: {"admission_receipt": seal.proof_sha256}
    directory._artifact_admission_seal_valid_sql = lambda alias: f"{alias}.stored_seal_valid"
    directory._artifact_seal_receipt_consistent_sql = lambda alias: f"{alias}.stored_receipt_consistent"
    driver = SimpleNamespace(fetchrow=AsyncMock(return_value=_replay_row()), fetchval=AsyncMock(return_value=42))
    source_body.publisher.connection = driver
    source_body.publisher.policy.owner_role = "source_owner"
    source_body.publisher.policy.runtime_roles = ("source_reader",)
    expected_by_field = dict(dataset_id="candidate", producer="fixed-producer")

    async def custody(connection, pin, owner, readers, *, source_policy):
        assert connection is driver and source_policy is source_body.publisher.policy
        assert pin.dataset_id == "candidate" and pin.resource_table_oid == 42 and pin.release_id == "a" * 64
        assert owner == "source_owner" and readers == ("source_reader",)
        return {"binding": {"identity": {} if changed_custody else expected_by_field}}

    monkeypatch.setattr(native, "_generic_partitioned_custody", custody)
    monkeypatch.setattr(storage, "candidate_identity", lambda *args: expected_by_field)
    validate_legacy = AsyncMock(side_effect=AssertionError("legacy raw proof reached"))
    monkeypatch.setattr(legacy, "_validated_row_seal", validate_legacy)
    if changed_custody:
        with pytest.raises(GenericCandidateError, match="admission_replay_changed"):
            await cms._replayed_registry_admission(directory, _replay_candidate())
        assert source_body.events == ["transaction", "rollback"]
    else:
        replay = await cms._replayed_registry_admission(directory, _replay_candidate())
        assert replay == {"dataset_hash": "b" * 64, "admission_receipt": "d" * 64}
        assert source_body.events == ["transaction", "release"]
    statement = driver.fetchrow.await_args.args[0]
    assert "stored_seal_valid" in statement and "stored_receipt_consistent" in statement and "FOR UPDATE" in statement
    assert "publication_metadata_json" not in statement and "subset_payload_sha256" not in statement
    directory.db.acquire.assert_not_called()
    directory._endpoint_dataset_content_proof.assert_not_awaited()
    validate_legacy.assert_not_awaited()
