# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Commit prepared CMS serving results through one caller-owned transaction."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

from sqlalchemy import text

from process import provider_directory_cms_serving_coverage as coverage
from process import provider_directory_cms_serving_receipt as receipts
from process.cms_doctors_preparation import apply_prepared_cms_doctors_generation
from process.entity_address_candidate_preparation import (
    publish_prepared_entity_address_generation,
)
from process.native_publication_read_access import preserve_publication_reads
from process.provider_directory_artifact_bundle_preparation import (
    apply_prepared_artifact_bundle,
)
from process.provider_directory_cms_address import admitted_native_input_fence
from process.provider_directory_cms_native_inputs import (
    assert_native_address_input_fence,
)
from process.provider_directory_cms_preparation import remaining_build_seconds
from process.provider_directory_projection_stage import _copy_driver
from process.reference_family_result_generation import RELATION_NAMES_BY_IMPORTER

_PIN_FIELDS = (
    "source_id",
    "endpoint_id",
    "dataset_id",
    "dataset_hash",
    "acquisition_root_run_id",
)


def _pin(pair):
    return {field: pair[field] for field in _PIN_FIELDS}


def _receipt_payload(execution, proof, predecessor, snapshot, archive_result=None):
    attestation = execution.attestation
    desired = sorted((_pin(pair) for pair in attestation.pairs), key=lambda pair: pair["source_id"])
    cms_pin_by_field = next((pin for pin in desired if pin["source_id"] == "cms-npd"), None)
    if cms_pin_by_field is None:
        if predecessor is None:
            raise RuntimeError("cms_serving_selection_invalid")
        cms_pin_by_field = _pin(predecessor["payload"]["cms"])
    if any(cms_pin_by_field[field] != proof[field] for field in ("dataset_id", "endpoint_id", "dataset_hash")):
        raise RuntimeError("cms_serving_candidate_coverage_changed")
    cms_pin_by_field = {**cms_pin_by_field, "release_id": proof["release_id"], "proof_version": proof["proof_version"]}
    if archive_result is None and predecessor is not None and "archive" in predecessor["payload"]:
        if cms_pin_by_field != predecessor["payload"]["cms"]:
            raise RuntimeError("cms_serving_archive_result_required")
        archive_result = predecessor["payload"]["archive"]
    return receipts.validate_receipt_payload(
        {
            "contract_version": 1,
            "predecessor_receipt_id": predecessor["receipt_id"] if predecessor else None,
            "expected_incumbent": _pin(predecessor["payload"]["cms"]) if predecessor else None,
            "cms": cms_pin_by_field,
            "desired_datasets": desired,
            "selection": {
                "proof_id": attestation.proof_id,
                "fingerprint": attestation.selection_fingerprint,
                "catalog_digest": attestation.catalog_digest,
            },
            **snapshot,
            **({"archive": archive_result} if archive_result is not None else {}),
        }
    )


def _assert_expected_incumbent(execution, predecessor):
    attestation = execution.attestation
    if attestation.desired_cms_dataset is None:
        return
    expected = _pin(attestation.expected_cms_incumbent) if attestation.expected_cms_incumbent else None
    incumbent = _pin(predecessor["payload"]["cms"]) if predecessor else None
    if expected != incumbent:
        raise RuntimeError("cms_serving_incumbent_changed")


async def _published_coverage_matches(fhir, session, proof):
    schema = fhir._schema()
    dataset = fhir._qt(schema, "provider_directory_endpoint_dataset")
    table = fhir._qt(schema, "provider_directory_cms_serving_coverage")
    return bool(
        await session.scalar(
            text(f"""SELECT EXISTS (SELECT 1 FROM {dataset} d JOIN {table} c USING (dataset_id)
            WHERE d.dataset_id=:dataset_id AND d.endpoint_id=:endpoint_id AND d.dataset_hash=:dataset_hash
              AND d.status='published' AND d.is_current AND d.published_at IS NOT NULL
              AND c.dataset_hash=d.dataset_hash AND c.published_at=d.published_at
              AND c.release_id=:release_id AND c.proof_version=:proof_version)"""),
            proof,
        )
    )


async def _seal_published_coverage(fhir, session, proof):
    async with coverage._source_publication_body(fhir, session):
        if not await _published_coverage_matches(fhir, session, proof):
            await coverage.seal_cms_candidate_coverage(
                fhir, SimpleNamespace(**proof), proof["release_id"], proof["dataset_hash"]
            )
        if not await _published_coverage_matches(fhir, session, proof):
            raise RuntimeError("cms_npd_coverage_cutover_changed")


async def _verify_commit(fhir, receipt_id, receipt_payload):
    async with fhir.db.session() as session:
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        await session.execute(text("SET LOCAL lock_timeout='1s'"))
        return await receipts.verify_historical_receipt(session, fhir._schema(), receipt_id, receipt_payload)


async def _consume_preparations(prepared, address, doctors, receipt_id, receipt_payload):
    """The caller has already verified the immutable committed common receipt."""
    if address is not None:
        # A subsequent publication may already have replaced the live pointer.
        # Historical common proof still establishes that these stages were consumed.
        address.committed = True
        address.context["publication_state"] = "published"
    if doctors is not None:
        await doctors.mark_committed(receipt_id, receipt_payload)
    archive = getattr(prepared, "archive_delta", None)
    if archive is not None:
        await archive.mark_committed(prepared.fhir, receipt_payload.get("archive"))
    await prepared.mark_committed(profile_result=receipt_payload["profile"])


async def _finalize_committed_publication(fhir, prepared, address, doctors, receipt_id, receipt_payload, is_recovered):
    """Bound recovery and ownership finalization independently of caller cancellation."""
    async with asyncio.timeout(10):
        registry_preparation = _registry_preparation(prepared)
        verified = (
            await _has_registry_commit(fhir, prepared, receipt_id, receipt_payload)
            if registry_preparation is not None
            else await _verify_commit(fhir, receipt_id, receipt_payload)
        )
        if not verified:
            return None
        prepared.metrics["cms_serving"] = {
            "receipt_id": receipt_id,
            "dataset_id": receipt_payload["cms"]["dataset_id"],
            "profile_generation_id": receipt_payload["profile"]["generation_id"],
            "address_generation": receipt_payload["address"]["local_generation"],
            "doctors_generation": receipt_payload["doctors"]["local_generation"],
            "recovered_commit": is_recovered,
        }
        await _consume_preparations(prepared, address, doctors, receipt_id, receipt_payload)
        return receipt_id


async def _has_registry_commit(fhir, prepared, receipt_id, receipt_payload):
    """Read all committed evidence in one fresh authenticated Publisher snapshot."""
    from process.network_registry_cms_prepared_pair import (
        read_committed_registry_cms_pair,
    )
    from process.registry_cms_source_notifications import RegistryCMSSourceNotification

    prepared_pair = _registry_preparation(prepared)
    job = prepared.nonprofile_admission.registry_source_job
    async with job.source_session_factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        await session.execute(text("SET LOCAL statement_timeout='5s'"))
        await session.execute(text("SET LOCAL lock_timeout='1s'"))
        notice = await job.read_receipt(
            session,
            source_schema=prepared_pair.request.source_pin.schema_name,
            receipt_id=receipt_id,
        )
        if type(notice) is not RegistryCMSSourceNotification or notice.source_attempt != job.source_attempt:
            return False
        if not await receipts.verify_historical_receipt(session, fhir._schema(), receipt_id, receipt_payload):
            return False
        pair = await read_committed_registry_cms_pair(session, prepared_pair)
        is_matching = (
            pair is not None
            and pair == notice.pair
            and pair.proof.serving_receipt_id == receipt_id
            and json.loads(pair.cms_receipt_payload) == receipt_payload
        )
    if is_matching:
        prepared.metrics["registry_source_notification"] = {
            "source_schema": notice.pair.proof.source_pin.schema_name,
            "receipt_id": notice.pair.proof.serving_receipt_id,
            "notification_sha256": notice.sha256,
        }
    return is_matching


def _assert_address_prepared(execution, prepared, address, candidate_proof, predecessor):
    if address is None and (
        execution.attestation.operation != "purge"
        or predecessor is None
        or any(
            stage.target_relation not in {"provider_directory_profile", "provider_directory_profile_evidence"}
            for stage in prepared.stages
        )
        or any(
            candidate_proof[field] != predecessor["payload"]["cms"][field]
            for field in ("dataset_id", "endpoint_id", "dataset_hash", "release_id", "proof_version")
        )
    ):
        raise RuntimeError("cms_serving_prepared_address_required")


@asynccontextmanager
async def _cutover_authorization(prepared):
    """Obtain fresh authorization before the owner's transaction takes any locks."""
    admission = prepared.nonprofile_admission
    if admission is None:
        yield
    else:
        async with admission.publication(prepared.fhir, prepared.fhir._schema()):
            yield


@asynccontextmanager
async def _publication_session(fhir, source_session_factory, *, capture_id=None):
    """Choose the source pool before opening the final owner's transaction."""
    from process import provider_directory_cms_publication_custody as wal_custody
    from process.provider_directory_generic_source_policy import is_source_storage_required, source_publication_sessions

    is_source_publication = is_source_storage_required()
    if source_session_factory is None:
        if is_source_publication:
            source_session_factory = await source_publication_sessions()
        else:
            async with wal_custody.database_session(fhir) as session:
                yield session
            return
    if not callable(source_session_factory) or fhir.db._transaction_binding() is not None:
        raise RuntimeError("cms_serving_publication_requires_own_transaction")
    if capture_id is not None:
        from process.network_registry_cms_capture_lock import registry_cms_capture_transaction

        options = {"fhir": fhir} if wal_custody.is_bounded(fhir) else {}
        if is_source_publication:
            options["isolation"] = "read committed"
        async with registry_cms_capture_transaction(
            source_session_factory, capture_id=capture_id, **options
        ) as session:
            async with (
                fhir.db.bind_existing_session(session),
                wal_custody.source_body(fhir, source_session_factory, capture_id, session),
                _original_source_publication(fhir, session),
            ):
                yield session
        return
    group = None
    try:
        async with source_session_factory() as session, session.begin():
            if is_source_publication:
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
            async with (
                fhir.db.bind_existing_session(session),
                wal_custody.source_body(fhir, source_session_factory, capture_id, session) as group,
            ):
                if not is_source_publication:
                    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                async with _original_source_publication(fhir, session):
                    yield session
    except BaseException as failure:
        if group is not None and group["failure"] is None:
            group["failure"] = failure
        raise


@asynccontextmanager
async def _original_source_publication(fhir, session):
    """Bind after the existing original transaction/WAL/database owners have started."""
    from process.provider_directory_generic_source_policy import is_source_storage_required, original_source_publication

    if is_source_storage_required():
        async with original_source_publication(fhir.db, session):
            yield
    else:
        yield


@asynccontextmanager
async def _publication_transaction(
    fhir, execution, prepared, native_dependencies, predecessor, native_input_fence, *, source_session_factory=None
):
    """Apply the native publication wall and lock order before any serving writes."""
    timeout_seconds = fhir._provider_directory_artifact_transaction_timeout_seconds(
        prepared.fence, profile_delta=prepared.profile_delta
    )
    async with _cutover_authorization(prepared):
        initial_build = fhir.profile_initial.build_from_stages(fhir, prepared.stages, prepared.profile_delta)
        if initial_build is not None:
            timeout_seconds = await fhir.profile_initial.preparation_timeout_seconds(
                fhir, prepared.stages, timeout_seconds
            )
            if prepared.nonprofile_admission is not None:
                timeout_seconds = min(
                    timeout_seconds, await remaining_build_seconds(fhir, prepared.nonprofile_admission)
                )
        async with (
            asyncio.timeout(timeout_seconds) as cutover_timeout,
            _publication_session(fhir, source_session_factory, capture_id=_source_capture_id(prepared)) as session,
        ):
            _schema, _relations, lock_timeout, statement_timeout = fhir._provider_directory_artifact_bundle_context(
                fhir._ordered_provider_directory_artifact_bundle(prepared.stages), prepared.profile_delta
            )
            await fhir._configure_provider_directory_artifact_promotion(lock_timeout, statement_timeout)
            await fhir.profile_initial.lock_metadata(fhir, prepared.stages, prepared.profile_delta)
            await _lock_retained_relations(fhir, session, prepared.fence)
            archive = getattr(prepared, "archive_delta", None)
            if archive is not None:
                await archive.before_lock(fhir, session)
            if native_input_fence is not None:
                await assert_native_address_input_fence(session, fhir._schema(), native_input_fence)
            await receipts.assert_native_dependencies(session, fhir._schema(), native_dependencies)
            current_receipt = await receipts.read_current_receipt(session, fhir._schema())
            if (current_receipt["receipt_id"] if current_receipt else None) != (
                predecessor["receipt_id"] if predecessor else None
            ):
                raise RuntimeError("cms_serving_predecessor_changed")
            _assert_expected_incumbent(execution, predecessor)
            await prepared.assert_ready(cutover=True)
            yield session, cutover_timeout


def _source_capture_id(prepared):
    from process.network_registry_cms_prepared_pair import PreparedRegistryCMSSourcePair

    pair = getattr(prepared, "registry_source_pair", None)
    if pair is None:
        return None
    if type(pair) is not PreparedRegistryCMSSourcePair:
        raise RuntimeError("cms_registry_source_preparation_required")
    return pair.address_ownership.dataset_id


async def _lock_retained_relations(fhir, session, fence):
    """Take endpoint locks before the captured native revision fence locks its inputs."""
    if fence.datasets:
        from process.provider_directory_generic_source_policy import is_source_storage_required

        if is_source_storage_required():
            await fhir._lock_source_publication_controls()
        await fhir._lock_and_verify_artifact_dataset_fence(fence)


def _live_swap_relations(prepared, address, doctors):
    """Let the common cut lock the complete composite set in one ordered call."""
    relation_names = {stage.target_relation for stage in prepared.stages}
    if address is not None:
        relation_names.update(swap.live_cls.__main_table__ for swap in address.swaps)
    if doctors is not None:
        relation_names.update(RELATION_NAMES_BY_IMPORTER["cms-doctors"])
    return tuple(sorted(relation_names))


async def _apply_prepared_results(
    fhir, execution, prepared, address, proof, predecessor, cutover_timeout, doctors=None
):
    """Install both families and validate their common proof before the owner commits."""
    session = fhir.db._transaction_binding().session
    await coverage.assert_sealed_cms_candidate_coverage(session, fhir._schema(), proof)
    archive = getattr(prepared, "archive_delta", None)
    archive_result = await archive.apply(fhir, session) if archive is not None else None
    if fhir.profile_initial.build_from_stages(fhir, prepared.stages, prepared.profile_delta) is not None:
        await prepared.assert_ready(cutover=True, archive_applied=archive_result is not None)

    async def before_swaps():
        """Keep every prepared swap behind the complete live lock phase."""
        if fhir.profile_initial.build_from_stages(fhir, prepared.stages, prepared.profile_delta) is not None:
            await prepared.assert_ready(cutover=True, archive_applied=archive_result is not None)
        if prepared.stages:
            driver = await _copy_driver(session)
            for stage in prepared.stages:
                await preserve_publication_reads(
                    driver, schema=stage.schema, live_table=stage.target_relation, stage_table=stage.stage_table
                )
        if doctors is not None:
            await apply_prepared_cms_doctors_generation(doctors)

    capacity_forecast = await apply_prepared_artifact_bundle(
        fhir,
        prepared.stages,
        profile_delta=prepared.profile_delta,
        cutover_timeout=None if prepared.profile_delta is not None else cutover_timeout,
        settings_configured=True,
        before_swaps=before_swaps,
        extra_live_relations=_live_swap_relations(prepared, address, doctors),
    )
    await _seal_published_coverage(fhir, session, proof)
    if address is not None:
        await publish_prepared_entity_address_generation(address)
    snapshot = await receipts.capture_native_dependencies(session, fhir._schema())
    receipt_payload = _receipt_payload(execution, proof, predecessor, snapshot, archive_result)
    registry_preparation = _registry_preparation(prepared)
    if registry_preparation is None:
        receipt_id = await receipts.append_serving_receipt(session, fhir._schema(), receipt_payload)
    else:
        receipt_id = await receipts.append_serving_receipt(
            session, fhir._schema(), receipt_payload, registry_source_pair=registry_preparation
        )
    await _bind_registry_source_receipt(session, prepared, receipt_id, receipt_payload)
    await session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    if capacity_forecast is not None:
        await fhir._validate_profile_delta_total_wal(
            fhir._provider_directory_profile_capacity_admission(), capacity_forecast
        )
    if prepared.nonprofile_admission is not None:
        await prepared.nonprofile_admission.assert_cutover_complete()
    return receipt_id, receipt_payload


async def _bind_registry_source_receipt(session, prepared, receipt_id, receipt_payload):
    """Bind retention and enqueue its notification in the source publication TX."""
    from process.network_registry_cms_prepared_pair import (
        bind_prepared_registry_cms_source_pair,
    )
    from process.provider_directory_cms_source_runtime import (
        registry_source_publication,
    )
    from process.provider_directory_generic_source_policy import source_publisher_body

    admission = getattr(prepared, "nonprofile_admission", None)
    job = getattr(admission, "registry_source_job", None)
    registry_preparation = _registry_preparation(prepared)
    if registry_preparation is None:
        return
    async with registry_source_publication(session, prepared, receipt_id, receipt_payload) as expected_pair:
        pair = await bind_prepared_registry_cms_source_pair(
            session,
            registry_preparation,
            receipt_id=receipt_id,
            receipt_payload=receipt_payload,
        )
        if pair != expected_pair:
            raise RuntimeError("cms_registry_source_runtime_invalid")
        async with source_publisher_body(session):
            await job.notify_receipt(session, receipt_id=receipt_id, pair=pair)


def _registry_preparation(prepared):
    """Require the captured prepared pair to match the actual authenticated job."""
    from process.network_registry_cms_prepared_pair import PreparedRegistryCMSSourcePair
    from process.provider_directory_cms_source_runtime import BoundCMSRegistrySourceJob

    admission = getattr(prepared, "nonprofile_admission", None)
    job = getattr(admission, "registry_source_job", None)
    if job is None:
        return None
    pair = getattr(prepared, "registry_source_pair", None)
    if (
        type(job) is not BoundCMSRegistrySourceJob
        or type(pair) is not PreparedRegistryCMSSourcePair
        or not callable(job.read_receipt)
    ):
        raise RuntimeError("cms_registry_source_runtime_invalid")
    plan, lease = getattr(admission, "plan", None), getattr(admission, "lease", None)
    expected = (
        job.retention_request,
        job.capture_id,
        job.owner_role,
        job.runtime_roles,
        job.source_attempt,
        getattr(plan, "capacity_geometry_hash", None),
        getattr(lease, "lease_digest", None),
        getattr(plan, "native_address_input_hash", None),
    )
    actual = (
        pair.request,
        pair.recipe.source_pin.retained_epoch.epoch_id,
        pair.owner_role,
        pair.runtime_roles,
        pair.source_attempt,
        pair.capacity_geometry_hash,
        pair.lease_digest,
        pair.native_address_input_hash,
    )
    if actual != expected:
        raise RuntimeError("cms_registry_source_runtime_invalid")
    return pair


async def commit_prepared_serving_generation(
    fhir,
    execution,
    prepared,
    *,
    address,
    candidate_proof,
    native_dependencies,
    predecessor,
    doctors=None,
):
    """Publish source, Profile, and native families together, including lost-ack recovery."""
    if fhir.db._transaction_binding() is not None:
        raise RuntimeError("cms_serving_publication_requires_own_transaction")
    _assert_address_prepared(execution, prepared, address, candidate_proof, predecessor)
    if doctors is not None and (address is None or execution.attestation.operation != "publish"):
        raise RuntimeError("cms_serving_doctors_preparation_invalid")
    native_input_fence = (
        admitted_native_input_fence(prepared, address, native_dependencies) if address is not None else None
    )
    receipt_id, receipt_payload, publication_error, cancellation = None, None, None, None
    try:
        async with _publication_transaction(
            fhir,
            execution,
            prepared,
            native_dependencies,
            predecessor,
            native_input_fence,
            source_session_factory=getattr(prepared, "source_session_factory", None),
        ) as (
            session,
            cutover_timeout,
        ):
            receipt_id, receipt_payload = await _apply_prepared_results(
                fhir, execution, prepared, address, candidate_proof, predecessor, cutover_timeout, doctors
            )
    except (Exception, asyncio.CancelledError) as error:
        if receipt_id is None or receipt_payload is None:
            raise
        publication_error = error
        cancellation = error if isinstance(error, asyncio.CancelledError) else None
    finalization = asyncio.create_task(
        _finalize_committed_publication(
            fhir, prepared, address, doctors, receipt_id, receipt_payload, publication_error is not None
        )
    )
    try:
        verified_receipt_id = await asyncio.shield(finalization)
    except asyncio.CancelledError as error:
        cancellation = error
        verified_receipt_id = await asyncio.shield(finalization)
    if not verified_receipt_id:
        if publication_error is not None:
            raise publication_error
        raise RuntimeError("cms_serving_commit_unproved")
    if cancellation is not None:
        prepared.metrics["cms_serving"]["recovered_commit"] = True
        raise cancellation
    return prepared.metrics
