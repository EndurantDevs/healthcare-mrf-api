# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Original sealed-capture reconstruction and ordinary owner-job results."""

import datetime as dt
import hashlib
import hmac
import json
from types import SimpleNamespace

from sqlalchemy import select

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportCaptureBundle,
    CustomImportExecution,
    CustomImportLease,
)
from process.custom_import import family_shard_handoff as handoff
from process.custom_import.build_counts import SourceOutcomeCounts
from process.custom_import.build_source import _set_timeout
from process.custom_import.family import CandidateRejected
from process.custom_import.runner_types import (
    CancellationRequested,
    CandidateRunnerError,
    CandidateRunResult,
    LeaseAuthorityLost,
)
from process.custom_import.snowflake_candidate import SnowflakeBundleCandidateRequest
from process.custom_import.snowflake_capture import SnowflakeCaptureResult
from process.custom_import.snowflake_segmented_runner import (
    _build_request,
    _finish,
    _result,
    _validated_policy,
    configured_request_identity,
)
from process.custom_import.snowflake_source_binding import load_snowflake_source_binding

NATIVE_FIELDS = frozenset(
    {
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "source_binding_revision_id",
        "source_binding_sha256",
        "request_identity_sha256",
        "engine_execution_id",
        "capture_bundle_id",
        "capture_manifest_sha256",
        "fence",
        "base_generation_id",
        "base_pointer_version",
        "build_deadline_at",
        "bearer_sha256",
        "controller_execution_id",
    }
)


def _utc(expected_value):
    if not isinstance(expected_value, dt.datetime) or expected_value.tzinfo is None:
        handoff.unavailable()
    return expected_value.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


async def original_request(factory, locator, bearer):
    """Never claim, replace a lease or enter any source/credential acquisition."""

    handoff.checked_bearer(bearer)
    async with factory() as session, session.begin():
        await _set_timeout(session, 5000)
        loaded = await load_snowflake_source_binding(
            session,
            definition_revision_id=locator["definition_revision_id"],
            source_binding_revision_id=locator["source_binding_revision_id"],
        )
        execution = (
            await session.scalars(
                select(CustomImportExecution).where(
                    CustomImportExecution.dataset_id == locator["dataset_id"],
                    CustomImportExecution.definition_revision_id == locator["definition_revision_id"],
                    CustomImportExecution.idempotency_key == locator["controller_execution_id"],
                )
            )
        ).one_or_none()
        if (
            execution is None
            or execution.state != "running"
            or execution.source_binding_revision_id != loaded.source_binding_revision_id
        ):
            handoff.unavailable()
        lease = await session.get(CustomImportLease, execution.execution_id)
        bundle = await session.get(CustomImportCaptureBundle, execution.capture_bundle_id)
        if (
            lease is None
            or bundle is None
            or lease.continuation_id is not None
            or lease.token_sha256 is None
            or not hmac.compare_digest(bytes(lease.token_sha256), hashlib.sha256(bearer).digest())
        ):
            handoff.unavailable()
        request, captured, identity, manifest_sha = _configured_original(loaded, execution, bundle, lease, bearer)
    policy = _validated_policy(getattr(loaded.binding, "processing_policy", None))
    # This performs the original sealed-capture, live lease, deadline and base checks.
    build_request = await _build_request(factory, request, captured, policy)
    native_by_field = dict(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        source_binding_revision_id=request.source_binding_revision_id,
        source_binding_sha256=request.source_binding_sha256.hex(),
        request_identity_sha256=identity.hex(),
        engine_execution_id=captured.execution_id,
        capture_bundle_id=captured.capture_bundle_id,
        capture_manifest_sha256=manifest_sha,
        fence=captured.fence,
        base_generation_id=build_request.expected_base_generation_id,
        base_pointer_version=build_request.expected_pointer_version,
        build_deadline_at=_utc(build_request.build_deadline_at),
        bearer_sha256=hashlib.sha256(bearer).hexdigest(),
        controller_execution_id=request.idempotency_key,
    )
    return SimpleNamespace(
        request=request, captured=captured, policy=policy, build_request=build_request, native=native_by_field
    )


def require_native(descriptor, native):
    """Require equality of every independently reconstructed native scope field."""
    if native.keys() != NATIVE_FIELDS or any(descriptor[name] != native[name] for name in NATIVE_FIELDS):
        handoff.unavailable()


async def retained_state(factory, descriptor):
    """Observe the original epoch after cancellation/finality, without renewing it."""
    async with factory() as session, session.begin():
        await _set_timeout(session, 5000)
        execution = await session.get(CustomImportExecution, descriptor["engine_execution_id"])
        lease = await session.get(CustomImportLease, descriptor["engine_execution_id"])
        bundle = await session.get(CustomImportCaptureBundle, descriptor["capture_bundle_id"])
        if execution is None or lease is None or bundle is None:
            handoff.unavailable()
        expected_by_field = {
            "dataset_id": descriptor["dataset_id"],
            "definition_revision_id": descriptor["definition_revision_id"],
            "schema_revision_id": descriptor["schema_revision_id"],
            "source_binding_revision_id": descriptor["source_binding_revision_id"],
            "idempotency_key": descriptor["controller_execution_id"],
            "capture_bundle_id": descriptor["capture_bundle_id"],
        }
        if (
            any(getattr(execution, name) != expected_value for name, expected_value in expected_by_field.items())
            or execution.request_identity_sha256 is None
            or bytes(execution.request_identity_sha256).hex() != descriptor["request_identity_sha256"]
            or lease.fence != descriptor["fence"]
            or lease.token_sha256 is None
            or (lease.continuation_id is None and bytes(lease.token_sha256).hex() != descriptor["bearer_sha256"])
        ):
            handoff.unavailable()
        if (
            bundle.capture_state != "sealed"
            or bytes(bundle.manifest_sha256).hex() != descriptor["capture_manifest_sha256"]
            or bytes(bundle.source_binding_sha256).hex() != descriptor["source_binding_sha256"]
        ):
            handoff.unavailable()
        build = (
            await session.scalars(
                select(CustomImportBuildAttempt).where(
                    CustomImportBuildAttempt.execution_id == execution.execution_id,
                    CustomImportBuildAttempt.producing_fence == lease.fence,
                )
            )
        ).one_or_none()
        if build is not None and (
            build.base_generation_id != descriptor["base_generation_id"]
            or build.base_pointer_version != descriptor["base_pointer_version"]
            or _utc(build.build_deadline_at) != descriptor["build_deadline_at"]
        ):
            handoff.unavailable()
        if lease.continuation_id is not None:
            await _continued_producer(session, descriptor, execution, lease, build)
        return execution.state


async def _continued_producer(session, descriptor, execution, lease, build):
    """Authenticate a current read-only epoch without turning a parent into a writer."""
    from db.models.custom_import_continuation import CustomImportBuildContinuation
    from process.custom_import.build_continuation import authorization_bytes, parse_build_continuation_authorization

    authorization_record = await session.get(CustomImportBuildContinuation, lease.continuation_id)
    if (
        authorization_record is None
        or build is None
        or (
            authorization_record.execution_id != execution.execution_id
            or authorization_record.build_id != build.build_id
            or authorization_record.dataset_id != execution.dataset_id
            or authorization_record.definition_revision_id != execution.definition_revision_id
            or authorization_record.schema_revision_id != execution.schema_revision_id
            or authorization_record.capture_bundle_id != execution.capture_bundle_id
            or authorization_record.producer_fence != lease.fence
            or bytes(authorization_record.producer_token_sha256).hex() != descriptor["bearer_sha256"]
            or authorization_record.producer_token_sha256 != build.producing_token_sha256
            or authorization_record.nonce_sha256 != lease.token_sha256
            or authorization_record.request_identity_sha256 != execution.request_identity_sha256
            or authorization_record.base_generation_id != build.base_generation_id
            or authorization_record.base_pointer_version != build.base_pointer_version
            or authorization_record.original_deadline_at != build.build_deadline_at
        )
    ):
        handoff.unavailable()
    document = parse_build_continuation_authorization(json.loads(authorization_record.canonical_authorization))
    if authorization_bytes(document).decode("utf-8") != authorization_record.canonical_authorization or (
        hashlib.sha256(authorization_bytes(document)).digest() != authorization_record.authorization_sha256
        or document["nonce_sha256"] != bytes(authorization_record.nonce_sha256).hex()
        or document["producer_token_sha256"] != descriptor["bearer_sha256"]
        or document["build_id"] != build.build_id
    ):
        handoff.unavailable()
    return authorization_record


async def _continued_binding(session, descriptor, authorization):
    """Prove the retained block layout before starting or reopening an epoch."""
    from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
    from process.custom_import.family_block_binding import resolve_family_block_binding
    from process.custom_import.family_block_layout import FAMILY_BLOCK_LAYOUT_CONTRACT

    loaded = await load_snowflake_source_binding(
        session,
        definition_revision_id=descriptor["definition_revision_id"],
        source_binding_revision_id=descriptor["source_binding_revision_id"],
    )
    if (loaded.dataset_id, loaded.schema_revision_id, loaded.source_binding_sha256.hex()) != (
        descriptor["dataset_id"],
        descriptor["schema_revision_id"],
        descriptor["source_binding_sha256"],
    ):
        handoff.unavailable()
    policy = _validated_policy(getattr(loaded.binding, "processing_policy", None))
    layout = await session.get(CustomImportFamilyBlockLayout, authorization["snapshot_family_id"])
    if (
        layout is None
        or layout.layout_contract != FAMILY_BLOCK_LAYOUT_CONTRACT
        or (bytes(layout.definition_sha256).hex() != loaded.definition.digest)
    ):
        handoff.unavailable()
    if (
        await resolve_family_block_binding(
            session,
            authorization["snapshot_family_id"],
            loaded.definition,
            layout.collection_slots,
            persistence="candidate",
        )
        is None
    ):
        handoff.unavailable()
    return policy


async def owner_continuation_request(factory, descriptor, authorization, nonce, *, start=False, inspect_only=False):
    """Use existing continuation transfer/start checks, with existing owner credentials."""
    from process.custom_import import build_continuation as continuation

    authorization = continuation.parse_build_continuation_authorization(authorization)
    async with factory() as session, session.begin():
        await _set_timeout(session, 5000)
        execution, lease, build = await continuation._locked_retained(
            session, descriptor["dataset_id"], descriptor["engine_execution_id"], authorization["build_id"]
        )
        snapshot = await continuation._snapshot_row(session, authorization, lock=False)
        continuation._same_build(authorization, execution, build, snapshot)
        policy = await _continued_binding(session, descriptor, authorization)
        if inspect_only:
            authorization_record = await _continued_producer(session, descriptor, execution, lease, build)
            if authorization_record.canonical_authorization != continuation.authorization_bytes(authorization).decode(
                "utf-8"
            ) or (not hmac.compare_digest(bytes(authorization_record.nonce_sha256), hashlib.sha256(nonce).digest())):
                handoff.unavailable()
            snapshot = await continuation._snapshot_row(session, authorization, lock=False)
            continuation._same_build(authorization, execution, build, snapshot)
            continuation._eligible(execution, lease, build, snapshot, first=False)
            now = await continuation.database_now(session)
            await continuation._require_base_and_capture(session, execution, build)
            if start:
                if (
                    lease.continuation_started_at is not None
                    or lease.expires_at > now
                    or authorization_record.launch_expires_at <= now
                ):
                    handoff.unavailable()
                request = await continuation._request(
                    session, authorization_record, build, nonce, policy.build.lease_seconds
                )
            else:
                request = await continuation.reopen_build_continuation(
                    session, authorization, nonce, lease_seconds=policy.build.lease_seconds
                )
        else:
            operation = continuation.start_build_continuation if start else continuation.reopen_build_continuation
            request = await operation(session, authorization, nonce, lease_seconds=policy.build.lease_seconds)
        return request


async def run_continued_owner(factory, request, job):
    """Keep continued cancellation and publication in the existing finite epoch."""
    from process.custom_import.execution import finish_execution
    from process.custom_import.family_shard_owner_job import advance_owner_shard_job

    is_canceled = False
    try:
        await advance_owner_shard_job(factory, job, attempt_root=job.root / "source" / "attempt")
        if not job.complete or job.output is None:
            raise CandidateRunnerError("continued owner did not establish terminal output")
        status = "no_change" if job.no_change is not None else "activated" if job.publication else "sealed_unpublished"
        counts = job.source_counts
        return CandidateRunResult(
            status,
            request.execution_id,
            job.output.generation_id,
            counts.accepted_family_count,
            counts.rejection_count,
            job.output.seal,
            job.no_change or job.publication,
        )
    except CancellationRequested:
        async with factory() as session, session.begin():
            await _set_timeout(session, 2000)
            transition = await finish_execution(
                session,
                execution_id=request.execution_id,
                fence=request.fence,
                token=request.lease_token,
                terminal_state="canceled",
                terminal_reason="cancellation_requested",
                continuation_id=request.continuation.continuation_id,
            )
        is_canceled = transition.state == "canceled"
        counts = job.source_counts
        return CandidateRunResult(
            "canceled" if is_canceled else "lease_lost",
            request.execution_id,
            accepted_family_count=counts.accepted_family_count,
            rejection_count=counts.rejection_count,
        )
    except LeaseAuthorityLost:
        counts = job.source_counts
        return CandidateRunResult(
            "lease_lost",
            request.execution_id,
            accepted_family_count=counts.accepted_family_count,
            rejection_count=counts.rejection_count,
        )
    finally:
        if job.complete or is_canceled:
            await job.close_terminal()


async def run_original_owner(factory, original, job, *, advance):
    """Keep the ordinary fenced finality/result behavior without a second activation."""
    counts = SourceOutcomeCounts(0, 0)
    is_canceled = False
    try:
        await advance(factory, job, attempt_root=job.root / "source" / "attempt")
        counts = job.source_counts
        if job.is_rejected:
            status = await _finish(factory, original.request, original.captured, original.policy, "failed")
            return _result(status, original.captured, counts)
        if not job.complete or job.output is None:
            raise CandidateRunnerError("owner did not establish terminal output")
        publication = job.no_change if job.no_change is not None else job.publication
        status = (
            "no_change" if job.no_change is not None else "sealed_unpublished" if publication is None else "activated"
        )
        return _result(status, original.captured, counts, job.output, publication)
    except CancellationRequested:
        status = await _finish(factory, original.request, original.captured, original.policy, "canceled")
        is_canceled = status == "canceled"
        counts = job.source_counts
        return _result(status, original.captured, counts)
    except LeaseAuthorityLost:
        counts = job.source_counts
        return _result("lease_lost", original.captured, counts)
    except CandidateRejected:
        status = await _finish(factory, original.request, original.captured, original.policy, "failed")
        return _result(status, original.captured, counts)
    except CandidateRunnerError:
        status = await _finish(
            factory,
            original.request,
            original.captured,
            original.policy,
            "failed",
            failure_reason="segmented_build_failed",
        )
        is_canceled = status == "canceled"
        counts = job.source_counts
        return _result(status, original.captured, counts)
    finally:
        if job.complete or is_canceled:
            await job.close_terminal()


def _configured_original(loaded, execution, bundle, lease, bearer):
    """Rebuild the original statement identity without source credentials or I/O."""
    from process.custom_import.snowflake_operator_cli import _resume_connector, _retained_bundle_request

    connector = _resume_connector(loaded)
    bundle_request = _retained_bundle_request(connector, loaded)
    processing = getattr(loaded.binding, "processing_policy", None)
    identity = configured_request_identity(
        bundle_request,
        connector.build_statement(bundle_request),
        source_binding_sha256=loaded.source_binding_sha256,
        processing_policy=processing,
    )
    if execution.request_identity_sha256 is None or not hmac.compare_digest(
        bytes(execution.request_identity_sha256), identity
    ):
        handoff.unavailable()
    request = SnowflakeBundleCandidateRequest(
        dataset_id=loaded.dataset_id,
        definition_revision_id=loaded.definition_revision_id,
        schema_revision_id=loaded.schema_revision_id,
        definition=loaded.definition,
        bundle_request=bundle_request,
        idempotency_key=execution.idempotency_key,
        lease_token=bearer,
        source_binding_revision_id=loaded.source_binding_revision_id,
        source_binding_sha256=loaded.source_binding_sha256,
    )
    captured = SnowflakeCaptureResult("capture_bound", execution.execution_id, bundle.capture_bundle_id, lease.fence)
    manifest_sha = bytes(bundle.manifest_sha256).hex()
    return request, captured, identity, manifest_sha
