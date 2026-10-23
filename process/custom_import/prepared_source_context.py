# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retained object-source scope for short, fenced preparation transactions."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import ClassVar

from sqlalchemy import text

from db.models.custom_import import CustomImportCaptureBundle, CustomImportExecution
from db.models.custom_import_prepared_source import CustomImportPreparedSource, CustomImportPreparedSourcePart
from process.custom_import import build_source as staging
from process.custom_import.capture_authority import _AUTHORITY_SQL
from process.custom_import.capture_store import _identity, _validated_streams
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import lease_token_sha256
from process.custom_import.object_processing_policy import ObjectProcessingPolicy
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.source_authorization import PERMIT_CONTRACT, SOURCE_PATH, SourcePermit
from process.custom_import.source_binding_store import LoadedObjectSourceBinding, load_source_binding


@dataclass(frozen=True)
class PreparedSourceContext:
    """Persisted pins only; a caller supplies neither replay rows nor limits."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    execution_id: int
    capture_bundle_id: int
    source_binding_revision_id: int
    source_binding_sha256: bytes
    request_identity_sha256: bytes
    raw_manifest_sha256: bytes
    definition: CustomImportDefinition = field(repr=False)
    policy: ObjectProcessingPolicy
    stream_slots: tuple[tuple[str, int], ...]
    fence: int
    lease_token: str | bytes = field(repr=False)
    build_deadline_at: datetime
    source_permit: SourcePermit = field(repr=False)
    continuation: ClassVar[None] = None
    operation_deadline: ClassVar[None] = None

    @property
    def lease_seconds(self):
        """Use the immutable build policy for lease renewal."""
        return self.policy.build.lease_seconds

    @property
    def statement_timeout_ms(self):
        """Use the immutable per-statement build budget."""
        return self.policy.build.statement_timeout_ms

    @property
    def authorization_expires_at(self):
        """Keep preparation bounded by the current SOURCE permit."""
        return self.source_permit.expires_at


def _unavailable():
    return CandidateRunnerError("prepared_source_scope_invalid")


def _digest(value):
    if not isinstance(value, (bytes, memoryview, bytearray)) or len(value) != 32:
        raise _unavailable()
    return bytes(value)


def _retained_pins(execution, bundle, loaded):
    """Refuse metadata-only, mixed, cross-owner or unfrozen original captures."""

    if (
        execution is None
        or not isinstance(loaded, LoadedObjectSourceBinding)
        or execution.source_binding_revision_id != loaded.source_binding_revision_id
        or (execution.dataset_id, execution.definition_revision_id, execution.schema_revision_id)
        != (loaded.dataset_id, loaded.definition_revision_id, loaded.schema_revision_id)
        or bundle is None
        or bundle.capture_bundle_id != execution.capture_bundle_id
        or (bundle.dataset_id, bundle.definition_revision_id, bundle.schema_revision_id)
        != (loaded.dataset_id, loaded.definition_revision_id, loaded.schema_revision_id)
        or bundle.capture_state != "sealed"
        or bundle.payload_contract is not None
        or not isinstance(execution.started_at, datetime)
        or execution.started_at.tzinfo is None
    ):
        raise _unavailable()
    return _digest(execution.request_identity_sha256), _digest(bundle.manifest_sha256)


async def load_prepared_source_context(session_factory, *, execution_id, fence, token, source_permit):
    """Load approved policy and original capture pins without opening source access."""

    from process.custom_import.source_batch import _expiry, _positive_id, _read_session

    _positive_id(execution_id)
    _positive_id(fence)
    if not isinstance(source_permit, SourcePermit) or (
        source_permit.contract != PERMIT_CONTRACT
        or source_permit.method != "POST"
        or source_permit.path != SOURCE_PATH
        or source_permit.issuer != "custom-import-execution-controller"
        or source_permit.audience != "custom-import-engine"
    ):
        raise _unavailable()
    _expiry(source_permit.expires_at)
    if type(token) not in (str, bytes):
        raise _unavailable()
    lease_token_sha256(token)
    async with _read_session(session_factory) as session:
        await staging._set_timeout(session, 1000)
        execution = await session.get(CustomImportExecution, execution_id)
        if execution is None or execution.source_binding_revision_id is None or execution.capture_bundle_id is None:
            raise _unavailable()
        if (
            execution.dataset_id,
            execution.definition_revision_id,
            execution.schema_revision_id,
            execution.source_binding_revision_id,
            execution.idempotency_key,
        ) != (
            source_permit.dataset_id,
            source_permit.definition_revision_id,
            source_permit.schema_revision_id,
            source_permit.source_binding_revision_id,
            source_permit.idempotency_key,
        ):
            raise _unavailable()
        loaded = await load_source_binding(
            session,
            definition_revision_id=execution.definition_revision_id,
            source_binding_revision_id=execution.source_binding_revision_id,
        )
        bundle = await session.get(CustomImportCaptureBundle, execution.capture_bundle_id)
        _retained_pins(execution, bundle, loaded)
        if loaded.source_binding_sha256.hex() != source_permit.source_binding_sha256:
            raise _unavailable()
        streams = await _validated_streams(
            session,
            _identity(
                dataset_id=loaded.dataset_id,
                definition_revision_id=loaded.definition_revision_id,
                schema_revision_id=loaded.schema_revision_id,
            ),
        )
    return _prepared_context(execution, bundle, loaded, streams, fence, token, source_permit)


def _prepared_context(execution, bundle, loaded, streams, fence, token, source_permit):
    """Construct value-only scope from the already owner-joined immutable rows."""
    request_digest, manifest_digest = _retained_pins(execution, bundle, loaded)
    return PreparedSourceContext(
        dataset_id=loaded.dataset_id,
        definition_revision_id=loaded.definition_revision_id,
        schema_revision_id=loaded.schema_revision_id,
        execution_id=execution.execution_id,
        capture_bundle_id=bundle.capture_bundle_id,
        source_binding_revision_id=loaded.source_binding_revision_id,
        source_binding_sha256=loaded.source_binding_sha256,
        request_identity_sha256=request_digest,
        raw_manifest_sha256=manifest_digest,
        definition=loaded.definition,
        policy=loaded.binding.processing_policy,
        stream_slots=tuple(streams),
        fence=fence,
        lease_token=token,
        build_deadline_at=execution.started_at
        + timedelta(seconds=loaded.binding.processing_policy.build.build_deadline_seconds),
        source_permit=source_permit,
    )


async def require_prepared_source_authority(session):
    """Require owner-equivalent access and no non-owner prepared write grants."""

    await staging._prepare_statement(session)
    connection = await session.connection()
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    quote = connection.dialect.identifier_preparer.quote
    relations = []
    for model in (CustomImportPreparedSource, CustomImportPreparedSourcePart):
        table = model.__table__
        schema = schema_map.get(table.schema, table.schema)
        if not schema:
            raise _unavailable()
        relations.append(f"{quote(schema)}.{quote(table.name)}")
    if await session.scalar(text(_AUTHORITY_SQL), {"relations": relations, "relation_count": 2}) is not True:
        raise _unavailable()


async def require_prepared_source_context(session, context):
    """Recheck signed scope, immutable originals and policy under live page locks."""

    from process.custom_import.source_batch import _verify_source_permit

    if not isinstance(context, PreparedSourceContext):
        raise _unavailable()
    await _verify_source_permit(session, context, context.source_permit)
    await require_prepared_source_authority(session)
    await staging._prepare_statement(session)
    execution = await session.get(CustomImportExecution, context.execution_id)
    await staging._prepare_statement(session)
    bundle = await session.get(CustomImportCaptureBundle, context.capture_bundle_id)
    await staging._prepare_statement(session)
    loaded = await load_source_binding(
        session,
        definition_revision_id=context.definition_revision_id,
        source_binding_revision_id=context.source_binding_revision_id,
    )
    request_digest, manifest_digest = _retained_pins(execution, bundle, loaded)
    if (
        request_digest != context.request_identity_sha256
        or manifest_digest != context.raw_manifest_sha256
        or loaded.source_binding_sha256 != context.source_binding_sha256
        or loaded.definition != context.definition
        or loaded.binding.processing_policy != context.policy
        or execution.started_at + timedelta(seconds=context.policy.build.build_deadline_seconds)
        != context.build_deadline_at
    ):
        raise _unavailable()
