# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Resolve immutable retained source identity without entering source acquisition."""

import hashlib
from dataclasses import dataclass

from sqlalchemy import text

from db.models.custom_import import CustomImportCaptureBundle, CustomImportExecution
from db.models.custom_import_retained_copy import CustomImportRetainedCaptureCopy
from process.custom_import import family_shard_handoff as custody
from process.custom_import import retained_capture_copy_contract as contract
from process.custom_import.capture_authority import _AUTHORITY_SQL
from process.custom_import.capture_pending import SEGMENTED_PAYLOAD_CONTRACT, PendingCaptureRequest
from process.custom_import.capture_retention import require_capture_retention_read_access
from process.custom_import.capture_store import _load_parquet_part_metadata
from process.custom_import.snowflake_candidate import SnowflakeBundleCandidateRequest, _validate_bound_source_identity
from process.custom_import.snowflake_capture import _validate_retained_source
from process.custom_import.snowflake_operator_cli import _resume_connector, _retained_bundle_request
from process.custom_import.snowflake_segmented_runner import _validated_policy, configured_request_identity
from process.custom_import.snowflake_source_binding import load_snowflake_source_binding

LOCATOR_FIELDS = frozenset(
    {
        "dataset_id",
        "definition_revision_id",
        "source_binding_revision_id",
        "controller_execution_id",
        "source_capture_bundle_id",
        "source_capture_manifest_sha256",
    }
)


@dataclass(frozen=True)
class CopySource:
    """Immutable old producer coordinates and verified sealed headers."""

    execution_id: int
    fence: int
    token_sha256: bytes
    capture_bundle_id: int
    manifest_sha256: bytes
    snapshot_token: str
    canonical_manifest: str
    receipts: tuple
    stream_slots: tuple


@dataclass(frozen=True)
class CopyInput:
    """Current registered request plus one exact retained capture."""

    request: object
    statement: object
    policy: object
    identity: bytes
    source: CopySource

    def pending(self, execution_id, fence):
        """Retain original source evidence while assigning the new producer."""
        return PendingCaptureRequest(
            dataset_id=self.request.dataset_id,
            definition_revision_id=self.request.definition_revision_id,
            schema_revision_id=self.request.schema_revision_id,
            execution_id=execution_id,
            fence=fence,
            token=self.request.lease_token,
            request_identity_sha256=self.identity,
            source_request_sha256=bytes.fromhex(self.statement.request.request_sha256),
            statement_sha256=bytes.fromhex(self.statement.statement_sha256),
            source_snapshot_token=self.source.snapshot_token,
            policy=self.policy.capture,
            source_binding_revision_id=self.request.source_binding_revision_id,
            source_binding_sha256=self.request.source_binding_sha256,
        )


def _configured(loaded, locator, bearer):
    connector = _resume_connector(loaded)
    bundle_request = _retained_bundle_request(connector, loaded)
    statement = connector.build_statement(bundle_request)
    policy = _validated_policy(getattr(loaded.binding, "processing_policy", None))
    request = SnowflakeBundleCandidateRequest(
        dataset_id=loaded.dataset_id,
        definition_revision_id=loaded.definition_revision_id,
        schema_revision_id=loaded.schema_revision_id,
        definition=loaded.definition,
        bundle_request=bundle_request,
        idempotency_key=locator["controller_execution_id"],
        lease_token=bearer,
        source_binding_revision_id=loaded.source_binding_revision_id,
        source_binding_sha256=loaded.source_binding_sha256,
    )
    identity = configured_request_identity(
        bundle_request, statement, source_binding_sha256=loaded.source_binding_sha256, processing_policy=policy
    )
    return request, statement, policy, identity


def _require_old_scope(bundle, request, identity):
    expected_by_field = {
        "dataset_id": request.dataset_id,
        "definition_revision_id": request.definition_revision_id,
        "schema_revision_id": request.schema_revision_id,
        "source_binding_revision_id": request.source_binding_revision_id,
        "payload_contract": SEGMENTED_PAYLOAD_CONTRACT,
        "capture_state": "sealed",
        "request_identity_sha256": identity,
        "source_binding_sha256": request.source_binding_sha256,
    }
    if bundle is None or any(getattr(bundle, name) != value for name, value in expected_by_field.items()):
        custody.unavailable()


async def load_source(session, locator, bearer):
    """Fail closed on changed configuration, incomplete evidence or foreign ownership."""
    if type(locator) is not dict or not LOCATOR_FIELDS <= locator.keys():
        custody.unavailable()
    await require_control_authority(session)
    loaded = await load_snowflake_source_binding(
        session,
        definition_revision_id=locator["definition_revision_id"],
        source_binding_revision_id=locator["source_binding_revision_id"],
    )
    request, statement, policy, identity = _configured(loaded, locator, custody.checked_bearer(bearer))
    if locator["dataset_id"] != loaded.dataset_id:
        custody.unavailable()
    await _validate_bound_source_identity(session, request, statement, policy)
    bundle = await session.get(CustomImportCaptureBundle, locator["source_capture_bundle_id"])
    _require_old_scope(bundle, request, identity)
    if (
        bytes(bundle.manifest_sha256).hex() != locator["source_capture_manifest_sha256"]
        or bundle.canonical_policy != policy.capture.canonical
        or bytes(bundle.policy_sha256).hex() != policy.capture.digest
        or bytes(bundle.source_request_sha256).hex() != statement.request.request_sha256
        or bytes(bundle.statement_sha256).hex() != statement.statement_sha256
    ):
        custody.unavailable()
    producer = await session.get(CustomImportExecution, bundle.producing_execution_id)
    if producer is None or (
        producer.capture_bundle_id,
        producer.dataset_id,
        producer.definition_revision_id,
        producer.schema_revision_id,
        producer.source_binding_revision_id,
    ) != (
        bundle.capture_bundle_id,
        request.dataset_id,
        request.definition_revision_id,
        request.schema_revision_id,
        request.source_binding_revision_id,
    ):
        custody.unavailable()
    await require_capture_retention_read_access(session)
    receipts, _, _ = await _load_parquet_part_metadata(session, request_identity(request), bundle.capture_bundle_id)
    await _validate_retained_source(session, request, statement, bundle)
    source_capture = CopySource(
        bundle.producing_execution_id,
        bundle.producing_fence,
        bytes(bundle.producing_token_sha256),
        bundle.capture_bundle_id,
        bytes(bundle.manifest_sha256),
        bundle.snapshot_token,
        bundle.canonical_manifest,
        tuple(receipts[slot] for slot in sorted(receipts)),
        tuple((receipts[slot].stream_id, slot) for slot in sorted(receipts)),
    )
    return CopyInput(request, statement, policy, identity, source_capture)


def request_identity(request):
    """Use the existing store identity without a new content-hash definition."""
    from process.custom_import.capture_store import _identity

    return _identity(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
    )


def descriptor_fields(value):
    """Expose only native scope and unchanged semantic identities to the signer."""
    request, source = value.request, value.source
    return {
        "contract": contract.CONTRACT,
        "controller_execution_id": request.idempotency_key,
        "dataset_id": request.dataset_id,
        "definition_revision_id": request.definition_revision_id,
        "schema_revision_id": request.schema_revision_id,
        "source_binding_revision_id": request.source_binding_revision_id,
        "source_binding_sha256": request.source_binding_sha256.hex(),
        "source_capture_bundle_id": source.capture_bundle_id,
        "source_capture_manifest_sha256": source.manifest_sha256.hex(),
        "request_identity_sha256": value.identity.hex(),
        "source_request_sha256": value.statement.request.request_sha256,
        "statement_sha256": value.statement.statement_sha256,
        "policy_sha256": value.policy.capture.digest,
        "bearer_sha256": hashlib.sha256(request.lease_token).hexdigest(),
    }


def require_descriptor(value, descriptor):
    """Compare every independently reconstructed source coordinate."""
    if any(descriptor[name] != expected for name, expected in descriptor_fields(value).items()):
        custody.unavailable()


async def require_control_authority(session):
    """Protect only the new copy ledger; legacy segmented grants remain supported."""
    connection = await session.connection()
    schema = CustomImportRetainedCaptureCopy.__table__.schema
    translations = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = translations.get(schema, schema)
    if not isinstance(schema, str) or not schema:
        custody.unavailable()
    quote = connection.dialect.identifier_preparer.quote
    identity = quote(schema) + "." + quote(CustomImportRetainedCaptureCopy.__tablename__)
    allowed = await session.scalar(text(_AUTHORITY_SQL), {"relations": [identity], "relation_count": 1})
    if allowed is not True:
        custody.unavailable()
