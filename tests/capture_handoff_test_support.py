# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic capture pins shared by focused handoff boundary regressions."""

from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from io import BytesIO
from types import SimpleNamespace

from sanic.compat import Header

from process.custom_import import admission_authorization as common
from process.custom_import import capture_authorization as authorization
from process.custom_import.capture import capture_stream
from process.custom_import.capture_wire import expected_content_sha256
from process.custom_import.definition import canonical_json
from process.custom_import.https_candidate import https_request_identity_sha256
from tests.test_custom_import_https_candidate import _request

NOW = datetime(2030, 1, 1, tzinfo=timezone.utc)
ORIGIN = "https://engine.example.invalid"


def _permit_document(request):
    return {
        "audience": "custom-import-engine",
        "contract": authorization.PERMIT_CONTRACT,
        "dataset_id": request.dataset_id,
        "definition_revision_id": request.definition_revision_id,
        "schema_revision_id": request.schema_revision_id,
        "definition_sha256": request.definition.digest,
        "expected_content_sha256": expected_content_sha256(
            request.definition, request.objects, request.source_snapshot_token
        ),
        "request_identity_sha256": https_request_identity_sha256(request, raw_originals=True).hex(),
        "idempotency_key": request.idempotency_key,
        "issued_at": "2030-01-01T00:00:00Z",
        "expires_at": "2030-01-01T00:15:00Z",
        "issuer": "custom-import-execution-controller",
        "limits": asdict(request.limits),
        "maximum_dataset_retained_bytes": request.maximum_dataset_retained_bytes,
        "timeout_seconds": request.timeout_seconds,
        "method": "POST",
        "origin": ORIGIN,
        "path": authorization.CAPTURE_PATH,
    }


def fixture():
    """Create complete pinned originals and a distinct, signed capture permit."""

    request, payloads_by_stream = _request()
    request = replace(request, maximum_dataset_retained_bytes=1_000_000)
    captures = tuple(
        capture_stream(
            BytesIO(payloads_by_stream[stream.stream_id]),
            stream,
            source_snapshot_token=request.source_snapshot_token,
        )
        for stream in request.definition.source_streams
    )
    ring = common.AdmissionKeyring("synthetic", (("synthetic", b"s" * 32),))
    signed = authorization.sign_permit(
        canonical_json(_permit_document(request)).encode("ascii"),
        expected_origin=ORIGIN,
        trusted_now=NOW,
        launch_expires_at=NOW + timedelta(hours=1),
        keyring=ring,
    )
    grant = SimpleNamespace(execution_id=40, fence=1)
    headers = Header(
        [
            ("Content-Type", "application/octet-stream"),
            ("Authorization", common.encode_lease_bearer(request.lease_token)),
            (authorization.CONTEXT_HEADER, signed.context),
            (authorization.KEY_ID_HEADER, signed.key_id),
            (authorization.SIGNATURE_HEADER, signed.signature),
            (authorization.EXECUTION_ID_HEADER, str(grant.execution_id)),
            (authorization.FENCE_HEADER, str(grant.fence)),
        ]
    )
    verified = authorization.verify_request(
        headers=[(str(name), header_value) for name, header_value in headers.items()],
        method="POST",
        path=authorization.CAPTURE_PATH + "-preflight",
        query_string="",
        trusted_now=NOW,
        expected_origin=ORIGIN,
        keyring=ring,
    )
    return SimpleNamespace(
        request=request,
        captures=captures,
        signed=signed,
        grant=grant,
        headers=headers,
        verified=verified,
        ring=ring,
    )
