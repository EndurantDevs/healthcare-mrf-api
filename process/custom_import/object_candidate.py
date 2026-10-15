# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native object acquisition using the existing fenced capture and replay lifecycle."""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import nullcontext
from dataclasses import asdict, dataclass, field
from functools import partial
from pathlib import Path
from typing import Literal

from process.custom_import import https_candidate, s3, upload
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.runner_types import CandidateRunResult, SessionFactory
from process.custom_import.s3_credentials import FixedLocalS3CredentialProvider

FIXED_S3_CREDENTIAL_DIRECTORY = Path("/run/custom-import-operator")
FIXED_UPLOAD_DIRECTORY = Path("/var/lib/custom-import-operator/uploads")


class ObjectCandidateError(ValueError):
    """A fixed acquisition failure without source descriptors or credentials."""


@dataclass(frozen=True)
class ObjectCandidateRequest:
    """A complete native bundle; physical access is composed separately by the operator."""

    source_kind: Literal["s3", "upload"]
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    definition: CustomImportDefinition = field(repr=False)
    objects: tuple[s3.S3Object | upload.UploadObject, ...]
    source_snapshot_token: str = field(repr=False)
    idempotency_key: str = field(repr=False)
    lease_token: str | bytes = field(repr=False)
    limits: CaptureLimits
    timeout_seconds: float
    complete_scope: bool
    maximum_dataset_retained_bytes: int


def _checked_request(request):
    if type(request) is not ObjectCandidateRequest or request.source_kind not in {"s3", "upload"}:
        raise ObjectCandidateError("object_request_invalid")
    expected_type = s3.S3Object if request.source_kind == "s3" else upload.UploadObject
    if not isinstance(request.objects, tuple) or not all(type(item) is expected_type for item in request.objects):
        raise ObjectCandidateError("object_request_invalid")
    request = https_candidate._checked_request_fields(request)
    for item, stream in zip(request.objects, request.definition.source_streams, strict=True):
        if request.source_kind == "s3":
            s3.validate_s3_object(item, stream, limits=request.limits, timeout_seconds=request.timeout_seconds)
        else:
            upload.upload_object_identity(item)
    return request


def object_request_identity_sha256(request: ObjectCandidateRequest) -> bytes:
    """Bind stable source identity and budgets without opening, signing or locating objects."""
    try:
        request = _checked_request(request)
        contract = f"custom-import/{request.source_kind}-request/v1"
        identity_of = s3.s3_object_identity_sha256 if request.source_kind == "s3" else upload.upload_object_identity
        objects = [identity_of(item) for item in request.objects]
        document_by_key = {
            "contract": contract,
            "dataset_id": request.dataset_id,
            "definition_revision_id": request.definition_revision_id,
            "schema_revision_id": request.schema_revision_id,
            "definition_sha256": request.definition.digest,
            "source_snapshot_token": request.source_snapshot_token,
            "complete_scope": request.complete_scope,
            "limits": asdict(request.limits),
            "timeout_seconds": request.timeout_seconds,
            "maximum_dataset_retained_bytes": request.maximum_dataset_retained_bytes,
            "objects": [item.hex() if isinstance(item, bytes) else item for item in objects],
        }
        encoded = json.dumps(document_by_key, allow_nan=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(contract.encode("ascii") + b"\0request\0" + encoded).digest()
    except Exception:
        raise ObjectCandidateError("object_request_invalid") from None


async def _acquire_object(request, item, stream, *, credential_provider, upload_provider, **options):
    if request.source_kind == "s3":
        provider_context = (
            FixedLocalS3CredentialProvider(FIXED_S3_CREDENTIAL_DIRECTORY)
            if credential_provider is None
            else nullcontext(credential_provider)
        )
        with provider_context as provider:
            return await s3.acquire_s3(item, stream, credential_provider=provider, **options)
    provider = upload.StagedUploadProvider(FIXED_UPLOAD_DIRECTORY) if upload_provider is None else upload_provider
    return await upload.acquire_upload(item, stream, provider=provider, **options)


async def run_object_candidate(
    session_factory: SessionFactory,
    request: ObjectCandidateRequest,
    *,
    resume: bool = False,
    credential_provider=None,
    upload_provider=None,
) -> CandidateRunResult:
    """Acquire only an unbound first lease; bound resume never accesses either provider."""
    try:
        if not callable(session_factory) or not isinstance(resume, bool):
            raise ObjectCandidateError("object_request_invalid")
        request = _checked_request(request)
        identity = object_request_identity_sha256(request)
        acquire_one = partial(
            _acquire_object, request, credential_provider=credential_provider, upload_provider=upload_provider
        )
        return await https_candidate._run(session_factory, request, identity, resume=resume, acquire_one=acquire_one)
    except asyncio.CancelledError:
        raise asyncio.CancelledError from None
    except Exception:
        raise ObjectCandidateError("object_candidate_failed") from None
