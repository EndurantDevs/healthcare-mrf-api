# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import sys
from typing import Any

from db.models import (
    PTG2ArtifactManifest,
    PTG2ContentIdentity,
    PTG2SourceFileVersion,
    PTG2SourceIdentity,
)
from process.ptg_parts.canonical import hash_prefix, semantic_hash
from process.ptg_parts.domain import (
    PTG2_ARTIFACT_RAW,
    PTG2LogicalArtifact,
    PTG2RawArtifact,
    PTG2SourceVersion,
)
from process.ptg_parts.progress import _utcnow
from process.ptg_parts.values import _source_identity_hash


async def _push_ptg2_objects_from_facade(rows: list[dict[str, Any]], cls, *, rewrite: bool = True) -> None:
    ptg_module = sys.modules.get("process.ptg")
    if ptg_module is None:
        raise RuntimeError("process.ptg facade is not loaded")
    await ptg_module._push_ptg2_objects(rows, cls, rewrite=rewrite)


async def _record_source_version(
    source_type: str,
    domain: str,
    raw_artifact: PTG2RawArtifact,
    logical_artifact: PTG2LogicalArtifact,
    import_run_id: str | None = None,
) -> PTG2SourceVersion:
    """Record one retained raw and logical source version."""
    source_identity_hash = _source_identity_hash(source_type, raw_artifact.canonical_url)
    logical_hash_deferred = bool(logical_artifact.logical_hash_deferred)
    content_identity_kind = (
        "raw_container_sha256_v1"
        if logical_hash_deferred
        else "logical_json_sha256_v1"
    )
    version_payload = {
        "source_identity_hash": source_identity_hash,
        "raw_sha256": raw_artifact.raw_sha256,
        "logical_sha256": logical_artifact.logical_sha256,
        "content_identity_kind": content_identity_kind,
        "etag": raw_artifact.head.etag if raw_artifact.head else None,
        "content_length": raw_artifact.head.content_length if raw_artifact.head else raw_artifact.byte_count,
        "last_modified": raw_artifact.head.last_modified if raw_artifact.head else None,
    }
    source_file_version_id = semantic_hash(version_payload, domain="source_file_version")[:32]
    content_hash = semantic_hash(
        {
            "domain": domain,
            "identity_kind": content_identity_kind,
            "sha256": logical_artifact.logical_sha256,
        },
        domain="content_identity",
    )
    now = _utcnow()
    await _push_ptg2_objects_from_facade(
        [
            {
                "source_identity_hash": source_identity_hash,
                "hash_prefix": hash_prefix(source_identity_hash),
                "source_type": source_type,
                "canonical_url": raw_artifact.canonical_url,
                "original_url": raw_artifact.original_url,
                "payload": {
                    "source_type": source_type,
                    "domain": domain,
                    "canonical_url": raw_artifact.canonical_url,
                    "original_url": raw_artifact.original_url,
                },
                "created_at": now,
            }
        ],
        PTG2SourceIdentity,
        rewrite=True,
    )
    await _push_ptg2_objects_from_facade(
        [
            {
                "content_hash": content_hash,
                "hash_prefix": hash_prefix(content_hash),
                "domain": domain,
                "logical_sha256": logical_artifact.logical_sha256,
                "canonical_payload": {
                    "domain": domain,
                    "logical_sha256": logical_artifact.logical_sha256,
                    "compression": logical_artifact.compression,
                    "member_name": logical_artifact.member_name,
                    "logical_hash_deferred": logical_hash_deferred,
                    "identity_kind": content_identity_kind,
                },
                "created_at": now,
            }
        ],
        PTG2ContentIdentity,
        rewrite=True,
    )
    await _push_ptg2_objects_from_facade(
        [
            {
                "source_file_version_id": source_file_version_id,
                "source_identity_hash": source_identity_hash,
                "content_hash": content_hash,
                "raw_storage_uri": raw_artifact.raw_storage_uri,
                "raw_sha256": raw_artifact.raw_sha256,
                "logical_sha256": logical_artifact.logical_sha256,
                "content_length": raw_artifact.head.content_length if raw_artifact.head else raw_artifact.byte_count,
                "etag": raw_artifact.head.etag if raw_artifact.head else None,
                "last_modified": raw_artifact.head.last_modified if raw_artifact.head else None,
                "reuse_policy": "metadata_or_hash",
                "verification_mode": raw_artifact.verification_mode,
                "reused_from_source_file_version_id": raw_artifact.reused_from_source_file_version_id,
                "verified_at": now,
                "created_at": now,
                "payload": {
                    "import_run_id": import_run_id,
                    "reused": raw_artifact.reused,
                    "logical_byte_count": logical_artifact.byte_count,
                    "raw_byte_count": raw_artifact.byte_count,
                    "logical_hash_deferred": logical_hash_deferred,
                },
            }
        ],
        PTG2SourceFileVersion,
        rewrite=True,
    )
    await _push_ptg2_objects_from_facade(
        [
            {
                "artifact_id": semantic_hash(
                    {"kind": PTG2_ARTIFACT_RAW, "storage_uri": raw_artifact.raw_storage_uri},
                    domain="artifact_manifest",
                )[:32],
                "snapshot_id": None,
                "import_run_id": import_run_id,
                "artifact_kind": PTG2_ARTIFACT_RAW,
                "storage_uri": raw_artifact.raw_storage_uri,
                "sha256": raw_artifact.raw_sha256,
                "byte_count": raw_artifact.byte_count,
                "payload": {
                    "canonical_url": raw_artifact.canonical_url,
                    "verification_mode": raw_artifact.verification_mode,
                    "reused": raw_artifact.reused,
                },
                "created_at": now,
            }
        ],
        PTG2ArtifactManifest,
        rewrite=True,
    )
    return PTG2SourceVersion(
        source_identity_hash=source_identity_hash,
        source_file_version_id=source_file_version_id,
        original_url=raw_artifact.original_url,
        canonical_url=raw_artifact.canonical_url,
        raw_storage_uri=raw_artifact.raw_storage_uri,
        raw_sha256=raw_artifact.raw_sha256,
        logical_sha256=logical_artifact.logical_sha256,
        logical_hash_deferred=logical_hash_deferred,
        content_length=raw_artifact.head.content_length if raw_artifact.head else raw_artifact.byte_count,
        etag=raw_artifact.head.etag if raw_artifact.head else None,
        last_modified=raw_artifact.head.last_modified if raw_artifact.head else None,
        verification_mode=raw_artifact.verification_mode,
        reused_from_source_file_version_id=raw_artifact.reused_from_source_file_version_id,
    )
