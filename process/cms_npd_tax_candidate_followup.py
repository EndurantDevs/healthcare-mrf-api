# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recoverable post-publication CMS tax-candidate report."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any

from sqlalchemy import text

from process.cms_npd_tax_candidate_lookup import current_sealed_v4_tax_pin
from process.cms_npd_tax_candidate_runner import (
    CMS_NPD_NPI_ONLY_POLICY,
    _file_sha256,
    _release_witness,
    _retain_report,
    run_admitted_cms_tax_candidate_report,
)


def _report_path(
    release_directory: Path,
    *,
    dataset_id: str,
    vector_sha256: str,
    snapshot_id: str,
    snapshot_key: int,
    manifest_sha256: str,
    cutoff: str,
) -> Path:
    directory = release_directory / "tax-candidates"
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.is_symlink() or stat.S_IMODE(directory.stat().st_mode) & 0o077:
        raise ValueError("CMS tax candidate report directory is not private")
    report_identity_parts = [
        dataset_id,
        vector_sha256,
        snapshot_id,
        snapshot_key,
        manifest_sha256,
        CMS_NPD_NPI_ONLY_POLICY.descriptor_sha256,
        cutoff,
    ]
    digest = hashlib.sha256(json.dumps(report_identity_parts, separators=(",", ":")).encode("utf-8")).hexdigest()
    return directory / f"{digest}.json"


def _receipt_path(report_path: Path) -> Path:
    return report_path.with_suffix(".receipt.json")


def _retain_receipt(report_path: Path, result: dict[str, Any]) -> None:
    """Seal the exact completed report hash for cheap, safe unchanged checks."""

    receipt_by_field = {
        "contract": "cms-npd-tax-candidate-receipt-v1",
        "dataset_id": result["dataset_id"],
        "vector_sha256": result["vector_sha256"],
        "generated_at": result["generated_at"],
        "snapshot_id": result["snapshot_id"],
        "snapshot_key": result["snapshot_key"],
        "tax_manifest_sha256": result["tax_manifest_sha256"],
        "report_sha256": result["report_sha256"],
        "candidate_organization_count": result["candidate_organization_count"],
    }
    encoded = (json.dumps(receipt_by_field, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(prefix=".cms-tax-receipt-", dir=report_path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        _retain_report(temporary_path, _receipt_path(report_path), hashlib.sha256(encoded).hexdigest())
    finally:
        temporary_path.unlink(missing_ok=True)


def _verified_report_receipt(report_path: Path, expected_by_field: dict[str, Any]) -> tuple[int, str] | None:
    """Require a private exact receipt and its completed report bytes."""

    receipt_path = _receipt_path(report_path)
    if (
        report_path.is_symlink()
        or receipt_path.is_symlink()
        or not report_path.is_file()
        or not receipt_path.is_file()
        or stat.S_IMODE(report_path.stat().st_mode) & 0o077
        or stat.S_IMODE(receipt_path.stat().st_mode) & 0o077
        or receipt_path.stat().st_size > 4096
    ):
        return None
    receipt_by_field = json.loads(receipt_path.read_bytes())
    if (
        not isinstance(receipt_by_field, dict)
        or any(receipt_by_field.get(field) != value for field, value in expected_by_field.items())
        or set(receipt_by_field) != set(expected_by_field) | {"report_sha256", "candidate_organization_count"}
    ):
        return None
    count = receipt_by_field["candidate_organization_count"]
    digest = receipt_by_field["report_sha256"]
    if type(count) is not int or count < 0 or type(digest) is not str or len(digest) != 64:
        return None
    return (count, digest) if _file_sha256(report_path) == digest else None


async def _current_tax_pin(fhir: Any) -> tuple[str, int, str] | None:
    async with fhir.db.session() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        return await current_sealed_v4_tax_pin(session, schema_name=fhir._schema())


async def completed_cms_tax_candidate_report(
    fhir: Any,
    *,
    release_directory: Path,
    dataset_id: str,
    vector_sha256: str,
    generated_at: str,
) -> dict[str, Any] | None:
    """Reuse only an intact report for this release and current sealed tax snapshot."""

    try:
        if release_directory.name != vector_sha256 or _release_witness(release_directory)[2] != vector_sha256:
            return None
        generated_date = dt.date.fromisoformat(generated_at)
        if generated_date.isoformat() != generated_at or not (release_directory / "tax-candidates").is_dir():
            return None
        cutoff = f"{generated_at}T23:59:59.999999Z"
        pin = await _current_tax_pin(fhir)
        if pin is None:
            return None
        snapshot_id, snapshot_key, manifest_sha256 = pin
        report_path = _report_path(
            release_directory,
            dataset_id=dataset_id,
            vector_sha256=vector_sha256,
            snapshot_id=snapshot_id,
            snapshot_key=snapshot_key,
            manifest_sha256=manifest_sha256,
            cutoff=cutoff,
        )
        verified = _verified_report_receipt(
            report_path,
            {
                "contract": "cms-npd-tax-candidate-receipt-v1",
                "dataset_id": dataset_id,
                "vector_sha256": vector_sha256,
                "generated_at": generated_at,
                "snapshot_id": snapshot_id,
                "snapshot_key": snapshot_key,
                "tax_manifest_sha256": manifest_sha256,
            },
        )
        if verified is None:
            return None
        count, digest = verified
        return {
            "status": "complete" if count else "empty",
            "retryable": False,
            "snapshot_id": snapshot_id,
            "snapshot_key": snapshot_key,
            "tax_manifest_sha256": manifest_sha256,
            "report_sha256": digest,
            "candidate_organization_count": count,
        }
    except OSError, ValueError, KeyError, TypeError:
        return None


async def cms_npd_tax_candidate_followup(
    fhir: Any,
    *,
    release_directory: Path,
    dataset_id: str,
    vector_sha256: str,
    generated_at: str,
) -> dict[str, Any]:
    """Retain evidence after publication; retry requires a same-byte import replay.

    This function does not schedule work. An unchanged-vector CMS import or an
    explicit retained-release replay must invoke it again after a failure.
    """

    try:
        generated_date = dt.date.fromisoformat(generated_at)
        if generated_date.isoformat() != generated_at or release_directory.name != vector_sha256:
            raise ValueError("CMS tax candidate release identity is invalid")
        cutoff = f"{generated_at}T23:59:59.999999Z"
        pin = await _current_tax_pin(fhir)
        if pin is None:
            return {"status": "unavailable", "retryable": True, "retry_via": "same_byte_import"}
        snapshot_id, snapshot_key, manifest_sha256 = pin
        output_path = _report_path(
            release_directory,
            dataset_id=dataset_id,
            vector_sha256=vector_sha256,
            snapshot_id=snapshot_id,
            snapshot_key=snapshot_key,
            manifest_sha256=manifest_sha256,
            cutoff=cutoff,
        )
        report_result = await run_admitted_cms_tax_candidate_report(
            fhir.db.session,
            release_directory=release_directory,
            output_path=output_path,
            schema_name=fhir._schema(),
            dataset_id=dataset_id,
            snapshot_key=snapshot_key,
            manifest_sha256=manifest_sha256,
            evidence_as_of=cutoff,
        )
        result_by_field = {
            "status": "complete" if report_result.candidate_organization_count else "empty",
            "retryable": False,
            "snapshot_id": snapshot_id,
            "snapshot_key": snapshot_key,
            "tax_manifest_sha256": manifest_sha256,
            "report_sha256": report_result.sha256,
            "candidate_organization_count": report_result.candidate_organization_count,
        }
        _retain_receipt(
            output_path,
            {**result_by_field, "dataset_id": dataset_id, "vector_sha256": vector_sha256, "generated_at": generated_at},
        )
        return result_by_field
    except Exception:
        # This follows the committed source-local publication. A same-byte
        # import replay reruns this step and can reuse an exact report artifact.
        return {"status": "failed", "retryable": True, "retry_via": "same_byte_import"}
