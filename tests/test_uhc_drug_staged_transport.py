# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import hashlib
import io
import json
from contextlib import contextmanager
from pathlib import Path

import pytest

import process.formulary_fhir.uhc_drug_spool as spool
import process.formulary_fhir.uhc_drug_staged_validation as staged_validation
import process.formulary_fhir.uhc_drug_transport as transport
from tests.test_uhc_drug_transport_boundaries import _identity
from tests.uhc_drug_parser_test_support import artifact_set
from tests.uhc_drug_parser_test_support import source_record


INVALID_SCALAR_PAYLOADS = (
    b'[{"value":' + (b"9" * 5_000) + b"}]",
    b'[{"value":1e999999999999999999999999}]',
    b'[{"value":"\\ud800"}]',
)


def _invalid_body() -> bytes:
    return json.dumps(
        [source_record(rxnorm_id="not-numeric")], sort_keys=True, separators=(",", ":")
    ).encode()


def test_stage_semantics_reject_source_data_but_preserve_local_io(tmp_path) -> None:
    invalid_body = _invalid_body()
    identity = _identity(expected_byte_count=len(invalid_body))
    source_path = tmp_path / "artifact.json"
    source_path.write_bytes(invalid_body)
    artifact_sha256 = hashlib.sha256(invalid_body).hexdigest()

    with pytest.raises(transport.UHCDrugArtifactAcquisitionError) as caught:
        transport.validate_staged_uhc_drug_artifact(
            source_path, identity, artifact_sha256, len(invalid_body)
        )
    assert caught.value.retryable is False
    assert caught.value.failure_evidence == ("artifact_rejected",)

    with pytest.raises(FileNotFoundError):
        transport.validate_staged_uhc_drug_artifact(
            tmp_path / "missing.json", identity, artifact_sha256, len(invalid_body)
        )


def test_staged_and_retained_validation_apply_spool_merge_semantics(
    monkeypatch,
    tmp_path,
) -> None:
    conflicting_record = source_record()
    conflicting_record["plans"].append(
        {**conflicting_record["plans"][0], "drug_tier": "Conflicting tier"}
    )
    artifacts, bodies_by_name = artifact_set({0: [conflicting_record]})
    artifact = artifacts.artifacts[0]
    body = bodies_by_name[artifact.identity.file_name]
    source_path = tmp_path / artifact.identity.file_name
    source_path.write_bytes(body)

    @contextmanager
    def open_retained(_artifact):
        yield io.BytesIO(body)

    monkeypatch.setattr(
        staged_validation,
        "open_verified_source_artifact",
        open_retained,
    )
    validations = (
        lambda: staged_validation.validate_staged_uhc_drug_artifact(
            source_path,
            artifact.identity,
            artifact.artifact_sha256,
            artifact.artifact_byte_count,
        ),
        lambda: staged_validation.validate_retained_uhc_drug_artifact(artifact),
    )
    for validate in validations:
        with pytest.raises(transport.UHCDrugArtifactAcquisitionError) as caught:
            validate()
        assert caught.value.failure_evidence == ("artifact_rejected",)

    monkeypatch.setattr(
        spool.sqlite3,
        "connect",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("synthetic local spool failure")
        ),
    )
    with pytest.raises(OSError, match="local spool failure"):
        validations[0]()


@pytest.mark.parametrize("invalid_body", INVALID_SCALAR_PAYLOADS)
def test_stage_semantics_reject_invalid_scalars(
    tmp_path: Path,
    invalid_body: bytes,
) -> None:
    identity = _identity(expected_byte_count=len(invalid_body))
    source_path = tmp_path / "artifact.json"
    source_path.write_bytes(invalid_body)

    with pytest.raises(transport.UHCDrugArtifactAcquisitionError) as caught:
        transport.validate_staged_uhc_drug_artifact(
            source_path,
            identity,
            hashlib.sha256(invalid_body).hexdigest(),
            len(invalid_body),
        )
    assert caught.value.failure_evidence == ("artifact_rejected",)


@pytest.mark.asyncio
async def test_download_stage_rejects_structurally_valid_invalid_records(monkeypatch, tmp_path) -> None:
    invalid_body = _invalid_body()
    identity = _identity(expected_byte_count=len(invalid_body))
    monkeypatch.setattr(transport, "_download_directory", lambda: tmp_path)

    async def download_invalid(_session, _identity, output_file, **_keywords):
        output_file.write(invalid_body)
        return hashlib.sha256(invalid_body).hexdigest(), len(invalid_body)

    monkeypatch.setattr(transport, "stream_uhc_drug_response", download_invalid)
    with pytest.raises(transport.UHCDrugArtifactAcquisitionError) as caught:
        await transport._download_to_stage(
            object(), identity, max_bytes=len(invalid_body), cancel_check=None
        )
    assert caught.value.failure_evidence == ("artifact_rejected",)
    assert tuple(tmp_path.glob("*.part")) == ()


async def _claim_check() -> None:
    return None


async def _acquire_with(monkeypatch, failure):
    monkeypatch.setattr(transport, "_download_to_stage", failure)
    return await transport._acquire_identity(
        object(),
        _identity(),
        asyncio.Semaphore(1),
        database=object(),
        cancel_check=None,
        claim_check=_claim_check,
        max_bytes=100,
    )


@pytest.mark.asyncio
async def test_acquire_identity_skips_marked_artifact_rejection(monkeypatch) -> None:
    async def reject_artifact(*_arguments, **_keywords):
        raise transport.UHCDrugArtifactAcquisitionError(
            "synthetic artifact rejection", failure_evidence=("artifact_rejected",)
        )

    identity = _identity()
    assert await _acquire_with(monkeypatch, reject_artifact) == (
        0,
        identity.family,
        identity.file_name,
        identity.source_file_id,
        True,
    )


@pytest.mark.asyncio
async def test_acquire_identity_preserves_local_staging_failure(monkeypatch) -> None:
    async def fail_staging(*_arguments, **_keywords):
        raise transport.UHCDrugArtifactAcquisitionError("synthetic local staging failure")

    with pytest.raises(transport.UHCDrugArtifactAcquisitionError, match="local staging failure"):
        await _acquire_with(monkeypatch, fail_staging)


@pytest.mark.asyncio
async def test_acquire_identity_excludes_retryable_source_failure(monkeypatch) -> None:
    async def fail_retryably(*_arguments, **_keywords):
        raise transport.UHCDrugArtifactAcquisitionError(
            "synthetic retryable failure",
            retryable=True,
        )

    identity = _identity()
    assert await _acquire_with(monkeypatch, fail_retryably) == (
        0,
        identity.family,
        identity.file_name,
        identity.source_file_id,
        True,
    )
