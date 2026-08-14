# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import io
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import process.formulary_fhir.uhc_drug_acquisition as acquisition
import process.formulary_fhir.uhc_drug_staged_validation as staged_validation
import process.formulary_fhir.uhc_drug_transport as transport
from process.provider_directory_retained_artifact_base import RetainedArtifactError
from tests.test_uhc_drug_acquisition import _Response
from tests.test_uhc_drug_acquisition import _Session
from tests.test_uhc_drug_acquisition import _identity
from tests.test_uhc_drug_acquisition import _install_acquisition_mocks
from tests.test_uhc_drug_acquisition import _session_factory
from tests.test_uhc_drug_acquisition import _verified_set
from tests.test_uhc_drug_acquisition import VALID_BODY


def _partial_acquisition_case(monkeypatch, tmp_path):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(tmp_path / "artifacts"))
    monkeypatch.setenv("HLTHPRT_UHC_FORMULARY_MIN_FREE_BYTES", "1")
    staging_root = tmp_path / "staging"
    staging_root.mkdir()
    monkeypatch.setattr(transport, "_download_directory", lambda: staging_root)
    identities = tuple(_identity(index) for index in range(48))
    retained_identity, rejected_identity = identities[:2]
    pending_identities = retained_identity, rejected_identity
    selected_identities = tuple(identity for identity in identities if identity != rejected_identity)
    selected_artifacts = _verified_set(selected_identities)
    session = _Session(
        {
            retained_identity.source_url: _Response(
                retained_identity.source_url,
                (VALID_BODY,),
                declared_length=len(VALID_BODY),
            ),
            rejected_identity.source_url: _Response(rejected_identity.source_url, (), status=302),
        }
    )
    bind = AsyncMock()
    _install_acquisition_mocks(
        monkeypatch, identities, selected_artifacts, pending_identities, bind=bind
    )
    selected_loader = AsyncMock(return_value=selected_artifacts)
    monkeypatch.setattr(acquisition, "load_selected_source_artifact_set", selected_loader)
    return SimpleNamespace(
        identities=identities,
        selected_identities=selected_identities,
        selected_artifacts=selected_artifacts,
        session=session,
        bind=bind,
        selected_loader=selected_loader,
        database=object(),
    )


@pytest.mark.asyncio
async def test_nonretryable_artifact_rejection_selects_verified_partial_set(monkeypatch, tmp_path):
    case = _partial_acquisition_case(monkeypatch, tmp_path)
    acquisition_result = await acquisition.acquire_uhc_drug_artifacts(
        {"retained": "proof"},
        database=case.database,
        session_factory=_session_factory(case.session),
    )

    assert acquisition_result.file_count == 47
    assert acquisition_result.expected_file_count == 48
    assert acquisition_result.excluded_file_count == 1
    assert acquisition_result.is_coverage_complete is False
    assert acquisition_result.downloaded_file_count == 1
    assert acquisition_result.reused_file_count == 46
    assert acquisition_result.downloaded_byte_count == len(VALID_BODY)
    assert acquisition_result.artifacts is case.selected_artifacts
    assert acquisition_result.excluded_source_file_ids == (
        case.identities[1].source_file_id,
    )
    assert case.identities[1].source_file_id not in repr(acquisition_result)
    assert set(case.session.requested_urls) == {
        case.identities[0].source_url,
        case.identities[1].source_url,
    }
    case.bind.assert_awaited_once()
    acquisition.load_complete_source_artifact_set.assert_not_awaited()
    case.selected_loader.assert_awaited_once_with(
        case.identities,
        selected_source_file_ids=tuple(
            identity.source_file_id for identity in case.selected_identities
        ),
        require_unselected_pending=False,
        database=case.database,
        cancel_check=None,
    )


@pytest.mark.asyncio
async def test_reused_source_invalid_artifacts_leave_any_nonempty_subset(
    monkeypatch,
) -> None:
    identities = tuple(_identity(index) for index in range(48))
    retained_identity = identities[0]
    complete_set = _verified_set(identities)
    selected_set = _verified_set((retained_identity,))
    _install_acquisition_mocks(monkeypatch, identities, complete_set, ())

    def validate_retained(artifact, **_keywords):
        if artifact.identity == retained_identity:
            return 1
        raise acquisition.UHCDrugArtifactAcquisitionError(
            "synthetic source rejection",
            failure_evidence=("artifact_rejected",),
        )

    selected_loader = AsyncMock(return_value=selected_set)
    monkeypatch.setattr(
        acquisition, "validate_retained_uhc_drug_artifact", validate_retained
    )
    monkeypatch.setattr(
        acquisition, "load_selected_source_artifact_set", selected_loader
    )
    database = object()

    acquisition_result = await acquisition.acquire_uhc_drug_artifacts(
        {"retained": "proof"}, database=database
    )

    assert acquisition_result.file_count == 1
    assert acquisition_result.downloaded_file_count == 0
    assert acquisition_result.reused_file_count == 1
    assert acquisition_result.excluded_file_count == 47
    assert acquisition_result.excluded_source_file_ids == tuple(
        identity.source_file_id for identity in identities[1:]
    )
    selected_loader.assert_awaited_once_with(
        identities,
        selected_source_file_ids=(retained_identity.source_file_id,),
        require_unselected_pending=False,
        database=database,
        cancel_check=None,
    )

    with pytest.raises(ValueError, match="result is invalid"):
        replace(acquisition_result, excluded_source_file_ids=())
    with pytest.raises(ValueError, match="result is invalid"):
        replace(
            acquisition_result,
            excluded_source_file_ids=(
                retained_identity.source_file_id,
                *acquisition_result.excluded_source_file_ids[1:],
            ),
        )


def test_retained_validation_reuses_staged_semantic_contract(monkeypatch) -> None:
    valid_artifact = _verified_set((_identity(0),)).artifacts[0]
    invalid_body = b"[{}]"
    invalid_artifact = _verified_set(
        (_identity(0, invalid_body),), body=invalid_body
    ).artifacts[0]
    body_by_artifact = {
        valid_artifact.artifact_sha256: VALID_BODY,
        invalid_artifact.artifact_sha256: invalid_body,
    }

    @contextmanager
    def open_retained(artifact):
        yield io.BytesIO(body_by_artifact[artifact.artifact_sha256])

    monkeypatch.setattr(
        staged_validation, "open_verified_source_artifact", open_retained
    )

    assert staged_validation.validate_retained_uhc_drug_artifact(valid_artifact) == 1
    with pytest.raises(acquisition.UHCDrugArtifactAcquisitionError) as caught:
        staged_validation.validate_retained_uhc_drug_artifact(invalid_artifact)
    assert caught.value.failure_evidence == ("artifact_rejected",)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_error",
    [
        OSError("synthetic retained I/O failure"),
        RetainedArtifactError("retained_artifact_path_unsafe"),
        RuntimeError("synthetic local validation failure"),
        asyncio.CancelledError("synthetic cancellation"),
    ],
)
async def test_reused_artifact_local_failures_remain_fatal(
    monkeypatch, source_error
) -> None:
    identities = tuple(_identity(index) for index in range(48))
    complete_set = _verified_set(identities)
    _install_acquisition_mocks(monkeypatch, identities, complete_set, ())

    def fail(*_arguments, **_keywords):
        raise source_error

    monkeypatch.setattr(acquisition, "validate_retained_uhc_drug_artifact", fail)
    with pytest.raises(type(source_error)):
        await acquisition.acquire_uhc_drug_artifacts(
            {"retained": "proof"}, database=object()
        )
