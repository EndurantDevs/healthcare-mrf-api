# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded transport failures for retained formulary source artifacts."""

from __future__ import annotations

import asyncio
from dataclasses import replace
import io
from types import SimpleNamespace

import aiohttp
import pytest

import process.formulary_fhir.uhc_drug_transport as transport
from process.formulary_fhir.uhc_drug_acquisition_lease import (
    UHCDrugSourceAcquisitionLeaseError,
)
from process.formulary_fhir.uhc_drug_payload import UHCDrugPayloadError
from tests.uhc_drug_parser_test_support import artifact_set


def _identity(*, expected_byte_count=10):
    artifacts, _bodies = artifact_set()
    return replace(
        artifacts.artifacts[0].identity,
        source_url=("https://providermrf.uhc.com/" "api/stream/ui/cs/drugs/test.json"),
        expected_byte_count=expected_byte_count,
    )


def test_environment_limits_reject_invalid_positive_values(
    monkeypatch,
) -> None:
    variable_name = "SYNTHETIC_POSITIVE_INTEGER"
    for raw_value in ("not-an-integer", "0"):
        monkeypatch.setenv(variable_name, raw_value)
        with pytest.raises(
            transport.UHCDrugArtifactAcquisitionError,
            match="positive integer",
        ):
            transport._positive_environment_integer(variable_name, 1)

    monkeypatch.setenv(
        "HLTHPRT_UHC_FORMULARY_DOWNLOAD_CONCURRENCY",
        str(transport.MAX_CONCURRENCY + 1),
    )
    with pytest.raises(
        transport.UHCDrugArtifactAcquisitionError,
        match="exceeds",
    ):
        transport.uhc_drug_download_concurrency()


@pytest.mark.asyncio
async def test_callback_and_default_session_are_async_safe() -> None:
    observed_values = []

    async def callback(value):
        observed_values.append(value)

    await transport._invoke(callback, "complete")
    assert observed_values == ["complete"]

    session = transport.default_uhc_drug_session_factory(aiohttp.ClientTimeout(total=1))
    assert session.auto_decompress is False
    await session.close()


def test_source_url_validation_rejects_invalid_and_noncanonical_values(
    monkeypatch,
) -> None:
    identity = _identity()
    invalid_identity = replace(
        identity,
        source_url="http://providermrf.uhc.com/file.json",
    )
    with pytest.raises(
        transport.UHCDrugArtifactAcquisitionError,
        match="URL is invalid",
    ):
        transport._validated_source_url(invalid_identity)

    monkeypatch.setattr(
        transport,
        "trusted_public_https_url",
        lambda _raw_url: "https://providermrf.uhc.com/different.json",
    )
    with pytest.raises(
        transport.UHCDrugArtifactAcquisitionError,
        match="not canonical",
    ):
        transport._validated_source_url(identity)


def test_response_headers_reject_transient_status_and_invalid_length() -> None:
    identity = _identity()
    source_url = identity.source_url
    retryable = SimpleNamespace(
        status=503,
        url=source_url,
        headers={},
        content_length=None,
    )
    with pytest.raises(transport.UHCDrugArtifactAcquisitionError) as caught:
        transport._declared_response_length(
            retryable,
            identity,
            source_url=source_url,
            max_bytes=100,
        )
    assert caught.value.retryable is True

    invalid_length = SimpleNamespace(
        status=200,
        url=source_url,
        headers={},
        content_length=0,
    )
    with pytest.raises(
        transport.UHCDrugArtifactAcquisitionError,
        match="declared byte count",
    ):
        transport._declared_response_length(
            invalid_length,
            identity,
            source_url=source_url,
            max_bytes=100,
        )


class _Content:
    def __init__(self, chunks):
        self.chunks = chunks

    async def iter_chunked(self, _chunk_size):
        for chunk in self.chunks:
            yield chunk


@pytest.mark.asyncio
async def test_response_body_skips_empty_chunks_and_enforces_byte_limit() -> None:
    output = io.BytesIO()
    digest, byte_count = await transport._download_response_body(
        SimpleNamespace(content=_Content([b"", b"abc"])),
        output,
        max_bytes=3,
        cancel_check=None,
    )
    assert byte_count == 3
    assert output.getvalue() == b"abc"
    assert len(digest) == 64

    with pytest.raises(
        transport.UHCDrugArtifactAcquisitionError,
        match="byte limit",
    ):
        await transport._download_response_body(
            SimpleNamespace(content=_Content([b"abcd"])),
            io.BytesIO(),
            max_bytes=3,
            cancel_check=None,
        )


def test_response_completion_distinguishes_empty_truncated_and_oversized() -> None:
    exact_identity = _identity(expected_byte_count=10)
    with pytest.raises(transport.UHCDrugArtifactAcquisitionError) as caught:
        transport._require_complete_response(
            exact_identity,
            declared_length=5,
            downloaded_byte_count=4,
        )
    assert caught.value.retryable is True

    with pytest.raises(transport.UHCDrugArtifactAcquisitionError) as caught:
        transport._require_complete_response(
            exact_identity,
            declared_length=None,
            downloaded_byte_count=9,
        )
    assert caught.value.retryable is True

    with pytest.raises(
        transport.UHCDrugArtifactAcquisitionError,
        match="inconsistent",
    ):
        transport._require_complete_response(
            exact_identity,
            declared_length=None,
            downloaded_byte_count=11,
        )

    transport._require_complete_response(
        _identity(expected_byte_count=None),
        declared_length=None,
        downloaded_byte_count=1,
    )


def test_object_array_validator_normalizes_parser_failure(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(
        transport,
        "uhc_drug_object_array_item_count",
        lambda *_arguments, **_keywords: (_ for _ in ()).throw(
            UHCDrugPayloadError("invalid")
        ),
    )
    with pytest.raises(
        transport.UHCDrugArtifactAcquisitionError,
        match="JSON structure",
    ):
        transport.validate_uhc_drug_object_array(tmp_path / "artifact.json")


@pytest.mark.asyncio
async def test_stage_cleanup_covers_transport_and_validation_failures(
    monkeypatch,
    tmp_path,
) -> None:
    identity = _identity()
    monkeypatch.setattr(transport, "_download_directory", lambda: tmp_path)

    async def transport_failure(*_arguments, **_keywords):
        raise aiohttp.ClientConnectionError("synthetic transport failure")

    monkeypatch.setattr(transport, "stream_uhc_drug_response", transport_failure)
    with pytest.raises(transport.UHCDrugArtifactAcquisitionError) as caught:
        await transport._download_to_stage(
            object(),
            identity,
            max_bytes=100,
            cancel_check=None,
        )
    assert caught.value.retryable is True
    assert tuple(tmp_path.glob("*.part")) == ()

    async def validation_failure(*_arguments, **_keywords):
        raise RuntimeError("synthetic validation failure")

    monkeypatch.setattr(transport, "stream_uhc_drug_response", validation_failure)
    with pytest.raises(RuntimeError, match="validation failure"):
        await transport._download_to_stage(
            object(),
            identity,
            max_bytes=100,
            cancel_check=None,
        )
    assert tuple(tmp_path.glob("*.part")) == ()


@pytest.mark.asyncio
async def test_stage_cleanup_handles_failures_before_path_creation(
    monkeypatch,
    tmp_path,
) -> None:
    identity = _identity()
    monkeypatch.setattr(transport, "_download_directory", lambda: tmp_path)

    def client_failure(*_arguments, **_keywords):
        raise aiohttp.ClientConnectionError("synthetic transport failure")

    monkeypatch.setattr(transport.tempfile, "NamedTemporaryFile", client_failure)
    with pytest.raises(transport.UHCDrugArtifactAcquisitionError):
        await transport._download_to_stage(
            object(),
            identity,
            max_bytes=100,
            cancel_check=None,
        )

    def local_failure(*_arguments, **_keywords):
        raise RuntimeError("synthetic local failure")

    monkeypatch.setattr(transport.tempfile, "NamedTemporaryFile", local_failure)
    with pytest.raises(RuntimeError, match="local failure"):
        await transport._download_to_stage(
            object(),
            identity,
            max_bytes=100,
            cancel_check=None,
        )


def test_preflight_rejects_file_aggregate_and_storage_bounds(
    monkeypatch,
    tmp_path,
) -> None:
    transport._preflight_pending_artifacts(
        (),
        max_file_bytes=100,
        concurrency=1,
    )
    identity = _identity(expected_byte_count=10)
    with pytest.raises(
        transport.UHCDrugArtifactAcquisitionError,
        match="configured byte limit",
    ):
        transport._preflight_pending_artifacts(
            (identity,),
            max_file_bytes=9,
            concurrency=1,
        )

    monkeypatch.setenv("HLTHPRT_UHC_FORMULARY_TOTAL_MAX_BYTES", "9")
    with pytest.raises(
        transport.UHCDrugArtifactAcquisitionError,
        match="aggregate byte limit",
    ):
        transport._preflight_pending_artifacts(
            (identity,),
            max_file_bytes=100,
            concurrency=1,
        )

    monkeypatch.setenv("HLTHPRT_UHC_FORMULARY_TOTAL_MAX_BYTES", "1000")
    monkeypatch.setenv("HLTHPRT_UHC_FORMULARY_MIN_FREE_BYTES", "1")
    monkeypatch.setattr(transport, "_download_directory", lambda: tmp_path)
    monkeypatch.setattr(
        transport.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=0),
    )
    with pytest.raises(
        transport.UHCDrugArtifactAcquisitionError,
        match="capacity",
    ):
        transport._preflight_pending_artifacts(
            (identity,),
            max_file_bytes=100,
            concurrency=1,
        )


@pytest.mark.asyncio
async def test_one_bad_file_does_not_starve_successes_and_retry_is_bounded() -> None:
    attempted_names: list[str] = []
    retained_names: set[str] = set()
    progress_rows: list[tuple[int, int, str, str]] = []

    async def acquire(file_name: str, *, fail: bool = False):
        attempted_names.append(file_name)
        await asyncio.sleep(0)
        if fail:
            raise transport.UHCDrugArtifactAcquisitionError(
                "private upstream detail",
                retryable=True,
            )
        retained_names.add(file_name)
        return 4, "cs", file_name, file_name, False

    first_tasks = (
        asyncio.create_task(acquire("bad.json", fail=True)),
        asyncio.create_task(acquire("good-a.json")),
        asyncio.create_task(acquire("good-b.json")),
    )
    with pytest.raises(
        transport.UHCDrugArtifactAcquisitionError,
        match="artifact set acquisition is incomplete",
    ) as caught:
        await transport._complete_pending_tasks(
            first_tasks,
            progress_callback=lambda *progress_row: progress_rows.append(progress_row),
        )

    assert attempted_names == ["bad.json", "good-a.json", "good-b.json"]
    assert retained_names == {"good-a.json", "good-b.json"}
    assert caught.value.retryable is True
    assert caught.value.failure_count == 1
    assert caught.value.retryable_failure_count == 1
    assert caught.value.failure_evidence == ("retryable_transport",)
    assert "private upstream detail" not in str(caught.value)
    assert {progress_row[3] for progress_row in progress_rows} == retained_names

    retry_result = await transport._complete_pending_tasks(
        (asyncio.create_task(acquire("bad.json")),),
        progress_callback=None,
    )
    assert retry_result == (4, ())
    assert attempted_names.count("bad.json") == 2
    assert attempted_names.count("good-a.json") == 1
    assert attempted_names.count("good-b.json") == 1
    assert retained_names == {"bad.json", "good-a.json", "good-b.json"}


@pytest.mark.asyncio
async def test_unmarked_nonretryable_failure_is_processing_evidence() -> None:
    async def fail():
        raise transport.UHCDrugArtifactAcquisitionError("private local detail")

    with pytest.raises(transport.UHCDrugArtifactAcquisitionError) as caught:
        await transport._complete_pending_tasks(
            (asyncio.create_task(fail()),),
            progress_callback=None,
        )

    assert caught.value.failure_evidence == ("artifact_processing",)


@pytest.mark.asyncio
async def test_lease_loss_is_preserved_and_cancels_pending_transport() -> None:
    sibling_started = asyncio.Event()
    sibling_cleaned = asyncio.Event()

    async def lease_lost() -> tuple[int, str, str, str, bool]:
        await sibling_started.wait()
        raise UHCDrugSourceAcquisitionLeaseError("lease_lost")

    async def sibling() -> tuple[int, str, str, str, bool]:
        sibling_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            sibling_cleaned.set()
        return 0, "cs", "never.json", "never", False

    tasks = (
        asyncio.create_task(lease_lost()),
        asyncio.create_task(sibling()),
    )
    with pytest.raises(UHCDrugSourceAcquisitionLeaseError) as caught:
        await transport._complete_pending_tasks(tasks, progress_callback=None)

    assert caught.value.code == "lease_lost"
    assert sibling_cleaned.is_set()
